"""Control socket framing, ownership, and CLI routing without a FUSE mount."""

import errno
import fcntl
import json
import logging
import os
import socket
import stat
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

import pytest
from click.testing import CliRunner

from storage import control as control_module
from storage.cli import main
from storage.control import (
    CONTROL_SOCKET_NAME,
    MAX_REQUEST_BYTES,
    ControlError,
    ControlServer,
    _check_peer,
    canonical_mountpoint,
    request_control_payload,
)


@pytest.fixture
def cache():
    # Keep the path under the Unix socket address limit with a short TMPDIR.
    with tempfile.TemporaryDirectory(prefix=".sc-") as directory:
        yield Path(directory)


def server_for(cache, dispatch):
    return ControlServer(cache, dispatch, logging.getLogger(__name__))


def read_response(conn):
    with conn.makefile("rb") as stream:
        return json.loads(stream.readline())


def socket_for(cache):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(3.0)
    sock.connect(str(cache / CONTROL_SOCKET_NAME))
    return sock


def test_fragmented_request_and_bounded_frames(cache):
    dispatched = threading.Event()

    def dispatch(command):
        dispatched.set()
        return {"command": command}

    server = server_for(cache, dispatch)
    server.start()
    try:
        with socket_for(cache) as conn:
            conn.sendall(b"streaming-")
            assert not dispatched.wait(0.05)
            conn.sendall(b"stats\n")
            assert read_response(conn) == {"command": "streaming-stats"}
        assert dispatched.is_set()
        dispatched.clear()

        with socket_for(cache) as conn:
            conn.sendall(b"x" * (MAX_REQUEST_BYTES + 1))
            assert "too large" in read_response(conn)["error"]
        with socket_for(cache) as conn:
            conn.sendall(b"without newline")
            conn.shutdown(socket.SHUT_WR)
            assert "Incomplete" in read_response(conn)["error"]
        with socket_for(cache) as conn:
            conn.sendall(b"\xff\n")
            assert "UTF-8" in read_response(conn)["error"]
        assert not dispatched.is_set()
        assert request_control_payload(cache, b"x" * MAX_REQUEST_BYTES, 2) == {
            "command": "x" * MAX_REQUEST_BYTES,
        }
    finally:
        server.stop()


def test_legacy_stats_request_may_end_at_eof(cache):
    frames = []
    server = server_for(cache, lambda frame: frames.append(frame) or {"frame": frame})
    server.start()
    try:
        with socket_for(cache) as conn:
            conn.sendall(b"streaming-stats")
            conn.shutdown(socket.SHUT_WR)
            assert read_response(conn) == {"frame": "streaming-stats"}
        # Only that exact frame: anything else unterminated is incomplete.
        for unterminated in (b"streaming-stats ", b"status", b"streaming-stat"):
            with socket_for(cache) as conn:
                conn.sendall(unterminated)
                conn.shutdown(socket.SHUT_WR)
                assert "Incomplete" in read_response(conn)["error"]
        assert frames == ["streaming-stats"]
    finally:
        server.stop()


def test_server_bounds_and_encodes_its_responses(cache, monkeypatch, caplog):
    monkeypatch.setattr("storage.control.MAX_RESPONSE_BYTES", 256)

    def dispatch(frame):
        if frame == "large":
            return {"data": "x" * 300}
        return {"data": object()}  # Not JSON-encodable.

    server = server_for(cache, dispatch)
    server.start()
    try:
        with pytest.raises(ControlError, match="^Control response is too large$"):
            request_control_payload(cache, b"large", 2)
        with caplog.at_level(logging.ERROR, logger=__name__):
            with pytest.raises(ControlError, match="^Could not encode control response$"):
                request_control_payload(cache, b"unencodable", 2)
        assert [r.getMessage() for r in caplog.records] == ["could not encode mount control response"]
    finally:
        server.stop()


def test_client_refuses_a_response_over_its_bound(cache, monkeypatch):
    monkeypatch.setattr("storage.control.MAX_RESPONSE_BYTES", 256)
    with raw_listener(cache / CONTROL_SOCKET_NAME) as listener:
        listener.settimeout(5)

        def answer():
            conn, _ = listener.accept()
            with conn:
                conn.recv(4096)
                try:
                    conn.sendall(b'{"data": "' + b"x" * 1024 + b'"}\n')
                except OSError:
                    pass  # The client stopped reading at its bound.

        responder = threading.Thread(target=answer)
        responder.start()
        try:
            with pytest.raises(ControlError, match="frame is too large"):
                request_control_payload(cache, b"status", 2)
        finally:
            responder.join(10)


def test_a_request_read_while_stopping_is_answered(cache, monkeypatch):
    # A complete, authenticated request that arrives as the mount stops gets
    # a reply saying so, not a bare EOF.
    dispatched = []
    server = server_for(cache, lambda frame: dispatched.append(frame) or {})
    server.start()
    real_read_line = control_module._read_line

    def read_then_stop(sock, *args, **kwargs):
        frame = real_read_line(sock, *args, **kwargs)
        server._stop.set()
        return frame

    monkeypatch.setattr("storage.control._read_line", read_then_stop)
    try:
        with socket_for(cache) as conn:
            conn.sendall(b"status\n")
            reply = read_response(conn)
        assert reply == {"error": "This mount is stopping; its control socket no longer serves requests"}
        assert dispatched == []
    finally:
        monkeypatch.undo()
        server.stop()


def test_request_timeout_uses_a_shared_deadline(cache, monkeypatch):
    monkeypatch.setattr("storage.control.REQUEST_TIMEOUT", 0.3)
    server = server_for(cache, lambda _: {"command_ran": True})
    server.start()
    try:
        with socket_for(cache) as conn:
            # The client keeps dripping until it has the reply, so a server
            # that restarted its timeout on each byte would never answer.
            # No wall-clock bound: a loaded host may answer late, not wrongly.
            conn.settimeout(30.0)
            stop_dripping = threading.Event()

            def drip():
                try:
                    while not stop_dripping.wait(0.04):
                        conn.sendall(b"x")
                except OSError:
                    pass  # Deadline expired and the server closed the connection.

            conn.sendall(b"partial")
            sender = threading.Thread(target=drip)
            sender.start()
            try:
                assert read_response(conn) == {"error": "Control request timed out"}
            finally:
                stop_dripping.set()
                sender.join()
        assert request_control_payload(cache, b"complete", 2) == {"command_ran": True}
    finally:
        server.stop()


def test_preserves_live_listener_and_recovers_stale_socket(cache):
    path = cache / CONTROL_SOCKET_NAME
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as obsolete:
        obsolete.bind(str(path))
    assert path.is_socket()

    first = server_for(cache, lambda _: {"mount": "first"})
    second = server_for(cache, lambda _: {"mount": "second"})
    first.start()
    try:
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert request_control_payload(cache, b"status", 2) == {"mount": "first"}
        second.start()
        second.stop()
        assert request_control_payload(cache, b"status", 2) == {"mount": "first"}
    finally:
        second.stop()
        first.stop()
    assert not path.exists()


def fds_open_on(directory):
    found = []
    for fd in os.listdir("/proc/self/fd"):
        try:
            if os.readlink(f"/proc/self/fd/{fd}") == str(directory):
                found.append(int(fd))
        except OSError:
            pass  # The listing's own fd, closed by now.
    return found


def test_cache_directory_is_pinned_while_listening(cache):
    # The reported identity must not outlive the directory: holding it open
    # keeps its inode from being reused while the socket can answer.
    assert fds_open_on(cache) == []
    server = server_for(cache, lambda _: {})
    server.start()
    try:
        pinned, = fds_open_on(cache)
        assert pinned == server._cache_fd
        entry = os.stat(cache)
        assert server.cache_identity == (entry.st_dev, entry.st_ino)
        # The pin is a separate open: it does not hold the cache lock.
        probe = os.open(cache, os.O_RDONLY | os.O_DIRECTORY)
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(probe)
    finally:
        server.stop()
    assert fds_open_on(cache) == []
    assert server._cache_fd is None and server.cache_identity is None
    assert not (cache / CONTROL_SOCKET_NAME).exists()


def test_stop_cleans_up_in_the_published_directory_after_a_rename(cache):
    server = server_for(cache, lambda _: {})
    server.start()
    moved = cache.with_name(cache.name + "-moved")
    try:
        cache.rename(moved)
        cache.mkdir()
    finally:
        server.stop()
    assert not (moved / CONTROL_SOCKET_NAME).exists()
    moved.rmdir()


def test_request_goes_through_the_given_cache_fd(cache):
    server = server_for(cache, lambda frame: {"via": frame})
    server.start()
    moved = cache.with_name(cache.name + "-moved")
    fd = os.open(cache, os.O_RDONLY | os.O_DIRECTORY)
    try:
        # The path now names an empty directory; the fd still names ours.
        cache.rename(moved)
        cache.mkdir()
        assert request_control_payload(cache, b"pinned", 2, cache_fd=fd) == {"via": "pinned"}
        with pytest.raises(ControlError, match="No running mount control socket"):
            request_control_payload(cache, b"by path", 2)
    finally:
        os.close(fd)
        server.stop()
        moved.rmdir()


def test_request_needs_only_search_permission_on_the_cache(cache):
    # As connecting by path did: a cache whose read bit was removed while
    # the mount runs stays reachable for its owner.
    server = server_for(cache, lambda frame: {"via": frame})
    server.start()
    try:
        cache.chmod(0o300)
        try:
            assert request_control_payload(cache, b"search-only", 2) == {"via": "search-only"}
        finally:
            cache.chmod(0o700)
    finally:
        server.stop()


def test_cli_identifies_and_connects_through_one_cache_open(cache, monkeypatch):
    server, requests = recording_server(cache, None)
    server.start()
    opened = []
    real_open = control_module.open_cache_directory

    def counting_open(path):
        fd = real_open(path)
        opened.append(fd)
        return fd

    monkeypatch.setattr("storage.cli_control.open_cache_directory", counting_open)
    try:
        result = CliRunner().invoke(main, ["control", "--cache", str(cache), "--json-output",
                                           "set", "--max-download-workers", "2"])
        assert result.exit_code == 0, result.output
        assert len(opened) == 1
        assert [request["command"] for request in requests] == ["status", "streaming.set"]
        # Closed when the command is done: only the server's pin is left.
        assert fds_open_on(cache) == [server._cache_fd]
    finally:
        server.stop()


def other_uid_in(function_name):
    """geteuid() as seen from `function_name` only: another user's uid."""
    real = os.geteuid()

    def geteuid():
        return real + 1 if sys._getframe(1).f_code.co_name == function_name else real

    return geteuid


def test_check_peer_refuses_another_users_peer(monkeypatch):
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    with left, right:
        _check_peer(left)  # Same user: accepted.
        other = os.geteuid() + 1
        monkeypatch.setattr("storage.control.os.geteuid", lambda: other)
        with pytest.raises(ControlError, match="not the mount owner"):
            _check_peer(left)


def test_server_drops_a_peer_that_is_not_the_mount_owner(cache, monkeypatch):
    dispatched = []
    server = server_for(cache, lambda frame: dispatched.append(frame) or {"served": True})
    server.start()
    try:
        # Applied after start: publication checks ownership too.
        monkeypatch.setattr("storage.control.os.geteuid", other_uid_in("_check_peer"))
        with socket_for(cache) as conn:
            # The server may close before, during or after our send.
            try:
                conn.sendall(b"status\n")
                answer = conn.recv(4096)
            except (BrokenPipeError, ConnectionResetError):
                answer = b""
            assert answer == b""
        assert dispatched == []
        monkeypatch.undo()
        assert request_control_payload(cache, b"status", 2) == {"served": True}
        assert dispatched == ["status"]
    finally:
        monkeypatch.undo()
        server.stop()


def raw_listener(path):
    """A plain listener owned by this user, recording what reaches it."""
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(path))
    listener.listen(4)
    listener.settimeout(0.2)
    return listener


def accepted(listener):
    try:
        conn, _ = listener.accept()
    except socket.timeout:
        return None
    with conn:
        conn.settimeout(2)
        return conn.recv(4096)


def test_client_refuses_a_peer_that_is_not_the_mount_owner(cache, monkeypatch):
    with raw_listener(cache / CONTROL_SOCKET_NAME) as listener:
        monkeypatch.setattr("storage.control.os.geteuid", other_uid_in("_check_peer"))
        with pytest.raises(ControlError, match="not the mount owner"):
            request_control_payload(cache, b"status", 2)
        # Connected (the check needs a peer), but nothing was sent.
        assert accepted(listener) == b""


@pytest.mark.parametrize("entry", ["symlink", "file", "fifo"])
def test_client_refuses_a_control_entry_that_is_not_our_socket(cache, entry):
    elsewhere = cache / "elsewhere"
    elsewhere.mkdir()
    with raw_listener(elsewhere / "live.sock") as listener:
        path = cache / CONTROL_SOCKET_NAME
        if entry == "symlink":
            path.symlink_to(elsewhere / "live.sock")
        elif entry == "file":
            path.write_text("not a socket")
        else:
            os.mkfifo(path)
        with pytest.raises(ControlError, match="Expected a mount control socket owned by this user"):
            request_control_payload(cache, b"status", 2)
        # Refused before connecting: the live socket behind the link saw nothing.
        assert accepted(listener) is None


def test_never_replaces_arbitrary_entries(cache):
    path = cache / CONTROL_SOCKET_NAME
    path.write_text("unrelated file")
    server = server_for(cache, lambda _: {})
    server.start()
    server.stop()
    assert path.read_text() == "unrelated file"

    path.unlink()
    target = cache / "target"
    target.write_text("symlink target")
    path.symlink_to(target)
    server.start()
    server.stop()
    assert path.is_symlink()
    assert target.read_text() == "symlink target"

    path.unlink()
    server.start()
    try:
        assert request_control_payload(cache, b"status", 2) == {}
        path.unlink()
        path.write_text("another publisher")
    finally:
        server.stop()
    assert path.read_text() == "another publisher"


def write_profile(cache, mountpoint="/never-mounted"):
    config = cache / "storage.toml"
    # TOML has no surrogate escapes, so write non-ASCII paths as UTF-8.
    config.write_text(f"""
[mounts.example]
mountpoint = {json.dumps(str(mountpoint), ensure_ascii=False)}
cache = {json.dumps(str(cache))}
backends = ["/no-member-probe"]
""", encoding="utf-8")
    return config


def cache_identity(cache):
    entry = os.stat(cache)
    return [entry.st_dev, entry.st_ino]


def recording_server(cache, mountpoint, reported_cache=None):
    """A stub mount reporting `mountpoint` and a cache identity (by default
    `cache`'s); it does not check the identities requests name."""
    requests = []
    reported_cache = cache_identity(cache) if reported_cache is None else reported_cache

    def dispatch(frame):
        request = json.loads(frame)
        requests.append(request)
        return {"ok": True, "result": "cancelled",
                "mount": {"mountpoint": mountpoint, "pid": 1, "cache": reported_cache}}

    return server_for(cache, dispatch), requests


def test_profile_selection_and_atomic_set_request(cache):
    config = write_profile(cache)
    server, requests = recording_server(cache, "/never-mounted")
    server.start()
    try:
        runner = CliRunner()
        options = ["control", "--profile", "example", "--config", str(config), "--json-output"]
        result = runner.invoke(main, options)
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["ok"] is True

        result = runner.invoke(main, options + ["set", "--max-streaming-sessions", "3",
                                                "--max-retained-streaming-sessions", "0"])
        assert result.exit_code == 0, result.output
        # Only the mutation's own response is printed, not the identity check.
        assert json.loads(result.output)["ok"] is True
        named = {"mountpoint": "/never-mounted", "cache": cache_identity(cache)}
        identity = {"command": "status", **named}
        assert requests == [
            identity,
            identity,
            {"command": "streaming.set", **named, "limits": {
                "max_streaming_sessions": 3, "max_retained_streaming_sessions": 0,
            }},
        ]
        assert runner.invoke(main, options + ["--cache", str(cache)]).exit_code == 2
        assert runner.invoke(main, options + ["set"]).exit_code == 2
        assert runner.invoke(main, ["control", "set", "--help"]).exit_code == 0
        assert len(requests) == 3

        # --cache names the cache directory only: no mountpoint to check or send.
        requests.clear()
        result = runner.invoke(main, ["control", "--cache", str(cache), "cancel", "0" * 32])
        assert result.exit_code == 0, result.output
        assert requests == [
            {"command": "status", "cache": cache_identity(cache)},
            {"command": "downloads.cancel", "id": "0" * 32, "cache": cache_identity(cache)},
        ]
    finally:
        server.stop()


def test_profile_refuses_a_socket_answering_for_another_mount(cache):
    # Two mounts sharing one cache: the socket belongs to the other one.
    config = write_profile(cache, "/profile-b")
    server, requests = recording_server(cache, "/profile-a")
    server.start()
    try:
        runner = CliRunner()
        options = ["control", "--profile", "example", "--config", str(config)]
        for command in (["set", "--max-download-workers", "2"], ["cancel", "0" * 32], ["status"]):
            requests.clear()
            result = runner.invoke(main, options + command)
            assert result.exit_code == 1, result.output
            assert "belongs to the mount at '/profile-a'" in result.output
            assert "mountpoint '/profile-b'" in result.output
            # Only the identity check (or the status itself) was sent.
            assert requests == [{"command": "status", "mountpoint": "/profile-b",
                                 "cache": cache_identity(cache)}]
    finally:
        server.stop()


@pytest.mark.parametrize("by_profile", [False, True])
def test_refuses_a_socket_published_in_another_cache(cache, by_profile):
    # A group writer moved another mount's live socket into this cache.
    config = write_profile(cache)
    other = cache_identity(cache)
    other[1] += 1
    server, requests = recording_server(cache, "/never-mounted", reported_cache=other)
    server.start()
    try:
        runner = CliRunner()
        if by_profile:
            options = ["control", "--profile", "example", "--config", str(config)]
        else:
            options = ["control", "--cache", str(cache)]
        for command in (["set", "--max-download-workers", "2"], ["cancel", "0" * 32], ["status"]):
            requests.clear()
            result = runner.invoke(main, options + command)
            assert result.exit_code == 1, result.output
            assert "published in a different cache directory" in result.output
            assert [request["command"] for request in requests] == ["status"]
            assert requests[0]["cache"] == cache_identity(cache)
    finally:
        server.stop()


def test_long_multibyte_mountpoint_fits_a_request(cache):
    # Directories of 63 four-byte characters (252 bytes each) to nearly
    # PATH_MAX, each character escaped to a twelve-byte surrogate pair in
    # the request. They exist: the mountpoint's parents must.
    component = "\U00020000" * 63
    path = str(cache)
    while len(os.fsencode(path)) + 2 * (len(os.fsencode(component)) + 1) < 4090:
        path = f"{path}/{component}"
        os.mkdir(path)
    mountpoint = canonical_mountpoint(f"{path}/{component}")
    assert 3800 < len(os.fsencode(mountpoint)) < 4096
    config = write_profile(cache, mountpoint)
    server, requests = recording_server(cache, mountpoint)
    server.start()
    try:
        runner = CliRunner()
        options = ["control", "--profile", "example", "--config", str(config), "--json-output"]
        for command in ([], ["set", "--max-download-workers", "2"]):
            result = runner.invoke(main, options + command)
            assert result.exit_code == 0, result.output
        assert [request["mountpoint"] for request in requests] == [mountpoint] * 3
    finally:
        server.stop()


def test_worst_case_escaped_mountpoint_fits_a_request(cache):
    # Every byte of a 4095-byte path undecodable, so each escapes to six bytes.
    mountpoint = os.fsdecode(b"/" + b"\xff" * 4094)
    request = json.dumps({"command": "downloads.cancel", "id": "0" * 32,
                          "mountpoint": mountpoint}).encode("ascii")
    assert len(request) > 6 * 4094
    server = server_for(cache, lambda frame: {"mountpoint": json.loads(frame)["mountpoint"]})
    server.start()
    try:
        assert request_control_payload(cache, request, 2) == {"mountpoint": mountpoint}
    finally:
        server.stop()


def test_reported_mountpoints_are_quoted(cache):
    # A mountpoint with control characters cannot forge output lines.
    config = write_profile(cache, "/profile-b")
    server, _ = recording_server(cache, "/mnt/a\nError: forged line")
    server.start()
    try:
        result = CliRunner().invoke(main, ["control", "--profile", "example", "--config", str(config)])
        assert result.exit_code == 1, result.output
        assert "'/mnt/a\\nError: forged line'" in result.output
        assert "\nError: forged" not in result.output
    finally:
        server.stop()

    stats = dict.fromkeys(["pid", "active_reader_sessions", "retained_streaming_sessions",
                           "download_workers_running", "download_workers",
                           "download_sessions_queued"], 0)
    server = server_for(cache, lambda _: {
        "mount": {"mountpoint": "/mnt/a\nQueued downloads: 0", "pid": 0, "cache": cache_identity(cache)},
        "stats": stats, "limits": {}, "restart_only": {},
    })
    server.start()
    try:
        result = CliRunner().invoke(main, ["control", "--cache", str(cache)])
        assert result.exit_code == 0, result.output
        assert "  Mountpoint: '/mnt/a\\nQueued downloads: 0'\n" in result.output
        assert result.output.count("Queued downloads: 0") == 2
    finally:
        server.stop()


def test_mount_without_a_mountpoint_is_reached_only_by_cache(cache):
    # A mount whose mountpoint could not be canonicalized reports null.
    config = write_profile(cache)
    server, requests = recording_server(cache, None)
    server.start()
    try:
        runner = CliRunner()
        result = runner.invoke(main, ["control", "--profile", "example", "--config", str(config),
                                      "set", "--max-download-workers", "2"])
        assert result.exit_code == 1, result.output
        assert "reports no mountpoint" in result.output
        assert [request["command"] for request in requests] == ["status"]
        requests.clear()
        result = runner.invoke(main, ["control", "--cache", str(cache), "--json-output",
                                      "set", "--max-download-workers", "2"])
        assert result.exit_code == 0, result.output
        assert [request["command"] for request in requests] == ["status", "streaming.set"]
    finally:
        server.stop()


def full_status(cache, mountpoint):
    stats = dict.fromkeys(["pid", "active_reader_sessions", "retained_streaming_sessions",
                           "download_workers_running", "download_workers",
                           "download_sessions_queued"], 0)
    return {"mount": {"mountpoint": mountpoint, "pid": 0, "cache": cache_identity(cache)},
            "stats": stats, "limits": {}, "restart_only": {}}


def test_status_says_how_to_reach_a_mount_without_a_mountpoint(cache):
    server = server_for(cache, lambda _: full_status(cache, None))
    server.start()
    try:
        result = CliRunner().invoke(main, ["control", "--cache", str(cache)])
        assert result.exit_code == 0, result.output
        assert "  Mountpoint: (not reported; reach it with --cache)\n" in result.output
        assert "None" not in result.output
    finally:
        server.stop()


@pytest.mark.parametrize("reason, text", [
    ("readers", "it has open readers"),
    ("finalizing", "it is being finalized"),
    ("stopping", "finishing a failed or cancelled download; retry shortly"),
    (None, "close its readers or wait for finalization"),
])
def test_busy_cancel_says_why(cache, reason, text):
    def dispatch(frame):
        if json.loads(frame)["command"] == "status":
            return full_status(cache, None)
        reply = {"id": "0" * 32, "result": "busy"}
        if reason is not None:
            reply["reason"] = reason
        return reply

    server = server_for(cache, dispatch)
    server.start()
    try:
        result = CliRunner().invoke(main, ["control", "--cache", str(cache), "cancel", "0" * 32])
        assert result.exit_code == 3, result.output
        assert text in result.output
    finally:
        server.stop()


@pytest.mark.parametrize("outcome, code", [("cancelled", 0), ("busy", 3), ("not-found", 4),
                                           ("unexpected", 1)])
def test_cancel_outcomes_have_distinct_exit_codes(cache, outcome, code):
    def dispatch(frame):
        if json.loads(frame)["command"] == "status":
            return full_status(cache, None)
        return {"id": "0" * 32, "result": outcome}

    server = server_for(cache, dispatch)
    server.start()
    try:
        for json_output in ([], ["--json-output"]):
            result = CliRunner().invoke(main, ["control", "--cache", str(cache), *json_output,
                                               "cancel", "0" * 32])
            assert result.exit_code == code, result.output
    finally:
        server.stop()


def test_profile_refuses_a_socket_that_reports_no_mount(cache):
    config = write_profile(cache)
    requests = []

    def dispatch(frame):
        requests.append(json.loads(frame))
        return {"ok": True}

    server = server_for(cache, dispatch)
    server.start()
    try:
        result = CliRunner().invoke(main, ["control", "--profile", "example", "--config", str(config),
                                           "set", "--max-download-workers", "2"])
        assert result.exit_code == 1, result.output
        assert "did not report which mount" in result.output
        assert [request["command"] for request in requests] == ["status"]
    finally:
        server.stop()


def test_canonical_mountpoint(tmp_path, monkeypatch):
    real = tmp_path / "real"
    (real / "mount").mkdir(parents=True)
    real = real.resolve()
    (tmp_path / "alias").symlink_to(real)
    (real / "final").symlink_to(real / "mount")
    monkeypatch.chdir(real)
    assert canonical_mountpoint(str(tmp_path / "alias" / "mount") + "/") == str(real / "mount")
    assert canonical_mountpoint("mount") == str(real / "mount")
    (real / "x").mkdir()
    assert canonical_mountpoint("./x/../mount") == str(real / "mount")
    # A final symlink is followed (readlink), to the directory mounted on.
    assert canonical_mountpoint(str(tmp_path / "alias" / "final")) == str(real / "mount")
    (real / "relative").symlink_to("final")
    assert canonical_mountpoint(str(real / "relative")) == str(real / "mount")
    (real / "dangling").symlink_to(real / "not yet")
    assert canonical_mountpoint(str(real / "dangling")) == str(real / "not yet")
    (real / "loop").symlink_to(real / "loop")
    with pytest.raises(ControlError, match="too many levels"):
        canonical_mountpoint(str(real / "loop"))
    (real / "dotted").symlink_to(str(real / "mount") + "/.")
    with pytest.raises(ControlError, match="without a final"):
        canonical_mountpoint(str(real / "dotted"))
    assert canonical_mountpoint("/") == "/"
    assert canonical_mountpoint("//") == "/"


def test_canonical_mountpoint_errors_are_control_errors(tmp_path, monkeypatch):
    with pytest.raises(ControlError, match="Cannot resolve mountpoint"):
        canonical_mountpoint("/mnt/with\0nul")

    def gone():
        raise FileNotFoundError(2, "No such file or directory")

    monkeypatch.setattr("storage.control.os.getcwd", gone)
    with pytest.raises(ControlError, match="Cannot resolve mountpoint 'relative'"):
        canonical_mountpoint("relative")

    def denied(path, *args, **kwargs):
        raise PermissionError(13, "Permission denied", path)

    monkeypatch.undo()
    monkeypatch.setattr("storage.control.os.readlink", denied)
    with pytest.raises(ControlError, match="Permission denied"):
        canonical_mountpoint(str(tmp_path / "x"))


def test_a_link_through_a_missing_directory_is_refused(tmp_path):
    # Without strict resolution, missing/../a would be read as the sibling a,
    # matching a mount there.
    (tmp_path / "a").mkdir()
    link = tmp_path / "media"
    link.symlink_to(tmp_path / "missing" / ".." / "a")
    with pytest.raises(ControlError, match="Cannot resolve mountpoint"):
        canonical_mountpoint(str(link))
    with pytest.raises(ControlError, match="Cannot resolve mountpoint"):
        canonical_mountpoint(str(tmp_path / "missing" / ".." / "a"))
    assert canonical_mountpoint(str(tmp_path / "a")) == str((tmp_path / "a").resolve())


def test_a_parent_through_a_regular_file_is_refused(tmp_path, monkeypatch):
    # realpath accepts regular-file/.. lexically; the kernel lookup does not.
    (tmp_path / "a").mkdir()
    (tmp_path / "regular-file").write_text("not a directory")
    with pytest.raises(ControlError, match="Not a directory"):
        canonical_mountpoint(str(tmp_path / "regular-file" / ".." / "a" / "mount"))
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ControlError, match="Not a directory"):
        canonical_mountpoint("regular-file/../a")
    link = tmp_path / "media"
    link.symlink_to(tmp_path / "regular-file" / ".." / "a")
    with pytest.raises(ControlError, match="Not a directory"):
        canonical_mountpoint(str(link))


def test_canonical_mountpoint_never_stats_the_final_directory(tmp_path, monkeypatch):
    # Only readlink touches the last component; a stat there would ask a
    # hung mount's filesystem.
    (tmp_path / "mount").mkdir()
    (tmp_path / "link").symlink_to(tmp_path / "mount")
    final = str(tmp_path.resolve() / "mount")
    real_realpath = os.path.realpath

    def guarded_realpath(path, *args, **kwargs):
        assert not str(path).startswith(final), f"resolved through {path!r}"
        return real_realpath(path, *args, **kwargs)

    monkeypatch.setattr("storage.control.os.path.realpath", guarded_realpath)
    assert canonical_mountpoint(str(tmp_path / "link")) == final
    assert canonical_mountpoint(final) == final


def test_profile_follows_a_retargeted_mountpoint_symlink(cache, tmp_path):
    # The mount was made through /…/media -> a; the link now points at b.
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    link = tmp_path / "media"
    link.symlink_to(tmp_path / "a")
    mounted_on = canonical_mountpoint(str(link))
    assert mounted_on == str((tmp_path / "a").resolve())
    config = write_profile(cache, link)
    server, requests = recording_server(cache, mounted_on)
    server.start()
    try:
        runner = CliRunner()
        options = ["control", "--profile", "example", "--config", str(config), "--json-output"]
        assert runner.invoke(main, options + ["set", "--max-download-workers", "2"]).exit_code == 0
        link.unlink()
        link.symlink_to(tmp_path / "b")
        requests.clear()
        result = runner.invoke(main, options + ["set", "--max-download-workers", "2"])
        assert result.exit_code == 1, result.output
        assert "belongs to the mount at" in result.output
        assert [request["command"] for request in requests] == ["status"]
        requests.clear()
        result = runner.invoke(main, ["control", "--cache", str(cache), "--json-output",
                                      "set", "--max-download-workers", "2"])
        assert result.exit_code == 0, result.output
        assert [request["command"] for request in requests] == ["status", "streaming.set"]
    finally:
        server.stop()


@pytest.mark.parametrize("suffix", ["/.", "/..", "/./", "/../", "/.//"])
def test_canonical_mountpoint_refuses_a_final_dot_component(tmp_path, monkeypatch, suffix):
    # Literal strings: Path("x") / "." would drop the suffix. Resolving a
    # final '.' or '..' would stat the mount root, which can hang.
    mountpoint = str(tmp_path / "mount") + suffix

    def no_resolution(path):
        raise AssertionError(f"resolved {path!r}")

    monkeypatch.setattr("storage.control.os.path.realpath", no_resolution)
    with pytest.raises(ControlError, match="without a final '.' or '..'"):
        canonical_mountpoint(mountpoint)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ControlError, match="without a final '.' or '..'"):
        canonical_mountpoint("mount" + suffix)


def test_profile_with_a_final_dot_mountpoint_is_refused(cache):
    config = write_profile(cache, "/mnt/profile/.")
    server, requests = recording_server(cache, "/mnt/profile")
    server.start()
    try:
        result = CliRunner().invoke(main, ["control", "--profile", "example", "--config", str(config)])
        assert result.exit_code == 1, result.output
        assert "without a final '.' or '..'" in result.output
        assert requests == []
    finally:
        server.stop()


def test_canonical_mountpoint_resolves_dotdot_after_symlinks(tmp_path):
    # srv/alias -> data/subdir, so the kernel resolves srv/alias/../mount
    # to data/mount; lexical normalization would give srv/mount.
    base = tmp_path.resolve()
    (base / "data" / "subdir").mkdir(parents=True)
    (base / "data" / "mount").mkdir()
    (base / "srv" / "mount").mkdir(parents=True)
    (base / "srv" / "alias").symlink_to(base / "data" / "subdir")
    through_alias = canonical_mountpoint(str(base / "srv" / "alias" / ".." / "mount"))
    assert through_alias == str(base / "data" / "mount")
    assert through_alias != canonical_mountpoint(str(base / "srv" / "mount"))
    assert os.path.samefile(through_alias, base / "srv" / "alias" / ".." / "mount")


def test_handler_errors_are_logged_and_answered(cache, caplog):
    # An OSError from the handler is a server bug, not a client hangup.
    def dispatch(command):
        if command == "control-error":
            raise ControlError("rejected request")
        raise OSError(5, "handler failed")

    server = server_for(cache, dispatch)
    server.start()
    try:
        with caplog.at_level(logging.ERROR, logger=__name__):
            with pytest.raises(ControlError, match="^Mount control request failed$"):
                request_control_payload(cache, b"os-error", 2)
            assert [r.getMessage() for r in caplog.records] == ["mount control request failed"]
            assert isinstance(caplog.records[0].exc_info[1], OSError)
            caplog.clear()
            with pytest.raises(ControlError, match="^rejected request$"):
                request_control_payload(cache, b"control-error", 2)
            assert caplog.records == []
    finally:
        server.stop()


def test_restart_while_a_stopped_thread_is_still_in_a_request(cache, monkeypatch, caplog):
    # stop() gives up on a handler that outlasts its join; a later start()
    # must still publish and serve, and the old thread must not serve again.
    monkeypatch.setattr("storage.control.REQUEST_TIMEOUT", 0.2)
    entered = threading.Event()
    release = threading.Event()

    def dispatch(command):
        if command == "block":
            entered.set()
            assert release.wait(30)
        return {"command": command}

    server = server_for(cache, dispatch)
    server.start()
    old_thread = server._thread
    try:
        with socket_for(cache) as blocked:
            blocked.settimeout(30.0)
            blocked.sendall(b"block\n")
            assert entered.wait(30)
            with caplog.at_level(logging.WARNING, logger=__name__):
                server.stop()
            assert old_thread.is_alive()
            assert any("still in a request" in r.getMessage() for r in caplog.records)

            server.start()
            assert server._thread is not old_thread
            assert request_control_payload(cache, b"after-restart", 5) == {
                "command": "after-restart",
            }
            release.set()
            assert read_response(blocked) == {"command": "block"}
        old_thread.join(30)
        assert not old_thread.is_alive()
        assert request_control_payload(cache, b"still-served", 5) == {"command": "still-served"}
    finally:
        release.set()
        server.stop()
    assert not (cache / CONTROL_SOCKET_NAME).exists()


def test_publication_failure_releases_the_waiting_thread(cache, monkeypatch):
    server = server_for(cache, lambda _: {})
    worker_exited = threading.Event()
    serve = server._serve

    def record_exit(*args):
        serve(*args)
        worker_exited.set()

    def fail_publication(listener):
        raise OSError(errno.EIO, "injected publication failure")

    monkeypatch.setattr(server, "_serve", record_exit)
    monkeypatch.setattr(server, "_publish", fail_publication)
    server.start()
    assert worker_exited.is_set()
    assert not server._listener
    assert list(cache.iterdir()) == []


class _StageSwappingSocket(socket.socket):
    """Renames the stage away at bind and leaves a symlink to `outside` in its place."""

    def __init__(self, cache, outside):
        super().__init__(socket.AF_UNIX, socket.SOCK_STREAM)
        self.cache = cache
        self.outside = outside
        self.stage = None

    def bind(self, address):
        stage, = [entry for entry in self.cache.iterdir() if entry.name.startswith(".storage.mount.")]
        stage.rename(self.cache / "moved stage")
        stage.symlink_to(self.outside, target_is_directory=True)
        self.stage = stage
        super().bind(address)


def test_swapped_stage_never_redirects_and_refuses(cache):
    # Before the fix, chmod after bind went through the stage's path and
    # followed this symlink to the outside file.
    with tempfile.TemporaryDirectory(prefix=".sc-outside-") as directory:
        outside = Path(directory)
        victim = outside / "socket"
        victim.write_text("not ours to chmod")
        victim.chmod(0o644)
        server = server_for(cache, lambda _: {})
        with _StageSwappingSocket(cache, outside) as listener:
            with pytest.raises(ControlError, match="stage directory changed"):
                server._publish(listener)
            swapped = listener.stage
        assert stat.S_IMODE(victim.stat().st_mode) == 0o644
        assert victim.read_text() == "not ours to chmod"
        assert not (cache / CONTROL_SOCKET_NAME).exists()
        assert not (cache / CONTROL_SOCKET_NAME).is_symlink()
        # Our own socket was removed from the stage that was moved away;
        # the stage name, now someone else's symlink, is left alone.
        assert list((cache / "moved stage").iterdir()) == []
        assert sorted(os.listdir(outside)) == ["socket"]
        assert swapped.is_symlink()


def substitute_on_mkdir(monkeypatch, cache, victim):
    """Wrap mkdir: right after the server's stage mkdir, a group writer
    renames our new directory away and puts `victim` at its name."""
    real_mkdir = os.mkdir
    replaced = []

    def mkdir_then_substitute(path, mode=0o777, *, dir_fd=None):
        real_mkdir(path, mode, dir_fd=dir_fd)
        if dir_fd is not None and not replaced:
            os.rename(path, "ours, moved away", src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
            os.rename(victim.name, path, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
            replaced.append(cache / path)

    monkeypatch.setattr("storage.control.os.mkdir", mkdir_then_substitute)
    return replaced


@pytest.mark.parametrize("victim_mode", [0o755, 0o700])
def test_directory_substituted_for_the_new_stage_is_left_alone(cache, monkeypatch, victim_mode):
    # A group writer replaces the stage name between mkdir and open with
    # another directory of ours. Before, the server chmodded it to 0700 and,
    # when the bind failed on its "socket", unlinked that file.
    cache.chmod(0o775)
    victim = cache / "victim"
    victim.mkdir()
    (victim / "socket").write_text("user data")
    victim.chmod(victim_mode)
    server = server_for(cache, lambda _: {})
    replaced = substitute_on_mkdir(monkeypatch, cache, victim)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        with pytest.raises(ControlError, match="not the one this mount created"):
            server._publish(listener)
    monkeypatch.undo()
    substituted, = replaced
    assert stat.S_IMODE(substituted.stat().st_mode) == victim_mode
    assert sorted(os.listdir(substituted)) == ["socket"]
    assert (substituted / "socket").read_text() == "user data"
    assert not (cache / CONTROL_SOCKET_NAME).exists()
    cache.chmod(0o700)


def test_empty_private_directory_substituted_for_the_stage(cache, monkeypatch):
    # Indistinguishable from the stage mkdir made: it is used, gets only our
    # nonce and socket. With a shared parent its provenance is ambiguous,
    # so only the entries the server made are removed.
    cache.chmod(0o775)
    victim = cache / "victim"
    victim.mkdir(0o700)
    victim.chmod(0o700)
    identity = victim.stat().st_ino
    server = server_for(cache, lambda _: {"published": True})
    replaced = substitute_on_mkdir(monkeypatch, cache, victim)
    server.start()
    monkeypatch.undo()
    try:
        assert request_control_payload(cache, b"status", 2) == {"published": True}
        substituted, = replaced
        assert substituted.stat().st_ino == identity
        assert list(substituted.iterdir()) == []
        # Our own mkdir'd directory, moved away, is not removed: it cannot be
        # told from anyone else's.
        assert (cache / "ours, moved away").is_dir()
        assert set(os.listdir(cache)) == {CONTROL_SOCKET_NAME, "ours, moved away", substituted.name}
    finally:
        server.stop()
        cache.chmod(0o700)


def socket_at(path):
    """A dead socket of ours at path: bound, then closed."""
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stale:
        stale.bind(str(path))


class FailingProbe(socket.socket):
    """A socket whose connect fails as configured, for the liveness probe."""

    error = None

    def connect(self, address):
        raise self.error


def record_renames(monkeypatch):
    """Record every renameat2 the control code makes, as (src, dst, flags)."""
    calls = []
    real = control_module.renameat2

    def recording(src, dst, flags):
        calls.append((os.path.basename(src), os.path.basename(dst), flags))
        return real(src, dst, flags)

    monkeypatch.setattr("storage.control.renameat2", recording)
    return calls


def no_renameat2(monkeypatch, error=errno.ENOSYS):
    def unsupported(src, dst, flags):
        raise OSError(error, "renameat2 flags unsupported here")

    monkeypatch.setattr("storage.control.renameat2", unsupported)


def moves_of_the_public_entry(calls):
    return [call for call in calls
            if call[2] == control_module.RENAME_EXCHANGE or call[0] == CONTROL_SOCKET_NAME]


@pytest.mark.parametrize("error", [BlockingIOError(11, "busy"), TimeoutError("timed out"),
                                   PermissionError(13, "denied"), InterruptedError(4, "interrupted")])
def test_a_probe_that_proves_nothing_leaves_the_entry(cache, monkeypatch, error):
    # Only a refused connect proves a socket dead; any other failure leaves
    # it at the name, unmoved, and publication refuses.
    path = cache / CONTROL_SOCKET_NAME
    socket_at(path)
    before = path.lstat()
    FailingProbe.error = error
    calls = record_renames(monkeypatch)
    server = server_for(cache, lambda _: {})
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        monkeypatch.setattr("storage.control.socket.socket", FailingProbe)
        with pytest.raises(ControlError, match="leaving it in place"):
            server._publish(listener)
    monkeypatch.undo()
    after = path.lstat()
    assert (after.st_dev, after.st_ino) == (before.st_dev, before.st_ino)
    assert moves_of_the_public_entry(calls) == []
    assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]


@pytest.mark.parametrize("renameat2_available", [True, False])
@pytest.mark.parametrize("kind", ["file", "directory", "symlink", "fifo"])
def test_an_entry_that_is_not_a_socket_is_never_moved(cache, monkeypatch, kind, renameat2_available):
    path = cache / CONTROL_SOCKET_NAME
    if kind == "file":
        path.write_text("unrelated")
    elif kind == "directory":
        path.mkdir()
        (path / "inside").write_text("unrelated")
    elif kind == "symlink":
        socket_at(cache / "dead.sock")
        path.symlink_to(cache / "dead.sock")
    else:
        os.mkfifo(path)
    before = path.lstat()
    if renameat2_available:
        calls = record_renames(monkeypatch)
    else:
        no_renameat2(monkeypatch)
        calls = []
    server = server_for(cache, lambda _: {})
    server.start()
    server.stop()
    monkeypatch.undo()
    assert server._cache_fd is None
    assert moves_of_the_public_entry(calls) == []
    after = path.lstat()
    assert (after.st_dev, after.st_ino) == (before.st_dev, before.st_ino)


def test_takeover_exchanges_a_proven_dead_socket(cache, monkeypatch):
    path = cache / CONTROL_SOCKET_NAME
    socket_at(path)
    calls = record_renames(monkeypatch)
    server = server_for(cache, lambda _: {"replaced": True})
    server.start()
    try:
        assert request_control_payload(cache, b"status", 2) == {"replaced": True}
        assert moves_of_the_public_entry(calls) == [
            (control_module.STAGE_SOCKET, CONTROL_SOCKET_NAME, control_module.RENAME_EXCHANGE),
        ]
        assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]
    finally:
        server.stop()


def test_the_pin_is_held_through_the_exchange_and_unlink(cache, monkeypatch):
    # The probed inode must stay pinned until the decision is final, or it
    # could be freed and its number reused by a swapped-in socket.
    path = cache / CONTROL_SOCKET_NAME
    socket_at(path)
    probe_fd = None
    observations = []
    real_probe = control_module._proven_dead
    real_rename = control_module.renameat2
    real_unlink = os.unlink

    def record_probe(fd):
        nonlocal probe_fd
        probe_fd = fd
        return real_probe(fd)

    def pins(entry):
        try:
            pinned = os.fstat(probe_fd)
        except OSError:
            return False
        return (pinned.st_dev, pinned.st_ino) == (entry.st_dev, entry.st_ino)

    def check_exchange(src, dst, flags):
        if flags == control_module.RENAME_EXCHANGE:
            observations.append(("exchange", pins(os.lstat(dst))))
        return real_rename(src, dst, flags)

    def check_unlink(name, *, dir_fd=None):
        if name == control_module.STAGE_SOCKET and dir_fd is not None:
            observations.append(("unlink", pins(os.stat(name, dir_fd=dir_fd))))
        return real_unlink(name, dir_fd=dir_fd)

    monkeypatch.setattr(control_module, "_proven_dead", record_probe)
    monkeypatch.setattr(control_module, "renameat2", check_exchange)
    monkeypatch.setattr(control_module.os, "unlink", check_unlink)
    server = server_for(cache, lambda _: {})
    server.start()
    try:
        assert observations == [("exchange", True), ("unlink", True)]
    finally:
        server.stop()


def test_a_failure_after_the_exchange_undoes_it(cache, monkeypatch):
    path = cache / CONTROL_SOCKET_NAME
    socket_at(path)
    before = path.lstat()
    calls = record_renames(monkeypatch)
    real_entry_at = control_module._entry_at

    failed = []

    def failing_entry_at(dir_fd, name):
        if (name == control_module.STAGE_SOCKET and not failed
                and any(call[2] == control_module.RENAME_EXCHANGE for call in calls)):
            failed.append(name)
            raise OSError(5, "simulated failure after the exchange")
        return real_entry_at(dir_fd, name)

    monkeypatch.setattr("storage.control._entry_at", failing_entry_at)
    server = server_for(cache, lambda _: {})
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        with pytest.raises(OSError, match="simulated failure after the exchange"):
            server._publish(listener)
    monkeypatch.undo()
    after = path.lstat()
    assert (after.st_dev, after.st_ino) == (before.st_dev, before.st_ino)
    assert [call[2] for call in moves_of_the_public_entry(calls)] == [control_module.RENAME_EXCHANGE] * 2
    assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]


def test_a_failure_after_the_move_at_stop_puts_our_socket_back(cache, monkeypatch):
    path = cache / CONTROL_SOCKET_NAME
    server = server_for(cache, lambda _: {})
    server.start()
    ours = path.lstat()
    real_entry_at = control_module._entry_at

    def failing_entry_at(dir_fd, name):
        if name == control_module.STAGE_MOVED:
            raise OSError(5, "simulated failure after the move")
        return real_entry_at(dir_fd, name)

    monkeypatch.setattr("storage.control._entry_at", failing_entry_at)
    server.stop()
    monkeypatch.undo()
    # Left as a dead socket of ours, which the next start takes over.
    after = path.lstat()
    assert (after.st_dev, after.st_ino) == (ours.st_dev, ours.st_ino)
    assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]
    server.start()
    try:
        assert request_control_payload(cache, b"status", 2) == {}
    finally:
        server.stop()
    assert os.listdir(cache) == []


@pytest.mark.parametrize("swapped_in", ["file", "socket", "directory"])
def test_a_swap_between_the_probe_and_the_exchange_is_undone(cache, monkeypatch, swapped_in):
    # The stale socket is proven dead, then a group writer replaces it
    # before the exchange. The exchange takes the newcomer, which is not the
    # pinned inode, so it is exchanged back and nothing is left in the stage.
    path = cache / CONTROL_SOCKET_NAME
    socket_at(path)
    real_probe = control_module._proven_dead
    replacement = None

    def probe_then_swap(entry_fd):
        nonlocal replacement
        result = real_probe(entry_fd)
        path.unlink()
        if swapped_in == "file":
            path.write_text("unrelated")
        elif swapped_in == "socket":
            socket_at(path)  # A dead socket of ours, but not the one probed.
        else:
            path.mkdir()
            (path / "fixture").write_text("unrelated")
        replacement = path.lstat()
        return result

    monkeypatch.setattr("storage.control._proven_dead", probe_then_swap)
    server = server_for(cache, lambda _: {})
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        with pytest.raises(ControlError, match="changed during startup"):
            server._publish(listener)
    restored = path.lstat()
    assert (restored.st_dev, restored.st_ino) == (replacement.st_dev, replacement.st_ino)
    if swapped_in == "file":
        assert path.read_text() == "unrelated"
    elif swapped_in == "directory":
        assert (path / "fixture").read_text() == "unrelated"
    assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]


def test_a_directory_swapped_in_after_probe_is_never_moved_without_exchange(cache, monkeypatch):
    path = cache / CONTROL_SOCKET_NAME
    socket_at(path)
    real_probe = control_module._proven_dead

    def probe_then_swap(fd):
        dead = real_probe(fd)
        path.unlink()
        path.mkdir()
        (path / "fixture").write_text("unrelated")
        return dead

    def unexpected_move(*args, **kwargs):
        pytest.fail("used a fallback to move the occupied control name")

    monkeypatch.setattr(control_module, "_proven_dead", probe_then_swap)
    monkeypatch.setattr(control_module.os, "rename", unexpected_move)
    no_renameat2(monkeypatch, errno.EINVAL)
    server = server_for(cache, lambda _: {})
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        with pytest.raises(ControlError, match="RENAME_EXCHANGE"):
            server._publish(listener)
    assert (path / "fixture").read_text() == "unrelated"
    assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]


def test_a_failed_exchange_rollback_keeps_the_displaced_entry(cache, monkeypatch, caplog):
    path = cache / CONTROL_SOCKET_NAME
    socket_at(path)
    real_rename = control_module.renameat2
    exchanges = 0

    def interfere(src, dst, flags):
        nonlocal exchanges
        if flags == control_module.RENAME_EXCHANGE:
            exchanges += 1
            if exchanges == 1:
                path.unlink()
                path.write_text("displaced entry")
            else:
                path.unlink()
        return real_rename(src, dst, flags)

    monkeypatch.setattr(control_module, "renameat2", interfere)
    server = server_for(cache, lambda _: {})
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        with pytest.raises(ControlError, match="changed during startup"):
            server._publish(listener)
    stage, = cache.iterdir()
    assert (stage / control_module.STAGE_SOCKET).read_text() == "displaced entry"
    assert "could not restore the displaced entry" in caplog.text
    assert str(stage) in caplog.text


@pytest.mark.parametrize("mode", [0o700, 0o770])
@pytest.mark.parametrize("name", [control_module.STAGE_SOCKET, control_module.STAGE_MOVED])
def test_restore_keeps_the_public_entry_and_reports_the_actual_stage(cache, caplog, mode, name):
    cache.chmod(mode)
    path = cache / CONTROL_SOCKET_NAME
    path.write_text("public")
    server = server_for(cache, lambda _: {})
    with control_module._locked_cache(cache) as root:
        stage = control_module._Stage(root)
        try:
            actual = cache / f"{control_module.STAGE_PREFIX}recovery"
            (cache / stage.name).rename(actual)
            displaced = actual / name
            displaced.write_text("displaced")
            server._restore(stage, root, name)
        finally:
            stage.remove()
    assert path.read_text() == "public"
    assert displaced.read_text() == "displaced"
    assert f"retained at {str(displaced)!r}" in caplog.text


_CRASH_AFTER_EXCHANGE = r"""
import logging, os, sys
from pathlib import Path
import storage.control as control
real = control.renameat2

def crash_after_exchange(src, dst, flags):
    result = real(src, dst, flags)
    if flags == control.RENAME_EXCHANGE:
        os._exit(17)
    return result

control.renameat2 = crash_after_exchange
server = control.ControlServer(Path(sys.argv[1]), lambda _: {}, logging.getLogger())
server.start()
os._exit(0)
"""


def test_a_crash_after_the_exchange_leaves_both_entries_and_allows_restart(cache):
    # Killed between the exchange and the check: our (now dead) socket is
    # public, the old dead socket is named in the stage; nothing is lost,
    # and the next start replaces the public one.
    path = cache / CONTROL_SOCKET_NAME
    socket_at(path)
    old = path.lstat()
    crashed = subprocess.run(
        [sys.executable, "-c", _CRASH_AFTER_EXCHANGE, str(cache)],
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}, timeout=60,
    )
    assert crashed.returncode == 17
    stage, = [entry for entry in cache.iterdir() if entry.name.startswith(".storage.mount.")]
    displaced = (stage / "socket").lstat()
    assert (displaced.st_dev, displaced.st_ino) == (old.st_dev, old.st_ino)
    assert stat.S_ISSOCK(path.lstat().st_mode) and path.lstat().st_ino != old.st_ino
    server = server_for(cache, lambda _: {"after crash": True})
    server.start()
    try:
        assert request_control_payload(cache, b"status", 2) == {"after crash": True}
        # No sweep: the leftover stage is the operator's (see README).
        assert stage.is_dir()
    finally:
        server.stop()


def test_shutdown_swap_after_the_final_check_keeps_the_new_entry(cache, monkeypatch):
    # Our socket is withdrawn, and a group writer puts an unrelated file at
    # the control name right after our entry was moved out and checked.
    path = cache / CONTROL_SOCKET_NAME
    server = server_for(cache, lambda _: {})
    server.start()
    real_entry_at = control_module._entry_at

    def entry_at_then_swap(dir_fd, name):
        found = real_entry_at(dir_fd, name)
        if name == control_module.STAGE_MOVED and not path.exists():
            path.write_text("unrelated")
        return found

    monkeypatch.setattr("storage.control._entry_at", entry_at_then_swap)
    server.stop()
    monkeypatch.undo()
    assert path.read_text() == "unrelated"
    assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]


@pytest.mark.parametrize("name_reclaimed", [False, True])
def test_shutdown_swap_after_pin_is_restored_without_clobbering(cache, monkeypatch, name_reclaimed):
    path = cache / CONTROL_SOCKET_NAME
    server = server_for(cache, lambda _: {})
    server.start()
    real_rename = control_module.renameat2

    def swap_before_move(src, dst, flags):
        if os.path.basename(src) == CONTROL_SOCKET_NAME:
            path.unlink()
            path.write_text("replacement")
            result = real_rename(src, dst, flags)
            if name_reclaimed:
                path.write_text("new entry")
            return result
        return real_rename(src, dst, flags)

    monkeypatch.setattr(control_module, "renameat2", swap_before_move)
    server.stop()
    if name_reclaimed:
        assert path.read_text() == "new entry"
        stage, = [entry for entry in cache.iterdir() if entry != path]
        assert (stage / control_module.STAGE_MOVED).read_text() == "replacement"
    else:
        assert path.read_text() == "replacement"
        assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]


def test_shutdown_leaves_an_entry_that_replaced_ours(cache, monkeypatch):
    path = cache / CONTROL_SOCKET_NAME
    server = server_for(cache, lambda _: {})
    server.start()
    path.unlink()
    socket_at(path)  # Someone else's stale socket, of our uid.
    replaced = path.lstat()
    calls = record_renames(monkeypatch)
    server.stop()
    assert moves_of_the_public_entry(calls) == []
    assert (path.lstat().st_dev, path.lstat().st_ino) == (replaced.st_dev, replaced.st_ino)
    assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]


@pytest.mark.parametrize("error", [errno.ENOSYS, errno.EINVAL])
def test_without_renameat2_only_a_free_name_is_published(cache, monkeypatch, caplog, error):
    no_renameat2(monkeypatch, error)
    path = cache / CONTROL_SOCKET_NAME
    server = server_for(cache, lambda _: {"linked": True})
    server.start()
    try:
        assert request_control_payload(cache, b"status", 2) == {"linked": True}
    finally:
        server.stop()
    # Withdrawal cannot move it, so our dead socket stays; harmless.
    assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]
    assert stat.S_ISSOCK(path.lstat().st_mode)
    left = path.lstat()
    # Occupied, even by a dead socket of ours: never replaced without renameat2.
    with caplog.at_level(logging.WARNING, logger=__name__):
        server.start()
    assert server._listener is None
    assert any("RENAME_EXCHANGE" in r.getMessage() for r in caplog.records)
    assert (path.lstat().st_dev, path.lstat().st_ino) == (left.st_dev, left.st_ino)
    monkeypatch.undo()
    server.start()  # With renameat2 back, the dead socket is replaced.
    try:
        assert request_control_payload(cache, b"status", 2) == {"linked": True}
    finally:
        server.stop()
    assert os.listdir(cache) == []


def test_publication_without_rename_noreplace_or_hard_links_refuses(cache, monkeypatch, caplog):
    def no_links(*args, **kwargs):
        raise OSError(errno.EPERM, "hard links not supported")

    no_renameat2(monkeypatch)
    monkeypatch.setattr("storage.control.os.link", no_links)
    server = server_for(cache, lambda _: {})
    with caplog.at_level(logging.WARNING, logger=__name__):
        server.start()
    monkeypatch.undo()
    assert server._listener is None
    assert any("neither no-replace rename nor hard links" in r.getMessage() for r in caplog.records)
    assert os.listdir(cache) == []


@pytest.mark.parametrize("mode", [0o707, 0o777, 0o702])
def test_refuses_a_world_writable_cache(cache, mode, caplog):
    cache.chmod(mode)
    server = server_for(cache, lambda _: {})
    with caplog.at_level(logging.WARNING, logger=__name__):
        server.start()
    try:
        assert server._listener is None
        assert any("writable by all users" in r.getMessage() for r in caplog.records)
        assert sorted(os.listdir(cache)) == []
    finally:
        server.stop()
        cache.chmod(0o700)


# 0775 and 0770 are what mkdir gives under umask 002 and 007.
@pytest.mark.parametrize("mode", [0o700, 0o755, 0o775, 0o770, 0o720, 0o1777, 0o1770])
def test_publishes_in_an_owned_cache_not_world_writable(cache, mode):
    cache.chmod(mode)
    shared_write = bool(mode & 0o022)
    server = server_for(cache, lambda _: {"mode": mode})
    server.start()
    try:
        assert request_control_payload(cache, b"status", 2) == {"mode": mode}
        stages = [entry for entry in cache.iterdir() if entry.name != CONTROL_SOCKET_NAME]
        assert len(stages) == int(shared_write)
        assert all(entry.is_dir() and list(entry.iterdir()) == [] for entry in stages)
    finally:
        server.stop()
        cache.chmod(0o700)
    stages = list(cache.iterdir())
    assert len(stages) == 2 * int(shared_write)
    assert all(entry.is_dir() and list(entry.iterdir()) == [] for entry in stages)


def test_refuses_a_cache_owned_by_another_user(cache, monkeypatch, caplog):
    real = os.geteuid()
    server = server_for(cache, lambda _: {})
    monkeypatch.setattr("storage.control.os.geteuid", lambda: real + 1)
    with caplog.at_level(logging.WARNING, logger=__name__):
        server.start()
    monkeypatch.undo()
    try:
        assert server._listener is None
        assert any("not owned by the mount's user" in r.getMessage() for r in caplog.records)
        assert sorted(os.listdir(cache)) == []
    finally:
        server.stop()
