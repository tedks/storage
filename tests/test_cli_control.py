"""Control socket framing, ownership, and CLI routing without a FUSE mount."""

import errno
import fcntl
import json
import logging
import os
import socket
import stat
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
    # Keep the path under the Unix socket address limit with long Bazel roots.
    with tempfile.TemporaryDirectory(prefix="storage-control-", dir="/tmp") as directory:
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
    config = write_profile(cache, "/mnt/profile-b")
    server, requests = recording_server(cache, "/mnt/profile-a")
    server.start()
    try:
        runner = CliRunner()
        options = ["control", "--profile", "example", "--config", str(config)]
        for command in (["set", "--max-download-workers", "2"], ["cancel", "0" * 32], ["status"]):
            requests.clear()
            result = runner.invoke(main, options + command)
            assert result.exit_code == 1, result.output
            assert "belongs to the mount at '/mnt/profile-a'" in result.output
            assert "mountpoint '/mnt/profile-b'" in result.output
            # Only the identity check (or the status itself) was sent.
            assert requests == [{"command": "status", "mountpoint": "/mnt/profile-b",
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
    # 16 components of 63 four-byte characters: 4048 bytes on disk, each
    # character escaped to a twelve-byte surrogate pair in the request.
    mountpoint = "/" + "/".join(["\U00020000" * 63] * 16)
    assert len(os.fsencode(mountpoint)) == 4048
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
    config = write_profile(cache, "/mnt/profile-b")
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
    assert canonical_mountpoint("./x/../mount") == str(real / "mount")
    # The final component is never resolved, so a hung mount root is not touched.
    assert canonical_mountpoint(str(tmp_path / "alias" / "final")) == str(real / "final")
    assert canonical_mountpoint("/") == "/"
    assert canonical_mountpoint("//") == "/"


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
    with tempfile.TemporaryDirectory(prefix="storage-outside-", dir="/tmp") as directory:
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
    # nonce and socket, and those are all that is removed from it (then the
    # empty directory itself, which its mover could rmdir anyway).
    cache.chmod(0o775)
    victim = cache / "victim"
    victim.mkdir(0o700)
    victim.chmod(0o700)
    server = server_for(cache, lambda _: {"published": True})
    replaced = substitute_on_mkdir(monkeypatch, cache, victim)
    server.start()
    monkeypatch.undo()
    try:
        assert request_control_payload(cache, b"status", 2) == {"published": True}
        substituted, = replaced
        assert not substituted.exists()
        # Our own mkdir'd directory, moved away, is not removed: it cannot be
        # told from anyone else's.
        assert (cache / "ours, moved away").is_dir()
        assert sorted(os.listdir(cache)) == sorted([CONTROL_SOCKET_NAME, "ours, moved away"])
    finally:
        server.stop()
        cache.chmod(0o700)


def write_stage(cache, name, *, nonce=True, socket_state=None, extra=None, mode=0o700):
    stage = cache / name
    stage.mkdir()
    stage.chmod(mode)
    if nonce:
        (stage / "nonce").write_text("0" * 32)
    if socket_state is not None:
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(stage / "socket"))
        if socket_state == "live":
            listener.listen(1)
            return stage, listener
        listener.close()
    if extra is not None:
        (stage / extra).write_text("user data")
    return stage, None


def test_publication_sweeps_only_stages_a_crash_left(cache):
    swept = [
        write_stage(cache, ".storage.mount.0000000000000001", socket_state="dead")[0],
        write_stage(cache, ".storage.mount.0000000000000002")[0],
        write_stage(cache, ".storage.mount.0000000000000003", nonce=False, socket_state="dead")[0],
    ]
    kept = [
        write_stage(cache, ".storage.mount.0000000000000004", extra="moved")[0],
        write_stage(cache, ".storage.mount.0000000000000005", nonce=False)[0],  # empty
        write_stage(cache, ".storage.mount.0000000000000006", mode=0o750)[0],
    ]
    live_stage, live = write_stage(cache, ".storage.mount.0000000000000007", socket_state="live")
    kept.append(live_stage)
    lock = cache / ".storage.mount.0123456789ab.lock"
    lock.write_text("4242")
    link = cache / ".storage.mount.0000000000000008"
    link.symlink_to(kept[0])
    try:
        server = server_for(cache, lambda _: {})
        server.start()
        try:
            assert server._listener is not None
            for stage in swept:
                assert not stage.exists(), stage
            for stage in kept:
                assert stage.is_dir(), stage
            assert (kept[0] / "moved").read_text() == "user data"
            assert lock.read_text() == "4242" and link.is_symlink()
        finally:
            server.stop()
    finally:
        live.close()


def socket_at(path):
    """A dead socket of ours at path: bound, then closed."""
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stale:
        stale.bind(str(path))


def test_startup_swap_after_the_final_check_keeps_the_new_entry(cache, monkeypatch):
    # A stale socket is replaced, but a group writer puts an unrelated file at
    # the control name right after the stale socket was checked (moved into
    # our stage and found dead). Publication must not clobber it.
    path = cache / CONTROL_SOCKET_NAME
    socket_at(path)
    real_refused = control_module._refused_socket

    def refused_then_swap(dir_fd, name, expected):
        result = real_refused(dir_fd, name, expected)
        if not path.exists():
            path.write_text("unrelated")
        return result

    monkeypatch.setattr("storage.control._refused_socket", refused_then_swap)
    server = server_for(cache, lambda _: {})
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        with pytest.raises(ControlError, match="changed during startup"):
            server._publish(listener)
    assert path.read_text() == "unrelated"
    assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]


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


def test_shutdown_puts_back_an_entry_that_replaced_ours(cache):
    path = cache / CONTROL_SOCKET_NAME
    server = server_for(cache, lambda _: {})
    server.start()
    path.unlink()
    socket_at(path)  # Someone else's stale socket, of our uid.
    replaced = path.lstat()
    server.stop()
    assert (path.lstat().st_dev, path.lstat().st_ino) == (replaced.st_dev, replaced.st_ino)
    assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]


def test_publication_without_no_replace_rename_falls_back_to_link(cache, monkeypatch):
    def unsupported(src, dst):
        raise OSError(errno.EINVAL, "renameat2 flags unsupported here")

    monkeypatch.setattr("storage.control.rename_noreplace", unsupported)
    path = cache / CONTROL_SOCKET_NAME
    socket_at(path)
    server = server_for(cache, lambda _: {"linked": True})
    server.start()
    try:
        assert request_control_payload(cache, b"status", 2) == {"linked": True}
        assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]
    finally:
        server.stop()
    assert os.listdir(cache) == []


def test_publication_without_rename_noreplace_or_hard_links_refuses(cache, monkeypatch, caplog):
    def unsupported(src, dst):
        raise OSError(errno.ENOSYS, "renameat2 unavailable")

    def no_links(*args, **kwargs):
        raise OSError(errno.EPERM, "hard links not supported")

    monkeypatch.setattr("storage.control.rename_noreplace", unsupported)
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
    server = server_for(cache, lambda _: {"mode": mode})
    server.start()
    try:
        assert request_control_payload(cache, b"status", 2) == {"mode": mode}
        # Only the socket: the stage directory is gone.
        assert sorted(os.listdir(cache)) == [CONTROL_SOCKET_NAME]
    finally:
        server.stop()
        cache.chmod(0o700)
    assert sorted(os.listdir(cache)) == []


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
