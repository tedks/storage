"""Control socket framing, ownership, and CLI routing without a FUSE mount."""

import json
import logging
import os
import socket
import stat
import tempfile
import threading
from pathlib import Path

import pytest
from click.testing import CliRunner

from storage.cli import main
from storage.control import (
    CONTROL_SOCKET_NAME,
    MAX_REQUEST_BYTES,
    ControlError,
    ControlServer,
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


def test_profile_selection_and_atomic_set_request(cache):
    config = cache / "storage.toml"
    config.write_text(f"""
[mounts.example]
mountpoint = "/never-mounted"
cache = {json.dumps(str(cache))}
backends = ["/no-member-probe"]
""")
    requests = []

    def dispatch(frame):
        request = json.loads(frame)
        requests.append(request)
        return {"ok": True}

    server = server_for(cache, dispatch)
    server.start()
    try:
        runner = CliRunner()
        options = ["control", "--profile", "example", "--config", str(config), "--json-output"]
        result = runner.invoke(main, options)
        assert result.exit_code == 0, result.output
        assert json.loads(result.output) == {"ok": True}

        result = runner.invoke(main, options + ["set", "--max-streaming-sessions", "3",
                                                "--max-retained-streaming-sessions", "0"])
        assert result.exit_code == 0, result.output
        assert requests == [
            {"command": "status"},
            {"command": "streaming.set", "limits": {
                "max_streaming_sessions": 3, "max_retained_streaming_sessions": 0,
            }},
        ]
        assert runner.invoke(main, options + ["--cache", str(cache)]).exit_code == 2
        assert runner.invoke(main, options + ["set"]).exit_code == 2
        assert runner.invoke(main, ["control", "set", "--help"]).exit_code == 0
        assert len(requests) == 2
    finally:
        server.stop()


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


@pytest.mark.parametrize("mode", [0o770, 0o707, 0o777, 0o720])
def test_refuses_a_cache_others_can_rename_in(cache, mode, caplog):
    cache.chmod(mode)
    server = server_for(cache, lambda _: {})
    with caplog.at_level(logging.WARNING, logger=__name__):
        server.start()
    try:
        assert server._listener is None
        assert any("writable by other users" in r.getMessage() for r in caplog.records)
        assert sorted(os.listdir(cache)) == []
    finally:
        server.stop()
        cache.chmod(0o700)


@pytest.mark.parametrize("mode", [0o700, 0o755, 0o1777, 0o1770])
def test_publishes_in_a_private_or_sticky_cache(cache, mode):
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
