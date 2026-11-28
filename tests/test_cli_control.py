"""Control socket framing, ownership, and CLI routing without a FUSE mount."""

import json
import logging
import socket
import stat
import tempfile
import threading
from pathlib import Path
from time import monotonic

import pytest
from click.testing import CliRunner

from storage.cli import main
from storage.control import (
    CONTROL_SOCKET_NAME,
    MAX_REQUEST_BYTES,
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
                started = monotonic()
                assert "timed out" in read_response(conn)["error"].lower()
                assert monotonic() - started < 1.0
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

        result = runner.invoke(main, options + ["set", "--max-download-workers", "auto"])
        assert result.exit_code == 0, result.output
        assert requests == [
            {"command": "status"},
            {"command": "streaming.set", "limits": {
                "max_download_workers": None,
            }},
        ]
        assert runner.invoke(main, options + ["--cache", str(cache)]).exit_code == 2
        assert runner.invoke(main, options + ["set"]).exit_code == 2
        assert runner.invoke(main, ["control", "set", "--help"]).exit_code == 0
        assert len(requests) == 2
    finally:
        server.stop()
