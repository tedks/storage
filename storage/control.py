"""Transport for the mount owner's cache-local control socket."""

import fcntl
import json
import logging
import math
import os
import socket
import stat
import struct
import tempfile
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path


CONTROL_SOCKET_NAME = ".storage.control.sock"
MAX_REQUEST_BYTES = 4096
# At most 2048 downloads, each with a Linux path up to 4096 bytes long.
# Even if JSON escapes every path byte as six ASCII bytes, the list fits.
MAX_RESPONSE_BYTES = 64 * 1024 * 1024
REQUEST_TIMEOUT = 2.0
_PEER_CREDENTIALS = struct.Struct("iII")


class ControlError(ValueError):
    """Invalid framing, a rejected command, or an invalid response."""


def _set_deadline(sock: socket.socket, deadline: float):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("control socket deadline expired")
    sock.settimeout(remaining)


def _read_line(sock: socket.socket, max_bytes: int, deadline: float) -> bytes | None:
    """Read one frame, ending at LF. A connection can carry one request."""
    data = bytearray()
    while True:
        _set_deadline(sock, deadline)
        chunk = sock.recv(min(65536, max_bytes + 1 - len(data)))
        if not chunk:
            if data:
                raise ControlError("Incomplete control socket frame")
            return None
        newline = chunk.find(b"\n")
        if newline != -1:
            data.extend(chunk[:newline])
            return bytes(data)
        data.extend(chunk)
        if len(data) > max_bytes:
            raise ControlError("Control socket frame is too large")


def _owner_socket(path: Path) -> os.stat_result:
    entry = path.lstat()
    if not stat.S_ISSOCK(entry.st_mode) or entry.st_uid != os.geteuid():
        raise ControlError(f"Expected a mount control socket owned by this user at {path}")
    return entry


def _check_peer(sock: socket.socket):
    # Linux SO_PEERCRED authenticates both ends independently of the socket
    # path and any filesystem ACLs or ownership changes after bind.
    credentials = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, _PEER_CREDENTIALS.size)
    _, uid, _ = _PEER_CREDENTIALS.unpack(credentials)
    if uid != os.geteuid():
        raise ControlError("Control socket peer is not the mount owner")


def request_control_payload(cache_root: Path, request: bytes, timeout: float) -> dict:
    """Send one request and read a bounded JSON response."""
    path = cache_root / CONTROL_SOCKET_NAME
    if not math.isfinite(timeout) or timeout <= 0:
        raise ControlError("Control socket timeout must be a positive finite number")
    if len(request) > MAX_REQUEST_BYTES or b"\n" in request:
        raise ControlError("Invalid outgoing control socket frame")
    try:
        _owner_socket(path)
        deadline = time.monotonic() + timeout
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            _set_deadline(sock, deadline)
            sock.connect(str(path))
            _check_peer(sock)
            _set_deadline(sock, deadline)
            sock.sendall(request + b"\n")
            response = _read_line(sock, MAX_RESPONSE_BYTES, deadline)
    except FileNotFoundError as exc:
        raise ControlError(f"No running mount control socket at {path}") from exc
    except (OSError, OverflowError) as exc:
        raise ControlError(f"Failed to query control socket {path}: {exc}") from exc

    if response is None:
        raise ControlError(f"No response from control socket {path}")
    try:
        payload = json.loads(response.decode("utf-8"))
    except (ValueError, RecursionError) as exc:
        raise ControlError(f"Invalid response from control socket {path}") from exc
    if not isinstance(payload, dict):
        raise ControlError(f"Expected an object from control socket {path}")
    if payload.get("error"):
        raise ControlError(str(payload["error"]))
    return payload


@contextmanager
def _locked_cache(cache_root: Path):
    # Exclude concurrent socket publication and cleanup by cache, including
    # two mountpoints configured with the same cache. The lock does not wait:
    # whoever holds it wins, and the other side fails with BlockingIOError
    # (start logs a warning and runs without a socket; stop leaves the entry,
    # which a later start takes over once its listener is gone). The cache
    # directory itself is the lock, so no new lockfile is needed.
    fd = os.open(cache_root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


def _identity(entry: os.stat_result) -> tuple[int, int]:
    return entry.st_dev, entry.st_ino


class ControlServer:
    def __init__(self, cache_root: Path, dispatch: Callable[[str], dict], logger: logging.Logger):
        self.path = cache_root.resolve() / CONTROL_SOCKET_NAME
        self._dispatch = dispatch
        self._logger = logger
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread = None
        self._listener = None
        self._owned_entry = None

    def _stale_entry(self) -> tuple[int, int] | None:
        try:
            entry = _owner_socket(self.path)
        except FileNotFoundError:
            return None
        # Also protect a live listener started by a version that didn't take
        # the cache lock. Only a refused connect establishes a stale socket.
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
            probe.settimeout(0.25)
            try:
                probe.connect(str(self.path))
            except ConnectionRefusedError:
                return _identity(entry)
        raise ControlError("a mount control socket is already listening")

    def _publish(self, listener: socket.socket) -> tuple[int, int]:
        if len(os.fsencode(self.path)) > 107:
            raise ControlError("control socket path exceeds the 107-byte Unix socket limit")
        with _locked_cache(self.path.parent):
            stale = self._stale_entry()
            # The stage is private even under umask 0. Bind through its fd
            # so the longer stage path doesn't reduce the Unix address limit.
            with tempfile.TemporaryDirectory(prefix=".storage.mount.", dir=self.path.parent) as stage:
                staged = Path(stage) / "socket"
                fd = os.open(stage, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    listener.bind(f"/proc/self/fd/{fd}/socket")
                finally:
                    os.close(fd)
                staged.chmod(0o600)
                listener.listen(8)
                listener.settimeout(0.25)
                owned = _identity(staged.lstat())
                if stale is None:
                    # Keep arbitrary existing entries intact on a racing bind.
                    os.link(staged, self.path)
                else:
                    if _identity(_owner_socket(self.path)) != stale:
                        raise ControlError("control socket changed during startup")
                    os.replace(staged, self.path)
                return owned

    def start(self):
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            listener = None
            try:
                listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self._owned_entry = self._publish(listener)
            except (OSError, ControlError) as exc:
                if listener is not None:
                    listener.close()
                self._logger.warning("failed to start control socket at %s: %s", self.path, exc)
                return
            self._stop.clear()
            self._listener = listener
            self._thread = threading.Thread(
                target=self._serve, args=(listener,), daemon=True, name="storage-control-socket",
            )
            self._thread.start()

    def stop(self):
        with self._lock:
            if self._listener is None:
                return
            self._stop.set()
            try:
                with _locked_cache(self.path.parent):
                    if _identity(_owner_socket(self.path)) == self._owned_entry:
                        self.path.unlink()
            except (OSError, ControlError):
                # A replacement belongs to its publisher. A stale socket
                # left by an interrupted cleanup can be taken over at start.
                pass
            self._listener.close()
            self._listener = None
            self._thread.join(timeout=REQUEST_TIMEOUT + 0.25)

    def _serve(self, listener: socket.socket):
        while not self._stop.is_set():
            try:
                conn, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with conn:
                self._serve_connection(conn)

    def _serve_connection(self, conn: socket.socket):
        try:
            _check_peer(conn)
        except (ControlError, OSError):
            return
        # Transport errors on the request are the client's problem and end
        # the connection quietly; anything the handler raises other than a
        # ControlError, OSError included, is a server bug and is logged.
        try:
            frame = _read_line(conn, MAX_REQUEST_BYTES, time.monotonic() + REQUEST_TIMEOUT)
        except TimeoutError:
            # The deadline check and a recv timeout word this differently.
            response = {"error": "Control request timed out"}
        except ControlError as exc:
            response = {"error": str(exc)}
        except OSError:
            return
        else:
            if frame is None or self._stop.is_set():
                return
            try:
                request = frame.decode("utf-8")
            except UnicodeDecodeError:
                response = {"error": "Control request must be UTF-8"}
            else:
                try:
                    response = self._dispatch(request)
                except ControlError as exc:
                    response = {"error": str(exc)}
                except Exception:
                    self._logger.exception("mount control request failed")
                    response = {"error": "Mount control request failed"}
        try:
            encoded = json.dumps(response, sort_keys=True).encode("ascii")
        except (TypeError, ValueError, RecursionError):
            self._logger.exception("could not encode mount control response")
            encoded = b'{"error":"Could not encode control response"}'
        if len(encoded) > MAX_RESPONSE_BYTES:
            encoded = b'{"error":"Control response is too large"}'
        try:
            conn.settimeout(REQUEST_TIMEOUT)
            conn.sendall(encoded + b"\n")
        except OSError:
            pass
