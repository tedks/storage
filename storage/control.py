"""Transport for the mount owner's cache-local control socket."""

import fcntl
import json
import logging
import math
import os
import secrets
import socket
import stat
import struct
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path


CONTROL_SOCKET_NAME = ".storage.control.sock"
# Requests are ASCII JSON, and a request may name a mountpoint: a Linux
# path up to 4096 bytes, which JSON can escape to six bytes per byte (a
# control character or an undecodable byte as \u00XX or \udcXX; a
# four-byte character is twelve bytes as a surrogate pair, three per
# byte). 24 KiB for the path leaves 8 KiB for the command and arguments.
MAX_REQUEST_BYTES = 32 * 1024
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


def _require_owner_socket(entry: os.stat_result, path: Path) -> os.stat_result:
    if not stat.S_ISSOCK(entry.st_mode) or entry.st_uid != os.geteuid():
        raise ControlError(f"Expected a mount control socket owned by this user at {path}")
    return entry


def _owner_socket(path: Path) -> os.stat_result:
    return _require_owner_socket(path.lstat(), path)


def canonical_mountpoint(mountpoint: str) -> str:
    """The absolute mountpoint a mount reports and a profile is matched against.

    The parent directory is resolved the way the kernel resolves it:
    symlinks first, then each `..` against what they point to, so nothing
    is normalized lexically (abspath would make `alias/../m` the sibling of
    `alias` even when `alias` links elsewhere). The final component is
    never resolved, so this never stats the mount root itself (a hung mount
    would block the CLI). A final `.` or `..` could be placed only by
    resolving through it, so such a mountpoint is refused. The mount and
    the CLI apply the same rule, so a relative mountpoint matches only when
    both resolve it from the same working directory, as for the cache path.
    """
    path = os.fsdecode(mountpoint)
    if not os.path.isabs(path):
        path = os.path.join(os.getcwd(), path)
    path = path.rstrip("/")
    if not path:
        return "/"
    parent, name = path.rsplit("/", 1)
    if name in (".", ".."):
        raise ControlError(
            f"Mountpoint {os.fsdecode(mountpoint)!r} ends in {name!r}; give it without a final '.' or '..'"
        )
    return os.path.join(os.path.realpath(parent or "/"), name)


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
    """Yield a locked fd for the cache directory.

    Exclude concurrent socket publication and cleanup by cache, including
    two mountpoints configured with the same cache. The lock does not wait:
    whoever holds it wins, and the other side fails with BlockingIOError
    (start logs a warning and runs without a socket; stop leaves the entry,
    which a later start takes over once its listener is gone). The cache
    directory itself is the lock, so no new lockfile is needed. Callers work
    relative to the fd, so renaming the cache path afterwards cannot move
    them to another directory.
    """
    fd = os.open(cache_root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield fd
    finally:
        os.close(fd)


def _require_private_cache(root_fd: int):
    """Refuse a cache directory we do not own or that anyone may write.

    The rule: the directory is owned by the mount's effective uid, and is
    not writable by other users unless it is sticky. Group write is
    allowed, because mkdir under umask 002 (the user-private-group default
    on Debian and Ubuntu) gives it. That is safe here: every step of
    publication runs relative to this directory's fd and the stage fd and
    checks identities, so a group member who renames or creates entries can
    make publication fail but never redirect it, and clients accept only a
    socket owned by, and a peer running as, the mount's user. World write
    without the sticky bit is still refused, as a cache nobody should run.
    Ancestors are not checked: renaming one moves the whole cache, and the
    CLI then finds no socket rather than another user's.
    """
    entry = os.fstat(root_fd)
    if entry.st_uid != os.geteuid():
        raise ControlError("cache directory is not owned by the mount's user")
    mode = stat.S_IMODE(entry.st_mode)
    if mode & stat.S_IWOTH and not mode & stat.S_ISVTX:
        raise ControlError("cache directory is writable by all users and not sticky")


def _identity(entry: os.stat_result) -> tuple[int, int]:
    return entry.st_dev, entry.st_ino


def _entry_at(dir_fd: int, name: str) -> os.stat_result:
    return os.stat(name, dir_fd=dir_fd, follow_symlinks=False)


def _require_new_stage(entry: os.stat_result, stage_fd: int):
    # A group writer can replace the name between mkdir and open, so only
    # a directory that looks exactly like the one mkdir just made is used:
    # ours, nothing for group or other, owner rwx, and empty. Nothing
    # touches it before this passes, and it is never chmodded.
    mode = stat.S_IMODE(entry.st_mode)
    if (not stat.S_ISDIR(entry.st_mode) or entry.st_uid != os.geteuid()
            or mode & 0o077 or mode & 0o700 != 0o700 or os.listdir(stage_fd)):
        raise ControlError("control socket stage directory is not the one this mount created")


def _remove_stage(root_fd: int, name: str, stage_fd: int, stage: tuple[int, int],
                  bound: tuple[int, int] | None):
    # Best effort: only the socket this attempt bound, then the stage only
    # while its name in the cache still names the directory we verified.
    if bound is not None:
        try:
            if _identity(_entry_at(stage_fd, "socket")) == bound:
                os.unlink("socket", dir_fd=stage_fd)
        except OSError:
            pass
    try:
        if _identity(_entry_at(root_fd, name)) == stage:
            os.rmdir(name, dir_fd=root_fd)
    except OSError:
        pass


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
        # (st_dev, st_ino) of the cache directory the socket was published
        # in, from its locked fd. A socket moved into another cache still
        # reports this one, so clients can tell. None until published.
        self.cache_identity = None

    def _stale_entry(self, root_fd: int) -> tuple[int, int] | None:
        try:
            entry = _require_owner_socket(_entry_at(root_fd, CONTROL_SOCKET_NAME), self.path)
        except FileNotFoundError:
            return None
        # Also protect a live listener started by a version that didn't take
        # the cache lock. Only a refused connect establishes a stale socket.
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
            probe.settimeout(0.25)
            try:
                probe.connect(f"/proc/self/fd/{root_fd}/{CONTROL_SOCKET_NAME}")
            except ConnectionRefusedError:
                return _identity(entry)
        raise ControlError("a mount control socket is already listening")

    def _publish(self, listener: socket.socket) -> tuple[tuple[int, int], tuple[int, int]]:
        """Publish the listener; return the socket's and the cache's identities."""
        if len(os.fsencode(self.path)) > 107:
            raise ControlError("control socket path exceeds the 107-byte Unix socket limit")
        with _locked_cache(self.path.parent) as root:
            _require_private_cache(root)
            cache = _identity(os.fstat(root))
            stale = self._stale_entry(root)
            # Every step below goes through the cache and stage fds, never a
            # path through the cache, so a renamed or swapped stage cannot
            # redirect chmod, stat, link or rename. Binding through the stage
            # fd keeps the longer stage path out of the Unix address limit.
            name = f".storage.mount.{secrets.token_hex(8)}"
            # 0700 at most under any umask; a umask that strips owner bits
            # fails the check below instead of being chmodded around.
            os.mkdir(name, 0o700, dir_fd=root)
            # If the open fails, the name may no longer be ours: leave it.
            stage_fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root)
            stage = bound = None
            try:
                entry = os.fstat(stage_fd)
                _require_new_stage(entry, stage_fd)
                stage = _identity(entry)
                staged = f"/proc/self/fd/{stage_fd}/socket"
                listener.bind(staged)
                # Nobody else can write the verified stage, so the entry is
                # the socket just bound; cleanup removes only that.
                bound = _identity(_require_owner_socket(_entry_at(stage_fd, "socket"), self.path))
                # Follows a final symlink, but only we can write the stage.
                os.chmod(staged, 0o600)
                listener.listen(8)
                listener.settimeout(0.25)
                # Our socket is still safe in the pinned stage, but a stage
                # name that no longer names it means someone is tampering
                # with the cache: refuse rather than publish beside them.
                if _identity(_entry_at(root, name)) != stage:
                    raise ControlError("control socket stage directory changed during startup")
                if stale is None:
                    # Keep arbitrary existing entries intact on a racing bind.
                    os.link("socket", CONTROL_SOCKET_NAME, src_dir_fd=stage_fd,
                            dst_dir_fd=root, follow_symlinks=False)
                else:
                    current = _require_owner_socket(_entry_at(root, CONTROL_SOCKET_NAME), self.path)
                    if _identity(current) != stale:
                        raise ControlError("control socket changed during startup")
                    os.rename("socket", CONTROL_SOCKET_NAME, src_dir_fd=stage_fd, dst_dir_fd=root)
                return bound, cache
            finally:
                if stage is not None:
                    _remove_stage(root, name, stage_fd, stage, bound)
                os.close(stage_fd)

    def start(self):
        with self._lock:
            if self._listener is not None:
                return
            listener = None
            try:
                listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self._owned_entry, self.cache_identity = self._publish(listener)
            except (OSError, ControlError) as exc:
                if listener is not None:
                    listener.close()
                self._logger.warning("failed to start control socket at %s: %s", self.path, exc)
                return
            # Each listener gets its own stop event, so a thread that outlived
            # an earlier stop() still sees its own stop and never serves again.
            self._stop = threading.Event()
            self._listener = listener
            self._thread = threading.Thread(
                target=self._serve, args=(listener, self._stop), daemon=True,
                name="storage-control-socket",
            )
            self._thread.start()

    def stop(self):
        with self._lock:
            if self._listener is None:
                return
            self._stop.set()
            try:
                with _locked_cache(self.path.parent) as root:
                    entry = _require_owner_socket(_entry_at(root, CONTROL_SOCKET_NAME), self.path)
                    if _identity(entry) == self._owned_entry:
                        os.unlink(CONTROL_SOCKET_NAME, dir_fd=root)
            except (OSError, ControlError):
                # A replacement belongs to its publisher. A stale socket
                # left by an interrupted cleanup can be taken over at start.
                pass
            self._listener.close()
            self._listener = None
            timeout = REQUEST_TIMEOUT + 0.25
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                # Its listener is closed and its stop event set, so it exits
                # after the request it is in; only that request is answered.
                self._logger.warning(
                    "control socket thread still in a request after %.2fs; it exits when that returns",
                    timeout,
                )
            self._thread = None

    def _serve(self, listener: socket.socket, stop: threading.Event):
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with conn:
                self._serve_connection(conn, stop)

    def _serve_connection(self, conn: socket.socket, stop: threading.Event):
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
            if frame is None or stop.is_set():
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
