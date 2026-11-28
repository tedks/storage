"""Transport for the mount owner's cache-local control socket."""

import errno
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
from contextlib import contextmanager, suppress
from pathlib import Path

from .backends.local import NOREPLACE_UNSUPPORTED, RENAME_EXCHANGE, RENAME_NOREPLACE, renameat2


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
_ACCEPT_RETRY_DELAY = 0.25
_RETRY_ACCEPT_ERRORS = {
    errno.EAGAIN, errno.EWOULDBLOCK, errno.EINTR, errno.ECONNABORTED,
    errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM, errno.ETIMEDOUT,
}
# The reply to a request that arrives while the mount stops its socket.
STOPPING_MESSAGE = "This mount is stopping; its control socket no longer serves requests"
_PEER_CREDENTIALS = struct.Struct("iII")


class ControlError(ValueError):
    """Invalid framing, a rejected command, or an invalid response."""


def _set_deadline(sock: socket.socket, deadline: float):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("control socket deadline expired")
    sock.settimeout(remaining)


# The request older clients sent without a newline, ended by EOF instead.
LEGACY_STATS_REQUEST = b"streaming-stats"


def _read_line(sock: socket.socket, max_bytes: int, deadline: float,
               unterminated: bytes | None = None) -> bytes | None:
    """Read one frame, ending at LF. A connection can carry one request.

    A frame ended by EOF is incomplete unless it is exactly `unterminated`.
    """
    data = bytearray()
    while True:
        _set_deadline(sock, deadline)
        chunk = sock.recv(min(65536, max_bytes + 1 - len(data)))
        if not chunk:
            if data and unterminated is not None and data == unterminated:
                return bytes(data)
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


# The kernel's own limit on symlinks followed in one lookup.
_MAX_SYMLINKS = 40


def canonical_mountpoint(mountpoint: str) -> str:
    """The absolute mountpoint a mount reports and a profile is matched against.

    The directory the mount is on, as the kernel reaches it: the parent is
    resolved symlinks first, then each `..` against what they point to, so
    nothing is normalized lexically (abspath would make `alias/../m` the
    sibling of `alias` even when `alias` links elsewhere). A final component
    that is a symlink is followed too, with readlink, so a profile whose
    mountpoint link is retargeted no longer matches the mount made through
    its old target. The final directory itself is never stat'ed: readlink
    on a mount root fails with EINVAL without asking the filesystem, so a
    hung mount does not block the CLI. A final `.` or `..` could be placed
    only by resolving through it, so such a mountpoint is refused, and so
    is one whose parent directories do not all exist. The
    mount and the CLI apply the same rule, so a relative mountpoint matches
    only when both resolve it from the same working directory, as for the
    cache path.
    """
    try:
        return _canonical_mountpoint(mountpoint)
    except ControlError:
        raise
    except (OSError, ValueError) as exc:
        # A vanished working directory, an embedded NUL, EACCES on a link:
        # the callers (the CLI, the mount's control setup) handle only
        # ControlError.
        raise ControlError(f"Cannot resolve mountpoint {os.fsdecode(mountpoint)!r}: {exc}") from exc


def _canonical_mountpoint(mountpoint: str) -> str:
    original = os.fsdecode(mountpoint)
    path = original or os.getcwd()
    for _ in range(_MAX_SYMLINKS + 1):
        path = path.rstrip("/")
        if not path:
            return "/"
        parent, name = os.path.split(path)
        if name in (".", ".."):
            raise ControlError(
                f"Mountpoint {original!r} ends in {name!r}; give it without a final '.' or '..'"
            )
        parent = parent or "."
        os.path.realpath(parent, strict=True)
        # The kernel also rejects a non-directory before '..'. Use its
        # directory fd for both the canonical path and the final readlink.
        parent_fd = os.open(parent, os.O_PATH | os.O_DIRECTORY)
        try:
            canonical_parent = os.readlink(f"/proc/self/fd/{parent_fd}")
            try:
                target = os.readlink(name, dir_fd=parent_fd)
            except OSError as exc:
                if exc.errno in (errno.EINVAL, errno.ENOENT):
                    return os.path.join(canonical_parent, name)
                raise
        finally:
            os.close(parent_fd)
        path = os.path.join(canonical_parent, target)
    raise ControlError(f"Mountpoint {original!r}: too many levels of symbolic links")


def _check_peer(sock: socket.socket):
    # Linux SO_PEERCRED authenticates both ends independently of the socket
    # path and any filesystem ACLs or ownership changes after bind.
    credentials = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, _PEER_CREDENTIALS.size)
    _, uid, _ = _PEER_CREDENTIALS.unpack(credentials)
    if uid != os.geteuid():
        raise ControlError("Control socket peer is not the mount owner")


def open_cache_directory(cache_root: Path) -> int:
    """Open a cache directory for request_control_payload(cache_fd=...).

    O_PATH: fstat, the relative socket lookup and the /proc/self/fd connect
    need only search permission on the directory, as connecting by path
    always did; a read-only open would also need read permission.
    """
    return os.open(cache_root, os.O_PATH | os.O_DIRECTORY)


def request_control_payload(cache_root: Path, request: bytes, timeout: float,
                            cache_fd: int | None = None) -> dict:
    """Send one request and read a bounded JSON response.

    The socket entry is opened (O_PATH) through an fd of the cache
    directory (cache_fd, or one opened here), then checked and connected
    through that one fd, so a caller that identified the cache by fstat on
    the same directory fd talks to the socket in that directory, even if
    the path is renamed meanwhile. cache_root names it in messages. The
    /proc/self/fd path is short, so the Unix address limit applies only to
    clients that connect by path, which the server's 107-byte check keeps
    working.
    """
    path = cache_root / CONTROL_SOCKET_NAME
    if not math.isfinite(timeout) or timeout <= 0:
        raise ControlError("Control socket timeout must be a positive finite number")
    if len(request) > MAX_REQUEST_BYTES or b"\n" in request:
        raise ControlError("Invalid outgoing control socket frame")
    opened = None
    try:
        if cache_fd is None:
            cache_fd = opened = open_cache_directory(cache_root)
        # Pin the entry, then check and connect to that same inode, so a
        # group writer cannot swap the name between the check and connect.
        # O_NOFOLLOW opens a symlink itself, which the check then refuses.
        socket_fd = os.open(CONTROL_SOCKET_NAME, os.O_PATH | os.O_NOFOLLOW, dir_fd=cache_fd)
        try:
            _require_owner_socket(os.fstat(socket_fd), path)
            deadline = time.monotonic() + timeout
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                _set_deadline(sock, deadline)
                sock.connect(f"/proc/self/fd/{socket_fd}")
                _check_peer(sock)
                _set_deadline(sock, deadline)
                sock.sendall(request + b"\n")
                response = _read_line(sock, MAX_RESPONSE_BYTES, deadline)
        finally:
            os.close(socket_fd)
    except FileNotFoundError as exc:
        raise ControlError(f"No running mount control socket at {path}") from exc
    except (OSError, OverflowError) as exc:
        raise ControlError(f"Failed to query control socket {path}: {exc}") from exc
    finally:
        if opened is not None:
            os.close(opened)

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
        with _locked_fd(fd):
            yield fd
    finally:
        os.close(fd)


@contextmanager
def _locked_fd(fd: int):
    # The same non-blocking cache lock, on an fd already open.
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        yield fd
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)


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


# Publication and cleanup use a private directory of the cache, named
# STAGE_PREFIX plus a 32-hex-digit nonce. A stage holds at most
# STAGE_NONCE (a file made O_EXCL holding
# the nonce), STAGE_SOCKET (the socket being published or the entry
# displaced at takeover), and STAGE_MOVED (the entry moved at stop).
# Cache walkers skip the prefix. README describes manual crash recovery.
STAGE_PREFIX = ".storage.mount."
STAGE_NONCE = "nonce"
STAGE_SOCKET = "socket"
STAGE_MOVED = "moved"


def _fd_path(dir_fd: int, name: str) -> str:
    return f"/proc/self/fd/{dir_fd}/{name}"


def _rename(src_fd: int, src: str, dst_fd: int, dst: str, flags: int) -> bool | None:
    """renameat2 between two directory fds: True if done, False if dst was
    taken (RENAME_NOREPLACE), None if the kernel or filesystem lacks the
    call or the flag. Moves of an existing control entry use only this:
    there is no fallback that could leave an entry in two places or none."""
    try:
        return renameat2(_fd_path(src_fd, src), _fd_path(dst_fd, dst), flags)
    except OSError as exc:
        if exc.errno in NOREPLACE_UNSUPPORTED:
            return None
        raise


def _publish_to_free_name(stage_fd: int, root_fd: int) -> bool:
    """Place the staged socket at the control name if the name is free.

    True if placed, False if the name is taken. RENAME_NOREPLACE, else (no
    renameat2) link() then unlinking the staged name: both refuse a taken
    name, and this moves only our own new socket. ControlError when the
    filesystem has neither.
    """
    placed = _rename(stage_fd, STAGE_SOCKET, root_fd, CONTROL_SOCKET_NAME, RENAME_NOREPLACE)
    if placed is not None:
        return placed
    try:
        os.link(STAGE_SOCKET, CONTROL_SOCKET_NAME, src_dir_fd=stage_fd, dst_dir_fd=root_fd,
                follow_symlinks=False)
    except FileExistsError:
        return False
    except OSError as exc:
        if exc.errno in (errno.EPERM, errno.ENOTSUP, errno.EOPNOTSUPP):
            raise ControlError(
                "the cache filesystem supports neither no-replace rename nor hard links"
            ) from exc
        raise
    os.unlink(STAGE_SOCKET, dir_fd=stage_fd)
    return True


def _proven_dead(entry_fd: int) -> bool:
    """Whether the socket that entry_fd (O_PATH) pins refuses a connection.

    Only ConnectionRefusedError proves it dead. A connection, a timeout,
    EAGAIN, EACCES, EINTR or any other error proves nothing, so the
    caller must leave the entry where it is.
    """
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.25)
        try:
            probe.connect(f"/proc/self/fd/{entry_fd}")
        except ConnectionRefusedError:
            return True
        except OSError:
            return False
        return False


def _pin_entry(dir_fd: int, name: str) -> int:
    """An O_PATH fd of the entry itself (a symlink is not followed)."""
    return os.open(name, os.O_PATH | os.O_NOFOLLOW, dir_fd=dir_fd)


class _Stage:
    """A private directory used while the cache lock is held.

    mkdir(0o700) at a random name, then open it O_NOFOLLOW through the
    cache fd and require a directory we own, with no group or other bits,
    owner rwx and no entries; nothing touches it before that, and it is
    never chmodded. A nonce file holding the nonce in the stage's name is
    made in it O_EXCL as a recovery aid. Cleanup removes only
    entries whose identity this process recorded when it made them. Only
    an owner-writable parent allows trusting that the directory we opened
    is the one we created and still owns its name at rmdir time.
    """

    def __init__(self, root_fd: int):
        self.root_fd = root_fd
        entry = os.fstat(root_fd)
        self.private_parent = (entry.st_uid == os.geteuid()
                               and not entry.st_mode & (stat.S_IWGRP | stat.S_IWOTH))
        nonce_text = secrets.token_hex(16)
        self.name = f"{STAGE_PREFIX}{nonce_text}"
        self.created: dict[str, tuple[int, int]] = {}
        # 0700 at most under any umask; a umask that strips owner bits
        # fails the check below instead of being chmodded around.
        os.mkdir(self.name, 0o700, dir_fd=root_fd)
        # If the open or the check fails, the name may not be ours: leave it.
        self.fd = os.open(self.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
        try:
            entry = os.fstat(self.fd)
            mode = stat.S_IMODE(entry.st_mode)
            if (not stat.S_ISDIR(entry.st_mode) or entry.st_uid != os.geteuid()
                    or mode & 0o077 or mode & 0o700 != 0o700 or os.listdir(self.fd)):
                raise ControlError("control socket stage directory is not the one this mount created")
            self.identity = _identity(entry)
        except BaseException:
            os.close(self.fd)
            raise
        try:
            nonce = os.open(STAGE_NONCE, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600,
                            dir_fd=self.fd)
            try:
                self.created[STAGE_NONCE] = _identity(os.fstat(nonce))
                os.write(nonce, nonce_text.encode("ascii"))
            finally:
                os.close(nonce)
        except BaseException:
            self.remove()
            raise

    def still_named(self) -> bool:
        try:
            return _identity(_entry_at(self.root_fd, self.name)) == self.identity
        except OSError:
            return False

    def recovery_location(self, name: str) -> str:
        # The stage or its parent can have been renamed while its fd is held.
        try:
            return repr(os.path.join(os.readlink(f"/proc/self/fd/{self.fd}"), name))
        except OSError:
            dev, inode = self.identity
            return f"{name!r} in stage inode {inode} on device {dev} (path unavailable)"

    def remove(self):
        """Remove the entries we made and, when ownership is clear, the directory."""
        try:
            for name, identity in self.created.items():
                try:
                    if _identity(_entry_at(self.fd, name)) == identity:
                        os.unlink(name, dir_fd=self.fd)
                except OSError:
                    pass
            try:
                if self.private_parent and not os.listdir(self.fd) and self.still_named():
                    os.rmdir(self.name, dir_fd=self.root_fd)
            except OSError:
                pass
        finally:
            os.close(self.fd)


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
        # in. A socket moved into another cache still reports this one, so
        # clients can tell. None while not published.
        self.cache_identity = None
        # An fd of that directory, held while the listener lives: it pins
        # the inode, so the identity cannot be reused by a new directory
        # while this socket can still answer, and stop() cleans up in the
        # directory the socket was published in even if the path moved.
        self._cache_fd = None

    def _publish(self, listener: socket.socket) -> tuple[tuple[int, int], int, tuple[int, int]]:
        """Publish the listener and return its identity, the cache fd and cache identity.

        The cache fd is a separate open of the locked directory, so holding
        it does not hold the lock. The caller owns it.
        """
        if len(os.fsencode(self.path)) > 107:
            raise ControlError("control socket path exceeds the 107-byte Unix socket limit")
        with _locked_cache(self.path.parent) as root:
            _require_private_cache(root)
            pinned = os.open(".", os.O_RDONLY | os.O_DIRECTORY, dir_fd=root)
            try:
                cache_identity = _identity(os.fstat(pinned))
                return self._publish_in(listener, root), pinned, cache_identity
            except BaseException:
                os.close(pinned)
                raise

    def _publish_in(self, listener: socket.socket, root: int) -> tuple[int, int]:
        # Every step below goes through the cache and stage fds, never a
        # path through the cache, so a renamed or swapped stage cannot
        # redirect chmod, stat, link or rename. Binding through the stage
        # fd keeps the longer stage path out of the Unix address limit.
        stage = _Stage(root)
        try:
            staged = _fd_path(stage.fd, STAGE_SOCKET)
            listener.bind(staged)
            # Nobody else can write the verified stage, so the entry is
            # the socket just bound; cleanup removes only that.
            bound = _identity(_require_owner_socket(_entry_at(stage.fd, STAGE_SOCKET), self.path))
            stage.created[STAGE_SOCKET] = bound
            # Follows a final symlink, but only we can write the stage.
            os.chmod(staged, 0o600)
            listener.listen(8)
            listener.settimeout(0.25)
            # Our socket is still safe in the pinned stage, but a stage
            # name that no longer names it means someone is tampering
            # with the cache: refuse rather than publish beside them.
            if not stage.still_named():
                raise ControlError("control socket stage directory changed during startup")
            if not _publish_to_free_name(stage.fd, root):
                self._take_over(stage, root)
            return bound
        finally:
            stage.remove()

    def _take_over(self, stage: _Stage, root: int):
        """Replace the entry at the control name with our staged socket, only
        if it is proven to be a dead socket of ours; otherwise refuse.

        The entry is pinned (O_PATH|O_NOFOLLOW) and stays pinned until the
        decision is final, so its inode cannot be freed and reused. It must
        be a socket of ours that refuses a connection through the pin. Then
        one RENAME_EXCHANGE swaps our socket onto the name and the old entry
        into the stage. If the process dies after the exchange, our dead
        socket is public and the displaced entry is available for recovery.
        If the stage side cannot be verified as the pinned inode and
        removed, try to restore it without displacing another entry.
        Without RENAME_EXCHANGE an occupied name is left in place.
        """
        try:
            pin = _pin_entry(root, CONTROL_SOCKET_NAME)
        except FileNotFoundError:
            # Freed meanwhile: one more no-clobber attempt, never a move.
            if _publish_to_free_name(stage.fd, root):
                return
            raise ControlError("control socket changed during startup") from None
        try:
            entry = os.fstat(pin)
            if not stat.S_ISSOCK(entry.st_mode) or entry.st_uid != os.geteuid() or not _proven_dead(pin):
                raise ControlError(
                    "the control socket name holds a live socket, another user's entry, "
                    "or something that is not a socket; leaving it in place"
                )
            probed = _identity(entry)
            exchanged = _rename(stage.fd, STAGE_SOCKET, root, CONTROL_SOCKET_NAME, RENAME_EXCHANGE)
            if exchanged is None:
                raise ControlError(
                    "the cache filesystem cannot exchange entries atomically (renameat2 "
                    "RENAME_EXCHANGE), so the dead control socket there is not replaced"
                )
            try:
                if _identity(_entry_at(stage.fd, STAGE_SOCKET)) != probed:
                    raise ControlError("control socket changed during startup")
                os.unlink(STAGE_SOCKET, dir_fd=stage.fd)
            except BaseException:
                self._restore(stage, root, STAGE_SOCKET)
                raise
        finally:
            os.close(pin)

    def _restore(self, stage: _Stage, root: int, name: str):
        # In a shared parent the public entry can change between the
        # identity check and rename. Only restore into a free name there.
        try:
            flags = RENAME_NOREPLACE
            if name == STAGE_SOCKET and stage.private_parent:
                with suppress(FileNotFoundError):
                    public = _entry_at(root, CONTROL_SOCKET_NAME)
                    if _identity(public) == stage.created.get(STAGE_SOCKET):
                        flags = RENAME_EXCHANGE
            restored = _rename(stage.fd, name, root, CONTROL_SOCKET_NAME, flags)
        except OSError as exc:
            restored = exc
        if restored is not True:
            self._logger.warning(
                "could not restore the displaced entry at %s; retained at %s (%s)",
                self.path, stage.recovery_location(name), restored,
            )

    def start(self):
        with self._lock:
            if self._listener is not None:
                return
            listener = None
            thread = None
            ready = threading.Event()
            stop = threading.Event()
            try:
                listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                # A failed thread launch should leave no public endpoint.
                # A launched thread waits until the mount identity is set.
                thread = threading.Thread(
                    target=self._serve, args=(listener, stop, ready), daemon=True,
                    name="storage-control-socket",
                )
                thread.start()
                self._owned_entry, self._cache_fd, self.cache_identity = self._publish(listener)
                self._listener, self._thread, self._stop = listener, thread, stop
            except (OSError, ControlError, RuntimeError) as exc:
                self._logger.warning("failed to start control socket at %s: %s", self.path, exc)
            finally:
                failed = self._listener is None
                if failed:
                    stop.set()
                ready.set()
                if failed:
                    if listener is not None:
                        listener.close()
                    if thread is not None and thread.ident is not None:
                        thread.join()

    def stop(self):
        with self._lock:
            if self._listener is None:
                return
            thread = self._thread
            self._retire_listener()
        # The worker may be handling a fatal accept error and acquiring
        # this same lock to retire its listener.
        timeout = REQUEST_TIMEOUT + 0.25
        thread.join(timeout=timeout)
        if thread.is_alive():
            self._logger.warning(
                "control socket thread still in a request after %.2fs; it exits when that returns",
                timeout,
            )

    def _retire_listener(self):
        """Withdraw and close the current listener. Caller holds self._lock."""
        self._stop.set()
        try:
            with _locked_fd(self._cache_fd) as root:
                self._withdraw(root)
        except (OSError, ControlError):
            # Leave any replacement or stale socket for its owner or a later start.
            pass
        self._listener.close()
        os.close(self._cache_fd)
        self._listener = self._thread = self._cache_fd = None
        self.cache_identity = self._owned_entry = None

    def _withdraw(self, root: int):
        """Remove our published socket, and only it.

        The entry at the name is pinned and must be the socket we published.
        It is moved with RENAME_NOREPLACE into a fresh stage, where it must
        still be that inode to be unlinked; otherwise, or on any failure, it
        is put back with RENAME_NOREPLACE. Without renameat2 it is left: a
        dead socket of ours is harmless, and the next start replaces it.
        """
        try:
            pin = _pin_entry(root, CONTROL_SOCKET_NAME)
        except FileNotFoundError:
            return
        try:
            if _identity(os.fstat(pin)) != self._owned_entry:
                return  # A replacement belongs to its publisher: never moved.
            stage = _Stage(root)
            try:
                moved = _rename(root, CONTROL_SOCKET_NAME, stage.fd, STAGE_MOVED, RENAME_NOREPLACE)
                if not moved:
                    # Unsupported (None), or the fresh stage already had the
                    # name (False, impossible): leave the socket in place.
                    return
                try:
                    if _identity(_entry_at(stage.fd, STAGE_MOVED)) != self._owned_entry:
                        raise ControlError("control socket changed during stop")
                    os.unlink(STAGE_MOVED, dir_fd=stage.fd)
                except BaseException as exc:
                    self._restore(stage, root, STAGE_MOVED)
                    if not isinstance(exc, ControlError):
                        raise
            finally:
                stage.remove()
        finally:
            os.close(pin)

    def _serve(self, listener: socket.socket, stop: threading.Event, ready: threading.Event):
        ready.wait()
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except OSError as exc:
                if isinstance(exc, socket.timeout) and exc.errno is None:
                    continue  # The socket's normal timed wait already blocked.
                if exc.errno in _RETRY_ACCEPT_ERRORS:
                    stop.wait(_ACCEPT_RETRY_DELAY)
                    continue
                with self._lock:
                    if self._listener is listener:
                        self._logger.warning("control socket at %s stopped accepting: %s", self.path, exc)
                        self._retire_listener()
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
            frame = _read_line(conn, MAX_REQUEST_BYTES, time.monotonic() + REQUEST_TIMEOUT,
                               unterminated=LEGACY_STATS_REQUEST)
        except TimeoutError:
            # The deadline check and a recv timeout word this differently.
            response = {"error": "Control request timed out"}
        except ControlError as exc:
            response = {"error": str(exc)}
        except OSError:
            return
        else:
            if frame is None:
                return
            try:
                request = frame.decode("utf-8")
            except UnicodeDecodeError:
                response = {"error": "Control request must be UTF-8"}
            else:
                if stop.is_set():
                    # Complete and authenticated, but too late to act on.
                    response = {"error": STOPPING_MESSAGE}
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
