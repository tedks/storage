"""
One OpenSSH connection per host for the system ssh processes.

Each member's paramiko transport (ssh_pool) serves its metadata and reads.
rsync uploads and the remote watcher run the system ssh instead, and each of
those used to open a connection of its own: a full handshake per uploaded
file, and one more for each watcher group. OpenSSH connection sharing makes
the first of them the master and the rest clients of its socket.

The sockets live in a directory made for this process (mkdtemp: mode 0700,
a unique name), under XDG_RUNTIME_DIR when there is one, because a unix
socket path is limited to about a hundred bytes and the socket names here are
short hashes. The directory goes away when the mount ends (close, also run
at interpreter exit). A master that outlives a crash exits by itself after
its idle time (ControlPersist) and takes its socket with it; one that is
alive but hung is ended by reset(), which callers run when an rsync times out
or loses its connection. ControlMaster=auto already falls back to a direct
connection when the socket is dead.

A master is one more TCP connection to the member, apart from paramiko's, so
it counts against the server's MaxSessions on its own: every rsync and the
watcher are session channels on it, a handful at a time.

STORAGE_SSH_MUX=0 turns sharing off (each ssh connects for itself, as before).
"""

import atexit
import hashlib
import logging
import os
import shutil
import subprocess
import tempfile
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# sockaddr_un.sun_path holds 108 bytes with the terminating NUL, and ssh binds
# a temporary name (the path, a dot and 16 random characters) before renaming
# the socket into place.
_MAX_CONTROL_PATH = 108 - 1 - 17

# How long an idle master stays up once its last client is gone.
DEFAULT_PERSIST_SECONDS = 60

# Bound on the ssh -O exit that ends a master.
_EXIT_TIMEOUT = 5.0

_SocketKey = Tuple[str, str, int, str]


class ControlSockets:
    """The control sockets of one process's system ssh connections."""

    def __init__(self, parent_dir: Optional[Path] = None,
                 persist_seconds: int = DEFAULT_PERSIST_SECONDS,
                 ssh: str = "ssh",
                 logger: Optional[logging.Logger] = None):
        self._parent_dir = Path(parent_dir) if parent_dir is not None else None
        self._persist = int(persist_seconds)
        self._ssh = ssh
        self.logger = logger or logging.getLogger(__name__)
        self._lock = threading.Lock()
        self._dir: Optional[Path] = None
        self._keys: Dict[Path, _SocketKey] = {}
        self._atexit_registered = False

    # ------------------------------------------------------------------ #
    # Options for a client
    # ------------------------------------------------------------------ #
    def options(self, username: Optional[str], host: str, port: int,
                key_filename: Optional[str] = None) -> List[str]:
        """ssh options that share one connection to this identity.

        Empty when no socket path can be made (no directory, a path too long
        for a unix socket): the caller then connects for itself.
        """
        path = self._socket_path(username or "", host, port, key_filename or "")
        if path is None:
            return []
        return ["-o", "ControlMaster=auto",
                "-o", f"ControlPath={path}",
                "-o", f"ControlPersist={self._persist}"]

    def _socket_path(self, username: str, host: str, port: int,
                     key_filename: str) -> Optional[Path]:
        key: _SocketKey = (username, host, port, key_filename)
        digest = hashlib.sha256("\0".join(map(str, key)).encode()).hexdigest()[:16]
        with self._lock:
            directory = self._ensure_dir_locked()
            if directory is None:
                return None
            path = directory / digest
            if len(str(path)) > _MAX_CONTROL_PATH:
                self.logger.warning(
                    "ssh connection sharing is off: %s is too long for a unix socket", path)
                return None
            self._keys[path] = key
            return path

    def _ensure_dir_locked(self) -> Optional[Path]:
        if self._dir is not None:
            return self._dir
        candidates = []
        if self._parent_dir is not None:
            candidates.append(self._parent_dir)
        else:
            runtime = os.environ.get("XDG_RUNTIME_DIR")
            if runtime:
                candidates.append(Path(runtime))
            candidates.append(Path(tempfile.gettempdir()))
        for parent in candidates:
            try:
                directory = Path(tempfile.mkdtemp(prefix="storage-ssh-", dir=parent))
            except OSError:
                continue
            if len(str(directory)) + 17 > _MAX_CONTROL_PATH:
                shutil.rmtree(directory, ignore_errors=True)  # just made, ours
                continue
            self._dir = directory
            if not self._atexit_registered:
                atexit.register(self.close)
                self._atexit_registered = True
            return directory
        self.logger.warning("ssh connection sharing is off: no usable socket directory")
        return None

    # ------------------------------------------------------------------ #
    # Ending masters
    # ------------------------------------------------------------------ #
    def reset(self, username: Optional[str], host: str, port: int,
              key_filename: Optional[str] = None) -> None:
        """End this identity's master and drop its socket.

        For a connection that hung or was lost: the next client starts a
        fresh master instead of waiting on a dead one.
        """
        key: _SocketKey = (username or "", host, port, key_filename or "")
        with self._lock:
            paths = [path for path, known in self._keys.items() if known == key]
        for path in paths:
            self._end_master(path, key)

    def close(self) -> None:
        """End every master and remove the socket directory."""
        with self._lock:
            keys = dict(self._keys)
            directory = self._dir
            self._keys = {}
            self._dir = None
        for path, key in keys.items():
            self._end_master(path, key)
        if directory is not None:
            shutil.rmtree(directory, ignore_errors=True)  # made by mkdtemp above

    def _end_master(self, path: Path, key: _SocketKey) -> None:
        username, host, port, _key_filename = key
        target = f"{username}@{host}" if username else host
        try:
            subprocess.run(
                [self._ssh, "-o", f"ControlPath={path}", "-o", "BatchMode=yes",
                 "-p", str(port), "-O", "exit", target],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, timeout=_EXIT_TIMEOUT, check=False)
        except (OSError, subprocess.SubprocessError):
            pass  # no master, or one that does not answer: the socket goes below
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            self.logger.debug("could not remove %s", path, exc_info=True)


_shared: Optional[ControlSockets] = None
_shared_lock = threading.Lock()


def shared_control_sockets() -> Optional[ControlSockets]:
    """The process-wide sockets (one mount per process), or None when
    STORAGE_SSH_MUX=0 turns connection sharing off."""
    global _shared
    if os.environ.get("STORAGE_SSH_MUX", "1") == "0":
        return None
    with _shared_lock:
        if _shared is None:
            _shared = ControlSockets()
        return _shared


def close_shared_control_sockets() -> None:
    """End the shared masters and remove their directory (unmount)."""
    with _shared_lock:
        shared = _shared
    if shared is not None:
        shared.close()
