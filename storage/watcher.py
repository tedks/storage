"""
Remote filesystem watcher using inotifywait over SSH.

Provides push-based cache invalidation: instead of polling backends for
changes every TTL seconds, we run ``inotifywait`` on the remote host and
stream change events back.  This eliminates readdir polling cost entirely
for quiescent directories.

Multiple Storage mounts on the same machine that share a backend will
share a single watcher process (PID-file deduplication).
"""

from __future__ import annotations

import fcntl
import logging
import os
import re
import threading
import time
from pathlib import Path, PurePosixPath
from typing import Callable, Dict, Optional, Set

logger = logging.getLogger(__name__)

# inotifywait events that mean "contents may have changed".
_CHANGE_EVENTS = frozenset({
    "CREATE", "DELETE", "MODIFY", "MOVED_FROM", "MOVED_TO",
    "ATTRIB", "CLOSE_WRITE",
})


class RemoteWatcher:
    """
    Watch a remote directory tree via ``inotifywait`` over SSH.

    When a change is detected, calls ``on_change(virtual_path)`` so the
    filesystem can invalidate its directory and file caches for that path.

    Parameters
    ----------
    backend_name : str
        Logical name of the backend (used for lock-file deduplication).
    ssh_host : str
        SSH host (user@host or host).
    ssh_port : int
        SSH port.
    remote_path : str
        Absolute path on the remote to watch (the backend root).
    on_change : callable
        ``on_change(virtual_path: str)`` is called for each detected change.
    username : str, optional
        SSH username.
    key_filename : str, optional
        Path to SSH private key.
    lock_dir : Path, optional
        Directory for PID lock files.  Defaults to ``/tmp``.
    """

    def __init__(
        self,
        backend_name: str,
        ssh_host: str,
        ssh_port: int,
        remote_path: str,
        on_change: Callable[[str], None],
        username: Optional[str] = None,
        key_filename: Optional[str] = None,
        lock_dir: Optional[Path] = None,
    ):
        self.backend_name = backend_name
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.remote_path = remote_path.rstrip("/")
        self.on_change = on_change
        self.username = username
        self.key_filename = key_filename
        self.lock_dir = lock_dir or Path("/tmp")

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock_fd: Optional[int] = None
        self._process = None

    # ------------------------------------------------------------------ #
    # PID-file deduplication
    # ------------------------------------------------------------------ #

    def _lock_path(self) -> Path:
        """Per-backend lock file so multiple mounts share one watcher."""
        safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", self.backend_name)
        return self.lock_dir / f".storage.watcher.{safe_name}.lock"

    def _try_acquire_lock(self) -> bool:
        """
        Try to acquire an exclusive lock.  Returns True if we got it
        (meaning we should start the watcher), False if another process
        already holds it (meaning a watcher is already running).
        """
        lock_path = self._lock_path()
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Write our PID for debugging.
            os.ftruncate(fd, 0)
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, f"{os.getpid()}\n".encode())
            self._lock_fd = fd
            return True
        except (BlockingIOError, OSError):
            # Another process holds the lock.
            try:
                os.close(fd)
            except Exception:
                pass
            return False

    def _release_lock(self):
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                os.close(self._lock_fd)
            except Exception:
                pass
            self._lock_fd = None
            try:
                self._lock_path().unlink(missing_ok=True)
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    # SSH + inotifywait subprocess
    # ------------------------------------------------------------------ #

    def _build_ssh_command(self) -> list:
        """Build the SSH command that runs inotifywait on the remote."""
        ssh_cmd = ["ssh", "-T", "-o", "BatchMode=yes",
                   "-o", "ServerAliveInterval=30",
                   "-o", "ServerAliveCountMax=3",
                   "-p", str(self.ssh_port)]
        if self.key_filename:
            ssh_cmd += ["-i", self.key_filename]
        target = f"{self.username}@{self.ssh_host}" if self.username else self.ssh_host
        ssh_cmd.append(target)

        # inotifywait: recursive, monitor mode, CSV output, relevant events.
        events = ",".join(sorted(_CHANGE_EVENTS))
        inotify_cmd = (
            f"inotifywait -mrq --format '%e %w%f' "
            f"-e {events} "
            f"{self.remote_path}"
        )
        ssh_cmd.append(inotify_cmd)
        return ssh_cmd

    def _parse_event_line(self, line: str) -> Optional[str]:
        """
        Parse an inotifywait output line into a virtual path.

        Line format: ``EVENTS /absolute/path``
        Returns the virtual path (relative to remote_path) or None.
        """
        line = line.strip()
        if not line:
            return None

        # Split on first space: "CREATE,ISDIR /remote/root/subdir/"
        parts = line.split(" ", 1)
        if len(parts) < 2:
            return None

        events_str, abs_path = parts
        abs_path = abs_path.rstrip("/")

        # Convert absolute remote path to virtual path.
        if abs_path.startswith(self.remote_path):
            rel = abs_path[len(self.remote_path):]
            if not rel:
                rel = "/"
            if not rel.startswith("/"):
                rel = "/" + rel
            return rel

        return None

    def _event_to_dir(self, virtual_path: str) -> str:
        """Return the parent directory of a virtual path (for dir cache invalidation)."""
        parent = str(PurePosixPath(virtual_path).parent)
        return parent if parent != virtual_path else "/"

    def _watch_loop(self):
        """Main loop: run inotifywait over SSH and dispatch change events."""
        import subprocess

        cmd = self._build_ssh_command()
        logger.info("Starting remote watcher for %s: %s",
                    self.backend_name, " ".join(cmd))

        while not self._stop_event.is_set():
            try:
                self._process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )

                for line in self._process.stdout:
                    if self._stop_event.is_set():
                        break
                    vpath = self._parse_event_line(line)
                    if vpath is not None:
                        try:
                            # Invalidate both the file and its parent directory.
                            self.on_change(vpath)
                            parent = self._event_to_dir(vpath)
                            if parent != vpath:
                                self.on_change(parent)
                        except Exception:
                            logger.exception("Error in on_change callback")

                self._process.wait()
                rc = self._process.returncode

                if self._stop_event.is_set():
                    break

                if rc != 0:
                    stderr = self._process.stderr.read() if self._process.stderr else ""
                    logger.warning(
                        "inotifywait exited with code %d for %s: %s",
                        rc, self.backend_name, stderr.strip()
                    )

            except Exception:
                logger.exception("Watcher error for %s", self.backend_name)

            if not self._stop_event.is_set():
                # Backoff before restart.
                self._stop_event.wait(5.0)

        self._cleanup_process()

    def _cleanup_process(self):
        if self._process is not None:
            try:
                self._process.terminate()
                self._process.wait(timeout=5)
            except Exception:
                try:
                    self._process.kill()
                except Exception:
                    pass
            self._process = None

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def start(self) -> bool:
        """
        Start the watcher.

        Returns True if this process started a new watcher, False if
        another process already has one running for this backend.
        """
        if not self._try_acquire_lock():
            logger.info(
                "Watcher for %s already running (lock held by another process)",
                self.backend_name,
            )
            return False

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._watch_loop,
            name=f"Watcher-{self.backend_name}",
            daemon=True,
        )
        self._thread.start()
        logger.info("Started remote watcher for %s", self.backend_name)
        return True

    def stop(self):
        """Stop the watcher and release the lock."""
        self._stop_event.set()
        self._cleanup_process()
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None
        self._release_lock()
        logger.info("Stopped remote watcher for %s", self.backend_name)

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()
