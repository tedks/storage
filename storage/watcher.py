"""
Remote filesystem watcher using inotifywait over SSH.

Provides push-based cache invalidation: instead of polling backends for
changes every TTL seconds, we run ``inotifywait`` on the remote host and
stream change events back.  This eliminates readdir polling cost entirely
for quiescent directories.

Each Storage mount starts its own watcher per SSH backend.  This means
each mount gets its own ``on_change`` callbacks without any IPC.  Multiple
mounts on the same machine will each have their own SSH+inotifywait
connection to the remote -- this is intentional so that every mount
receives every event.
"""

from __future__ import annotations

import logging
import os
import shlex
import threading
import time
from pathlib import PurePosixPath
from typing import Callable, Optional

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
        Logical name of the backend (for logging).
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
    ):
        self.backend_name = backend_name
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.remote_path = remote_path.rstrip("/") or "/"
        self.on_change = on_change
        self.username = username
        self.key_filename = key_filename

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._process = None

    # ------------------------------------------------------------------ #
    # SSH + inotifywait subprocess
    # ------------------------------------------------------------------ #

    def _build_ssh_command(self) -> list:
        """Build the SSH command that runs inotifywait on the remote."""
        ssh_cmd = ["ssh", "-T", "-o", "BatchMode=yes",
                   "-o", "StrictHostKeyChecking=accept-new",
                   "-o", "ServerAliveInterval=30",
                   "-o", "ServerAliveCountMax=3",
                   "-p", str(self.ssh_port)]
        if self.key_filename:
            ssh_cmd += ["-i", self.key_filename]
        target = f"{self.username}@{self.ssh_host}" if self.username else self.ssh_host
        ssh_cmd.append(target)

        # inotifywait: recursive, monitor mode, relevant events.
        # Quote remote_path to prevent shell injection from paths with
        # spaces or metacharacters.
        events = ",".join(sorted(_CHANGE_EVENTS))
        inotify_cmd = (
            f"inotifywait -mrq --format '%e %w%f' "
            f"-e {events} "
            f"{shlex.quote(self.remote_path)}"
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
        # Use directory-boundary check to avoid /data matching /datafile.txt.
        if abs_path == self.remote_path:
            return "/"
        prefix = self.remote_path if self.remote_path.endswith("/") else self.remote_path + "/"
        if abs_path.startswith(prefix):
            rel = abs_path[len(prefix) - 1:]  # Keep leading /
            return rel if rel else "/"

        return None

    def _event_to_dir(self, virtual_path: str) -> str:
        """Return the parent directory of a virtual path (for dir cache invalidation)."""
        parent = str(PurePosixPath(virtual_path).parent)
        return parent if parent != virtual_path else "/"

    _MAX_BACKOFF = 300.0  # 5 minutes
    _INITIAL_BACKOFF = 5.0

    def _watch_loop(self):
        """Main loop: run inotifywait over SSH and dispatch change events."""
        import subprocess

        cmd = self._build_ssh_command()
        logger.info("Starting remote watcher for %s: %s",
                    self.backend_name, " ".join(cmd))

        backoff = self._INITIAL_BACKOFF

        while not self._stop_event.is_set():
            try:
                # Start SSH+inotifywait in its own process group so we can
                # kill the entire tree (ssh + remote shell + inotifywait)
                # on cleanup, preventing orphaned remote processes.
                self._process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    start_new_session=True,
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
                    stderr_text = ""
                    try:
                        stderr_text = self._process.stderr.read().strip()
                    except Exception:
                        pass
                    if stderr_text:
                        logger.warning(
                            "inotifywait exited with code %d for %s: %s",
                            rc, self.backend_name, stderr_text,
                        )
                    else:
                        logger.warning(
                            "inotifywait exited with code %d for %s",
                            rc, self.backend_name,
                        )
                else:
                    # Successful run (inotifywait exited cleanly, e.g.
                    # watched path was deleted).  Reset backoff.
                    backoff = self._INITIAL_BACKOFF

            except Exception:
                logger.exception("Watcher error for %s", self.backend_name)

            if not self._stop_event.is_set():
                logger.info(
                    "Restarting watcher for %s in %.0fs",
                    self.backend_name, backoff,
                )
                self._stop_event.wait(backoff)
                backoff = min(backoff * 2, self._MAX_BACKOFF)

        self._cleanup_process()

    def _cleanup_process(self):
        proc = self._process
        if proc is None:
            return
        # Kill the entire process group (ssh + remote shell) so that
        # the remote inotifywait doesn't outlive the local SSH client.
        try:
            os.killpg(proc.pid, 9)
        except (ProcessLookupError, PermissionError):
            # Process already exited or we don't own the group.
            pass
        except OSError:
            # Fallback: kill just the SSH process.
            try:
                proc.kill()
            except Exception:
                pass
        try:
            proc.wait(timeout=5)
        except Exception:
            pass
        self._process = None

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def start(self):
        """Start the watcher thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._watch_loop,
            name=f"Watcher-{self.backend_name}",
            daemon=True,
        )
        self._thread.start()
        logger.info("Started remote watcher for %s", self.backend_name)

    def stop(self):
        """Stop the watcher."""
        self._stop_event.set()
        self._cleanup_process()
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None
        logger.info("Stopped remote watcher for %s", self.backend_name)

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()
