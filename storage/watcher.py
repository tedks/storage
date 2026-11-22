"""
Remote filesystem watcher using inotifywait over SSH.

Provides push-based cache invalidation: instead of polling backends for
changes every TTL seconds, we run ``inotifywait`` on the remote host and
stream change events back.  This eliminates readdir polling cost entirely
for quiescent directories.

When multiple backends share the same SSH host, a single SSH connection
and inotifywait process watches all paths.  Each mount still gets its
own watcher instances (no IPC between mounts).
"""

from __future__ import annotations

from contextlib import contextmanager, suppress
import logging
import os
import shlex
import tempfile
import threading
import time
from pathlib import PurePosixPath
from typing import Callable, Dict, List, Optional, Tuple

from .backends.openssh_pool import WatcherUnsupported
from .backends.ssh_mux import ControlSockets
from .backends.ssh_pool import BulkCapacityCancelled

logger = logging.getLogger(__name__)

# inotifywait events that mean "contents may have changed".
_CHANGE_EVENTS = frozenset({
    "CREATE", "DELETE", "MODIFY", "MOVED_FROM", "MOVED_TO",
    "ATTRIB", "CLOSE_WRITE",
})


class _MuxWatch:
    """Close remote inotifywait's keepalive input before cancelling mux."""

    def __init__(self, session, keepalive):
        self._session = session
        self._keepalive = keepalive

    def wait(self, timeout):
        return self._session.wait(timeout)

    def poll(self):
        return self._session.poll()

    def close(self):
        self._keepalive.close()
        try:
            with suppress(ConnectionError, TimeoutError):
                self._session.wait(1)
        finally:
            self._session.close()


class RemoteWatcher:
    """
    Watch one or more remote directory trees via ``inotifywait`` over SSH.

    When a change is detected, calls ``on_change(virtual_path)`` so the
    filesystem can invalidate its directory and file caches for that path.

    Multiple remote paths on the same host are watched by a single
    inotifywait process to avoid redundant SSH connections.

    Parameters
    ----------
    backend_name : str
        Logical name for logging.  When watching multiple backends on
        one host, this is a comma-joined label like ``remote0,remote1``.
    ssh_host : str
        SSH host.
    ssh_port : int
        SSH port.
    remote_paths : list of str
        Absolute paths on the remote to watch.
    on_change : callable
        ``on_change(virtual_path: str)`` is called for each detected change.
    username : str, optional
        SSH username.
    key_filename : str, optional
        Path to SSH private key.
    on_resync : callable, optional
        ``on_resync()`` is called each time inotifywait reports that its
        watches are established, the first time and after every restart.
        Events between the old process's end and that moment were never
        seen, so the caller drops what it learned before it (listings).
        It runs on a helper thread, holding nothing.
    """

    def __init__(
        self,
        backend_name: str,
        ssh_host: str,
        ssh_port: int,
        remote_path: str = "",
        on_change: Optional[Callable[[str], None]] = None,
        username: Optional[str] = None,
        key_filename: Optional[str] = None,
        *,
        remote_paths: Optional[List[str]] = None,
        control_sockets: Optional[ControlSockets] = None,
        on_resync: Optional[Callable[[], None]] = None,
        session_pool=None,
    ):
        self.backend_name = backend_name
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.on_change = on_change
        self.on_resync = on_resync
        self.username = username
        self.key_filename = key_filename
        self._control_sockets = control_sockets
        self._session_pool = session_pool

        # Accept either remote_paths (list) or remote_path (single string).
        if not remote_paths and remote_path:
            remote_paths = [remote_path]
        elif not remote_paths:
            remote_paths = []
        # Sort longest-first so that overlapping prefixes like /data/sub
        # match before /data.
        self._remote_paths: List[str] = sorted(
            [(p.rstrip("/") or "/") for p in remote_paths],
            key=len,
            reverse=True,
        )

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._process = None
        self._process_lock = threading.Lock()

    # Legacy property for tests that read self.remote_path.
    @property
    def remote_path(self) -> str:
        return self._remote_paths[0] if self._remote_paths else "/"

    # ------------------------------------------------------------------ #
    # SSH + inotifywait subprocess
    # ------------------------------------------------------------------ #

    def _build_ssh_command(self) -> list:
        """Build the SSH command that runs inotifywait on the remote."""
        # Preserve trusted keys for other members sharing this hostname;
        # OpenSSH can otherwise prune keys the current member did not offer.
        ssh_cmd = ["ssh", "-T", "-o", "BatchMode=yes",
                   "-o", "StrictHostKeyChecking=accept-new",
                   "-o", "UpdateHostKeys=no",
                   "-o", "ServerAliveInterval=30",
                   "-o", "ServerAliveCountMax=3",
                   "-p", str(self.ssh_port)]
        if self._control_sockets is not None:
            ssh_cmd += self._control_sockets.options(
                self.username, self.ssh_host, self.ssh_port, self.key_filename)
        if self.key_filename:
            ssh_cmd += ["-i", self.key_filename]
        target = f"{self.username}@{self.ssh_host}" if self.username else self.ssh_host
        ssh_cmd.append(target)

        ssh_cmd.append(self._inotify_command())
        return ssh_cmd

    def _inotify_command(self) -> str:
        events = ",".join(sorted(_CHANGE_EVENTS))
        quoted_paths = " ".join(shlex.quote(p) for p in self._remote_paths)
        return f"inotifywait -mr --format '%e %w%f' -e {events} -- {quoted_paths}"

    def _parse_event_line(self, line: str) -> Optional[str]:
        """
        Parse an inotifywait output line into a virtual path.

        Line format: ``EVENTS /absolute/path``
        Returns the virtual path (relative to the matching remote_path)
        or None if it doesn't match any watched path.
        """
        line = line.strip()
        if not line:
            return None

        parts = line.split(" ", 1)
        if len(parts) < 2:
            return None

        events_str, abs_path = parts
        abs_path = abs_path.rstrip("/")

        # Try each watched path to find which backend root this event belongs to.
        for rpath in self._remote_paths:
            if abs_path == rpath:
                return "/"
            prefix = rpath if rpath.endswith("/") else rpath + "/"
            if abs_path.startswith(prefix):
                rel = abs_path[len(prefix) - 1:]  # Keep leading /
                return rel if rel else "/"

        return None

    def _event_to_dir(self, virtual_path: str) -> str:
        """Return the parent directory of a virtual path (for dir cache invalidation)."""
        parent = str(PurePosixPath(virtual_path).parent)
        return parent if parent != virtual_path else "/"

    # inotifywait's own progress lines (stderr; -q would hide them, and
    # "Watches established." is the signal that no event is lost from here).
    _WATCHES_ESTABLISHED = b"Watches established"
    _INFO_LINES = ("Setting up watches", "Watches established")

    def _announce_watches(self, fd: int, process, done: threading.Event):
        """Call on_resync once inotifywait's stderr says its watches are set.

        Polls the stderr file with pread (the child shares its offset),
        incrementally, keeping the marker's length across reads. Owns fd
        (a duplicate) and closes it on return.
        """
        try:
            self._poll_for_watches(fd, process, done)
        finally:
            os.close(fd)

    def _poll_for_watches(self, fd: int, process, done: threading.Event):
        offset = 0
        tail = b""
        while not done.is_set() and not self._stop_event.is_set():
            try:
                exited = process.poll() is not None  # before the last read
            except ConnectionError:
                return
            try:
                chunk = os.pread(fd, 65536, offset)
            except OSError:
                return
            offset += len(chunk)
            text = tail + chunk
            if self._WATCHES_ESTABLISHED in text:
                if self.on_resync is not None and not self._stop_event.is_set():
                    try:
                        self.on_resync()
                    except Exception:
                        logger.exception("Error in on_resync callback")
                return
            tail = text[-len(self._WATCHES_ESTABLISHED):]
            if exited and not chunk:
                return
            if not chunk:
                done.wait(0.1)

    _MAX_BACKOFF = 300.0  # 5 minutes
    _INITIAL_BACKOFF = 5.0

    @contextmanager
    def _watch_process(self, stderr_file):
        import subprocess

        if self._session_pool is None:
            cmd = self._build_ssh_command()
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=stderr_file,
                                       text=True, start_new_session=True)
            try:
                yield process, process.stdout
            finally:
                self._close_process(process)
                process.stdout.close()
            return

        read_fd, write_fd = os.pipe()
        with os.fdopen(read_fd, "r", errors="replace") as out, os.fdopen(write_fd, "wb") as child:
            input_fd, keepalive_fd = os.pipe()
            with os.fdopen(input_fd, "rb") as inp, os.fdopen(keepalive_fd, "wb") as keepalive:
                # sshd does not always signal a non-PTY command on channel
                # close. Tie the idle remote inotifywait to stdin EOF.
                guard = f"""
exec 3<&0
{self._inotify_command()} </dev/null &
watch=$!
( IFS= read -r unused <&3; kill "$watch" 2>/dev/null ) &
guard=$!
wait "$watch"
status=$?
kill "$guard" 2>/dev/null
wait "$guard" 2>/dev/null
exit "$status"
"""
                with self._session_pool.command(
                    "sh -c " + shlex.quote(guard), inp.fileno(), child.fileno(), stderr_file.fileno(),
                    watcher_stop=self._stop_event.is_set,
                ) as session:
                    inp.close()
                    child.close()
                    process = _MuxWatch(session, keepalive)
                    try:
                        yield process, out
                    except ConnectionError:
                        if not self._stop_event.is_set():
                            raise
                    finally:
                        process.close()

    def _dispatch_events(self, output):
        for line in output:
            if self._stop_event.is_set():
                break
            vpath = self._parse_event_line(line)
            if vpath is not None:
                try:
                    self.on_change(vpath)
                    parent = self._event_to_dir(vpath)
                    if parent != vpath:
                        self.on_change(parent)
                except Exception:
                    logger.exception("Error in on_change callback")

    def _watch_loop(self):
        """Run inotifywait over SSH and dispatch change events."""
        backoff = self._INITIAL_BACKOFF
        while not self._stop_event.is_set():
            try:
                # Older OpenSSH clients can leave stderr held by the master.
                # A file lets readiness polling and diagnostics read it safely.
                with tempfile.TemporaryFile("w+", errors="replace") as err, \
                        self._watch_process(err) as (process, output):
                    with self._process_lock:
                        self._process = process
                        stopped = self._stop_event.is_set()
                    if stopped:
                        return
                    announce_done = threading.Event()
                    announcer = None
                    try:
                        if self.on_resync is not None:
                            announce_fd = os.dup(err.fileno())
                            announcer = threading.Thread(
                                target=self._announce_watches,
                                args=(announce_fd, process, announce_done),
                                name=f"WatcherReady-{self.backend_name}",
                                daemon=True,
                            )
                            try:
                                announcer.start()
                            except BaseException:
                                announcer = None
                                os.close(announce_fd)
                                raise
                        self._dispatch_events(output)
                        if self._stop_event.is_set():
                            return
                        if self._session_pool is None:
                            rc = process.wait()
                        else:
                            rc = process.wait(5)
                    finally:
                        announce_done.set()
                        if announcer is not None:
                            announcer.join(timeout=10)
                    if rc:
                        if rc == 255 and self._control_sockets is not None:
                            self._control_sockets.reset(
                                self.username, self.ssh_host, self.ssh_port, self.key_filename)
                        err.seek(0)
                        detail = "\n".join(
                            line for line in err.read().splitlines()
                            if line.strip() and not line.startswith(self._INFO_LINES)
                        )
                        logger.warning("inotifywait exited with code %d for %s: %s",
                                       rc, self.backend_name, detail)
                    else:
                        backoff = self._INITIAL_BACKOFF
            except (WatcherUnsupported, BulkCapacityCancelled):
                pass
            except Exception:
                logger.exception("Watcher error for %s", self.backend_name)
            finally:
                with self._process_lock:
                    self._process = None
            if not self._stop_event.wait(backoff):
                backoff = min(backoff * 2, self._MAX_BACKOFF)

    def _close_process(self, process):
        if self._session_pool is not None:
            process.close()
            return
        if process.poll() is None:
            try:
                os.killpg(process.pid, 9)
            except (ProcessLookupError, PermissionError):
                pass
            except OSError:
                try:
                    process.kill()
                except OSError:
                    pass
        try:
            process.wait(timeout=5)
        except Exception:
            pass

    def _cleanup_process(self):
        with self._process_lock:
            process = self._process
        if process is not None:
            self._close_process(process)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def start(self):
        """Start the watcher thread."""
        self._stop_event.clear()
        if self._session_pool is not None:
            self._session_pool.set_watching(True)
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
        if self._session_pool is not None:
            self._session_pool.set_watching(False)
        logger.info("Stopped remote watcher for %s", self.backend_name)

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()
