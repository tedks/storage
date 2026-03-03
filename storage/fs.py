"""
FUSE filesystem implementation for Storage

Main filesystem layer that provides:
- Read path: local cache → remote backends → recomputation
- Write path: local cache first → background sync to backends
- LRU cache management
- N-way replication
"""

import os
import errno
import threading
import logging
import fcntl
import stat
import time
import hashlib
import yaml
import json
import socket
import concurrent.futures
from dataclasses import dataclass, field
from fuse import FUSE, FuseOSError, Operations
from pathlib import Path
from typing import Dict, Optional, Set, List, Tuple

from .backends.base import Backend
from .cache import CacheManager
from .state import StateManager, FileMetadata, FileConfig, SyncStatus, FileLocation
from .replication import ReplicationManager
from .sync import SyncManager, DirOperation
from .recompute import RecomputeEngine
from .checksums import ChecksumRecord


@dataclass
class PrefetchSession:
    """Tracks streaming state for an uncached file."""

    canonical_path: str
    virtual_path: str
    backend: Backend
    file_size: int
    final_cache_path: Path
    partial_cache_path: Path
    chunk_size: int
    io_fd: int
    available_ranges: List[Tuple[int, int]] = field(default_factory=list)
    in_flight_chunks: Set[int] = field(default_factory=set)
    refcount: int = 0
    prefetch_complete: bool = False
    finalized: bool = False
    stop_requested: bool = False
    error: Optional[Exception] = None
    worker: Optional[threading.Thread] = None
    has_foreground_read: bool = False
    last_logged_status: str = ""
    last_logged_downloaded_bytes: int = 0
    last_foreground_offset: int = 0

    def __post_init__(self):
        self.lock = threading.RLock()
        self.io_lock = threading.Lock()
        self.condition = threading.Condition(self.lock)


@dataclass
class FDEntry:
    """Bookkeeping record for an opened file descriptor."""

    virtual_path: str
    cache_path: Path
    real_fd: int
    flags: int
    backend_name: Optional[str]
    canonical_path: str
    prefetch_session: Optional[PrefetchSession] = None


class StorageFS(Operations):
    """
    Storage FUSE filesystem

    Implements a filesystem that multiplexes over multiple backing filesystems
    with local caching, replication, and background sync.
    """

    CHECKSUM_XATTR = "user.storage.checksums"
    CACHE_STATE_XATTR = "user.storage.cache"
    _STUB_XATTR = "user.storage.stub"
    _PARTIAL_SUFFIX = ".storage.partial"
    _STREAM_STATE_XATTR = "user.storage.stream"

    def __init__(self, cache_root: Path, state_db: Path, backends: Dict[str, Backend],
                 cache_size: int, num_sync_workers: int = 2, start_sync: bool = True,
                 streaming_reads: bool = False, streaming_chunk_size: int = 1024 * 1024,
                 max_streaming_sessions: int = 8):
        """
        Initialize Storage filesystem

        Args:
            cache_root: Root directory for local cache
            state_db: Path to SQLite state database
            backends: Dictionary of backend name -> Backend instance
            cache_size: Maximum cache size in bytes
            num_sync_workers: Number of background sync worker threads
            streaming_reads: Enable uncached range streaming + background prefetch
            streaming_chunk_size: Range size used for foreground/background remote reads
            max_streaming_sessions: Maximum concurrent streaming sessions
        """
        self.state_manager = StateManager(str(state_db))
        self.cache_manager = CacheManager(cache_root, self.state_manager, cache_size)
        self.replication_manager = ReplicationManager(self.state_manager, backends)
        self.backends = backends

        self.logger = logging.getLogger(__name__)

        self._streaming_reads = streaming_reads
        self._streaming_chunk_size = max(4096, int(streaming_chunk_size))
        self._max_streaming_sessions = max(1, int(max_streaming_sessions))
        self._prefetch_sessions: Dict[str, PrefetchSession] = {}
        self._prefetch_sessions_lock = threading.RLock()
        self._streaming_stats_lock = threading.Lock()
        self._streaming_stats = {
            "streaming_sessions_started": 0,
            "streaming_fallback_opens": 0,
            "foreground_remote_bytes": 0,
            "background_remote_bytes": 0,
            "foreground_fetches": 0,
            "background_fetches": 0,
        }
        self._control_socket_path = self.cache_manager.cache_root / ".storage.control.sock"
        self._control_stop_event = threading.Event()
        self._control_socket_lock = threading.Lock()
        self._control_socket: Optional[socket.socket] = None
        self._control_thread: Optional[threading.Thread] = None

        # Background sync manager
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            self.replication_manager,
            num_workers=num_sync_workers
        )
        self._sync_started = False
        if start_sync:
            self.sync_manager.start()
            self._sync_started = True

        # Recomputation engine
        self.recompute_engine = RecomputeEngine(self.state_manager)

        # File descriptor tracking
        self._fd_counter = 0
        self._fd_map: Dict[int, FDEntry] = {}
        self._fd_lock = threading.Lock()

        # Directory listing cache (per directory, per backend) for fallback when backend unavailable
        self._dir_cache: Dict[str, Dict[str, Set[str]]] = {}
        self._dir_cache_lock = threading.Lock()
        self._readdir_snapshot_ttl = 1.0
        self._readdir_snapshot: Dict[str, Tuple[float, List[str]]] = {}

        # Per-backend checksum index used for conflict surfacing
        self._checksum_lock = threading.RLock()
        self._checksum_records: Dict[tuple, ChecksumRecord] = {}
        self._checksum_tasks: Dict[tuple, concurrent.futures.Future] = {}
        self._checksum_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

        self._cleanup_stale_partials()

    def init(self, path):
        """
        FUSE init hook (runs after mount, post-fork in background mode).

        Args:
            path: Mount path provided by FUSE (unused in this implementation).
        """
        control_thread = self._control_thread
        if control_thread is None or not control_thread.is_alive():
            self._start_control_server()

        if not self._sync_started:
            self.sync_manager.start()
            self._sync_started = True

    def _get_config(self, virtual_path: str) -> FileConfig:
        """Get configuration for a file"""
        cache_path = self.cache_manager.get_cache_path(virtual_path)
        return self.state_manager.get_inherited_config(cache_path)

    def _backend_availability(self) -> Tuple[List[str], List[str]]:
        """Return backend names split by current availability."""
        available: List[str] = []
        unavailable: List[str] = []
        for name, backend in self.backends.items():
            if backend.is_available():
                available.append(name)
            else:
                unavailable.append(name)
        return available, unavailable

    def _cache_state_payload(self, path: str, canonical_path: str) -> Dict[str, object]:
        """Build synthetic cache-state xattr payload for a virtual path."""
        cache_path = self.cache_manager.get_cache_path(path)
        partial_path = self._partial_cache_path(cache_path)
        available_backends, unavailable_backends = self._backend_availability()

        cache_present = bool(cache_path.exists() and cache_path.is_file())
        partial_present = bool(partial_path.exists() and partial_path.is_file())

        payload: Dict[str, object] = {
            "path": canonical_path,
            "cache_path": str(cache_path),
            "cache_present": cache_present,
            "partial_present": partial_present,
            "backend_available": available_backends,
            "backend_unavailable": unavailable_backends,
            "updated_at": time.time(),
        }

        try:
            payload["cache_size"] = cache_path.stat().st_size if cache_present else 0
        except OSError:
            payload["cache_size"] = 0

        try:
            payload["partial_size"] = partial_path.stat().st_size if partial_present else 0
        except OSError:
            payload["partial_size"] = 0

        return payload

    # ------------------------------------------------------------------ #
    # Conflict / checksum helpers
    # ------------------------------------------------------------------ #
    def _resolve_backend_target(self, path: str) -> tuple[str, Optional[str]]:
        """
        Split a virtual path into (canonical_path, backend_name).

        If the name ends with .<backend>, we interpret that suffix as a request
        to operate directly on that backend while keeping the canonical path
        (without suffix) for remote operations.
        """
        p = Path(path)
        name = p.name
        for backend_name in self.backends.keys():
            suffix = f".{backend_name}"
            if name.endswith(suffix):
                base = name[: -len(suffix)]
                canonical = str(p.parent / base)
                if not canonical.startswith("/"):
                    canonical = "/" + canonical
                return canonical, backend_name
        canonical = path if path.startswith("/") else "/" + path
        return canonical, None

    def _invalidate_dir_cache_entry(self, path: str):
        """Remove a directory entry from the cached listing."""
        if not path:
            path = '/'
        if not path.startswith('/'):
            path = '/' + path
        with self._dir_cache_lock:
            self._dir_cache.pop(path, None)
            self._readdir_snapshot.pop(path, None)

    def _get_target_backends(self, backend_hint: Optional[str]) -> List[Backend]:
        """Return the backend instances affected by an operation."""
        if backend_hint:
            backend = self.backends.get(backend_hint)
            return [backend] if backend else []
        return list(self.backends.values())

    def _propagate_dir_operation(self, operation: DirOperation, path: str,
                                 backend_hint: Optional[str] = None,
                                 new_path: Optional[str] = None) -> List[str]:
        """Best-effort synchronous propagation; returns backend names that failed."""
        failures: List[str] = []
        for backend in self._get_target_backends(backend_hint):
            if not backend:
                continue
            try:
                if operation == DirOperation.MKDIR:
                    backend.mkdir(path)
                elif operation == DirOperation.RMDIR:
                    backend.rmdir(path)
                elif operation == DirOperation.RENAME:
                    if not new_path:
                        raise ValueError("rename requires new_path")
                    backend.rename(path, new_path)
            except Exception as exc:
                failures.append(backend.name)
                self.logger.debug("dir op %s failed for %s on %s: %s",
                                   operation.value, path, backend.name, exc)
        return failures

    def _record_checksum(self, canonical_path: str, backend_name: str, checksum: str, timestamp: float):
        """Record a checksum for (path, backend) and keep only the latest."""
        key = (canonical_path, backend_name)
        with self._checksum_lock:
            self._checksum_records[key] = ChecksumRecord(checksum=checksum, timestamp=timestamp)

    def _aggregate_checksums_yaml(self, canonical_path: str) -> bytes:
        """Return YAML list of '<backend>:<checksum>:<timestamp>' entries."""
        with self._checksum_lock:
            entries = [
                f"{backend}:{record.checksum}:{record.timestamp}"
                for (path, backend), record in self._checksum_records.items()
                if path == canonical_path
            ]
        return yaml.dump(entries, default_flow_style=False).encode()

    def _schedule_directory_checksums(self, dir_path: str, backend_entries: Dict[str, Set[str]]):
        """Queue checksum computations for backend collisions in the directory."""
        owners: Dict[str, Set[str]] = {}
        for backend_name, names in backend_entries.items():
            for name in names:
                owners.setdefault(name, set()).add(backend_name)

        for backend_name, names in backend_entries.items():
            backend = self.backends.get(backend_name)
            if not backend:
                continue
            for name in names:
                # Avoid expensive checksum work for entries that only exist on one backend.
                if len(owners.get(name, set())) <= 1:
                    continue
                virtual_child = str(Path(dir_path) / name)
                try:
                    if backend.isdir(virtual_child):
                        continue
                except Exception:
                    pass
                self._schedule_checksum_task(backend, virtual_child)

    def _schedule_checksum_task(self, backend: Backend, canonical_path: str):
        key = (canonical_path, backend.name)
        with self._checksum_lock:
            if key in self._checksum_tasks:
                return

            future = self._checksum_executor.submit(
                self._compute_and_store_checksum, backend, canonical_path
            )
            self._checksum_tasks[key] = future

            def _cleanup(_):
                with self._checksum_lock:
                    self._checksum_tasks.pop(key, None)

            future.add_done_callback(_cleanup)

    def _compute_and_store_checksum(self, backend: Backend, canonical_path: str):
        """Compute checksum on backend side and store in xattr + memory."""
        try:
            checksum = backend.get_fast_checksum(canonical_path)
            ts = time.time()
            # Write to backend xattr (best effort)
            try:
                yaml_blob = yaml.dump(
                    [f"{backend.name}:{checksum}:{ts}"], default_flow_style=False
                ).encode()
                backend.set_xattr(canonical_path, self.CHECKSUM_XATTR, yaml_blob)
            except Exception:
                pass

            self._record_checksum(canonical_path, backend.name, checksum, ts)
        except Exception as exc:
            self.logger.debug("checksum for %s on %s failed: %s", canonical_path, backend.name, exc)

    def _apply_conflict_view(self, dir_path: str, base_entries: Set[str]) -> Set[str]:
        """
        Convert raw directory entries into conflict-aware view.

        If multiple backends contain the same name with differing checksums,
        present them as <name>.<backend>. Otherwise present the plain name.
        """
        with self._dir_cache_lock:
            backend_entries = self._dir_cache.get(dir_path, {})

        aggregated: Dict[str, Set[str]] = {}
        for backend_name, names in backend_entries.items():
            for name in names:
                aggregated.setdefault(name, set()).add(backend_name)

        visible: Set[str] = set()
        for name, backend_names in aggregated.items():
            if len(backend_names) <= 1:
                visible.add(name)
                continue

            canonical = str(Path(dir_path) / name)
            with self._checksum_lock:
                checks = [
                    self._checksum_records.get((canonical, backend))
                    for backend in backend_names
                ]
            if all(checks) and len({c.checksum for c in checks if c}) > 1:
                for backend in backend_names:
                    visible.add(f"{name}.{backend}")
            else:
                visible.add(name)

        # Keep any purely cached entries (new files not on backends yet)
        for entry in base_entries:
            if entry in aggregated:
                continue

            # Suppress cached conflict artifacts once checksums agree
            suppressed = False
            for backend_name in self.backends.keys():
                suffix = f".{backend_name}"
                if entry.endswith(suffix):
                    base_candidate = entry[: -len(suffix)]
                    if base_candidate in aggregated:
                        suppressed = True
                        break
            if suppressed:
                continue

            visible.add(entry)

        return visible

    def _cleanup_stale_partials(self):
        """Delete stale partial-cache files left by prior crashes."""
        removed = 0
        for partial_path in self.cache_manager.cache_root.rglob(f"*{self._PARTIAL_SUFFIX}"):
            try:
                partial_path.unlink()
                removed += 1
            except OSError:
                continue
        if removed:
            self.cache_manager.mark_cache_size_dirty()
            self.logger.info("Removed %d stale partial cache files", removed)

    @staticmethod
    def _is_read_only_open(flags: int) -> bool:
        write_bits = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC
        return (flags & write_bits) == 0

    def _partial_cache_path(self, cache_path: Path) -> Path:
        return Path(f"{cache_path}{self._PARTIAL_SUFFIX}")

    def _select_backend_for_read(self, canonical_path: str) -> Optional[Backend]:
        """Pick a readable backend, preferring existing replication metadata."""
        backend = self.replication_manager.get_backend_for_read(canonical_path)
        if backend:
            return backend

        available = [b for b in self.backends.values() if b.is_available()]
        available.sort(key=lambda b: b.speed_priority)
        for candidate in available:
            try:
                if candidate.exists(canonical_path):
                    return candidate
            except Exception:
                continue
        return None

    @staticmethod
    def _is_range_available(ranges: List[Tuple[int, int]], start: int, end: int) -> bool:
        if start >= end:
            return True

        cursor = start
        for range_start, range_end in ranges:
            if range_end <= cursor:
                continue
            if range_start > cursor:
                return False
            cursor = max(cursor, range_end)
            if cursor >= end:
                return True

        return cursor >= end

    @staticmethod
    def _first_missing_range(ranges: List[Tuple[int, int]], start: int, end: int) -> Optional[Tuple[int, int]]:
        if start >= end:
            return None

        cursor = start
        for range_start, range_end in ranges:
            if range_end <= cursor:
                continue
            if range_start > cursor:
                return (cursor, min(end, range_start))
            cursor = max(cursor, range_end)
            if cursor >= end:
                return None

        if cursor < end:
            return (cursor, end)
        return None

    @staticmethod
    def _add_available_range(ranges: List[Tuple[int, int]], start: int, end: int):
        if start >= end:
            return

        merged: List[Tuple[int, int]] = []
        inserted = False

        for range_start, range_end in ranges:
            if range_end < start:
                merged.append((range_start, range_end))
                continue
            if end < range_start:
                if not inserted:
                    merged.append((start, end))
                    inserted = True
                merged.append((range_start, range_end))
                continue

            start = min(start, range_start)
            end = max(end, range_end)

        if not inserted:
            merged.append((start, end))

        ranges[:] = merged

    @staticmethod
    def _contiguous_available_prefix(ranges: List[Tuple[int, int]], file_size: int) -> int:
        cursor = 0
        for range_start, range_end in ranges:
            if range_end <= cursor:
                continue
            if range_start > cursor:
                break
            cursor = max(cursor, range_end)
            if cursor >= file_size:
                return file_size
        return min(cursor, file_size)

    def _snapshot_streaming_state(self, session: PrefetchSession, status: str) -> Dict[str, object]:
        with session.lock:
            ranges = list(session.available_ranges)
            downloaded_bytes = sum(max(0, end - start) for start, end in ranges)
            contiguous_bytes = self._contiguous_available_prefix(ranges, session.file_size)
            payload = {
                "status": status,
                "path": session.canonical_path,
                "backend": session.backend.name,
                "file_size": session.file_size,
                "downloaded_bytes": downloaded_bytes,
                "contiguous_bytes": contiguous_bytes,
                "prefetch_complete": bool(session.prefetch_complete),
                "finalized": bool(session.finalized),
                "active_readers": session.refcount,
                "has_foreground_read": bool(session.has_foreground_read),
                "updated_at": time.time(),
            }
            if session.error is not None:
                error_text = str(session.error).strip()
                if not error_text:
                    error_text = repr(session.error)
                payload["error"] = error_text
                payload["error_type"] = type(session.error).__name__
        return payload

    def _log_streaming_state_change(self, session: PrefetchSession, payload: Dict[str, object]):
        status = str(payload.get("status", "unknown"))
        downloaded = int(payload.get("downloaded_bytes", 0))
        contiguous = int(payload.get("contiguous_bytes", 0))
        file_size = int(payload.get("file_size", 0))
        readers = int(payload.get("active_readers", 0))

        should_log = False
        with session.lock:
            if status != session.last_logged_status:
                should_log = True
                session.last_logged_status = status
                session.last_logged_downloaded_bytes = downloaded
            elif downloaded >= file_size and downloaded != session.last_logged_downloaded_bytes:
                should_log = True
                session.last_logged_downloaded_bytes = downloaded
            elif downloaded - session.last_logged_downloaded_bytes >= max(1, session.chunk_size * 4):
                should_log = True
                session.last_logged_downloaded_bytes = downloaded

        if not should_log:
            return

        error_text = payload.get("error")
        error_type = payload.get("error_type")
        if error_text:
            self.logger.info(
                "streaming %s path=%s backend=%s downloaded=%d/%d contiguous=%d readers=%d error_type=%s error=%s",
                status, session.canonical_path, session.backend.name, downloaded, file_size, contiguous, readers, error_type, error_text
            )
        else:
            self.logger.info(
                "streaming %s path=%s backend=%s downloaded=%d/%d contiguous=%d readers=%d",
                status, session.canonical_path, session.backend.name, downloaded, file_size, contiguous, readers
            )

    def _update_streaming_state_xattr(self, session: PrefetchSession, status: str):
        payload = self._snapshot_streaming_state(session, status)
        self._log_streaming_state_change(session, payload)

        with session.lock:
            target_path = session.final_cache_path if session.finalized else session.partial_cache_path

        try:
            os.setxattr(str(target_path), self._STREAM_STATE_XATTR, json.dumps(payload, sort_keys=True).encode("utf-8"))
        except OSError:
            pass

    def _streaming_stats_snapshot(self) -> Dict[str, object]:
        """Return a point-in-time snapshot of streaming counters."""
        with self._streaming_stats_lock:
            stats = dict(self._streaming_stats)

        with self._prefetch_sessions_lock:
            active_streaming_sessions = len(self._prefetch_sessions)

        stats.update({
            "active_streaming_sessions": active_streaming_sessions,
            "pid": os.getpid(),
            "timestamp": time.time(),
            "streaming_enabled": bool(self._streaming_reads),
        })
        return stats

    def _handle_control_request(self, request: str) -> Dict[str, object]:
        if request == "streaming-stats":
            return self._streaming_stats_snapshot()
        return {"error": f"unsupported request: {request}"}

    def _control_server_loop(self):
        """Serve simple debug requests from local CLI clients."""
        while not self._control_stop_event.is_set():
            with self._control_socket_lock:
                control_socket = self._control_socket

            if control_socket is None:
                return

            try:
                conn, _ = control_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._control_stop_event.is_set():
                    return
                continue

            with conn:
                try:
                    conn.settimeout(2.0)
                    payload = conn.recv(4096)
                    request = payload.decode("utf-8", errors="replace").strip()
                    response = self._handle_control_request(request)
                except Exception as exc:
                    response = {"error": f"control request failed: {exc}"}

                try:
                    conn.sendall((json.dumps(response, sort_keys=True) + "\n").encode("utf-8"))
                except Exception:
                    pass

    def _start_control_server(self):
        """Start the local control socket used by debug commands."""
        self._control_stop_event.clear()

        try:
            self._control_socket_path.unlink(missing_ok=True)
        except OSError:
            pass

        try:
            control_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            control_socket.bind(str(self._control_socket_path))
            os.chmod(self._control_socket_path, 0o600)
            control_socket.listen(8)
            control_socket.settimeout(0.5)
        except OSError as exc:
            self.logger.warning("failed to start control socket at %s: %s", self._control_socket_path, exc)
            try:
                control_socket.close()
            except Exception:
                pass
            return

        with self._control_socket_lock:
            self._control_socket = control_socket

        self._control_thread = threading.Thread(
            target=self._control_server_loop,
            daemon=True,
            name="storage-control-socket",
        )
        self._control_thread.start()

    def _stop_control_server(self):
        self._control_stop_event.set()

        with self._control_socket_lock:
            control_socket = self._control_socket
            self._control_socket = None

        if control_socket is not None:
            try:
                control_socket.close()
            except OSError:
                pass

        control_thread = self._control_thread
        self._control_thread = None
        if control_thread and control_thread.is_alive():
            control_thread.join(timeout=2.0)

        try:
            self._control_socket_path.unlink(missing_ok=True)
        except OSError:
            pass

    def _record_streaming_stat(self, key: str, value: int = 1):
        with self._streaming_stats_lock:
            self._streaming_stats[key] = self._streaming_stats.get(key, 0) + value

    def _write_session_bytes(self, session: PrefetchSession, offset: int, data: bytes):
        if not data:
            return

        with session.io_lock:
            if hasattr(os, "pwrite"):
                os.pwrite(session.io_fd, data, offset)
                return

            os.lseek(session.io_fd, offset, os.SEEK_SET)
            os.write(session.io_fd, data)

    def _fetch_range_into_session(self, session: PrefetchSession, start: int, end: int, source: str) -> int:
        target_end = min(end, session.file_size)
        if start >= target_end:
            return start

        cursor = start
        chunks: List[Tuple[int, bytes]] = []
        total_bytes = 0

        while cursor < target_end:
            remaining = target_end - cursor
            chunk = session.backend.read_file(session.canonical_path, cursor, remaining)
            if chunk is None:
                chunk = b""

            max_bytes = max(0, session.file_size - cursor)
            if len(chunk) > max_bytes:
                chunk = chunk[:max_bytes]

            if len(chunk) == 0:
                break

            chunks.append((cursor, chunk))
            cursor += len(chunk)
            total_bytes += len(chunk)

        fetched_end = cursor

        if start < session.file_size and fetched_end == start:
            raise IOError(f"range fetch returned no data for {session.canonical_path} at offset {start}")

        for chunk_offset, chunk_data in chunks:
            self._write_session_bytes(session, chunk_offset, chunk_data)

        with session.lock:
            self._add_available_range(session.available_ranges, start, fetched_end)
            if self._is_range_available(session.available_ranges, 0, session.file_size):
                session.prefetch_complete = True
            session.condition.notify_all()

        self._record_streaming_stat(f"{source}_fetches", 1)
        self._record_streaming_stat(f"{source}_remote_bytes", total_bytes)
        self._update_streaming_state_xattr(session, "prefetching")
        return fetched_end

    def _ensure_streaming_range(self, session: PrefetchSession, start: int, end: int):
        if start >= end:
            return

        while True:
            with session.lock:
                session.last_foreground_offset = start
                if not session.has_foreground_read:
                    session.has_foreground_read = True
                    session.condition.notify_all()

                if self._is_range_available(session.available_ranges, start, end):
                    return

                if session.error is not None:
                    raise FuseOSError(errno.EIO)

                missing = self._first_missing_range(session.available_ranges, start, end)
                if missing is None:
                    return

                chunk_start = (missing[0] // session.chunk_size) * session.chunk_size
                chunk_end = min(chunk_start + session.chunk_size, session.file_size)

                if chunk_start in session.in_flight_chunks:
                    session.condition.wait(timeout=0.05)
                    continue

                session.in_flight_chunks.add(chunk_start)

            try:
                _ = self._fetch_range_into_session(session, chunk_start, chunk_end, source="foreground")
            except Exception as exc:
                self.logger.warning(
                    "streaming foreground fetch failed path=%s backend=%s chunk=[%d,%d): %r",
                    session.canonical_path,
                    session.backend.name,
                    chunk_start,
                    chunk_end,
                    exc,
                )
                with session.lock:
                    if session.error is None:
                        session.error = exc
                    session.condition.notify_all()
                self._update_streaming_state_xattr(session, "error")
                raise FuseOSError(errno.EIO)
            finally:
                with session.lock:
                    session.in_flight_chunks.discard(chunk_start)
                    session.condition.notify_all()

    def _close_streaming_session_resources(self, session: PrefetchSession):
        with session.lock:
            io_fd = session.io_fd
            session.io_fd = -1

        if io_fd >= 0:
            try:
                os.close(io_fd)
            except OSError:
                pass

    def _remove_prefetch_session(self, session: PrefetchSession):
        with self._prefetch_sessions_lock:
            current = self._prefetch_sessions.get(session.canonical_path)
            if current is session:
                self._prefetch_sessions.pop(session.canonical_path, None)

    def _discard_unregistered_prefetch_session(self, session: PrefetchSession):
        self._close_streaming_session_resources(session)
        try:
            session.partial_cache_path.unlink(missing_ok=True)
        except OSError:
            pass

    def _cleanup_prefetch_session(self, session: PrefetchSession, status: str):
        self._update_streaming_state_xattr(session, status)
        self._remove_prefetch_session(session)
        self._discard_unregistered_prefetch_session(session)
        self.cache_manager.mark_cache_size_dirty()

    def _cleanup_failed_session(self, session: PrefetchSession):
        self._cleanup_prefetch_session(session, "failed")

    def _cleanup_cancelled_session(self, session: PrefetchSession):
        self._cleanup_prefetch_session(session, "cancelled")

    def _finalize_streaming_session(self, session: PrefetchSession):
        with session.lock:
            if session.finalized or session.error is not None:
                return
            if not self._is_range_available(session.available_ranges, 0, session.file_size):
                return
            io_fd = session.io_fd

        if io_fd < 0:
            return

        try:
            os.fsync(io_fd)
            session.final_cache_path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(session.partial_cache_path, session.final_cache_path)
        except OSError as exc:
            with session.lock:
                session.error = exc
                session.condition.notify_all()
            return

        self.cache_manager.mark_cache_size_dirty()

        try:
            st = os.lstat(str(session.final_cache_path))
        except OSError:
            st = None

        if st is not None:
            metadata = self.state_manager.get_file_metadata(session.canonical_path)
            if not metadata:
                config = self._get_config(session.canonical_path)
                metadata = FileMetadata(
                    virtual_path=session.canonical_path,
                    size=st.st_size,
                    checksum=None,
                    last_access=time.time(),
                    last_modified=st.st_mtime,
                    sync_status=SyncStatus.CLEAN,
                    replica_count=config.replica_count,
                    recomputable=config.recomputable,
                    recompute_cmd=config.recompute_cmd,
                    encrypted=config.encrypted,
                    encryption_key_id=None,
                )
            else:
                metadata.size = st.st_size
                metadata.last_access = time.time()
                metadata.last_modified = st.st_mtime
                if metadata.sync_status != SyncStatus.DIRTY:
                    metadata.sync_status = SyncStatus.CLEAN
            self.state_manager.set_file_metadata(metadata)

            try:
                self.state_manager.add_file_location(
                    FileLocation(
                        virtual_path=session.canonical_path,
                        backend_name=session.backend.name,
                        checksum=metadata.checksum or "",
                        last_synced=time.time(),
                        is_cached=False,
                    )
                )
            except Exception:
                pass

            try:
                self.state_manager.add_file_location(
                    FileLocation(
                        virtual_path=session.canonical_path,
                        backend_name="cache",
                        checksum=metadata.checksum or "",
                        last_synced=time.time(),
                        is_cached=True,
                    )
                )
            except Exception:
                pass

        with session.lock:
            session.prefetch_complete = True
            session.finalized = True
            should_close = session.refcount == 0
            session.condition.notify_all()

        self._update_streaming_state_xattr(session, "complete")

        self._remove_prefetch_session(session)

        if should_close:
            self._close_streaming_session_resources(session)

    def _background_prefetch_worker(self, session: PrefetchSession):
        cursor = 0
        wrapped = False

        while True:
            with session.lock:
                if session.stop_requested or session.error is not None:
                    break

                if not session.has_foreground_read:
                    session.condition.wait(timeout=0.05)
                    continue

                if not wrapped:
                    desired_start = (session.last_foreground_offset // session.chunk_size) * session.chunk_size
                    if cursor < desired_start:
                        cursor = desired_start

                if cursor >= session.file_size:
                    missing = self._first_missing_range(session.available_ranges, 0, session.file_size)
                    if missing is None:
                        break
                    cursor = (missing[0] // session.chunk_size) * session.chunk_size
                    wrapped = True
                    continue

                chunk_start = cursor
                chunk_end = min(chunk_start + session.chunk_size, session.file_size)

                if self._is_range_available(session.available_ranges, chunk_start, chunk_end):
                    cursor = chunk_end
                    continue

                if chunk_start in session.in_flight_chunks:
                    session.condition.wait(timeout=0.05)
                    continue

                session.in_flight_chunks.add(chunk_start)

            try:
                fetched_end = self._fetch_range_into_session(session, chunk_start, chunk_end, source="background")
                cursor = max(fetched_end, chunk_start + 1)
            except Exception as exc:
                self.logger.warning(
                    "streaming background fetch failed path=%s backend=%s chunk=[%d,%d): %r",
                    session.canonical_path,
                    session.backend.name,
                    chunk_start,
                    chunk_end,
                    exc,
                )
                with session.lock:
                    if session.error is None:
                        session.error = exc
                    session.condition.notify_all()
                self._update_streaming_state_xattr(session, "error")
                break
            finally:
                with session.lock:
                    session.in_flight_chunks.discard(chunk_start)
                    session.condition.notify_all()

        self._finalize_streaming_session(session)

        with session.lock:
            has_error = session.error is not None
            no_readers = session.refcount == 0
            stopped = session.stop_requested
            finalized = session.finalized

        if has_error and no_readers:
            self._cleanup_failed_session(session)
        elif stopped and not finalized and no_readers:
            self._cleanup_cancelled_session(session)

    def _create_or_attach_prefetch_session(self, virtual_path: str, canonical_path: str) -> Optional[PrefetchSession]:
        with self._prefetch_sessions_lock:
            existing = self._prefetch_sessions.get(canonical_path)
            if existing:
                with existing.lock:
                    existing.refcount += 1
                self.logger.info("streaming attach path=%s backend=%s readers=%d", canonical_path, existing.backend.name, existing.refcount)
                return existing

            if len(self._prefetch_sessions) >= self._max_streaming_sessions:
                self.logger.warning(
                    "Streaming session limit reached (%d); falling back to full-file cache for %s",
                    self._max_streaming_sessions,
                    canonical_path,
                )
                return None

        backend = self._select_backend_for_read(canonical_path)
        if not backend:
            self.logger.debug("streaming session unavailable: no readable backend for %s", canonical_path)
            return None

        if not backend.supports_range_reads():
            self.logger.debug(
                "streaming session unavailable: backend %s lacks range-read support for %s",
                backend.name,
                canonical_path,
            )
            return None

        try:
            file_size = backend.get_size(canonical_path)
        except Exception as exc:
            self.logger.debug(
                "streaming session unavailable: failed to stat %s on %s: %s",
                canonical_path,
                backend.name,
                exc,
            )
            return None

        if file_size < 0:
            self.logger.debug("streaming session unavailable: invalid size %s for %s", file_size, canonical_path)
            return None

        if not self.cache_manager.ensure_space(file_size):
            self.logger.warning(
                "streaming session unavailable: insufficient cache space for %s (%d bytes)",
                canonical_path,
                file_size,
            )
            return None

        final_cache_path = self.cache_manager.get_cache_path(virtual_path)
        partial_cache_path = self._partial_cache_path(final_cache_path)
        final_cache_path.parent.mkdir(parents=True, exist_ok=True)

        io_fd = -1
        try:
            partial_cache_path.unlink(missing_ok=True)
            io_fd = os.open(str(partial_cache_path), os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
            os.ftruncate(io_fd, file_size)
        except OSError as exc:
            self.logger.debug(
                "streaming session unavailable: failed to prepare partial cache file %s: %s",
                partial_cache_path,
                exc,
            )
            if io_fd >= 0:
                os.close(io_fd)
            return None

        session = PrefetchSession(
            canonical_path=canonical_path,
            virtual_path=virtual_path,
            backend=backend,
            file_size=file_size,
            final_cache_path=final_cache_path,
            partial_cache_path=partial_cache_path,
            chunk_size=self._streaming_chunk_size,
            io_fd=io_fd,
            refcount=1,
        )

        with self._prefetch_sessions_lock:
            existing = self._prefetch_sessions.get(canonical_path)
            if existing:
                with existing.lock:
                    existing.refcount += 1
                self.logger.info("streaming attach path=%s backend=%s readers=%d", canonical_path, existing.backend.name, existing.refcount)
                self._discard_unregistered_prefetch_session(session)
                return existing
            if len(self._prefetch_sessions) >= self._max_streaming_sessions:
                self.logger.warning(
                    "Streaming session limit reached (%d) during registration; falling back to full-file cache for %s",
                    self._max_streaming_sessions,
                    canonical_path,
                )
                self._discard_unregistered_prefetch_session(session)
                return None
            self._prefetch_sessions[canonical_path] = session

        self.cache_manager.mark_cache_size_dirty()
        self._record_streaming_stat("streaming_sessions_started", 1)

        worker = threading.Thread(
            target=self._background_prefetch_worker,
            args=(session,),
            daemon=True,
            name=f"storage-prefetch-{canonical_path}",
        )
        self.logger.info(
            "streaming session started for %s via backend %s (size=%d, chunk=%d)",
            canonical_path,
            backend.name,
            file_size,
            session.chunk_size,
        )
        session.worker = worker
        worker.start()
        self._update_streaming_state_xattr(session, "awaiting-read")

        return session

    def _release_prefetch_session(self, session: PrefetchSession):
        with session.lock:
            if session.refcount > 0:
                session.refcount -= 1
            has_error = session.error is not None
            finalized = session.finalized
            no_readers = session.refcount == 0
            had_foreground_read = session.has_foreground_read

        worker = session.worker

        if no_readers and not finalized and not has_error and not had_foreground_read:
            with session.lock:
                session.stop_requested = True
                session.condition.notify_all()

            if worker and worker.is_alive():
                worker.join(timeout=2.0)
            if worker and worker.is_alive():
                return

            with self._prefetch_sessions_lock:
                still_registered = self._prefetch_sessions.get(session.canonical_path) is session

            if still_registered:
                with session.lock:
                    finalized = session.finalized
                    has_error = session.error is not None
                if not finalized and not has_error:
                    self._cleanup_cancelled_session(session)
            return

        if has_error and no_readers:
            if worker and worker.is_alive():
                worker.join(timeout=2.0)
            if worker and worker.is_alive():
                return
            with session.lock:
                finalized = session.finalized
            if not finalized:
                self._cleanup_failed_session(session)
                return

        if finalized and no_readers:
            self._close_streaming_session_resources(session)

    def _ensure_file_in_cache(self, virtual_path: str,
                              canonical_path: Optional[str] = None,
                              backend_name: Optional[str] = None) -> bool:
        """
        Ensure a file is in the cache, downloading or recomputing if necessary

        Returns:
            True if file is available in cache, False otherwise
        """
        canonical_path = canonical_path or virtual_path
        cache_path = self.cache_manager.get_cache_path(virtual_path)

        # Check if already cached
        if self.cache_manager.is_cached(virtual_path):
            if not backend_name:
                self.cache_manager.mark_accessed(virtual_path)
            return True

        # If a backend is explicitly requested (conflict suffix), bypass replication manager.
        if backend_name:
            backend = self.backends.get(backend_name)
            cache_path = self.cache_manager.get_cache_path(virtual_path)
            cache_path.parent.mkdir(parents=True, exist_ok=True)

            if backend and backend.exists(canonical_path):
                try:
                    backend.copy_to_local(canonical_path, cache_path)
                    return True
                except Exception:
                    pass

            # Graceful fallback when a cached copy already exists locally.
            if cache_path.exists() and cache_path.is_file():
                return True

            if backend and not backend.is_available():
                raise FuseOSError(errno.EHOSTUNREACH)

            return False

        # Try to download from a backend
        backend = self._select_backend_for_read(canonical_path)

        if backend:
            # Download to cache
            if self.cache_manager.cache_file(virtual_path, backend, source_virtual_path=canonical_path):
                # Populate state metadata and locations if missing
                cache_path = self.cache_manager.get_cache_path(virtual_path)
                try:
                    st = os.lstat(str(cache_path))
                except OSError:
                    return True

                metadata = self.state_manager.get_file_metadata(canonical_path)
                if not metadata:
                    config = self._get_config(canonical_path)
                    metadata = FileMetadata(
                        virtual_path=canonical_path,
                        size=st.st_size,
                        checksum=None,
                        last_access=time.time(),
                        last_modified=st.st_mtime,
                        sync_status=SyncStatus.CLEAN,
                        replica_count=config.replica_count,
                        recomputable=config.recomputable,
                        recompute_cmd=config.recompute_cmd,
                        encrypted=config.encrypted,
                        encryption_key_id=None,
                    )
                    self.state_manager.set_file_metadata(metadata)

                # Record locations
                try:
                    self.state_manager.add_file_location(
                        FileLocation(
                            virtual_path=canonical_path,
                            backend_name=backend.name,
                            checksum=metadata.checksum or '',
                            last_synced=time.time(),
                            is_cached=False,
                        )
                    )
                except Exception:
                    pass
                try:
                    self.state_manager.add_file_location(
                        FileLocation(
                            virtual_path=canonical_path,
                            backend_name="cache",
                            checksum=metadata.checksum or '',
                            last_synced=time.time(),
                            is_cached=True,
                        )
                    )
                except Exception:
                    pass

                return True

        # File not in cache or backends - try recomputation
        if self.recompute_engine.can_recompute(virtual_path, cache_path):
            # Recompute the file synchronously (blocks until complete)
            result = self.recompute_engine.recompute_file(virtual_path, cache_path, timeout=30.0)

            if result.success:
                # File was recomputed successfully
                self.cache_manager.mark_accessed(virtual_path)
                return True

        available_backends, _ = self._backend_availability()
        if not available_backends and self.backends:
            raise FuseOSError(errno.EHOSTUNREACH)

        # File not available
        return False

    # Filesystem methods

    def getattr(self, path, fh=None):
        """Get file attributes"""
        canonical_path, backend_name = self._resolve_backend_target(path)
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            st = os.lstat(str(cache_path))
            return dict((key, getattr(st, key)) for key in (
                'st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime',
                'st_nlink', 'st_size', 'st_uid'))
        except OSError as e:
            # Fallback: query backends for non-cached entries (e.g., remote-only files)
            backend_iter = []
            if backend_name:
                backend_iter = [(backend_name, self.backends.get(backend_name))]
            else:
                backend_iter = list(self.backends.items())

            for bname, backend in backend_iter:
                if not backend:
                    continue
                try:
                    st = backend.stat(canonical_path)
                    return {
                        'st_atime': getattr(st, 'st_atime', time.time()),
                        'st_ctime': getattr(st, 'st_ctime', getattr(st, 'st_mtime', time.time())),
                        'st_gid': getattr(st, 'st_gid', os.getgid()),
                        'st_mode': getattr(st, 'st_mode', stat.S_IFREG | 0o644),
                        'st_mtime': getattr(st, 'st_mtime', time.time()),
                        'st_nlink': getattr(st, 'st_nlink', 1),
                        'st_size': getattr(st, 'st_size', 0),
                        'st_uid': getattr(st, 'st_uid', os.getuid()),
                    }
                except Exception as be:
                    self.logger.debug("getattr backend fallback failed for %s on %s: %s", canonical_path, bname, be)

            available_backends, _ = self._backend_availability()
            if not available_backends and self.backends:
                raise FuseOSError(errno.EHOSTUNREACH)

            raise FuseOSError(e.errno)

    def readdir(self, path, fh):
        """Read directory contents"""
        now = time.monotonic()
        with self._dir_cache_lock:
            snapshot = self._readdir_snapshot.get(path)
        if snapshot and (now - snapshot[0]) <= self._readdir_snapshot_ttl:
            for entry in snapshot[1]:
                yield entry
            return

        cache_path = self.cache_manager.get_cache_path(path)

        entries: Set[str] = {'.', '..'}

        # Get from cache
        if cache_path.exists() and cache_path.is_dir():
            entries.update(
                entry for entry in os.listdir(str(cache_path))
                if not entry.endswith(self._PARTIAL_SUFFIX)
            )

        # Merge listings from backends; cache for fallback on connection issues
        with self._dir_cache_lock:
            dir_cache = self._dir_cache.setdefault(path, {})

            for backend_name, backend in self.backends.items():
                try:
                    attrs = backend.listdir_attrs(path)
                    if attrs is None:
                        backend_entries = set(backend.listdir(path))
                    else:
                        backend_entries = {name for name, _ in attrs}
                    entries.update(backend_entries)
                    dir_cache[backend_name] = backend_entries
                except Exception as exc:
                    cached = dir_cache.get(backend_name, set())
                    if cached:
                        entries.update(cached)
                    self.logger.debug(
                        "readdir fallback for %s on backend %s due to %s", path, backend_name, exc
                    )

        # Schedule checksum computation in background
        self._schedule_directory_checksums(path, dir_cache)

        # Apply conflict view using known checksums
        visible_entries = self._apply_conflict_view(path, entries)
        ordered_entries = sorted(visible_entries)

        with self._dir_cache_lock:
            self._readdir_snapshot[path] = (time.monotonic(), ordered_entries)

        for entry in ordered_entries:
            yield entry

    def mkdir(self, path, mode):
        """Create a directory"""
        canonical_path, backend_name = self._resolve_backend_target(path)
        cache_path = self.cache_manager.get_cache_path(canonical_path)

        try:
            cache_path.mkdir(parents=True, exist_ok=False)
            os.chmod(cache_path, mode)

            failures = self._propagate_dir_operation(DirOperation.MKDIR, canonical_path, backend_hint=backend_name)

            parent_dir = str(Path(canonical_path).parent)
            self._invalidate_dir_cache_entry(parent_dir)
            self._invalidate_dir_cache_entry(canonical_path)

            for failed_backend in failures:
                self.sync_manager.queue_dir_sync(
                    canonical_path,
                    DirOperation.MKDIR,
                    mode=mode,
                    backend_hint=failed_backend,
                    priority=80,
                    force=True,
                )

        except OSError as e:
            raise FuseOSError(e.errno)

    def rmdir(self, path):
        """Remove a directory"""
        canonical_path, backend_name = self._resolve_backend_target(path)
        cache_path = self.cache_manager.get_cache_path(canonical_path)

        try:
            cache_path.rmdir()

            failures = self._propagate_dir_operation(DirOperation.RMDIR, canonical_path, backend_hint=backend_name)

            parent_dir = str(Path(canonical_path).parent)
            self._invalidate_dir_cache_entry(parent_dir)
            self._invalidate_dir_cache_entry(canonical_path)

            for failed_backend in failures:
                self.sync_manager.queue_dir_sync(
                    canonical_path,
                    DirOperation.RMDIR,
                    backend_hint=failed_backend,
                    priority=80,
                    force=True,
                )

        except OSError as e:
            raise FuseOSError(e.errno)

    def unlink(self, path):
        """Remove a file"""
        canonical_path, backend_name = self._resolve_backend_target(path)
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            cache_path.unlink()
            self.cache_manager.mark_cache_size_dirty()

            parent_dir = str(Path(canonical_path).parent)
            self._invalidate_dir_cache_entry(parent_dir)
            self._invalidate_dir_cache_entry(canonical_path)

            # Remove from state tracking
            if not backend_name:
                self.state_manager.delete_file(canonical_path)
            backend = self.backends.get(backend_name) if backend_name else None
            if backend:
                try:
                    backend.delete_file(canonical_path)
                except Exception:
                    pass

        except OSError as e:
            raise FuseOSError(e.errno)

    def rename(self, old, new):
        """Rename a file or directory"""
        old_canonical, old_backend = self._resolve_backend_target(old)
        new_canonical, new_backend = self._resolve_backend_target(new)

        if old_backend != new_backend:
            raise FuseOSError(errno.EXDEV)

        old_cache = self.cache_manager.get_cache_path(old_canonical)
        new_cache = self.cache_manager.get_cache_path(new_canonical)

        try:
            new_cache.parent.mkdir(parents=True, exist_ok=True)
            old_cache.rename(new_cache)

            renamed_entries = self.state_manager.rename_path(old_canonical, new_canonical)

            is_directory = new_cache.is_dir()

            failures = self._propagate_dir_operation(
                DirOperation.RENAME,
                old_canonical,
                backend_hint=old_backend,
                new_path=new_canonical,
            )

            parent_old = str(Path(old_canonical).parent)
            parent_new = str(Path(new_canonical).parent)
            self._invalidate_dir_cache_entry(parent_old)
            self._invalidate_dir_cache_entry(parent_new)
            self._invalidate_dir_cache_entry(old_canonical)
            self._invalidate_dir_cache_entry(new_canonical)

            if renamed_entries:
                for _, updated in renamed_entries:
                    metadata = self.state_manager.get_file_metadata(updated)
                    if metadata and metadata.sync_status == SyncStatus.DIRTY:
                        self.sync_manager.queue_sync(updated, priority=80, force=True)

            for failed_backend in failures:
                self.sync_manager.queue_dir_sync(
                    old_canonical,
                    DirOperation.RENAME,
                    new_path=new_canonical,
                    backend_hint=failed_backend,
                    priority=90,
                    force=True,
                )

        except OSError as e:
            raise FuseOSError(e.errno)

    def chmod(self, path, mode):
        """Change file mode"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            os.chmod(cache_path, mode)
        except OSError as e:
            raise FuseOSError(e.errno)

    def chown(self, path, uid, gid):
        """Change file owner"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            os.chown(cache_path, uid, gid)
        except OSError as e:
            raise FuseOSError(e.errno)

    def truncate(self, path, length, fh=None):
        """Truncate a file"""
        canonical_path, backend_name = self._resolve_backend_target(path)
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            with open(cache_path, 'r+b') as f:
                f.truncate(length)
            self.cache_manager.mark_cache_size_dirty()

            # Mark as dirty
            if not backend_name:
                metadata = self.state_manager.get_file_metadata(canonical_path)
                if metadata:
                    metadata.sync_status = SyncStatus.DIRTY
                    self.state_manager.set_file_metadata(metadata)

        except OSError as e:
            raise FuseOSError(e.errno)

    # File operations

    def open(self, path, flags):
        """Open a file"""
        canonical_path, backend_name = self._resolve_backend_target(path)
        cache_path = self.cache_manager.get_cache_path(path)
        prefetch_session: Optional[PrefetchSession] = None

        # Streaming mode applies only to canonical read-only opens of uncached files.
        if (
            self._streaming_reads
            and backend_name is None
            and self._is_read_only_open(flags)
            and not self.cache_manager.is_cached(path)
        ):
            prefetch_session = self._create_or_attach_prefetch_session(path, canonical_path)
            if prefetch_session:
                cache_path = prefetch_session.partial_cache_path
                if not cache_path.exists() and prefetch_session.final_cache_path.exists():
                    # Session finished before open() reached os.open.
                    cache_path = prefetch_session.final_cache_path
            else:
                self._record_streaming_stat("streaming_fallback_opens", 1)
                self.logger.info("streaming fallback path=%s reason=unavailable-session", canonical_path)

        # Fallback path: preserve existing full-file cache semantics.
        if prefetch_session is None:
            if not self._ensure_file_in_cache(path, canonical_path, backend_name):
                raise FuseOSError(errno.ENOENT)
            cache_path = self.cache_manager.get_cache_path(path)

        try:
            try:
                real_fd = os.open(str(cache_path), flags)
            except FileNotFoundError:
                if prefetch_session is not None and prefetch_session.final_cache_path.exists():
                    cache_path = prefetch_session.final_cache_path
                    real_fd = os.open(str(cache_path), flags)
                else:
                    raise

            with self._fd_lock:
                vfd = self._fd_counter
                self._fd_counter += 1
                self._fd_map[vfd] = FDEntry(
                    virtual_path=path,
                    cache_path=cache_path,
                    real_fd=real_fd,
                    flags=flags,
                    backend_name=backend_name,
                    canonical_path=canonical_path,
                    prefetch_session=prefetch_session,
                )

            if backend_name is None:
                self.cache_manager.mark_accessed(path)

            return vfd

        except OSError as e:
            if prefetch_session is not None:
                self._release_prefetch_session(prefetch_session)
            raise FuseOSError(e.errno)

    def create(self, path, mode, fi=None):
        """Create a file"""
        canonical_path, backend_name = self._resolve_backend_target(path)
        cache_path = self.cache_manager.create_cached_file(path)

        try:
            real_fd = os.open(str(cache_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)

            with self._fd_lock:
                vfd = self._fd_counter
                self._fd_counter += 1
                self._fd_map[vfd] = FDEntry(
                    virtual_path=path,
                    cache_path=cache_path,
                    real_fd=real_fd,
                    flags=os.O_WRONLY,
                    backend_name=backend_name,
                    canonical_path=canonical_path,
                    prefetch_session=None,
                )

            config = self._get_config(canonical_path)
            metadata = FileMetadata(
                virtual_path=canonical_path,
                size=0,
                checksum=None,
                last_access=time.time(),
                last_modified=time.time(),
                sync_status=SyncStatus.DIRTY,
                replica_count=config.replica_count,
                recomputable=config.recomputable,
                recompute_cmd=config.recompute_cmd,
                encrypted=config.encrypted,
                encryption_key_id=None
            )
            self.state_manager.set_file_metadata(metadata)

            parent_dir = str(Path(canonical_path).parent)
            self._invalidate_dir_cache_entry(parent_dir)
            self._invalidate_dir_cache_entry(canonical_path)

            return vfd

        except OSError as e:
            raise FuseOSError(e.errno)

    def read(self, path, size, offset, fh):
        """Read from a file"""
        with self._fd_lock:
            entry = self._fd_map.get(fh)
            if entry is None:
                raise FuseOSError(errno.EBADF)

        session = entry.prefetch_session

        if session is not None:
            read_start = max(0, offset)
            read_end = min(session.file_size, offset + size)

            if read_start >= read_end:
                return b""

            self._ensure_streaming_range(session, read_start, read_end)

        try:
            if hasattr(os, "pread"):
                data = os.pread(entry.real_fd, size, offset)
            else:
                os.lseek(entry.real_fd, offset, os.SEEK_SET)
                data = os.read(entry.real_fd, size)

            if entry.backend_name is None:
                self.cache_manager.mark_accessed(path)

            return data

        except OSError as e:
            raise FuseOSError(e.errno)

    def write(self, path, data, offset, fh):
        """Write to a file"""
        with self._fd_lock:
            entry = self._fd_map.get(fh)
            if entry is None:
                raise FuseOSError(errno.EBADF)

        if entry.prefetch_session is not None:
            raise FuseOSError(errno.EROFS)

        try:
            os.lseek(entry.real_fd, offset, os.SEEK_SET)
            written = os.write(entry.real_fd, data)
            self.cache_manager.mark_cache_size_dirty()

            if entry.backend_name is None:
                self.cache_manager.mark_accessed(path)
                metadata = self.state_manager.get_file_metadata(entry.canonical_path)
                if metadata:
                    metadata.sync_status = SyncStatus.DIRTY
                    metadata.last_modified = time.time()
                    self.state_manager.set_file_metadata(metadata)

            return written

        except OSError as e:
            raise FuseOSError(e.errno)

    def release(self, path, fh):
        """Release (close) a file"""
        with self._fd_lock:
            entry = self._fd_map.pop(fh, None)

        if entry is None:
            return 0

        close_error: Optional[OSError] = None
        try:
            os.close(entry.real_fd)
        except OSError as exc:
            close_error = exc

        if entry.prefetch_session is not None:
            self._release_prefetch_session(entry.prefetch_session)
            if close_error is not None:
                raise FuseOSError(close_error.errno)
            return 0

        if close_error is not None:
            raise FuseOSError(close_error.errno)

        try:
            self.cache_manager.flush_file(path)

            if entry.backend_name:
                backend = self.backends.get(entry.backend_name)
                if backend:
                    try:
                        backend.copy_from_local(entry.cache_path, entry.canonical_path)
                        backend.sync()
                        checksum = backend.get_fast_checksum(entry.canonical_path)
                        ts = time.time()
                        self._record_checksum(entry.canonical_path, entry.backend_name, checksum, ts)
                        try:
                            yaml_blob = yaml.dump(
                                [f"{entry.backend_name}:{checksum}:{ts}"], default_flow_style=False
                            ).encode()
                            backend.set_xattr(entry.canonical_path, self.CHECKSUM_XATTR, yaml_blob)
                        except Exception:
                            pass
                    except Exception as exc:
                        self.logger.error(
                            "direct sync failed for %s on %s: %s",
                            entry.canonical_path,
                            entry.backend_name,
                            exc,
                        )
            else:
                metadata = self.state_manager.get_file_metadata(entry.canonical_path)
                if metadata and metadata.sync_status == SyncStatus.DIRTY:
                    self.sync_manager.queue_sync(entry.canonical_path, priority=100)

            return 0

        except OSError as e:
            raise FuseOSError(e.errno)

    def fsync(self, path, datasync, fh):
        """Sync file to disk"""
        with self._fd_lock:
            entry = self._fd_map.get(fh)
            if entry is None:
                return 0

        try:
            if datasync:
                os.fdatasync(entry.real_fd)
            else:
                os.fsync(entry.real_fd)
            return 0

        except OSError as e:
            raise FuseOSError(e.errno)

    # Extended attributes

    def getxattr(self, path, name, position=0):
        """Get extended attribute"""
        canonical_path, _ = self._resolve_backend_target(path)

        if name == self.CHECKSUM_XATTR:
            return self._aggregate_checksums_yaml(canonical_path)

        if name == self.CACHE_STATE_XATTR:
            payload = self._cache_state_payload(path, canonical_path)
            return json.dumps(payload, sort_keys=True).encode("utf-8")

        cache_path = self.cache_manager.get_cache_path(path)

        try:
            return os.getxattr(str(cache_path), name)
        except OSError as e:
            raise FuseOSError(e.errno)

    def setxattr(self, path, name, value, options, position=0):
        """Set extended attribute"""
        if name in {self.CHECKSUM_XATTR, self.CACHE_STATE_XATTR}:
            # Ignore manual writes for derived attrs.
            return
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            os.setxattr(str(cache_path), name, value)
        except OSError as e:
            raise FuseOSError(e.errno)

    def listxattr(self, path):
        """List extended attributes"""
        canonical_path, _ = self._resolve_backend_target(path)
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            attrs = set(os.listxattr(str(cache_path)))
        except OSError:
            attrs = set()

        # Expose checksum key if we have data
        with self._checksum_lock:
            has_checksum = any(
                key[0] == canonical_path for key in self._checksum_records.keys()
            )
        if has_checksum:
            attrs.add(self.CHECKSUM_XATTR)

        attrs.add(self.CACHE_STATE_XATTR)
        return list(attrs)

    def removexattr(self, path, name):
        """Remove extended attribute"""
        if name in {self.CHECKSUM_XATTR, self.CACHE_STATE_XATTR}:
            # Derived; ignore removals.
            return
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            os.removexattr(str(cache_path), name)
        except OSError as e:
            raise FuseOSError(e.errno)

    # Symbolic links

    def readlink(self, path):
        """Read symbolic link"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            return os.readlink(str(cache_path))
        except OSError as e:
            raise FuseOSError(e.errno)

    def symlink(self, target, source):
        """Create symbolic link"""
        cache_path = self.cache_manager.get_cache_path(source)

        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            os.symlink(target, str(cache_path))
        except OSError as e:
            raise FuseOSError(e.errno)

    # Time operations

    def utimens(self, path, times=None):
        """Update file timestamps"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            os.utime(str(cache_path), times)
        except OSError as e:
            raise FuseOSError(e.errno)

    def destroy(self, path):
        """Cleanup on unmount"""
        self._stop_control_server()

        with self._prefetch_sessions_lock:
            sessions = list(self._prefetch_sessions.values())
            self._prefetch_sessions.clear()

        for session in sessions:
            with session.lock:
                session.stop_requested = True
                session.condition.notify_all()

        for session in sessions:
            worker = session.worker
            if worker and worker.is_alive():
                worker.join(timeout=2.0)
            if worker and worker.is_alive():
                self.logger.warning(
                    "Streaming worker still alive during destroy; skipping forced close for %s",
                    session.canonical_path,
                )
                continue

            self._close_streaming_session_resources(session)
            if not session.finalized:
                try:
                    session.partial_cache_path.unlink(missing_ok=True)
                except OSError:
                    pass
        if sessions:
            self.cache_manager.mark_cache_size_dirty()

        try:
            self._checksum_executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

        self.sync_manager.stop()
        self.state_manager.close()


def _acquire_mount_lock(lock_path: Path, mountpoint: str):
    """Take an exclusive, non-blocking lock to prevent duplicate mounts."""
    lock_file = None
    try:
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        lock_file = os.fdopen(fd, 'r+')
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            try:
                existing = lock_file.read().strip()
            finally:
                lock_file.close()
            raise RuntimeError(
                f"Mount already active for {mountpoint}" + (f" (pid {existing})" if existing else "")
            )

        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        return lock_file
    except Exception:
        if lock_file:
            lock_file.close()
        raise


def mount_storage(mountpoint: str, cache_root: Path, state_db: Path,
                 backends: Dict[str, Backend], cache_size: int,
                 foreground: bool = False, num_sync_workers: int = 2,
                 allow_other: Optional[bool] = None,
                 debug: bool = False,
                 streaming_reads: bool = False,
                 streaming_chunk_size: int = 1024 * 1024,
                 max_streaming_sessions: int = 8):
    """
    Mount a Storage filesystem

    Args:
        mountpoint: Where to mount the filesystem
        cache_root: Root directory for cache
        state_db: Path to state database
        backends: Dictionary of backends
        cache_size: Maximum cache size in bytes
        foreground: Run in foreground (default: False)
        num_sync_workers: Number of background sync workers
        allow_other: Allow other users to access the mount (requires root or fuse.conf setting).
                     If None, auto-detects based on whether running as root.
        debug: Enable verbose debug logging
        streaming_reads: Enable uncached range streaming + background prefetch
        streaming_chunk_size: Chunk size used for remote range reads
        max_streaming_sessions: Maximum concurrent streaming prefetch sessions

    Raises:
        PermissionError: If allow_other=True but not running as root and user_allow_other not set
    """
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    is_root = os.geteuid() == 0

    # Auto-detect allow_other if not specified
    if allow_other is None:
        allow_other = is_root
        if not is_root:
            logger.warning("Not running as root - mounting with allow_other=False (only you can access)")

    # Validate allow_other setting
    if allow_other and not is_root:
        # Check if user_allow_other is set in /etc/fuse.conf
        try:
            with open('/etc/fuse.conf', 'r') as f:
                fuse_conf = f.read()
                if 'user_allow_other' not in fuse_conf:
                    raise PermissionError(
                        "allow_other=True requires either root privileges or 'user_allow_other' "
                        "to be set in /etc/fuse.conf"
                    )
        except FileNotFoundError:
            raise PermissionError(
                "allow_other=True requires either root privileges or 'user_allow_other' "
                "to be set in /etc/fuse.conf (file not found)"
            )

    # Prevent duplicate mounts on the same cache/mountpoint
    mount_hash = hashlib.sha256(str(mountpoint).encode('utf-8')).hexdigest()[:12]
    lock_path = cache_root / f'.storage.mount.{mount_hash}.lock'
    try:
        lock_file = _acquire_mount_lock(lock_path, mountpoint)
        logger.info("Acquired mount lock at %s for %s", lock_path, mountpoint)
    except RuntimeError as e:
        logger.error(str(e))
        raise

    fs = StorageFS(
        cache_root,
        state_db,
        backends,
        cache_size,
        num_sync_workers,
        start_sync=False,
        streaming_reads=streaming_reads,
        streaming_chunk_size=streaming_chunk_size,
        max_streaming_sessions=max_streaming_sessions,
    )
    try:
        FUSE(fs, mountpoint, nothreads=True, foreground=foreground, allow_other=allow_other)
    finally:
        try:
            lock_file.close()
        except Exception:
            logger.exception("Exception occurred while closing mount lock file")
