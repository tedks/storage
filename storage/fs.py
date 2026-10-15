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
import time
import hashlib
import yaml
import json
import concurrent.futures
from dataclasses import dataclass
from fuse import FUSE, FuseOSError, Operations
from pathlib import Path
from typing import Dict, Optional, Set, List, Tuple

from .backends.base import Backend
from .cache import CacheManager
from .cache_metadata import XATTR_CACHE_METADATA_NAME
from .file_config import FileConfig, get_inherited_config
from .models import FileLocation, FileMetadata, SyncStatus
from .state import StateManager
from .replication import ReplicationManager
from .sync import SyncManager
from .sync_tasks import DirOperation
from .recompute import RecomputeEngine
from .checksums import ChecksumRecord
from .pathing import ResolvedPath, resolve_path
from .namespace import NamespaceService
from .streaming import PrefetchSession, StreamingReadManager


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
    CACHE_METADATA_XATTR = XATTR_CACHE_METADATA_NAME
    _STUB_XATTR = "user.storage.stub"
    _PARTIAL_SUFFIX = ".storage.partial"
    _STREAM_STATE_XATTR = "user.storage.stream"
    # Internal artifacts that should be hidden from readdir listings.
    # Entries matching these exactly are filtered, plus anything ending
    # with _PARTIAL_SUFFIX.
    _HIDDEN_NAMES = frozenset({
        ".storage.rsync-tmp",      # rsync --partial-dir scratch directory
        ".storage.control.sock",   # FUSE control socket
    })
    _HIDDEN_PREFIXES = (
        ".storage.mount.",         # mount lock files (.storage.mount.*.lock)
    )

    @classmethod
    def _is_hidden_entry(cls, name: str) -> bool:
        """Return True if name is an internal artifact that should be hidden from listings."""
        if name in cls._HIDDEN_NAMES:
            return True
        if name.endswith(cls._PARTIAL_SUFFIX):
            return True
        for prefix in cls._HIDDEN_PREFIXES:
            if name.startswith(prefix):
                return True
        return False

    def __init__(self, cache_root: Path, state_db: Path, backends: Dict[str, Backend],
                 cache_size: int, num_sync_workers: int = 2, start_sync: bool = True,
                 streaming_reads: bool = False, streaming_chunk_size: int = 1024 * 1024,
                 max_streaming_sessions: int = 8,
                 readdir_ttl: float = 1.0,
                 enable_watchers: bool = False):
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
            readdir_ttl: Seconds to cache readdir results before re-querying
                         backends (pull-based invalidation). Ignored when
                         enable_watchers is True.
            enable_watchers: Use push-based cache invalidation via inotifywait
                             on SSH backends instead of TTL polling. When True,
                             readdir_ttl is ignored and directory listings are
                             only invalidated by remote change events. Requires
                             inotify-tools on the remote host.
        """
        self.state_manager = StateManager(str(state_db))
        self.cache_manager = CacheManager(cache_root, self.state_manager, cache_size)
        self.replication_manager = ReplicationManager(self.state_manager, backends)
        self.backends = backends

        self.logger = logging.getLogger(__name__)

        # File descriptor tracking
        self._fd_counter = 0
        self._fd_map: Dict[int, FDEntry] = {}
        self._fd_lock = threading.Lock()

        # Background sync manager
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            self.replication_manager,
            num_workers=num_sync_workers,
            should_sync_path_cb=self._should_sync_path,
        )
        self._sync_started = False
        if start_sync:
            self.sync_manager.start()
            self._sync_started = True

        # Recomputation engine
        self.recompute_engine = RecomputeEngine(self.state_manager)

        # Directory listing cache (per directory, per backend) for fallback when backend unavailable
        self._dir_cache: Dict[str, Dict[str, Set[str]]] = {}
        self._dir_cache_lock = threading.Lock()
        self._readdir_snapshot: Dict[str, Tuple[float, List[str]]] = {}

        # Invalidation strategy: push-based (watchers) or pull-based (TTL).
        # When watchers are enabled AND all storage members expose push
        # invalidation, TTL is infinite — only watcher callbacks invalidate
        # snapshots. If any member lacks that capability, TTL stays as fallback.
        self._enable_watchers = enable_watchers
        self._watchers = []
        all_watchable = backends and all(
            b.supports_push_invalidation() for b in backends.values()
        )
        if enable_watchers and all_watchable:
            self._readdir_snapshot_ttl = float('inf')
        elif enable_watchers and not all_watchable:
            # Mixed mount: push invalidation for capable members, TTL for the rest.
            self._readdir_snapshot_ttl = max(0.0, float(readdir_ttl))
        else:
            self._readdir_snapshot_ttl = max(0.0, float(readdir_ttl))

        # Per-backend checksum index used for conflict surfacing
        self._checksum_lock = threading.RLock()
        self._checksum_records: Dict[tuple, ChecksumRecord] = {}
        self._checksum_tasks: Dict[tuple, concurrent.futures.Future] = {}
        self._checksum_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

        self.namespace = NamespaceService(
            backends=self.backends,
            cache_manager=self.cache_manager,
            state_manager=self.state_manager,
            sync_manager=self.sync_manager,
            logger=self.logger,
            resolve_path=lambda path: self._resolve_path(path),
            schedule_checksum_task=lambda backend, path: self._schedule_checksum_task(backend, path),
            partial_suffix=self._PARTIAL_SUFFIX,
            is_hidden_entry=self._is_hidden_entry,
            dir_cache=self._dir_cache,
            dir_cache_lock=self._dir_cache_lock,
            readdir_snapshot=self._readdir_snapshot,
            readdir_snapshot_ttl=self._readdir_snapshot_ttl,
            checksum_records=self._checksum_records,
            checksum_lock=self._checksum_lock,
        )

        self._streaming_manager = StreamingReadManager(
            cache_manager=self.cache_manager,
            state_manager=self.state_manager,
            logger=self.logger,
            get_config=self._get_config,
            select_backend_for_read=self._select_backend_for_read,
            streaming_reads=streaming_reads,
            streaming_chunk_size=streaming_chunk_size,
            max_streaming_sessions=max_streaming_sessions,
            partial_suffix=self._PARTIAL_SUFFIX,
            stream_state_xattr=self._STREAM_STATE_XATTR,
        )
        self._streaming_reads = self._streaming_manager.streaming_reads
        self._streaming_chunk_size = self._streaming_manager.chunk_size
        self._max_streaming_sessions = self._streaming_manager.max_streaming_sessions
        self._prefetch_sessions = self._streaming_manager.prefetch_sessions
        self._prefetch_sessions_lock = self._streaming_manager.prefetch_sessions_lock

        self._cleanup_stale_partials()

    def init(self, path):
        """
        FUSE init hook (runs after mount, post-fork in background mode).

        Args:
            path: Mount path provided by FUSE (unused in this implementation).
        """
        self._streaming_manager.init()

        if not self._sync_started:
            self.sync_manager.start()
            self._sync_started = True

        if self._enable_watchers and not self._watchers:
            self._start_watchers()

    def _get_config(self, virtual_path: str) -> FileConfig:
        """Get configuration for a file"""
        cache_path = self.cache_manager.get_cache_path(virtual_path)
        return get_inherited_config(cache_path)

    def _should_sync_path(self, virtual_path: str) -> bool:
        """Only sync paths that do not currently have an open writer."""
        with self._fd_lock:
            for entry in self._fd_map.values():
                if entry.canonical_path != virtual_path:
                    continue
                if entry.prefetch_session is not None:
                    continue
                if (entry.flags & os.O_ACCMODE) != os.O_RDONLY:
                    return False
        return True

    def _backend_availability(self) -> Tuple[List[str], List[str]]:
        """Return backend names split by current availability."""
        return self.namespace.backend_availability()

    def _cache_state_payload(self, resolved: ResolvedPath) -> Dict[str, object]:
        """Build synthetic cache-state xattr payload for a virtual path."""
        return self.namespace.cache_state_payload(resolved)

    # ------------------------------------------------------------------ #
    # Conflict / checksum helpers
    # ------------------------------------------------------------------ #
    def _resolve_path(self, path: str) -> ResolvedPath:
        """Resolve a FUSE path into canonical and cache targets."""
        return resolve_path(path, self.backends.keys(), self.cache_manager.get_cache_path)

    def _invalidate_dir_cache_entry(self, path: str):
        """Remove a directory entry from the cached listing."""
        self.namespace.invalidate_dir_cache_entry(path)

    def _invalidate_file_cache(self, virtual_path: str):
        """
        Invalidate a cached file, forcing the next access to re-fetch
        from the backend.  Used by the remote watcher on push events.

        Handles both canonical paths (/foo) and backend-suffixed conflict
        paths (/foo.remote0).  For suffixed paths, metadata is stored under
        the canonical path, so we resolve it for the metadata lookup while
        still unlinking the cache at the suffixed path.

        Holds _fd_lock across the open-writer check and unlink so that
        no writer can open the file between the check and the delete.
        """
        cache_path = self.cache_manager.get_cache_path(virtual_path)
        if not (cache_path.exists() and cache_path.is_file()):
            return

        # Resolve canonical path for metadata lookup.  Suffixed conflict
        # paths (e.g. /foo.remote0) store metadata under the canonical
        # path (/foo), so we need to resolve it.
        resolved = self._resolve_path(virtual_path)
        canonical_path = resolved.canonical_path

        with self._fd_lock:
            for entry in self._fd_map.values():
                # Check both the suffixed virtual_path and canonical_path
                # since open() may use either.
                if entry.virtual_path != virtual_path and entry.canonical_path != canonical_path:
                    continue
                if entry.prefetch_session is not None:
                    continue
                if (entry.flags & os.O_ACCMODE) != os.O_RDONLY:
                    return

            # For canonical paths, check metadata directly.
            # For suffixed paths, check the canonical metadata.
            meta = self.state_manager.get_file_metadata(canonical_path)
            if meta and meta.sync_status != SyncStatus.CLEAN:
                return

            try:
                cache_path.unlink()
                self.state_manager.remove_file_location(virtual_path, "cache")
                self.cache_manager.mark_cache_size_dirty()
                if meta:
                    meta.checksum = None
                    meta.size = 0
                    meta.last_modified = 0.0
                    self.state_manager.set_file_metadata(meta)
            except OSError:
                pass

        # Clear in-memory checksum records and cancel pending tasks.
        # Records are keyed (canonical_path, backend), so clear both the
        # virtual_path (which may be suffixed) and the canonical_path to
        # cover conflict-view caches like /foo.remote0 whose checksums
        # are stored under /foo.
        invalidate_paths = {virtual_path, canonical_path}
        with self._checksum_lock:
            stale_keys = [k for k in self._checksum_records if k[0] in invalidate_paths]
            for k in stale_keys:
                self._checksum_records.pop(k, None)
            # Cancel pending checksum tasks so they don't re-insert
            # stale results after we just cleared them.
            stale_tasks = [k for k in self._checksum_tasks if k[0] in invalidate_paths]
            for k in stale_tasks:
                future = self._checksum_tasks.pop(k, None)
                if future is not None:
                    future.cancel()

    def _on_remote_change(self, virtual_path: str):
        """
        Callback from RemoteWatcher when inotifywait detects a change.

        Invalidates the canonical path AND any backend-suffixed conflict
        variants (e.g. /foo.remote0, /foo.remote1) so that cached
        conflict views are also refreshed.
        """
        self._invalidate_dir_cache_entry(virtual_path)
        self._invalidate_file_cache(virtual_path)
        # Also invalidate backend-suffixed conflict caches.
        # When /foo exists on multiple backends with different content,
        # the FUSE layer exposes /foo.remote0, /foo.remote1 etc.
        # These are cached under the suffixed path, so a remote change
        # to /foo must also invalidate /foo.<backend> for each backend.
        for backend_name in self.backends:
            suffixed = f"{virtual_path}.{backend_name}"
            self._invalidate_file_cache(suffixed)

    def _start_watchers(self):
        """Start push-invalidation watchers, coalescing members by watch scope."""
        from .watcher import RemoteWatcher

        # Group watch-capable members by connection identity so members on the
        # same host share a single SSH+inotifywait process.
        host_groups: dict[tuple, list[tuple[str, str]]] = {}
        host_meta: dict[tuple, dict] = {}
        for name, backend in self.backends.items():
            watcher_spec = backend.push_invalidation_spec()
            if watcher_spec is None:
                continue
            key = watcher_spec.group_key
            host_groups.setdefault(key, []).append((name, watcher_spec.remote_path))
            host_meta[key] = {
                "ssh_host": watcher_spec.ssh_host,
                "ssh_port": watcher_spec.ssh_port,
                "username": watcher_spec.username,
                "key_filename": watcher_spec.key_filename,
            }

        for key, backends_on_host in host_groups.items():
            names = [b[0] for b in backends_on_host]
            paths = [b[1] for b in backends_on_host]
            meta = host_meta[key]
            label = ",".join(names)

            watcher = RemoteWatcher(
                backend_name=label,
                ssh_host=meta["ssh_host"],
                ssh_port=meta["ssh_port"],
                remote_paths=paths,
                on_change=self._on_remote_change,
                username=meta["username"],
                key_filename=meta["key_filename"],
            )
            watcher.start()
            self._watchers.append(watcher)
            self.logger.info(
                "Started watcher for %s (%d paths on %s)",
                label, len(paths), meta["ssh_host"],
            )

    def _stop_watchers(self):
        """Stop all remote watchers."""
        for watcher in self._watchers:
            watcher.stop()
        self._watchers.clear()

    def _get_target_backends(self, backend_hint: Optional[str],
                             backend_names: Optional[Set[str]] = None) -> List[Backend]:
        """Return the backend instances affected by an operation."""
        return self.namespace._get_target_backends(backend_hint, backend_names=backend_names)

    def _propagate_dir_operation(self, operation: DirOperation, path: str,
                                 backend_hint: Optional[str] = None,
                                 new_path: Optional[str] = None,
                                 backend_names: Optional[Set[str]] = None) -> List[str]:
        """Best-effort synchronous propagation; returns backend names that failed."""
        return self.namespace._propagate_dir_operation(
            operation,
            path,
            backend_hint=backend_hint,
            new_path=new_path,
            backend_names=backend_names,
        )

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
        self.namespace._schedule_directory_checksums(dir_path, backend_entries)

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
        return self.namespace._apply_conflict_view(dir_path, base_entries)

    def _cleanup_stale_partials(self):
        self._streaming_manager.cleanup_stale_partials()

    @staticmethod
    def _is_read_only_open(flags: int) -> bool:
        write_bits = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC
        return (flags & write_bits) == 0

    def _partial_cache_path(self, cache_path: Path) -> Path:
        return self._streaming_manager._partial_cache_path(cache_path)

    def _select_backend_for_read(self, canonical_path: str) -> Optional[Backend]:
        """Pick a readable backend, preferring existing replication metadata."""
        backend = self.replication_manager.get_backend_for_read(canonical_path)
        if backend:
            return backend

        available = [b for b in self.backends.values() if b.is_available()]
        if available:
            candidates = available
        else:
            # Opportunistic reprobe when all backends are marked unavailable.
            # Member cooldown logic prevents tight reconnect loops.
            candidates = [b for b in self.backends.values() if b.supports_reconnect_probe()]

        candidates.sort(key=lambda b: b.speed_priority)
        for candidate in candidates:
            try:
                if candidate.exists(canonical_path):
                    return candidate
            except Exception:
                continue
        return None

    @staticmethod
    def _is_range_available(ranges: List[Tuple[int, int]], start: int, end: int) -> bool:
        return StreamingReadManager._is_range_available(ranges, start, end)

    @staticmethod
    def _first_missing_range(ranges: List[Tuple[int, int]], start: int, end: int) -> Optional[Tuple[int, int]]:
        return StreamingReadManager._first_missing_range(ranges, start, end)

    @staticmethod
    def _add_available_range(ranges: List[Tuple[int, int]], start: int, end: int):
        StreamingReadManager._add_available_range(ranges, start, end)

    @staticmethod
    def _contiguous_available_prefix(ranges: List[Tuple[int, int]], file_size: int) -> int:
        return StreamingReadManager._contiguous_available_prefix(ranges, file_size)

    def _snapshot_streaming_state(self, session: PrefetchSession, status: str) -> Dict[str, object]:
        return self._streaming_manager._snapshot_streaming_state(session, status)

    def _log_streaming_state_change(self, session: PrefetchSession, payload: Dict[str, object]):
        self._streaming_manager._log_streaming_state_change(session, payload)

    def _update_streaming_state_xattr(self, session: PrefetchSession, status: str):
        self._streaming_manager._update_streaming_state_xattr(session, status)

    def _streaming_stats_snapshot(self) -> Dict[str, object]:
        return self._streaming_manager._streaming_stats_snapshot()

    def _handle_control_request(self, request: str) -> Dict[str, object]:
        return self._streaming_manager._handle_control_request(request)

    def _control_server_loop(self):
        self._streaming_manager._control_server_loop()

    def _start_control_server(self):
        self._streaming_manager._start_control_server()

    def _stop_control_server(self):
        self._streaming_manager._stop_control_server()

    def _record_streaming_stat(self, key: str, value: int = 1):
        self._streaming_manager._record_streaming_stat(key, value)

    def _write_session_bytes(self, session: PrefetchSession, offset: int, data: bytes):
        self._streaming_manager._write_session_bytes(session, offset, data)

    def _fetch_range_into_session(self, session: PrefetchSession, start: int, end: int, source: str) -> int:
        return self._streaming_manager._fetch_range_into_session(session, start, end, source)

    def _ensure_streaming_range(self, session: PrefetchSession, start: int, end: int):
        self._streaming_manager._ensure_streaming_range(session, start, end)

    def _close_streaming_session_resources(self, session: PrefetchSession):
        self._streaming_manager._close_streaming_session_resources(session)

    def _remove_prefetch_session(self, session: PrefetchSession):
        self._streaming_manager._remove_prefetch_session(session)

    def _discard_unregistered_prefetch_session(self, session: PrefetchSession):
        self._streaming_manager._discard_unregistered_prefetch_session(session)

    def _cleanup_prefetch_session(self, session: PrefetchSession, status: str):
        self._streaming_manager._cleanup_prefetch_session(session, status)

    def _promote_failed_session_cache_prefix(self, session: PrefetchSession) -> bool:
        return self._streaming_manager._promote_failed_session_cache_prefix(session)

    def _cleanup_failed_session(self, session: PrefetchSession):
        self._streaming_manager._cleanup_failed_session(session)

    def _cleanup_cancelled_session(self, session: PrefetchSession):
        self._streaming_manager._cleanup_cancelled_session(session)

    def _finalize_streaming_session(self, session: PrefetchSession):
        self._streaming_manager._finalize_streaming_session(session)

    def _background_prefetch_worker(self, session: PrefetchSession):
        self._streaming_manager._background_prefetch_worker(session)

    def _create_or_attach_prefetch_session(self, virtual_path: str, canonical_path: str) -> Optional[PrefetchSession]:
        return self._streaming_manager._create_or_attach_prefetch_session(
            virtual_path,
            canonical_path,
        )

    def _release_prefetch_session(self, session: PrefetchSession):
        self._streaming_manager._release_prefetch_session(session)

    def _ensure_file_in_cache(self, resolved: ResolvedPath) -> bool:
        """
        Ensure a file is in the cache, downloading or recomputing if necessary

        Returns:
            True if file is available in cache, False otherwise
        """
        canonical_path = resolved.canonical_path
        cache_path = resolved.cache_path

        # Check if already cached
        if self.cache_manager.is_cached(resolved.requested_path):
            if not resolved.backend_name:
                self.cache_manager.mark_accessed(resolved.requested_path)
            return True

        # If a backend is explicitly requested (conflict suffix), bypass replication manager.
        if resolved.backend_name:
            backend = self.backends.get(resolved.backend_name)
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
            if self.cache_manager.cache_file(
                resolved.requested_path,
                backend,
                source_virtual_path=canonical_path,
            ):
                # Populate state metadata and locations if missing
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

                self.cache_manager.sync_cache_metadata(
                    canonical_path,
                    metadata,
                    cache_path=cache_path,
                )

                return True

        # File not in cache or backends - try recomputation
        if self.recompute_engine.can_recompute(resolved.requested_path, cache_path):
            # Recompute the file synchronously (blocks until complete)
            result = self.recompute_engine.recompute_file(
                resolved.requested_path,
                cache_path,
                timeout=30.0,
            )

            if result.success:
                # File was recomputed successfully
                self.cache_manager.mark_accessed(resolved.requested_path)
                return True

        available_backends, _ = self._backend_availability()
        if not available_backends and self.backends:
            raise FuseOSError(errno.EHOSTUNREACH)

        # File not available
        return False

    # Filesystem methods

    def getattr(self, path, fh=None):
        """Get file attributes"""
        return self.namespace.getattr(path)

    def readdir(self, path, fh):
        """Read directory contents"""
        for entry in self.namespace.readdir(path):
            yield entry

    def mkdir(self, path, mode):
        """Create a directory"""
        self.namespace.mkdir(path, mode)

    def rmdir(self, path):
        """Remove a directory"""
        self.namespace.rmdir(path)

    def unlink(self, path):
        """Remove a file"""
        self.namespace.unlink(path)

    def rename(self, old, new):
        """Rename a file or directory"""
        self.namespace.rename(old, new)

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

    def _mark_cache_file_dirty(self, canonical_path: str, cache_path: Path) -> None:
        """Record a modification of a cached file: DIRTY, new size and mtime, new generation.

        One UPDATE on the index row, so a sync that is in flight for the old
        content sees the generation move and does not report it as synced.
        The row is then mirrored into the cache file's metadata xattr.
        """
        st = os.lstat(str(cache_path))
        generation = self.state_manager.mark_dirty(
            canonical_path, size=st.st_size, last_modified=st.st_mtime,
        )
        if generation is None:
            return
        metadata = self.state_manager.get_file_metadata(canonical_path)
        if metadata:
            self.cache_manager.sync_cache_metadata(canonical_path, metadata, cache_path=cache_path)

    def truncate(self, path, length, fh=None):
        """Truncate a file"""
        resolved = self._resolve_path(path)
        cache_path = resolved.cache_path

        try:
            with open(cache_path, 'r+b') as f:
                f.truncate(length)
            self.cache_manager.mark_cache_size_dirty()

            # Mark as dirty
            if not resolved.backend_name:
                self._mark_cache_file_dirty(resolved.canonical_path, cache_path)

        except OSError as e:
            raise FuseOSError(e.errno)

    # File operations

    def open(self, path, flags):
        """Open a file"""
        resolved = self._resolve_path(path)
        cache_path = resolved.cache_path
        prefetch_session: Optional[PrefetchSession] = None

        # Streaming mode applies only to canonical read-only opens of uncached files.
        if (
            self._streaming_reads
            and resolved.backend_name is None
            and self._is_read_only_open(flags)
            and not self.cache_manager.is_cached(resolved.requested_path)
        ):
            prefetch_session = self._create_or_attach_prefetch_session(
                resolved.requested_path,
                resolved.canonical_path,
            )
            if prefetch_session:
                cache_path = prefetch_session.partial_cache_path
                if not cache_path.exists() and prefetch_session.final_cache_path.exists():
                    # Session finished before open() reached os.open.
                    cache_path = prefetch_session.final_cache_path
            else:
                self._record_streaming_stat("streaming_fallback_opens", 1)
                self.logger.info(
                    "streaming fallback path=%s reason=unavailable-session",
                    resolved.canonical_path,
                )

        # Fallback path: preserve existing full-file cache semantics.
        if prefetch_session is None:
            if not self._ensure_file_in_cache(resolved):
                raise FuseOSError(errno.ENOENT)
            cache_path = resolved.cache_path

        try:
            # Hold _fd_lock across os.open AND _fd_map registration so that
            # _invalidate_file_cache (which also holds _fd_lock) cannot
            # unlink the cache file between os.open and registration.
            with self._fd_lock:
                try:
                    real_fd = os.open(str(cache_path), flags)
                except FileNotFoundError:
                    if prefetch_session is not None and prefetch_session.final_cache_path.exists():
                        cache_path = prefetch_session.final_cache_path
                        real_fd = os.open(str(cache_path), flags)
                    else:
                        raise

                vfd = self._fd_counter
                self._fd_counter += 1
                self._fd_map[vfd] = FDEntry(
                    virtual_path=resolved.requested_path,
                    cache_path=cache_path,
                    real_fd=real_fd,
                    flags=flags,
                    backend_name=resolved.backend_name,
                    canonical_path=resolved.canonical_path,
                    prefetch_session=prefetch_session,
                )

            if resolved.backend_name is None:
                self.cache_manager.mark_accessed(resolved.requested_path)

            return vfd

        except OSError as e:
            if prefetch_session is not None:
                self._release_prefetch_session(prefetch_session)
            raise FuseOSError(e.errno)

    def create(self, path, mode, fi=None):
        """Create a file"""
        resolved = self._resolve_path(path)
        cache_path = self.cache_manager.create_cached_file(resolved.requested_path)

        try:
            with self._fd_lock:
                real_fd = os.open(str(cache_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
                vfd = self._fd_counter
                self._fd_counter += 1
                self._fd_map[vfd] = FDEntry(
                    virtual_path=resolved.requested_path,
                    cache_path=cache_path,
                    real_fd=real_fd,
                    flags=os.O_WRONLY,
                    backend_name=resolved.backend_name,
                    canonical_path=resolved.canonical_path,
                    prefetch_session=None,
                )

            config = self._get_config(resolved.canonical_path)
            metadata = FileMetadata(
                virtual_path=resolved.canonical_path,
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
            self._mark_cache_file_dirty(resolved.canonical_path, cache_path)

            parent_dir = resolved.parent_dir
            self._invalidate_dir_cache_entry(parent_dir)
            self._invalidate_dir_cache_entry(resolved.canonical_path)

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
                self._mark_cache_file_dirty(entry.canonical_path, entry.cache_path)

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
        resolved = self._resolve_path(path)

        if name == self.CHECKSUM_XATTR:
            return self._aggregate_checksums_yaml(resolved.canonical_path)

        if name == self.CACHE_STATE_XATTR:
            payload = self._cache_state_payload(resolved)
            return json.dumps(payload, sort_keys=True).encode("utf-8")

        try:
            return os.getxattr(str(resolved.cache_path), name)
        except OSError as e:
            raise FuseOSError(e.errno)

    def setxattr(self, path, name, value, options, position=0):
        """Set extended attribute"""
        if name in {self.CHECKSUM_XATTR, self.CACHE_STATE_XATTR, self.CACHE_METADATA_XATTR}:
            # Ignore manual writes for derived attrs.
            return
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            os.setxattr(str(cache_path), name, value)
        except OSError as e:
            raise FuseOSError(e.errno)

    def listxattr(self, path):
        """List extended attributes"""
        resolved = self._resolve_path(path)

        try:
            attrs = set(os.listxattr(str(resolved.cache_path)))
        except OSError:
            attrs = set()

        # Expose checksum key if we have data
        with self._checksum_lock:
            has_checksum = any(
                key[0] == resolved.canonical_path for key in self._checksum_records.keys()
            )
        if has_checksum:
            attrs.add(self.CHECKSUM_XATTR)

        attrs.add(self.CACHE_STATE_XATTR)
        return list(attrs)

    def removexattr(self, path, name):
        """Remove extended attribute"""
        if name in {self.CHECKSUM_XATTR, self.CACHE_STATE_XATTR, self.CACHE_METADATA_XATTR}:
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
        self._streaming_manager.destroy()
        self._stop_watchers()

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
                 max_streaming_sessions: int = 8,
                 readdir_ttl: float = 1.0,
                 enable_watchers: bool = False):
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
        readdir_ttl=readdir_ttl,
        enable_watchers=enable_watchers,
    )
    try:
        FUSE(fs, mountpoint, nothreads=True, foreground=foreground, allow_other=allow_other)
    finally:
        try:
            lock_file.close()
        except Exception:
            logger.exception("Exception occurred while closing mount lock file")
