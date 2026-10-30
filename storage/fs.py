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
import stat
import threading
import logging
import fcntl
import time
import hashlib
import math
import yaml
import json
import concurrent.futures
import contextlib
import signal
from dataclasses import dataclass, field
from fuse import FUSE, FuseOSError, Operations
from pathlib import Path
from typing import Dict, Optional, Set, List, Tuple

from .backends.base import Backend, ChecksumUnavailable, interactive_request
from .backends.ssh_mux import close_shared_control_sockets
from .cache import PARTIAL_SUFFIX, CacheManager
from .locks import NamespaceLock, PathLocks, UploadLocks
from .cache_metadata import XATTR_CACHE_METADATA_NAME, scan_cache_entries
from .file_config import FileConfig, get_inherited_config
from .models import FileLocation, FileMetadata, StreamingKeepAfterClose, SyncStatus
from .state import StateManager
from .replication import ReplicationManager
from .sync import SyncManager
from .sync_tasks import DirOperation
from .recompute import RecomputeEngine
from .checksum_queue import DEFAULT_CHECKSUM_REVERIFY_INTERVAL, MemberHashQueues
from .checksums import ChecksumBasis, ChecksumRecord
from .pathing import ResolvedPath, resolve_path
from .namespace import DEFAULT_LISTING_MAX_STALE, NamespaceService
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


@dataclass(frozen=True)
class MemberCheck:
    """What is known about a cached file's members, and since when.

    cache_identity is the cache file's (st_ino, st_size, st_mtime_ns) when the
    check was recorded; a check whose identity no longer matches the file
    describes some earlier copy. members maps a member name to its
    (st_size, st_mtime) when the cache file was known to match it: the
    baseline a later check compares against. verified_at is time.monotonic()
    of the last check (or fetch).
    """

    cache_identity: Tuple[int, int, int]
    verified_at: float
    members: Dict[str, Tuple[int, float]] = field(default_factory=dict)


class StorageFS(Operations):
    """
    Storage FUSE filesystem

    Implements a filesystem that multiplexes over multiple backing filesystems
    with local caching, replication, and background sync.
    """

    CHECKSUM_XATTR = "user.storage.checksums"
    # On a directory: setting it hashes the directory's same-size names on
    # several members again; reading it reports how far that has got
    # (storage conflicts --verify). Not listed by listxattr.
    VERIFY_XATTR = "user.storage.verify"
    # How long destroy() waits for an in-flight post-crash sweep.
    SWEEP_JOIN_TIMEOUT = 60.0
    CACHE_STATE_XATTR = "user.storage.cache"
    CACHE_METADATA_XATTR = XATTR_CACHE_METADATA_NAME
    _STUB_XATTR = "user.storage.stub"
    _PARTIAL_SUFFIX = PARTIAL_SUFFIX
    _STREAM_STATE_XATTR = "user.storage.stream"
    # Member baselines for revalidation on open (see _store_member_check).
    MEMBER_STAT_XATTR = "user.storage.member_stat"
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

    def __call__(self, op, *args):
        # Every FUSE operation has a caller waiting on it: members with a
        # connection pool serve it from the channel kept for that.
        with interactive_request():
            return super().__call__(op, *args)

    def __init__(self, cache_root: Path, state_db: Path, backends: Dict[str, Backend],
                 cache_size: int, num_sync_workers: int = 2, start_sync: bool = True,
                 streaming_reads: bool = False, streaming_chunk_size: int = 1024 * 1024,
                 max_streaming_sessions: int = 8,
                 readdir_ttl: Optional[float] = None,
                 enable_watchers: bool = False,
                 streaming_keep_after_close: Optional[StreamingKeepAfterClose] = None,
                 revalidate_after: float = 60.0,
                 listing_max_stale: float = DEFAULT_LISTING_MAX_STALE,
                 checksum_reverify_interval: float = DEFAULT_CHECKSUM_REVERIFY_INTERVAL):
        """
        Initialize Storage filesystem

        Args:
            cache_root: Root directory for local cache
            state_db: Path to SQLite state database
            backends: Dictionary of backend name -> Backend instance
            cache_size: Maximum cache size in bytes
            num_sync_workers: Number of background sync worker threads
            streaming_reads: Enable uncached range streaming + background prefetch
            streaming_chunk_size: Unit of background read-ahead (foreground
                reads fetch only what the reader asked for); read-ahead
                starts once readers have read one chunk
            max_streaming_sessions: Maximum concurrent streaming sessions
            streaming_keep_after_close: When read-ahead outlives the last
                close of a streaming session; None for the default, 4 chunks
                or 50% of the file
            readdir_ttl: Seconds a directory listing is fresh: it answers
                         readdir, getattr of its entries and lookups of
                         names it lacks without asking a member. None
                         follows revalidate_after. Ignored when watchers
                         cover every member (listings then stay until
                         invalidated).
            enable_watchers: Use push-based cache invalidation via inotifywait
                             on SSH backends instead of TTL polling. When True,
                             readdir_ttl is ignored and directory listings are
                             only invalidated by remote change events. Requires
                             inotify-tools on the remote host.
            revalidate_after: Seconds after which opening a CLEAN cached
                file stats the member holding it, and refetches it if the
                member's copy changed (0: every open)
            listing_max_stale: Seconds after which a listing past its fresh
                window is no longer served while a background refresh
                runs, but replaced by listing every member at once
            checksum_reverify_interval: Seconds between re-verifications
                of persisted member hashes: each interval, every member
                whose hash worker is idle hashes again the one file whose
                persisted hash was checked longest ago (and over
                CHECKSUM_REVERIFY_MIN_AGE ago); 0 turns it off
        """
        self.state_manager = StateManager(str(state_db))
        self.cache_manager = CacheManager(cache_root, self.state_manager, cache_size)
        self.cache_manager.descriptor_guard = self._descriptor_guard
        self.replication_manager = ReplicationManager(self.state_manager, backends)
        self.backends = backends

        self.logger = logging.getLogger(__name__)

        # File descriptor tracking
        self._fd_counter = 0
        self._fd_map: Dict[int, FDEntry] = {}
        self._fd_lock = threading.Lock()

        # The outer locks for a multithreaded FUSE loop; storage/locks.py
        # has the full lock order. Under nothreads they are never contended.
        self._namespace_lock = NamespaceLock()
        self._path_locks = PathLocks()
        # Held by a sync upload; rename and unlink wait for it (locks.py).
        self._upload_locks = UploadLocks()

        # Revalidation on open, keyed by canonical path (see
        # _revalidate_cached_file). Check times are in memory only: after a
        # restart every cached file is checked at its first open; baselines
        # also live in a cache-file xattr. _member_checks_lock is a leaf:
        # nothing is taken under it, though it is held across local
        # getxattr/setxattr calls, and eviction takes it while holding the
        # cache, index and descriptor locks.
        self._revalidate_after = max(0.0, float(revalidate_after))
        self._member_checks: Dict[str, MemberCheck] = {}
        self._member_checks_lock = threading.Lock()
        # Members whose failed revalidation has been logged; cleared by the
        # member's next successful stat, so an outage is logged once.
        # Deliberately unlocked: with fuse_threads a race costs at most a
        # duplicate or missed log line.
        self._revalidate_outages: Set[str] = set()
        # A check that found a change pins the refetch to the member it
        # checked (consumed by _select_backend_for_read). If that member is
        # gone by then and another supplies the bytes, its name is kept in
        # _recheck so its next check refetches again instead of adopting the
        # change as a baseline. Both under _member_checks_lock.
        self._refetch_pins: Dict[str, Backend] = {}
        self._recheck: Dict[str, Set[str]] = {}

        # Background sync manager
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            self.replication_manager,
            num_workers=num_sync_workers,
            should_sync_path_cb=self._should_sync_path,
            member_written_cb=self._record_member_written,
            upload_guard=self._upload_locks.hold_for_upload,
            path_guard=self._path_locked,
        )
        self._sync_started = False
        if start_sync:
            self.sync_manager.start()
            self._sync_started = True
        # Set by mount_storage. A clean unmount blanks the pid in it; a pid
        # still there at the next mount means this session did not finish.
        self.mount_lock_file = None
        # True from mount until sweep_stale_cache_metadata has completed
        # after an unclean shutdown. While True the pid stays in the lock
        # file, so an interrupted sweep is rerun by the next mount.
        self._sweep_pending = False
        self._sweep_thread: Optional[threading.Thread] = None

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
        else:
            # Watchers off, or a mixed mount (push invalidation for capable
            # members, the fresh window for the rest).
            self._readdir_snapshot_ttl = listing_fresh_window(readdir_ttl, revalidate_after)

        # Per-backend checksum index used for conflict surfacing
        self._checksum_lock = threading.RLock()
        self._checksum_records: Dict[tuple, ChecksumRecord] = {}
        self._checksum_tasks: Dict[tuple, concurrent.futures.Future] = {}
        # One low-priority worker per member, newest first, bounded (see
        # checksum_queue); threads start on first use, after the fork.
        self._checksum_queues = MemberHashQueues(logger=self.logger)
        # Re-verification of persisted hashes (_reverify_tick), on a thread
        # started by init(), after the daemonizing fork.
        # Clamped to what Event.wait accepts.
        self._checksum_reverify_interval = min(max(0.0, float(checksum_reverify_interval)),
                                               threading.TIMEOUT_MAX)
        # {member: {path: monotonic time}}: rows a re-verification could not
        # settle, skipped until then (_settle_reverify). Under _checksum_lock.
        self._reverify_skipped: Dict[str, Dict[str, float]] = {}
        # (path, member or None, subtree): persisted rows a failed write may
        # have left describing an older file; never loaded this mount.
        # Under _checksum_lock.
        self._persisted_refused: Set[Tuple[str, Optional[str], bool]] = set()
        # {dir: {name: members}} queued by the dir's last forced
        # verification (_force_verify). Under _checksum_lock.
        self._verify_results: Dict[str, Dict[str, List[str]]] = {}
        self._reverify_stop = threading.Event()
        self._reverify_thread: Optional[threading.Thread] = None

        self.namespace = NamespaceService(
            backends=self.backends,
            cache_manager=self.cache_manager,
            state_manager=self.state_manager,
            sync_manager=self.sync_manager,
            logger=self.logger,
            resolve_path=lambda path: self._resolve_path(path),
            schedule_checksum_task=lambda backend, path, basis=None, **kw: self._schedule_checksum_task(
                backend, path, basis, **kw
            ),
            partial_suffix=self._PARTIAL_SUFFIX,
            is_hidden_entry=self._is_hidden_entry,
            dir_cache=self._dir_cache,
            dir_cache_lock=self._dir_cache_lock,
            readdir_snapshot=self._readdir_snapshot,
            readdir_snapshot_ttl=self._readdir_snapshot_ttl,
            checksum_records=self._checksum_records,
            checksum_lock=self._checksum_lock,
            listing_max_stale=max(0.0, float(listing_max_stale)),
            # Finite even with watchers (an infinite fresh window): see
            # NamespaceService.negative_window.
            negative_window=listing_fresh_window(readdir_ttl, revalidate_after),
        )
        self.cache_manager.forget_listed_attrs = self._forget_cached_copy_attrs
        self.cache_manager.path_guard = self._try_path_locked

        self._streaming_manager = StreamingReadManager(
            cache_manager=self.cache_manager,
            state_manager=self.state_manager,
            logger=self.logger,
            get_config=self._get_config,
            select_backend_for_read=self._select_backend_for_read,
            streaming_reads=streaming_reads,
            streaming_chunk_size=streaming_chunk_size,
            max_streaming_sessions=max_streaming_sessions,
            keep_after_close=streaming_keep_after_close,
            on_finalized=lambda session: self._record_fetched(
                session.canonical_path, session.final_cache_path,
                session.backend, session.member_stat,
            ),
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

        # Threads started before FUSE() do not survive the daemonizing fork
        # in background mode, so the sweep starts here, like the workers.
        if self._sweep_pending and self._sweep_thread is None:
            self._sweep_thread = threading.Thread(
                target=self._run_stale_sweep, name="stale-cache-sweep", daemon=True,
            )
            self._sweep_thread.start()

        if self._enable_watchers and not self._watchers:
            self._start_watchers()

        if self._checksum_reverify_interval > 0 and self._reverify_thread is None:
            self._reverify_thread = threading.Thread(
                target=self._run_reverify, name="checksum-reverify", daemon=True,
            )
            self._reverify_thread.start()

    def _get_config(self, virtual_path: str) -> FileConfig:
        """Get configuration for a file"""
        cache_path = self.cache_manager.get_cache_path(virtual_path)
        return get_inherited_config(cache_path)

    @contextlib.contextmanager
    def _path_locked(self, *paths: str):
        """Hold the namespace lock shared and the given canonical paths' locks."""
        with self._namespace_lock.shared(), self._path_locks.hold(*paths):
            yield

    @contextlib.contextmanager
    def _try_path_locked(self, canonical_path: str):
        """Yield whether both outer locks were taken for the path without waiting.

        For eviction, which runs under the cache lock (inside the outer
        locks' order) and so must skip a busy path rather than wait.
        """
        with self._namespace_lock.try_shared() as shared:
            if not shared:
                yield False
                return
            with self._path_locks.try_hold(canonical_path) as held:
                yield held

    @contextlib.contextmanager
    def _descriptor_guard(self, virtual_path: str):
        """Hold _fd_lock and yield whether a non-prefetch descriptor is open on the path.

        Eviction stats and unlinks the cache file inside this, so no open()
        can slip a descriptor onto the file in between: open() takes
        _fd_lock across its os.open and registration. Nothing under
        _fd_lock takes the index or cache lock: every other _fd_lock section
        touches only the descriptor table and raw fd calls, and
        _invalidate_file_cache nests its _fd_lock section inside the index
        lock and takes the cache lock only after releasing both. So the
        order cache lock, index lock, _fd_lock has no inversion.
        """
        with self._fd_lock:
            yield any(
                entry.canonical_path == virtual_path and entry.prefetch_session is None
                for entry in self._fd_map.values()
            )

    def _has_open_descriptor(self, virtual_path: str) -> bool:
        """Whether any descriptor other than a prefetch session is open on the path."""
        with self._descriptor_guard(virtual_path) as is_open:
            return is_open

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

        The same critical section as eviction (CacheManager._evict_if_still_clean):
        the index lock is held from the row read through the index writes, so
        no write can be recorded in between, and inside it _fd_lock is held
        across the open-writer check, the stat and the unlink, so no writer
        can get a descriptor in between either (open() takes _fd_lock across
        its os.open and registration). The path's listing attributes are
        forgotten before the unlink. A CLEAN row older than the file is a
        write whose row update was lost: it is marked dirty and kept. The
        cache lock is taken only after the index lock is released, keeping the
        order cache lock, index lock, _fd_lock.

        The checksum records for the path go whether or not it is cached, is
        dirty or has a writer open: they describe the members, which changed.
        """
        # Resolve canonical path for metadata lookup.  Suffixed conflict
        # paths (e.g. /foo.remote0) store metadata under the canonical
        # path (/foo), so we need to resolve it.
        resolved = self._resolve_path(virtual_path)
        canonical_path = resolved.canonical_path
        # Records are keyed (canonical_path, backend), so clear both the
        # virtual_path (which may be suffixed) and the canonical_path to
        # cover conflict-view caches like /foo.remote0 whose checksums
        # are stored under /foo.
        self._forget_checksums({virtual_path, canonical_path})

        cache_path = self.cache_manager.get_cache_path(virtual_path)
        if not (cache_path.exists() and cache_path.is_file()):
            return

        with self.state_manager.metadata_lock:
            # For canonical paths, check metadata directly.
            # For suffixed paths, check the canonical metadata.
            meta = self.state_manager.get_file_metadata(canonical_path)
            if meta and meta.sync_status != SyncStatus.CLEAN:
                return

            stale = False
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
                try:
                    st = os.lstat(str(cache_path))
                    if meta and not resolved.backend_name:
                        stale = st.st_size != meta.size or st.st_mtime != meta.last_modified
                    if not stale:
                        # Before the unlink: no getattr may find the copy
                        # gone and its name still in the parent's listing.
                        self._forget_cached_copy_attrs(virtual_path)
                        cache_path.unlink()
                except OSError:
                    return

            if stale:
                # A write whose row update was lost: keep the file and record
                # it.
                self.cache_manager.mark_file_dirty(canonical_path, cache_path)
            else:
                self.state_manager.remove_file_location(virtual_path, "cache")
                # The canonical row describes the canonical cache file, not a
                # suffixed conflict copy; zeroing it for /foo.remote0 would
                # make a still-cached /foo look like a lost write.
                if meta and not resolved.backend_name:
                    meta.checksum = None
                    meta.size = 0
                    meta.last_modified = 0.0
                    self.state_manager.set_file_metadata(meta)
        if not stale:
            self.cache_manager.mark_cache_size_dirty()

    def _forget_checksums(self, paths: Set[str], subtree: bool = False):
        """Drop the checksum records and tasks for paths (and, with subtree,
        everything under them), persisted records included.

        A task that is already running cannot be cancelled; dropping it from
        _checksum_tasks is what keeps its result from being recorded. The
        rows go under _checksum_lock, which every record write holds, so no
        result recorded before the forget lands after it.
        """
        prefixes = tuple(p.rstrip("/") + "/" for p in paths) if subtree else ()

        def matches(path: str) -> bool:
            return path in paths or (bool(prefixes) and path.startswith(prefixes))

        with self._checksum_lock:
            for k in [k for k in self._checksum_records if matches(k[0])]:
                del self._checksum_records[k]
            for k in [k for k in self._checksum_tasks if matches(k[0])]:
                self._checksum_tasks.pop(k).cancel()
            self._drop_persisted_checksums(paths, subtree=subtree)

    def _drop_persisted_checksums(self, paths, *, subtree: bool = False, backend_name: Optional[str] = None):
        """Delete persisted hashes; the caller holds _checksum_lock.

        A row that could not be deleted describes a member that changed,
        possibly without a change to its basis (a watcher event, an
        upload), so it is refused for the rest of this mount
        (_persisted_refused) rather than trusted on its basis.
        """
        paths = list(paths)
        try:
            self.state_manager.delete_member_checksums(paths, subtree=subtree, backend_name=backend_name)
        except Exception as exc:
            self.logger.warning("could not drop persisted checksums: %r", exc)
            for path in paths:
                self._persisted_refused.add((path, backend_name, subtree))
            return
        if self._persisted_refused:
            self._retry_refused_deletes()

    def _retry_refused_deletes(self):
        """Delete the rows failed writes left, so a refusal outlives the
        mount; the caller holds _checksum_lock. Called once the index
        answers again (a delete succeeded, or before a write) and at
        destroy. One statement batch per (member, subtree) group. A row
        written under a refused path since the failure goes too: that
        costs a hash after a remount, never trusts a stale row."""
        groups: Dict[Tuple[Optional[str], bool], List[str]] = {}
        for path, backend_name, subtree in self._persisted_refused:
            groups.setdefault((backend_name, subtree), []).append(path)
        for (backend_name, subtree), paths in groups.items():
            try:
                self.state_manager.delete_member_checksums(paths, subtree=subtree,
                                                           backend_name=backend_name)
            except Exception:
                return
            for path in paths:
                self._persisted_refused.discard((path, backend_name, subtree))

    def _persisted_row_refused(self, key: tuple) -> bool:
        """Whether a failed write covered key; the caller holds _checksum_lock."""
        path, backend_name = key
        for refused_path, refused_backend, subtree in self._persisted_refused:
            if refused_backend not in (None, backend_name):
                continue
            if path == refused_path or (
                    subtree and path.startswith(refused_path.rstrip("/") + "/")):
                return True
        return False

    def _on_remote_change(self, virtual_path: str):
        """
        Callback from RemoteWatcher when inotifywait detects a change.

        Invalidates the canonical path AND any backend-suffixed conflict
        variants (e.g. /foo.remote0, /foo.remote1) so that cached
        conflict views are also refreshed.

        Runs on the watcher's thread holding nothing; it waits for the
        path's lock, so an open's fetch, a rename or an upload of the path
        finishes before the invalidation looks at the cache file.
        """
        with self._path_locked(self._resolve_path(virtual_path).canonical_path):
            self._on_remote_change_locked(virtual_path)

    def _on_remote_change_locked(self, virtual_path: str):
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
        from .backends.ssh_mux import shared_control_sockets

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
                control_sockets=shared_control_sockets(),
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

    def _record_checksum(self, canonical_path: str, backend_name: str, checksum: Optional[str], timestamp: float,
                         basis: Optional[ChecksumBasis] = None):
        """Record a checksum for (path, backend) and keep only the latest.

        A task still hashing the same key started before this result, so its
        result is dropped rather than allowed to replace this one. A digest
        with a basis is persisted too, so the next mount does not hash the
        file again while the member reports the same basis; any other
        result replaces the persisted one by none.
        """
        key = (canonical_path, backend_name)
        record = ChecksumRecord(checksum=checksum, timestamp=timestamp, basis=basis)
        with self._checksum_lock:
            superseded = self._checksum_tasks.pop(key, None)
            if superseded is not None:
                superseded.cancel()
            self._checksum_records[key] = record
            self._persist_checksum(canonical_path, backend_name, record)

    def _persist_checksum(self, canonical_path: str, backend_name: str, record: Optional[ChecksumRecord]):
        """Write (or, for None or a record not worth keeping, delete) the
        persisted hash; the caller holds _checksum_lock."""
        if record is None or record.checksum is None or record.basis is None:
            self._drop_persisted_checksums({canonical_path}, backend_name=backend_name)
            return
        if self._persisted_refused:
            # Before the write, so a refused subtree or member-wide delete
            # does not remove the row written now.
            self._retry_refused_deletes()
        try:
            self.state_manager.put_member_checksum(canonical_path, backend_name, record)
        except Exception as exc:
            # The row left behind, if any, predates this record.
            self.logger.warning("could not persist a checksum: %r", exc)
            self._persisted_refused.add((canonical_path, backend_name, False))
            return
        self._persisted_refused.discard((canonical_path, backend_name, False))

    def _load_persisted_checksum(self, key: tuple, basis: ChecksumBasis) -> bool:
        """Install the persisted record for key if it was taken from basis;
        the caller holds _checksum_lock. Returns whether it did."""
        if self._persisted_refused and self._persisted_row_refused(key):
            return False
        try:
            record = self.state_manager.get_member_checksum(*key)
        except Exception as exc:
            self.logger.debug("could not read a persisted checksum: %r", exc)
            return False
        if record is None or record.basis != basis:
            return False
        self._checksum_records[key] = record
        return True

    def _aggregate_checksums_yaml(self, canonical_path: str) -> bytes:
        """Return a YAML list of '<backend>:<value>:<timestamp>' entries.

        value is the member's hash, or 'unverified' when the member ran its
        hash and gave no digest (the view never downloads to hash). A name
        the published listing split by size lists 'size=<bytes>' for every
        copy instead, timestamped with that listing, as the view does: a
        hash recorded before the sizes diverged describes an older file.
        """
        sizes, listed_at = self.namespace.size_split_markers(canonical_path)
        if sizes:
            entries = [f"{backend}:size={size}:{listed_at}"
                       for backend, size in sorted(sizes.items())]
            return yaml.dump(entries, default_flow_style=False).encode()
        with self._checksum_lock:
            entries = [
                f"{backend}:{record.checksum or 'unverified'}:{record.timestamp}"
                for (path, backend), record in self._checksum_records.items()
                if path == canonical_path
            ]
        return yaml.dump(entries, default_flow_style=False).encode()

    def _schedule_directory_checksums(self, dir_path: str, backend_entries: Dict[str, Set[str]]):
        """Queue checksum computations for backend collisions in the directory."""
        self.namespace._schedule_directory_checksums(dir_path, backend_entries)

    def _schedule_checksum_task(self, backend: Backend, canonical_path: str,
                                basis: Optional[ChecksumBasis] = None, *,
                                check_dir: bool = False, replace: bool = False):
        """Hash canonical_path on backend in the background.

        basis is the member's (st_size, st_mtime, st_ctime) from the listing,
        st_ctime None where the member does not report it (SFTP). A record
        made from the same basis is still current (an unverified one too:
        a hash that gave no digest or timed out is not run again until the
        file changes or its records are forgotten), so nothing is
        scheduled;
        without a basis (no listed attributes, or a symlink) the file is
        always hashed again. A record persisted by an earlier mount (or
        by sync, for what it uploaded) counts as made from its basis: one
        that matches is loaded instead of hashing. check_dir: the worker first asks the member
        whether the path is a directory and records nothing if so.
        replace: the member's copy just changed (a release uploaded it), so
        a task already queued or running for it is superseded rather than
        reused; the old record stays until the new hash replaces it.

        Only queues, under _checksum_lock; the member is called on its own
        hash worker, never on the calling (FUSE) thread.
        """
        key = (canonical_path, backend.name)
        with self._checksum_lock:
            previous = self._checksum_tasks.get(key)
            if previous is not None:
                if not replace:
                    return
                # Popped first: its done callback then finds itself gone
                # and records nothing.
                del self._checksum_tasks[key]
                previous.cancel()
            elif not replace and basis is not None:
                record = self._checksum_records.get(key)
                if record is not None and record.basis == basis:
                    return
                # Sync may have persisted a hash of what it uploaded.
                if self._load_persisted_checksum(key, basis):
                    return

            try:
                # A replacing hash (an upload, conflicts --verify) is never
                # dropped from a full queue: nothing would schedule it again.
                future = self._checksum_queues.submit(
                    backend.name,
                    lambda: self._compute_checksum(backend, canonical_path, check_dir),
                    pinned=replace,
                )
            except RuntimeError:
                return  # destroy() shut the queues down
            self._checksum_tasks[key] = future

            def _record(done):
                with self._checksum_lock:
                    # Gone or replaced: the path was invalidated while this
                    # ran, so its result may describe the old file.
                    if self._checksum_tasks.get(key) is not done:
                        return
                    del self._checksum_tasks[key]
                    if done.cancelled():
                        # Still registered, so dropped by the member's
                        # full queue, not superseded or invalidated: a
                        # record left from before would outlive the change
                        # that scheduled this hash (an upload, a new basis).
                        self._checksum_records.pop(key, None)
                        self._persist_checksum(canonical_path, backend.name, None)
                        return
                    result = done.result()
                    if result is None:
                        # Could not be asked: whatever was recorded before
                        # predates the change that scheduled this hash.
                        self._checksum_records.pop(key, None)
                        self._persist_checksum(canonical_path, backend.name, None)
                    elif result is not self._IS_DIRECTORY:
                        checksum, ts = result
                        self._record_checksum(canonical_path, backend.name, checksum, ts, basis)

            future.add_done_callback(_record)

    def _force_verify(self, dir_path: str):
        """List the directory and hash again every copy of its names on
        several members that the listing does not split by size, whatever
        is recorded (storage conflicts --verify). The hashes are queued;
        the listing itself calls the members, as a readdir would. Which
        names it queued is kept for _verify_status, for the last
        VERIFY_RESULTS_KEPT directories, never dropping one still pending."""
        queued = self.namespace.verify_listing(dir_path)
        with self._checksum_lock:
            self._verify_results.pop(dir_path, None)
            self._verify_results[dir_path] = queued
            # Pending directories are never dropped, so with many verified
            # at once this holds more; each is bounded by its directory.
            for old in list(self._verify_results):
                if len(self._verify_results) <= self.VERIFY_RESULTS_KEPT:
                    break
                if old != dir_path and not self._verify_counts(old)["pending"]:
                    del self._verify_results[old]

    # Directories whose last forced verification _verify_status reports.
    VERIFY_RESULTS_KEPT = 64

    def _verify_status(self, dir_path: str) -> Dict[str, int]:
        """Of the names the directory's last forced verification hashed
        again (regular files of one size on several members): how many
        have a hash still pending, how many are verified on every member,
        and how many are not (a member could not hash its copy, or the
        copy changed and its record was forgotten). known is 0 when no
        verification of the directory is on record (none yet, or one
        settled and since dropped), and the counts are then zero."""
        with self._checksum_lock:
            if dir_path not in self._verify_results:
                return {"known": 0, "pending": 0, "verified": 0, "unverified": 0}
            return dict(self._verify_counts(dir_path), known=1)

    def _verify_counts(self, dir_path: str) -> Dict[str, int]:
        """_verify_status's counts; the caller holds _checksum_lock."""
        status = {"pending": 0, "verified": 0, "unverified": 0}
        for name, members in self._verify_results.get(dir_path, {}).items():
            keys = [(str(Path(dir_path) / name), m) for m in members]
            if any(key in self._checksum_tasks for key in keys):
                status["pending"] += 1
            elif all(getattr(self._checksum_records.get(key), "checksum", None) for key in keys):
                status["verified"] += 1
            else:
                status["unverified"] += 1
        return status

    _IS_DIRECTORY = object()
    _GONE = object()

    # A persisted hash checked more recently than this is not re-verified:
    # with few rows, the pass would otherwise hash the same files over and
    # over.
    CHECKSUM_REVERIFY_MIN_AGE = 3600.0

    def _run_reverify(self):
        while not self._reverify_stop.wait(self._checksum_reverify_interval):
            try:
                self._reverify_tick()
            except Exception:
                self.logger.exception("checksum re-verification pass failed")

    def _reverify_tick(self):
        """Queue one re-verification per member that is idle and reachable.

        Persisted hashes are trusted while the member reports the same
        basis, which a same-size rewrite within the member's mtime
        resolution (or one that kept the mtime, on a member that reports
        no ctime) does not change. Hashing the oldest-checked row again,
        one file per member per interval, finds such a rewrite eventually,
        off the browse path: only an idle hash worker takes one, so a
        listing's hashes always go first. Only names recorded on several
        members are taken: they are the ones the view compares. A row the
        pass could not settle (not asked, no digest, changed while hashed)
        is skipped for CHECKSUM_REVERIFY_MIN_AGE, so one file cannot hold
        up the rest.
        """
        verified_before = time.time() - self.CHECKSUM_REVERIFY_MIN_AGE
        now = time.monotonic()
        for name, backend in self.backends.items():
            if not self._checksum_queues.idle(name):
                continue
            with self._checksum_lock:
                skipped = self._reverify_skipped.get(name, {})
                for path in [p for p, until in skipped.items() if until <= now]:
                    del skipped[path]
                exclude = set(skipped)
            try:
                if not backend.is_available():
                    continue
                row = self.state_manager.oldest_member_checksum(name, verified_before, exclude)
            except Exception as exc:
                self.logger.debug("re-verification on %s skipped: %r", name, exc)
                continue
            if row is not None:
                self._schedule_reverify(backend, row[0], row[1])

    def _schedule_reverify(self, backend: Backend, canonical_path: str, persisted: ChecksumRecord):
        """Hash canonical_path on backend again and settle its persisted row.

        Registered like a listing's hash, so a forget, an upload or a
        replacing hash supersedes it, and a listing meanwhile waits for it.
        Settled under _checksum_lock (_settle_reverify).
        """
        key = (canonical_path, backend.name)
        with self._checksum_lock:
            if key in self._checksum_tasks:
                return
            # Read again under the lock: a record written since the pass
            # read the row replaced it, and this re-verification would
            # judge the new row by the old one.
            try:
                if self.state_manager.get_member_checksum(*key) != persisted:
                    return
            except Exception:
                return
            try:
                future = self._checksum_queues.submit(
                    backend.name,
                    lambda: self._reverify_member_copy(backend, canonical_path, persisted.basis),
                )
            except RuntimeError:
                return
            self._checksum_tasks[key] = future

            def _settle(done):
                with self._checksum_lock:
                    if self._checksum_tasks.get(key) is not done:
                        return
                    del self._checksum_tasks[key]
                    if not done.cancelled():
                        self._settle_reverify(key, persisted, done.result())

            future.add_done_callback(_settle)

    def _reverify_member_copy(self, backend: Backend, canonical_path: str,
                              persisted_basis: ChecksumBasis):
        """On the member's hash worker: stat, hash, stat again.

        Returns (checksum or None, timestamp); _GONE when the member no
        longer has the file (or it is not a regular file) or reports
        another basis than persisted_basis (the row describes an older
        file; nothing is hashed); None when the member could not be asked
        or the file changed while it was hashed.
        """
        def basis_of():
            st = backend.stat(canonical_path)
            mode = getattr(st, "st_mode", None)
            if mode is None or not stat.S_ISREG(mode):
                return None
            return (st.st_size, st.st_mtime, getattr(st, "st_ctime", None))

        try:
            before = basis_of()
        except FileNotFoundError:
            return self._GONE
        except Exception as exc:
            self.logger.debug("re-verification stat on %s failed: %r", backend.name, exc)
            return None
        if before is None or before != persisted_basis:
            return self._GONE
        result = self._compute_checksum(backend, canonical_path)
        if result is None:
            return None
        try:
            after = basis_of()
        except Exception as exc:
            self.logger.debug("re-verification stat on %s failed: %r", backend.name, exc)
            return None
        if after != before:
            return None  # changed while hashed
        return result

    def _settle_reverify(self, key: tuple, persisted: ChecksumRecord, result):
        """Apply a re-verification; the caller holds _checksum_lock.

        Gone, or another basis: the row goes, and the in-memory record if
        it was made from the same basis. Not asked, changed while hashed,
        or no digest (the member cannot hash it now; the row is still what
        it last reported): the row stays as it is, and is skipped by the
        pass for CHECKSUM_REVERIFY_MIN_AGE. The same digest: checked. Another
        digest from the same basis: a rewrite the basis could not show; the
        new hash replaces the row and, where the view tracks the name, its
        record.
        """
        canonical_path, backend_name = key
        if result is self._GONE:
            record = self._checksum_records.get(key)
            if record is not None and record.basis == persisted.basis:
                del self._checksum_records[key]
            self._persist_checksum(canonical_path, backend_name, None)
            return
        if result is None or result[0] is None:
            self._reverify_skipped.setdefault(backend_name, {})[canonical_path] = (
                time.monotonic() + self.CHECKSUM_REVERIFY_MIN_AGE)
            return
        checksum, ts = result
        if checksum == persisted.checksum:
            try:
                self.state_manager.mark_member_checksum_verified(
                    canonical_path, backend_name, persisted.checksum, ts)
            except Exception as exc:
                self.logger.debug("could not mark a checksum verified: %r", exc)
            return
        self.logger.info(
            "re-verification found a copy on %s rewritten without a change "
            "to its size or timestamps", backend_name,
        )
        record = ChecksumRecord(checksum=checksum, timestamp=ts, basis=persisted.basis)
        if key in self._checksum_records:
            self._checksum_records[key] = record
        self._persist_checksum(canonical_path, backend_name, record)

    def _compute_checksum(self, backend: Backend, canonical_path: str, check_dir: bool = False):
        """Compute a checksum on the member side, on its hash worker.

        Returns (checksum, timestamp), with checksum None when the member's
        hash gave no digest or did not finish (ChecksumUnavailable:
        unverified, no download fallback here); None when the member could
        not be asked (connection lost, no channel) or a local read failed,
        which drops any older record and lets the next listing retry; or
        _IS_DIRECTORY when check_dir found a directory. The caller records
        it in memory. Nothing is written to the member: an xattr write
        raises the watcher's ATTRIB event, whose invalidation would drop the
        record this hash made, and nothing reads it back (getxattr answers
        user.storage.checksums from memory).
        """
        try:
            if check_dir and backend.isdir(canonical_path):
                return self._IS_DIRECTORY
        except Exception as exc:
            self.logger.debug("isdir for %s on %s failed: %s", canonical_path, backend.name, exc)
        try:
            checksum = backend.get_fast_checksum(canonical_path, allow_download=False)
            return checksum, time.time()
        except ChecksumUnavailable as exc:
            self.logger.debug("checksum for %s on %s unavailable: %s", canonical_path, backend.name, exc)
            return None, time.time()
        except Exception as exc:
            self.logger.debug("checksum for %s on %s failed: %s", canonical_path, backend.name, exc)
            return None

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
        """Pick a readable backend, preferring existing replication metadata.

        A refetch after revalidation reads from the member that was checked
        (see _revalidate_cached_file) while it is available.
        """
        with self._member_checks_lock:
            pinned = self._refetch_pins.pop(canonical_path, None)
        if pinned is not None and pinned.is_available():
            # The recheck mark stays until the bytes arrive (_record_fetched,
            # for a full fetch or a finished stream); a transfer that fails
            # after this leaves it owed.
            return pinned
        backend = self._select_unpinned_backend_for_read(canonical_path)
        if pinned is not None and backend is not pinned:
            with self._member_checks_lock:
                self._recheck.setdefault(canonical_path, set()).add(pinned.name)
        return backend

    def _clear_recheck(self, canonical_path: str, backend_name: str):
        """backend_name is supplying the file's bytes now: no forced refetch is owed to it."""
        with self._member_checks_lock:
            marks = self._recheck.get(canonical_path)
            if marks is not None:
                marks.discard(backend_name)
                if not marks:
                    del self._recheck[canonical_path]

    def _select_unpinned_backend_for_read(self, canonical_path: str) -> Optional[Backend]:
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

    # ------------------------------------------------------------------ #
    # Revalidation of cached files on open
    # ------------------------------------------------------------------ #
    @staticmethod
    def _cache_identity(st: os.stat_result) -> Tuple[int, int, int]:
        return (st.st_ino, st.st_size, st.st_mtime_ns)

    def _forget_member_check(self, virtual_path: str, subtree: bool = False,
                             path_gone: bool = False):
        """Drop the in-memory check (a persisted baseline dies with its file,
        or is ignored once the file's identity no longer matches).

        With path_gone (unlink, a failed rename) the refetch pin and recheck
        marks go too; a cached copy that is only being dropped or rewritten
        keeps them, since they are about the members, not the copy.
        """
        prefix = virtual_path.rstrip("/") + "/"
        tables = [self._member_checks]
        if path_gone:
            tables += [self._refetch_pins, self._recheck]
        with self._member_checks_lock:
            for table in tables:
                table.pop(virtual_path, None)
                if subtree:
                    for key in [k for k in table if k.startswith(prefix)]:
                        del table[key]

    def _drop_persisted_baselines(self, virtual_path: str):
        """Remove the member-baseline xattr from a cached file or every file under a directory.

        For a rename that failed part way: the cache files may have moved
        while the members did not, and a baseline matched on identity alone
        would then describe a member file at another path.
        """
        root = self.cache_manager.get_cache_path(virtual_path)
        if root.is_dir() and not root.is_symlink():
            files = (Path(d) / name for d, _dirs, names in os.walk(root) for name in names)
        else:
            files = iter([root])
        for path in files:
            try:
                os.removexattr(str(path), self.MEMBER_STAT_XATTR, follow_symlinks=False)
            except OSError:
                pass

    def _move_member_checks(self, old: str, new: str):
        """A rename succeeded: the checks (and recheck marks) follow the files.

        The cache files keep their identity across a rename, and the members
        are renamed too, so the baselines still hold. Whatever was recorded
        under the new name (a file replaced by the rename) goes.
        """
        if old == new:
            return
        old_prefix = old.rstrip("/") + "/"
        new_prefix = new.rstrip("/") + "/"
        with self._member_checks_lock:
            for table in (self._member_checks, self._refetch_pins, self._recheck):
                for key in [k for k in table if k == new or k.startswith(new_prefix)]:
                    del table[key]
                moved = {}
                for key in [k for k in table if k == old or k.startswith(old_prefix)]:
                    target = new if key == old else new_prefix + key[len(old_prefix):]
                    moved[target] = table.pop(key)
                table.update(moved)

    def _forget_cached_copy_attrs(self, virtual_path: str, *, removing: bool = True):
        """Forget what was known about a cached copy that is going or changing.

        Installed as CacheManager.forget_listed_attrs, which eviction calls
        before an unlink and mark_file_dirty before recording a write: in both
        the member check no longer describes the cache file.
        """
        self.namespace.forget_listed_attrs(virtual_path, removing=removing)
        self._forget_member_check(virtual_path)

    def _load_persisted_members(self, cache_path: Path,
                                identity: Tuple[int, int, int]) -> Dict[str, Tuple[int, float]]:
        """Member baselines from the cache file's xattr, if it still describes this file.

        Matched on the cache file's (ino, size, mtime_ns) alone: a rename
        keeps both the file and its baselines valid, and a copy of the xattr
        onto another file (copy2 to a local member and back) has another
        inode.
        """
        try:
            payload = json.loads(os.getxattr(str(cache_path), self.MEMBER_STAT_XATTR))
            if tuple(payload.get("cache")) != identity:
                return {}
            return {
                str(name): (int(size), float(mtime))
                for name, (size, mtime) in payload.get("members", {}).items()
            }
        except (OSError, ValueError, TypeError, AttributeError):
            return {}

    def _store_member_check(self, canonical_path: str, cache_path: Path,
                            identity: Tuple[int, int, int],
                            updates: Dict[str, Tuple[int, float]], *, replace: bool = False,
                            drop: Tuple[str, ...] = (),
                            drop_unless_checked_since: Optional[MemberCheck] = None):
        """Record a check now, merging member baselines, in memory and on the cache file.

        The baselines already known for this cache file (in memory, else from
        its xattr) are kept and updates applied on top; replace=True starts
        from nothing (a fetch: the old copy's baselines describe other bytes),
        and the members in drop lose theirs unless that member's baseline was
        recorded since drop_unless_checked_since (the MemberCheck the caller
        read before, or None). Compared by object: a record merges the
        other members' baseline tuples unchanged and stores a new tuple for
        each member it observed, so a baseline recorded meanwhile, even with
        the same values, is newer and stays, while a record for another
        member does not hold the drop back. A baseline that came from the
        xattr has no object to compare once a record is made, and stays.
        Updates and drops name different members.
        The read, merge and both writes are one critical section under
        _member_checks_lock (a leaf: getxattr and setxattr take no other
        lock), so a sync worker and the FUSE thread cannot drop each other's
        baselines. The xattr carries the cache file's identity, so a
        rewritten or refetched file ignores it; it lets the first check after
        a restart compare against a baseline instead of guessing.
        """
        with self._member_checks_lock:
            previous = self._member_checks.get(canonical_path)
            if replace:
                members = {}
            elif previous is not None and previous.cache_identity == identity:
                members = dict(previous.members)
            else:
                members = self._load_persisted_members(cache_path, identity)
            members.update(updates)
            dropped = []
            token = drop_unless_checked_since
            for name in drop:
                if name not in members:
                    continue
                if previous is not token:
                    # Compared by object: the merge above must carry each
                    # member's tuple unchanged, and every observation must
                    # store a fresh one (never rebuilt, cached or reused).
                    read = token.members.get(name) if token is not None else None
                    recorded_since = (read is None or previous is None
                                      or previous.members.get(name) is not read)
                    if recorded_since:
                        continue
                del members[name]
                dropped.append(name)
            check = MemberCheck(cache_identity=identity, verified_at=time.monotonic(),
                                members=members)
            self._member_checks[canonical_path] = check
            if (members or dropped) and (previous is None or previous.cache_identity != identity
                                         or previous.members != members):
                payload = {"cache": list(identity),
                           "members": {name: list(stat) for name, stat in members.items()}}
                try:
                    os.setxattr(str(cache_path), self.MEMBER_STAT_XATTR,
                                json.dumps(payload, sort_keys=True).encode("utf-8"))
                except OSError as exc:
                    self.logger.debug("could not persist member baseline: %r", exc)

    def _record_fetched(self, canonical_path: str, cache_path: Path, backend: Backend,
                        member_stat: Optional[os.stat_result]):
        """A full fetch from backend just finished; member_stat was taken before it.

        Stat before copy: a change landing in between leaves a baseline older
        than the copy, which costs one needless refetch at the next check;
        the other order could pair a new baseline with old bytes and never
        see the change.
        """
        try:
            identity = self._cache_identity(os.lstat(str(cache_path)))
        except OSError:
            self._forget_member_check(canonical_path)
            return
        members = {}
        if member_stat is not None:
            members[backend.name] = (member_stat.st_size, member_stat.st_mtime)
        self._store_member_check(canonical_path, cache_path, identity, members, replace=True)
        self._clear_recheck(canonical_path, backend.name)

    def _record_member_written(self, virtual_path: str, backend: Backend, cache_path: Path,
                               checksum: Optional[str] = None):
        """Sync callback: backend now holds the cache file's content.

        checksum is the hash of what was uploaded (None: not known, a write
        may have overlapped the upload). With the member's stat after the
        upload as its basis it becomes the member's conflict-view record,
        so the mount's own writes never wait on, or hide from, a member
        hash: a same-size rewrite within the member's mtime resolution
        (or one that kept the mtime) is recorded as what it is.

        Runs in a sync worker, so the stat costs the FUSE loop nothing. The
        member's stat after an upload is its baseline for later checks:
        without it, an upload that does not keep the mtime (SFTP) would look
        like a change on the member at the next stale open. Baselines for
        the other members are kept while the cache file is the same one. If
        the member cannot be stat'd, its old baseline no longer describes
        it (the upload replaced the file) and is dropped: its next check
        uses the rule for a member without one.
        """
        try:
            identity = self._cache_identity(os.lstat(str(cache_path)))
        except OSError as exc:
            self.logger.debug("post-sync stat of the cache file failed: %r", exc)
            self._record_uploaded_checksum(virtual_path, backend.name, None, None)
            return
        with self._member_checks_lock:
            check = self._member_checks.get(virtual_path)
            if check is not None and check.cache_identity == identity:
                had_baseline = backend.name in check.members
            else:
                had_baseline = backend.name in self._load_persisted_members(cache_path, identity)
        try:
            member_stat = backend.stat(virtual_path)
        except Exception as exc:
            self.logger.debug("post-sync stat on %s failed: %r", backend.name, exc)
            self._record_uploaded_checksum(virtual_path, backend.name, None, None)
            if had_baseline:
                self._store_member_check(virtual_path, cache_path, identity, {},
                                         drop=(backend.name,), drop_unless_checked_since=check)
            return
        self._record_uploaded_checksum(virtual_path, backend.name, checksum, member_stat)
        self._store_member_check(virtual_path, cache_path, identity,
                                 {backend.name: (member_stat.st_size, member_stat.st_mtime)})

    def _record_uploaded_checksum(self, virtual_path: str, backend_name: str,
                                  checksum: Optional[str], member_stat):
        """The member now holds content hashing to checksum, with
        member_stat as its basis; None for either: its content is not
        known, and whatever was recorded for it predates the upload.

        Persisted always, so a listing that reports this basis loads it;
        installed in memory only where the name is already tracked (a
        record or a hash in flight, which the upload supersedes), so
        uploads of names on one member do not grow the in-memory table.
        A conflict copy (name.member) uploads through release(), which
        schedules its own hash.
        """
        resolved = self._resolve_path(virtual_path)
        if resolved.backend_name is not None:
            return
        key = (resolved.canonical_path, backend_name)
        record = None
        if checksum is not None and member_stat is not None:
            mode, size = getattr(member_stat, "st_mode", None), getattr(member_stat, "st_size", None)
            mtime = getattr(member_stat, "st_mtime", None)
            if mode is not None and stat.S_ISREG(mode) and size is not None and mtime is not None:
                # The listing's basis: st_ctime is absent where the member
                # does not report it (SFTP), as in its listings.
                basis = (size, mtime, getattr(member_stat, "st_ctime", None))
                record = ChecksumRecord(checksum=checksum, timestamp=time.time(), basis=basis)
        with self._checksum_lock:
            superseded = self._checksum_tasks.pop(key, None)
            if superseded is not None:
                superseded.cancel()
            if record is None:
                self._checksum_records.pop(key, None)
            elif superseded is not None or key in self._checksum_records:
                self._checksum_records[key] = record
            self._persist_checksum(key[0], backend_name, record)

    def _member_for_revalidation(self, canonical_path: str) -> Tuple[Optional[Backend], List[str]]:
        """Pick the member to stat, from the index alone (no remote call).

        The fastest available holder, the member a refetch would read from
        (ReplicationManager.get_backend_for_read orders the same way), so a
        change found there is what the refetch brings in. Returns it and the
        names of the holders that are unavailable.
        """
        holders = []
        for location in self.state_manager.get_file_locations(canonical_path):
            backend = self.backends.get(location.backend_name)
            if backend is not None and backend not in holders:
                holders.append(backend)
        available = sorted((b for b in holders if b.is_available()), key=lambda b: b.speed_priority)
        unavailable = [b.name for b in holders if not b.is_available()]
        return (available[0] if available else None), unavailable

    def _note_revalidation_outage(self, backend_name: str, reason: str):
        if backend_name in self._revalidate_outages:
            return
        self._revalidate_outages.add(backend_name)
        self.logger.warning(
            "Cannot revalidate cached files against %s (%s); serving cached copies",
            backend_name, reason,
        )

    def _revalidate_cached_file(self, resolved: ResolvedPath):
        """Refetch a CLEAN cached file whose member copy changed.

        Called by open() for a canonical path. Within revalidate_after of the
        last check this is a dict lookup. Past it, and only for a CLEAN row,
        it costs one stat of one member (with the FUSE loop single-threaded
        this blocks every other operation; with fuse_threads, only this
        path's): the fastest available holder in the index,
        never a listing or an exists() probe. The member's copy changed when:

        - with a baseline for that member (its stat when the cache file was
          last known to match it: at a full or streamed fetch, after a sync, or at the last
          check; kept in memory and in the cache file's xattr): size or
          mtime differ from it;
        - without one (a member never checked, a stat that failed before a
          fetch, an xattr that could not be written): the size differs from the cache
          file's, or the member's mtime is later than the cache file's. A
          fetch keeps the member mtime (local, rsync) or stamps the fetch
          time (SFTP), so an unchanged member is not later unless its clock
          is ahead; the refetch then records a baseline, so that costs one
          refetch, not one per check.

        A changed copy goes through _invalidate_file_cache, which keeps a
        file that became DIRTY or has a writer open; whatever is no longer
        cached is then fetched by the caller as usual, pinned to the member
        that was checked (_select_backend_for_read). If that member is gone
        by then, another supplies the bytes and the checked member is marked
        for a forced refetch at its next check (_recheck), so the change is
        never adopted as a baseline unseen. An unavailable member,
        a failed stat or a member that no longer has the file keeps the
        cached copy and its baselines: the open never fails here. Failures
        are logged once per member until it answers again.
        """
        canonical_path = resolved.canonical_path
        cache_path = resolved.cache_path
        with self._member_checks_lock:
            # A pin no fetch consumed (the open attached to a running stream).
            self._refetch_pins.pop(canonical_path, None)
            check = self._member_checks.get(canonical_path)
        if check is not None and time.monotonic() - check.verified_at < self._revalidate_after:
            return

        metadata = self.state_manager.get_file_metadata(canonical_path)
        if metadata is None or metadata.sync_status != SyncStatus.CLEAN:
            return
        try:
            cache_stat = os.lstat(str(cache_path))
        except OSError:
            return
        identity = self._cache_identity(cache_stat)
        if check is not None and check.cache_identity == identity:
            members = dict(check.members)
        else:
            members = self._load_persisted_members(cache_path, identity)

        backend, unavailable = self._member_for_revalidation(canonical_path)
        if backend is None:
            # Nothing is recorded as checked, so the next open looks again;
            # with no available holder that costs no remote call.
            for name in unavailable:
                self._note_revalidation_outage(name, "unavailable")
            return

        try:
            member_stat = backend.stat(canonical_path)
        except FileNotFoundError:
            # The member may have lost the file or its disk; the cached copy
            # may be the only one left, so it stays.
            self.logger.info("revalidate: %s no longer has %s; keeping the cached copy",
                             backend.name, canonical_path)
            self._store_member_check(canonical_path, cache_path, identity, {})
            return
        except Exception as exc:
            # Counts as a check, so a member that fails without being marked
            # unavailable is not stat'd at every open.
            self._note_revalidation_outage(backend.name, f"stat failed: {exc!r}")
            self._store_member_check(canonical_path, cache_path, identity, {})
            return
        if backend.name in self._revalidate_outages:
            self._revalidate_outages.discard(backend.name)
            self.logger.info("Revalidating cached files against %s again", backend.name)

        observed = (member_stat.st_size, member_stat.st_mtime)
        baseline = members.get(backend.name)
        with self._member_checks_lock:
            # The last refetch for a change here came from another member.
            must_refetch = backend.name in self._recheck.get(canonical_path, ())
        if must_refetch:
            changed = True
        elif baseline is not None:
            changed = observed != baseline
        else:
            changed = (member_stat.st_size != cache_stat.st_size
                       or member_stat.st_mtime > cache_stat.st_mtime)
        if not changed:
            self._store_member_check(canonical_path, cache_path, identity,
                                     {backend.name: observed})
            return

        self.logger.info("revalidate: %s changed on %s; refetching", canonical_path, backend.name)
        with self._member_checks_lock:
            # A recheck mark stays until this member supplies the bytes
            # (_clear_recheck): the invalidation below may be refused.
            self._refetch_pins[canonical_path] = backend
        self._invalidate_file_cache(resolved.requested_path)
        if self.cache_manager.is_cached(resolved.requested_path):
            # Kept (a writer is open, or it turned out to be a lost write):
            # do not stat again before the age passes; the baselines stay,
            # so the next check still sees the change.
            try:
                kept_identity = self._cache_identity(os.lstat(str(cache_path)))
            except OSError:
                return
            with self._member_checks_lock:
                self._refetch_pins.pop(canonical_path, None)
            if kept_identity == identity:
                self._store_member_check(canonical_path, cache_path, identity, {})
        else:
            with self._member_checks_lock:
                self._member_checks.pop(canonical_path, None)

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
                # A CLEAN row older than the file means a write's row update
                # was lost; the file is dirty whatever the row says.
                self.cache_manager.heal_stale_clean(canonical_path, cache_path)
            return True

        # If a backend is explicitly requested (conflict suffix), bypass replication manager.
        if resolved.backend_name:
            backend = self.backends.get(resolved.backend_name)

            # The probe stays outside the try: its errors propagate, as
            # they always have; only a failed transfer falls back below.
            if backend and backend.exists(canonical_path):
                try:
                    if self.cache_manager.cache_file(
                        resolved.requested_path, backend, source_virtual_path=canonical_path,
                    ):
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
            # The member's stat before the copy is the baseline for
            # revalidation (see _record_fetched); without it the fetch
            # still works, and the first check falls back to comparing the
            # member against the cache file.
            try:
                member_stat = backend.stat(canonical_path)
            except Exception:
                member_stat = None
            # Download to cache
            if self.cache_manager.cache_file(
                resolved.requested_path,
                backend,
                source_virtual_path=canonical_path,
            ):
                # Fresh from the member: no check needed until revalidate_after.
                self._record_fetched(canonical_path, cache_path, backend, member_stat)
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
                    # Writes the row and mirrors it.
                    self.cache_manager.store_file_metadata(metadata, cache_path=cache_path)
                else:
                    # A refetch after eviction: the member's copy is what the
                    # row already calls synced, but the transfer does not keep
                    # the mtime, so the row's stat must follow the new file or
                    # heal_stale_clean would resync unchanged content. Only
                    # the stat columns move, and only at the generation read
                    # above; a write recorded since keeps its own stat. The
                    # mirror rewrites the xattr the evicted file lost.
                    with self.state_manager.metadata_lock:
                        self.state_manager.record_cached_stat(
                            canonical_path,
                            metadata.dirty_generation,
                            size=st.st_size,
                            last_modified=st.st_mtime,
                            last_access=time.time(),
                        )
                        metadata = self.state_manager.get_file_metadata(canonical_path) or metadata
                        self.cache_manager.sync_cache_metadata(
                            canonical_path, metadata, cache_path=cache_path,
                        )

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
        with self._path_locked(self._resolve_path(path).canonical_path):
            return self._mkdir(path, mode)

    def _mkdir(self, path, mode):
        """Create a directory"""
        self.namespace.mkdir(path, mode)

    def rmdir(self, path):
        """Remove a directory

        Exclusive: every path under it is affected at once.
        """
        with self._namespace_lock.exclusive():
            self._rmdir(path)

    def _rmdir(self, path):
        try:
            self.namespace.rmdir(path)
        finally:
            self._forget_checksums({self._resolve_path(path).canonical_path}, subtree=True)

    def unlink(self, path):
        """Remove a file

        Waits for an upload of the path in flight, so its location row is
        not recorded after the file's row is gone.
        """
        canonical_path = self._resolve_path(path).canonical_path
        with self._path_locked(canonical_path), self._upload_locks.hold(canonical_path):
            return self._unlink(path)

    def _unlink(self, path):
        """Remove a file"""
        try:
            self.namespace.unlink(path)
        finally:
            canonical_path = self._resolve_path(path).canonical_path
            self._forget_checksums({canonical_path})
            self._forget_member_check(canonical_path, path_gone=True)

    def rename(self, old, new):
        """Rename a file or directory

        A file rename holds both paths' locks (sorted) across the cache move
        and the member renames, so an open of either name waits for the
        members to agree with the cache and cannot fetch a stale copy under
        the old name. A directory rename changes every path under it, so it
        holds the namespace lock exclusive, which waits for every in-flight
        path-locked handler (ditz threading-hierarchical-namespace-lock).
        A name the cache does not hold is classified by getattr (served
        from the listing when it is fresh); one that does not exist is
        renamed as a file, which fails as before. A directory appearing in
        the cache meanwhile (a fetch beneath it) is caught by the recheck
        under the path locks.

        Either kind waits for sync uploads of the paths it moves (ditz
        sync-namespace-orphans): an upload finishing after the rename would
        put the file back on its member under the old name, a name the
        member rename skipped because the upload had not recorded its
        location yet. A file rename takes both upload locks under its path
        locks; a directory rename drains both subtrees before it takes the
        namespace lock, so it does not stall other paths while it waits.
        """
        old_resolved = self._resolve_path(old)
        new_canonical = self._resolve_path(new).canonical_path
        cache_path = old_resolved.canonical_cache_path
        while True:
            if os.path.isdir(cache_path):
                is_dir = True
            elif os.path.lexists(cache_path):
                is_dir = False
            else:
                try:
                    is_dir = stat.S_ISDIR(self.getattr(old)["st_mode"])
                except FuseOSError:
                    is_dir = False
            if is_dir:
                with self._upload_locks.drain(old_resolved.canonical_path, new_canonical):
                    with self._namespace_lock.exclusive():
                        return self._rename(old, new)
            with self._path_locked(old_resolved.canonical_path, new_canonical):
                with self._upload_locks.hold(old_resolved.canonical_path, new_canonical):
                    if not os.path.isdir(cache_path):
                        return self._rename(old, new)

    def _rename(self, old, new):
        old_canonical = self._resolve_path(old).canonical_path
        new_canonical = self._resolve_path(new).canonical_path
        try:
            self.namespace.rename(old, new, on_local_moved=lambda: self._repoint_descriptors(old, new))
        except BaseException:
            # If the cached copy moved but the rename failed after (or
            # before) the members', its baselines, matched on identity alone,
            # may describe a member file at the other path: drop them. If it
            # did not move, they still describe this path and stay.
            old_cache = self.cache_manager.get_cache_path(old_canonical)
            new_cache = self.cache_manager.get_cache_path(new_canonical)
            if not os.path.lexists(old_cache) and os.path.lexists(new_cache):
                self._forget_member_check(old_canonical, subtree=True, path_gone=True)
                self._forget_member_check(new_canonical, subtree=True, path_gone=True)
                self._drop_persisted_baselines(new_canonical)
            raise
        finally:
            # Records are keyed by path, so neither name's records (nor, for a
            # directory, its children's) describe what is there now. Dropping
            # them is always safe, so a failed rename drops them too.
            self._forget_checksums({old_canonical, new_canonical}, subtree=True)
        self._move_member_checks(old_canonical, new_canonical)

    def _repoint_descriptors(self, old, new):
        """Open descriptors follow the file (or everything under a renamed directory).

        write() records against, and eviction and sync look up, the path
        they carry. Called by NamespaceService.rename under the index lock,
        right after the cache move and the index rewrite.
        """
        prefix = old.rstrip("/") + "/"
        with self._fd_lock:
            for entry in self._fd_map.values():
                if entry.virtual_path == old:
                    moved = new
                elif entry.virtual_path.startswith(prefix):
                    moved = new.rstrip("/") + "/" + entry.virtual_path[len(prefix):]
                else:
                    continue
                target = self._resolve_path(moved)
                entry.virtual_path = moved
                entry.canonical_path = target.canonical_path
                entry.cache_path = target.cache_path

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
        """Record a modification of a cached file (see CacheManager.mark_file_dirty)."""
        self.cache_manager.mark_file_dirty(canonical_path, cache_path)

    def truncate(self, path, length, fh=None):
        """Truncate a file"""
        with self._path_locked(self._resolve_path(path).canonical_path):
            return self._truncate(path, length, fh)

    def _truncate(self, path, length, fh=None):
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
        with self._path_locked(self._resolve_path(path).canonical_path):
            return self._open(path, flags)

    def _open(self, path, flags):
        """Open a file"""
        resolved = self._resolve_path(path)
        cache_path = resolved.cache_path
        prefetch_session: Optional[PrefetchSession] = None

        # Before the streaming decision: a copy dropped here is refetched
        # like any uncached file, streamed if streaming is on.
        # An O_TRUNC open discards the bytes, so their freshness does not matter.
        if (
            resolved.backend_name is None
            and not flags & os.O_TRUNC
            and self.cache_manager.is_cached(resolved.requested_path)
        ):
            self._revalidate_cached_file(resolved)

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
        with self._path_locked(self._resolve_path(path).canonical_path):
            return self._create(path, mode, fi)

    def _create(self, path, mode, fi=None):
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
            # Positional: a descriptor's offset is shared state, and
            # concurrent writes through one fh must not move each other's.
            written = os.pwrite(entry.real_fd, data, offset)
            self.cache_manager.mark_cache_size_dirty()

            if entry.backend_name is None:
                self.cache_manager.mark_accessed(path)
                # The path is read under the index lock, which a rename
                # holds from the cache move until it has re-pointed the
                # descriptors: the mark lands on the row that describes
                # this file now, never on a path whose row has moved.
                with self.state_manager.metadata_lock:
                    with self._fd_lock:
                        canonical_path, cache_path = entry.canonical_path, entry.cache_path
                    self._mark_cache_file_dirty(canonical_path, cache_path)

            return written

        except OSError as e:
            raise FuseOSError(e.errno)

    def release(self, path, fh):
        """Release (close) a file

        Holds the path's lock (the descriptor's current path, read again
        once the lock is held, since a rename re-points it): closing a
        member copy (a conflict-suffixed path) uploads it, and no rename,
        unlink, refetch or watcher invalidation may run under the upload; a
        canonical close reads the path's row and queues its sync, which a
        rename must not move in between. A streaming descriptor takes no
        lock: its close only releases the session.
        """
        while True:
            with self._fd_lock:
                entry = self._fd_map.get(fh)
            if entry is None or entry.prefetch_session is not None:
                return self._release(path, fh)
            locked_path = entry.canonical_path
            with self._path_locked(locked_path):
                with self._fd_lock:
                    current = self._fd_map.get(fh)
                    moved = current is not None and current.canonical_path != locked_path
                if not moved:
                    return self._release(path, fh)

    def _release(self, path, fh):
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
            self.cache_manager.flush_file(entry.virtual_path)

            if entry.backend_name:
                backend = self.backends.get(entry.backend_name)
                if backend:
                    try:
                        backend.copy_from_local(entry.cache_path, entry.canonical_path)
                        backend.sync()
                        # Hashed on the member's worker, not this thread.
                        self._schedule_checksum_task(backend, entry.canonical_path, replace=True)
                        # The member's listed size and mtime are now old;
                        # a size split on the name drops the parent's
                        # listing (see NamespaceService.forget_listed_attrs).
                        self.namespace.forget_listed_attrs(entry.virtual_path)
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

        if name == self.VERIFY_XATTR:
            return json.dumps(self._verify_status(resolved.canonical_path), sort_keys=True).encode()

        if name == self.CACHE_STATE_XATTR:
            payload = self._cache_state_payload(resolved)
            return json.dumps(payload, sort_keys=True).encode("utf-8")

        try:
            return os.getxattr(str(resolved.cache_path), name)
        except OSError as e:
            raise FuseOSError(e.errno)

    def setxattr(self, path, name, value, options, position=0):
        """Set extended attribute"""
        if name in {self.CHECKSUM_XATTR, self.CACHE_STATE_XATTR, self.CACHE_METADATA_XATTR,
                    self.MEMBER_STAT_XATTR}:
            # Ignore manual writes for derived attrs.
            return
        if name == self.VERIFY_XATTR:
            if not stat.S_ISDIR(self.getattr(path)["st_mode"]):
                raise FuseOSError(errno.ENOTDIR)
            self._force_verify(self._resolve_path(path).canonical_path)
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
        if has_checksum or self.namespace.size_split_markers(resolved.canonical_path)[0]:
            attrs.add(self.CHECKSUM_XATTR)

        attrs.add(self.CACHE_STATE_XATTR)
        return list(attrs)

    def removexattr(self, path, name):
        """Remove extended attribute"""
        if name in {self.CHECKSUM_XATTR, self.CACHE_STATE_XATTR, self.CACHE_METADATA_XATTR,
                    self.MEMBER_STAT_XATTR}:
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

    # Symlinks are refused until they replicate (ruling
    # oq-symlink-through-mount B; ditz symlink-replication): a link kept
    # only in the cache would vanish with it.
    _SYMLINKS_ENABLED = False

    def symlink(self, target, source):
        """Create symbolic link: `ln -s source target`; refused with EPERM.

        symlink(2) documents EPERM for a filesystem that does not support
        symbolic links. fusepy passes the new link's path first and its
        contents second; _symlink keeps that order for when links
        replicate.
        """
        if not self._SYMLINKS_ENABLED:
            raise FuseOSError(errno.EPERM)
        resolved = self._resolve_path(target)
        with self._path_locked(resolved.canonical_path):
            return self._symlink(resolved, source)

    def _symlink(self, resolved: ResolvedPath, source):
        cache_path = resolved.cache_path

        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            os.symlink(source, str(cache_path))
        except OSError as e:
            raise FuseOSError(e.errno)

        # Like create: a listing taken before the link would miss it, and
        # any listing kept under the new path itself is now wrong.
        self._invalidate_dir_cache_entry(resolved.parent_dir)
        self._invalidate_dir_cache_entry(resolved.canonical_path)

    # Time operations

    def utimens(self, path, times=None):
        """Update file timestamps"""
        with self._path_locked(self._resolve_path(path).canonical_path):
            return self._utimens(path, times)

    def _utimens(self, path, times=None):
        """Update file timestamps"""
        resolved = self._resolve_path(path)
        cache_path = resolved.cache_path

        try:
            before = os.lstat(str(cache_path))
            os.utime(str(cache_path), times)
            after = os.lstat(str(cache_path))
        except OSError as e:
            raise FuseOSError(e.errno)

        # The index row and the xattr both record a cached file's mtime; a
        # touch that moves it must be recorded as a modification, or the file
        # looks like a lost write to heal_stale_clean at the next open. Only
        # regular files are tracked, and an atime-only change is not a
        # modification.
        if (
            not resolved.backend_name
            and stat.S_ISREG(after.st_mode)
            and after.st_mtime != before.st_mtime
        ):
            self._mark_cache_file_dirty(resolved.canonical_path, cache_path)

    def sweep_stale_cache_metadata(self) -> int:
        """Mark DIRTY every cached file written after its row last said CLEAN.

        Run after an unclean shutdown: a write whose bytes landed but whose
        row update was lost leaves a CLEAN row with an older size or mtime
        than the file. One lstat, one getxattr and one row read per cached
        file. Safe alongside FUSE operations and the sync monitor: it only
        ever records a modification through mark_file_dirty. Returns the
        number of files marked.
        """
        marked = 0
        for entry in scan_cache_entries(self.cache_manager.cache_root):
            if self.cache_manager.heal_stale_clean(
                entry.virtual_path,
                entry.cache_path,
                stat_size=entry.stat_size,
                stat_mtime=entry.stat_mtime,
            ):
                marked += 1
        if marked:
            self.logger.warning("Marked %d cached files dirty after an unclean shutdown", marked)
        return marked

    def request_stale_sweep(self) -> None:
        """Arrange for sweep_stale_cache_metadata to run once FUSE is up."""
        self._sweep_pending = True

    def _run_stale_sweep(self) -> None:
        try:
            self.sweep_stale_cache_metadata()
        except Exception:
            self.logger.exception("Stale cache sweep failed; the next mount will rerun it")
            return
        self._sweep_pending = False

    def destroy(self, path):
        """Cleanup on unmount"""
        self._streaming_manager.destroy()
        self._stop_watchers()

        self._reverify_stop.set()
        with self._checksum_lock:
            if self._persisted_refused:
                self._retry_refused_deletes()
        try:
            self._checksum_queues.shutdown()
        except Exception:
            pass
        self.namespace.shutdown()
        if self._reverify_thread is not None:
            self._reverify_thread.join(timeout=5)

        # The sweep uses the index; give it a bounded chance to finish before
        # the index closes. If it has not, _sweep_pending stays True, the pid
        # stays in the lock file, and the next mount sweeps again.
        if self._sweep_thread is not None and self._sweep_thread.is_alive():
            self._sweep_thread.join(timeout=self.SWEEP_JOIN_TIMEOUT)

        self.sync_manager.stop()
        # After the last rsync upload and the watchers: they share the masters.
        close_shared_control_sockets()
        self.state_manager.close()
        if not self._sweep_pending:
            _clear_mount_lock_pid(self.mount_lock_file)


def _clear_mount_lock_pid(lock_file) -> None:
    """Blank the pid in a held lock file to record a clean shutdown."""
    if lock_file is None or lock_file.closed:
        return
    try:
        lock_file.seek(0)
        lock_file.truncate()
        lock_file.flush()
    except (OSError, ValueError):
        logging.getLogger(__name__).exception("Could not clear the mount lock pid")


def _acquire_mount_lock(lock_path: Path, mountpoint: str):
    """Take an exclusive, non-blocking lock to prevent duplicate mounts."""
    return _acquire_mount_lock_ex(lock_path, mountpoint)[0]


def _acquire_mount_lock_ex(lock_path: Path, mountpoint: str):
    """Take the mount lock; also report whether the previous session left its pid.

    A clean unmount blanks the pid (_clear_mount_lock_pid). A pid still there
    once we hold the lock means the last session did not get that far: its
    in-flight row updates may be lost and the cache needs
    sweep_stale_cache_metadata. Read under the lock, so a session that takes
    the lock and crashes between our read and our acquisition is not missed.
    The first mount after upgrading from a version that never blanked the
    pid sweeps once. Returns (lock_file, previous_pid_left).
    """
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
        previous_pid_left = lock_file.read().strip() != ""
        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        os.fsync(lock_file.fileno())
        return lock_file, previous_pid_left
    except Exception:
        if lock_file:
            lock_file.close()
        raise


def fuse_subtype_options(backends: Dict[str, Backend]) -> Dict[str, str]:
    """Extra FUSE() keyword arguments that mark a mount with remote members.

    GLib decides whether a mount is remote from the mount-table type, and
    lists only nfs4, cifs, smb2, ncpfs, fuse.sshfs and fuse.portal as remote.
    A plain "fuse" mount reads as a local disk, so Nautilus counts items and
    makes thumbnails, and LocalSearch crawls, through it as if reads were
    cheap. A mount with any SSH or rsync-over-SSH member therefore asks for
    subtype "sshfs"; a mount of only local members keeps the plain type.
    The fsname is unchanged. Typing the mount as sshfs is deliberate: other
    tools that key on fuse.sshfs (systemd network ordering, updatedb's
    PRUNEFS) will see this mount the same way, which suits a mount that is
    backed by remote hosts. Any remote member makes the whole mount remote;
    there is no opt-out (ditz open-questions).
    """
    if any(backend.transport_is_remote for backend in backends.values()):
        return {"subtype": "sshfs"}
    return {}


def listing_fresh_window(readdir_ttl: Optional[float], revalidate_after: float) -> float:
    """Seconds a listing is fresh with watchers off: readdir_ttl when set,
    else revalidate_after, so directories follow the content policy."""
    return max(0.0, float(revalidate_after if readdir_ttl is None else readdir_ttl))


# Kernel entry timeout while watchers run (libfuse's default). The mount
# cannot evict kernel entries (see fuse_mount_options), so a watcher event
# reaches lookups only once the kernel asks again.
WATCHER_KERNEL_TIMEOUT = 1.0
# Kernel attribute timeout: the kernel asks getattr at every stat (see
# fuse_mount_options for why it is never longer).
KERNEL_ATTR_TIMEOUT = 0.0
# How long the kernel remembers that a name does not exist. Short, because a
# remembered miss hides a name that appears behind the mount's back (a file
# added on a member, or a conflict copy such as name.member0 after a split).
DEFAULT_NEGATIVE_TIMEOUT = 10.0


def _kernel_timeout(name: str, value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number of seconds (got {value!r})")
    value = float(value)
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be a finite number of seconds, 0 or more (got {value})")
    return value


def fuse_mount_options(backends: Dict[str, Backend], *, enable_watchers: bool,
                       fresh_window: float,
                       entry_timeout: Optional[float] = None,
                       negative_timeout: Optional[float] = None) -> Dict[str, object]:
    """Every FUSE() keyword argument beyond nothreads, foreground and allow_other.

    The mount and every mounted test pass these, so tests run under the
    kernel cache the mount really has.

    entry_timeout: how long the kernel trusts a name without a lookup.
    fresh_window is how long the mount trusts a directory listing with
    watchers off (listing_fresh_window: readdir_ttl, else revalidate_after);
    names are kept as long, so a path walk asks nothing for a minute. With watchers on, entries stay at 1 s: a
    watcher invalidates the mount's own listings, but fusepy runs libfuse
    2's high-level fuse_main, which owns the inode table and exposes no
    handle for fuse_lowlevel_notify_inval_entry, so nothing can evict the
    kernel's.

    attr_timeout is 0, so every stat reaches getattr, which answers from
    the cache file or the parent's listing while it is fresh or stale, and
    otherwise stats a member (see NamespaceService.getattr). The kernel refreshes a file's size
    only from expired attributes, even for a read past the size it holds:
    with any attribute timeout, a stat answered from the old cache copy
    just before an open that refetches a file grown on its member makes
    the reads stop at the old size until the attributes expire (a copy out
    of the mount comes out truncated). Not even libfuse's 1 s default is
    safe; tests/test_sync_system.py TestKernelCacheRefetch shows it. So
    it has no override.

    negative_timeout: a missing name is remembered for 10 s. Short, because
    a remembered miss hides a name added behind the mount's back (on a
    member, or a conflict copy such as name.member0 after a split).

    auto_cache keeps a file's pages across opens while its mtime and size
    are unchanged and drops them otherwise, so a refetch after revalidation
    is not read through old pages (kernel_cache would keep them). libfuse
    compares against attributes it fetched within ac_attr_timeout, which
    defaults to attr_timeout; ac_attr_timeout=0 states that a getattr
    follows every open rather than leaving it implied.

    Writes through the mount stay visible at once: create, mkdir, unlink,
    rmdir and rename update the kernel's entries themselves. (symlink is
    refused for now and creates nothing.)

    The override arguments replace the defaults (per profile); each, and
    fresh_window, must be finite and 0 or more.
    """
    window = _kernel_timeout("fresh_window", fresh_window)
    if enable_watchers:
        window = WATCHER_KERNEL_TIMEOUT
    options: Dict[str, object] = dict(fuse_subtype_options(backends))
    options["attr_timeout"] = KERNEL_ATTR_TIMEOUT
    options["entry_timeout"] = window if entry_timeout is None else _kernel_timeout("entry_timeout", entry_timeout)
    options["negative_timeout"] = (
        DEFAULT_NEGATIVE_TIMEOUT if negative_timeout is None
        else _kernel_timeout("negative_timeout", negative_timeout)
    )
    options["auto_cache"] = True
    options["ac_attr_timeout"] = 0.0
    return options


def _discard_sigpipe(signum, frame) -> None:
    """A SIGPIPE handler that does nothing: the failed write raises EPIPE."""


@contextlib.contextmanager
def _sigpipe_survives_fuse():
    """Keep a write to a dead socket an EPIPE error through FUSE()'s teardown.

    Python starts with SIGPIPE ignored. libfuse 2's teardown
    (fuse_remove_signal_handlers) resets a SIGPIPE disposition it finds
    ignored to the default, behind Python's back (signal.getsignal() still
    says SIG_IGN), and it does so before it runs the filesystem's destroy():
    closing an SSH transport or streaming socket whose peer is gone, there
    or afterwards, would end the process with exit 141 instead of raising
    BrokenPipeError. libfuse leaves a handler alone, so install a no-op one
    for the duration of the mount and ignore it again afterwards.

    Only an ignored SIGPIPE is at risk; a handler or the default the caller
    chose is left in place for the whole mount. signal.signal() works only
    on the main thread (the CLI's); a mount run from another thread cannot
    be guarded and leaves SIGPIPE to the caller.
    """
    if (threading.current_thread() is not threading.main_thread()
            or signal.getsignal(signal.SIGPIPE) != signal.SIG_IGN):
        yield
        return
    signal.signal(signal.SIGPIPE, _discard_sigpipe)
    try:
        yield
    finally:
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)


def mount_storage(mountpoint: str, cache_root: Path, state_db: Path,
                 backends: Dict[str, Backend], cache_size: int,
                 foreground: bool = False, num_sync_workers: int = 2,
                 allow_other: Optional[bool] = None,
                 debug: bool = False,
                 streaming_reads: bool = False,
                 streaming_chunk_size: int = 1024 * 1024,
                 max_streaming_sessions: int = 8,
                 readdir_ttl: Optional[float] = None,
                 enable_watchers: bool = False,
                 streaming_keep_after_close: Optional[StreamingKeepAfterClose] = None,
                 revalidate_after: float = 60.0,
                 listing_max_stale: float = DEFAULT_LISTING_MAX_STALE,
                 entry_timeout: Optional[float] = None,
                 negative_timeout: Optional[float] = None,
                 fuse_threads: bool = False,
                 checksum_reverify_interval: float = DEFAULT_CHECKSUM_REVERIFY_INTERVAL):
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
        streaming_keep_after_close: When read-ahead outlives the last close;
            None for the default
        revalidate_after: Seconds after which opening a CLEAN cached file
            stats its member (see StorageFS._revalidate_cached_file)
        readdir_ttl, listing_max_stale: listing freshness (see StorageFS)
        checksum_reverify_interval: seconds between re-verifications of
            persisted member hashes; 0 turns them off (see StorageFS)
        entry_timeout, negative_timeout: kernel cache timeouts in seconds;
            None derives them (see fuse_mount_options)
        fuse_threads: Run libfuse's multithreaded loop instead of nothreads.
            The handlers lock as storage/locks.py describes. libfuse 2 has no
            thread cap (a worker is started whenever none is idle, and idle
            workers above 10 exit), so a burst of requests can start as many
            threads, each waiting on a path lock or a member channel; the
            member transport pools bound only what reaches the members
            (ditz threading-fuse-worker-cap). On unmount libfuse joins every
            worker before destroy, so an unmount waits for the slowest member
            call in flight.

    Raises:
        PermissionError: If allow_other=True but not running as root and user_allow_other not set
    """
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    # Before the mount lock, so a bad timeout fails without side effects.
    fuse_options = fuse_mount_options(
        backends,
        enable_watchers=enable_watchers,
        fresh_window=listing_fresh_window(readdir_ttl, revalidate_after),
        entry_timeout=entry_timeout,
        negative_timeout=negative_timeout,
    )

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
        lock_file, unclean_shutdown = _acquire_mount_lock_ex(lock_path, mountpoint)
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
        streaming_keep_after_close=streaming_keep_after_close,
        revalidate_after=revalidate_after,
        listing_max_stale=listing_max_stale,
        checksum_reverify_interval=checksum_reverify_interval,
    )
    fs.mount_lock_file = lock_file
    if unclean_shutdown:
        logger.warning(
            "Previous session of %s did not shut down cleanly; the cache will be swept", mountpoint,
        )
        fs.request_stale_sweep()  # started by init(), after FUSE's fork
    try:
        with _sigpipe_survives_fuse():
            FUSE(fs, mountpoint, nothreads=not fuse_threads, foreground=foreground, allow_other=allow_other,
                 **fuse_options)
    finally:
        try:
            if not fs._sweep_pending:
                _clear_mount_lock_pid(lock_file)
            lock_file.close()
        except Exception:
            logger.exception("Exception occurred while closing mount lock file")
