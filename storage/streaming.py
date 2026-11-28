import errno
import json
import os
import re
import resource
import secrets
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from fuse import FuseOSError

from .backends.base import Backend, capacity_wait_cancellation
from .backends.ssh_pool import BulkCapacityCancelled, BulkCapacityTimeout, is_connection_error
from .backends.local import NOREPLACE_UNSUPPORTED
from .backends.local import rename_noreplace as _rename_noreplace
from .cache_metadata import PREFIX_XATTR, copy_member_attributes
from .control import ControlError, ControlServer
from .models import FileLocation, FileMetadata, StreamingKeepAfterClose, SyncStatus
from .streaming_settings import (
    DEFAULT_STREAMING_KEEP_AFTER_CLOSE, DEFAULT_STREAMING_READ_AHEAD_WINDOW,
    parse_keep_after_close, parse_read_ahead_window,
)

# A foreground read fetches only the bytes the reader asked for that are not
# already there or being fetched; streaming_chunk_size is the unit of the
# background worker's read-ahead. The worker starts only once the readers have
# shown they are consuming the file: a contiguous span of this many chunks,
# or the keep-after-close threshold below if that is smaller (so a file the
# threshold calls consumed always gets read-ahead). A content-type sniff
# reads a few KiB (the kernel turns that into at most its readahead, 128K by
# default) and so costs about what it reads, as on a plain sftp mount, with
# nothing speculative; a copy or a player crosses one chunk in its first
# second. Read-ahead, once started, follows the readers for the rest of the
# session.
STREAMING_READ_AHEAD_START_CHUNKS = 1
# Past this many disjoint ranges in a session, foreground reads go back to
# fetching the whole chunk around them. A sniff or a thumbnailer makes a
# handful of reads; a reader making this many scattered ones (a database, an
# archive index) gets chunk locality again, and the range list, which every
# read walks under the session lock, stays near this plus one per chunk
# instead of growing with every read.
STREAMING_MAX_FOREGROUND_FRAGMENTS = 1024

# Bound on the spans tracked per session before the threshold is reached;
# past it the shortest span is dropped. Only a reader making that many
# scattered reads gets here, and losing a short span can only delay the
# threshold, never fake it.
STREAMING_MAX_TRACKED_READ_SPANS = 64
DOWNLOAD_WORKER_IDLE_SECONDS = 30.0
# Yield a turn to a queued closed file after this many active-reader turns.
ACTIVE_DOWNLOAD_BURST = 4
# Automatic worker mode: turns at once on a member that reports no
# background_range_capacity of its own (local directories). SSH and rsync
# members are bounded by their bulk SFTP capacity instead.
AUTOMATIC_UNLIMITED_MEMBER_WORKERS = 4
# Retained fills stop being kept once registered sessions (one partial
# descriptor each) reach 1/this of the soft RLIMIT_NOFILE.
STREAMING_DESCRIPTOR_SHARE = 2
# How long an open waits for a claimed failed-session cleanup (a local
# truncate, fsync and link of the prefix) before it returns EAGAIN.
STREAMING_CLEANUP_WAIT_SECONDS = 30.0

# link() errors after which _install_partial (and StorageFS's prefix
# restore) tries RENAME_NOREPLACE: no hard links on the cache filesystem (EMLINK cannot happen on a fresh
# partial, but the rename would work there too); renameat2 errors meaning
# it has no RENAME_NOREPLACE either.
LINK_UNSUPPORTED = (errno.EPERM, errno.EOPNOTSUPP, errno.ENOTSUP, errno.ENOSYS, errno.EMLINK)
_NOREPLACE_UNSUPPORTED = NOREPLACE_UNSUPPORTED


OBSOLETE_STREAMING_LIMITS = ("max_streaming_sessions", "max_retained_streaming_sessions")


def validate_download_limits(limits):
    for name, value in limits.items():
        if name in OBSOLETE_STREAMING_LIMITS:
            raise ValueError(f"obsolete streaming setting: {name}")
        if name != "max_download_workers":
            raise ValueError(f"unknown download limit: {name}")
        if value is not None and (type(value) is not int or value < 1):
            raise ValueError(f"{name} must be a positive integer or automatic")


class StreamingOpen(Enum):
    CACHED = auto()
    # The cache path holds a failed stream's promoted prefix that the index
    # does not describe: not the file. The opener drops it and tries again.
    PREFIX_CACHED = auto()
    RANGE_UNSUPPORTED = auto()
    SOURCE_MISSING = auto()


# The largest file size a partial can be truncated to (off_t is signed 64-bit).
_MAX_PARTIAL_SIZE = (1 << 63) - 1


@dataclass(frozen=True)
class StreamSource:
    backend: Backend
    # None when the member has no stat. Both are None for a member without
    # range reads: it is never probed for a size it does not need.
    member_stat: Optional[os.stat_result]
    file_size: Optional[int]


class StreamSourceError(OSError):
    """No streaming source could be probed: the cause's errno, raised at open.

    Distinct from the other open errors so StorageFS can still recompute a
    recomputable file, as the whole-file path does when no member serves it.
    """


class _CleanupPending(Exception):
    """The registered session's failed cleanup is claimed; wait for it to leave the registry."""

    def __init__(self, session):
        super().__init__("streaming cleanup in progress")
        self.session = session


def streaming_read_errno(exc: Exception) -> int:
    """The errno read(2) returns for exc: a file gone from the member mid-read is stale, not absent.

    ENOENT from read(2) would tell the caller its open file does not exist.
    """
    if isinstance(exc, FileNotFoundError):
        return errno.ESTALE
    error = streaming_errno(exc)
    return errno.ESTALE if error == errno.ENOENT else error


def streaming_errno(exc: Exception) -> int:
    if isinstance(exc, BulkCapacityCancelled):
        return errno.EINTR
    if isinstance(exc, BulkCapacityTimeout):
        return errno.EAGAIN
    if isinstance(exc, OSError) and exc.errno:
        return exc.errno
    if isinstance(exc, TimeoutError):
        return errno.ETIMEDOUT
    if isinstance(exc, ConnectionError) or is_connection_error(exc):
        return errno.EHOSTUNREACH
    if isinstance(exc, MemoryError):
        return errno.ENOMEM
    return errno.EIO


def _unique_control_fields(pairs):
    fields = dict(pairs)
    if len(fields) != len(pairs):
        raise ControlError("Duplicate field in control request")
    return fields


@dataclass(frozen=True)
class _ReadAheadSettings:
    keep_after_close: StreamingKeepAfterClose
    window_bytes: int


@dataclass(eq=False)
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
    session_id: str = field(default_factory=lambda: secrets.token_hex(16))
    available_ranges: List[Tuple[int, int]] = field(default_factory=list)
    # Byte ranges claimed by a fetch in progress, foreground or background.
    # A range is claimed only where it is missing and not already claimed,
    # so no byte is requested from the member twice.
    in_flight_ranges: List[Tuple[int, int]] = field(default_factory=list)
    refcount: int = 0
    prefetch_complete: bool = False
    finalized: bool = False
    stop_requested: bool = False
    cache_full: bool = False
    error: Optional[Exception] = None
    background_cursor: int = 0
    background_wrapped: bool = False
    has_foreground_read: bool = False
    # Set (claimed under session.lock, inside prefetch_sessions_lock; see
    # _claim_detach) when the session leaves the registry
    # for good: stopped by the last reader's close, or cleaned up after a
    # failure or cancel. Its partial file is already unlinked or promoted, so
    # it no longer owns the partial path: a new session for the same file may
    # be using it. Cleanup, promotion and finalize skip the path once this is
    # set, and its worker only closes its own descriptor. State-xattr writes
    # check it too, but not atomically with the write (see
    # _update_streaming_state_xattr).
    detached: bool = False
    # Set by the worker's tail once it no longer writes the partial, in the
    # same session.lock section where it reads refcount to decide whether it
    # cleans up a failed session. A release decides in the section where it
    # drops its ref: it cleans up a failed session only if this is already
    # set, and otherwise leaves it to the tail, which will then see no
    # readers. Exactly one of the two sees both conditions.
    worker_done: bool = False
    # Claimed (see _claim_failed_cleanup) in the session.lock section that
    # decides a failed session has no readers left, by the worker's tail or
    # the last release; the claimer alone promotes and removes the session.
    # Opens refuse to attach once it is set, so no reader joins a session
    # whose partial is about to be truncated and renamed.
    failed_cleanup_claimed: bool = False
    # Merged spans the readers asked for, until readers_consumed is set.
    foreground_read_spans: List[Tuple[int, int]] = field(default_factory=list)
    # Set once a span reaches the read-ahead start (see
    # STREAMING_READ_AHEAD_START_CHUNKS); the worker fetches nothing before.
    read_ahead_started: bool = False
    readers_consumed: bool = False
    last_logged_status: str = ""
    last_logged_downloaded_bytes: int = 0
    last_foreground_offset: int = 0
    # The member's stat at session start (None when the member has no
    # stat); handed to on_finalized as the revalidation baseline.
    member_stat: Optional[object] = None
    # Demand access can arrive before the index row exists. Carry the last
    # timestamp into the new row even if the pending index batch has flushed.
    last_access: float = 0.0

    def __post_init__(self):
        self.lock = threading.RLock()
        self.io_lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.background_finished = threading.Event()


class _DownloadPool:
    """One turn per session, with priority reconsidered between chunks.

    Callers update a session's readiness under its lock. Workers release
    the pool condition before touching a session, so parked sessions hold
    neither a worker nor a network channel. FIFO within each priority gives
    separate readers turns even if one is far ahead.
    """

    def __init__(self, limit, run_turn, logger):
        self.limit = limit
        self.run_turn = run_turn
        self.logger = logger
        self.idle_seconds = DOWNLOAD_WORKER_IDLE_SECONDS
        self.condition = threading.Condition()
        self.ready: dict[PrefetchSession, bool] = {}
        self.running: set[PrefetchSession] = set()
        self.threads: set[threading.Thread] = set()
        self.active_turns = 0
        self.closing = False

    def _member_capacity(self, session):
        """Concurrent turns allowed on the session's member; None for no member bound.

        A member that sets no limit of its own (a local directory) gets
        AUTOMATIC_UNLIMITED_MEMBER_WORKERS while the pool is automatic, so
        demand alone (one ready file each) cannot start a thread per
        retained fill. Under an explicit max_download_workers that limit is
        such a member's only bound, as before.
        """
        capacity = session.backend.background_range_capacity()
        if capacity is None and self.limit is None:
            return AUTOMATIC_UNLIMITED_MEMBER_WORKERS
        return capacity

    def _eligible(self):
        running_by_member = Counter(session.backend.name for session in self.running)
        for session in self.ready:
            member = session.backend.name
            capacity = self._member_capacity(session)
            if capacity is None or running_by_member[member] < capacity:
                yield session
                running_by_member[member] += 1

    def _grow(self):
        demand = len(self.running) + sum(1 for _ in self._eligible())
        retried = False
        target = demand if self.limit is None else min(self.limit, demand)
        while len(self.threads) < target:
            worker = None
            try:
                worker = threading.Thread(target=self._run, name="storage-prefetch-worker", daemon=True)
                self.threads.add(worker)
                worker.start()
            except (RuntimeError, OSError, MemoryError) as exc:
                self.threads.discard(worker)
                # Thread.start may report resource exhaustion as RuntimeError without an errno.
                error = errno.ENOMEM if isinstance(exc, MemoryError) else getattr(exc, "errno", None)
                self.logger.warning("streaming worker start failed errno=%s", error or "unknown")
                # There may never be another offer to retry the first
                # worker, e.g. when a complete file is queued to publish.
                if self.threads or retried:
                    break
                retried = True

    def offer(self, session, ready, finished_turn=False):
        with self.condition:
            if finished_turn:
                self.running.remove(session)
            if session not in self.running:
                if ready and not self.closing:
                    self.ready[session] = session.refcount > 0
                    self._grow()
                else:
                    self.ready.pop(session, None)
            self.condition.notify_all()

    def set_limit(self, limit):
        with self.condition:
            self.limit = limit
            self._grow()
            self.condition.notify_all()

    def cancel(self, session):
        """Withdraw queued work; return True if no worker owns the session."""
        with self.condition:
            self.ready.pop(session, None)
            return session not in self.running

    def take_unstaffed(self, session):
        """Let a caller finish a local-only publication when no thread starts."""
        with self.condition:
            if self.threads or session not in self.ready:
                return False
            self.ready.pop(session)
            self.running.add(session)
            return True

    def snapshot(self):
        with self.condition:
            return len(self.threads), len(self.running), len(self.ready)

    def _take(self):
        if self.limit is not None and len(self.running) >= self.limit:
            return None
        active = closed = None
        # Member headroom is based on in-flight work. Each priority gets
        # its first choice of that headroom before either is selected.
        running_by_member = Counter(session.backend.name for session in self.running)
        for session, has_reader in self.ready.items():
            capacity = self._member_capacity(session)
            if capacity is not None and running_by_member[session.backend.name] >= capacity:
                continue
            if has_reader and active is None:
                active = session
            if not has_reader and closed is None:
                closed = session
            if active is not None and closed is not None:
                break
        if closed is not None and (active is None or self.active_turns >= ACTIVE_DOWNLOAD_BURST):
            session = closed
            self.active_turns = 0
        else:
            session = active
            self.active_turns = self.active_turns + 1 if closed is not None else 0
        if session is not None:
            self.ready.pop(session)
            self.running.add(session)
        return session

    def _run(self):
        while True:
            with self.condition:
                idle_until = time.monotonic() + self.idle_seconds
                while (session := self._take()) is None:
                    remaining = idle_until - time.monotonic()
                    if self.closing or remaining <= 0:
                        self.threads.remove(threading.current_thread())
                        return
                    self.condition.wait(remaining)
            try:
                self.run_turn(session)
            except Exception:
                self.logger.exception("Streaming download turn failed for %s", session.canonical_path)
                with self.condition:
                    self.running.discard(session)
                    self.condition.notify_all()

    def shutdown(self, timeout):
        with self.condition:
            self.closing = True
            self.condition.notify_all()
            workers = list(self.threads)
        deadline = time.monotonic() + timeout
        for worker in workers:
            worker.join(timeout=max(0.0, deadline - time.monotonic()))


class StreamingReadManager:
    """Owns streaming read sessions, range fetches, and control socket state."""

    def __init__(
        self,
        *,
        cache_manager,
        state_manager,
        logger,
        get_config,
        select_stream_source: Callable[[str], Optional[StreamSource]],
        streaming_reads: bool,
        streaming_chunk_size: int,
        partial_suffix: str,
        stream_state_xattr: str,
        keep_after_close: Optional[StreamingKeepAfterClose] = None,
        on_finalized: Optional[Callable[["PrefetchSession"], None]] = None,
        max_download_workers: Optional[int] = None,
        read_ahead_window: int = DEFAULT_STREAMING_READ_AHEAD_WINDOW,
        is_unindexed_prefix: Optional[Callable[[str, Path], bool]] = None,
    ):
        self.cache_manager = cache_manager
        # Called once the complete file is in the cache (StorageFS records
        # the member baseline for revalidation on open).
        self.on_finalized = on_finalized
        self.state_manager = state_manager
        self.logger = logger
        self.get_config = get_config
        # Probes the members for a source: None when no member has the
        # file; raises when none could be probed.
        self.select_stream_source = select_stream_source
        # Whether the file at a cache path is a promoted prefix the index
        # does not describe (StorageFS._is_unindexed_prefix).
        self.is_unindexed_prefix = is_unindexed_prefix or (lambda canonical_path, cache_path: False)
        # Descriptors held by streaming sessions and their staging files,
        # registered or not (a replaced session's readers keep one open).
        self._session_descriptors = 0
        self._session_descriptors_lock = threading.Lock()
        self.streaming_reads = streaming_reads
        self.chunk_size = max(4096, int(streaming_chunk_size))
        limits = {"max_download_workers": max_download_workers}
        validate_download_limits(limits)
        self._download_limits = limits
        self._download_pool = _DownloadPool(
            max_download_workers,
            self._run_download_turn,
            logger,
        )
        if type(read_ahead_window) is not int or read_ahead_window < 0:
            raise ValueError("read-ahead window must be a non-negative byte count")
        if keep_after_close is None:
            keep_after_close = parse_keep_after_close(DEFAULT_STREAMING_KEEP_AFTER_CLOSE)
        self._read_ahead_settings = _ReadAheadSettings(keep_after_close, read_ahead_window)
        self.partial_suffix = partial_suffix
        self.stream_state_xattr = stream_state_xattr

        self.prefetch_sessions: Dict[str, PrefetchSession] = {}
        self.prefetch_sessions_lock = threading.RLock()
        # Set under prefetch_sessions_lock when destroy() starts; no session
        # registers or is attached to after that.
        self._destroying = False
        self._warned_no_noreplace = False
        self._warned_no_noreplace_lock = threading.Lock()
        self._streaming_stats_lock = threading.Lock()
        self._streaming_stats = {
            "streaming_sessions_started": 0,
            "streaming_fallback_opens": 0,
            "streaming_open_error_source_probe": 0,
            "streaming_open_error_descriptors": 0,
            "streaming_open_error_cache_space": 0,
            "foreground_remote_bytes": 0,
            "background_remote_bytes": 0,
            "foreground_fetches": 0,
            "background_fetches": 0,
        }
        self._control_server = ControlServer(
            self.cache_manager.cache_root, self._handle_control_request, self.logger,
        )

    def init(self):
        self._control_server.start()

    def _download_limits_snapshot(self):
        """Serialize the current settings. Caller holds prefetch_sessions_lock."""
        settings = self._read_ahead_settings
        qualification = settings.keep_after_close
        return {
            **self._download_limits,
            "streaming_keep_after_close": {
                "mode": "all" if qualification.match_all else "any",
                "bytes": list(qualification.byte_thresholds),
                "percent": [str(value) for value in qualification.percent_thresholds],
            },
            "streaming_read_ahead_window": settings.window_bytes,
        }

    def download_limits(self):
        with self.prefetch_sessions_lock:
            return self._download_limits_snapshot()

    def update_download_limits(self, **changes):
        read_ahead_changes = {}
        for key, parse in (
            ("streaming_keep_after_close", parse_keep_after_close),
            ("streaming_read_ahead_window", parse_read_ahead_window),
        ):
            if key in changes:
                try:
                    read_ahead_changes[key] = parse(changes.pop(key))
                except ValueError as exc:
                    raise ValueError(f"{key}: {exc}") from exc
        validate_download_limits(changes)

        with self.prefetch_sessions_lock:
            if self._destroying:
                raise ValueError("mount is shutting down")
            previous = self._read_ahead_settings
            settings = _ReadAheadSettings(
                read_ahead_changes.get("streaming_keep_after_close", previous.keep_after_close),
                read_ahead_changes.get("streaming_read_ahead_window", previous.window_bytes),
            )
            self._download_limits.update(changes)
            self._read_ahead_settings = settings
            if "max_download_workers" in changes:
                self._download_pool.set_limit(changes["max_download_workers"])
            if settings != previous:
                for session in self.prefetch_sessions.values():
                    with session.lock:
                        if session.readers_consumed or self._download_terminal(session):
                            continue
                        self._refresh_read_ahead_qualification(session, settings.keep_after_close)
                        self._schedule_background(session)
            return self._download_limits_snapshot()

    def list_download_sessions(self):
        with self.prefetch_sessions_lock:
            sessions = []
            for session in self.prefetch_sessions.values():
                with session.lock:
                    sessions.append({
                        "id": session.session_id,
                        "path": session.virtual_path,
                        "backend": session.backend.name,
                        "readers": session.refcount,
                        "size_bytes": session.file_size,
                        "downloaded_bytes": sum(end - start for start, end in session.available_ranges),
                        "status": self._download_status(session),
                    })
            return sessions

    @staticmethod
    def _download_status(session):
        if session.finalized:
            return "complete"
        if session.error is not None:
            return "error"
        if session.prefetch_complete:
            return "finalizing"
        if not session.has_foreground_read:
            return "awaiting-read"
        return "reading" if session.refcount > 0 else "retained"

    def cancel_download_session(self, selector: str):
        with self.prefetch_sessions_lock:
            for session in self.prefetch_sessions.values():
                if session.session_id != selector:
                    continue
                with session.lock:
                    if session.refcount > 0 or self._download_terminal(session):
                        return "busy"
                    idle = self._stop_background_locked(session)
                break
            else:
                return "not-found"
        self._complete_cancellation(session, idle)
        return "cancelled"

    def destroy(self):
        """Stop every session and reject subsequent streaming opens.

        The refusal is set in the same prefetch_sessions_lock section that
        takes the sessions, so an open racing destroy either registered
        before it (and is stopped here) or registers nothing. Each session
        is detached by claim, as on any other way out of the registry, so
        its partial is unlinked once and a cleanup still running elsewhere
        skips the path. A session whose bytes have all arrived is left to
        finish its finalize while destroy drains the worker pool (see
        _claim_detach for why a complete session is never stopped). One
        whose worker outlives the wait is claimed anyway, against that
        rule, and that is safe only because no successor can register now:
        an unlink before finalize's link makes the link fail (the session
        errors), one after it removes the partial's second name only.

        An open that registered before destroy but has not yet opened the
        partial finds it unlinked and fails with ENOENT, a clean error.
        libfuse runs destroy after its loop has stopped serving requests,
        so the multithreaded loop does not reach this either.
        """
        self._control_server.stop()

        with self.prefetch_sessions_lock:
            self._destroying = True
            sessions = list(self.prefetch_sessions.values())
            for session in sessions:
                self._detach_for_destroy(session)

        self._download_pool.shutdown(timeout=2.0)
        # If thread creation failed, provide the same local publication for
        # fully fetched files. In-flight turns still own their descriptors.
        for session in sessions:
            self._finish_unstaffed_download(session)
        with self.prefetch_sessions_lock:
            for session in sessions:
                self._detach_for_destroy(session, complete_too=True)
        if sessions:
            self.cache_manager.mark_cache_size_dirty()

    def _detach_for_destroy(self, session: PrefetchSession, complete_too: bool = False):
        """Stop the session and, unless it is finishing, claim it and unlink its partial.

        Caller holds prefetch_sessions_lock. Without complete_too a session
        whose bytes have all arrived can still finalize.
        """
        with session.lock:
            if session.prefetch_complete and not complete_too:
                return
            session.stop_requested = True
            session.condition.notify_all()
            if not session.finalized and self._claim_detach(session):
                self._unregister_and_unlink(session)
            idle = self._download_pool.cancel(session)
        if idle:
            self._close_cancelled_download(session)

    def cleanup_stale_partials(self):
        """Delete stale partial-cache files left by prior crashes.

        Startup only (StorageFS.__init__, before the mount serves anything):
        it removes every name ending in the partial suffix, including a live
        session's partial or staging file.
        """
        removed = 0
        for partial_path in self.cache_manager.cache_root.rglob(f"*{self.partial_suffix}"):
            try:
                partial_path.unlink()
                removed += 1
            except OSError:
                continue
        if removed:
            self.cache_manager.mark_cache_size_dirty()
            self.logger.info("Removed %d stale partial cache files", removed)

    def _partial_cache_path(self, cache_path: Path) -> Path:
        return Path(f"{cache_path}{self.partial_suffix}")

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
    def _first_missing_range(
        ranges: List[Tuple[int, int]],
        start: int,
        end: int,
    ) -> Optional[Tuple[int, int]]:
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
                "readers_consumed": bool(session.readers_consumed),
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
                status,
                session.canonical_path,
                session.backend.name,
                downloaded,
                file_size,
                contiguous,
                readers,
                error_type,
                error_text,
            )
        else:
            self.logger.info(
                "streaming %s path=%s backend=%s downloaded=%d/%d contiguous=%d readers=%d",
                status,
                session.canonical_path,
                session.backend.name,
                downloaded,
                file_size,
                contiguous,
                readers,
            )

    def _update_streaming_state_xattr(self, session: PrefetchSession, status: str):
        payload = self._snapshot_streaming_state(session, status)
        self._log_streaming_state_change(session, payload)

        # Written through the session's own descriptor, under io_lock so
        # it stays open: a successor's partial at the same path (after a
        # replacement, _attach) is never written, whatever the timing. Once
        # the descriptor is closed only a finalized session writes, to its
        # final path. A detached session writes nothing; its state is no
        # longer anyone's.
        value = json.dumps(payload, sort_keys=True).encode("utf-8")
        with session.io_lock:
            with session.lock:
                if session.detached:
                    return
                io_fd = session.io_fd
                target_path = session.final_cache_path if session.finalized else None
            try:
                if io_fd >= 0:
                    # The session's own inode: a successor's partial at the
                    # same path is never written, detached or not.
                    os.setxattr(io_fd, self.stream_state_xattr, value)
                elif target_path is not None:
                    os.setxattr(str(target_path), self.stream_state_xattr, value)
            except OSError:
                pass

    def _streaming_stats_snapshot(self) -> Dict[str, object]:
        with self._streaming_stats_lock:
            stats = dict(self._streaming_stats)

        with self.prefetch_sessions_lock:
            active_reader_sessions, retained_sessions = self._download_session_counts()
            active_streaming_sessions = len(self.prefetch_sessions)
        workers, running, queued = self._download_pool.snapshot()

        stats.update({
            "active_streaming_sessions": active_streaming_sessions,
            "streaming_session_descriptors": self.session_descriptor_count(),
            "active_reader_sessions": active_reader_sessions,
            "retained_streaming_sessions": retained_sessions,
            "download_workers": workers,
            "download_workers_running": running,
            "download_sessions_queued": queued,
            "pid": os.getpid(),
            "timestamp": time.time(),
            "streaming_enabled": bool(self.streaming_reads),
        })
        return stats

    def _control_parameters(self, limits: dict) -> dict:
        return {
            "limits": limits,
            "restart_only": {
                "streaming_reads": bool(self.streaming_reads),
                "streaming_chunk_size": self.chunk_size,
            },
        }

    def _handle_control_request(self, request: str) -> dict:
        if request.strip() == "streaming-stats":  # Backward-compatible request.
            return self._streaming_stats_snapshot()
        try:
            arguments = json.loads(request, object_pairs_hook=_unique_control_fields)
        except ControlError:
            raise
        except (ValueError, RecursionError) as exc:
            raise ControlError("Expected a JSON control request object") from exc
        if not isinstance(arguments, dict):
            raise ControlError("Expected a JSON control request object")

        command = arguments.pop("command", None)
        if command in ("status", "streaming.get", "downloads.list") and arguments:
            raise ControlError("Unexpected arguments for control command")
        if command == "status":
            return {
                "stats": self._streaming_stats_snapshot(),
                **self._control_parameters(self.download_limits()),
            }
        if command == "streaming.get":
            return self._control_parameters(self.download_limits())
        if command == "streaming.set":
            changes = arguments.get("limits")
            if set(arguments) != {"limits"} or not isinstance(changes, dict) or not changes:
                raise ControlError("Provide at least one live streaming limit to set")
            try:
                return self._control_parameters(self.update_download_limits(**changes))
            except ValueError as exc:
                raise ControlError(str(exc)) from exc
        if command == "downloads.list":
            return {"downloads": self.list_download_sessions()}
        if command == "downloads.cancel":
            session_id = arguments.get("id")
            if (set(arguments) != {"id"} or not isinstance(session_id, str)
                    or re.fullmatch(r"[0-9a-f]{32}", session_id) is None):
                raise ControlError("Provide a complete download ID from 'storage control ... downloads'")
            return {"id": session_id, "result": self.cancel_download_session(session_id)}
        raise ControlError("Unknown mount control command")

    def _record_streaming_stat(self, key: str, value: int = 1):
        with self._streaming_stats_lock:
            self._streaming_stats[key] = self._streaming_stats.get(key, 0) + value

    def _write_session_bytes(self, session: PrefetchSession, offset: int, data: bytes) -> bool:
        with session.io_lock:
            if session.stop_requested or session.io_fd < 0:
                return False
            try:
                cached = self.cache_manager.write_streaming_chunk(session.io_fd, offset, data)
            except OSError as exc:
                if exc.errno not in (errno.ENOSPC, errno.EDQUOT):
                    raise
                cached = False
            with session.lock:
                session.cache_full = not cached
                if cached:
                    self._add_available_range(session.available_ranges, offset, offset + len(data))
                    if self._is_range_available(session.available_ranges, 0, session.file_size):
                        session.prefetch_complete = True
                session.condition.notify_all()
            return cached

    def _fetch_range_into_session(
        self,
        session: PrefetchSession,
        start: int,
        end: int,
        source: str,
    ) -> Tuple[int, List[Tuple[int, bytes]]]:
        target_end = min(end, session.file_size)
        if start >= target_end:
            return start, []

        cursor = start
        transient: List[Tuple[int, bytes]] = []
        total_bytes = 0

        while cursor < target_end:
            if session.stop_requested:
                return start, []
            remaining = target_end - cursor
            chunk = session.backend.read_file(session.canonical_path, cursor, remaining)
            if not chunk:
                break
            chunk = chunk[:remaining]

            total_bytes += len(chunk)
            if not self._write_session_bytes(session, cursor, chunk):
                if source == "background":
                    break
                transient.append((cursor, chunk))
            cursor += len(chunk)

        fetched_end = cursor

        if session.stop_requested:
            return start, []
        if total_bytes == 0:
            raise IOError(
                f"range fetch returned no data for {session.canonical_path} at offset {start}"
            )

        self._record_streaming_stat(f"{source}_fetches", 1)
        self._record_streaming_stat(f"{source}_remote_bytes", total_bytes)
        self._update_streaming_state_xattr(session, "prefetching")
        return fetched_end, transient

    @staticmethod
    def _refresh_read_ahead_qualification(session: PrefetchSession, policy: StreamingKeepAfterClose):
        """Evaluate observed reads using one policy. Caller holds session.lock."""
        spans = session.foreground_read_spans
        if session.readers_consumed or not spans:
            return
        longest = max(end - start for start, end in spans)
        threshold = policy.threshold(session.file_size)
        if longest >= threshold:
            session.read_ahead_started = True
            session.readers_consumed = True
            spans.clear()
        elif longest >= STREAMING_READ_AHEAD_START_CHUNKS * session.chunk_size:
            session.read_ahead_started = True

    def _record_foreground_read(self, session: PrefetchSession, start: int, end: int):
        """Track the spans readers asked for. Caller holds session.lock."""
        if session.readers_consumed:
            return
        spans = session.foreground_read_spans
        self._add_available_range(spans, start, end)
        self._refresh_read_ahead_qualification(session, self._read_ahead_settings.keep_after_close)
        if len(spans) > STREAMING_MAX_TRACKED_READ_SPANS:
            # Never the span this read just joined: the read-ahead bound is
            # measured from it, and short sequential reads must be able to
            # grow it to the threshold.
            spans.remove(min(
                (span for span in spans if not span[0] <= start < span[1]),
                key=lambda span: span[1] - span[0],
            ))

    def _unbounded_read_ahead_chunk(
        self,
        session: PrefetchSession,
        cursor: int,
        wrapped: bool,
    ) -> Tuple[Optional[int], bool]:
        """The next chunk to fetch once the readers consumed the file.

        Caller holds session.lock. Runs forward from the readers' latest
        offset to the end of the file, then wraps once to fill what is
        missing before it. Returns (chunk start, wrapped); the start is None
        when nothing is missing.
        """
        if not wrapped:
            desired_start = (session.last_foreground_offset // session.chunk_size) * session.chunk_size
            cursor = max(cursor, desired_start)
        # The chunk-sized steps from cursor (which a short fetch can leave
        # unaligned) to the first one holding a missing byte, found from the
        # range list rather than by testing each chunk under the lock.
        if cursor < session.file_size:
            missing = self._first_missing_range(session.available_ranges, cursor, session.file_size)
            if missing is not None:
                steps = (missing[0] - cursor) // session.chunk_size
                return cursor + steps * session.chunk_size, wrapped
        missing = self._first_missing_range(session.available_ranges, 0, session.file_size)
        if missing is None:
            return None, wrapped
        return (missing[0] // session.chunk_size) * session.chunk_size, True

    def _bounded_read_ahead_range(self, session: PrefetchSession) -> Optional[Tuple[int, int]]:
        """The next unclaimed bytes within the speculative window, or None.

        Caller holds session.lock. Start after the span containing the latest
        read and clip both the window and the fetch to their byte boundaries.
        """
        offset = session.last_foreground_offset
        for span_start, span_end in session.foreground_read_spans:
            if span_start <= offset < span_end:
                limit = min(session.file_size, span_end + self._read_ahead_settings.window_bytes)
                fetch = self._first_unclaimed_missing_range(session, span_end, limit)
                if fetch is not None:
                    start, end = fetch
                    return start, min(end, start + session.chunk_size)
                break
        return None

    @staticmethod
    def _covered_ranges(session: PrefetchSession) -> List[Tuple[int, int]]:
        """The session's available and in-flight ranges, sorted. Caller holds session.lock."""
        return sorted(session.available_ranges + session.in_flight_ranges)

    @staticmethod
    def _missing_run_start(covered: List[Tuple[int, int]], offset: int) -> int:
        """Where the run of unfetched, unclaimed bytes holding offset begins.

        covered is _covered_ranges(session), taken under session.lock.
        """
        run_start = 0
        for range_start, range_end in covered:
            if range_start > offset:
                break
            run_start = max(run_start, range_end)
        return run_start

    @staticmethod
    def _first_unclaimed_missing_range(
        session: PrefetchSession,
        start: int,
        end: int,
        covered: Optional[List[Tuple[int, int]]] = None,
    ) -> Optional[Tuple[int, int]]:
        """The first range in [start, end) neither available nor in flight.

        Caller holds session.lock. covered, if given, is _covered_ranges(session)
        taken in the same locked section (a caller asking twice sorts once).
        """
        if covered is None:
            covered = StreamingReadManager._covered_ranges(session)
        return StreamingReadManager._first_missing_range(covered, start, end)

    def _ensure_streaming_range(
        self, session: PrefetchSession, start: int, end: int,
    ) -> List[Tuple[int, bytes]]:
        if start >= end:
            return []

        recorded = False
        transient: List[Tuple[int, bytes]] = []
        while True:
            with session.lock:
                if not recorded:
                    # In the section that claims the reader's range below,
                    # so a worker woken here cannot claim it first.
                    self._record_foreground_read(session, start, end)
                    session.last_foreground_offset = start
                    recorded = True
                session.has_foreground_read = True

                available = session.available_ranges
                if transient:
                    available = sorted(available + [(pos, pos + len(data)) for pos, data in transient])
                if self._first_missing_range(available, start, end) is None:
                    self._schedule_background(session)
                    return transient

                if session.stop_requested:
                    raise FuseOSError(errno.EINTR)
                if session.error is not None:
                    raise FuseOSError(streaming_read_errno(session.error))

                # Only what the reader asked for: not rounded up to a chunk,
                # which is what made a 4 KiB sniff cost a whole chunk.
                covered = sorted(available + session.in_flight_ranges)
                fetch = self._first_unclaimed_missing_range(session, start, end, covered)
                if fetch is None:
                    # The rest is in flight; its fetch notifies when done.
                    self._schedule_background(session)
                    session.condition.wait(timeout=0.05)
                    continue
                if not transient and len(session.available_ranges) > STREAMING_MAX_FOREGROUND_FRAGMENTS:
                    # The missing run around the read, out to its chunk.
                    chunk_start = (fetch[0] // session.chunk_size) * session.chunk_size
                    chunk_end = min(chunk_start + session.chunk_size, session.file_size)
                    fetch = self._first_unclaimed_missing_range(
                        session, max(chunk_start, self._missing_run_start(covered, fetch[0])), chunk_end,
                        covered,
                    )

                session.in_flight_ranges.append(fetch)
                self._schedule_background(session)

            try:
                _, uncached = self._fetch_range_into_session(
                    session,
                    fetch[0],
                    fetch[1],
                    source="foreground",
                )
                transient.extend(uncached)
            except Exception as exc:
                self.logger.warning("streaming foreground range failed errno=%d", streaming_errno(exc))
                with session.lock:
                    if session.error is None:
                        session.error = exc
                    session.condition.notify_all()
                self._update_streaming_state_xattr(session, "error")
                raise FuseOSError(streaming_read_errno(exc)) from exc
            finally:
                with session.lock:
                    session.in_flight_ranges.remove(fetch)
                    self._schedule_background(session)
                    session.condition.notify_all()

    def _count_session_descriptor(self, delta: int):
        with self._session_descriptors_lock:
            self._session_descriptors += delta

    def session_descriptor_count(self) -> int:
        with self._session_descriptors_lock:
            return self._session_descriptors

    def _close_streaming_session_resources(self, session: PrefetchSession):
        with session.io_lock:
            with session.lock:
                io_fd = session.io_fd
                session.io_fd = -1

        if io_fd >= 0:
            try:
                # Releases the partial's block charge if it is unlinked.
                self.cache_manager.close_streaming_fd(io_fd)
            except OSError:
                pass
            self._count_session_descriptor(-1)

    def open_session_inode(self, session: PrefetchSession, flags: int) -> int:
        """Open the session's own file for a reader that holds a ref on it.

        Never by its path: once a session fails and a later open replaces
        it, its partial path names the successor's file, whose range map is
        not this session's (ranges this session has would read as the
        successor's holes). /proc/self/fd reopens the inode the session
        writes, linked, renamed into the cache or unlinked. io_lock keeps
        the descriptor from closing meanwhile; it closes at the last
        release, at destroy (the open gets ENOENT), or when finalize saw no
        readers: then -1, and the caller opens the installed cache file by
        name. Without procfs the open is EIO, logged.
        """
        with session.io_lock:
            io_fd = session.io_fd
            if io_fd >= 0:
                try:
                    return os.open(f"/proc/self/fd/{io_fd}", flags & ~os.O_NOFOLLOW)
                except FileNotFoundError:
                    if os.path.isdir("/proc/self/fd"):
                        raise
                    self.logger.error("streaming needs /proc/self/fd to open a session's file")
                    raise OSError(errno.EIO, "procfs unavailable") from None
            with session.lock:
                finalized = session.finalized
        if finalized:
            # Finalize closes the descriptor when it saw no readers, which a
            # reader attaching after that decision does not change; the
            # session's inode is then the cache file it installed, which the
            # caller opens by name under its descriptor-table lock (-1).
            return -1
        raise FileNotFoundError(errno.ENOENT, "streaming session closed")

    def _remove_prefetch_session(self, session: PrefetchSession):
        with self.prefetch_sessions_lock:
            current = self.prefetch_sessions.get(session.canonical_path)
            if current is session:
                self.prefetch_sessions.pop(session.canonical_path, None)

    def _discard_staging_file(self, io_fd: int, path: Path):
        # Best effort cleanup must preserve the failure which brought us here.
        try:
            os.close(io_fd)
        except OSError:
            pass
        self._count_session_descriptor(-1)
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        self.cache_manager.mark_cache_size_dirty()

    def _discard_unregistered_prefetch_session(self, session: PrefetchSession):
        self._close_streaming_session_resources(session)
        with session.lock:
            if session.detached:
                return
        try:
            session.partial_cache_path.unlink(missing_ok=True)
        except OSError:
            pass
        self.cache_manager.mark_cache_size_dirty()

    @staticmethod
    def _claim_detach(session: PrefetchSession) -> bool:
        """Mark the session detached; False if it already was. Caller holds session.lock.

        Callers that also decide something about the session (the stop on
        last close) claim in the same session.lock section as the decision.
        Finalize checks detached and range completeness under session.lock
        and installs the partial after dropping it, so the stop alone does
        not exclude an install already past that check. What does is that the stop also
        requires prefetch_complete to be unset, and _fetch_range_into_session
        sets it in the same section that completes the ranges: a finalize
        past its check implies prefetch_complete, so no stop. Keep both, the
        predicate and where prefetch_complete is set, or this breaks.
        """
        if session.detached:
            return False
        session.detached = True
        return True

    def _unregister_and_unlink(self, session: PrefetchSession):
        """Drop a just-claimed session from the registry and unlink its partial.

        Caller holds prefetch_sessions_lock, which is what keeps a fresh
        session from installing its partial at the same path (the rename at
        registration) until the unlink is done. A session no longer in the
        registry here is one destroy() claims after its worker stopped
        short of finalizing; destroy refuses every open from its start, so
        no successor owns the path and the unlink is still ours.
        """
        if self.prefetch_sessions.get(session.canonical_path) is session:
            self.prefetch_sessions.pop(session.canonical_path, None)
        try:
            session.partial_cache_path.unlink(missing_ok=True)
        except OSError as exc:
            # The next session for this path renames its own file over it
            # at registration; a mount restart sweeps it otherwise.
            self.logger.warning(
                "streaming could not remove partial %s: %r", session.partial_cache_path, exc,
            )

    def _cleanup_prefetch_session(self, session: PrefetchSession, status: str):
        # Single-shot: release and the worker's tail can both reach cleanup
        # for one failed session, and only the first may touch the path.
        self._update_streaming_state_xattr(session, status)
        with self.prefetch_sessions_lock:
            with session.lock:
                claimed = self._claim_detach(session)
                # An open waiting for this cleanup (_wait_for_cleanup) retries.
                session.condition.notify_all()
            if claimed:
                self._unregister_and_unlink(session)
        self._close_streaming_session_resources(session)
        self.cache_manager.mark_cache_size_dirty()

    def _install_partial(self, session: PrefetchSession) -> str:
        """Move the session's partial to the final cache path unless that path is taken.

        Returns "installed"; or "taken", leaving the partial where it is, if
        the final path already exists. A session starts only for a file that is not cached,
        so anything at the final path now was put there since: a full-file
        fetch (a write-open, or an open that found no streaming slot),
        create(), or a rename onto the path. That file is at least as new as
        the member bytes the session fetched, and a writer may hold it open,
        so the session's bytes never replace it. The no-replace is atomic:
        link() fails if the name exists. Where the cache filesystem has no
        hard links, renameat2(RENAME_NOREPLACE) does the same; where it has
        neither, the partial is not installed at all ("unsupported"): a
        check followed by a rename would lose a fill in between. The
        readers keep their bytes, and the file is not cached by this
        session (warned once per mount). Raises OSError otherwise.
        """
        partial_path = session.partial_cache_path
        final_path = session.final_cache_path
        try:
            os.link(partial_path, final_path)
        except FileExistsError:
            return "taken"
        except OSError as exc:
            if exc.errno not in LINK_UNSUPPORTED:
                raise
            try:
                return "installed" if _rename_noreplace(partial_path, final_path) else "taken"
            except OSError as rename_exc:
                if rename_exc.errno not in _NOREPLACE_UNSUPPORTED:
                    raise
            with self._warned_no_noreplace_lock:
                first_warning = not self._warned_no_noreplace
                self._warned_no_noreplace = True
            if first_warning:
                self.logger.warning(
                    "streaming cannot install files in the cache without replacing: its filesystem "
                    "has neither hard links nor RENAME_NOREPLACE; streamed files are not cached",
                )
            return "unsupported"
        try:
            os.unlink(partial_path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            # A second name for the cached file; the next session for the
            # path renames its own partial over it, a restart sweeps it.
            self.logger.warning(
                "streaming could not remove partial %s after install: %r", partial_path, exc,
            )
        return "installed"

    @staticmethod
    def _not_installed_reason(outcome: str) -> str:
        """The log reason for an _install_partial outcome other than "installed"."""
        return "cache-path-filled" if outcome == "taken" else outcome

    def _discard_superseded_session(self, session: PrefetchSession, outcome: str):
        """Finish a complete session whose partial was not installed (outcome, see _install_partial).

        Mostly "taken": the final path was filled by something else.

        The partial is dropped (see _install_partial) and the session leaves
        the registry, so later opens use the cached file. Readers still
        attached keep reading their descriptors on the partial's inode, which
        holds every byte. finalized is set so their last release closes the
        session's descriptor; no metadata is recorded and on_finalized does
        not run, since the cache holds the other file.
        """
        self.logger.info(
            "streaming not installed path=%s backend=%s reason=%s; partial discarded",
            session.canonical_path,
            session.backend.name,
            self._not_installed_reason(outcome),
        )
        with self.prefetch_sessions_lock:
            with session.lock:
                claimed = self._claim_detach(session)
            if claimed:
                self._unregister_and_unlink(session)
        with session.lock:
            session.prefetch_complete = True
            session.finalized = True
            should_close = session.refcount == 0
            session.condition.notify_all()
        self._log_streaming_state_change(session, self._snapshot_streaming_state(
            session, "superseded" if outcome == "taken" else outcome))
        self.cache_manager.mark_cache_size_dirty()
        if should_close:
            self._close_streaming_session_resources(session)

    def _promote_failed_session_cache_prefix(self, session: PrefetchSession) -> bool:
        with session.lock:
            if session.detached:
                return False
            ranges = list(session.available_ranges)
            contiguous_bytes = self._contiguous_available_prefix(ranges, session.file_size)
            final_path = session.final_cache_path
            io_fd = session.io_fd

        if contiguous_bytes <= 0:
            return False

        try:
            if io_fd >= 0:
                with session.io_lock:
                    self.cache_manager.truncate_streaming_fd(io_fd, contiguous_bytes)
                    # A full fetch that loses the install race to this
                    # prefix must not index it (StorageFS._ensure_file_in_cache);
                    # an unmarked prefix is not kept.
                    os.setxattr(io_fd, PREFIX_XATTR, b"1")
                    os.fsync(io_fd)
            else:
                os.setxattr(str(session.partial_cache_path), PREFIX_XATTR, b"1")

            final_path.parent.mkdir(parents=True, exist_ok=True)
            # A prefix is kept only for a file the index does not describe
            # (never cached): served offline, never synced, never evicted.
            # A path with a row (evicted, or invalidated by a member change)
            # would take the prefix for the file: the next open would see a
            # lost write and mark it DIRTY, and the sync would upload the
            # prefix over the member, or report a false CONFLICT after an
            # invalidation (ditz streaming-promoted-prefix-false-conflict).
            # The check and the install are one step under the index lock.
            # A create makes its file before its row, which the no-replace
            # install refuses; a full fetch that loses the race to this
            # install finds the marker and fetches again instead of indexing
            # the prefix.
            with self.state_manager.metadata_lock:
                if self.state_manager.get_file_metadata(session.canonical_path) is not None:
                    outcome = "indexed"
                else:
                    outcome = self._install_partial(session)
            if outcome != "installed":
                self.logger.info(
                    "streaming partial-cache skipped path=%s backend=%s reason=%s",
                    session.canonical_path,
                    session.backend.name,
                    self._not_installed_reason(outcome),
                )
                return False
            self.logger.info(
                "streaming partial-cache path=%s backend=%s bytes=%d",
                session.canonical_path,
                session.backend.name,
                contiguous_bytes,
            )
            return True
        except OSError as exc:
            self.logger.warning(
                "streaming partial-cache promotion failed path=%s backend=%s bytes=%d: %r",
                session.canonical_path,
                session.backend.name,
                contiguous_bytes,
                exc,
            )
            return False

    @staticmethod
    def _claim_failed_cleanup(session: PrefetchSession) -> bool:
        """Claim the cleanup of a failed session; False if it was claimed already.

        Caller holds session.lock, in the same section where it saw the
        error and no readers. The claim has to be made there, not later in
        _cleanup_failed_session: an open attaching in between would hold a
        ref to a session whose partial is then truncated to its contiguous
        prefix (short reads), and _create_or_attach_prefetch_session checks
        the claim under session.lock to refuse exactly that. Promotion
        checks detached and then renames outside session.lock, so the claim
        also keeps a second cleanup out entirely.
        """
        if session.failed_cleanup_claimed:
            return False
        session.failed_cleanup_claimed = True
        return True

    def _cleanup_failed_session(self, session: PrefetchSession):
        """Promote a failed session's prefix, or drop it; caller claimed the cleanup.

        The claim is taken before promote and cleanup run, so if either
        raised something other than OSError (promotion catches those) no
        later release could retry, and the session would stay registered.
        That cannot happen today; do not add a raising step here without
        handling it.
        """
        assert session.failed_cleanup_claimed, "claim with _claim_failed_cleanup first"
        if self._promote_failed_session_cache_prefix(session):
            self._cleanup_prefetch_session(session, "partial-cache")
            return
        self._cleanup_prefetch_session(session, "failed")

    def _cleanup_cancelled_session(self, session: PrefetchSession):
        self._cleanup_prefetch_session(session, "cancelled")

    def _finalize_streaming_session(self, session: PrefetchSession):
        with session.lock:
            if session.finalized or session.error is not None or session.detached:
                return
            if not self._is_range_available(session.available_ranges, 0, session.file_size):
                return
            io_fd = session.io_fd

        if io_fd < 0:
            return

        # From the install until the rows are stored, under the index lock:
        # once the file is at its final path a write-open finds it cached
        # and creates no row, so a write's mark_dirty (which takes this
        # lock) must find the row this stores, not none. Otherwise finalize
        # stores CLEAN from a stat taken before the write and the release
        # queues no sync (ditz streaming-finalize-row-window). The location
        # rows go in the same section, so an eviction or rename cannot run
        # between the row and them. Only local file calls run under it (no
        # fsync), and no cache lock is taken under it. Only the install and the
        # calls before it raise OSError here: the row and location stores
        # catch their own.
        metadata = None
        try:
            copy_member_attributes(io_fd, session.member_stat)
            os.fsync(io_fd)
            session.final_cache_path.parent.mkdir(parents=True, exist_ok=True)
            with self.state_manager.metadata_lock:
                outcome = self._install_partial(session)
                if outcome == "installed":
                    metadata = self._store_finalized_row(session)
                    if metadata is not None:
                        self._add_finalized_locations(session, metadata)
        except OSError as exc:
            with session.lock:
                session.error = exc
                session.condition.notify_all()
            return

        if outcome != "installed":
            self._discard_superseded_session(session, outcome)
            return

        # The installed inode stays charged through io_fd, and pinned
        # against eviction, until _close_streaming_session_resources closes
        # it (CacheManager.close_streaming_fd): until then unlinking the
        # name would free nothing.

        with session.lock:
            session.prefetch_complete = True
            session.finalized = True
            should_close = session.refcount == 0
            session.condition.notify_all()

        if self.on_finalized is not None:
            try:
                self.on_finalized(session)
            except Exception:
                self.logger.exception("on_finalized failed for %s", session.canonical_path)

        self._update_streaming_state_xattr(session, "complete")
        self._remove_prefetch_session(session)

        if should_close:
            self._close_streaming_session_resources(session)

    def _store_finalized_row(self, session: PrefetchSession) -> Optional[FileMetadata]:
        """Store the row for a just-installed file; caller holds the index lock.

        Returns the stored row, or None when the file is already gone.
        """
        try:
            st = os.lstat(str(session.final_cache_path))
        except OSError:
            return None
        with session.lock:
            last_access = session.last_access
        metadata = self.state_manager.get_file_metadata(session.canonical_path)
        if not metadata:
            config = self.get_config(session.canonical_path)
            metadata = FileMetadata(
                virtual_path=session.canonical_path,
                size=st.st_size,
                checksum=None,
                last_access=last_access,
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
            metadata.last_access = last_access
            metadata.last_modified = st.st_mtime
            if metadata.sync_status != SyncStatus.DIRTY:
                metadata.sync_status = SyncStatus.CLEAN
        # Mirrors the row as stored: a write during the fill keeps its
        # DIRTY in SQLite, and the xattr must say the same.
        return self.cache_manager.store_file_metadata(metadata, cache_path=session.final_cache_path)

    def _add_finalized_locations(self, session: PrefetchSession, metadata: FileMetadata):
        """Record the member and the cache as holding the file; caller holds the index lock."""
        for backend_name, is_cached in ((session.backend.name, False), ("cache", True)):
            try:
                self.state_manager.add_file_location(
                    FileLocation(
                        virtual_path=session.canonical_path,
                        backend_name=backend_name,
                        checksum=metadata.checksum or "",
                        last_synced=time.time(),
                        is_cached=is_cached,
                    )
                )
            except Exception:
                pass

    @staticmethod
    def _download_terminal(session: PrefetchSession) -> bool:
        return session.stop_requested or session.error is not None or session.prefetch_complete

    def _next_background_fetch(self, session: PrefetchSession):
        """Return a missing span and its wrap flag. Caller holds session.lock."""
        if self._download_terminal(session) or session.cache_full or not session.read_ahead_started:
            return None
        wrapped = session.background_wrapped
        if not session.readers_consumed:
            fetch = self._bounded_read_ahead_range(session)
            return (fetch, wrapped) if fetch is not None else None
        chunk_start, wrapped = self._unbounded_read_ahead_chunk(
            session, session.background_cursor, wrapped,
        )
        if chunk_start is None:
            return None
        chunk_end = min(chunk_start + session.chunk_size, session.file_size)
        fetch = self._first_unclaimed_missing_range(session, chunk_start, chunk_end)
        return (fetch, wrapped) if fetch is not None else None

    def _schedule_background(self, session: PrefetchSession, finished_turn=False):
        """Reconsider work after a read, close, reattach or registration.

        Caller holds session.lock and has already claimed any foreground
        range it plans to fetch. A file waiting for a read or for a range
        already claimed by a reader takes no pool slot.
        """
        ready = not session.worker_done and (
            self._download_terminal(session) or self._next_background_fetch(session) is not None
        )
        self._download_pool.offer(session, ready, finished_turn=finished_turn)

    def _fetch_background_chunk(self, session: PrefetchSession):
        with session.lock:
            next_fetch = self._next_background_fetch(session)
            if next_fetch is None:
                return
            fetch, wrapped = next_fetch
            session.in_flight_ranges.append(fetch)

        try:
            fetched_end, _ = self._fetch_range_into_session(session, *fetch, source="background")
            with session.lock:
                # The first byte this turn did not keep: a refused chunk is
                # fetched again from its start. Progress does not need a
                # step past it: a turn that keeps nothing has raised (no
                # data), stopped, or set cache_full, which pauses background
                # fetches until a kept write clears it.
                session.background_cursor = fetched_end
                session.background_wrapped = wrapped
        finally:
            with session.lock:
                session.in_flight_ranges.remove(fetch)
                session.condition.notify_all()
        with self.prefetch_sessions_lock:
            with session.lock:
                if session.cache_full and session.refcount == 0 and not self._download_terminal(session):
                    # This runs inside the session's own pool turn, so the
                    # pool cannot report it idle and nothing here completes
                    # the cancellation: _run_download_turn sees
                    # stop_requested next and _finish_background_session
                    # cleans up, closing the descriptor (which releases the
                    # partial's charge). Calling this from anywhere else
                    # would need _complete_cancellation, as a release does.
                    idle = self._stop_background_locked(session)
                    assert not idle, "a session stopped from its own turn cannot be idle"

    def _run_download_turn(self, session: PrefetchSession):
        with session.lock:
            fetch_due = self._next_background_fetch(session) is not None

        if fetch_due:
            try:
                # Do not own a range while waiting for a bulk lease. A
                # reader can claim the bytes and use the interactive reserve.
                with capacity_wait_cancellation(
                    lambda: "streaming session" if session.stop_requested else None
                ):
                    session.backend.run_background_range_read(
                        session.canonical_path, lambda: self._fetch_background_chunk(session),
                    )
            except BulkCapacityCancelled:
                # Only this session's stop request can cancel the bulk wait.
                pass
            except Exception as exc:
                with session.lock:
                    if not session.stop_requested and session.error is None:
                        session.error = exc
                    session.condition.notify_all()
                if not session.stop_requested:
                    self.logger.warning(
                        "streaming background fetch failed path=%s backend=%s: %r",
                        session.canonical_path, session.backend.name, exc,
                    )
                    self._update_streaming_state_xattr(session, "error")

        with session.lock:
            if not self._download_terminal(session):
                # Release this turn even if another chunk is due, so queue
                # priority and the member ceiling apply anew.
                self._schedule_background(session, finished_turn=True)
                return

        try:
            self._finish_background_session(session)
        finally:
            with session.lock:
                self._download_pool.offer(session, False, finished_turn=True)
                session.background_finished.set()

    def _finish_background_session(self, session: PrefetchSession):
        try:
            self._finalize_streaming_session(session)
        except Exception as exc:
            self.logger.exception("Streaming download finalize failed for %s", session.canonical_path)
            with session.lock:
                if session.error is None:
                    session.error = exc
                session.condition.notify_all()

        with session.lock:
            session.worker_done = True
            has_error = session.error is not None
            no_readers = session.refcount == 0
            stopped = session.stop_requested
            finalized = session.finalized
            clean_up_failed = has_error and no_readers and self._claim_failed_cleanup(session)

        if clean_up_failed:
            self._cleanup_failed_session(session)
        elif stopped and not finalized and (no_readers or self._destroying):
            self._cleanup_cancelled_session(session)

    def _close_cancelled_download(self, session: PrefetchSession):
        """Complete a cancelled session that has no pool turn in progress."""
        with session.lock:
            session.worker_done = True
        self._close_streaming_session_resources(session)
        session.background_finished.set()

    def _finish_unstaffed_download(self, session: PrefetchSession):
        with self.prefetch_sessions_lock, session.lock:
            if not self._download_terminal(session) and session.refcount > 0:
                return
            if not self._download_pool.take_unstaffed(session):
                return
            if not self._download_terminal(session):
                # Thread creation failed for all workers. Readers can fetch
                # their own ranges, but a closed file cannot make progress.
                self._stop_background_locked(session)
                self._record_streaming_stat("streaming_unstaffed_cancellations")
        self._run_download_turn(session)

    def _attach(self, session: PrefetchSession) -> Optional[PrefetchSession]:
        """Add a reader to the registered session; None if it was failed and is now replaced.

        Caller holds prefetch_sessions_lock. A failed session lacks bytes it
        will never fetch, so a new open does not join it: it leaves the
        registry here (claimed, its partial unlinked, as a stop does) and
        the caller starts a fresh session. Readers still attached keep
        their descriptors on its inode and its error; the last release, or
        the worker's tail, closes its descriptor (see _stop_and_detach_session).
        Without this, one transient member failure seen by a reader that
        stays open would fail every later open of the file until it closed.
        A failed session that has every byte is still joined: only its
        finalize failed.

        Raises _CleanupPending once the failed cleanup is claimed: the
        claimer is promoting the prefix by the partial's path, so the
        session must not be detached from under it (a successor's partial
        at that path could be installed in its place).
        """
        with session.lock:
            if session.failed_cleanup_claimed:
                raise _CleanupPending(session)
            if session.error is not None and not session.prefetch_complete:
                if self._claim_detach(session):
                    # Its readers keep the partial's blocks once it is
                    # unlinked. They stay charged through the session's
                    # descriptor (CacheManager tracks it from its first
                    # grant), which closes at the last release.
                    self._unregister_and_unlink(session)
                self._record_streaming_stat("streaming_failed_sessions_replaced")
                self.logger.info(
                    "streaming replacing failed session errno=%d", streaming_errno(session.error),
                )
                return None
            session.refcount += 1
            self._schedule_background(session)
        return session

    def _wait_for_cleanup(self, session: PrefetchSession) -> bool:
        """Wait, holding no registry lock, until a claimed cleanup leaves the registry."""
        deadline = time.monotonic() + STREAMING_CLEANUP_WAIT_SECONDS
        with session.lock:
            while not session.detached:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                session.condition.wait(remaining)
        return True

    def report_open_error(self, reason: str, exc: Exception):
        error = streaming_errno(exc)
        if error in (errno.EMFILE, errno.ENFILE):
            reason = "descriptors"
        elif error == errno.ENOSPC and reason in ("cache_prepare", "cache_register", "cache_open"):
            # The local cache filesystem is full, whichever stage saw it.
            reason = "cache_space"
        self._record_streaming_stat(f"streaming_open_error_{reason}")
        self.logger.warning("streaming open failed reason=%s errno=%d", reason, error)


    def _download_session_counts(self):
        """Counts of files with readers and retained fills. Caller holds the registry lock."""
        active = retained = 0
        for session in self.prefetch_sessions.values():
            with session.lock:
                if session.refcount > 0:
                    active += 1
                elif not self._download_terminal(session):
                    retained += 1
        return active, retained

    def _create_or_attach_prefetch_session(
        self,
        virtual_path: str,
        canonical_path: str,
    ) -> PrefetchSession | StreamingOpen:
        """Attach to the file's session or start one; see StreamingOpen for the other outcomes.

        A failed session whose cleanup is claimed is waited out (the
        cleanup does local I/O only), then the open starts over: by then the
        cleanup has promoted its prefix or removed it. A promoted prefix is
        reported as PREFIX_CACHED, never CACHED: it is not the file.
        """
        while True:
            try:
                return self._create_or_attach_once(virtual_path, canonical_path)
            except _CleanupPending as pending:
                if not self._wait_for_cleanup(pending.session):
                    exc = OSError(errno.EAGAIN, "streaming cleanup in progress")
                    self.report_open_error("existing_session", exc)
                    raise exc from None
                self._record_streaming_stat("streaming_open_cleanup_waits")
                # Start over: a promoted prefix is then PREFIX_CACHED, never CACHED.

    def _cached_outcome(self, virtual_path: str, canonical_path: str) -> Optional[StreamingOpen]:
        """CACHED for a whole cached file, PREFIX_CACHED for a promoted prefix, None for none."""
        if not self.cache_manager.is_cached(virtual_path):
            return None
        return self._final_path_outcome(canonical_path, self.cache_manager.get_cache_path(virtual_path))

    def _final_path_outcome(self, canonical_path: str, final_cache_path: Path) -> StreamingOpen:
        if self.is_unindexed_prefix(canonical_path, final_cache_path):
            return StreamingOpen.PREFIX_CACHED
        return StreamingOpen.CACHED

    def _create_or_attach_once(
        self,
        virtual_path: str,
        canonical_path: str,
    ) -> PrefetchSession | StreamingOpen:
        with self.prefetch_sessions_lock:
            if self._destroying:
                raise OSError(errno.EINTR, "streaming manager is shutting down")
            existing = self.prefetch_sessions.get(canonical_path)
            if existing:
                attached = self._attach(existing)
                if attached is not None:
                    return attached

        try:
            source = self.select_stream_source(canonical_path)
        except Exception as exc:
            # A whole file cached meanwhile serves the open; a promoted
            # prefix does not. The caller reports the error (it may still
            # recompute the file).
            if self._cached_outcome(virtual_path, canonical_path) == StreamingOpen.CACHED:
                return StreamingOpen.CACHED
            raise StreamSourceError(streaming_errno(exc), "streaming source probe failed") from exc
        if source is None:
            if self._cached_outcome(virtual_path, canonical_path) == StreamingOpen.CACHED:
                return StreamingOpen.CACHED
            self._record_streaming_stat("streaming_source_not_found")
            self.logger.debug("streaming source unavailable reason=not_found")
            return StreamingOpen.SOURCE_MISSING

        backend, member_stat, file_size = source.backend, source.member_stat, source.file_size
        if not backend.supports_range_reads():
            return StreamingOpen.RANGE_UNSUPPORTED
        if type(file_size) is not int or file_size < 0:
            exc = OSError(errno.EIO, "streaming source returned an invalid size")
            self.report_open_error("source_size", exc)
            raise exc
        if file_size > _MAX_PARTIAL_SIZE:
            exc = OSError(errno.EFBIG, "streaming source size exceeds the largest local file")
            self.report_open_error("source_size", exc)
            raise exc

        final_cache_path = self.cache_manager.get_cache_path(virtual_path)
        partial_cache_path = self._partial_cache_path(final_cache_path)
        # Prepared under a name of its own and renamed onto the partial path
        # at registration, inside prefetch_sessions_lock: another open that
        # also missed the registry prepares its own file meanwhile, and only
        # the one that registers takes the shared path. The name is short
        # (a file name near the length limit must not fail here) and keeps
        # the partial suffix so listings hide it and a restart sweeps it. The
        # prefix is not a bare token, so a virtual file named like one has
        # no partial that can match a staging file (O_EXCL covers the rest).
        staging_path = (
            final_cache_path.parent / f".storage-staging-{secrets.token_hex(8)}{self.partial_suffix}"
        )

        io_fd = -1
        try:
            final_cache_path.parent.mkdir(parents=True, exist_ok=True)
            io_fd = os.open(str(staging_path), os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o644)
            self._count_session_descriptor(1)
            # Sparse: nothing is charged until fetched bytes need blocks
            # (CacheManager.write_streaming_chunk).
            os.ftruncate(io_fd, file_size)
        except BaseException as exc:
            # Until the session owns it, the staging file is ours to remove,
            # whatever was raised (OverflowError from ftruncate, say).
            if io_fd >= 0:
                self._discard_staging_file(io_fd, staging_path)
            if isinstance(exc, OSError):
                self.report_open_error("cache_space" if exc.errno == errno.ENOSPC else "cache_prepare", exc)
            raise

        try:
            session = PrefetchSession(
                canonical_path=canonical_path,
                virtual_path=virtual_path,
                backend=backend,
                file_size=file_size,
                final_cache_path=final_cache_path,
                partial_cache_path=staging_path,
                chunk_size=self.chunk_size,
                io_fd=io_fd,
                refcount=1,
                member_stat=member_stat,
            )
        except BaseException as exc:
            self._discard_staging_file(io_fd, staging_path)
            if isinstance(exc, MemoryError):
                self.report_open_error("cache_prepare", exc)
                raise OSError(errno.ENOMEM, "streaming session allocation failed") from exc
            raise

        # Until it is registered, the staging session (its descriptor and
        # file) is this open's: every way out below that does not register
        # it discards it, whatever was raised.
        registered = False
        replaced_orphan = False
        try:
            with self.prefetch_sessions_lock:
                if self._destroying:
                    raise OSError(errno.EINTR, "streaming manager is shutting down")
                existing = self.prefetch_sessions.get(canonical_path)
                if existing:
                    attached = self._attach(existing)
                    if attached is not None:
                        return attached
                    # A failed session left the registry: register this one.
                if os.path.lexists(final_cache_path):
                    # A concurrent fill or local writer already owns the final path.
                    return self._final_path_outcome(canonical_path, final_cache_path)
                # No registered session owns the name. An orphan left by an
                # earlier unlink failure can be replaced safely here.
                try:
                    replaced_orphan = os.path.lexists(partial_cache_path)
                    os.replace(staging_path, partial_cache_path)
                except OSError as exc:
                    self.report_open_error("cache_register", exc)
                    raise
                session.partial_cache_path = partial_cache_path
                self.prefetch_sessions[canonical_path] = session
                registered = True
        finally:
            if not registered:
                self._discard_unregistered_prefetch_session(session)

        # Registered: the caller owns the ref, and bookkeeping does not fail the open.
        try:
            # A new sparse partial holds no blocks; only an orphan it
            # replaced changes what the tree holds.
            if replaced_orphan:
                self.cache_manager.mark_cache_size_dirty()
            self._record_streaming_stat("streaming_sessions_started", 1)
            self.logger.info(
                "streaming session started for %s via backend %s (size=%d, chunk=%d)",
                canonical_path,
                backend.name,
                file_size,
                session.chunk_size,
            )
            self._update_streaming_state_xattr(session, "awaiting-read")
        except Exception:
            self.logger.exception("streaming session start bookkeeping failed")
        return session

    def _should_stop_on_last_close(self, session: PrefetchSession) -> bool:
        """Whether the last reader's close stops an unfinished session.

        Caller holds session.lock. The session keeps prefetching only if the
        readers consumed it (see StreamingKeepAfterClose). A session whose bytes have all arrived is
        never stopped, even if the worker has not renamed it yet: the stop
        and finalize decisions are both made under session.lock, so either
        this sees prefetch_complete and the session finalizes, or the stop
        lands first and finalize sees detached.
        """
        return (
            session.refcount == 0
            and not session.finalized
            and not session.prefetch_complete
            and session.error is None
            and (session.cache_full or not session.readers_consumed
                 or self._retained_descriptors_low(session))
        )

    def _retained_descriptors_low(self, session: PrefetchSession) -> bool:
        """Whether retaining this fill would leave too few descriptors for the mount.

        Caller holds prefetch_sessions_lock. Every session holds one
        descriptor on its partial whether or not it has readers, and so does
        a replaced session while its readers keep it, out of the registry;
        session_descriptor_count counts them all, staging files included.
        Retained fills are refused once those reach
        1 / STREAMING_DESCRIPTOR_SHARE of the soft RLIMIT_NOFILE: the rest
        stays for readers' descriptors, the database, member connections
        and whole-file copies. The fill is discarded as an unqualified one
        is; nothing else changes.
        """
        try:
            soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        except (OSError, ValueError):
            return False
        if soft == resource.RLIM_INFINITY or soft <= 0:
            return False
        held = self.session_descriptor_count()
        if held * STREAMING_DESCRIPTOR_SHARE < soft:
            return False
        self._record_streaming_stat("streaming_retained_descriptor_cancellations")
        self.logger.warning(
            "streaming not retaining an unfinished fill: sessions hold %d descriptors (soft limit %d)",
            held, soft,
        )
        return True

    def _stop_background_locked(self, session: PrefetchSession) -> bool:
        """Unregister a stopped session. Caller holds the registry and session locks."""
        session.stop_requested = True
        session.condition.notify_all()
        if self._claim_detach(session):
            self._unregister_and_unlink(session)
        return self._download_pool.cancel(session)

    def _complete_cancellation(self, session: PrefetchSession, idle: bool):
        self._log_streaming_state_change(session, self._snapshot_streaming_state(session, "cancelled"))
        self.cache_manager.mark_cache_size_dirty()
        if idle:
            self._cleanup_cancelled_session(session)
            self._close_cancelled_download(session)

    def _stop_and_detach_session(self, session: PrefetchSession) -> str:
        """Drop a reader's ref and decide what its release does.

        Returns "stopped" or "stopped-in-flight" for a session now detached;
        "cleanup-failed" if it was the last ref of
        a failed session whose worker is done (see worker_done); "close" if
        it was the last ref of a finalized session; otherwise "".

        Runs under prefetch_sessions_lock so the decision, the removal from the
        registry and the unlink of the partial file are one step against
        _create_or_attach_prefetch_session: an open either attached before
        this (refcount is then above zero and nothing stops) or finds no
        session and creates a fresh one, never the stopped one. The unlink is
        inside the lock because a fresh session prepares its partial file at
        the same path once the registry no longer holds this one.
        """
        with self.prefetch_sessions_lock:
            with session.lock:
                if session.refcount > 0:
                    session.refcount -= 1
                if session.refcount > 0:
                    return ""
                if session.finalized:
                    return "close"
                # Decided with the decrement, not after it: see worker_done.
                # Before the detached check: a failed session that a new
                # open replaced (_attach) is detached with readers still on
                # it, and its last release closes its descriptor here
                # (cleanup skips the path of a detached session).
                if (session.error is not None and session.worker_done
                        and self._claim_failed_cleanup(session)):
                    return "cleanup-failed"
                if session.detached:
                    return ""

                if self._should_stop_on_last_close(session):
                    idle = self._stop_background_locked(session)
                    return "stopped" if idle else "stopped-in-flight"
                self._schedule_background(session)
                return ""

    def _release_prefetch_session(self, session: PrefetchSession):
        action = self._stop_and_detach_session(session)

        if action in ("stopped", "stopped-in-flight"):
            # No join: waiting here on a worker mid-chunk would hold this
            # release (and, with the FUSE loop single-threaded, every other
            # operation on the mount) for the length of a remote fetch. The
            # worker writes only to its own descriptor, now on an unlinked
            # inode, and closes it on exit (its cancelled cleanup).
            self._complete_cancellation(session, idle=action == "stopped")
        elif action == "cleanup-failed":
            # No join here either: a failed session whose worker is not done
            # yet is cleaned up by the worker's tail.
            self._cleanup_failed_session(session)
        elif action == "close":
            self._close_streaming_session_resources(session)
        else:
            self._finish_unstaffed_download(session)
