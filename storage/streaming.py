import ctypes
import errno
import json
import os
import secrets
import socket
import threading
import time
from dataclasses import dataclass, field
from fractions import Fraction
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from fuse import FuseOSError

from .backends.base import Backend
from .cache_metadata import PREFIX_XATTR
from .models import FileLocation, FileMetadata, StreamingKeepAfterClose, SyncStatus

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

# Read-ahead outlives the last close only for readers that consumed the file.
# The evidence is the longest contiguous span the readers asked for (not what
# was fetched: the worker runs ahead of the readers), checked against the
# mount's streaming_keep_after_close. Its
# default is this many chunks (4 MiB at the default 1 MiB chunk) or half the
# file, whichever is smaller. A content-type sniff or a thumbnailer reads a
# few KiB to a few MiB at scattered offsets and stays under it; a copy or a
# player that has played a few seconds crosses it. A span anywhere counts,
# so a player resuming mid-file qualifies too. Below the threshold the close
# cancels the session and the partial is discarded.
STREAMING_KEEP_AFTER_CLOSE_CHUNKS = 4
STREAMING_KEEP_AFTER_CLOSE_PERCENT = Fraction(50)
# Bound on the spans tracked per session before the threshold is reached;
# past it the shortest span is dropped. Only a reader making that many
# scattered reads gets here, and losing a short span can only delay the
# threshold, never fake it.
STREAMING_MAX_TRACKED_READ_SPANS = 64

# link() errors after which _install_partial tries RENAME_NOREPLACE: no
# hard links on the cache filesystem (EMLINK cannot happen on a fresh
# partial, but the rename would work there too); renameat2 errors meaning
# it has no RENAME_NOREPLACE either.
_LINK_UNSUPPORTED = (errno.EPERM, errno.EOPNOTSUPP, errno.ENOTSUP, errno.ENOSYS, errno.EMLINK)
_NOREPLACE_UNSUPPORTED = (errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOTSUP)
_AT_FDCWD = -100

_RENAME_NOREPLACE = 1


def _load_renameat2():
    try:
        renameat2 = ctypes.CDLL(None, use_errno=True).renameat2
    except (AttributeError, OSError):
        return None
    renameat2.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
    return renameat2


_RENAMEAT2 = _load_renameat2()


def _rename_noreplace(src, dst) -> bool:
    """rename(src, dst) unless dst exists: True if renamed, False if dst exists.

    Raises OSError, with ENOSYS where libc has no renameat2.
    """
    renameat2 = _RENAMEAT2
    if renameat2 is None:
        raise OSError(errno.ENOSYS, "renameat2 unavailable")
    if renameat2(_AT_FDCWD, os.fsencode(src), _AT_FDCWD, os.fsencode(dst), _RENAME_NOREPLACE) == 0:
        return True
    err = ctypes.get_errno()
    if err == errno.EEXIST:
        return False
    raise OSError(err, os.strerror(err), str(src))


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
    # Byte ranges claimed by a fetch in progress, foreground or background.
    # A range is claimed only where it is missing and not already claimed,
    # so no byte is requested from the member twice.
    in_flight_ranges: List[Tuple[int, int]] = field(default_factory=list)
    refcount: int = 0
    prefetch_complete: bool = False
    finalized: bool = False
    stop_requested: bool = False
    error: Optional[Exception] = None
    worker: Optional[threading.Thread] = None
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

    def __post_init__(self):
        self.lock = threading.RLock()
        self.io_lock = threading.Lock()
        self.condition = threading.Condition(self.lock)


class StreamingReadManager:
    """Owns streaming read sessions, range fetches, and control socket state."""

    def __init__(
        self,
        *,
        cache_manager,
        state_manager,
        logger,
        get_config,
        select_backend_for_read,
        streaming_reads: bool,
        streaming_chunk_size: int,
        max_streaming_sessions: int,
        partial_suffix: str,
        stream_state_xattr: str,
        keep_after_close: Optional[StreamingKeepAfterClose] = None,
        on_finalized: Optional[Callable[["PrefetchSession"], None]] = None,
    ):
        self.cache_manager = cache_manager
        # Called once the complete file is in the cache (StorageFS records
        # the member baseline for revalidation on open).
        self.on_finalized = on_finalized
        self.state_manager = state_manager
        self.logger = logger
        self.get_config = get_config
        self.select_backend_for_read = select_backend_for_read
        self.streaming_reads = streaming_reads
        self.chunk_size = max(4096, int(streaming_chunk_size))
        self.max_streaming_sessions = max(1, int(max_streaming_sessions))
        if keep_after_close is None:
            keep_after_close = StreamingKeepAfterClose(
                byte_thresholds=(STREAMING_KEEP_AFTER_CLOSE_CHUNKS * self.chunk_size,),
                percent_thresholds=(STREAMING_KEEP_AFTER_CLOSE_PERCENT,),
            )
        self.keep_after_close = keep_after_close
        self.partial_suffix = partial_suffix
        self.stream_state_xattr = stream_state_xattr

        self.prefetch_sessions: Dict[str, PrefetchSession] = {}
        self.prefetch_sessions_lock = threading.RLock()
        # Set under prefetch_sessions_lock when destroy() starts; no session
        # registers or is attached to after that.
        self._destroying = False
        self._warned_no_noreplace = False
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

    def init(self):
        control_thread = self._control_thread
        if control_thread is None or not control_thread.is_alive():
            self._start_control_server()

    def destroy(self):
        """Stop every session; from here on, opens get no session (full-file path).

        The refusal is set in the same prefetch_sessions_lock section that
        takes the sessions, so an open racing destroy either registered
        before it (and is stopped here) or registers nothing. Each session
        is detached by claim, as on any other way out of the registry, so
        its partial is unlinked once and a cleanup still running elsewhere
        skips the path. A session whose bytes have all arrived is left to
        finish its finalize while destroy waits for its worker (see
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
        self._stop_control_server()

        with self.prefetch_sessions_lock:
            self._destroying = True
            sessions = list(self.prefetch_sessions.values())
            for session in sessions:
                self._detach_for_destroy(session)

        for session in sessions:
            worker = session.worker
            if worker and worker.is_alive():
                worker.join(timeout=2.0)
            with self.prefetch_sessions_lock:
                self._detach_for_destroy(session, complete_too=True)
            if worker and worker.is_alive():
                self.logger.warning(
                    "Streaming worker still alive during destroy; skipping forced close for %s",
                    session.canonical_path,
                )
                continue

            self._close_streaming_session_resources(session)
        if sessions:
            self.cache_manager.mark_cache_size_dirty()

    def _detach_for_destroy(self, session: PrefetchSession, complete_too: bool = False):
        """Stop the session and, unless it is finishing, claim it and unlink its partial.

        Caller holds prefetch_sessions_lock. Without complete_too a session
        whose bytes have all arrived only gets the stop request, so its
        worker can still finalize it.
        """
        with session.lock:
            session.stop_requested = True
            session.condition.notify_all()
            if session.finalized:
                return
            if session.prefetch_complete and not complete_too:
                return
            claimed = self._claim_detach(session)
        if claimed:
            self._unregister_and_unlink(session)

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

        # The detached check is not atomic with the setxattr below: a session
        # detached in between could write one stale state onto a successor's
        # partial. The successor would have to finish a remote get_size in
        # that gap, and the xattr is informational, so it is not locked out.
        # No fallback to the final path when the partial is gone: the
        # final path may hold another file by then (a superseded session,
        # see _install_partial), and the state describes this session only.
        with session.lock:
            if session.detached:
                return
            target_path = session.final_cache_path if session.finalized else session.partial_cache_path

        try:
            os.setxattr(
                str(target_path),
                self.stream_state_xattr,
                json.dumps(payload, sort_keys=True).encode("utf-8"),
            )
        except OSError:
            pass

    def _streaming_stats_snapshot(self) -> Dict[str, object]:
        with self._streaming_stats_lock:
            stats = dict(self._streaming_stats)

        with self.prefetch_sessions_lock:
            active_streaming_sessions = len(self.prefetch_sessions)

        stats.update({
            "active_streaming_sessions": active_streaming_sessions,
            "pid": os.getpid(),
            "timestamp": time.time(),
            "streaming_enabled": bool(self.streaming_reads),
        })
        return stats

    def _handle_control_request(self, request: str) -> Dict[str, object]:
        if request == "streaming-stats":
            return self._streaming_stats_snapshot()
        return {"error": f"unsupported request: {request}"}

    def _control_server_loop(self):
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
            self.logger.warning(
                "failed to start control socket at %s: %s",
                self._control_socket_path,
                exc,
            )
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

    def _fetch_range_into_session(
        self,
        session: PrefetchSession,
        start: int,
        end: int,
        source: str,
    ) -> int:
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
            raise IOError(
                f"range fetch returned no data for {session.canonical_path} at offset {start}"
            )

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

    def _keep_after_close_threshold(self, session: PrefetchSession) -> int:
        return self.keep_after_close.threshold(session.file_size)

    def _read_ahead_start_threshold(self, session: PrefetchSession) -> int:
        return min(
            STREAMING_READ_AHEAD_START_CHUNKS * session.chunk_size,
            self._keep_after_close_threshold(session),
        )

    def _record_foreground_read(self, session: PrefetchSession, start: int, end: int):
        """Track the spans readers asked for. Caller holds session.lock."""
        if session.readers_consumed:
            return
        spans = session.foreground_read_spans
        self._add_available_range(spans, start, end)
        longest = max((span_end - span_start for span_start, span_end in spans), default=0)
        if longest >= self._read_ahead_start_threshold(session):
            session.read_ahead_started = True
        if longest >= self._keep_after_close_threshold(session):
            session.readers_consumed = True
            spans.clear()
        elif len(spans) > STREAMING_MAX_TRACKED_READ_SPANS:
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

    def _bounded_read_ahead_chunk(self, session: PrefetchSession) -> Optional[int]:
        """The next chunk the worker may fetch before readers_consumed is set.

        Caller holds session.lock. Until the readers have consumed the file,
        the worker stays within the keep-after-close threshold past the end
        of the span the latest read belongs to (rounded up to whole chunks),
        fetching forward from that read only, so a reader that sniffs the
        head and holds the file open pulls at most that much, not the whole
        file. A reader reading on reaches the threshold inside that window
        and lifts the bound. None means nothing is due until the readers
        move or the session ends.
        """
        offset = session.last_foreground_offset
        frontier = offset
        for span_start, span_end in session.foreground_read_spans:
            if span_start <= offset < span_end:
                frontier = span_end
                break
        limit = min(session.file_size, frontier + self._keep_after_close_threshold(session))
        aligned = (offset // session.chunk_size) * session.chunk_size
        missing = self._first_missing_range(session.available_ranges, aligned, limit)
        if missing is None:
            return None
        return (missing[0] // session.chunk_size) * session.chunk_size

    @staticmethod
    def _missing_run_start(session: PrefetchSession, offset: int) -> int:
        """Where the run of unfetched, unclaimed bytes holding offset begins.

        Caller holds session.lock.
        """
        run_start = 0
        for range_start, range_end in sorted(session.available_ranges + session.in_flight_ranges):
            if range_start > offset:
                break
            run_start = max(run_start, range_end)
        return run_start

    @staticmethod
    def _first_unclaimed_missing_range(
        session: PrefetchSession,
        start: int,
        end: int,
    ) -> Optional[Tuple[int, int]]:
        """The first range in [start, end) neither available nor in flight.

        Caller holds session.lock.
        """
        covered = sorted(session.available_ranges + session.in_flight_ranges)
        return StreamingReadManager._first_missing_range(covered, start, end)

    def _ensure_streaming_range(self, session: PrefetchSession, start: int, end: int):
        if start >= end:
            return

        recorded = False
        while True:
            with session.lock:
                if not recorded:
                    # In the section that claims the reader's range below,
                    # so a worker woken here cannot claim it first.
                    self._record_foreground_read(session, start, end)
                    # Wakes a worker held at its read-ahead bound (see
                    # _bounded_read_ahead_chunk): the bound moves with the
                    # reader.
                    session.last_foreground_offset = start
                    session.condition.notify_all()
                    recorded = True
                # Reported in the state xattr only; the worker waits on
                # read_ahead_started.
                session.has_foreground_read = True

                if self._is_range_available(session.available_ranges, start, end):
                    return

                if session.error is not None:
                    raise FuseOSError(errno.EIO)

                # Only what the reader asked for: not rounded up to a chunk,
                # which is what made a 4 KiB sniff cost a whole chunk.
                fetch = self._first_unclaimed_missing_range(session, start, end)
                if fetch is None:
                    # The rest is in flight; its fetch notifies when done.
                    session.condition.wait(timeout=0.05)
                    continue
                if len(session.available_ranges) > STREAMING_MAX_FOREGROUND_FRAGMENTS:
                    # The missing run around the read, out to its chunk.
                    chunk_start = (fetch[0] // session.chunk_size) * session.chunk_size
                    chunk_end = min(chunk_start + session.chunk_size, session.file_size)
                    fetch = self._first_unclaimed_missing_range(
                        session, max(chunk_start, self._missing_run_start(session, fetch[0])), chunk_end,
                    )

                session.in_flight_ranges.append(fetch)

            try:
                _ = self._fetch_range_into_session(
                    session,
                    fetch[0],
                    fetch[1],
                    source="foreground",
                )
            except Exception as exc:
                self.logger.warning(
                    "streaming foreground fetch failed path=%s backend=%s range=[%d,%d): %r",
                    session.canonical_path,
                    session.backend.name,
                    fetch[0],
                    fetch[1],
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
                    session.in_flight_ranges.remove(fetch)
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
        with self.prefetch_sessions_lock:
            current = self.prefetch_sessions.get(session.canonical_path)
            if current is session:
                self.prefetch_sessions.pop(session.canonical_path, None)

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
            if exc.errno not in _LINK_UNSUPPORTED:
                raise
            try:
                return "installed" if _rename_noreplace(partial_path, final_path) else "taken"
            except OSError as rename_exc:
                if rename_exc.errno not in _NOREPLACE_UNSUPPORTED:
                    raise
            if not self._warned_no_noreplace:
                self._warned_no_noreplace = True
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
            "cache-path-filled" if outcome == "taken" else outcome,
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
        self._log_streaming_state_change(session, self._snapshot_streaming_state(session, "superseded"))
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
                    os.ftruncate(io_fd, contiguous_bytes)
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
                    "cache-path-filled" if outcome == "taken" else outcome,
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
        """Promote a failed session's prefix, or drop it; caller claimed the cleanup."""
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
        # fsync); the cache lock is taken after. Only the install and the
        # calls before it raise OSError here: the row and location stores
        # catch their own.
        metadata = None
        try:
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

        self.cache_manager.mark_cache_size_dirty()

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
        metadata = self.state_manager.get_file_metadata(session.canonical_path)
        if not metadata:
            config = self.get_config(session.canonical_path)
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

    def _background_prefetch_worker(self, session: PrefetchSession):
        cursor = 0
        wrapped = False

        while True:
            with session.lock:
                if session.stop_requested or session.error is not None:
                    break

                # Readers that fetched every byte themselves still need the
                # worker to finalize, hence prefetch_complete.
                if not session.read_ahead_started and not session.prefetch_complete:
                    # Reads, fetches and stop notify; the timeout is only a
                    # backstop.
                    session.condition.wait(timeout=1.0)
                    continue

                if session.readers_consumed:
                    chunk_start, wrapped = self._unbounded_read_ahead_chunk(session, cursor, wrapped)
                    if chunk_start is None:
                        break
                else:
                    if self._first_missing_range(session.available_ranges, 0, session.file_size) is None:
                        break
                    chunk_start = self._bounded_read_ahead_chunk(session)
                    if chunk_start is None:
                        # Reads, consumption, stop, errors and fetches all
                        # notify; the timeout is only a backstop.
                        session.condition.wait(timeout=1.0)
                        continue
                cursor = chunk_start
                chunk_end = min(chunk_start + session.chunk_size, session.file_size)
                # The chunk's missing bytes that no reader is fetching: the
                # readers' own fetches leave holes of any size in it.
                fetch = self._first_unclaimed_missing_range(session, chunk_start, chunk_end)
                if fetch is None:
                    session.condition.wait(timeout=0.05)
                    continue

                session.in_flight_ranges.append(fetch)

            try:
                fetched_end = self._fetch_range_into_session(
                    session,
                    fetch[0],
                    fetch[1],
                    source="background",
                )
                cursor = max(fetched_end, fetch[0] + 1)
            except Exception as exc:
                self.logger.warning(
                    "streaming background fetch failed path=%s backend=%s range=[%d,%d): %r",
                    session.canonical_path,
                    session.backend.name,
                    fetch[0],
                    fetch[1],
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
                    session.in_flight_ranges.remove(fetch)
                    session.condition.notify_all()

        self._finalize_streaming_session(session)

        with session.lock:
            session.worker_done = True
            has_error = session.error is not None
            no_readers = session.refcount == 0
            stopped = session.stop_requested
            finalized = session.finalized
            clean_up_failed = has_error and no_readers and self._claim_failed_cleanup(session)

        if clean_up_failed:
            self._cleanup_failed_session(session)
        elif not has_error and stopped and not finalized and no_readers:
            self._cleanup_cancelled_session(session)

    def _attach(self, session: PrefetchSession) -> bool:
        """Add a reader to a registered session; False if its failed cleanup is claimed.

        Caller holds prefetch_sessions_lock. The claim is read under
        session.lock, where it is taken together with the no-readers
        decision (see _claim_failed_cleanup), so either this reader counts
        in that decision or it does not attach. A refused open takes the
        full-file path: if its fetch fills the final path first, promotion
        leaves that file alone (see _install_partial); if promotion links
        first, the opener gets the promoted prefix as the cached file, as
        any open after a failed session's cleanup does.
        """
        with session.lock:
            if session.failed_cleanup_claimed:
                refused = True
            else:
                refused = False
                session.refcount += 1
                readers = session.refcount
        if refused:
            self.logger.info(
                "streaming attach refused path=%s backend=%s: failed session being cleaned up",
                session.canonical_path,
                session.backend.name,
            )
            return False
        self.logger.info(
            "streaming attach path=%s backend=%s readers=%d",
            session.canonical_path,
            session.backend.name,
            readers,
        )
        return True

    def _create_or_attach_prefetch_session(
        self,
        virtual_path: str,
        canonical_path: str,
    ) -> Optional[PrefetchSession]:
        with self.prefetch_sessions_lock:
            if self._destroying:
                return None
            existing = self.prefetch_sessions.get(canonical_path)
            if existing:
                if not self._attach(existing):
                    return None
                return existing

            if len(self.prefetch_sessions) >= self.max_streaming_sessions:
                self.logger.warning(
                    "Streaming session limit reached (%d); falling back to full-file cache for %s",
                    self.max_streaming_sessions,
                    canonical_path,
                )
                return None

        backend = self.select_backend_for_read(canonical_path)
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

        member_stat = None
        try:
            # One round trip either way: get_size is a stat on every member.
            try:
                member_stat = backend.stat(canonical_path)
                file_size = member_stat.st_size
            except NotImplementedError:
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
            self.logger.debug(
                "streaming session unavailable: invalid size %s for %s",
                file_size,
                canonical_path,
            )
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
        # Prepared under a name of its own and renamed onto the partial path
        # at registration, inside prefetch_sessions_lock: another open that
        # also missed the registry prepares its own file meanwhile, and only
        # the one that registers takes the shared path. The name is short
        # (a file name near the length limit must not fail here) and keeps
        # the partial suffix so listings hide it and a restart sweeps it.
        staging_path = final_cache_path.parent / f".{secrets.token_hex(8)}{self.partial_suffix}"

        io_fd = -1
        try:
            io_fd = os.open(str(staging_path), os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o644)
            os.ftruncate(io_fd, file_size)
        except OSError as exc:
            self.logger.debug(
                "streaming session unavailable: failed to prepare partial cache file %s: %s",
                staging_path,
                exc,
            )
            if io_fd >= 0:
                os.close(io_fd)
                try:
                    staging_path.unlink(missing_ok=True)
                except OSError:
                    pass
            return None

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

        with self.prefetch_sessions_lock:
            if self._destroying:
                self._discard_unregistered_prefetch_session(session)
                return None
            existing = self.prefetch_sessions.get(canonical_path)
            if existing:
                self._discard_unregistered_prefetch_session(session)
                if not self._attach(existing):
                    return None
                return existing
            if os.path.lexists(final_cache_path):
                # Filled since open() found the file uncached: a full-file
                # fetch by a writer or another open, or create(). That file
                # is the one to read (see _install_partial).
                self.logger.info(
                    "streaming session unavailable: %s was cached during setup",
                    canonical_path,
                )
                self._discard_unregistered_prefetch_session(session)
                return None
            if len(self.prefetch_sessions) >= self.max_streaming_sessions:
                self.logger.warning(
                    "Streaming session limit reached (%d) during registration; falling back to full-file cache for %s",
                    self.max_streaming_sessions,
                    canonical_path,
                )
                self._discard_unregistered_prefetch_session(session)
                return None
            # Replaces any partial a detached session failed to unlink. No
            # registered session owns the path: detaching unlinks it inside
            # this lock, and a registered one would have been attached above.
            try:
                os.replace(staging_path, partial_cache_path)
            except OSError as exc:
                self.logger.debug(
                    "streaming session unavailable: failed to install partial cache file %s: %s",
                    partial_cache_path,
                    exc,
                )
                self._discard_unregistered_prefetch_session(session)
                return None
            session.partial_cache_path = partial_cache_path
            self.prefetch_sessions[canonical_path] = session

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
            and not session.readers_consumed
        )

    def _stop_and_detach_session(self, session: PrefetchSession) -> str:
        """Drop a reader's ref and decide what its release does.

        Returns "stopped" if this was the last ref of an unfinished session,
        now stopped and detached; "cleanup-failed" if it was the last ref of
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
                if not self._should_stop_on_last_close(session):
                    if session.refcount > 0:
                        return ""
                    if session.finalized:
                        return "close"
                    # Decided with the decrement, not after it: see worker_done.
                    if (
                        session.error is not None
                        and session.worker_done
                        and self._claim_failed_cleanup(session)
                    ):
                        return "cleanup-failed"
                    return ""
                # One section with the decision: see _claim_detach.
                if not self._claim_detach(session):
                    return ""
                session.stop_requested = True
                session.condition.notify_all()
            self._unregister_and_unlink(session)

        self._log_streaming_state_change(session, self._snapshot_streaming_state(session, "cancelled"))
        self.cache_manager.mark_cache_size_dirty()
        return "stopped"

    def _release_prefetch_session(self, session: PrefetchSession):
        action = self._stop_and_detach_session(session)
        worker = session.worker

        if action == "stopped":
            # No join: waiting here on a worker mid-chunk would hold this
            # release (and, with the FUSE loop single-threaded, every other
            # operation on the mount) for the length of a remote fetch. The
            # worker writes only to its own descriptor, now on an unlinked
            # inode, and closes it on exit (its cancelled cleanup).
            if worker is None or not worker.is_alive():
                self._close_streaming_session_resources(session)
        elif action == "cleanup-failed":
            # No join here either: a failed session whose worker is not done
            # yet is cleaned up by the worker's tail.
            self._cleanup_failed_session(session)
        elif action == "close":
            self._close_streaming_session_resources(session)
