import errno
import json
import os
import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from fuse import FuseOSError

from .backends.base import Backend
from .state import FileLocation, FileMetadata, SyncStatus


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
    ):
        self.cache_manager = cache_manager
        self.state_manager = state_manager
        self.logger = logger
        self.get_config = get_config
        self.select_backend_for_read = select_backend_for_read
        self.streaming_reads = streaming_reads
        self.chunk_size = max(4096, int(streaming_chunk_size))
        self.max_streaming_sessions = max(1, int(max_streaming_sessions))
        self.partial_suffix = partial_suffix
        self.stream_state_xattr = stream_state_xattr

        self.prefetch_sessions: Dict[str, PrefetchSession] = {}
        self.prefetch_sessions_lock = threading.RLock()
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
        self._stop_control_server()

        with self.prefetch_sessions_lock:
            sessions = list(self.prefetch_sessions.values())
            self.prefetch_sessions.clear()

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

    def cleanup_stale_partials(self):
        """Delete stale partial-cache files left by prior crashes."""
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

        with session.lock:
            target_path = session.final_cache_path if session.finalized else session.partial_cache_path
            final_path = session.final_cache_path

        if not target_path.exists() and final_path.exists():
            target_path = final_path

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
                _ = self._fetch_range_into_session(
                    session,
                    chunk_start,
                    chunk_end,
                    source="foreground",
                )
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
        with self.prefetch_sessions_lock:
            current = self.prefetch_sessions.get(session.canonical_path)
            if current is session:
                self.prefetch_sessions.pop(session.canonical_path, None)

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

    def _promote_failed_session_cache_prefix(self, session: PrefetchSession) -> bool:
        with session.lock:
            ranges = list(session.available_ranges)
            contiguous_bytes = self._contiguous_available_prefix(ranges, session.file_size)
            partial_path = session.partial_cache_path
            final_path = session.final_cache_path
            io_fd = session.io_fd

        if contiguous_bytes <= 0:
            return False

        try:
            if io_fd >= 0:
                with session.io_lock:
                    os.ftruncate(io_fd, contiguous_bytes)
                    os.fsync(io_fd)

            final_path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(partial_path, final_path)
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

    def _cleanup_failed_session(self, session: PrefetchSession):
        if self._promote_failed_session_cache_prefix(session):
            self._cleanup_prefetch_session(session, "partial-cache")
            return
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
                fetched_end = self._fetch_range_into_session(
                    session,
                    chunk_start,
                    chunk_end,
                    source="background",
                )
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

    def _create_or_attach_prefetch_session(
        self,
        virtual_path: str,
        canonical_path: str,
    ) -> Optional[PrefetchSession]:
        with self.prefetch_sessions_lock:
            existing = self.prefetch_sessions.get(canonical_path)
            if existing:
                with existing.lock:
                    existing.refcount += 1
                self.logger.info(
                    "streaming attach path=%s backend=%s readers=%d",
                    canonical_path,
                    existing.backend.name,
                    existing.refcount,
                )
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
            chunk_size=self.chunk_size,
            io_fd=io_fd,
            refcount=1,
        )

        with self.prefetch_sessions_lock:
            existing = self.prefetch_sessions.get(canonical_path)
            if existing:
                with existing.lock:
                    existing.refcount += 1
                self.logger.info(
                    "streaming attach path=%s backend=%s readers=%d",
                    canonical_path,
                    existing.backend.name,
                    existing.refcount,
                )
                self._discard_unregistered_prefetch_session(session)
                return existing
            if len(self.prefetch_sessions) >= self.max_streaming_sessions:
                self.logger.warning(
                    "Streaming session limit reached (%d) during registration; falling back to full-file cache for %s",
                    self.max_streaming_sessions,
                    canonical_path,
                )
                self._discard_unregistered_prefetch_session(session)
                return None
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

            with self.prefetch_sessions_lock:
                still_registered = self.prefetch_sessions.get(session.canonical_path) is session

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
