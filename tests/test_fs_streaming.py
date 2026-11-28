import errno
import json
import os
import re
import shutil
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fuse import FuseOSError

from storage.backends.base import (
    current_capacity_cancel_probe, current_interactive_cancel_probe, is_interactive_request,
)
from storage.backends.local import LocalBackend
from storage.backends.ssh_pool import BulkCapacityCancelled, BulkCapacityTimeout
from storage.cache_metadata import MEMBER_ATTRS_XATTR
from storage.control import ControlError
from storage.fs import StorageFS
from storage.models import SyncStatus
from storage.streaming import (
    ACTIVE_DOWNLOAD_BURST, PrefetchSession, _DownloadPool, _PublicationYielded,
    validate_download_limits,
)
from storage.mount_profiles import parse_keep_after_close


class SlowRangeLocalBackend(LocalBackend):
    """Local backend test double with controllable read/copy latency and failures."""

    def __init__(self, name, root_path, range_delay=0.0, copy_delay=0.0,
                 fail_after_offset=None, max_read_bytes=None):
        super().__init__(name, root_path)
        self.range_delay = range_delay
        self.copy_delay = copy_delay
        self.fail_after_offset = fail_after_offset
        self.max_read_bytes = max_read_bytes
        # When set, a background worker's range read signals worker_in_read
        # and blocks until worker_gate is set; foreground reads pass through.
        self.worker_gate = None
        self.worker_in_read = threading.Event()

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        if self.fail_after_offset is not None and offset >= self.fail_after_offset:
            raise OSError(errno.EIO, "simulated backend range read failure")
        gate = self.worker_gate
        if gate is not None and threading.current_thread().name.startswith("storage-prefetch-"):
            self.worker_in_read.set()
            gate.wait(timeout=10.0)
        if self.range_delay:
            time.sleep(self.range_delay)
        data = super().read_file(virtual_path, offset, size)
        if self.max_read_bytes is not None and len(data) > self.max_read_bytes:
            return data[:self.max_read_bytes]
        return data

    def copy_to_local(self, virtual_path: str, local_path: Path):
        if self.copy_delay:
            time.sleep(self.copy_delay)
        return super().copy_to_local(virtual_path, local_path)


class TestFSStreamingIntegration(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_dir = self.temp_dir / "cache"
        self.cache_dir.mkdir()
        self.backend_dir = self.temp_dir / "backend"
        self.backend_dir.mkdir()
        self.db_path = self.temp_dir / "state.db"

        self.backend = SlowRangeLocalBackend(
            "backend",
            str(self.backend_dir),
            range_delay=0.02,
            copy_delay=0.35,
        )

        self.fs = self._make_fs()

    def _make_fs(self, keep_after_close=None, chunk_size=32 * 1024):
        return StorageFS(
            cache_root=self.cache_dir,
            state_db=self.db_path,
            backends={"backend": self.backend},
            cache_size=32 * 1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
            streaming_reads=True,
            streaming_chunk_size=chunk_size,
            max_streaming_sessions=2,
            streaming_keep_after_close=(
                parse_keep_after_close(keep_after_close) if keep_after_close is not None else None
            ),
        )

    def _remake_fs(self, keep_after_close=None, chunk_size=32 * 1024):
        self.fs.destroy('/')
        self.fs = self._make_fs(keep_after_close, chunk_size)

    def tearDown(self):
        if self.backend.worker_gate is not None:
            self.backend.worker_gate.set()
        try:
            self.fs.destroy('/')
        except Exception:
            pass
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _wait_for(self, predicate, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.05)
        return predicate()

    def test_download_workers_grow_for_runnable_files_and_retire_when_idle(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_streaming_sessions=3, max_download_workers=2)
        manager._download_pool.idle_seconds = 0.05
        self.backend.worker_gate = threading.Event()
        sessions = []
        for index in range(3):
            name = f"ramp-{index}.bin"
            payload = os.urandom(512 * 1024)
            (self.backend_dir / name).write_bytes(payload)
            path = "/" + name
            fd = self.fs.open(path, os.O_RDONLY)
            sessions.append((path, fd, self._session_for(fd)))
            self.assertEqual(self.fs.read(path, 32 * 1024, 0, fd), payload[:32 * 1024])
        self.assertTrue(self._wait_for(lambda: manager._download_pool.snapshot() == (2, 2, 1)))

        for path, fd, session in sessions:
            self.fs.release(path, fd)
            self.assertTrue(session.detached)
        self.assertEqual(manager._download_pool.snapshot(), (2, 2, 0))
        self.backend.worker_gate.set()
        for _, _, session in sessions:
            self.assertTrue(session.background_finished.wait(timeout=3))
            self.assertEqual(session.io_fd, -1)
        self.assertTrue(self._wait_for(lambda: manager._download_pool.snapshot() == (0, 0, 0)))

    def test_foreground_overlapping_range_can_use_interactive_reserve(self):
        payload = os.urandom(512 * 1024)
        (self.backend_dir / "reserve.bin").write_bytes(payload)
        chunk = 32 * 1024
        path = "/reserve.bin"
        bulk_waiting = threading.Event()
        bulk_available = threading.Event()
        self.addCleanup(bulk_available.set)
        reader_done = threading.Event()
        results = []

        def await_bulk(_path, fetch):
            bulk_waiting.set()
            if not bulk_available.wait(timeout=8):
                raise TimeoutError("bulk-capacity gate did not release")
            return fetch()

        def read_next(fd):
            try:
                results.append(self.fs.read(path, chunk, chunk, fd))
            finally:
                reader_done.set()

        with patch.object(self.backend, "run_background_range_read", side_effect=await_bulk), \
                patch.object(self.backend, "read_file", wraps=self.backend.read_file) as read:
            fd = self.fs.open(path, os.O_RDONLY)
            session = self._session_for(fd)
            self.assertEqual(self.fs.read(path, chunk, 0, fd), payload[:chunk])
            self.assertTrue(bulk_waiting.wait(timeout=3))
            reader = threading.Thread(target=read_next, args=(fd,), daemon=True)
            reader.start()
            try:
                self.assertTrue(reader_done.wait(timeout=3), "reader joined a speculative lease waiter")
                self.assertEqual(results, [payload[chunk:2 * chunk]])
                self.fs.release(path, fd)
            finally:
                bulk_available.set()
                reader.join(timeout=3)
            self.assertTrue(session.background_finished.wait(timeout=3))
            self.assertEqual([call.args for call in read.call_args_list], [(path, 0, chunk), (path, chunk, chunk)])

    def test_lower_retained_limit_drains_existing_fill_and_applies_to_new_closes(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_download_workers=1, max_retained_streaming_sessions=1)
        self.backend.worker_gate = threading.Event()
        payload = os.urandom(384 * 1024)

        def open_and_read(name):
            (self.backend_dir / name).write_bytes(payload)
            path = "/" + name
            fd = self.fs.open(path, os.O_RDONLY)
            session = self._session_for(fd)
            self.assertEqual(self.fs.read(path, 128 * 1024, 0, fd), payload[:128 * 1024])
            return path, fd, session

        older, fd, old_session = open_and_read("old-retained.bin")
        self.assertTrue(self.backend.worker_in_read.wait(timeout=3))
        self.fs.release(older, fd)
        self.assertEqual(manager.update_download_limits(max_retained_streaming_sessions=0)
                         ["max_retained_streaming_sessions"], 0)
        self.assertFalse(old_session.detached)

        newer, fd, new_session = open_and_read("new-retained.bin")
        self.assertTrue(new_session.readers_consumed)
        self.fs.release(newer, fd)
        self.assertTrue(new_session.detached)
        self.assertTrue(new_session.background_finished.wait(timeout=3))
        self.assertFalse((self.cache_dir / "new-retained.bin.storage.partial").exists())
        self.assertEqual(manager.list_download_sessions()[0]["id"], old_session.session_id)

        self.backend.worker_gate.set()
        self.assertTrue(old_session.background_finished.wait(timeout=3))
        self.assertEqual((self.cache_dir / "old-retained.bin").read_bytes(), payload)

    def test_reader_reclaims_retained_space_without_touching_local_writes(self):
        self.fs.cache_manager.max_size_bytes = 512 * 1024
        self.backend.worker_gate = threading.Event()
        payload = os.urandom(384 * 1024)
        for name in ("retained.bin", "new-reader.bin"):
            (self.backend_dir / name).write_bytes(payload)
        fd = self.fs.create("/dirty.bin", 0o644)
        self.fs.write("/dirty.bin", b"local write", 0, fd)
        self.fs.release("/dirty.bin", fd)

        fd = self.fs.open("/retained.bin", os.O_RDONLY)
        retained = self._session_for(fd)
        self.assertEqual(self.fs.read("/retained.bin", 128 * 1024, 0, fd), payload[:128 * 1024])
        self.assertTrue(self.backend.worker_in_read.wait(timeout=3))
        self.fs.release("/retained.bin", fd)
        self.assertFalse(retained.detached)

        fd = self.fs.open("/new-reader.bin", os.O_RDONLY)
        self._session_for(fd)
        self.assertEqual(self.fs.read("/new-reader.bin", 4096, 0, fd), payload[:4096])
        self.fs.release("/new-reader.bin", fd)
        self.assertTrue(retained.detached)
        self.assertEqual((self.cache_dir / "dirty.bin").read_bytes(), b"local write")
        self.assertEqual(self.fs._streaming_stats_snapshot()["streaming_retained_space_cancellations"], 1)

        self.backend.worker_gate.set()
        self.assertTrue(retained.background_finished.wait(timeout=3))
        self.assertEqual(retained.io_fd, -1)

    def _open_retained_fill(self, name, payload, read_bytes=128 * 1024):
        """Open, read enough to keep it after close, and close: a retained fill.

        Needs worker_gate set, so the worker is parked mid-chunk. The read
        is one foreground fetch of read_bytes from the start.
        """
        (self.backend_dir / name).write_bytes(payload)
        path = "/" + name
        fd = self.fs.open(path, os.O_RDONLY)
        session = self._session_for(fd)
        self.assertEqual(self.fs.read(path, read_bytes, 0, fd), payload[:read_bytes])
        self.assertTrue(session.readers_consumed)
        self.assertTrue(self.backend.worker_in_read.wait(timeout=3))
        self.fs.release(path, fd)
        self.assertFalse(session.detached)
        return session

    def test_reader_displaces_fill_retained_over_a_lowered_limit(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(
            max_streaming_sessions=1, max_retained_streaming_sessions=1, max_download_workers=1,
        )
        self.backend.worker_gate = threading.Event()
        payload = os.urandom(384 * 1024)
        retained = self._open_retained_fill("retained.bin", payload)
        manager.update_download_limits(max_retained_streaming_sessions=0)
        # Lowering alone lets it drain.
        self.assertFalse(retained.detached)

        (self.backend_dir / "reader.bin").write_bytes(payload)
        fd = self.fs.open("/reader.bin", os.O_RDONLY)
        reader = self._session_for(fd)
        self.assertTrue(retained.detached)
        self.assertFalse((self.cache_dir / "retained.bin.storage.partial").exists())
        self.assertEqual(
            self.fs._streaming_stats_snapshot()["streaming_retained_reader_cancellations"], 1)
        self.assertEqual(self.fs.read("/reader.bin", 4096, 0, fd), payload[:4096])
        self.assertEqual([entry["id"] for entry in manager.list_download_sessions()],
                         [reader.session_id])
        self.fs.release("/reader.bin", fd)

        self.backend.worker_gate.set()
        self.assertTrue(retained.background_finished.wait(timeout=3))
        self.assertEqual(retained.io_fd, -1)
        self.assertFalse((self.cache_dir / "retained.bin").exists())

    def test_reader_is_refused_only_by_readers_not_by_retained_fills(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_streaming_sessions=1, max_retained_streaming_sessions=0)
        payload = os.urandom(64 * 1024)
        (self.backend_dir / "first.bin").write_bytes(payload)
        (self.backend_dir / "second.bin").write_bytes(payload)
        fd = self.fs.open("/first.bin", os.O_RDONLY)
        first = self._session_for(fd)
        other = self.fs.open("/second.bin", os.O_RDONLY)
        with self.fs._fd_lock:
            self.assertIsNone(self.fs._fd_map[other].prefetch_session)
        self.assertFalse(first.detached)
        self.fs.release("/second.bin", other)
        self.fs.release("/first.bin", fd)

    def test_whole_file_fill_reclaims_space_held_by_retained_fills(self):
        self.fs.cache_manager.max_size_bytes = 512 * 1024
        self.backend.worker_gate = threading.Event()
        payload = os.urandom(384 * 1024)
        retained = self._open_retained_fill("retained.bin", payload)

        written = os.urandom(384 * 1024)
        (self.backend_dir / "write-open.bin").write_bytes(written)
        fd = self.fs.open("/write-open.bin", os.O_RDWR)
        self.assertTrue(retained.detached)
        self.assertEqual(self.fs.read("/write-open.bin", len(written), 0, fd), written)
        self.fs.release("/write-open.bin", fd)
        self.assertEqual((self.cache_dir / "write-open.bin").read_bytes(), written)
        self.assertEqual(self.fs._streaming_stats_snapshot()["streaming_retained_space_cancellations"], 1)

        self.backend.worker_gate.set()
        self.assertTrue(retained.background_finished.wait(timeout=3))
        self.assertEqual(retained.io_fd, -1)

    def test_whole_file_fill_still_fails_when_retained_fills_cannot_cover_it(self):
        self.fs.cache_manager.max_size_bytes = 512 * 1024
        self.backend.worker_gate = threading.Event()
        retained = self._open_retained_fill("retained.bin", os.urandom(256 * 1024))
        (self.backend_dir / "too-big.bin").write_bytes(os.urandom(768 * 1024))
        with self.assertRaises(FuseOSError):
            self.fs.open("/too-big.bin", os.O_RDWR)
        # Nothing is cancelled for a fill it cannot make room for.
        self.assertFalse(retained.detached)

    def test_closed_fill_is_not_retained_when_no_worker_can_start(self):
        manager = self.fs._streaming_manager
        real_start = threading.Thread.start

        def start(thread):
            if thread.name == "storage-prefetch-worker":
                raise RuntimeError("worker creation failed")
            return real_start(thread)

        payload = os.urandom(384 * 1024)
        (self.backend_dir / "unstaffed.bin").write_bytes(payload)
        with patch.object(threading.Thread, "start", start):
            fd = self.fs.open("/unstaffed.bin", os.O_RDONLY)
            session = self._session_for(fd)
            self.assertEqual(self.fs.read("/unstaffed.bin", 128 * 1024, 0, fd), payload[:128 * 1024])
            self.assertTrue(session.readers_consumed)
            self.assertEqual(manager._download_pool.snapshot(), (0, 0, 1))
            self.fs.release("/unstaffed.bin", fd)
        self.assertTrue(session.detached)
        self.assertTrue(session.background_finished.is_set())
        self.assertEqual(session.io_fd, -1)
        self.assertEqual(manager.list_download_sessions(), [])
        self.assertEqual(manager._download_pool.snapshot(), (0, 0, 0))
        self.assertFalse((self.cache_dir / "unstaffed.bin.storage.partial").exists())
        self.assertEqual(self.fs._streaming_stats_snapshot()["streaming_unstaffed_cancellations"], 1)

    def test_worker_blocked_on_one_members_bulk_capacity_yields_to_another(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_download_workers=1)
        other = SlowRangeLocalBackend("other", str(self.backend_dir), range_delay=0.0)
        payload = os.urandom(256 * 1024)
        for name in ("blocked.bin", "free.bin"):
            (self.backend_dir / name).write_bytes(payload)
        released = threading.Event()
        self.addCleanup(released.set)
        yields = []

        def await_bulk(_path, fetch):
            # Like the SSH pool's bulk wait: poll the turn's probe.
            probe = current_capacity_cancel_probe()
            while not released.is_set():
                reason = probe()
                if reason:
                    yields.append(reason)
                    raise BulkCapacityCancelled(reason)
                time.sleep(0.02)
            return fetch()

        def select(path):
            return other if path == "/free.bin" else self.backend

        with patch.object(manager, "select_backend_for_read", side_effect=select), \
                patch.object(self.backend, "run_background_range_read", side_effect=await_bulk):
            blocked_fd = self.fs.open("/blocked.bin", os.O_RDONLY)
            blocked = self._session_for(blocked_fd)
            self.assertEqual(self.fs.read("/blocked.bin", 128 * 1024, 0, blocked_fd), payload[:128 * 1024])
            self.assertTrue(self._wait_for(lambda: manager._download_pool.snapshot()[1] == 1))
            free_fd = self.fs.open("/free.bin", os.O_RDONLY)
            free = self._session_for(free_fd)
            self.assertIs(free.backend, other)
            self.assertEqual(self.fs.read("/free.bin", 128 * 1024, 0, free_fd), payload[:128 * 1024])
            self.assertTrue(free.background_finished.wait(timeout=10))
            self.assertTrue(free.finalized)
            self.assertFalse(blocked.prefetch_complete)
            self.assertTrue(yields)
            released.set()
            self.assertTrue(blocked.background_finished.wait(timeout=10))
            self.assertTrue(blocked.finalized)
            self.fs.release("/free.bin", free_fd)
            self.fs.release("/blocked.bin", blocked_fd)
        self.assertEqual((self.cache_dir / "free.bin").read_bytes(), payload)
        self.assertEqual((self.cache_dir / "blocked.bin").read_bytes(), payload)

    def test_download_worker_start_retry_and_inline_discard_on_release(self):
        manager = self.fs._streaming_manager
        manager._download_pool.idle_seconds = 0.05
        real_start = threading.Thread.start
        start_failures = True
        attempts = 0

        def start(thread):
            nonlocal attempts
            if thread.name == "storage-prefetch-worker":
                attempts += 1
                if start_failures or attempts == 1:
                    raise RuntimeError("worker creation failed")
            return real_start(thread)

        def read_file(name):
            payload = os.urandom(8192)
            (self.backend_dir / name).write_bytes(payload)
            path = "/" + name
            fd = self.fs.open(path, os.O_RDONLY)
            self.assertEqual(self.fs.read(path, len(payload), 0, fd), payload)
            return path, fd, payload

        with patch.object(threading.Thread, "start", start):
            path, fd, payload = read_file("no-workers.bin")
            self.assertGreaterEqual(attempts, 2)
            self.fs.release(path, fd)
            # Finished by the release with no worker: not confirmed, so
            # discarded, not published.
            self.assertFalse((self.cache_dir / "no-workers.bin").exists())
            self.assertEqual(list(self.cache_dir.rglob("*.storage.partial")), [])
            self.assertEqual(self.fs._streaming_stats_snapshot()["streaming_unconfirmed_discards"], 1)
            self.assertEqual(manager._download_pool.snapshot(), (0, 0, 0))

            start_failures = False
            attempts = 0
            path, fd, payload = read_file("retry.bin")
            session = self._session_for(fd)
            self.assertTrue(session.background_finished.wait(timeout=3))
            self.fs.release(path, fd)
            self.assertEqual(attempts, 2)
            self.assertEqual((self.cache_dir / "retry.bin").read_bytes(), payload)

    @contextmanager
    def _reclaim_measurements(self, on_measure):
        """Call on_measure(deficit) after each deficit measurement space reclaim makes.

        Patches the callees: _reclaim_retained_space marks its own extent
        (per thread), and get_free_space_needed reports inside it only.
        Inside reclaim nothing else measures the deficit.
        """
        manager = self.fs._streaming_manager
        cache = self.fs.cache_manager
        real_reclaim = manager._reclaim_retained_space
        real_needed = cache.get_free_space_needed
        inside = threading.local()

        def reclaim(*args, **kwargs):
            inside.active = True
            try:
                return real_reclaim(*args, **kwargs)
            finally:
                inside.active = False

        def needed(required):
            result = real_needed(required)
            if getattr(inside, "active", False):
                on_measure(result)
            return result

        with patch.object(manager, "_reclaim_retained_space", reclaim), \
                patch.object(cache, "get_free_space_needed", needed):
            yield

    def _bulk_gate(self, poll_probe=True):
        """A run_background_range_read that waits for bulk capacity until released.

        Like the SSH pool's bulk wait, it polls the turn's cancellation
        probe (unless poll_probe is False) and raises BulkCapacityCancelled
        with the probe's reason. Returns (fake, released, waiting, reasons).
        """
        released = threading.Event()
        self.addCleanup(released.set)
        waiting = threading.Event()
        reasons = []

        def await_bulk(_path, fetch):
            probe = current_capacity_cancel_probe()
            waiting.set()
            while not released.is_set():
                reason = probe() if poll_probe else None
                if reason:
                    reasons.append(reason)
                    raise BulkCapacityCancelled(reason)
                time.sleep(0.02)
            return fetch()

        return await_bulk, released, waiting, reasons

    def _fail_worker_starts(self, after):
        """A Thread.start that fails for download workers past the first `after`."""
        real_start = threading.Thread.start
        started = []

        def start(thread):
            if thread.name == "storage-prefetch-worker":
                started.append(thread)
                if len(started) > after:
                    raise RuntimeError("worker creation failed")
            return real_start(thread)

        return start, started

    def test_bulk_wait_ends_when_readers_fetch_every_byte(self):
        payload = os.urandom(256 * 1024)
        (self.backend_dir / "foreground.bin").write_bytes(payload)
        path = "/foreground.bin"
        half = 128 * 1024
        await_bulk, released, waiting, reasons = self._bulk_gate()
        with patch.object(self.backend, "run_background_range_read", side_effect=await_bulk):
            fd = self.fs.open(path, os.O_RDONLY)
            session = self._session_for(fd)
            self.assertEqual(self.fs.read(path, half, 0, fd), payload[:half])
            self.assertTrue(waiting.wait(timeout=3))
            # The interactive reserve serves the rest while the turn still
            # waits for a bulk channel that never comes free.
            self.assertEqual(self.fs.read(path, half, half, fd), payload[half:])
            self.assertTrue(session.background_finished.wait(timeout=3))
            self.assertTrue(session.finalized)
            self.assertFalse(released.is_set())
            self.assertTrue(reasons)
            self.fs.release(path, fd)
        self.assertEqual((self.cache_dir / "foreground.bin").read_bytes(), payload)
        self.assertEqual(self.fs._streaming_manager.list_download_sessions(), [])

    def _open_blocked_and_complete(self, await_bulk, waiting):
        """Open a file whose turn waits for bulk capacity, then read a second file whole.

        Both are on self.backend. Returns (blocked, blocked_fd, complete, complete_fd).
        """
        payload = os.urandom(256 * 1024)
        for name in ("blocked.bin", "complete.bin"):
            (self.backend_dir / name).write_bytes(payload)
        blocked_fd = self.fs.open("/blocked.bin", os.O_RDONLY)
        blocked = self._session_for(blocked_fd)
        self.assertEqual(self.fs.read("/blocked.bin", 128 * 1024, 0, blocked_fd), payload[:128 * 1024])
        self.assertTrue(waiting.wait(timeout=3))
        complete_fd = self.fs.open("/complete.bin", os.O_RDONLY)
        complete = self._session_for(complete_fd)
        self.assertEqual(self.fs.read("/complete.bin", len(payload), 0, complete_fd), payload)
        self.assertTrue(complete.prefetch_complete)
        return blocked, blocked_fd, complete, complete_fd, payload

    def test_publication_turn_is_not_held_by_the_members_bulk_ceiling(self):
        # The fake never yields, so only the scheduler can start the
        # publication: one member bulk slot, held by the blocked turn.
        await_bulk, released, waiting, _ = self._bulk_gate(poll_probe=False)
        with patch.object(self.backend, "run_background_range_read", side_effect=await_bulk), \
                patch.object(self.backend, "background_range_capacity", return_value=1):
            blocked, blocked_fd, complete, complete_fd, payload = self._open_blocked_and_complete(
                await_bulk, waiting)
            self.assertTrue(complete.background_finished.wait(timeout=3))
            self.assertTrue(complete.finalized)
            self.assertFalse(blocked.prefetch_complete)
            released.set()
            self.assertTrue(blocked.background_finished.wait(timeout=5))
            self.fs.release("/complete.bin", complete_fd)
            self.fs.release("/blocked.bin", blocked_fd)
        self.assertEqual((self.cache_dir / "complete.bin").read_bytes(), payload)
        self.assertEqual((self.cache_dir / "blocked.bin").read_bytes(), payload)

    def test_busy_workers_yield_to_a_queued_publication_on_their_own_member(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_download_workers=1)
        await_bulk, released, waiting, reasons = self._bulk_gate()
        with patch.object(self.backend, "run_background_range_read", side_effect=await_bulk):
            blocked, blocked_fd, complete, complete_fd, payload = self._open_blocked_and_complete(
                await_bulk, waiting)
            self.assertTrue(complete.background_finished.wait(timeout=5))
            self.assertTrue(complete.finalized)
            self.assertTrue(reasons)
            self.assertFalse(blocked.prefetch_complete)
            released.set()
            self.assertTrue(blocked.background_finished.wait(timeout=5))
            self.fs.release("/complete.bin", complete_fd)
            self.fs.release("/blocked.bin", blocked_fd)
        self.assertEqual((self.cache_dir / "complete.bin").read_bytes(), payload)
        self.assertEqual((self.cache_dir / "blocked.bin").read_bytes(), payload)

    def test_worker_yields_when_the_pools_second_worker_could_not_start(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_download_workers=2)
        other = SlowRangeLocalBackend("other", str(self.backend_dir), range_delay=0.0)
        payload = os.urandom(256 * 1024)
        for name in ("blocked.bin", "free.bin"):
            (self.backend_dir / name).write_bytes(payload)
        await_bulk, released, waiting, reasons = self._bulk_gate()
        start, started = self._fail_worker_starts(after=1)

        def select(path):
            return other if path == "/free.bin" else self.backend

        with patch.object(manager, "select_backend_for_read", side_effect=select), \
                patch.object(self.backend, "run_background_range_read", side_effect=await_bulk), \
                patch.object(threading.Thread, "start", start):
            blocked_fd = self.fs.open("/blocked.bin", os.O_RDONLY)
            blocked = self._session_for(blocked_fd)
            self.assertEqual(self.fs.read("/blocked.bin", 128 * 1024, 0, blocked_fd), payload[:128 * 1024])
            self.assertTrue(waiting.wait(timeout=3))
            free_fd = self.fs.open("/free.bin", os.O_RDONLY)
            free = self._session_for(free_fd)
            self.assertIs(free.backend, other)
            self.assertEqual(self.fs.read("/free.bin", 128 * 1024, 0, free_fd), payload[:128 * 1024])
            self.assertTrue(free.background_finished.wait(timeout=10))
            self.assertTrue(free.finalized)
            self.assertGreater(len(started), 1)
            self.assertEqual(manager._download_pool.snapshot()[0], 1)
            self.assertTrue(reasons)
            self.assertFalse(blocked.prefetch_complete)
            released.set()
            self.assertTrue(blocked.background_finished.wait(timeout=10))
            self.fs.release("/free.bin", free_fd)
            self.fs.release("/blocked.bin", blocked_fd)
        self.assertEqual((self.cache_dir / "free.bin").read_bytes(), payload)
        self.assertEqual((self.cache_dir / "blocked.bin").read_bytes(), payload)

    def test_concurrent_last_closes_without_workers_finish_both_files(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_download_workers=1)
        start, _ = self._fail_worker_starts(after=0)
        real_finalize = manager._finalize_streaming_session
        paused = threading.Event()
        resume = threading.Event()
        self.addCleanup(resume.set)

        def finalize(session):
            if session.canonical_path == "/first.bin":
                paused.set()
                resume.wait(timeout=10)
            return real_finalize(session)

        files = {}
        with patch.object(threading.Thread, "start", start), \
                patch.object(manager, "_finalize_streaming_session", side_effect=finalize):
            for name in ("first.bin", "second.bin"):
                payload = os.urandom(8192)
                (self.backend_dir / name).write_bytes(payload)
                path = "/" + name
                fd = self.fs.open(path, os.O_RDONLY)
                session = self._session_for(fd)
                self.assertEqual(self.fs.read(path, len(payload), 0, fd), payload)
                self.assertTrue(session.prefetch_complete)
                files[name] = (path, fd, session, payload)
            self.assertEqual(manager._download_pool.snapshot(), (0, 0, 2))

            path, fd, first, _ = files["first.bin"]
            closer = threading.Thread(target=self.fs.release, args=(path, fd), daemon=True)
            closer.start()
            try:
                self.assertTrue(paused.wait(timeout=3))
                path, fd, second, _ = files["second.bin"]
                # The inline publication of first.bin holds the only turn.
                self.fs.release(path, fd)
                self.assertFalse(second.finalized)
            finally:
                resume.set()
                closer.join(timeout=5)
            self.assertFalse(closer.is_alive())
        for name, (_, _, session, payload) in files.items():
            # Both finished (inline, so discarded, not published).
            self.assertTrue(session.finalized, name)
            self.assertTrue(session.background_finished.is_set(), name)
            self.assertEqual(session.io_fd, -1, name)
            self.assertFalse((self.cache_dir / name).exists(), name)
        self.assertEqual(list(self.cache_dir.rglob("*.storage.partial")), [])
        self.assertEqual(self.fs._streaming_stats_snapshot()["streaming_unconfirmed_discards"], 2)
        self.assertEqual(manager.list_download_sessions(), [])
        self.assertEqual(manager._download_pool.snapshot(), (0, 0, 0))

    def test_whole_file_fill_uses_space_a_cancellation_frees_during_reclaim(self):
        manager = self.fs._streaming_manager
        cache = self.fs.cache_manager
        cache.max_size_bytes = 512 * 1024
        self.backend.worker_gate = threading.Event()
        retained = self._open_retained_fill("retained.bin", os.urandom(384 * 1024))
        written = os.urandom(384 * 1024)
        (self.backend_dir / "write-open.bin").write_bytes(written)
        injected = []

        def measured(deficit):
            # Between reclaim's first look at the deficit and its scan of
            # the registry, the retained fill is cancelled by hand.
            if not injected:
                injected.append(deficit)
                self.assertEqual(manager.cancel_download_session(retained.session_id), "cancelled")

        with self._reclaim_measurements(measured):
            fd = self.fs.open("/write-open.bin", os.O_RDWR)
        self.assertEqual(len(injected), 1)
        self.assertGreater(injected[0], 0)
        self.assertTrue(retained.detached)
        self.assertEqual(self.fs.read("/write-open.bin", len(written), 0, fd), written)
        self.fs.release("/write-open.bin", fd)
        self.assertEqual((self.cache_dir / "write-open.bin").read_bytes(), written)
        self.assertNotIn("streaming_retained_space_cancellations", self.fs._streaming_stats_snapshot())
        self.backend.worker_gate.set()
        self.assertTrue(retained.background_finished.wait(timeout=3))

    def test_space_reclaim_stops_the_retained_fill_with_fewest_bytes_first(self):
        self.fs.cache_manager.max_size_bytes = 1024 * 1024
        self.backend.worker_gate = threading.Event()
        more = self._open_retained_fill("more.bin", os.urandom(384 * 1024), read_bytes=256 * 1024)
        fewer = self._open_retained_fill("fewer.bin", os.urandom(384 * 1024))
        written = os.urandom(384 * 1024)
        (self.backend_dir / "write-open.bin").write_bytes(written)
        fd = self.fs.open("/write-open.bin", os.O_RDWR)
        self.fs.release("/write-open.bin", fd)
        self.assertTrue(fewer.detached)
        self.assertFalse(more.detached)
        self.assertEqual(self.fs._streaming_stats_snapshot()["streaming_retained_space_cancellations"], 1)
        self.backend.worker_gate.set()
        self.assertTrue(more.background_finished.wait(timeout=5))
        self.assertTrue(more.finalized)

    def test_reader_that_cannot_fit_displaces_no_retained_fill(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_streaming_sessions=1, max_retained_streaming_sessions=1)

        def fake(name, **state):
            session = PrefetchSession(
                canonical_path="/" + name, virtual_path="/" + name, backend=self.backend,
                file_size=4096, final_cache_path=self.cache_dir / name,
                partial_cache_path=self.cache_dir / (name + ".storage.partial"),
                chunk_size=4096, io_fd=-1,
            )
            for key, value in state.items():
                setattr(session, key, value)
            return session

        # Two readerless files still publishing fill the bound of two with
        # no reader; stopping the retained fill could not make room.
        sessions = [fake("publishing-0", prefetch_complete=True),
                    fake("publishing-1", prefetch_complete=True),
                    fake("retained")]
        with manager.prefetch_sessions_lock:
            for session in sessions:
                manager.prefetch_sessions[session.canonical_path] = session
            try:
                displaced = []
                self.assertFalse(manager._reader_fits_locked(displaced))
                self.assertEqual(displaced, [])
                self.assertFalse(sessions[2].stop_requested)
            finally:
                for session in sessions:
                    manager.prefetch_sessions.pop(session.canonical_path, None)

    def test_registration_survives_a_retained_fill_completing_before_the_victim_scan(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(
            max_streaming_sessions=1, max_retained_streaming_sessions=1, max_download_workers=1,
        )
        self.backend.worker_gate = threading.Event()
        retained = self._open_retained_fill("retained.bin", os.urandom(384 * 1024))
        manager.update_download_limits(max_retained_streaming_sessions=0)
        payload = os.urandom(64 * 1024)
        (self.backend_dir / "reader.bin").write_bytes(payload)
        real_counts = manager._download_session_counts
        real_fits = manager._reader_fits_locked
        displacing = threading.local()
        counted = []

        def fits(displaced=None):
            displacing.active = displaced is not None
            try:
                return real_fits(displaced)
            finally:
                displacing.active = False

        def counts():
            result = real_counts()
            # In the displacing call: the fill completes (under its own
            # lock only) after the count and before the victim scan.
            if not counted and getattr(displacing, "active", False):
                counted.append(result)
                with retained.lock:
                    retained.prefetch_complete = True
            return result

        with patch.object(manager, "_reader_fits_locked", fits), \
                patch.object(manager, "_download_session_counts", counts):
            fd = self.fs.open("/reader.bin", os.O_RDONLY)
        self.assertEqual(counted, [(0, 1)])
        with self.fs._fd_lock:
            # Refused (the completed fill still holds the slot): the
            # whole-file path, with the renamed partial unlinked.
            self.assertIsNone(self.fs._fd_map[fd].prefetch_session)
        self.assertFalse(retained.detached)
        self.assertEqual(sorted(path.name for path in self.cache_dir.rglob("*.storage.partial")),
                         ["retained.bin.storage.partial"])
        self.assertEqual(self.fs.read("/reader.bin", len(payload), 0, fd), payload)
        self.fs.release("/reader.bin", fd)

    def test_reclaim_selects_again_when_a_cancellation_shrinks_the_deficit(self):
        manager = self.fs._streaming_manager
        cache = self.fs.cache_manager
        cache.max_size_bytes = 1024 * 1024
        self.backend.worker_gate = threading.Event()
        large = self._open_retained_fill("large.bin", os.urandom(512 * 1024))
        small = self._open_retained_fill("small.bin", os.urandom(192 * 1024), read_bytes=96 * 1024)
        written = os.urandom(896 * 1024)
        (self.backend_dir / "write-open.bin").write_bytes(written)
        injected = []

        def measured(deficit):
            # The large fill is cancelled by hand after reclaim's first
            # look: the deficit drops from 576 KiB, which the small fill
            # cannot cover, to 64 KiB, which it can.
            if not injected:
                injected.append(deficit)
                self.assertEqual(manager.cancel_download_session(large.session_id), "cancelled")

        with self._reclaim_measurements(measured):
            fd = self.fs.open("/write-open.bin", os.O_RDWR)
        self.assertEqual(injected, [576 * 1024])
        self.assertTrue(small.detached)
        self.assertEqual(self.fs._streaming_stats_snapshot()["streaming_retained_space_cancellations"], 1)
        self.assertEqual(self.fs.read("/write-open.bin", len(written), 0, fd), written)
        self.fs.release("/write-open.bin", fd)
        self.backend.worker_gate.set()
        self.assertTrue(small.background_finished.wait(timeout=3))
        self.assertTrue(large.background_finished.wait(timeout=3))

    def test_reclaim_keeps_selecting_while_cancellations_shrink_the_deficit(self):
        manager = self.fs._streaming_manager
        cache = self.fs.cache_manager
        cache.max_size_bytes = 1024 * 1024
        self.backend.worker_gate = threading.Event()
        fills = [self._open_retained_fill(f"fill-{index}.bin", os.urandom(256 * 1024))
                 for index in range(4)]
        written = os.urandom(992 * 1024)
        (self.backend_dir / "write-open.bin").write_bytes(written)
        measured = []

        def measure(deficit):
            measured.append(deficit)
            # After each of the first three measurements a different fill
            # is cancelled by hand before reclaim scans, so each of those
            # selections falls short of a deficit that then shrinks by a
            # fill. The fourth is covered by the last.
            if len(measured) <= 3:
                self.assertEqual(
                    manager.cancel_download_session(fills[len(measured) - 1].session_id), "cancelled")

        with self._reclaim_measurements(measure):
            fd = self.fs.open("/write-open.bin", os.O_RDWR)
        self.assertEqual(measured, [kib * 1024 for kib in (992, 736, 480, 224)])
        self.assertTrue(all(fill.detached for fill in fills))
        self.assertEqual(self.fs._streaming_stats_snapshot()["streaming_retained_space_cancellations"], 1)
        self.assertEqual(self.fs.read("/write-open.bin", len(written), 0, fd), written)
        self.fs.release("/write-open.bin", fd)
        self.backend.worker_gate.set()
        for fill in fills:
            self.assertTrue(fill.background_finished.wait(timeout=3))

    def test_reclaim_without_retained_fills_measures_once_more_and_stops(self):
        # No retained fill exists; unrelated activity keeps shrinking the
        # deficit without clearing it. Reclaim remeasures once, then gives
        # up instead of rescanning for as long as the deficit shrinks.
        manager = self.fs._streaming_manager
        deficits = iter([5000, 4000, 3000, 2000, 1000])
        measured = []

        def needed(required):
            measured.append(required)
            return next(deficits)

        with patch.object(self.fs.cache_manager, "get_free_space_needed", needed):
            self.assertFalse(manager._reclaim_retained_space(4096, for_reader=False))
        self.assertEqual(len(measured), 2)

    def test_stalled_publication_does_not_hold_the_members_range_slot(self):
        manager = self.fs._streaming_manager
        real_finalize = manager._finalize_streaming_session
        paused = threading.Event()
        resume = threading.Event()
        self.addCleanup(resume.set)

        def finalize(session):
            if session.canonical_path == "/publishing.bin":
                paused.set()
                resume.wait(timeout=10)
            return real_finalize(session)

        publishing_payload = os.urandom(64 * 1024)
        (self.backend_dir / "publishing.bin").write_bytes(publishing_payload)
        payload = os.urandom(256 * 1024)
        (self.backend_dir / "reading.bin").write_bytes(payload)
        with patch.object(self.backend, "background_range_capacity", return_value=1), \
                patch.object(manager, "_finalize_streaming_session", side_effect=finalize):
            publishing_fd = self.fs.open("/publishing.bin", os.O_RDONLY)
            publishing = self._session_for(publishing_fd)
            # One chunk read; the worker fetches the last one and moves
            # on to publication, which stalls.
            self.assertEqual(self.fs.read("/publishing.bin", 32 * 1024, 0, publishing_fd),
                             publishing_payload[:32 * 1024])
            self.assertTrue(paused.wait(timeout=3))
            try:
                reading_fd = self.fs.open("/reading.bin", os.O_RDONLY)
                reading = self._session_for(reading_fd)
                self.assertEqual(self.fs.read("/reading.bin", 128 * 1024, 0, reading_fd), payload[:128 * 1024])
                self.assertTrue(reading.background_finished.wait(timeout=5))
                self.assertTrue(reading.finalized)
                self.assertFalse(publishing.finalized)
            finally:
                resume.set()
            self.assertTrue(publishing.background_finished.wait(timeout=3))
            self.fs.release("/reading.bin", reading_fd)
            self.fs.release("/publishing.bin", publishing_fd)
        self.assertEqual((self.cache_dir / "publishing.bin").read_bytes(), publishing_payload)
        self.assertEqual((self.cache_dir / "reading.bin").read_bytes(), payload)

    def test_publications_arriving_faster_than_drained_do_not_starve_range_reads(self):
        class Fake:
            """A queued session as the pool sees it; hashed by identity."""

            def __init__(self, name):
                self.canonical_path = name
                self.refcount = 0
                self.backend = SimpleNamespace(name="member", background_range_capacity=lambda: None)

        pool = _DownloadPool(1, lambda session: None, self.fs.logger)
        ranged = Fake("/retained")
        taken = []
        with pool.condition:
            pool.ready[ranged] = (False, False)
            for index in range(ACTIVE_DOWNLOAD_BURST + 1):
                # One more publication is queued before every take.
                pool.ready[Fake(f"/published-{index}")] = (False, True)
                session = pool._take()
                taken.append(session)
                del pool.running[session]
        self.assertTrue(all(session is not ranged for session in taken[:-1]))
        self.assertIs(taken[-1], ranged)

    def test_take_checks_capacity_only_until_each_priority_has_a_choice(self):
        checks = []

        class Fake:
            def __init__(self, name):
                self.canonical_path = name
                self.refcount = 0
                self.backend = SimpleNamespace(
                    name="member", background_range_capacity=lambda: checks.append(name))

        pool = _DownloadPool(1, lambda session: None, self.fs.logger)
        with pool.condition:
            for index in range(3):
                pool.ready[Fake(f"/active-{index}")] = (True, False)
                pool.ready[Fake(f"/retained-{index}")] = (False, False)
            publication = Fake("/published")
            pool.ready[publication] = (False, True)
            # Only _take's own scan: the contention refresh after a take
            # checks every queued session by design (_eligible).
            with patch.object(pool, "_refresh_contention"):
                self.assertIs(pool._take(), publication)
        self.assertEqual(checks, ["/active-0", "/retained-0"])

    def _bulk_gated_stat(self, gate, applies):
        """A stat that waits on gate when called as a bulk request and applies()."""
        real_stat = self.backend.stat
        bulk_calls = []

        def stat(path):
            if not is_interactive_request() and applies():
                bulk_calls.append(path)
                gate.wait(timeout=10)
            return real_stat(path)

        return stat, bulk_calls

    def test_confirming_stat_on_a_worker_cannot_wedge_the_pool(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_download_workers=1)
        other = SlowRangeLocalBackend("other", str(self.backend_dir), range_delay=0.0)
        payload = os.urandom(64 * 1024)
        for name in ("gated.bin", "free.bin"):
            (self.backend_dir / name).write_bytes(payload)
        gate = threading.Event()
        self.addCleanup(gate.set)
        # Like sync holding every bulk channel: a bulk stat from a download
        # worker would wait for the whole test.
        stat, bulk_calls = self._bulk_gated_stat(
            gate, lambda: threading.current_thread().name.startswith("storage-prefetch-"))

        def select(path):
            return other if path == "/free.bin" else self.backend

        with patch.object(manager, "select_backend_for_read", side_effect=select), \
                patch.object(self.backend, "stat", side_effect=stat):
            gated_fd = self.fs.open("/gated.bin", os.O_RDONLY)
            gated = self._session_for(gated_fd)
            # One chunk read; the worker fetches the rest, then finalizes.
            self.assertEqual(self.fs.read("/gated.bin", 32 * 1024, 0, gated_fd), payload[:32 * 1024])
            self.assertTrue(gated.background_finished.wait(timeout=5))
            self.assertTrue(gated.finalized)
            free_fd = self.fs.open("/free.bin", os.O_RDONLY)
            free = self._session_for(free_fd)
            self.assertEqual(self.fs.read("/free.bin", 32 * 1024, 0, free_fd), payload[:32 * 1024])
            self.assertTrue(free.background_finished.wait(timeout=5))
            self.assertTrue(free.finalized)
            self.assertEqual(bulk_calls, [])
            self.fs.release("/free.bin", free_fd)
            self.fs.release("/gated.bin", gated_fd)
        self.assertEqual((self.cache_dir / "gated.bin").read_bytes(), payload)

    def test_inline_publication_makes_no_member_stat_and_discards(self):
        start, _ = self._fail_worker_starts(after=0)
        payload = os.urandom(8192)
        (self.backend_dir / "inline.bin").write_bytes(payload)
        real_stat = self.backend.stat
        armed = threading.Event()
        stats = []

        def stat(path):
            if armed.is_set():
                stats.append(path)
            return real_stat(path)

        with patch.object(threading.Thread, "start", start), \
                patch.object(self.backend, "stat", side_effect=stat):
            fd = self.fs.open("/inline.bin", os.O_RDONLY)
            session = self._session_for(fd)
            self.assertEqual(self.fs.read("/inline.bin", len(payload), 0, fd), payload)
            armed.set()
            self.fs.release("/inline.bin", fd)
        # The member could be stat'ed, but an inline publication does not.
        self.assertEqual(stats, [])
        self.assertTrue(session.finalized)
        self.assertEqual(session.io_fd, -1)
        self.assertFalse((self.cache_dir / "inline.bin").exists())
        self.assertEqual(list(self.cache_dir.rglob("*.storage.partial")), [])
        self.assertEqual(self.fs._streaming_stats_snapshot()["streaming_unconfirmed_discards"], 1)
        # The next open (with workers) fetches the file again.
        with patch.object(self.backend, "read_file", wraps=self.backend.read_file) as read:
            fd = self.fs.open("/inline.bin", os.O_RDONLY)
            again = self._session_for(fd)
            self.assertEqual(self.fs.read("/inline.bin", len(payload), 0, fd), payload)
            self.assertTrue(again.background_finished.wait(timeout=5))
            self.fs.release("/inline.bin", fd)
        self.assertTrue(read.called)
        self.assertEqual((self.cache_dir / "inline.bin").read_bytes(), payload)

    def test_retained_fill_whose_version_changed_releases_its_charge(self):
        manager = self.fs._streaming_manager
        self.backend.worker_gate = threading.Event()
        real_stat = self.backend.stat

        def stat(path):
            st = real_stat(path)
            if not threading.current_thread().name.startswith("storage-prefetch-"):
                return st
            # The confirming stat sees a newer version (one second later).
            fields = {key: getattr(st, key) for key in (
                "st_mode", "st_uid", "st_gid", "st_nlink", "st_size", "st_atime", "st_ctime",
                "st_atime_ns")}
            fields.update(st_mtime=st.st_mtime + 1, st_mtime_ns=st.st_mtime_ns + 1_000_000_000)
            return SimpleNamespace(**fields)

        with patch.object(self.backend, "stat", side_effect=stat):
            retained = self._open_retained_fill("changed.bin", os.urandom(256 * 1024))
            self.backend.worker_gate.set()
            self.assertTrue(retained.background_finished.wait(timeout=5))
        stats = self.fs._streaming_stats_snapshot()
        self.assertEqual(stats.get("streaming_member_changed_discards"), 1)
        self.assertFalse([key for key in stats if key.startswith("streaming_retained_")
                          and key.endswith("_cancellations")])
        self.assertTrue(retained.detached)
        self.assertEqual(retained.io_fd, -1)
        self.assertEqual(manager.list_download_sessions(), [])
        self.assertFalse((self.cache_dir / "changed.bin").exists())
        self.assertEqual(list(self.cache_dir.rglob("*.storage.partial")), [])
        self.assertEqual(self.fs.cache_manager.get_total_cache_size(), 0)

    def test_bulk_timeout_after_readers_complete_the_file_is_not_an_error(self):
        payload = os.urandom(64 * 1024)
        (self.backend_dir / "raced.bin").write_bytes(payload)
        waiting = threading.Event()
        completed = threading.Event()
        self.addCleanup(completed.set)

        def timed_out(_path, _fetch):
            waiting.set()
            # Paused between the timeout and its handling while the reader
            # fetches the last byte.
            completed.wait(timeout=10)
            raise BulkCapacityTimeout("no bulk SFTP capacity came free")

        with patch.object(self.backend, "run_background_range_read", side_effect=timed_out):
            fd = self.fs.open("/raced.bin", os.O_RDONLY)
            session = self._session_for(fd)
            self.assertEqual(self.fs.read("/raced.bin", 32 * 1024, 0, fd), payload[:32 * 1024])
            self.assertTrue(waiting.wait(timeout=3))
            self.assertEqual(self.fs.read("/raced.bin", 32 * 1024, 32 * 1024, fd), payload[32 * 1024:])
            self.assertTrue(session.prefetch_complete)
            completed.set()
            self.assertTrue(session.background_finished.wait(timeout=5))
            self.assertIsNone(session.error)
            self.assertTrue(session.finalized)
            self.fs.release("/raced.bin", fd)
        self.assertEqual((self.cache_dir / "raced.bin").read_bytes(), payload)

    def test_background_bytes_are_written_after_the_member_channel_is_returned(self):
        manager = self.fs._streaming_manager
        state = {"held": False}
        writes = []
        real_write = manager._write_session_bytes

        def lease(_path, fetch):
            state["held"] = True
            try:
                return fetch()
            finally:
                state["held"] = False

        def write(session, offset, data):
            if threading.current_thread().name.startswith("storage-prefetch-"):
                writes.append(state["held"])
            return real_write(session, offset, data)

        payload = os.urandom(128 * 1024)
        (self.backend_dir / "lease.bin").write_bytes(payload)
        with patch.object(self.backend, "run_background_range_read", side_effect=lease), \
                patch.object(manager, "_write_session_bytes", side_effect=write):
            fd = self.fs.open("/lease.bin", os.O_RDONLY)
            session = self._session_for(fd)
            # Half the file: consumed, so the worker fetches the rest.
            self.assertEqual(self.fs.read("/lease.bin", 64 * 1024, 0, fd), payload[:64 * 1024])
            self.assertTrue(session.readers_consumed)
            self.assertTrue(session.background_finished.wait(timeout=5))
            self.fs.release("/lease.bin", fd)
        self.assertTrue(writes)
        self.assertEqual(set(writes), {False})
        self.assertEqual((self.cache_dir / "lease.bin").read_bytes(), payload)

    def test_partial_is_charged_once_while_registration_renames_it(self):
        cache = self.fs.cache_manager
        cache.max_size_bytes = 512 * 1024
        payload = os.urandom(384 * 1024)
        (self.backend_dir / "charged.bin").write_bytes(payload)
        real_replace = os.replace
        needed = {}

        def scan_and_measure(label, scan):
            with cache._lock:
                if scan:
                    cache._size_dirty = True
                else:
                    # A scan that saw neither name: the staging file is
                    # leaving, the partial not yet there.
                    cache._current_size_bytes = 0
                    cache._size_dirty = False
                needed[label] = cache.get_free_space_needed(256 * 1024)

        def replace(src, dst):
            if ".storage-staging-" not in str(src):
                return real_replace(src, dst)
            scan_and_measure("staging name", scan=True)
            scan_and_measure("neither name", scan=False)
            result = real_replace(src, dst)
            scan_and_measure("partial name", scan=True)
            return result

        with patch("storage.streaming.os.replace", side_effect=replace):
            fd = self.fs.open("/charged.bin", os.O_RDONLY)
        self._session_for(fd)
        # 512 KiB - 384 KiB charged once leaves 128 KiB of a 256 KiB
        # request short, whichever name the scan saw, or neither.
        self.assertEqual(needed, {label: 128 * 1024
                                  for label in ("staging name", "neither name", "partial name")})
        # After registration the partial is counted by name, once.
        self.assertEqual(cache._reserved_bytes, 0)
        self.assertEqual(cache.get_free_space_needed(256 * 1024), 128 * 1024)
        self.fs.release("/charged.bin", fd)

    def test_retained_fill_whose_turn_raises_is_not_stranded(self):
        manager = self.fs._streaming_manager
        self.backend.worker_gate = threading.Event()
        raise_now = threading.Event()
        real_probe = manager._bulk_wait_probe

        def probe(session):
            # Outside every handler in _run_download_turn: the turn raises
            # into the pool.
            if raise_now.is_set():
                raise RuntimeError("unexpected turn failure")
            return real_probe(session)

        with patch.object(manager, "_bulk_wait_probe", side_effect=probe):
            retained = self._open_retained_fill("raises.bin", os.urandom(384 * 1024))
            raise_now.set()
            self.backend.worker_gate.set()
            self.assertTrue(retained.background_finished.wait(timeout=5))
        self.assertIsNotNone(retained.error)
        self.assertTrue(retained.detached)
        self.assertEqual(retained.io_fd, -1)
        self.assertEqual(manager.list_download_sessions(), [])
        self.assertTrue(self._wait_for(lambda: manager._download_pool.snapshot()[1:] == (0, 0)))

    INVALID_DOWNLOAD_LIMITS = (
        {"max_streaming_sessions": 0},
        {"max_streaming_sessions": 1025},
        {"max_download_workers": 0},
        {"max_download_workers": 33},
        {"max_retained_streaming_sessions": -1},
        {"max_retained_streaming_sessions": 1025},
        {"max_download_workers": True},
        {"max_streaming_sessions": 2.0},
        {"max_streaming_sessions": "2"},
        {"max_streaming_sessions": None},
        {"max_parallel_downloads": 2},
    )

    def test_invalid_download_limits_are_rejected(self):
        manager = self.fs._streaming_manager
        before = manager.download_limits()
        for limits in self.INVALID_DOWNLOAD_LIMITS:
            with self.subTest(limits=limits):
                with self.assertRaises(ValueError):
                    validate_download_limits(limits)
                with self.assertRaises(ValueError):
                    manager.update_download_limits(**limits)
                self.assertEqual(manager.download_limits(), before)
        for limits in ({"max_streaming_sessions": 1}, {"max_retained_streaming_sessions": 0},
                       {"max_download_workers": 32}):
            with self.subTest(limits=limits):
                validate_download_limits(limits)

    def _channel_busy_stat(self, busy, applies=lambda path: True):
        """A stat whose interactive requests find the member's one channel taken.

        Like the SSH pool: while busy is set the request waits, polling the
        interactive wait probe and ending with BulkCapacityCancelled when it
        gives a reason; with no probe it waits (as for the connect
        deadline) until busy clears.
        """
        real_stat = self.backend.stat
        reasons = []

        def stat(path):
            if is_interactive_request() and applies(path):
                probe = current_interactive_cancel_probe()
                token, started = object(), time.monotonic()
                deadline = started + 10
                while busy.is_set() and time.monotonic() < deadline:
                    reason = probe(token, time.monotonic() - started) if probe else None
                    if reason:
                        reasons.append(reason)
                        raise BulkCapacityCancelled(reason)
                    time.sleep(0.02)
            return real_stat(path)

        return stat, reasons

    def test_confirmation_waiting_for_a_channel_yields_the_worker(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_download_workers=1)
        other = SlowRangeLocalBackend("other", str(self.backend_dir), range_delay=0.0)
        payload = os.urandom(64 * 1024)
        for name in ("waiting.bin", "free.bin"):
            (self.backend_dir / name).write_bytes(payload)
        busy = threading.Event()
        self.addCleanup(busy.clear)
        stat, reasons = self._channel_busy_stat(busy, lambda path: path == "/waiting.bin")

        def select(path):
            return other if path == "/free.bin" else self.backend

        with patch.object(manager, "select_backend_for_read", side_effect=select), \
                patch.object(self.backend, "stat", side_effect=stat):
            waiting_fd = self.fs.open("/waiting.bin", os.O_RDONLY)
            waiting = self._session_for(waiting_fd)
            busy.set()
            # The whole file by the reader; the worker's turn publishes,
            # and its confirming stat finds the channel taken.
            self.assertEqual(self.fs.read("/waiting.bin", len(payload), 0, waiting_fd), payload)
            free_fd = self.fs.open("/free.bin", os.O_RDONLY)
            free = self._session_for(free_fd)
            self.assertEqual(self.fs.read("/free.bin", 32 * 1024, 0, free_fd), payload[:32 * 1024])
            self.assertTrue(free.background_finished.wait(timeout=5))
            self.assertTrue(free.finalized)
            self.assertTrue(reasons)
            self.assertFalse(waiting.finalized)
            busy.clear()
            self.assertTrue(waiting.background_finished.wait(timeout=5))
            self.assertTrue(waiting.finalized)
            self.fs.release("/free.bin", free_fd)
            self.fs.release("/waiting.bin", waiting_fd)
        self.assertEqual((self.cache_dir / "waiting.bin").read_bytes(), payload)

    def test_yielded_confirmation_is_not_held_by_the_members_bulk_ceiling(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_download_workers=2, max_streaming_sessions=3)
        other = SlowRangeLocalBackend("other", str(self.backend_dir), range_delay=0.0)
        payload = os.urandom(64 * 1024)
        for name in ("bulk.bin", "confirming.bin", "free.bin"):
            (self.backend_dir / name).write_bytes(payload)
        busy = threading.Event()
        self.addCleanup(busy.clear)
        stat, reasons = self._channel_busy_stat(busy, lambda path: path == "/confirming.bin")
        await_bulk, released, waiting, _ = self._bulk_gate(poll_probe=False)

        def select(path):
            return other if path == "/free.bin" else self.backend

        with patch.object(manager, "select_backend_for_read", side_effect=select), \
                patch.object(self.backend, "stat", side_effect=stat), \
                patch.object(self.backend, "background_range_capacity", return_value=1), \
                patch.object(self.backend, "run_background_range_read", side_effect=await_bulk):
            # One worker holds the member's only bulk slot, and keeps it.
            bulk_fd = self.fs.open("/bulk.bin", os.O_RDONLY)
            self.assertEqual(self.fs.read("/bulk.bin", 32 * 1024, 0, bulk_fd), payload[:32 * 1024])
            self.assertTrue(waiting.wait(timeout=3))
            # The other publishes a complete file; its confirmation waits
            # for a channel and yields to a file on another member.
            confirming_fd = self.fs.open("/confirming.bin", os.O_RDONLY)
            confirming = self._session_for(confirming_fd)
            busy.set()
            self.assertEqual(self.fs.read("/confirming.bin", len(payload), 0, confirming_fd), payload)
            free_fd = self.fs.open("/free.bin", os.O_RDONLY)
            free = self._session_for(free_fd)
            self.assertEqual(self.fs.read("/free.bin", 32 * 1024, 0, free_fd), payload[:32 * 1024])
            self.assertTrue(free.background_finished.wait(timeout=5))
            self.assertTrue(reasons)
            self.assertFalse(confirming.finalized)
            # The interactive reserve frees while bulk stays saturated.
            busy.clear()
            self.assertTrue(confirming.background_finished.wait(timeout=5))
            self.assertTrue(confirming.finalized)
            self.assertFalse(released.is_set())
            released.set()
            for path, fd in (("/free.bin", free_fd), ("/confirming.bin", confirming_fd),
                             ("/bulk.bin", bulk_fd)):
                self.fs.release(path, fd)
        self.assertEqual((self.cache_dir / "confirming.bin").read_bytes(), payload)

    def test_stop_during_a_confirmation_wait_cleans_up_the_session(self):
        payload = os.urandom(64 * 1024)
        (self.backend_dir / "unmounting.bin").write_bytes(payload)
        busy = threading.Event()
        self.addCleanup(busy.clear)
        stat, reasons = self._channel_busy_stat(busy)
        real_finalize = self.fs._streaming_manager._finalize_streaming_session
        confirming = threading.Event()

        def finalize(session):
            confirming.set()
            return real_finalize(session)

        with patch.object(self.backend, "stat", side_effect=stat), \
                patch.object(self.fs._streaming_manager, "_finalize_streaming_session", side_effect=finalize):
            fd = self.fs.open("/unmounting.bin", os.O_RDONLY)
            session = self._session_for(fd)
            busy.set()
            self.assertEqual(self.fs.read("/unmounting.bin", len(payload), 0, fd), payload)
            self.fs.release("/unmounting.bin", fd)
            self.assertTrue(confirming.wait(timeout=3))
            # Unmount while the worker's confirmation waits for a channel.
            stopper = threading.Thread(target=self.fs.destroy, args=("/",), daemon=True)
            stopper.start()
            stopper.join(timeout=10)
            self.assertFalse(stopper.is_alive(), "destroy did not finish")
        # The stop ends the wait (its reason is the session, not a yield).
        self.assertTrue(session.background_finished.wait(timeout=3))
        self.assertEqual(reasons[-1:], ["streaming session"])
        self.assertEqual(session.io_fd, -1)
        self.assertFalse(session.finalized)
        self.assertFalse((self.cache_dir / "unmounting.bin").exists())

    def test_admission_during_registration_charges_the_replaced_partial_too(self):
        cache = self.fs.cache_manager
        cache.max_size_bytes = 1024 * 1024
        payload = os.urandom(384 * 1024)
        (self.backend_dir / "replacing.bin").write_bytes(payload)
        # A detached session's partial left at the path: the rename
        # replaces it, and it is charged until it does.
        orphan = self.cache_dir / "replacing.bin.storage.partial"
        orphan.write_bytes(b"o" * (128 * 1024))
        cache.mark_cache_size_dirty()
        real_replace = os.replace
        needed = []

        def replace(src, dst):
            if ".storage-staging-" in str(src):
                with cache._lock:
                    cache._size_dirty = True
                    # 1024 KiB less 128 KiB (old partial) and 384 KiB (new).
                    needed.append(cache.get_free_space_needed(600 * 1024))
            return real_replace(src, dst)

        with patch("storage.streaming.os.replace", side_effect=replace):
            fd = self.fs.open("/replacing.bin", os.O_RDONLY)
        self._session_for(fd)
        self.assertEqual(needed, [88 * 1024])
        # After the replacement only the new partial is charged.
        self.assertEqual(cache.get_free_space_needed(600 * 1024), 0)
        self.fs.release("/replacing.bin", fd)

    def test_destroy_landing_after_a_confirmation_yield_cleans_up(self):
        manager = self.fs._streaming_manager
        pool = manager._download_pool
        payload = os.urandom(64 * 1024)
        (self.backend_dir / "yielding.bin").write_bytes(payload)
        busy = threading.Event()
        self.addCleanup(busy.clear)
        stat, _ = self._channel_busy_stat(busy)
        real_finish = manager._finish_background_session
        yielded = threading.Event()
        resume = threading.Event()
        self.addCleanup(resume.set)

        def finish(session):
            try:
                return real_finish(session)
            except _PublicationYielded:
                # Between the contention yield and the turn's requeue.
                yielded.set()
                resume.wait(timeout=10)
                raise

        def contended(_session):
            return lambda _token, _waited: "streaming publication yielded to other work"

        with patch.object(self.backend, "stat", side_effect=stat), \
                patch.object(manager, "_publication_wait_probe", side_effect=contended), \
                patch.object(manager, "_finish_background_session", side_effect=finish):
            fd = self.fs.open("/yielding.bin", os.O_RDONLY)
            session = self._session_for(fd)
            busy.set()
            self.assertEqual(self.fs.read("/yielding.bin", len(payload), 0, fd), payload)
            self.fs.release("/yielding.bin", fd)
            self.assertTrue(yielded.wait(timeout=3))
            # destroy's complete_too pass, with the pool already closing.
            with pool.condition:
                pool.closing = True
            with manager.prefetch_sessions_lock:
                manager._detach_for_destroy(session, complete_too=True)
            resume.set()
            self.assertTrue(session.background_finished.wait(timeout=3))
        self.assertEqual(session.io_fd, -1)
        self.assertFalse(session.finalized)
        self.assertFalse((self.cache_dir / "yielding.bin").exists())
        self.assertEqual(list(self.cache_dir.rglob("*.storage.partial")), [])
        self.assertEqual(manager.list_download_sessions(), [])

    def test_same_member_confirmations_do_not_trade_the_worker(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_download_workers=1)
        busy = threading.Event()
        self.addCleanup(busy.clear)
        stat, reasons = self._channel_busy_stat(busy)
        sessions = []
        with patch.object(self.backend, "stat", side_effect=stat), \
                patch.object(self.backend, "interactive_wait_budget", return_value=1.0):
            busy.set()
            for name in ("first-held.bin", "second-held.bin"):
                payload = os.urandom(64 * 1024)
                (self.backend_dir / name).write_bytes(payload)
                fd = self.fs.open("/" + name, os.O_RDONLY)
                sessions.append(self._session_for(fd))
                self.assertEqual(self.fs.read("/" + name, len(payload), 0, fd), payload)
                self.fs.release("/" + name, fd)
            for session in sessions:
                self.assertTrue(session.background_finished.wait(timeout=8))
        # Each waited out its own budget once; neither yielded to the other.
        self.assertEqual(reasons, ["streaming publication's channel wait budget"] * 2)
        self.assertEqual(self.fs._streaming_stats_snapshot()["streaming_unconfirmed_discards"], 2)
        for name in ("first-held.bin", "second-held.bin"):
            self.assertFalse((self.cache_dir / name).exists())

    @staticmethod
    def _probe_session(budget):
        backend = SimpleNamespace(name="backend", interactive_wait_budget=lambda: budget)
        return SimpleNamespace(stop_requested=False, backend=backend,
                               confirmation_waited=0.0, confirmation_timed_out=False)

    def _with_contention(self, members):
        pool = self.fs._streaming_manager._download_pool
        with pool.condition:
            saved, pool.contended = pool.contended, frozenset(members)

        def restore():
            with pool.condition:
                pool.contended = saved

        self.addCleanup(restore)
        return pool

    def test_confirmation_wait_budget_is_not_reset_by_a_yield(self):
        manager = self.fs._streaming_manager
        pool = self._with_contention({"other"})
        session = self._probe_session(1.0)
        probe = manager._publication_wait_probe(session)
        acquisition = object()
        self.assertIsNone(probe(acquisition, 0.0))
        self.assertIsNone(probe(acquisition, 0.4))
        self.assertEqual(probe(acquisition, 0.6), "streaming publication yielded to other work")
        self.assertAlmostEqual(session.confirmation_waited, 0.6)
        # The requeued attempt (a new turn's probe) has 0.4 s left.
        probe = manager._publication_wait_probe(session)
        with pool.condition:
            pool.contended = frozenset()
        acquisition = object()
        self.assertIsNone(probe(acquisition, 0.0))
        self.assertIsNone(probe(acquisition, 0.3))
        self.assertEqual(probe(acquisition, 0.4), "streaming publication's channel wait budget")
        self.assertTrue(session.confirmation_timed_out)

    def test_late_polls_still_reach_the_yield_and_the_budget(self):
        manager = self.fs._streaming_manager
        self._with_contention({"other"})
        # Polls 0.3 s apart in one acquisition (a loaded host).
        probe = manager._publication_wait_probe(self._probe_session(10.0))
        acquisition = object()
        self.assertIsNone(probe(acquisition, 0.0))
        self.assertIsNone(probe(acquisition, 0.3))
        self.assertEqual(probe(acquisition, 0.6), "streaming publication yielded to other work")
        pool = self._with_contention(set())
        session = self._probe_session(1.0)
        probe = manager._publication_wait_probe(session)
        answers = [probe(acquisition, waited) for waited in (0.0, 0.3, 0.6, 0.9, 1.2)]
        self.assertEqual(answers, [None] * 4 + ["streaming publication's channel wait budget"])

    def test_a_granted_wait_is_charged_before_the_retry(self):
        manager = self.fs._streaming_manager
        self._with_contention(set())
        session = self._probe_session(1.0)
        probe = manager._publication_wait_probe(session)
        first = object()
        self.assertIsNone(probe(first, 0.0))
        self.assertIsNone(probe(first, 0.3))
        # The channel came free at 0.7 s, after the last poll; the grant
        # reports the whole wait, and cannot be refused.
        self.assertIsNone(probe(first, 0.7, granted=True))
        # The stat failed; the retry has the 0.3 s that is left.
        retry = object()
        self.assertIsNone(probe(retry, 0.0))
        self.assertIsNone(probe(retry, 0.2))
        self.assertEqual(probe(retry, 0.3), "streaming publication's channel wait budget")

    def test_confirmation_wait_budget_counts_only_time_spent_waiting(self):
        manager = self.fs._streaming_manager
        self._with_contention(set())
        session = self._probe_session(1.0)
        clock = [0.0]
        with patch("storage.streaming.time.monotonic", side_effect=lambda: clock[0]):
            probe = manager._publication_wait_probe(session)
            # A short wait, a channel, then the stat runs and fails (5 s
            # of wall time); the retry is a new acquisition.
            first = object()
            self.assertIsNone(probe(first, 0.0))
            self.assertIsNone(probe(first, 0.1))
            clock[0] = 5.0
            retry = object()
            self.assertIsNone(probe(retry, 0.0), "the stat's own time was charged as waiting")
            self.assertIsNone(probe(retry, 0.8))
            self.assertFalse(session.confirmation_timed_out)
            # 0.1 s + 0.9 s of waiting reach the budget.
            self.assertEqual(probe(retry, 0.9), "streaming publication's channel wait budget")

    def test_failed_rename_leaves_the_planted_partial_charged(self):
        cache = self.fs.cache_manager
        cache.max_size_bytes = 1024 * 1024
        payload = os.urandom(384 * 1024)
        (self.backend_dir / "planted.bin").write_bytes(payload)
        planted = self.cache_dir / "planted.bin.storage.partial"
        planted.write_bytes(b"p" * (128 * 1024))
        cache.mark_cache_size_dirty()
        real_replace = os.replace
        needed = []

        def replace(src, dst):
            if ".storage-staging-" in str(src):
                with cache._lock:
                    cache._size_dirty = True
                    # The planted file and one reservation for the new one.
                    needed.append(cache.get_free_space_needed(600 * 1024))
                raise OSError(errno.EIO, "simulated rename failure")
            return real_replace(src, dst)

        with patch("storage.streaming.os.replace", side_effect=replace):
            fd = self.fs.open("/planted.bin", os.O_RDONLY)
        self.assertEqual(needed, [88 * 1024])
        with self.fs._fd_lock:
            self.assertIsNone(self.fs._fd_map[fd].prefetch_session)
        self.fs.release("/planted.bin", fd)
        self.assertTrue(planted.exists())
        self.assertEqual(cache._reserved_bytes, 0)
        on_disk = sum(path.stat().st_size for path in self.cache_dir.rglob("*")
                      if path.is_file() and not path.is_symlink())
        self.assertEqual(cache.get_total_cache_size(), on_disk)
        self.assertGreaterEqual(on_disk, 128 * 1024)

    def test_offer_during_turn_failure_recovery_starts_no_second_turn(self):
        manager = self.fs._streaming_manager
        pool = manager._download_pool
        self.backend.worker_gate = threading.Event()
        raise_now = threading.Event()
        real_probe = manager._bulk_wait_probe
        real_recover = pool.on_turn_failure
        seen = []

        def probe(session):
            if raise_now.is_set():
                raise RuntimeError("unexpected turn failure")
            return real_probe(session)

        def recover(session):
            # A reader's read offers the session between the exception and
            # the recovery.
            with session.lock:
                manager._schedule_background(session)
            with pool.condition:
                seen.append((session in pool.running, session in pool.ready))
            return real_recover(session)

        with patch.object(manager, "_bulk_wait_probe", side_effect=probe), \
                patch.object(pool, "on_turn_failure", recover):
            retained = self._open_retained_fill("recovering.bin", os.urandom(384 * 1024))
            raise_now.set()
            self.backend.worker_gate.set()
            self.assertTrue(retained.background_finished.wait(timeout=5))
        self.assertEqual(seen, [(True, False)])
        self.assertIsNotNone(retained.error)
        self.assertEqual(manager.list_download_sessions(), [])
        self.assertTrue(self._wait_for(lambda: pool.snapshot()[1:] == (0, 0)))

    def test_bulk_wait_yield_interval_restarts_for_each_acquisition(self):
        manager = self.fs._streaming_manager
        session = SimpleNamespace(stop_requested=False, prefetch_complete=False, error=None,
                                  backend=SimpleNamespace(name="backend"))
        pool = manager._download_pool
        with pool.condition:
            saved, pool.contended = pool.contended, frozenset({"other"})
        try:
            with patch("storage.streaming.DOWNLOAD_TURN_YIELD_SECONDS", 0.05):
                probe, restart = manager._bulk_wait_probe(session)
                self.assertIsNone(probe())
                time.sleep(0.1)
                self.assertIsNotNone(probe())
                # A retry after a lost connection waits its own interval.
                restart()
                self.assertIsNone(probe())
                session.prefetch_complete = True
                self.assertIsNotNone(probe())
        finally:
            with pool.condition:
                pool.contended = saved

    def test_unexpected_download_finalize_error_releases_worker_for_next_file(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_download_workers=1)
        real_finalize = manager._finalize_streaming_session

        def finalize(session):
            if session.canonical_path == "/error.bin":
                raise RuntimeError("unexpected finalize failure")
            return real_finalize(session)

        with patch.object(manager, "_finalize_streaming_session", side_effect=finalize):
            for name in ("error.bin", "survivor.bin"):
                path = "/" + name
                payload = os.urandom(8192)
                (self.backend_dir / name).write_bytes(payload)
                fd = self.fs.open(path, os.O_RDONLY)
                session = self._session_for(fd)
                self.assertEqual(self.fs.read(path, len(payload), 0, fd), payload)
                self.fs.release(path, fd)
                self.assertTrue(session.background_finished.wait(timeout=3))
                self.assertEqual(session.io_fd, -1)
        self.assertEqual((self.cache_dir / "survivor.bin").read_bytes(), payload)

    def test_streaming_open_and_read_without_waiting_full_copy(self):
        payload = os.urandom(256 * 1024)
        remote_file = self.backend_dir / "large.bin"
        remote_file.write_bytes(payload)

        vfd = self.fs.open('/large.bin', os.O_RDONLY)
        with self.fs._fd_lock:
            entry = self.fs._fd_map[vfd]
        self.assertIsNotNone(entry.prefetch_session)

        cache_path = self.cache_dir / "large.bin"
        partial_path = self.cache_dir / "large.bin.storage.partial"
        self.assertTrue(partial_path.exists())
        self.assertFalse(cache_path.exists())

        first = self.fs.read('/large.bin', 4096, 0, vfd)
        self.assertEqual(first, payload[:4096])
        # Reading on past the keep-after-close threshold (128 KiB here) lifts
        # the read-ahead bound, so the worker fetches the rest while open.
        rest = self.fs.read('/large.bin', 128 * 1024, 4096, vfd)
        self.assertEqual(rest, payload[4096:4096 + 128 * 1024])

        self.assertTrue(self._wait_for(lambda: cache_path.exists() and not partial_path.exists(), timeout=8.0))
        self.assertEqual(cache_path.read_bytes(), payload)

        self.fs.release('/large.bin', vfd)

    def _stream_whole(self, name, size):
        vfd = self.fs.open("/" + name, os.O_RDONLY)
        try:
            data = self.fs.read("/" + name, size, 0, vfd)
        finally:
            self.fs.release("/" + name, vfd)
        cache_path = self.cache_dir / name
        self.assertTrue(self._wait_for(cache_path.exists, timeout=8.0))
        return data, cache_path

    def test_streaming_fill_without_member_mode_is_read_only(self):
        # Size and mtime known, mode missing: the staging file's own mode
        # (owner-writable) must not become the reported, enforced one.
        import stat as stat_module
        from types import SimpleNamespace
        payload = os.urandom(64 * 1024)
        (self.backend_dir / "nomode.bin").write_bytes(payload)
        os.chmod(self.backend_dir / "nomode.bin", 0o444)
        fake = SimpleNamespace(st_size=len(payload), st_mtime=1_600_000_000.0, st_mode=None)
        with patch.object(self.backend, "stat", return_value=fake):
            data, cache_path = self._stream_whole("nomode.bin", len(payload))
        self.assertEqual(data, payload)
        attrs = self.fs.getattr("/nomode.bin")
        self.assertEqual(attrs["st_mode"] & 0o222, 0)
        self.assertEqual(attrs["st_mtime"], 1_600_000_000.0)
        with self.assertRaises(FuseOSError) as raised:
            self.fs.open("/nomode.bin", os.O_WRONLY)
        self.assertEqual(raised.exception.errno, errno.EACCES)
        # An explicit chmod through the mount grants write.
        self.fs.chmod("/nomode.bin", 0o644)
        self.fs.release("/nomode.bin", self.fs.open("/nomode.bin", os.O_WRONLY))
        self.assertTrue(stat_module.S_ISREG(attrs["st_mode"]))

    def _stream_and_finish(self, name, size):
        """Read name whole through a stream and wait for its session to end."""
        vfd = self.fs.open("/" + name, os.O_RDONLY)
        with self.fs._fd_lock:
            session = self.fs._fd_map[vfd].prefetch_session
        self.assertIsNotNone(session)
        try:
            data = self.fs.read("/" + name, size, 0, vfd)
            self.assertTrue(self._wait_for(
                lambda: session.finalized or session.error is not None, timeout=8.0))
        finally:
            self.fs.release("/" + name, vfd)
        return data, session

    def test_streaming_fill_of_a_member_changed_since_its_probe_is_discarded(self):
        old, new = b"o" * (64 * 1024), b"n" * (64 * 1024)
        member = self.backend_dir / "moved.bin"
        member.write_bytes(old)
        os.utime(member, (1_600_000_000, 1_600_000_000))
        real_stat, calls = self.backend.stat, []

        def stat(path):
            calls.append(path)
            if len(calls) == 2:  # the confirmation at finalize: replaced meanwhile
                member.write_bytes(new)
                os.utime(member, (1_650_000_000, 1_650_000_000))
            return real_stat(path)

        with patch.object(self.backend, "stat", side_effect=stat):
            data, session = self._stream_and_finish("moved.bin", len(old))
            # Counted once the partial is gone and the session finished.
            self.assertTrue(self._wait_for(lambda: self.fs._streaming_manager._streaming_stats.get(
                "streaming_member_changed_discards") == 1, timeout=8.0))
        self.assertEqual(data, old)  # the reader kept its bytes
        cache_path = self.cache_dir / "moved.bin"
        self.assertFalse(cache_path.exists())  # nothing pairs 1_650_000_000 with old bytes
        self.assertFalse((self.cache_dir / "moved.bin.storage.partial").exists())
        self.assertEqual(self.fs._streaming_manager._streaming_stats.get(
            "streaming_member_changed_discards"), 1)
        # A reopen fetches the replacement; stat and fstat report its mtime.
        self.assertEqual(self._stream_whole("moved.bin", len(new))[0], new)
        self.assertEqual(self.fs.getattr("/moved.bin")["st_mtime"], 1_650_000_000)
        with open(cache_path, "rb") as f:
            self.assertEqual((f.read(), os.fstat(f.fileno()).st_mtime), (new, 1_650_000_000))

    def test_interrupted_confirmation_fails_the_session_and_wakes_readers(self):
        from storage.backends.ssh_pool import BulkCapacityTimeout
        payload = os.urandom(64 * 1024)
        (self.backend_dir / "cut.bin").write_bytes(payload)
        real_stat, calls = self.backend.stat, []

        def stat(path):
            calls.append(path)
            if len(calls) >= 2:
                raise BulkCapacityTimeout("no capacity")
            return real_stat(path)

        with patch.object(self.backend, "stat", side_effect=stat):
            data, session = self._stream_and_finish("cut.bin", len(payload))
        self.assertEqual(data, payload)
        self.assertIsInstance(session.error, OSError)
        self.assertEqual(session.error.errno, errno.EAGAIN)
        # Not installed as a confirmed fill: the failed session's usual
        # cleanup applies (at most an unindexed prefix, refetched on open).
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/cut.bin"))

    def test_streaming_open_uses_cached_file_without_prefetch_session(self):
        payload = os.urandom(128 * 1024)
        (self.backend_dir / "warm.bin").write_bytes(payload)

        cache_path = self.cache_dir / "warm.bin"
        cache_path.write_bytes(payload)
        partial_path = self.cache_dir / "warm.bin.storage.partial"
        self.assertFalse(partial_path.exists())

        stats_before = self.fs._streaming_stats_snapshot()

        vfd_first = self.fs.open('/warm.bin', os.O_RDONLY)
        with self.fs._fd_lock:
            first_entry = self.fs._fd_map[vfd_first]
        self.assertIsNone(first_entry.prefetch_session)
        first = self.fs.read('/warm.bin', 4096, 0, vfd_first)
        self.assertEqual(first, payload[:4096])
        self.fs.release('/warm.bin', vfd_first)

        vfd_second = self.fs.open('/warm.bin', os.O_RDONLY)
        with self.fs._fd_lock:
            second_entry = self.fs._fd_map[vfd_second]
        self.assertIsNone(second_entry.prefetch_session)
        second = self.fs.read('/warm.bin', 4096, 8192, vfd_second)
        self.assertEqual(second, payload[8192:12288])
        self.fs.release('/warm.bin', vfd_second)

        with self.fs._prefetch_sessions_lock:
            self.assertNotIn('/warm.bin', self.fs._prefetch_sessions)
        self.assertFalse(partial_path.exists())

        stats_after = self.fs._streaming_stats_snapshot()
        self.assertEqual(
            stats_after["streaming_sessions_started"],
            stats_before["streaming_sessions_started"],
        )
        self.assertEqual(
            stats_after["streaming_fallback_opens"],
            stats_before["streaming_fallback_opens"],
        )
        self.assertEqual(
            stats_after["foreground_fetches"],
            stats_before["foreground_fetches"],
        )
        self.assertEqual(
            stats_after["background_fetches"],
            stats_before["background_fetches"],
        )

    def test_streaming_seek_fetches_missed_range(self):
        payload = os.urandom(256 * 1024)
        (self.backend_dir / "seek.bin").write_bytes(payload)

        vfd = self.fs.open('/seek.bin', os.O_RDONLY)
        prefix = self.fs.read('/seek.bin', 1024, 0, vfd)
        self.assertEqual(prefix, payload[:1024])

        offset = 180 * 1024
        jumped = self.fs.read('/seek.bin', 1024, offset, vfd)
        self.assertEqual(jumped, payload[offset:offset + 1024])

        self.fs.release('/seek.bin', vfd)

    def test_streaming_prefetch_follows_first_read_forward(self):
        self.backend.range_delay = 0.03
        payload = os.urandom(2 * 1024 * 1024)
        (self.backend_dir / "follow.bin").write_bytes(payload)

        vfd = self.fs.open('/follow.bin', os.O_RDONLY)
        offset = 1024 * 1024
        sample = self.fs.read('/follow.bin', 4096, offset, vfd)
        self.assertEqual(sample, payload[offset:offset + 4096])

        chunk = self.fs._streaming_chunk_size
        with self.fs._prefetch_sessions_lock:
            session = self.fs._prefetch_sessions.get('/follow.bin')
        self.assertIsNotNone(session)

        time.sleep(0.15)
        with session.lock:
            ranges = list(session.available_ranges)

        self.assertTrue(self.fs._is_range_available(ranges, offset, offset + 4096))
        self.assertFalse(self.fs._is_range_available(ranges, 0, chunk))
        self.fs.release('/follow.bin', vfd)

    def test_streaming_state_xattr_tracks_progress(self):
        payload = os.urandom(256 * 1024)
        (self.backend_dir / "xattr.bin").write_bytes(payload)

        vfd = self.fs.open('/xattr.bin', os.O_RDONLY)
        first = self.fs.read('/xattr.bin', 4096, 0, vfd)
        self.assertEqual(first, payload[:4096])

        cache_path = self.cache_dir / "xattr.bin"
        partial_path = self.cache_dir / "xattr.bin.storage.partial"
        self.assertTrue(self._wait_for(lambda: partial_path.exists() or cache_path.exists(), timeout=3.0))
        probe_path = partial_path if partial_path.exists() else cache_path

        try:
            raw = os.getxattr(str(probe_path), self.fs._STREAM_STATE_XATTR)
        except OSError as exc:
            if exc.errno in (getattr(errno, "EOPNOTSUPP", -1), getattr(errno, "ENOTSUP", -1), getattr(errno, "ENODATA", -1)):
                self.skipTest("xattrs unsupported in this environment")
            raise

        state = json.loads(raw.decode())
        self.assertEqual(state["file_size"], len(payload))
        self.assertGreaterEqual(state["downloaded_bytes"], 4096)

        # Read past the keep-after-close threshold so the close lets the
        # session run to "complete".
        self.fs.read('/xattr.bin', 128 * 1024, 0, vfd)
        self.fs.release('/xattr.bin', vfd)

        # The partial is renamed into place before the session is marked
        # finalized and the "complete" state is written, so wait for the
        # terminal state itself, not for the final path to exist.
        def final_state():
            if not cache_path.exists():
                return None
            try:
                raw = os.getxattr(str(cache_path), self.fs._STREAM_STATE_XATTR)
            except OSError as exc:
                if exc.errno in (getattr(errno, "EOPNOTSUPP", -1), getattr(errno, "ENOTSUP", -1)):
                    self.skipTest("xattrs unsupported in this environment")
                return None  # ENODATA: renamed, state not yet written
            return json.loads(raw.decode())

        self.assertTrue(self._wait_for(
            lambda: (final_state() or {}).get("status") == "complete", timeout=8.0,
        ))
        state = final_state()
        self.assertEqual(state["status"], "complete")
        self.assertTrue(state["finalized"])

    def test_streaming_tolerates_backend_short_reads(self):
        self.backend.max_read_bytes = 1024
        payload = os.urandom(192 * 1024)
        (self.backend_dir / "short.bin").write_bytes(payload)

        vfd = self.fs.open('/short.bin', os.O_RDONLY)
        head = self.fs.read('/short.bin', 8192, 0, vfd)
        self.assertEqual(head, payload[:8192])

        middle_offset = 64 * 1024
        middle = self.fs.read('/short.bin', 4096, middle_offset, vfd)
        self.assertEqual(middle, payload[middle_offset:middle_offset + 4096])

        self.fs.release('/short.bin', vfd)


    def test_streaming_read_surfaces_backend_range_failures(self):
        self.backend.fail_after_offset = 64 * 1024
        payload = os.urandom(128 * 1024)
        (self.backend_dir / "fault.bin").write_bytes(payload)

        vfd = self.fs.open('/fault.bin', os.O_RDONLY)
        ok = self.fs.read('/fault.bin', 4096, 0, vfd)
        self.assertEqual(ok, payload[:4096])

        with self.assertRaises(FuseOSError) as ctx:
            self.fs.read('/fault.bin', 4096, 96 * 1024, vfd)
        self.assertEqual(ctx.exception.errno, errno.EIO)

        self.fs.release('/fault.bin', vfd)

    def test_streaming_failure_reuses_cached_prefix_offline(self):
        self.backend.fail_after_offset = 64 * 1024
        payload = os.urandom(192 * 1024)
        (self.backend_dir / "offline-prefix.bin").write_bytes(payload)

        vfd = self.fs.open('/offline-prefix.bin', os.O_RDONLY)
        head = self.fs.read('/offline-prefix.bin', 4096, 0, vfd)
        self.assertEqual(head, payload[:4096])

        with self.assertRaises(FuseOSError):
            self.fs.read('/offline-prefix.bin', 4096, 96 * 1024, vfd)

        self.fs.release('/offline-prefix.bin', vfd)

        cache_path = self.cache_dir / "offline-prefix.bin"
        partial_path = self.cache_dir / "offline-prefix.bin.storage.partial"
        self.assertTrue(self._wait_for(lambda: cache_path.exists(), timeout=5.0))
        self.assertFalse(partial_path.exists())

        cached_size = cache_path.stat().st_size
        self.assertGreater(cached_size, 0)
        self.assertLess(cached_size, len(payload))

        self.backend.set_available(False)

        retry_vfd = self.fs.open('/offline-prefix.bin', os.O_RDONLY)
        retry_head = self.fs.read('/offline-prefix.bin', 4096, 0, retry_vfd)
        retry_tail = self.fs.read('/offline-prefix.bin', 4096, max(0, cached_size - 4096), retry_vfd)
        self.fs.release('/offline-prefix.bin', retry_vfd)

        self.assertEqual(retry_head, payload[:4096])
        self.assertEqual(retry_tail, payload[max(0, cached_size - 4096):cached_size])

    def _sync_now(self, path):
        from storage.sync_executor import SyncExecutor
        from storage.sync_tasks import SyncTask
        executor = SyncExecutor(
            worker_id=0, state_manager=self.fs.state_manager, cache_manager=self.fs.cache_manager,
            replication_manager=self.fs.replication_manager, config=self.fs.sync_manager.config,
        )
        executor.process(SyncTask(path))

    def _read_whole(self, path, size):
        vfd = self.fs.open(path, os.O_RDONLY)
        try:
            data = b""
            while len(data) < size:
                chunk = self.fs.read(path, 32 * 1024, len(data), vfd)
                if not chunk:
                    break
                data += chunk
            return data
        finally:
            self.fs.release(path, vfd)

    def _fail_streamed_refetch(self, path, payload):
        """Open and read past fail_after_offset, so the session fails after a prefix."""
        vfd = self.fs.open(path, os.O_RDONLY)
        try:
            self.assertIsNotNone(self._session_for(vfd))
            self.assertEqual(self.fs.read(path, 4096, 0, vfd), payload[:4096])
            with self.assertRaises(FuseOSError):
                self.fs.read(path, 4096, 96 * 1024, vfd)
        finally:
            self.fs.release(path, vfd)
        partial = self.cache_dir / (path.lstrip('/') + ".storage.partial")
        self.assertTrue(self._wait_for(lambda: not partial.exists(), timeout=5.0))

    def test_failed_refetch_of_an_evicted_file_never_reaches_the_member(self):
        # ditz streaming-promoted-prefix-false-conflict, worse case: an
        # evicted file keeps its CLEAN row and location checksums. A failed
        # streamed refetch promoted its prefix, the next open saw a lost
        # write (the row's stat is the whole file's) and marked it DIRTY,
        # and the sync, finding the member unchanged, uploaded the prefix
        # over it.
        self.backend.copy_delay = 0
        payload = os.urandom(192 * 1024)
        fh = self.fs.create('/evicted.bin', 0o644)
        self.fs.write('/evicted.bin', payload, 0, fh)
        self.fs.release('/evicted.bin', fh)
        self._sync_now('/evicted.bin')
        self.assertEqual(payload, (self.backend_dir / "evicted.bin").read_bytes())
        cache_path = self.cache_dir / "evicted.bin"
        self.assertGreater(self.fs.cache_manager._evict_if_still_clean('/evicted.bin', cache_path), 0)

        self.backend.fail_after_offset = 64 * 1024
        self._fail_streamed_refetch('/evicted.bin', payload)
        self.assertFalse(cache_path.exists(), "a prefix was promoted for an indexed file")

        self.backend.fail_after_offset = None
        self.assertEqual(self._read_whole('/evicted.bin', len(payload)), payload)
        self.assertEqual(SyncStatus.CLEAN, self.fs.state_manager.get_file_metadata('/evicted.bin').sync_status)
        self._sync_now('/evicted.bin')
        self.assertEqual(payload, (self.backend_dir / "evicted.bin").read_bytes())

    def test_failed_refetch_after_invalidation_is_no_conflict(self):
        # The issue's case: after _invalidate_file_cache the row is CLEAN with
        # size 0 and no checksum; a promoted prefix then looked like a write,
        # and the sync reported CONFLICT on a file nobody wrote.
        self.backend.copy_delay = 0
        payload = os.urandom(192 * 1024)
        (self.backend_dir / "changed.bin").write_bytes(payload)
        self.assertEqual(self._read_whole('/changed.bin', len(payload)), payload)
        self.assertTrue(self._wait_for(lambda: (self.cache_dir / "changed.bin").exists(), timeout=5.0))
        self.assertIsNotNone(self.fs.state_manager.get_file_metadata('/changed.bin'))

        changed = os.urandom(192 * 1024)
        (self.backend_dir / "changed.bin").write_bytes(changed)
        self.fs._invalidate_file_cache('/changed.bin')
        self.backend.fail_after_offset = 64 * 1024
        self._fail_streamed_refetch('/changed.bin', changed)
        self.assertFalse((self.cache_dir / "changed.bin").exists())

        self.backend.fail_after_offset = None
        self.assertEqual(self._read_whole('/changed.bin', len(changed)), changed)
        self.assertTrue(self._wait_for(lambda: (self.cache_dir / "changed.bin").exists(), timeout=5.0))
        row = self.fs.state_manager.get_file_metadata('/changed.bin')
        self.assertEqual(SyncStatus.CLEAN, row.sync_status)
        self.assertEqual(len(changed), row.size)

    def test_full_fetch_losing_to_a_promoted_prefix_does_not_index_it(self):
        # Council round 1 on #146: a full fetch (a write-open) copies the
        # whole file while a streaming session for the same never-cached
        # path fails and promotes its prefix; the fetch's link then finds
        # the prefix at the path and used to index it as the whole file
        # (CLEAN, prefix size), so it was served truncated and, once
        # written, synced over the member.
        self.backend.copy_delay = 0
        payload = os.urandom(192 * 1024)
        (self.backend_dir / "raced.bin").write_bytes(payload)

        real_copy = self.backend.copy_to_local
        in_copy = threading.Event()
        go = threading.Event()
        self.addCleanup(go.set)
        copies = []

        def held_copy(virtual_path, local_path):
            copies.append(virtual_path)
            if len(copies) == 1:
                in_copy.set()
                self.assertTrue(go.wait(timeout=10.0))
            return real_copy(virtual_path, local_path)

        # The read-ahead worker waits at its first read until the failure is
        # set up, so it can neither finish the file nor pass 64 KiB first.
        self.backend.worker_gate = threading.Event()
        rvfd = self.fs.open('/raced.bin', os.O_RDONLY)
        self.assertIsNotNone(self._session_for(rvfd))
        self.assertEqual(self.fs.read('/raced.bin', 4096, 0, rvfd), payload[:4096])

        outcome = {}

        def write_open():
            try:
                outcome["vfd"] = self.fs.open('/raced.bin', os.O_RDWR)
            except BaseException as exc:  # noqa: BLE001 - reported below
                outcome["error"] = exc

        with patch.object(self.backend, "copy_to_local", side_effect=held_copy):
            writer = threading.Thread(target=write_open, daemon=True)
            writer.start()
            self.assertTrue(in_copy.wait(timeout=5.0))
            self.backend.fail_after_offset = 64 * 1024
            with self.assertRaises(FuseOSError):
                self.fs.read('/raced.bin', 4096, 96 * 1024, rvfd)
            self.fs.release('/raced.bin', rvfd)
            self.backend.worker_gate.set()
            cache_path = self.cache_dir / "raced.bin"
            self.assertTrue(self._wait_for(lambda: cache_path.exists(), timeout=5.0))
            self.assertLess(cache_path.stat().st_size, len(payload))  # the prefix won
            go.set()
            writer.join(timeout=5.0)
        self.assertFalse(writer.is_alive())
        self.assertNotIn("error", outcome)
        self.fs.release('/raced.bin', outcome["vfd"])

        self.assertEqual(payload, cache_path.read_bytes())
        row = self.fs.state_manager.get_file_metadata('/raced.bin')
        self.assertEqual(len(payload), row.size)
        self.assertEqual(SyncStatus.CLEAN, row.sync_status)

    def test_a_member_file_carrying_the_prefix_marker_is_fetched_whole_and_indexed(self):
        # Council round 2 on #146: a member file may carry the marker (an
        # older local member copy kept xattrs). A whole-file fetch clears it,
        # or the file would be served unindexed for ever.
        from storage.cache_metadata import PREFIX_XATTR
        member_file = self.backend_dir / "marked.bin"
        member_file.write_bytes(b"whole file")
        os.setxattr(str(member_file), PREFIX_XATTR, b"1")
        # Read-only, as the local member's copy keeps it: removing a user
        # xattr needs write permission (council round 3, Codex).
        os.chmod(member_file, 0o444)
        # The whole-file fetch path (a write-open's; a read-only open streams).
        self.assertTrue(self.fs._ensure_file_in_cache(self.fs._resolve_path('/marked.bin')))
        cache_path = self.cache_dir / "marked.bin"
        self.assertNotIn(PREFIX_XATTR, os.listxattr(str(cache_path)))
        self.assertIsNotNone(self.fs.state_manager.get_file_metadata('/marked.bin'))

    def test_an_owner_unreadable_marked_copy_is_cleared(self):
        # Council round 5 on #146: getxattr needs read permission, and the
        # copy keeps the member file's mode (write-only here).
        from storage.cache import _remove_prefix_marker
        from storage.cache_metadata import PREFIX_XATTR
        for mode, marked in ((0o200, True), (0o200, False), (0o000, True), (0o000, False)):
            path = self.cache_dir / f"wo-{mode:o}-{marked}"
            path.write_bytes(b"x")
            if marked:
                os.setxattr(str(path), PREFIX_XATTR, b"1")
            os.chmod(path, mode)
            _remove_prefix_marker(path)
            self.assertEqual(mode, os.stat(path).st_mode & 0o777)
            os.chmod(path, 0o600)
            self.assertNotIn(PREFIX_XATTR, os.listxattr(str(path)))

    def test_a_read_only_member_file_without_the_marker_is_fetched(self):
        # Council round 4 on #146: on a read-only copy the kernel answers
        # EACCES before ENODATA, so removing an absent marker failed the
        # fetch of every read-only file.
        member_file = self.backend_dir / "plain-ro.bin"
        member_file.write_bytes(b"whole file")
        os.chmod(member_file, 0o444)
        self.assertTrue(self.fs._ensure_file_in_cache(self.fs._resolve_path('/plain-ro.bin')))
        cache_path = self.cache_dir / "plain-ro.bin"
        self.assertEqual(b"whole file", cache_path.read_bytes())
        self.assertEqual(0o444, self.fs.getattr("/plain-ro.bin")["st_mode"] & 0o777)
        self.assertEqual(0o644, os.stat(cache_path).st_mode & 0o777)
        self.assertIsNotNone(self.fs.state_manager.get_file_metadata('/plain-ro.bin'))

    def test_unindexed_prefix_is_dropped_only_when_unopened_and_unindexed(self):
        from storage.cache_metadata import PREFIX_XATTR
        cache_path = self.cache_dir / "p.bin"
        cache_path.write_bytes(b"prefix")
        os.setxattr(str(cache_path), PREFIX_XATTR, b"1")
        vfd = self.fs.open('/p.bin', os.O_RDONLY)
        self.assertFalse(self.fs._drop_unindexed_prefix('/p.bin', cache_path))
        self.assertTrue(cache_path.exists())
        self.fs.release('/p.bin', vfd)
        self.assertTrue(self.fs._drop_unindexed_prefix('/p.bin', cache_path))
        self.assertFalse(cache_path.exists())

        # An indexed file is never a droppable prefix, marker or not.
        fh = self.fs.create('/q.bin', 0o644)
        self.fs.release('/q.bin', fh)
        q_path = self.cache_dir / "q.bin"
        os.setxattr(str(q_path), PREFIX_XATTR, b"1")
        self.assertFalse(self.fs._drop_unindexed_prefix('/q.bin', q_path))
        self.assertTrue(q_path.exists())

    def test_streaming_session_is_shared_across_readers(self):
        payload = os.urandom(512 * 1024)
        (self.backend_dir / "shared.bin").write_bytes(payload)

        vfd_a = self.fs.open('/shared.bin', os.O_RDONLY)
        vfd_b = self.fs.open('/shared.bin', os.O_RDONLY)

        with self.fs._fd_lock:
            entry_a = self.fs._fd_map[vfd_a]
            entry_b = self.fs._fd_map[vfd_b]

        self.assertIsNotNone(entry_a.prefetch_session)
        self.assertIs(entry_a.prefetch_session, entry_b.prefetch_session)

        read_a = self.fs.read('/shared.bin', 2048, 0, vfd_a)
        read_b = self.fs.read('/shared.bin', 2048, 2048, vfd_b)
        self.assertEqual(read_a, payload[:2048])
        self.assertEqual(read_b, payload[2048:4096])

        self.fs.release('/shared.bin', vfd_a)
        self.fs.release('/shared.bin', vfd_b)



    def test_streaming_prefetch_continues_after_last_reader_release(self):
        # streaming_keep_after_close = "0" restores the behaviour before the
        # threshold: any read keeps read-ahead going after the last close.
        self._remake_fs("0")
        self.backend.range_delay = 0.03
        payload = os.urandom(2 * 1024 * 1024)
        (self.backend_dir / "orphan.bin").write_bytes(payload)

        vfd = self.fs.open('/orphan.bin', os.O_RDONLY)
        _ = self.fs.read('/orphan.bin', 4096, 0, vfd)
        self.fs.release('/orphan.bin', vfd)

        self.assertTrue(
            self._wait_for(
                lambda: '/orphan.bin' not in self.fs._prefetch_sessions,
                timeout=8.0,
            )
        )

        cache_path = self.cache_dir / "orphan.bin"
        partial_path = self.cache_dir / "orphan.bin.storage.partial"
        self.assertTrue(cache_path.exists())
        self.assertFalse(partial_path.exists())
        self.assertEqual(cache_path.read_bytes(), payload)

    def test_streaming_keep_after_close_zero_still_cancels_without_a_read(self):
        self._remake_fs("0")
        (self.backend_dir / "unread.bin").write_bytes(os.urandom(256 * 1024))

        vfd = self.fs.open('/unread.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        self.fs.release('/unread.bin', vfd)

        self.assertTrue(session.detached)
        self._assert_not_cached("unread.bin")

    def test_streaming_keep_after_close_bytes_and_percent(self):
        # "64K,90%" on a 2 MiB file: 64 KiB is reached first.
        self._remake_fs("64K,90%")
        self.backend.range_delay = 0.03
        payload = os.urandom(2 * 1024 * 1024)
        (self.backend_dir / "custom.bin").write_bytes(payload)

        vfd = self.fs.open('/custom.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        self.fs.read('/custom.bin', 60 * 1024, 0, vfd)
        with session.lock:
            self.assertFalse(session.readers_consumed)
        self.fs.read('/custom.bin', 4 * 1024, 60 * 1024, vfd)
        with session.lock:
            self.assertTrue(session.readers_consumed)
        self.fs.release('/custom.bin', vfd)

        cache_path = self.cache_dir / "custom.bin"
        self.assertTrue(self._wait_for(cache_path.exists, timeout=8.0))
        self.assertEqual(cache_path.read_bytes(), payload)

    def test_streaming_keep_after_close_full_percent_cancels_partial_reader(self):
        # "100%": only a reader of the whole file qualifies, and by then every
        # byte has arrived, so a close with anything missing cancels.
        self._remake_fs("100%")
        self.backend.range_delay = 0.03
        # 4 MiB so the worker, running alongside the 768 KiB read, cannot
        # finish the file before the close.
        payload = os.urandom(4 * 1024 * 1024)
        (self.backend_dir / "most.bin").write_bytes(payload)

        vfd = self.fs.open('/most.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        self.fs.read('/most.bin', 768 * 1024, 0, vfd)
        self.fs.release('/most.bin', vfd)

        self.assertTrue(session.detached)
        self.assertTrue(self._wait_for(lambda: session.io_fd == -1, timeout=3.0))
        self._assert_not_cached("most.bin")

    def test_streaming_prefetch_continues_after_consuming_reader_release(self):
        # 4 chunks of 32 KiB read sequentially reach the keep-after-close
        # threshold (min(4 chunks, half the file)), so read-ahead outlives
        # the close.
        self.backend.range_delay = 0.03
        payload = os.urandom(2 * 1024 * 1024)
        (self.backend_dir / "orphan.bin").write_bytes(payload)

        vfd = self.fs.open('/orphan.bin', os.O_RDONLY)
        for offset in range(0, 4 * 32 * 1024, 16 * 1024):
            self.assertEqual(
                self.fs.read('/orphan.bin', 16 * 1024, offset, vfd),
                payload[offset:offset + 16 * 1024],
            )
        self.fs.release('/orphan.bin', vfd)

        self.assertTrue(
            self._wait_for(
                lambda: '/orphan.bin' not in self.fs._prefetch_sessions,
                timeout=8.0,
            )
        )

        cache_path = self.cache_dir / "orphan.bin"
        partial_path = self.cache_dir / "orphan.bin.storage.partial"
        self.assertTrue(cache_path.exists())
        self.assertFalse(partial_path.exists())

    def test_streaming_open_retries_on_partial_enoent_with_final_fallback(self):
        payload = os.urandom(256 * 1024)
        (self.backend_dir / "race.bin").write_bytes(payload)

        seed_vfd = self.fs.open('/race.bin', os.O_RDONLY)
        with self.fs._fd_lock:
            seed_entry = self.fs._fd_map[seed_vfd]
        self.assertIsNotNone(seed_entry.prefetch_session)
        session = seed_entry.prefetch_session

        session.final_cache_path.parent.mkdir(parents=True, exist_ok=True)
        session.final_cache_path.write_bytes(payload)

        real_os_open = os.open
        partial_path = str(session.partial_cache_path)
        raised_once = {"value": False}

        def flaky_open(path, flags, mode=0o777):
            if path == partial_path and not raised_once["value"]:
                raised_once["value"] = True
                raise FileNotFoundError(errno.ENOENT, "simulated partial rename race", path)
            return real_os_open(path, flags, mode)

        with patch("storage.fs.os.open", side_effect=flaky_open):
            retry_vfd = self.fs.open('/race.bin', os.O_RDONLY)

        self.fs.release('/race.bin', retry_vfd)
        self.fs.release('/race.bin', seed_vfd)

    def test_streaming_release_drops_reader_ref_when_close_fails(self):
        payload = os.urandom(128 * 1024)
        (self.backend_dir / "closefail.bin").write_bytes(payload)

        vfd = self.fs.open('/closefail.bin', os.O_RDONLY)
        with self.fs._fd_lock:
            entry = self.fs._fd_map[vfd]
        self.assertIsNotNone(entry.prefetch_session)

        with self.assertRaises(FuseOSError) as ctx:
            with patch("storage.fs.os.close", side_effect=OSError(errno.EIO, "simulated close failure")):
                self.fs.release('/closefail.bin', vfd)

        self.assertEqual(ctx.exception.errno, errno.EIO)
        with entry.prefetch_session.lock:
            self.assertEqual(entry.prefetch_session.refcount, 0)
        with self.fs._fd_lock:
            self.assertNotIn(vfd, self.fs._fd_map)

    def test_streaming_release_before_first_read_cleans_session(self):
        for index in range(4):
            payload = os.urandom(64 * 1024)
            (self.backend_dir / f"idle-{index}.bin").write_bytes(payload)

        for index in range(3):
            path = f"/idle-{index}.bin"
            vfd = self.fs.open(path, os.O_RDONLY)
            self.fs.release(path, vfd)

            partial_path = self.cache_dir / f"idle-{index}.bin.storage.partial"
            final_path = self.cache_dir / f"idle-{index}.bin"
            self.assertFalse(partial_path.exists())
            self.assertFalse(final_path.exists())

        self.assertTrue(self._wait_for(lambda: len(self.fs._prefetch_sessions) == 0, timeout=3.0))

        vfd = self.fs.open('/idle-3.bin', os.O_RDONLY)
        with self.fs._fd_lock:
            entry = self.fs._fd_map[vfd]
        self.assertIsNotNone(entry.prefetch_session)
        self.fs.release('/idle-3.bin', vfd)

    def test_streaming_release_before_first_read_marks_cancelled_not_failed(self):
        payload = os.urandom(64 * 1024)
        (self.backend_dir / "cancelled.bin").write_bytes(payload)

        with patch.object(
            self.fs._streaming_manager,
            "_cleanup_cancelled_session",
            wraps=self.fs._streaming_manager._cleanup_cancelled_session,
        ) as cancelled_cleanup:
            with patch.object(
                self.fs._streaming_manager,
                "_cleanup_failed_session",
                wraps=self.fs._streaming_manager._cleanup_failed_session,
            ) as failed_cleanup:
                vfd = self.fs.open('/cancelled.bin', os.O_RDONLY)
                self.fs.release('/cancelled.bin', vfd)

                self.assertTrue(self._wait_for(lambda: len(self.fs._prefetch_sessions) == 0, timeout=3.0))
                # The stop unregisters at once; the worker's tail, which runs
                # the cancelled cleanup, follows on its own thread.
                self.assertTrue(self._wait_for(lambda: cancelled_cleanup.call_count >= 1, timeout=3.0))
                self.assertEqual(failed_cleanup.call_count, 0)


    def _session_for(self, vfd):
        with self.fs._fd_lock:
            session = self.fs._fd_map[vfd].prefetch_session
        self.assertIsNotNone(session)
        return session

    def test_cancel_refuses_a_finalizing_download(self):
        # All bytes arrived (prefetch_complete) but finalize has not run: a
        # cancel then would race finalize, so it reports busy and changes
        # nothing, even with no readers.
        (self.backend_dir / "finalizing.bin").write_bytes(os.urandom(128 * 1024))
        manager = self.fs._streaming_manager
        vfd = self.fs.open('/finalizing.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        try:
            with session.lock:
                # As if the last reader had closed: refcount 0 is otherwise
                # cancellable, so only prefetch_complete makes this busy.
                readers, session.refcount = session.refcount, 0
                session.prefetch_complete = True
            try:
                self.assertEqual(manager.cancel_download_session(session.session_id), "busy")
                self.assertEqual(manager.cancel_download_session_with_reason(session.session_id),
                                 ("busy", "finalizing"))
                with session.lock:
                    self.assertFalse(session.stop_requested)
                row, = manager.list_download_sessions()
                self.assertEqual(row["status"], "finalizing")
                with session.lock:
                    session.prefetch_complete = False
                    session.error = OSError(5, "simulated member failure")
                try:
                    self.assertEqual(manager.cancel_download_session_with_reason(session.session_id),
                                     ("busy", "stopping"))
                finally:
                    with session.lock:
                        session.error = None
            finally:
                with session.lock:
                    session.refcount = readers
                    session.prefetch_complete = False
        finally:
            self.fs.release('/finalizing.bin', vfd)

    def test_streaming_unread_session_uses_no_worker_and_reopens_fresh(self):
        payload = os.urandom(128 * 1024)
        (self.backend_dir / "reopen.bin").write_bytes(payload)

        vfd_a = self.fs.open('/reopen.bin', os.O_RDONLY)
        session_a = self._session_for(vfd_a)
        self.assertEqual(self.fs._streaming_stats_snapshot()["download_workers"], 0)
        self.fs.release('/reopen.bin', vfd_a)
        self.assertTrue(session_a.detached)
        self.assertTrue(session_a.background_finished.is_set())
        self.assertEqual(session_a.io_fd, -1)

        vfd_b = self.fs.open('/reopen.bin', os.O_RDONLY)
        session_b = self._session_for(vfd_b)
        self.assertIsNot(session_b, session_a)
        self.assertEqual(self.fs.read('/reopen.bin', len(payload), 0, vfd_b), payload)
        self.fs.release('/reopen.bin', vfd_b)
        self.assertTrue(session_b.background_finished.wait(timeout=3.0))
        self.assertEqual((self.cache_dir / "reopen.bin").read_bytes(), payload)

    def test_streaming_stop_and_finalize_cannot_both_proceed(self):
        # The stop decision and the detach must be one session.lock section.
        # Right after release's stop section exits, run what a worker
        # finishing the last chunk would do: complete the ranges and
        # finalize. Finalize must see the session detached and not rename the
        # partial (which a successor may already own) into the cache as CLEAN.
        payload = os.urandom(128 * 1024)
        (self.backend_dir / "gap.bin").write_bytes(payload)
        manager = self.fs._streaming_manager
        # The real worker cannot finish the file first: nothing is read, so
        # it waits for a first read until the stop wakes it.

        vfd = self.fs.open('/gap.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        real_lock = session.lock
        test_thread = threading.get_ident()
        state = {"depth": 0, "fired": False}

        def worker_finishes_last_chunk():
            with real_lock:
                session.available_ranges = [(0, session.file_size)]
                session.prefetch_complete = True
            manager._finalize_streaming_session(session)

        class GapLock:
            def __enter__(proxy):
                real_lock.acquire()
                if threading.get_ident() == test_thread:
                    state["depth"] += 1
                return proxy

            def __exit__(proxy, *exc):
                fire = False
                if threading.get_ident() == test_thread:
                    state["depth"] -= 1
                    fire = state["depth"] == 0 and session.stop_requested and not state["fired"]
                real_lock.release()
                if fire:
                    state["fired"] = True
                    worker_finishes_last_chunk()
                return False

            def acquire(proxy, *args, **kwargs):
                return real_lock.acquire(*args, **kwargs)

            def release(proxy):
                real_lock.release()

        session.lock = GapLock()
        try:
            self.fs.release('/gap.bin', vfd)
        finally:
            session.lock = real_lock

        self.assertTrue(state["fired"])
        self.assertTrue(session.detached)
        self.assertFalse(session.finalized)
        self.assertFalse((self.cache_dir / "gap.bin").exists())
        self.assertFalse((self.cache_dir / "gap.bin.storage.partial").exists())
        self.assertFalse(self.fs.cache_manager.is_cached("/gap.bin"))
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/gap.bin"))

    def test_streaming_failed_session_cleanup_is_single_shot(self):
        # The worker's tail and release can both clean up one failed session.
        # A successor created after the first cleanup must survive the second.
        payload = os.urandom(128 * 1024)
        (self.backend_dir / "twice.bin").write_bytes(payload)
        partial_path = self.cache_dir / "twice.bin.storage.partial"
        manager = self.fs._streaming_manager

        vfd_a = self.fs.open('/twice.bin', os.O_RDONLY)
        session_a = self._session_for(vfd_a)
        with session_a.lock:
            session_a.error = OSError(errno.EIO, "simulated failure")
            manager._schedule_background(session_a)
        self.assertTrue(session_a.background_finished.wait(timeout=3.0))

        # First cleanup, as the worker's tail runs it once no reader is left:
        # claimed, then run.
        with session_a.lock:
            self.assertTrue(manager._claim_failed_cleanup(session_a))
        manager._cleanup_failed_session(session_a)
        self.assertFalse(partial_path.exists())

        # A successor session takes the path (it is open by two handles here
        # only because the test keeps vfd_a to release it afterwards).
        vfd_b = self.fs.open('/twice.bin', os.O_RDONLY)
        session_b = self._session_for(vfd_b)
        self.assertIsNot(session_b, session_a)
        partial_inode = partial_path.stat().st_ino

        # Second cleanup, through release of the failed session's reader. It
        # stops at failed_cleanup_claimed (the first cleanup claimed it), so
        # it never reaches the detached guard in promote/claim; that guard is
        # covered by test_streaming_stopped_session_leaves_successor_partial_alone.
        self.fs.release('/twice.bin', vfd_a)

        self.assertTrue(partial_path.exists())
        self.assertEqual(partial_path.stat().st_ino, partial_inode)
        with self.fs._prefetch_sessions_lock:
            self.assertIs(self.fs._prefetch_sessions.get('/twice.bin'), session_b)
        self.assertEqual(self.fs.read('/twice.bin', 4096, 0, vfd_b), payload[:4096])
        self.fs.release('/twice.bin', vfd_b)

    def test_streaming_stopped_session_leaves_successor_partial_alone(self):
        # Once stopped, a session no longer owns the partial path: its late
        # cleanup, failure promotion and state xattr updates must not touch
        # the file a new session for the same path created there.
        payload = os.urandom(128 * 1024)
        (self.backend_dir / "successor.bin").write_bytes(payload)
        partial_path = self.cache_dir / "successor.bin.storage.partial"
        cache_path = self.cache_dir / "successor.bin"
        manager = self.fs._streaming_manager

        vfd_a = self.fs.open('/successor.bin', os.O_RDONLY)
        session_a = self._session_for(vfd_a)
        self.fs.release('/successor.bin', vfd_a)
        self.assertTrue(session_a.detached)
        self.assertFalse(partial_path.exists())

        # The successor's worker cannot fetch or finish: it waits for a
        # first read, and none happens until the checks below are done.
        vfd_b = self.fs.open('/successor.bin', os.O_RDONLY)
        session_b = self._session_for(vfd_b)
        self.assertIsNot(session_b, session_a)
        self.assertTrue(partial_path.exists())
        partial_inode = partial_path.stat().st_ino
        state_before = os.getxattr(str(partial_path), self.fs._STREAM_STATE_XATTR)

        with session_a.lock:
            session_a.error = OSError(errno.EIO, "late failure")
            session_a.available_ranges = [(0, 4096)]
            self.assertTrue(manager._claim_failed_cleanup(session_a))
        manager._update_streaming_state_xattr(session_a, "error")
        manager._cleanup_failed_session(session_a)
        manager._cleanup_cancelled_session(session_a)
        manager._finalize_streaming_session(session_a)

        self.assertTrue(partial_path.exists())
        self.assertEqual(partial_path.stat().st_ino, partial_inode)
        self.assertFalse(cache_path.exists())
        self.assertEqual(os.getxattr(str(partial_path), self.fs._STREAM_STATE_XATTR), state_before)
        with self.fs._prefetch_sessions_lock:
            self.assertIs(self.fs._prefetch_sessions.get('/successor.bin'), session_b)

        self.assertEqual(self.fs.read('/successor.bin', 4096, 0, vfd_b), payload[:4096])
        self.fs.release('/successor.bin', vfd_b)


    def _assert_not_cached(self, name):
        self.assertFalse((self.cache_dir / name).exists())
        self.assertFalse((self.cache_dir / f"{name}.storage.partial").exists())
        self.assertFalse(self.fs.cache_manager.is_cached(f"/{name}"))
        self.assertIsNone(self.fs.state_manager.get_file_metadata(f"/{name}"))

    def test_streaming_small_read_then_close_does_not_fetch_whole_file(self):
        # A content sniff: a few KiB from the start, then close. The session
        # stops; nothing is renamed into the cache or recorded.
        self.backend.range_delay = 0.03
        payload = os.urandom(2 * 1024 * 1024)
        (self.backend_dir / "sniff.bin").write_bytes(payload)

        vfd = self.fs.open('/sniff.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        self.assertEqual(self.fs.read('/sniff.bin', 4096, 0, vfd), payload[:4096])
        self.fs.release('/sniff.bin', vfd)

        with self.fs._prefetch_sessions_lock:
            self.assertNotIn('/sniff.bin', self.fs._prefetch_sessions)
        self.assertTrue(session.background_finished.wait(timeout=3.0))
        self.assertEqual(session.io_fd, -1)
        with session.lock:
            fetched = sum(end - start for start, end in session.available_ranges)
        self.assertLess(fetched, len(payload))
        time.sleep(0.3)
        self._assert_not_cached("sniff.bin")

    def test_streaming_scattered_small_reads_do_not_count_as_consumption(self):
        # A thumbnailer: small reads at scattered offsets add up past the
        # threshold but no contiguous span reaches it.
        self.backend.range_delay = 0.03
        payload = os.urandom(2 * 1024 * 1024)
        (self.backend_dir / "thumb.bin").write_bytes(payload)

        vfd = self.fs.open('/thumb.bin', os.O_RDONLY)
        # 8 reads of 24 KiB (192 KiB in all) against a 128 KiB threshold,
        # spaced 8 chunks apart so the worker, which follows the reader,
        # cannot finish the file before the close.
        for offset in range(0, len(payload), 256 * 1024):
            self.assertEqual(
                self.fs.read('/thumb.bin', 24 * 1024, offset, vfd),
                payload[offset:offset + 24 * 1024],
            )
        session = self._session_for(vfd)
        with session.lock:
            self.assertFalse(session.readers_consumed)
        self.fs.release('/thumb.bin', vfd)

        self.assertTrue(self._wait_for(lambda: session.io_fd == -1, timeout=3.0))
        self._assert_not_cached("thumb.bin")

    def test_streaming_reopen_after_stop_while_worker_mid_chunk(self):
        # The stop lands while the worker is mid-chunk, and release returns
        # without waiting for it. The reopen right after gets a fresh
        # session on a fresh partial file; the old worker's late write goes
        # to its own unlinked inode, and the new session caches correct bytes.
        self.backend.range_delay = 0.3
        payload = os.urandom(512 * 1024)
        (self.backend_dir / "midchunk.bin").write_bytes(payload)

        vfd_a = self.fs.open('/midchunk.bin', os.O_RDONLY)
        session_a = self._session_for(vfd_a)
        # One chunk, so the worker starts read-ahead.
        self.assertEqual(self.fs.read('/midchunk.bin', 32 * 1024, 0, vfd_a), payload[:32 * 1024])
        self.assertTrue(self._wait_for(lambda: bool(session_a.in_flight_ranges), timeout=2.0))

        self.fs.release('/midchunk.bin', vfd_a)
        self.assertTrue(session_a.detached)
        self.assertFalse(session_a.background_finished.is_set())

        vfd_b = self.fs.open('/midchunk.bin', os.O_RDONLY)
        session_b = self._session_for(vfd_b)
        self.assertIsNot(session_b, session_a)
        for offset in range(0, len(payload), 64 * 1024):
            self.assertEqual(
                self.fs.read('/midchunk.bin', 64 * 1024, offset, vfd_b),
                payload[offset:offset + 64 * 1024],
            )
        self.fs.release('/midchunk.bin', vfd_b)

        self.assertTrue(self._wait_for(lambda: session_a.io_fd == -1, timeout=3.0))
        cache_path = self.cache_dir / "midchunk.bin"
        self.assertTrue(self._wait_for(cache_path.exists, timeout=8.0))
        self.assertEqual(cache_path.read_bytes(), payload)

    def test_streaming_stopping_release_does_not_wait_for_the_worker(self):
        # A release that waited for a worker mid-chunk would hold the release
        # (and, with the single-threaded loop, every other operation) for the
        # fetch.
        self.backend.range_delay = 1.5
        payload = os.urandom(512 * 1024)
        (self.backend_dir / "slow.bin").write_bytes(payload)

        vfd = self.fs.open('/slow.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        # One chunk, so the worker starts read-ahead.
        self.fs.read('/slow.bin', 32 * 1024, 0, vfd)
        self.assertTrue(self._wait_for(lambda: bool(session.in_flight_ranges), timeout=3.0))

        started = time.monotonic()
        self.fs.release('/slow.bin', vfd)
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertTrue(session.detached)
        self.assertTrue(session.background_finished.wait(timeout=5.0))
        self.assertEqual(session.io_fd, -1)
        self._assert_not_cached("slow.bin")

    def test_streaming_reopen_after_stop_restarts_cleanly(self):
        self.backend.range_delay = 0.03
        payload = os.urandom(1024 * 1024)
        (self.backend_dir / "again.bin").write_bytes(payload)

        vfd = self.fs.open('/again.bin', os.O_RDONLY)
        first = self._session_for(vfd)
        self.fs.read('/again.bin', 4096, 0, vfd)
        self.fs.release('/again.bin', vfd)
        self.assertTrue(first.detached)

        vfd = self.fs.open('/again.bin', os.O_RDONLY)
        second = self._session_for(vfd)
        self.assertIsNot(second, first)
        self.assertEqual(self.fs.read('/again.bin', 8192, 300 * 1024, vfd), payload[300 * 1024:308 * 1024])
        self.fs.release('/again.bin', vfd)
        self.assertTrue(self._wait_for(lambda: second.io_fd == -1, timeout=3.0))
        self._assert_not_cached("again.bin")

    def test_streaming_session_complete_before_finalize_is_not_stopped(self):
        # All bytes arrived but the worker has not renamed yet when the last
        # reader closes: the close must not cancel it.
        payload = os.urandom(16 * 1024)
        (self.backend_dir / "done.bin").write_bytes(payload)
        manager = self.fs._streaming_manager
        real_finalize = manager._finalize_streaming_session
        gate = threading.Event()

        def held_finalize(session):
            gate.wait(timeout=5.0)
            real_finalize(session)

        with patch.object(manager, "_finalize_streaming_session", side_effect=held_finalize):
            vfd = self.fs.open('/done.bin', os.O_RDONLY)
            session = self._session_for(vfd)
            self.assertEqual(self.fs.read('/done.bin', len(payload), 0, vfd), payload)
            with session.lock:
                self.assertTrue(session.prefetch_complete)
                self.assertFalse(session.finalized)
            releaser = threading.Thread(target=self.fs.release, args=('/done.bin', vfd))
            releaser.start()
            self.assertTrue(self._wait_for(lambda: session.refcount == 0, timeout=2.0))
            self.assertFalse(session.detached)
            gate.set()
            releaser.join(timeout=5.0)

        cache_path = self.cache_dir / "done.bin"
        self.assertTrue(self._wait_for(cache_path.exists, timeout=5.0))
        self.assertEqual(cache_path.read_bytes(), payload)


    def _release_in_thread(self, path, vfd):
        releaser = threading.Thread(target=self.fs.release, args=(path, vfd), daemon=True)
        releaser.start()
        return releaser

    def test_streaming_failed_release_does_not_wait_for_worker_mid_fetch(self):
        # A foreground fetch fails while the worker is blocked in a remote
        # read. The last release must return without waiting for that read
        # (with the single-threaded loop it would stall the mount); the
        # worker cleans up when it ends.
        self.backend.worker_gate = threading.Event()
        payload = os.urandom(256 * 1024)
        (self.backend_dir / "errjoin.bin").write_bytes(payload)
        partial_path = self.cache_dir / "errjoin.bin.storage.partial"

        vfd = self.fs.open('/errjoin.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        # One chunk, so the worker starts read-ahead.
        self.assertEqual(self.fs.read('/errjoin.bin', 32 * 1024, 0, vfd), payload[:32 * 1024])
        self.assertTrue(self.backend.worker_in_read.wait(timeout=3.0))

        self.backend.fail_after_offset = 128 * 1024
        with self.assertRaises(FuseOSError):
            self.fs.read('/errjoin.bin', 4096, 192 * 1024, vfd)

        releaser = self._release_in_thread('/errjoin.bin', vfd)
        releaser.join(timeout=1.0)
        self.assertFalse(releaser.is_alive(), "release waited for the worker's remote read")
        self.assertFalse(session.background_finished.is_set())
        self.assertTrue(partial_path.exists())

        self.backend.worker_gate.set()
        self.assertTrue(session.background_finished.wait(timeout=3.0))
        self.assertEqual(session.io_fd, -1)
        self.assertTrue(session.detached)
        self.assertFalse(partial_path.exists())
        with self.fs._prefetch_sessions_lock:
            self.assertNotIn('/errjoin.bin', self.fs._prefetch_sessions)
        # The worker's tail promoted the contiguous prefix it had.
        cached = (self.cache_dir / "errjoin.bin").read_bytes()
        self.assertGreater(len(cached), 0)
        self.assertEqual(cached, payload[:len(cached)])

    def test_streaming_failed_release_after_worker_tail_cleans_up(self):
        # The worker's tail ran while a reader still held the failed session,
        # so it left cleanup to the release. The thread has not ended yet
        # (here held after its tail; in practice, just scheduled out) when
        # the last release comes: release must clean up now, without waiting
        # for the thread, or nothing ever removes the session.
        manager = self.fs._streaming_manager
        real_finish = manager._finish_background_session
        tail_done = threading.Event()
        hold = threading.Event()
        self.addCleanup(hold.set)

        def held_finish(session):
            real_finish(session)
            tail_done.set()
            hold.wait(timeout=10.0)

        payload = os.urandom(128 * 1024)
        (self.backend_dir / "tail.bin").write_bytes(payload)
        partial_path = self.cache_dir / "tail.bin.storage.partial"

        with patch.object(manager, "_finish_background_session", side_effect=held_finish):
            vfd = self.fs.open('/tail.bin', os.O_RDONLY)
            session = self._session_for(vfd)
            with session.lock:
                session.error = OSError(errno.EIO, "simulated failure")
                manager._schedule_background(session)
            self.assertTrue(tail_done.wait(timeout=3.0))
            self.assertFalse(session.background_finished.is_set())
            self.assertTrue(partial_path.exists())

            releaser = self._release_in_thread('/tail.bin', vfd)
            releaser.join(timeout=1.0)
            self.assertFalse(releaser.is_alive(), "release waited for the worker turn")
            self.assertFalse(session.background_finished.is_set())
            self.assertEqual(session.io_fd, -1)
            self.assertTrue(session.detached)
            self.assertFalse(partial_path.exists())
            with self.fs._prefetch_sessions_lock:
                self.assertNotIn('/tail.bin', self.fs._prefetch_sessions)
            hold.set()

    def test_streaming_failed_session_cleaned_by_exactly_one_side(self):
        # Release drops the last ref of a failed session while the worker is
        # just short of its tail; the tail then runs before release goes on.
        # Who cleans up must be decided with the decrement: the tail sees no
        # readers and cleans up, and release must not clean up as well (two
        # cleanups in flight can both pass promotion's detached check).
        manager = self.fs._streaming_manager
        real_finalize = manager._finalize_streaming_session
        real_stop = manager._stop_and_detach_session
        real_cleanup = manager._cleanup_failed_session
        at_tail = threading.Event()
        go_tail = threading.Event()
        self.addCleanup(go_tail.set)
        cleanups = []

        def held_finalize(session):
            at_tail.set()
            go_tail.wait(timeout=5.0)
            real_finalize(session)

        def stop_then_tail(session):
            result = real_stop(session)
            go_tail.set()
            self.assertTrue(session.background_finished.wait(timeout=5.0))
            return result

        def counted_cleanup(session):
            cleanups.append(threading.current_thread().name)
            real_cleanup(session)

        payload = os.urandom(128 * 1024)
        (self.backend_dir / "once.bin").write_bytes(payload)

        with patch.object(manager, "_finalize_streaming_session", side_effect=held_finalize), \
                patch.object(manager, "_stop_and_detach_session", side_effect=stop_then_tail), \
                patch.object(manager, "_cleanup_failed_session", side_effect=counted_cleanup):
            vfd = self.fs.open('/once.bin', os.O_RDONLY)
            session = self._session_for(vfd)
            with session.lock:
                session.error = OSError(errno.EIO, "simulated failure")
                manager._schedule_background(session)
            self.assertTrue(at_tail.wait(timeout=3.0))
            self.fs.release('/once.bin', vfd)

        self.assertTrue(session.background_finished.is_set())
        self.assertEqual(len(cleanups), 1, cleanups)
        self.assertTrue(cleanups[0].startswith("storage-prefetch-"), cleanups)
        self.assertEqual(session.io_fd, -1)
        self.assertTrue(session.detached)

    def test_streaming_concurrent_first_opens_share_one_partial(self):
        # Two opens of one uncached file both miss the registry and prepare a
        # partial before either registers. The loser attaches to the winner;
        # neither preparation may remove or replace the file the registered
        # session writes, and both readers must see the file's bytes.
        payload = os.urandom(128 * 1024)
        (self.backend_dir / "race.bin").write_bytes(payload)
        partial_path = self.cache_dir / "race.bin.storage.partial"
        import storage.streaming as streaming_module
        real_session_cls = streaming_module.PrefetchSession
        first_prepared = threading.Event()
        second_prepared = threading.Event()
        go_first = threading.Event()
        go_second = threading.Event()
        self.addCleanup(go_first.set)
        self.addCleanup(go_second.set)
        calls = []
        calls_lock = threading.Lock()

        def paused_session(**kwargs):
            # Runs after the partial is prepared, before registration.
            with calls_lock:
                calls.append(threading.current_thread().name)
                first = len(calls) == 1
            if first:
                first_prepared.set()
                go_first.wait(timeout=5.0)
            else:
                second_prepared.set()
                go_second.wait(timeout=5.0)
            return real_session_cls(**kwargs)

        results = {}

        def opener(name):
            try:
                # The unlocked body: StorageFS.open holds the path's lock, so
                # two opens of one path never reach the registry together
                # through it; this drives the streaming manager's own guard.
                results[name] = self.fs._open('/race.bin', os.O_RDONLY)
            except Exception as exc:
                results[name] = exc

        with patch.object(streaming_module, "PrefetchSession", side_effect=paused_session):
            first = threading.Thread(target=opener, args=("first",), name="opener-first")
            second = threading.Thread(target=opener, args=("second",), name="opener-second")
            first.start()
            self.assertTrue(first_prepared.wait(timeout=3.0))
            second.start()
            self.assertTrue(second_prepared.wait(timeout=3.0))
            go_first.set()
            first.join(timeout=5.0)
            go_second.set()
            second.join(timeout=5.0)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertIsInstance(results["first"], int, results["first"])
        self.assertIsInstance(results["second"], int, results["second"])
        session = self._session_for(results["first"])
        self.assertIs(self._session_for(results["second"]), session)
        self.assertTrue(partial_path.exists())
        self.assertEqual(os.fstat(session.io_fd).st_ino, partial_path.stat().st_ino)
        self.assertEqual(
            [p.name for p in self.cache_dir.iterdir() if p.name.endswith(".storage.partial")],
            ["race.bin.storage.partial"],
        )

        for vfd in (results["first"], results["second"]):
            for offset in range(0, len(payload), 32 * 1024):
                self.assertEqual(
                    self.fs.read('/race.bin', 32 * 1024, offset, vfd),
                    payload[offset:offset + 32 * 1024],
                )
        self.fs.release('/race.bin', results["first"])
        self.fs.release('/race.bin', results["second"])
        cache_path = self.cache_dir / "race.bin"
        self.assertTrue(self._wait_for(cache_path.exists, timeout=5.0))
        self.assertEqual(cache_path.read_bytes(), payload)

    def test_streaming_first_open_replaces_orphan_partial(self):
        # A partial a detached session failed to unlink is left at the path.
        # The next session's registration renames its own file over it.
        payload = os.urandom(64 * 1024)
        (self.backend_dir / "orphan-partial.bin").write_bytes(payload)
        partial_path = self.cache_dir / "orphan-partial.bin.storage.partial"
        partial_path.write_bytes(b"stale" * 100)
        orphan_inode = partial_path.stat().st_ino

        vfd = self.fs.open('/orphan-partial.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        self.assertNotEqual(partial_path.stat().st_ino, orphan_inode)
        self.assertEqual(os.fstat(session.io_fd).st_ino, partial_path.stat().st_ino)
        self.assertEqual(partial_path.stat().st_size, len(payload))
        self.assertEqual(
            [p.name for p in self.cache_dir.iterdir() if p.name.endswith(".storage.partial")],
            ["orphan-partial.bin.storage.partial"],
        )
        self.assertEqual(self.fs.read('/orphan-partial.bin', 4096, 0, vfd), payload[:4096])
        self.fs.release('/orphan-partial.bin', vfd)

    def test_streaming_open_sniffer_does_not_pull_whole_file(self):
        # A reader reads the first chunk, which starts read-ahead, and holds
        # the file open. Until the readers show consumption, read-ahead stays
        # within the keep-after-close threshold (here 4 chunks: 128 KiB) past
        # what they read, rounded up to whole chunks, and follows the reader
        # when it moves.
        self.backend.range_delay = 0.005
        payload = os.urandom(2 * 1024 * 1024)
        (self.backend_dir / "held.bin").write_bytes(payload)
        chunk = 32 * 1024
        threshold = 4 * chunk
        manager = self.fs._streaming_manager
        # create=True so that, without the bound, the test fails on the
        # forbidden fetch below rather than on a missing attribute.
        real_bounded = getattr(manager, "_bounded_read_ahead_chunk", None)
        real_read_file = self.backend.read_file
        parked_at = []
        background_reads = []

        def spy_bounded(session):
            # The worker asks this under session.lock; None means it is
            # parked at the bound for the reader's current offset.
            result = real_bounded(session)
            if result is None:
                parked_at.append(session.last_foreground_offset)
            return result

        def spy_read_file(virtual_path, offset, size):
            if threading.current_thread().name.startswith("storage-prefetch-"):
                background_reads.append((offset, size))
            return real_read_file(virtual_path, offset, size)

        def fetched(session):
            with session.lock:
                return list(session.available_ranges)

        with patch.object(manager, "_bounded_read_ahead_chunk", side_effect=spy_bounded, create=True), \
                patch.object(self.backend, "read_file", side_effect=spy_read_file):
            vfd = self.fs.open('/held.bin', os.O_RDONLY)
            session = self._session_for(vfd)
            self.assertEqual(self.fs.read('/held.bin', chunk, 0, vfd), payload[:chunk])
            # Chunks up to the end of the read + threshold, and no further.
            first_end = chunk + threshold
            self.assertTrue(self._wait_for(
                lambda: 0 in parked_at or any(start >= first_end for start, _ in background_reads),
                timeout=5.0,
            ))
            self.assertEqual(fetched(session), [(0, first_end)])
            self.assertTrue(all(start < first_end for start, _ in background_reads), background_reads)

            # The bound follows the reader to a new offset, forward only.
            offset = 1024 * 1024
            self.assertEqual(self.fs.read('/held.bin', 4096, offset, vfd), payload[offset:offset + 4096])
            self.assertTrue(self._wait_for(lambda: offset in parked_at, timeout=5.0))
            second_end = ((offset + 4096 + threshold) // chunk + 1) * chunk
            self.assertEqual(fetched(session), [(0, first_end), (offset, second_end)])
            self.assertTrue(
                all(start < first_end or offset <= start < second_end for start, _ in background_reads),
                background_reads,
            )
            with session.lock:
                self.assertFalse(session.prefetch_complete)
                self.assertFalse(session.readers_consumed)

            self.fs.release('/held.bin', vfd)
        self.assertTrue(session.detached)
        self._assert_not_cached("held.bin")

    def test_streaming_consuming_reader_lifts_read_ahead_bound(self):
        # Once a contiguous span reaches the threshold, read-ahead runs to the
        # end of the file while the reader still holds it open.
        self.backend.range_delay = 0.005
        payload = os.urandom(2 * 1024 * 1024)
        (self.backend_dir / "lift.bin").write_bytes(payload)

        vfd = self.fs.open('/lift.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        for offset in range(0, 4 * 32 * 1024, 16 * 1024):
            self.assertEqual(
                self.fs.read('/lift.bin', 16 * 1024, offset, vfd),
                payload[offset:offset + 16 * 1024],
            )
        with session.lock:
            self.assertTrue(session.readers_consumed)
        cache_path = self.cache_dir / "lift.bin"
        self.assertTrue(self._wait_for(cache_path.exists, timeout=8.0))
        self.assertEqual(cache_path.read_bytes(), payload)
        self.fs.release('/lift.bin', vfd)

    def test_streaming_keep_after_close_zero_leaves_read_ahead_unbounded(self):
        # "0" restores the old behaviour while open too: any read counts as
        # consumption, so the worker prefetches the whole file.
        self._remake_fs("0")
        self.backend.range_delay = 0.005
        payload = os.urandom(1024 * 1024)
        (self.backend_dir / "zero.bin").write_bytes(payload)

        vfd = self.fs.open('/zero.bin', os.O_RDONLY)
        self.assertEqual(self.fs.read('/zero.bin', 4096, 0, vfd), payload[:4096])
        cache_path = self.cache_dir / "zero.bin"
        self.assertTrue(self._wait_for(cache_path.exists, timeout=8.0))
        self.assertEqual(cache_path.read_bytes(), payload)
        self.fs.release('/zero.bin', vfd)

    def test_streaming_span_trim_keeps_the_latest_read(self):
        # Past the tracked-span bound the shortest span is dropped, but never
        # the one the latest read joined: the read-ahead bound is measured
        # from it, and sequential short reads must be able to grow it.
        payload = os.urandom(4 * 1024 * 1024)
        (self.backend_dir / "spans.bin").write_bytes(payload)
        manager = self.fs._streaming_manager
        vfd = self.fs.open('/spans.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        with session.lock:
            for index in range(64):
                manager._record_foreground_read(session, index * 64 * 1024, index * 64 * 1024 + 2048)
            manager._record_foreground_read(session, 4096 * 1024 - 1024, 4096 * 1024 - 512)
            spans = list(session.foreground_read_spans)
            self.assertFalse(session.readers_consumed)
        self.assertEqual(len(spans), 64)
        self.assertIn((4096 * 1024 - 1024, 4096 * 1024 - 512), spans)
        self.fs.release('/spans.bin', vfd)


    def _record_member_reads(self, on_background_read=None):
        """Patch the backend to log (offset, size, background) per range read."""
        reads = []
        real_read_file = self.backend.read_file

        def spy_read_file(virtual_path, offset, size):
            background = threading.current_thread().name.startswith("storage-prefetch-")
            reads.append((offset, size, background))
            data = real_read_file(virtual_path, offset, size)
            if background and on_background_read is not None:
                on_background_read()
            return data

        return reads, patch.object(self.backend, "read_file", side_effect=spy_read_file)

    def _sniff(self, name, size, read_size=4096):
        payload = os.urandom(size)
        (self.backend_dir / name).write_bytes(payload)
        reads, spy = self._record_member_reads()
        with spy:
            vfd = self.fs.open(f'/{name}', os.O_RDONLY)
            session = self._session_for(vfd)
            self.assertEqual(self.fs.read(f'/{name}', read_size, 0, vfd), payload[:read_size])
            self.fs.release(f'/{name}', vfd)
            # The release stops the worker; once it has exited, every range
            # read the session will ever make is in reads.
            self.assertTrue(session.background_finished.wait(timeout=5.0))
        return reads

    def test_streaming_sniff_requests_only_what_it_reads(self):
        # A content-type sniff of an uncached multi-MiB file at the default
        # 1 MiB chunk: the member is asked for the 4 KiB read, nothing else.
        self._remake_fs(chunk_size=1024 * 1024)
        reads = self._sniff("sniff-large.bin", 6 * 1024 * 1024)
        self.assertEqual(reads, [(0, 4096, False)])
        self._assert_not_cached("sniff-large.bin")

    def test_streaming_sniff_of_file_under_one_chunk_is_not_kept(self):
        # Below one chunk the old rounding fetched the whole file, which then
        # finalized into the cache.
        self._remake_fs(chunk_size=1024 * 1024)
        reads = self._sniff("sniff-small.bin", 700 * 1024)
        self.assertEqual(reads, [(0, 4096, False)])
        self._assert_not_cached("sniff-small.bin")

    def test_streaming_reads_below_one_chunk_start_no_read_ahead(self):
        # Sequential reads that stop short of one chunk (32 KiB here) are
        # served from the member as asked, with no speculative fetch.
        payload = os.urandom(2 * 1024 * 1024)
        (self.backend_dir / "short.bin").write_bytes(payload)
        reads, spy = self._record_member_reads()
        with spy:
            vfd = self.fs.open('/short.bin', os.O_RDONLY)
            session = self._session_for(vfd)
            for offset in range(0, 28 * 1024, 4096):
                self.assertEqual(
                    self.fs.read('/short.bin', 4096, offset, vfd),
                    payload[offset:offset + 4096],
                )
            self.fs.release('/short.bin', vfd)
            self.assertTrue(session.background_finished.wait(timeout=5.0))
        self.assertEqual(reads, [(offset, 4096, False) for offset in range(0, 28 * 1024, 4096)])
        self._assert_not_cached("short.bin")

    def test_streaming_consuming_reader_starts_read_ahead_after_one_chunk(self):
        # Once the readers' span reaches one chunk the worker prefetches
        # ahead of them, from where they are, without being asked.
        chunk = 32 * 1024
        payload = os.urandom(2 * 1024 * 1024)
        (self.backend_dir / "consume.bin").write_bytes(payload)
        background_read = threading.Event()
        reads, spy = self._record_member_reads(on_background_read=background_read.set)
        with spy:
            vfd = self.fs.open('/consume.bin', os.O_RDONLY)
            session = self._session_for(vfd)
            for offset in range(0, chunk, 8 * 1024):
                self.assertEqual(
                    self.fs.read('/consume.bin', 8 * 1024, offset, vfd),
                    payload[offset:offset + 8 * 1024],
                )
            self.assertTrue(background_read.wait(timeout=5.0))
            self.fs.release('/consume.bin', vfd)
            self.assertTrue(session.background_finished.wait(timeout=5.0))
        background = [(offset, size) for offset, size, is_bg in reads if is_bg]
        # Read-ahead starts past what the reader already fetched.
        self.assertGreaterEqual(background[0][0], chunk)
        foreground = [(offset, size) for offset, size, is_bg in reads if not is_bg]
        self.assertEqual(foreground, [(offset, 8 * 1024) for offset in range(0, chunk, 8 * 1024)])

    def test_streaming_scattered_reader_falls_back_to_chunk_fetches(self):
        # A reader making many tiny scattered reads would leave one range per
        # read. Past the fragment limit its reads fetch the missing run out
        # to the chunk boundary, so the range list stays bounded.
        from storage.streaming import STREAMING_MAX_FOREGROUND_FRAGMENTS as limit

        self.backend.range_delay = 0
        chunk = 32 * 1024
        stride = 2048
        payload = os.urandom(4 * 1024 * 1024)
        (self.backend_dir / "scatter.bin").write_bytes(payload)
        reads, spy = self._record_member_reads()
        with spy:
            vfd = self.fs.open('/scatter.bin', os.O_RDONLY)
            session = self._session_for(vfd)
            for offset in range(0, len(payload), stride):
                self.assertEqual(
                    self.fs.read('/scatter.bin', 1, offset, vfd),
                    payload[offset:offset + 1],
                )
            self.fs.release('/scatter.bin', vfd)
            self.assertTrue(session.background_finished.wait(timeout=5.0))
        self.assertFalse(any(background for _o, _s, background in reads))
        # The first reads cost one byte each; past the limit a read's fetch
        # runs to the end of its chunk.
        self.assertEqual(reads[:limit + 1], [(offset, 1, False) for offset in range(0, (limit + 1) * stride, stride)])
        # What bounds the range list: every fetch past the limit ends on a
        # chunk boundary or at the end of the file, so it merges into a run
        # of whole chunks.
        # (the read that makes the limit-th range is itself one byte)
        for offset, size, _bg in reads[limit + 1:]:
            end = offset + size
            self.assertTrue(end % chunk == 0 or end == len(payload), (offset, size))
        # The first fallback fetch reaches back to the start of the missing
        # run (the byte after the last range) and out to the chunk end.
        self.assertEqual(reads[limit + 1], (limit * stride + 1, ((limit + 1) * stride // chunk + 1) * chunk - (limit * stride + 1), False))
        with session.lock:
            self.assertLessEqual(len(session.available_ranges), limit + 1 + len(payload) // chunk)
        offset, size, _bg = reads[-1]
        self.assertEqual(offset + size, len(payload))
        self.assertGreater(size, 1)

    # Concurrent writers, destroy and failed cleanup. Each test parks one
    # side at an Event (no sleeps) and runs the other side in between.

    def _hold_finalize(self):
        """Patch the worker's finalize to wait; returns (patcher, at_finalize, go)."""
        manager = self.fs._streaming_manager
        real_finalize = manager._finalize_streaming_session
        at_finalize = threading.Event()
        go = threading.Event()
        self.addCleanup(go.set)

        def held_finalize(session):
            at_finalize.set()
            go.wait(timeout=10.0)
            real_finalize(session)

        return patch.object(manager, "_finalize_streaming_session", side_effect=held_finalize), at_finalize, go

    def _stream_whole_file(self, name, payload):
        vfd = self.fs.open('/' + name, os.O_RDONLY)
        session = self._session_for(vfd)
        for offset in range(0, len(payload), 32 * 1024):
            self.assertEqual(
                self.fs.read('/' + name, 32 * 1024, offset, vfd),
                payload[offset:offset + 32 * 1024],
            )
        return vfd, session

    def _read_through_fs(self, name, size):
        vfd = self.fs.open('/' + name, os.O_RDONLY)
        try:
            with self.fs._fd_lock:
                self.assertIsNone(self.fs._fd_map[vfd].prefetch_session)
            return self.fs.read('/' + name, size, 0, vfd)
        finally:
            self.fs.release('/' + name, vfd)

    def _assert_cache_holds(self, name, expected, dirty=True):
        cache_path = self.cache_dir / name
        self.assertEqual(cache_path.read_bytes(), expected)
        st = cache_path.stat()
        meta = self.fs.state_manager.get_file_metadata('/' + name)
        self.assertIsNotNone(meta)
        self.assertEqual(meta.size, st.st_size)
        self.assertEqual(meta.last_modified, st.st_mtime)
        if dirty:
            self.assertEqual(meta.sync_status, SyncStatus.DIRTY)
        self.assertEqual(
            [p.name for p in self.cache_dir.iterdir() if p.name.endswith(".storage.partial")], [],
        )
        with self.fs._prefetch_sessions_lock:
            self.assertNotIn('/' + name, self.fs._prefetch_sessions)

    def _write_open_and_edit(self, name, edit):
        vfd = self.fs.open('/' + name, os.O_RDWR)
        with self.fs._fd_lock:
            self.assertIsNone(self.fs._fd_map[vfd].prefetch_session)
        self.assertEqual(self.fs.write('/' + name, edit, 0, vfd), len(edit))
        return vfd

    def test_streaming_finalize_keeps_an_open_writers_file(self):
        # A write-open during a stream fetches the whole file into the cache
        # path and writes to it. The stream's finalize must not replace that
        # file: the writer's bytes would be on an unlinked inode and the
        # index row would describe bytes the cache no longer holds.
        self.backend.copy_delay = 0
        payload = os.urandom(64 * 1024)
        (self.backend_dir / "writer.bin").write_bytes(payload)
        edit = b"W" * 4096
        expected = edit + payload[len(edit):]

        hold, at_finalize, go = self._hold_finalize()
        with hold:
            rvfd, session = self._stream_whole_file("writer.bin", payload)
            self.assertTrue(at_finalize.wait(timeout=3.0))
            wvfd = self._write_open_and_edit("writer.bin", edit)
            cache_file = self.cache_dir / "writer.bin"
            winning_mtime = cache_file.stat().st_mtime_ns
            winning_attrs = os.getxattr(cache_file, MEMBER_ATTRS_XATTR)
            go.set()
            self.assertTrue(session.background_finished.wait(timeout=5.0))
        self.assertEqual(cache_file.stat().st_mtime_ns, winning_mtime)
        self.assertEqual(os.getxattr(cache_file, MEMBER_ATTRS_XATTR), winning_attrs)

        with self.fs._fd_lock:
            writer_fd = self.fs._fd_map[wvfd].real_fd
        self.assertEqual(os.fstat(writer_fd).st_ino, (self.cache_dir / "writer.bin").stat().st_ino)
        self.assertEqual(self._read_through_fs("writer.bin", len(payload)), expected)
        self._assert_cache_holds("writer.bin", expected)
        self.assertTrue(session.detached)
        # The streaming reader keeps the bytes it opened.
        self.assertEqual(self.fs.read('/writer.bin', 4096, 0, rvfd), payload[:4096])
        self.fs.release('/writer.bin', rvfd)
        self.assertEqual(session.io_fd, -1)
        self.fs.release('/writer.bin', wvfd)
        self._assert_cache_holds("writer.bin", expected)

    def test_streaming_finalize_keeps_a_closed_writers_file(self):
        # As above, but the writer has closed before the stream finishes: no
        # descriptor names the path any more, and its bytes still win.
        self.backend.copy_delay = 0
        payload = os.urandom(64 * 1024)
        (self.backend_dir / "closed.bin").write_bytes(payload)
        edit = b"C" * 4096
        expected = edit + payload[len(edit):]

        hold, at_finalize, go = self._hold_finalize()
        with hold:
            rvfd, session = self._stream_whole_file("closed.bin", payload)
            self.assertTrue(at_finalize.wait(timeout=3.0))
            wvfd = self._write_open_and_edit("closed.bin", edit)
            self.fs.release('/closed.bin', wvfd)
            go.set()
            self.assertTrue(session.background_finished.wait(timeout=5.0))
        self.assertTrue(session.background_finished.is_set())

        self.assertEqual(self._read_through_fs("closed.bin", len(payload)), expected)
        self._assert_cache_holds("closed.bin", expected)
        self.fs.release('/closed.bin', rvfd)

    def test_streaming_finalize_records_a_write_landing_before_the_row(self):
        # ditz streaming-finalize-row-window: once the partial is installed a
        # write-open finds the file cached. Its write must be recorded even
        # though the never-cached file had no index row yet: finalize stores
        # the row under the index lock it holds from the install, so the
        # write's mark_dirty lands on that row instead of finding none (and
        # finalize then storing CLEAN from its pre-write stat).
        self.backend.copy_delay = 0
        payload = os.urandom(64 * 1024)
        (self.backend_dir / "window.bin").write_bytes(payload)
        edit = b"E" * 4096
        expected = edit + payload[len(edit):]

        # The index lock, traced: finalize is let go only once the writer
        # has either finished inside the window (the old code) or found the
        # lock held and is waiting for it (the fix), never on a timer.
        writer_settled = threading.Event()
        real_lock = self.fs.state_manager._db_lock

        class TracedLock:
            def acquire(self, blocking=True, timeout=-1):
                if threading.current_thread().name == "window-writer":
                    if real_lock.acquire(blocking=False):
                        return True
                    writer_settled.set()
                return real_lock.acquire(blocking, timeout)

            def release(self):
                real_lock.release()

            __enter__ = acquire

            def __exit__(self, *exc):
                real_lock.release()

        self.fs.state_manager._db_lock = TracedLock()
        self.addCleanup(setattr, self.fs.state_manager, "_db_lock", real_lock)

        cache = self.fs.cache_manager
        real_store = cache.store_file_metadata
        at_store = threading.Event()
        go = threading.Event()
        self.addCleanup(go.set)

        def held_store(metadata, **kwargs):
            if threading.current_thread().name.startswith("storage-prefetch-"):
                at_store.set()
                self.assertTrue(go.wait(timeout=10.0))
            return real_store(metadata, **kwargs)

        outcome = {}

        def write_through_mount():
            try:
                vfd = self._write_open_and_edit("window.bin", edit)
                self.fs.release('/window.bin', vfd)
                outcome["done"] = True
            except BaseException as exc:  # noqa: BLE001 - reported below
                outcome["error"] = exc
            finally:
                writer_settled.set()

        with patch.object(cache, "store_file_metadata", side_effect=held_store):
            rvfd, session = self._stream_whole_file("window.bin", payload)
            self.assertTrue(at_store.wait(timeout=3.0))
            self.assertTrue((self.cache_dir / "window.bin").exists())
            writer = threading.Thread(target=write_through_mount, name="window-writer", daemon=True)
            writer.start()
            self.assertTrue(writer_settled.wait(timeout=3.0))
            go.set()
            writer.join(timeout=5.0)
            self.assertTrue(session.background_finished.wait(timeout=5.0))
        self.assertFalse(writer.is_alive())
        self.assertNotIn("error", outcome)
        self.assertTrue(session.background_finished.is_set())

        self._assert_cache_holds("window.bin", expected)
        self.assertIn("/window.bin", self.fs.sync_manager._queued_tasks)
        self.fs.release('/window.bin', rvfd)

    def test_streaming_session_does_not_start_over_a_writers_file(self):
        # The mirror case: a read-open found the file uncached, and a
        # write-open fetched it and wrote to it before the read-open
        # registered its session. The reader must get the writer's file, not
        # a stream of the member's older bytes that would later replace it.
        self.backend.copy_delay = 0
        manager = self.fs._streaming_manager
        real_select = manager.select_backend_for_read
        in_setup = threading.Event()
        go = threading.Event()
        self.addCleanup(go.set)

        def paused_select(canonical_path):
            if threading.current_thread().name == "late-reader":
                in_setup.set()
                go.wait(timeout=10.0)
            return real_select(canonical_path)

        payload = os.urandom(64 * 1024)
        (self.backend_dir / "mirror.bin").write_bytes(payload)
        edit = b"M" * 4096
        expected = edit + payload[len(edit):]
        results = {}

        def reader():
            try:
                # The unlocked body: StorageFS.open holds the path's lock, so
                # the write-open below could not run inside this open through
                # it; this drives the streaming manager's own refusal.
                results["reader"] = self.fs._open('/mirror.bin', os.O_RDONLY)
            except Exception as exc:
                results["reader"] = exc

        with patch.object(manager, "select_backend_for_read", side_effect=paused_select):
            late = threading.Thread(target=reader, name="late-reader")
            late.start()
            self.assertTrue(in_setup.wait(timeout=3.0))
            wvfd = self._write_open_and_edit("mirror.bin", edit)
            go.set()
            late.join(timeout=5.0)
        self.assertFalse(late.is_alive())

        rvfd = results["reader"]
        self.assertIsInstance(rvfd, int, rvfd)
        with self.fs._fd_lock:
            self.assertIsNone(self.fs._fd_map[rvfd].prefetch_session)
        self.assertEqual(self.fs.read('/mirror.bin', len(payload), 0, rvfd), expected)
        self.fs.release('/mirror.bin', rvfd)
        self.fs.release('/mirror.bin', wvfd)
        self._assert_cache_holds("mirror.bin", expected)

    def test_streaming_open_racing_destroy_registers_no_session(self):
        # An open misses the registry and prepares its partial; destroy runs
        # to the end before the open registers. The open must not register
        # a session (nothing would ever stop it): it takes the full-file path.
        self.backend.copy_delay = 0
        manager = self.fs._streaming_manager
        import storage.streaming as streaming_module
        real_session_cls = streaming_module.PrefetchSession
        prepared = threading.Event()
        go = threading.Event()
        self.addCleanup(go.set)

        def paused_session(**kwargs):
            prepared.set()
            go.wait(timeout=10.0)
            return real_session_cls(**kwargs)

        payload = os.urandom(64 * 1024)
        (self.backend_dir / "unmount.bin").write_bytes(payload)
        results = {}

        def opener():
            try:
                results["open"] = self.fs.open('/unmount.bin', os.O_RDONLY)
            except Exception as exc:
                results["open"] = exc

        with patch.object(streaming_module, "PrefetchSession", side_effect=paused_session):
            thread = threading.Thread(target=opener, name="racing-open")
            thread.start()
            self.assertTrue(prepared.wait(timeout=3.0))
            manager.destroy()
            go.set()
            thread.join(timeout=5.0)
        self.assertFalse(thread.is_alive())

        vfd = results["open"]
        self.assertIsInstance(vfd, int, vfd)
        with self.fs._fd_lock:
            self.assertIsNone(self.fs._fd_map[vfd].prefetch_session)
        self.assertEqual(self.fs.read('/unmount.bin', len(payload), 0, vfd), payload)
        self.fs.release('/unmount.bin', vfd)
        self._assert_cache_holds("unmount.bin", payload, dirty=False)

    def test_streaming_destroy_leaves_an_open_during_destroy_alone(self):
        # destroy has taken the sessions and is waiting for a worker when an
        # open of the same file comes in. The open must not register a new
        # session at the partial path destroy is about to unlink.
        self.backend.copy_delay = 0
        manager = self.fs._streaming_manager
        payload = os.urandom(128 * 1024)
        (self.backend_dir / "during.bin").write_bytes(payload)
        in_join = threading.Event()
        go = threading.Event()
        self.addCleanup(go.set)

        vfd_a = self.fs.open('/during.bin', os.O_RDONLY)
        session_a = self._session_for(vfd_a)
        real_shutdown = manager._download_pool.shutdown

        def held_shutdown(timeout):
            in_join.set()
            go.wait(timeout=10.0)
            real_shutdown(timeout)

        with patch.object(manager._download_pool, 'shutdown', side_effect=held_shutdown):
            destroyer = threading.Thread(target=manager.destroy, name="destroyer")
            destroyer.start()
            self.assertTrue(in_join.wait(timeout=3.0))
            vfd_b = self.fs.open('/during.bin', os.O_RDONLY)
            go.set()
            destroyer.join(timeout=5.0)
        self.assertFalse(destroyer.is_alive())

        with self.fs._fd_lock:
            self.assertIsNone(self.fs._fd_map[vfd_b].prefetch_session)
        self.assertTrue(session_a.detached)
        self.assertEqual(self.fs.read('/during.bin', len(payload), 0, vfd_b), payload)
        self.fs.release('/during.bin', vfd_a)
        self.fs.release('/during.bin', vfd_b)
        self._assert_cache_holds("during.bin", payload, dirty=False)

    def test_streaming_open_does_not_attach_to_a_session_being_cleaned_up(self):
        # The worker's tail has decided to clean up a failed session (no
        # readers) and is promoting its prefix when an open comes in. The
        # open must not attach: promotion truncates the partial to its
        # contiguous prefix, so reads past it would come back short.
        self.backend.copy_delay = 0
        manager = self.fs._streaming_manager
        real_promote = manager._promote_failed_session_cache_prefix
        in_promote = threading.Event()
        go_promote = threading.Event()
        self.addCleanup(go_promote.set)

        def held_promote(session):
            in_promote.set()
            go_promote.wait(timeout=10.0)
            return real_promote(session)

        payload = os.urandom(192 * 1024)
        (self.backend_dir / "cleanup.bin").write_bytes(payload)
        hold, at_finalize, go_tail = self._hold_finalize()
        with hold, patch.object(manager, "_promote_failed_session_cache_prefix", side_effect=held_promote):
            vfd_a = self.fs.open('/cleanup.bin', os.O_RDONLY)
            session = self._session_for(vfd_a)
            # Available: [0, 4K) and [16K, 20K); the contiguous prefix is 4K.
            self.assertEqual(self.fs.read('/cleanup.bin', 4096, 0, vfd_a), payload[:4096])
            self.assertEqual(
                self.fs.read('/cleanup.bin', 4096, 16 * 1024, vfd_a), payload[16 * 1024:20 * 1024],
            )
            with session.lock:
                session.error = OSError(errno.EIO, "simulated failure")
                manager._schedule_background(session)
            self.assertTrue(at_finalize.wait(timeout=3.0))
            # The worker is short of its tail, so this release leaves cleanup to it.
            self.fs.release('/cleanup.bin', vfd_a)
            go_tail.set()
            self.assertTrue(in_promote.wait(timeout=3.0))

            vfd_b = self.fs.open('/cleanup.bin', os.O_RDONLY)
            go_promote.set()
            self.assertTrue(session.background_finished.wait(timeout=5.0))
        self.assertTrue(session.background_finished.is_set())
        self.assertTrue(session.detached)

        with self.fs._fd_lock:
            self.assertIsNone(self.fs._fd_map[vfd_b].prefetch_session)
        self.assertEqual(
            self.fs.read('/cleanup.bin', 4096, 16 * 1024, vfd_b), payload[16 * 1024:20 * 1024],
        )
        self.fs.release('/cleanup.bin', vfd_b)
        # The full fetch filled the path first; the promotion left it alone.
        self._assert_cache_holds("cleanup.bin", payload, dirty=False)

    def test_streaming_open_does_not_attach_to_a_failed_session(self):
        # The session has an error and a reader still attached, so its
        # cleanup is not claimed yet. An open attaching now would get EIO on
        # every range the session lacks; it takes the full-file path.
        self.backend.copy_delay = 0
        manager = self.fs._streaming_manager
        payload = os.urandom(192 * 1024)
        (self.backend_dir / "failed.bin").write_bytes(payload)
        vfd_a = self.fs.open('/failed.bin', os.O_RDONLY)
        session = self._session_for(vfd_a)
        self.assertEqual(self.fs.read('/failed.bin', 4096, 0, vfd_a), payload[:4096])
        with session.lock:
            session.error = OSError(errno.EIO, "simulated failure")
            manager._schedule_background(session)
            self.assertFalse(session.failed_cleanup_claimed)

        vfd_b = self.fs.open('/failed.bin', os.O_RDONLY)

        with self.fs._fd_lock:
            self.assertIsNone(self.fs._fd_map[vfd_b].prefetch_session)
        self.assertEqual(self.fs.read('/failed.bin', len(payload), 0, vfd_b), payload)
        self.fs.release('/failed.bin', vfd_b)
        self.fs.release('/failed.bin', vfd_a)

    def test_streaming_open_attaches_to_an_errored_session_with_every_byte(self):
        # Only its finalize failed: the session lacks no range, so an attach
        # reads from it and the opener does not refetch the file.
        self.backend.copy_delay = 0
        manager = self.fs._streaming_manager
        payload = os.urandom(64 * 1024)
        (self.backend_dir / "whole.bin").write_bytes(payload)
        vfd_a = self.fs.open('/whole.bin', os.O_RDONLY)
        session = self._session_for(vfd_a)
        hold, at_finalize, go = self._hold_finalize()
        with hold:
            self.assertEqual(self.fs.read('/whole.bin', len(payload), 0, vfd_a), payload)
            self.assertTrue(at_finalize.wait(timeout=5.0))
            with session.lock:
                session.error = OSError(errno.EIO, "simulated finalize failure")
                manager._schedule_background(session)

            vfd_b = self.fs.open('/whole.bin', os.O_RDONLY)

            self.assertIs(self._session_for(vfd_b), session)
            self.assertEqual(self.fs.read('/whole.bin', len(payload), 0, vfd_b), payload)
            go.set()
        self.fs.release('/whole.bin', vfd_b)
        self.fs.release('/whole.bin', vfd_a)

    def test_streaming_claimed_chunk_uses_no_worker_until_read_finishes(self):
        manager = self.fs._streaming_manager
        payload = os.urandom(192 * 1024)
        (self.backend_dir / "claimed.bin").write_bytes(payload)
        vfd = self.fs.open('/claimed.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        with session.lock:
            session.in_flight_ranges.append((0, session.file_size))
            session.readers_consumed = True
            session.read_ahead_started = True
            manager._schedule_background(session)
        self.assertEqual(manager._download_pool.snapshot(), (0, 0, 0))
        with session.lock:
            session.in_flight_ranges.clear()
            manager._schedule_background(session)
        self.assertTrue(session.background_finished.wait(timeout=3.0))
        self.assertEqual((self.cache_dir / "claimed.bin").read_bytes(), payload)
        self.fs.release('/claimed.bin', vfd)

    def test_streaming_staging_file_name_is_not_a_bare_token(self):
        (self.backend_dir / "staged.bin").write_bytes(os.urandom(64 * 1024))
        created = []
        real_open = os.open

        def spy_open(path, *args, **kwargs):
            created.append(Path(path).name)
            return real_open(path, *args, **kwargs)

        with patch.object(os, "open", side_effect=spy_open):
            vfd = self.fs.open('/staged.bin', os.O_RDONLY)
        self.fs.release('/staged.bin', vfd)
        self.assertTrue(any(name.startswith(".storage-staging-") for name in created), created)
        self.assertFalse(any(re.match(r"\.[0-9a-f]{16}\.", name) for name in created), created)

    def _install_case(self, name, final_bytes=None):
        from types import SimpleNamespace
        partial = self.cache_dir / (name + ".storage.partial")
        final = self.cache_dir / name
        partial.write_bytes(b"streamed")
        if final_bytes is not None:
            final.write_bytes(final_bytes)
        session = SimpleNamespace(
            partial_cache_path=partial, final_cache_path=final, canonical_path='/' + name,
        )
        return session, partial, final

    def _no_hard_links(self):
        real_link = os.link

        def link(src, dst, *args, **kwargs):
            if str(src).startswith(str(self.cache_dir)):
                raise OSError(errno.EPERM, "no hard links here")
            return real_link(src, dst, *args, **kwargs)

        return patch("os.link", side_effect=link)

    def test_streaming_install_without_hard_links_does_not_replace(self):
        manager = self.fs._streaming_manager
        session, partial, final = self._install_case("nolink-taken.bin", final_bytes=b"writer")
        with self._no_hard_links():
            outcome = manager._install_partial(session)
        if outcome == "unsupported":
            self.skipTest("the test filesystem has no RENAME_NOREPLACE")
        self.assertEqual(outcome, "taken")
        self.assertEqual(final.read_bytes(), b"writer")
        self.assertTrue(partial.exists())

        session, partial, final = self._install_case("nolink-free.bin")
        with self._no_hard_links():
            self.assertEqual(manager._install_partial(session), "installed")
        self.assertEqual(final.read_bytes(), b"streamed")
        self.assertFalse(partial.exists())

    def test_streaming_install_without_any_no_replace_declines(self):
        # Neither link() nor RENAME_NOREPLACE: a check and a rename would
        # race a writer, so the partial is not installed at all.
        import storage.streaming as streaming_module
        manager = self.fs._streaming_manager
        session, partial, final = self._install_case("noreplace.bin")
        unsupported = OSError(errno.ENOSYS, "no renameat2")
        with self._no_hard_links(), \
                patch.object(streaming_module, "_rename_noreplace", side_effect=unsupported):
            self.assertEqual(manager._install_partial(session), "unsupported")
        self.assertFalse(os.path.lexists(final))
        self.assertTrue(partial.exists())

    def test_streaming_discard_labels_state_with_the_real_outcome(self):
        manager = self.fs._streaming_manager
        for outcome, status in (("taken", "superseded"), ("unsupported", "unsupported")):
            name = f"label-{outcome}.bin"
            (self.backend_dir / name).write_bytes(os.urandom(64 * 1024))
            vfd = self.fs.open('/' + name, os.O_RDONLY)
            session = self._session_for(vfd)
            # As in production, discard runs with no worker fetching.
            with session.lock:
                session.stop_requested = True
                manager._schedule_background(session)
            self.assertTrue(session.background_finished.wait(timeout=5.0))
            with patch.object(manager, "_snapshot_streaming_state") as snapshot, \
                    patch.object(manager, "_log_streaming_state_change"):
                manager._discard_superseded_session(session, outcome)
            snapshot.assert_called_once_with(session, status)
            self.fs.release('/' + name, vfd)
        self.assertEqual(manager._not_installed_reason("taken"), "cache-path-filled")
        self.assertEqual(manager._not_installed_reason("unsupported"), "unsupported")


class TestControlRequests(unittest.TestCase):
    """The mount control dispatcher, called directly: no socket, no mount."""

    VALID_ID = "0123456789abcdef0123456789abcdef"

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        (self.temp_dir / "cache").mkdir()
        (self.temp_dir / "backend").mkdir()
        self.fs = StorageFS(
            cache_root=self.temp_dir / "cache",
            state_db=self.temp_dir / "state.db",
            backends={"backend": LocalBackend("backend", str(self.temp_dir / "backend"))},
            cache_size=1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
            streaming_reads=True,
            streaming_chunk_size=32 * 1024,
            max_streaming_sessions=2,
            mountpoint="/mnt/example",
        )
        self.manager = self.fs._streaming_manager
        # As if published; the socket itself is not started here.
        self.manager._control_server.cache_identity = (1, 2)
        self.initial_limits = self.manager.download_limits()

    def tearDown(self):
        try:
            self.fs.destroy('/')
        finally:
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def handle(self, request):
        if not isinstance(request, str):
            request = json.dumps(request)
        return self.manager._handle_control_request(request)

    def test_rejected_requests(self):
        set_message = "Provide at least one live streaming limit to set"
        cancel_message = "Provide a complete download ID from 'storage control ... downloads'"
        extra_message = "Unexpected arguments for control command"
        object_message = "Expected a JSON control request object"
        cases = [
            ('{"command": "status", "command": "status"}', "Duplicate field in control request"),
            ('{"command": "streaming.set", "limits": {"max_download_workers": 2, '
             '"max_download_workers": 3}}', "Duplicate field in control request"),
            ("not json", object_message),
            ("[]", object_message),
            ('"status"', object_message),
            ("null", object_message),
            ("[" * 100000, object_message),
            ({}, "Unknown mount control command"),
            ({"command": "streaming.delete"}, "Unknown mount control command"),
            ({"command": None}, "Unknown mount control command"),
            ({"command": ["status"]}, "Unknown mount control command"),
            ({"command": "status", "verbose": True}, extra_message),
            ({"command": "streaming.get", "limits": {}}, extra_message),
            ({"command": "downloads.list", "id": self.VALID_ID}, extra_message),
            ({"command": "streaming.set"}, set_message),
            ({"command": "streaming.set", "limits": {}}, set_message),
            ({"command": "streaming.set", "limits": None}, set_message),
            ({"command": "streaming.set", "limits": [["max_download_workers", 2]]}, set_message),
            ({"command": "streaming.set", "limits": "max_download_workers=2"}, set_message),
            ({"command": "streaming.set", "limits": {"max_download_workers": 2}, "extra": 1},
             set_message),
            ({"command": "streaming.set", "limits": {"max_download_workers": 2, "bogus": 1}},
             "unknown download limit: bogus"),
            ({"command": "streaming.set", "limits": {"self": 1}}, "unknown download limit: self"),
            ({"command": "streaming.set", "limits": {"limits": 1}}, "unknown download limit: limits"),
            ({"command": "streaming.set", "limits": {"max_download_workers": True}},
             "max_download_workers must be an integer between"),
            ({"command": "streaming.set", "limits": {"max_streaming_sessions": 2.0}},
             "max_streaming_sessions must be an integer between"),
            ({"command": "streaming.set", "limits": {"max_streaming_sessions": 0}},
             "max_streaming_sessions must be an integer between"),
            ({"command": "streaming.set", "limits": {
                "max_streaming_sessions": 3, "max_download_workers": 1000}},
             "max_download_workers must be an integer between"),
            ({"command": "downloads.cancel"}, cancel_message),
            ({"command": "downloads.cancel", "id": self.VALID_ID[:-1]}, cancel_message),
            ({"command": "downloads.cancel", "id": self.VALID_ID + "0"}, cancel_message),
            ({"command": "downloads.cancel", "id": self.VALID_ID.upper()}, cancel_message),
            ({"command": "downloads.cancel", "id": self.VALID_ID + "\n"}, cancel_message),
            ({"command": "downloads.cancel", "id": int(self.VALID_ID, 16)}, cancel_message),
            ({"command": "downloads.cancel", "id": [self.VALID_ID]}, cancel_message),
            ({"command": "downloads.cancel", "id": self.VALID_ID, "force": True}, cancel_message),
            ({"command": "streaming.set", "limits": {"max_download_workers": 2},
              "mountpoint": "/mnt/other"},
             "This control socket belongs to the mount at '/mnt/example', not '/mnt/other'"),
            ({"command": "streaming.set", "limits": {"max_download_workers": 2},
              "mountpoint": "/mnt/example/"},
             "This control socket belongs to the mount at '/mnt/example', not '/mnt/example/'"),
            ({"command": "streaming.set", "limits": {"max_download_workers": 2},
              "mountpoint": None}, "Expected the target mountpoint as a string"),
            ({"command": "downloads.cancel", "id": self.VALID_ID, "mountpoint": "/mnt/other"},
             "This control socket belongs to the mount at '/mnt/example', not '/mnt/other'"),
            ({"command": "streaming.set", "limits": {"max_download_workers": 2}, "cache": [1, 3]},
             "This control socket was published in a different cache directory"),
            ({"command": "downloads.cancel", "id": self.VALID_ID, "cache": [2, 2]},
             "This control socket was published in a different cache directory"),
            ({"command": "streaming.set", "limits": {"max_download_workers": 2}, "cache": "1:2"},
             "Expected the target cache identity as [device, inode]"),
            ({"command": "streaming.set", "limits": {"max_download_workers": 2}, "cache": [1, True]},
             "Expected the target cache identity as [device, inode]"),
            ({"command": "streaming.set", "limits": {"max_download_workers": 2}, "cache": [1, 2, 3]},
             "Expected the target cache identity as [device, inode]"),
            ({"command": "status", "mountpoint": "/mnt/other"},
             "This control socket belongs to the mount at '/mnt/example', not '/mnt/other'"),
        ]
        for request, message in cases:
            with self.subTest(request=request):
                with self.assertRaises(ControlError) as raised:
                    self.handle(request)
                self.assertTrue(str(raised.exception).startswith(message), str(raised.exception))
                # A rejected request, including a partly valid set, changes nothing.
                self.assertEqual(self.manager.download_limits(), self.initial_limits)

    def test_accepted_requests(self):
        legacy = self.handle("streaming-stats\n")
        self.assertEqual(legacy["active_streaming_sessions"], 0)
        self.assertNotIn("limits", legacy)

        status = self.handle({"command": "status"})
        self.assertEqual(set(status), {"mount", "stats", "limits", "restart_only"})
        self.assertEqual(status["mount"], {"mountpoint": "/mnt/example", "pid": os.getpid(),
                                           "cache": [1, 2]})
        named = self.handle({"command": "status", "mountpoint": "/mnt/example"})
        self.assertEqual(named["mount"], status["mount"])
        self.assertEqual(named["limits"], status["limits"])
        self.assertEqual(status["limits"], self.initial_limits)
        self.assertTrue(status["restart_only"]["streaming_reads"])
        self.assertEqual(status["restart_only"]["streaming_chunk_size"], 32 * 1024)

        self.assertEqual(
            self.handle({"command": "streaming.get"}),
            {"limits": self.initial_limits, "restart_only": status["restart_only"]},
        )

        changed = self.handle({"command": "streaming.set", "mountpoint": "/mnt/example",
                               "cache": [1, 2], "limits": {
            "max_streaming_sessions": 3, "max_download_workers": 2,
        }})
        expected = {**self.initial_limits, "max_streaming_sessions": 3, "max_download_workers": 2}
        self.assertEqual(changed["limits"], expected)
        self.assertEqual(self.manager.download_limits(), expected)

        self.assertEqual(self.handle({"command": "downloads.list"}), {"downloads": []})
        self.assertEqual(
            self.handle({"command": "downloads.cancel", "id": self.VALID_ID}),
            {"id": self.VALID_ID, "result": "not-found"},
        )

    def test_mount_through_a_symlink_reports_its_target(self):
        target = self.temp_dir / "mounted here"
        target.mkdir()
        (self.temp_dir / "link").symlink_to(target)
        fs = StorageFS(
            cache_root=self.temp_dir / "cache",
            state_db=self.temp_dir / "linked.db",
            backends={"backend": LocalBackend("backend", str(self.temp_dir / "backend"))},
            cache_size=1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
            mountpoint=str(self.temp_dir / "link"),
        )
        try:
            self.assertEqual(fs._streaming_manager.mountpoint, str(target.resolve()))
        finally:
            fs.destroy('/')

    def test_final_dot_mountpoint_publishes_without_a_mountpoint(self):
        fs = StorageFS(
            cache_root=self.temp_dir / "cache",
            state_db=self.temp_dir / "other.db",
            backends={"backend": LocalBackend("backend", str(self.temp_dir / "backend"))},
            cache_size=1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
            mountpoint="/mnt/example/..",
        )
        try:
            manager = fs._streaming_manager
            self.assertIsNone(manager.mountpoint)
            # The socket still starts, for --cache and streaming-stats.
            with patch.object(manager._control_server, "start") as start:
                with self.assertLogs(manager.logger, "WARNING") as logged:
                    manager.init()
            start.assert_called_once_with()
            self.assertIn("report no mountpoint", logged.output[0])
            self.assertIn("without a final '.' or '..'", logged.output[0])

            manager._control_server.cache_identity = (1, 2)
            status = manager._handle_control_request(json.dumps({"command": "status", "cache": [1, 2]}))
            self.assertIsNone(status["mount"]["mountpoint"])
            self.assertEqual(status["mount"]["cache"], [1, 2])
            changed = manager._handle_control_request(json.dumps({
                "command": "streaming.set", "cache": [1, 2], "limits": {"max_download_workers": 2},
            }))
            self.assertEqual(changed["limits"]["max_download_workers"], 2)
            for named in ("/mnt/example/..", "/mnt", "/mnt/example"):
                with self.assertRaisesRegex(ControlError, "reports no mountpoint .*use --cache"):
                    manager._handle_control_request(json.dumps({
                        "command": "streaming.set", "mountpoint": named,
                        "limits": {"max_download_workers": 3},
                    }))
            self.assertEqual(manager.download_limits()["max_download_workers"], 2)
        finally:
            fs.destroy('/')

    def test_request_during_stop_says_the_mount_is_stopping(self):
        self.manager._control_server.cache_identity = None  # As stop() leaves it.
        with self.assertRaisesRegex(ControlError, "^This mount is stopping"):
            self.handle({"command": "streaming.set", "cache": [1, 2],
                         "limits": {"max_download_workers": 2}})
        self.assertEqual(self.manager.download_limits(), self.initial_limits)

    def test_update_download_limits_takes_a_dict_or_keywords(self):
        expected = {**self.initial_limits, "max_download_workers": 2, "max_streaming_sessions": 3}
        self.assertEqual(self.manager.update_download_limits({"max_download_workers": 2},
                                                             max_streaming_sessions=3), expected)
        with self.assertRaisesRegex(ValueError, "^unknown download limit: self$"):
            self.manager.update_download_limits({"self": 1})
        with self.assertRaisesRegex(ValueError, "^unknown download limit: self$"):
            self.manager.update_download_limits(**{"self": 1})
        self.assertEqual(self.manager.download_limits(), expected)

    def test_set_after_shutdown_is_refused(self):
        self.fs.destroy('/')
        with self.assertRaisesRegex(ControlError, "^mount is shutting down$"):
            self.handle({"command": "streaming.set", "limits": {"max_download_workers": 2}})
        self.assertEqual(self.manager.download_limits(), self.initial_limits)


if __name__ == '__main__':
    unittest.main()
