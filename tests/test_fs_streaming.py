import errno
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fuse import FuseOSError

from storage.backends.local import LocalBackend
from storage.fs import StorageFS
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

    def _make_fs(self, keep_after_close=None):
        return StorageFS(
            cache_root=self.cache_dir,
            state_db=self.db_path,
            backends={"backend": self.backend},
            cache_size=32 * 1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
            streaming_reads=True,
            streaming_chunk_size=32 * 1024,
            max_streaming_sessions=2,
            streaming_keep_after_close=(
                parse_keep_after_close(keep_after_close) if keep_after_close is not None else None
            ),
        )

    def _remake_fs(self, keep_after_close):
        self.fs.destroy('/')
        self.fs = self._make_fs(keep_after_close)

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

        self.assertTrue(self._wait_for(lambda: cache_path.exists() and not partial_path.exists(), timeout=8.0))
        self.assertEqual(cache_path.read_bytes(), payload)

        self.fs.release('/large.bin', vfd)

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

    def test_streaming_reopen_while_stopped_worker_alive_gets_fresh_session(self):
        # The mount is single-threaded, so the next open follows the close.
        # If the stopped session's worker is still running then (here a stub
        # reports it alive and returns from join at once, as a worker stuck
        # in a remote fetch past a join timeout would), the open must not
        # attach to the stopped session: nothing would ever finish or remove
        # it, and it would hold a max_streaming_sessions slot for good.
        payload = os.urandom(128 * 1024)
        (self.backend_dir / "reopen.bin").write_bytes(payload)

        vfd_a = self.fs.open('/reopen.bin', os.O_RDONLY)
        session_a = self._session_for(vfd_a)
        real_worker = session_a.worker

        class StuckWorker:
            def is_alive(stub):
                return True

            def join(stub, timeout=None):
                return None

        session_a.worker = StuckWorker()
        self.fs.release('/reopen.bin', vfd_a)
        self.assertTrue(session_a.detached)

        vfd_b = self.fs.open('/reopen.bin', os.O_RDONLY)
        session_b = self._session_for(vfd_b)
        self.assertIsNot(session_b, session_a)
        for offset in range(0, len(payload), 32 * 1024):
            self.assertEqual(
                self.fs.read('/reopen.bin', 32 * 1024, offset, vfd_b),
                payload[offset:offset + 32 * 1024],
            )
        self.fs.release('/reopen.bin', vfd_b)

        # The real worker (never read, so idle) exits on the stop and its
        # cancelled cleanup closes the descriptor.
        self.assertTrue(self._wait_for(lambda: not real_worker.is_alive(), timeout=3.0))
        self.assertEqual(session_a.io_fd, -1)
        cache_path = self.cache_dir / "reopen.bin"
        self.assertTrue(self._wait_for(cache_path.exists, timeout=8.0))
        self.assertEqual(cache_path.read_bytes(), payload)

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
            session_a.condition.notify_all()
        self.assertTrue(self._wait_for(lambda: not session_a.worker.is_alive(), timeout=3.0))

        # First cleanup, as the worker's tail runs it once no reader is left.
        manager._cleanup_failed_session(session_a)
        self.assertFalse(partial_path.exists())

        # A successor session takes the path (it is open by two handles here
        # only because the test keeps vfd_a to release it afterwards).
        vfd_b = self.fs.open('/twice.bin', os.O_RDONLY)
        session_b = self._session_for(vfd_b)
        self.assertIsNot(session_b, session_a)
        partial_inode = partial_path.stat().st_ino

        # Second cleanup, through release of the failed session's reader.
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
        self.assertTrue(self._wait_for(lambda: not session.worker.is_alive(), timeout=3.0))
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
        self.assertEqual(self.fs.read('/midchunk.bin', 4096, 0, vfd_a), payload[:4096])
        self.assertTrue(self._wait_for(lambda: bool(session_a.in_flight_chunks), timeout=2.0))

        self.fs.release('/midchunk.bin', vfd_a)
        self.assertTrue(session_a.detached)
        self.assertTrue(session_a.worker.is_alive())

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
        # The mount is single-threaded: a release that waited for a worker
        # mid-chunk would stall every other operation for the fetch.
        self.backend.range_delay = 1.5
        payload = os.urandom(512 * 1024)
        (self.backend_dir / "slow.bin").write_bytes(payload)

        vfd = self.fs.open('/slow.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        self.fs.read('/slow.bin', 4096, 0, vfd)
        self.assertTrue(self._wait_for(lambda: bool(session.in_flight_chunks), timeout=3.0))

        started = time.monotonic()
        self.fs.release('/slow.bin', vfd)
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertTrue(session.detached)
        self.assertTrue(self._wait_for(lambda: not session.worker.is_alive(), timeout=5.0))
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
        # reader closes after a small read: the close must not cancel it.
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
            self.assertEqual(self.fs.read('/done.bin', 4096, 0, vfd), payload[:4096])
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
        # read. The mount is single-threaded, so the last release must return
        # without waiting for that read; the worker cleans up when it ends.
        self.backend.worker_gate = threading.Event()
        payload = os.urandom(256 * 1024)
        (self.backend_dir / "errjoin.bin").write_bytes(payload)
        partial_path = self.cache_dir / "errjoin.bin.storage.partial"

        vfd = self.fs.open('/errjoin.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        self.assertEqual(self.fs.read('/errjoin.bin', 4096, 0, vfd), payload[:4096])
        self.assertTrue(self.backend.worker_in_read.wait(timeout=3.0))

        self.backend.fail_after_offset = 128 * 1024
        with self.assertRaises(FuseOSError):
            self.fs.read('/errjoin.bin', 4096, 192 * 1024, vfd)

        releaser = self._release_in_thread('/errjoin.bin', vfd)
        releaser.join(timeout=1.0)
        self.assertFalse(releaser.is_alive(), "release waited for the worker's remote read")
        self.assertTrue(session.worker.is_alive())
        self.assertTrue(partial_path.exists())

        self.backend.worker_gate.set()
        self.assertTrue(self._wait_for(lambda: not session.worker.is_alive(), timeout=3.0))
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
        real_worker = manager._background_prefetch_worker
        tail_done = threading.Event()
        hold = threading.Event()
        self.addCleanup(hold.set)

        def held_worker(session):
            real_worker(session)
            tail_done.set()
            hold.wait(timeout=10.0)

        payload = os.urandom(128 * 1024)
        (self.backend_dir / "tail.bin").write_bytes(payload)
        partial_path = self.cache_dir / "tail.bin.storage.partial"

        with patch.object(manager, "_background_prefetch_worker", side_effect=held_worker):
            vfd = self.fs.open('/tail.bin', os.O_RDONLY)
        session = self._session_for(vfd)
        with session.lock:
            session.error = OSError(errno.EIO, "simulated failure")
            session.condition.notify_all()
        self.assertTrue(tail_done.wait(timeout=3.0))
        self.assertTrue(session.worker.is_alive())
        self.assertTrue(partial_path.exists())

        releaser = self._release_in_thread('/tail.bin', vfd)
        releaser.join(timeout=1.0)
        self.assertFalse(releaser.is_alive(), "release waited for the worker thread")
        self.assertTrue(session.worker.is_alive())
        self.assertEqual(session.io_fd, -1)
        self.assertTrue(session.detached)
        self.assertFalse(partial_path.exists())
        with self.fs._prefetch_sessions_lock:
            self.assertNotIn('/tail.bin', self.fs._prefetch_sessions)

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
            session.worker.join(timeout=5.0)
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
                session.condition.notify_all()
            self.assertTrue(at_tail.wait(timeout=3.0))
            self.fs.release('/once.bin', vfd)

        self.assertFalse(session.worker.is_alive())
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
                results[name] = self.fs.open('/race.bin', os.O_RDONLY)
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


if __name__ == '__main__':
    unittest.main()
