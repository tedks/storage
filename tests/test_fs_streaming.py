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
from storage.models import SyncStatus
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
        # One chunk, so the worker starts read-ahead.
        self.assertEqual(self.fs.read('/midchunk.bin', 32 * 1024, 0, vfd_a), payload[:32 * 1024])
        self.assertTrue(self._wait_for(lambda: bool(session_a.in_flight_ranges), timeout=2.0))

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
        # One chunk, so the worker starts read-ahead.
        self.fs.read('/slow.bin', 32 * 1024, 0, vfd)
        self.assertTrue(self._wait_for(lambda: bool(session.in_flight_ranges), timeout=3.0))

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
        # read. The mount is single-threaded, so the last release must return
        # without waiting for that read; the worker cleans up when it ends.
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
            session.worker.join(timeout=5.0)
            self.assertFalse(session.worker.is_alive())
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
            session.worker.join(timeout=5.0)
            self.assertFalse(session.worker.is_alive())
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
            session.worker.join(timeout=5.0)
            self.assertFalse(session.worker.is_alive())
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
                with session.lock:
                    self.assertLessEqual(
                        len(session.available_ranges), limit + 1 + len(payload) // chunk,
                    )
            self.fs.release('/scatter.bin', vfd)
            session.worker.join(timeout=5.0)
            self.assertFalse(session.worker.is_alive())
        self.assertFalse(any(background for _o, _s, background in reads))
        # The first reads cost one byte each; past the limit a read's fetch
        # runs to the end of its chunk.
        self.assertEqual(reads[:limit], [(offset, 1, False) for offset in range(0, limit * stride, stride)])
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
            go.set()
            session.worker.join(timeout=5.0)
        self.assertFalse(session.worker.is_alive())

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
            session.worker.join(timeout=5.0)
        self.assertFalse(session.worker.is_alive())

        self.assertEqual(self._read_through_fs("closed.bin", len(payload)), expected)
        self._assert_cache_holds("closed.bin", expected)
        self.fs.release('/closed.bin', rvfd)

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
                results["reader"] = self.fs.open('/mirror.bin', os.O_RDONLY)
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
        real_worker = session_a.worker

        class JoinSpy:
            # Alive until joined, so destroy always waits here, however fast
            # the stopped worker really exits.
            def is_alive(self):
                return not in_join.is_set() or real_worker.is_alive()

            def join(self, timeout=None):
                in_join.set()
                go.wait(timeout=10.0)
                real_worker.join(timeout)

        session_a.worker = JoinSpy()
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
                session.condition.notify_all()
            self.assertTrue(at_finalize.wait(timeout=3.0))
            # The worker is short of its tail, so this release leaves cleanup to it.
            self.fs.release('/cleanup.bin', vfd_a)
            go_tail.set()
            self.assertTrue(in_promote.wait(timeout=3.0))

            vfd_b = self.fs.open('/cleanup.bin', os.O_RDONLY)
            go_promote.set()
            session.worker.join(timeout=5.0)
        self.assertFalse(session.worker.is_alive())
        self.assertTrue(session.detached)

        with self.fs._fd_lock:
            self.assertIsNone(self.fs._fd_map[vfd_b].prefetch_session)
        self.assertEqual(
            self.fs.read('/cleanup.bin', 4096, 16 * 1024, vfd_b), payload[16 * 1024:20 * 1024],
        )
        self.fs.release('/cleanup.bin', vfd_b)
        # The full fetch filled the path first; the promotion left it alone.
        self._assert_cache_holds("cleanup.bin", payload, dirty=False)

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
            self.assertFalse(manager._install_partial(session))
        self.assertEqual(final.read_bytes(), b"writer")
        self.assertTrue(partial.exists())

        session, partial, final = self._install_case("nolink-free.bin")
        with self._no_hard_links():
            self.assertTrue(manager._install_partial(session))
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
            self.assertFalse(manager._install_partial(session))
        self.assertFalse(os.path.lexists(final))
        self.assertTrue(partial.exists())

if __name__ == '__main__':
    unittest.main()
