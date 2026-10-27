import errno
import json
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fuse import FuseOSError

from storage.backends.local import LocalBackend
from storage.fs import StorageFS


class SlowRangeLocalBackend(LocalBackend):
    """Local backend test double with controllable read/copy latency and failures."""

    def __init__(self, name, root_path, range_delay=0.0, copy_delay=0.0,
                 fail_after_offset=None, max_read_bytes=None):
        super().__init__(name, root_path)
        self.range_delay = range_delay
        self.copy_delay = copy_delay
        self.fail_after_offset = fail_after_offset
        self.max_read_bytes = max_read_bytes

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        if self.fail_after_offset is not None and offset >= self.fail_after_offset:
            raise OSError(errno.EIO, "simulated backend range read failure")
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

        self.fs = StorageFS(
            cache_root=self.cache_dir,
            state_db=self.db_path,
            backends={"backend": self.backend},
            cache_size=32 * 1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
            streaming_reads=True,
            streaming_chunk_size=32 * 1024,
            max_streaming_sessions=2,
        )

    def tearDown(self):
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
                self.assertGreaterEqual(cancelled_cleanup.call_count, 1)
                self.assertEqual(failed_cleanup.call_count, 0)


    def _session_for(self, vfd):
        with self.fs._fd_lock:
            session = self.fs._fd_map[vfd].prefetch_session
        self.assertIsNotNone(session)
        return session

    def test_streaming_open_during_last_close_stop_gets_fresh_session(self):
        # An open that lands while the last close is stopping the session
        # (here: while release waits on the worker) must not attach to the
        # stopped session, whose descriptor the stop then closes.
        payload = os.urandom(128 * 1024)
        (self.backend_dir / "reopen.bin").write_bytes(payload)

        vfd_a = self.fs.open('/reopen.bin', os.O_RDONLY)
        session_a = self._session_for(vfd_a)
        real_worker = session_a.worker
        reopened = {}

        class JoinHook:
            def is_alive(hook):
                return real_worker.is_alive()

            def join(hook, timeout=None):
                if "vfd" not in reopened:
                    reopened["vfd"] = self.fs.open('/reopen.bin', os.O_RDONLY)
                real_worker.join(timeout)

        session_a.worker = JoinHook()
        self.fs.release('/reopen.bin', vfd_a)

        self.assertIn("vfd", reopened)
        vfd_b = reopened["vfd"]
        session_b = self._session_for(vfd_b)
        self.assertIsNot(session_b, session_a)
        for offset in range(0, len(payload), 32 * 1024):
            self.assertEqual(
                self.fs.read('/reopen.bin', 32 * 1024, offset, vfd_b),
                payload[offset:offset + 32 * 1024],
            )
        self.fs.release('/reopen.bin', vfd_b)

        self.assertTrue(self._wait_for(lambda: session_a.io_fd == -1, timeout=3.0))
        cache_path = self.cache_dir / "reopen.bin"
        self.assertTrue(self._wait_for(cache_path.exists, timeout=8.0))
        self.assertEqual(cache_path.read_bytes(), payload)

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

        # Hold the successor's worker before its first fetch: no read yet.
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



if __name__ == '__main__':
    unittest.main()
