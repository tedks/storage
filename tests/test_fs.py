import unittest
import errno
import tempfile
import shutil
import stat
from pathlib import Path
import multiprocessing as mp
import threading
import os
import json
import time
from unittest.mock import patch
from fuse import FuseOSError

from storage.fs import StorageFS
from storage.fs import _acquire_mount_lock
from storage.backends.local import LocalBackend
from storage.sync import DirOperation


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


class TestReaddirMerging(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_dir = self.temp_dir / "cache"
        self.cache_dir.mkdir()
        self.backend1_dir = self.temp_dir / "backend1"
        self.backend2_dir = self.temp_dir / "backend2"
        self.backend1_dir.mkdir()
        self.backend2_dir.mkdir()
        self.db_path = self.temp_dir / "state.db"

        self.backends = {
            "backend1": LocalBackend("backend1", str(self.backend1_dir)),
            "backend2": LocalBackend("backend2", str(self.backend2_dir)),
        }

        self.fs = StorageFS(
            cache_root=self.cache_dir,
            state_db=self.db_path,
            backends=self.backends,
            cache_size=10 * 1024 * 1024,
            num_sync_workers=1,
        )

    def tearDown(self):
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _wait_for(self, predicate, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.05)
        return predicate()

    def test_readdir_merges_backends_and_cache(self):
        # Files on different backends
        (self.backend1_dir / "foo.txt").write_text("one")
        (self.backend2_dir / "bar.txt").write_text("two")

        # Cache entry only
        cache_file = self.cache_dir / "baz.txt"
        cache_file.write_text("three")

        entries = set(self.fs.readdir('/', None))
        self.assertIn('foo.txt', entries)
        self.assertIn('bar.txt', entries)
        self.assertIn('baz.txt', entries)

    def test_readdir_falls_back_to_last_known_on_error(self):
        # Prime cache with successful listing
        (self.backend1_dir / "primed.txt").write_text("data")
        list(self.fs.readdir('/', None))

        # Simulate backend failure on subsequent calls
        original = self.backends['backend1'].listdir
        def _failing(_path):
            raise RuntimeError("backend offline")
        self.backends['backend1'].listdir = _failing

        entries = set(self.fs.readdir('/', None))
        self.assertIn('primed.txt', entries)

        # Restore for cleanliness
        self.backends['backend1'].listdir = original

    def test_readdir_uses_recent_snapshot_for_bursty_calls(self):
        (self.backend1_dir / "snap.txt").write_text("data")
        entries = set(self.fs.readdir('/', None))
        self.assertIn('snap.txt', entries)

        with patch.object(self.backends['backend1'], 'listdir_attrs', side_effect=RuntimeError('should not be called')):
            snap_entries = set(self.fs.readdir('/', None))
        self.assertIn('snap.txt', snap_entries)


    def test_getattr_remote_only_file(self):
        content = "hello"
        (self.backend1_dir / "remote.txt").write_text(content)

        attrs = self.fs.getattr('/remote.txt')
        self.assertTrue(stat.S_ISREG(attrs['st_mode']))
        self.assertEqual(attrs['st_size'], len(content))
        self.assertNotEqual(attrs['st_mtime'], 0)
        self.assertNotEqual(attrs['st_uid'], 0)

    def test_getattr_remote_only_directory(self):
        (self.backend2_dir / "subdir").mkdir()

        attrs = self.fs.getattr('/subdir')
        self.assertTrue(stat.S_ISDIR(attrs['st_mode']))

    def test_open_reads_remote_only_file_and_populates_state(self):
        content = "remote-data"
        (self.backend1_dir / "remote.txt").write_text(content)

        vfd = self.fs.open('/remote.txt', os.O_RDONLY)
        data = self.fs.read('/remote.txt', 1024, 0, vfd)
        self.fs.release('/remote.txt', vfd)

        self.assertEqual(data.decode(), content)
        meta = self.fs.state_manager.get_file_metadata('/remote.txt')
        self.assertIsNotNone(meta)
        locations = self.fs.state_manager.get_file_locations('/remote.txt')
        backend_names = {loc.backend_name for loc in locations}
        self.assertIn('backend1', backend_names)
        self.assertIn('cache', backend_names)

    def test_readdir_thread_safety(self):
        (self.backend1_dir / "threaded.txt").write_text("data")

        def worker():
            list(self.fs.readdir('/', None))

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def test_readdir_prefers_listdir_attrs_when_available(self):
        (self.backend1_dir / "attrs.txt").write_text("data")

        with patch.object(self.backends["backend1"], "listdir", side_effect=RuntimeError("listdir should not be used")):
            entries = set(self.fs.readdir('/', None))

        self.assertIn("attrs.txt", entries)

    def test_readdir_schedules_checksums_only_for_collisions(self):
        (self.backend1_dir / "left.txt").write_text("left")
        (self.backend2_dir / "right.txt").write_text("right")

        with patch.object(self.fs, "_schedule_checksum_task") as schedule_task:
            list(self.fs.readdir('/', None))
        schedule_task.assert_not_called()

        (self.backend2_dir / "left.txt").write_text("same-name")
        self.fs._invalidate_dir_cache_entry('/')
        with patch.object(self.fs, "_schedule_checksum_task") as schedule_task:
            list(self.fs.readdir('/', None))
        self.assertEqual(schedule_task.call_count, 2)

    def test_dir_cache_scoped_per_instance(self):
        # Populate cache in first instance
        (self.backend1_dir / "onlyfirst.txt").write_text("one")
        list(self.fs.readdir('/', None))

        # New instance with same cache path but different DB should not share dir_cache
        other_db = self.temp_dir / "other.db"
        other_fs = StorageFS(
            cache_root=self.cache_dir,
            state_db=other_db,
            backends=self.backends,
            cache_size=10 * 1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
        )
        self.assertEqual({}, other_fs._dir_cache)
        other_fs.state_manager.close()

    def test_mkdir_propagates_to_backends(self):
        self.fs.mkdir('/newdir', 0o755)

        self.assertTrue((self.cache_dir / 'newdir').is_dir())
        self.assertTrue(self._wait_for(lambda: self.backends['backend1'].isdir('/newdir')))
        self.assertTrue(self._wait_for(lambda: self.backends['backend2'].isdir('/newdir')))

    def test_rmdir_propagates_to_backends(self):
        path = '/todelete'
        self.fs.mkdir(path, 0o755)
        self.assertTrue((self.cache_dir / 'todelete').is_dir())

        self.fs.rmdir(path)

        self.assertFalse((self.cache_dir / 'todelete').exists())
        self.assertTrue(self._wait_for(lambda: not self.backends['backend1'].isdir(path)))
        self.assertTrue(self._wait_for(lambda: not self.backends['backend2'].isdir(path)))

    def test_rename_directory_syncs_backends_and_state(self):
        self.fs.mkdir('/dir', 0o755)
        vfd = self.fs.create('/dir/file.txt', 0o644)
        data = b'data'
        self.fs.write('/dir/file.txt', data, 0, vfd)
        self.fs.release('/dir/file.txt', vfd)

        self.assertTrue(self.fs.sync_manager.sync_now('/dir/file.txt', timeout=5.0))

        self.fs.rename('/dir', '/renamed')

        self.assertTrue((self.cache_dir / 'renamed').is_dir())
        self.assertFalse((self.cache_dir / 'dir').exists())

        self.assertTrue(self._wait_for(lambda: not self.backend1_dir.joinpath('dir').exists()))
        self.assertTrue(self._wait_for(lambda: self.backend1_dir.joinpath('renamed').is_dir()))
        self.assertTrue(self._wait_for(lambda: self.backend1_dir.joinpath('renamed', 'file.txt').exists()))

        old_meta = self.fs.state_manager.get_file_metadata('/dir/file.txt')
        new_meta = self.fs.state_manager.get_file_metadata('/renamed/file.txt')
        self.assertIsNone(old_meta)
        self.assertIsNotNone(new_meta)

    def test_mkdir_retry_targets_failed_backend(self):
        path = '/needretry'
        original = self.backends['backend2'].mkdir

        def failing_mkdir(_path):
            raise RuntimeError("backend2 offline")

        self.backends['backend2'].mkdir = failing_mkdir
        try:
            with patch.object(self.fs.sync_manager, 'queue_dir_sync') as mock_queue:
                self.fs.mkdir(path, 0o755)
            mock_queue.assert_called_with(
                path,
                DirOperation.MKDIR,
                mode=0o755,
                backend_hint='backend2',
                priority=80,
                force=True,
            )
        finally:
            self.backends['backend2'].mkdir = original

    def test_rmdir_retry_targets_failed_backend(self):
        path = '/deleteme'
        self.fs.mkdir(path, 0o755)
        original = self.backends['backend1'].rmdir

        def failing_rmdir(_path):
            raise RuntimeError("backend1 busy")

        self.backends['backend1'].rmdir = failing_rmdir
        try:
            with patch.object(self.fs.sync_manager, 'queue_dir_sync') as mock_queue:
                self.fs.rmdir(path)
            mock_queue.assert_called_with(
                path,
                DirOperation.RMDIR,
                backend_hint='backend1',
                priority=80,
                force=True,
            )
        finally:
            self.backends['backend1'].rmdir = original

    def test_rename_retry_targets_failed_backend(self):
        self.fs.mkdir('/orig', 0o755)
        target = '/renamed'
        original = self.backends['backend2'].rename

        def failing_rename(old, new):
            raise RuntimeError("backend2 down")

        self.backends['backend2'].rename = failing_rename
        try:
            with patch.object(self.fs.sync_manager, 'queue_dir_sync') as mock_queue:
                self.fs.rename('/orig', target)
            mock_queue.assert_called_with(
                '/orig',
                DirOperation.RENAME,
                new_path=target,
                backend_hint='backend2',
                priority=90,
                force=True,
            )
        finally:
            self.backends['backend2'].rename = original

    def test_mkdir_retry_eventually_replicates(self):
        path = '/asyncdir'
        backend2 = self.backends['backend2']
        call_count = {'n': 0}
        original = backend2.mkdir

        def flaky_mkdir(target):
            call_count['n'] += 1
            if call_count['n'] == 1:
                raise RuntimeError("transient failure")
            return original(target)

        backend2.mkdir = flaky_mkdir
        try:
            self.fs.mkdir(path, 0o755)
            # Should not exist on backend2 immediately
            self.assertFalse(backend2.isdir(path))

            def replicated():
                return backend2.isdir(path)

            self.assertTrue(self._wait_for(replicated, timeout=5.0))
        finally:
            backend2.mkdir = original


class TestStreamingReadPrefetch(unittest.TestCase):
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

        start = time.perf_counter()
        vfd = self.fs.open('/large.bin', os.O_RDONLY)
        elapsed = time.perf_counter() - start

        # Legacy full copy path sleeps for 0.35s; streaming open should return much sooner.
        self.assertLess(elapsed, 0.20)

        first = self.fs.read('/large.bin', 4096, 0, vfd)
        self.assertEqual(first, payload[:4096])

        cache_path = self.cache_dir / "large.bin"
        partial_path = self.cache_dir / "large.bin.storage.partial"
        self.assertTrue(self._wait_for(lambda: cache_path.exists() and not partial_path.exists(), timeout=8.0))
        self.assertEqual(cache_path.read_bytes(), payload)

        self.fs.release('/large.bin', vfd)

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
        self.assertTrue(self._wait_for(lambda: cache_path.exists(), timeout=8.0))

        try:
            final_raw = os.getxattr(str(cache_path), self.fs._STREAM_STATE_XATTR)
        except OSError as exc:
            if exc.errno in (getattr(errno, "EOPNOTSUPP", -1), getattr(errno, "ENOTSUP", -1), getattr(errno, "ENODATA", -1)):
                self.skipTest("xattrs unsupported in this environment")
            raise

        final_state = json.loads(final_raw.decode())
        self.assertEqual(final_state["status"], "complete")
        self.assertTrue(final_state["finalized"])

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


def _try_lock(lock_path_str, mountpoint, return_dict):
    lock_path = Path(lock_path_str)
    try:
        with _acquire_mount_lock(lock_path, mountpoint) as lf:
            return_dict['acquired'] = True
    except RuntimeError:
        return_dict['acquired'] = False


class TestMountLock(unittest.TestCase):
    def test_mount_lock_blocks_other_process(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / 'lock'

            # Acquire in parent
            lock_file = _acquire_mount_lock(lock_path, '/mnt/test')

            manager = mp.Manager()
            ret = manager.dict()
            p = mp.Process(target=_try_lock, args=(str(lock_path), '/mnt/test', ret))
            p.start()
            p.join(10)

            self.assertFalse(ret.get('acquired', True))

            # Release lock after verification
            lock_file.close()

    def test_mount_lock_allows_same_cache_different_mount(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_root = Path(tmpdir)
            lock_a = cache_root / '.storage.mount.lock'
            # direct calls with different mountpoints should not interfere when caller chooses different paths
            lf_a = _acquire_mount_lock(lock_a, '/mnt/a')
            # simulate mount_storage behavior by using a different lock path
            lock_b = cache_root / '.storage.mount.other.lock'
            lf_b = _acquire_mount_lock(lock_b, '/mnt/b')
            lf_a.close()
            lf_b.close()

    def test_stale_lock_file_is_cleaned(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / 'lock'

            # Create a stale pid file
            lock_path.write_text("999999")

            lock_file = _acquire_mount_lock(lock_path, '/mnt/test')
            # If we got here, stale lock was ignored and lock acquired
            lock_file.close()


if __name__ == '__main__':
    unittest.main()
