import unittest
import tempfile
import shutil
import stat
from pathlib import Path
import multiprocessing as mp
import threading
import os

from storage.fs import StorageFS
from storage.fs import _acquire_mount_lock
from storage.backends.local import LocalBackend


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
