import multiprocessing as mp
import tempfile
import unittest
from pathlib import Path

from storage.fs import _acquire_mount_lock


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
