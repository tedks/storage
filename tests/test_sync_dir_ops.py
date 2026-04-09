import shutil
import tempfile
import time
import unittest
from pathlib import Path

from storage.backends.local import LocalBackend
from storage.cache import CacheManager
from storage.replication import ReplicationManager
from storage.state import StateManager
from storage.sync import DirOperation, SyncManager


class TestDirSyncOperations(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend1_dir = Path(self.temp_dir) / "backend1"
        self.backend2_dir = Path(self.temp_dir) / "backend2"
        self.db_path = Path(self.temp_dir) / "state.db"

        self.cache_dir.mkdir()
        self.backend1_dir.mkdir()
        self.backend2_dir.mkdir()

        self.state_manager = StateManager(str(self.db_path))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 32 * 1024 * 1024)
        self.backends = {
            "backend1": LocalBackend("backend1", str(self.backend1_dir), speed_priority=0),
            "backend2": LocalBackend("backend2", str(self.backend2_dir), speed_priority=5),
        }
        self.replication_manager = ReplicationManager(self.state_manager, self.backends)
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            self.replication_manager,
            num_workers=1,
        )

    def tearDown(self):
        self.sync_manager.stop()
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _wait_for(self, predicate, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.05)
        return predicate()

    def test_mkdir_sync(self):
        self.sync_manager.start()
        test_dir = "/queued"
        self.sync_manager.queue_dir_sync(test_dir, DirOperation.MKDIR, priority=100)
        self.assertTrue(self._wait_for(lambda: (self.backend1_dir / 'queued').is_dir()))
        self.assertTrue(self._wait_for(lambda: (self.backend2_dir / 'queued').is_dir()))

    def test_rmdir_sync(self):
        self.sync_manager.start()
        test_dir = "/toremove"
        (self.backend1_dir / "toremove").mkdir()
        (self.backend2_dir / "toremove").mkdir()
        self.sync_manager.queue_dir_sync(test_dir, DirOperation.RMDIR, priority=100)
        self.assertTrue(self._wait_for(lambda: not (self.backend1_dir / 'toremove').exists()))
        self.assertTrue(self._wait_for(lambda: not (self.backend2_dir / 'toremove').exists()))

    def test_rename_file_sync(self):
        self.sync_manager.start()
        old_path = "/oldname.txt"
        new_path = "/newname.txt"
        for backend_dir in (self.backend1_dir, self.backend2_dir):
            fpath = backend_dir / old_path.lstrip('/')
            fpath.parent.mkdir(parents=True, exist_ok=True)
            fpath.write_text("payload")
        new_cache = self.cache_manager.get_cache_path(new_path)
        new_cache.parent.mkdir(parents=True, exist_ok=True)
        new_cache.write_text("payload")

        self.sync_manager.queue_dir_sync(old_path, DirOperation.RENAME, new_path=new_path, priority=100)

        self.assertTrue(self._wait_for(lambda: (self.backend1_dir / new_path.lstrip('/')).exists()))
        self.assertTrue(self._wait_for(lambda: (self.backend2_dir / new_path.lstrip('/')).exists()))
        self.assertTrue(self._wait_for(lambda: not (self.backend1_dir / old_path.lstrip('/')).exists()))



if __name__ == '__main__':
    unittest.main()
