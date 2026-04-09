import shutil
import tempfile
import time
import unittest
from pathlib import Path

from storage.models import FileLocation, FileMetadata, SyncStatus
from storage.state import StateManager


class TestStateManagerRename(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "state.db"
        self.state_manager = StateManager(str(self.db_path))

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_rename_file_metadata(self):
        meta = FileMetadata(
            virtual_path="/old.txt",
            size=10,
            checksum="abc",
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.CLEAN,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        self.state_manager.set_file_metadata(meta)
        self.state_manager.rename_file("/old.txt", "/new.txt")
        self.assertIsNone(self.state_manager.get_file_metadata("/old.txt"))
        renamed = self.state_manager.get_file_metadata("/new.txt")
        self.assertIsNotNone(renamed)
        self.assertEqual(renamed.size, 10)

    def test_rename_path_recursive(self):
        child = FileMetadata(
            virtual_path="/dir/child.txt",
            size=1,
            checksum="x",
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.CLEAN,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        self.state_manager.set_file_metadata(child)
        self.state_manager.rename_path("/dir", "/renamed")
        self.assertIsNone(self.state_manager.get_file_metadata("/dir/child.txt"))
        self.assertIsNotNone(self.state_manager.get_file_metadata("/renamed/child.txt"))


class TestPathNormalization(unittest.TestCase):
    def test_normalize_empty_path(self):
        self.assertEqual(StateManager._normalize_virtual_path(""), "/")

    def test_normalize_root(self):
        self.assertEqual(StateManager._normalize_virtual_path("/"), "/")

    def test_normalize_adds_leading_slash(self):
        self.assertEqual(StateManager._normalize_virtual_path("foo/bar"), "/foo/bar")

    def test_normalize_removes_trailing_slash(self):
        self.assertEqual(StateManager._normalize_virtual_path("/foo/bar/"), "/foo/bar")

    def test_normalize_preserves_valid_path(self):
        self.assertEqual(StateManager._normalize_virtual_path("/foo/bar"), "/foo/bar")

class TestHierarchicalRename(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "state.db"
        self.state_manager = StateManager(str(self.db_path))

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_metadata(self, path: str):
        meta = FileMetadata(
            virtual_path=path,
            size=1,
            checksum="x",
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.CLEAN,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        self.state_manager.set_file_metadata(meta)

    def test_rename_path_single_file(self):
        self._create_metadata("/old.txt")
        renamed = self.state_manager.rename_path("/old.txt", "/new.txt")
        self.assertEqual(len(renamed), 1)
        self.assertIsNotNone(self.state_manager.get_file_metadata("/new.txt"))
        self.assertIsNone(self.state_manager.get_file_metadata("/old.txt"))

    def test_rename_path_directory_with_children(self):
        self._create_metadata("/dir/a.txt")
        self._create_metadata("/dir/b.txt")
        self._create_metadata("/dir/sub/c.txt")
        renamed = self.state_manager.rename_path("/dir", "/newdir")
        self.assertEqual(len(renamed), 3)
        self.assertIsNotNone(self.state_manager.get_file_metadata("/newdir/a.txt"))
        self.assertIsNotNone(self.state_manager.get_file_metadata("/newdir/b.txt"))
        self.assertIsNotNone(self.state_manager.get_file_metadata("/newdir/sub/c.txt"))

    def test_rename_path_same_path_noop(self):
        self._create_metadata("/file.txt")
        renamed = self.state_manager.rename_path("/file.txt", "/file.txt")
        self.assertEqual(len(renamed), 0)
        self.assertIsNotNone(self.state_manager.get_file_metadata("/file.txt"))

    def test_rename_path_updates_locations(self):
        self._create_metadata("/dir/file.txt")
        location = FileLocation(
            virtual_path="/dir/file.txt",
            backend_name="backend1",
            checksum="abc",
            last_synced=time.time(),
            is_cached=False,
        )
        self.state_manager.add_file_location(location)
        self.state_manager.rename_path("/dir", "/newdir")
        self.assertEqual(len(self.state_manager.get_file_locations("/dir/file.txt")), 0)
        new_locations = self.state_manager.get_file_locations("/newdir/file.txt")
        self.assertEqual(len(new_locations), 1)
        self.assertEqual(new_locations[0].backend_name, "backend1")

    def test_rename_path_normalizes_inputs(self):
        self._create_metadata("/dir/file.txt")
        self.state_manager.rename_path("dir/", "newdir")
        self.assertIsNotNone(self.state_manager.get_file_metadata("/newdir/file.txt"))



if __name__ == '__main__':
    unittest.main()
