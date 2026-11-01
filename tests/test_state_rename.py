import errno
import shutil
import sqlite3
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




class TestRenameOntoAndAround(unittest.TestCase):
    """rename_path over an existing destination, and names with LIKE wildcards
    (ditz rename-over-existing-file)."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.state_manager = StateManager(str(Path(self.temp_dir) / "state.db"))

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def add(self, path, size, backend="m"):
        self.state_manager.set_file_metadata(FileMetadata(
            virtual_path=path, size=size, checksum=f"c{size}", last_access=time.time(),
            last_modified=time.time(), sync_status=SyncStatus.CLEAN, replica_count=1,
            recomputable=False, recompute_cmd=None, encrypted=False, encryption_key_id=None,
        ))
        self.state_manager.add_file_location(FileLocation(
            virtual_path=path, backend_name=backend, checksum=f"c{size}",
            last_synced=time.time(), is_cached=False,
        ))

    def test_rename_replaces_the_destination_rows(self):
        self.add("/src", 1, backend="a")
        self.add("/dst", 2, backend="b")
        self.assertEqual([("/src", "/dst")], self.state_manager.rename_path("/src", "/dst"))
        self.assertIsNone(self.state_manager.get_file_metadata("/src"))
        self.assertEqual(1, self.state_manager.get_file_metadata("/dst").size)
        self.assertEqual(["a"], [l.backend_name for l in self.state_manager.get_file_locations("/dst")])

    def test_like_wildcards_in_names_match_only_themselves(self):
        self.add("/a_b/f", 1)
        self.add("/axb/f", 2)
        self.add("/a%/g", 3)
        self.add("/aZ/g", 4)
        self.assertEqual([("/a_b/f", "/c/f")], self.state_manager.rename_path("/a_b", "/c"))
        self.assertEqual(2, self.state_manager.get_file_metadata("/axb/f").size)
        self.assertEqual([("/a%/g", "/d/g")], self.state_manager.rename_path("/a%", "/d"))
        self.assertEqual(4, self.state_manager.get_file_metadata("/aZ/g").size)

    def test_rename_onto_a_directory_with_rows_below_it_is_refused(self):
        # Its files were evicted, so the cache directory is empty and the
        # cache move succeeds; their rows are the only record of them.
        self.add("/src/f", 1)
        self.add("/dst/g", 2)
        with self.assertRaises(OSError) as raised:
            self.state_manager.rename_path("/src", "/dst")
        self.assertEqual(errno.ENOTEMPTY, raised.exception.errno)
        self.assertEqual(1, self.state_manager.get_file_metadata("/src/f").size)
        self.assertEqual(2, self.state_manager.get_file_metadata("/dst/g").size)
        self.assertTrue(self.state_manager.has_rows_below("/dst", excluding="/src"))
        self.assertFalse(self.state_manager.has_rows_below("/dst/g", excluding="/src"))

    def test_rename_onto_an_ancestor_counts_only_rows_outside_the_source(self):
        self.add("/a/b/f", 1)
        self.assertFalse(self.state_manager.has_rows_below("/a", excluding="/a/b"))
        self.add("/a/g", 2)
        self.assertTrue(self.state_manager.has_rows_below("/a", excluding="/a/b"))

    def test_rename_of_a_directory_onto_an_empty_directory(self):
        self.add("/src/f", 1, backend="a")
        # A location row with no files row would collide with the moved one.
        self.state_manager.add_file_location(FileLocation(
            virtual_path="/dst/f", backend_name="a", checksum="stale",
            last_synced=time.time(), is_cached=False,
        ))
        self.assertEqual([("/src/f", "/dst/f")], self.state_manager.rename_path("/src", "/dst"))
        self.assertEqual(["c1"], [l.checksum for l in self.state_manager.get_file_locations("/dst/f")])

    def test_root_is_not_a_rename_endpoint(self):
        self.add("/a/f", 1)
        self.add("/b", 2)
        for old, new in (("/a", "/"), ("/", "/a"), ("/a", "")):
            with self.assertRaises(ValueError):
                self.state_manager.rename_path(old, new)
        self.assertEqual(1, self.state_manager.get_file_metadata("/a/f").size)
        self.assertEqual(2, self.state_manager.get_file_metadata("/b").size)

    def test_a_source_without_rows_still_replaces_the_destination_rows(self):
        # The cache move already replaced the destination (an unindexed
        # source, such as an empty directory), so its rows are stale.
        self.add("/dst", 2)
        self.assertEqual([], self.state_manager.rename_path("/src", "/dst"))
        self.assertIsNone(self.state_manager.get_file_metadata("/dst"))
        self.assertEqual([], self.state_manager.get_file_locations("/dst"))

    def test_a_failed_rename_keeps_the_destination_rows(self):
        self.add("/src", 1, backend="a")
        self.add("/dst", 2, backend="b")
        real = self.state_manager.conn

        class FailingMove:
            def __getattr__(self, name):
                return getattr(real, name)

            def execute(self, sql, *args):
                if sql.startswith("UPDATE files SET virtual_path"):
                    raise sqlite3.OperationalError("injected")
                return real.execute(sql, *args)

        self.state_manager.conn = FailingMove()
        try:
            with self.assertRaises(sqlite3.OperationalError):
                self.state_manager.rename_path("/src", "/dst")
        finally:
            self.state_manager.conn = real
        self.assertEqual(2, self.state_manager.get_file_metadata("/dst").size)
        self.assertEqual(["b"], [l.backend_name for l in self.state_manager.get_file_locations("/dst")])
        self.assertEqual(1, self.state_manager.get_file_metadata("/src").size)


if __name__ == '__main__':
    unittest.main()
