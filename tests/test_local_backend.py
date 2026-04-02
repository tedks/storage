import shutil
import tempfile
import unittest
from pathlib import Path

from storage.backends.local import LocalBackend


class TestBackendRename(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.backend_root = Path(self.temp_dir) / "backend"
        self.backend_root.mkdir()
        self.backend = LocalBackend("test", str(self.backend_root))

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_file_rename(self):
        old_file = self.backend_root / "old.txt"
        old_file.write_text("data")
        self.backend.rename("/old.txt", "/new.txt")
        self.assertFalse(old_file.exists())
        self.assertEqual((self.backend_root / "new.txt").read_text(), "data")

    def test_directory_rename(self):
        old_dir = self.backend_root / "olddir"
        old_dir.mkdir()
        (old_dir / "file.txt").write_text("payload")
        self.backend.rename("/olddir", "/newdir")
        self.assertFalse(old_dir.exists())
        self.assertTrue((self.backend_root / "newdir" / "file.txt").exists())

    def test_creates_parent_directories(self):
        src = self.backend_root / "file.txt"
        src.write_text("payload")
        self.backend.rename("/file.txt", "/nested/dir/file.txt")
        self.assertFalse(src.exists())
        self.assertTrue((self.backend_root / "nested/dir/file.txt").exists())



if __name__ == '__main__':
    unittest.main()
