import os
import shutil
import stat
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


class TestCopyKeepsInternalXattrsOut(unittest.TestCase):
    """The mount's own user.storage.* state stays on cache files (ditz
    local-backend-internal-xattrs); user xattrs and the user.storage.config
    policy travel, as do the mode and times."""

    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.member_dir = self.root / "member"
        self.member_dir.mkdir()
        self.backend = LocalBackend("member", str(self.member_dir))

    def make_source(self, path):
        path.write_bytes(b"payload")
        os.chmod(path, 0o640)
        for name, value in (
            ("user.storage.metadata", b"{}"),
            ("user.storage.member_stat", b"{}"),
            ("user.storage.stream", b"{}"),
            ("user.storage.config", b"replica_count: 2"),
            ("user.tag", b"kept"),
        ):
            os.setxattr(path, name, value)
        os.utime(path, ns=(1_000_000_000_000_000_000, 1_000_000_000_123_456_789))
        return path

    def assert_copied(self, dst, mode=0o640):
        st = os.stat(dst)  # before the read, which may move atime
        self.assertEqual(1_000_000_000_123_456_789, st.st_mtime_ns)
        self.assertEqual(mode, stat.S_IMODE(st.st_mode))
        self.assertEqual(b"payload", dst.read_bytes())
        self.assertEqual(["user.storage.config", "user.tag"], self.user_xattrs(dst))
        self.assertEqual(b"replica_count: 2", os.getxattr(dst, "user.storage.config"))

    @staticmethod
    def user_xattrs(path):
        # Other namespaces (SELinux labels, default ACLs) depend on the host.
        return sorted(n for n in os.listxattr(path) if n.startswith("user."))

    def test_upload_leaves_internal_xattrs_behind(self):
        src = self.make_source(self.root / "cache-file")
        self.backend.copy_from_local(src, "/d/f")
        self.assert_copied(self.member_dir / "d" / "f")

    def test_upload_over_a_leaked_copy_removes_its_internal_xattrs(self):
        dst = self.member_dir / "f"
        dst.write_bytes(b"old")
        os.setxattr(dst, "user.storage.metadata", b"stale")
        src = self.make_source(self.root / "cache-file")
        self.backend.copy_from_local(src, "/f")
        self.assert_copied(dst)

    def test_fetch_ignores_internal_xattrs_on_the_member(self):
        self.make_source(self.member_dir / "f")
        local = self.root / "cache" / "f"
        self.backend.copy_to_local("/f", local)
        self.assert_copied(local)

    def test_a_read_only_source_keeps_its_xattrs(self):
        # Setting a user xattr needs write permission: copy before the chmod.
        src = self.make_source(self.root / "cache-file")
        os.chmod(src, 0o444)
        self.addCleanup(os.chmod, src, 0o644)
        self.backend.copy_from_local(src, "/ro")
        dst = self.member_dir / "ro"
        self.addCleanup(os.chmod, dst, 0o644)
        self.assert_copied(dst, mode=0o444)

    def test_upload_drops_user_xattrs_the_source_no_longer_has(self):
        src = self.make_source(self.root / "cache-file")
        self.backend.copy_from_local(src, "/f")
        os.removexattr(src, "user.storage.config")
        os.removexattr(src, "user.tag")
        self.backend.copy_from_local(src, "/f")
        self.assertEqual([], self.user_xattrs(self.member_dir / "f"))


if __name__ == "__main__":
    unittest.main()
