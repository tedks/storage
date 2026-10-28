"""Revalidation of cached files against their member on open.

Ruling on oq-watcher-outage-cache-policy: opening a CLEAN cached file whose
last check is older than revalidate_after stats the member holding it and
refetches the file when the member's copy changed. These tests drive
StorageFS directly (no mount) with local members, and age the in-memory
checks instead of sleeping.
"""

import dataclasses
import logging
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from storage.backends.local import LocalBackend
from storage.fs import StorageFS
from storage.models import SyncStatus


class RevalidateTestBase(unittest.TestCase):
    revalidate_after = 60.0

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_dir = self.temp_dir / "cache"
        self.cache_dir.mkdir()
        self.member_dir = self.temp_dir / "member"
        self.member_dir.mkdir()
        self.db_path = self.temp_dir / "state.db"
        self.member = LocalBackend("member", str(self.member_dir))
        self.fs = self._make_fs()

    def _make_fs(self, start_sync=False):
        return StorageFS(
            cache_root=self.cache_dir,
            state_db=self.db_path,
            backends={"member": self.member},
            cache_size=10 * 1024 * 1024,
            num_sync_workers=1,
            start_sync=start_sync,
            revalidate_after=self.revalidate_after,
        )

    def tearDown(self):
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    # helpers ----------------------------------------------------------
    def read_through(self, path, flags=os.O_RDONLY):
        fd = self.fs.open(path, flags)
        try:
            return self.fs.read(path, 1 << 20, 0, fd)
        finally:
            self.fs.release(path, fd)

    def age_checks(self, seconds=None):
        """Make every recorded check look `seconds` older (default: past the age)."""
        seconds = self.revalidate_after + 1 if seconds is None else seconds
        with self.fs._member_checks_lock:
            for path, check in list(self.fs._member_checks.items()):
                self.fs._member_checks[path] = dataclasses.replace(
                    check, verified_at=check.verified_at - seconds,
                )

    def rewrite_member(self, name, content, mtime=None):
        member_file = self.member_dir / name
        member_file.write_bytes(content)
        if mtime is not None:
            os.utime(member_file, (mtime, mtime))
        return member_file

    def cache_inode(self, name):
        return os.lstat(self.cache_dir / name).st_ino

    def count_stats(self):
        return patch.object(self.member, "stat", wraps=self.member.stat)


class TestRevalidateOnOpen(RevalidateTestBase):
    def setUp(self):
        super().setUp()
        self.member_file = self.rewrite_member("f.txt", b"original", mtime=1_700_000_000)
        self.assertEqual(b"original", self.read_through("/f.txt"))
        meta = self.fs.state_manager.get_file_metadata("/f.txt")
        self.assertEqual(SyncStatus.CLEAN, meta.sync_status)

    def test_within_age_makes_no_stat(self):
        self.rewrite_member("f.txt", b"changed-and-longer")
        with self.count_stats() as stat:
            self.assertEqual(b"original", self.read_through("/f.txt"))
        stat.assert_not_called()

    def test_unchanged_member_keeps_cached_copy_with_one_stat(self):
        inode = self.cache_inode("f.txt")
        self.age_checks()
        with self.count_stats() as stat:
            self.assertEqual(b"original", self.read_through("/f.txt"))
            # The check was refreshed: a second open within the age is free.
            self.assertEqual(b"original", self.read_through("/f.txt"))
        self.assertEqual(1, stat.call_count)
        self.assertEqual(inode, self.cache_inode("f.txt"))

    def test_size_change_refetches(self):
        self.rewrite_member("f.txt", b"changed-and-longer", mtime=1_700_000_000)
        self.age_checks()
        with self.count_stats() as stat:
            self.assertEqual(b"changed-and-longer", self.read_through("/f.txt"))
        self.assertEqual(1, stat.call_count)
        self.assertEqual(b"changed-and-longer", (self.cache_dir / "f.txt").read_bytes())
        meta = self.fs.state_manager.get_file_metadata("/f.txt")
        self.assertEqual(SyncStatus.CLEAN, meta.sync_status)
        self.assertEqual(len(b"changed-and-longer"), meta.size)

    def test_mtime_only_change_refetches(self):
        # Same size, later mtime: no baseline yet, so the heuristic applies.
        self.rewrite_member("f.txt", b"ORIGINAL", mtime=1_700_000_100)
        self.age_checks()
        self.assertEqual(b"ORIGINAL", self.read_through("/f.txt"))

    def test_mtime_only_change_against_baseline_refetches(self):
        # First stale open records the member's stat as the baseline.
        self.age_checks()
        self.assertEqual(b"original", self.read_through("/f.txt"))
        check = self.fs._member_checks["/f.txt"]
        self.assertEqual("member", check.backend_name)
        # Same size, mtime moved backwards: only the baseline can see this.
        self.rewrite_member("f.txt", b"ORIGINAL", mtime=1_600_000_000)
        self.age_checks()
        self.assertEqual(b"ORIGINAL", self.read_through("/f.txt"))

    def test_zero_age_checks_every_open(self):
        self.fs._revalidate_after = 0.0
        with self.count_stats() as stat:
            self.read_through("/f.txt")
            self.read_through("/f.txt")
        self.assertEqual(2, stat.call_count)

    def test_unavailable_member_serves_cached_copy_logs_once_no_stat(self):
        self.rewrite_member("f.txt", b"changed-and-longer")
        self.member.set_available(False)
        self.age_checks()
        with self.count_stats() as stat, self.assertLogs("storage.fs", logging.WARNING) as logs:
            self.assertEqual(b"original", self.read_through("/f.txt"))
            self.assertEqual(b"original", self.read_through("/f.txt"))
        stat.assert_not_called()
        self.assertEqual(1, sum("Cannot revalidate" in line for line in logs.output))
        # Back: the next open revalidates and picks up the change.
        self.member.set_available(True)
        self.assertEqual(b"changed-and-longer", self.read_through("/f.txt"))

    def test_failed_stat_serves_cached_copy_logs_once(self):
        self.rewrite_member("f.txt", b"changed-and-longer")
        self.age_checks()
        with patch.object(self.member, "stat", side_effect=OSError(5, "I/O error")) as stat, \
                self.assertLogs("storage.fs", logging.WARNING) as logs:
            self.assertEqual(b"original", self.read_through("/f.txt"))
            # The failed attempt counts as a check: no stat until the age passes.
            self.assertEqual(b"original", self.read_through("/f.txt"))
            self.assertEqual(1, stat.call_count)
            self.age_checks()
            self.assertEqual(b"original", self.read_through("/f.txt"))
            self.assertEqual(2, stat.call_count)
        self.assertEqual(1, sum("Cannot revalidate" in line for line in logs.output))

    def test_member_without_the_file_keeps_cached_copy(self):
        self.member_file.unlink()
        self.age_checks()
        self.assertEqual(b"original", self.read_through("/f.txt"))
        self.assertTrue((self.cache_dir / "f.txt").exists())

    def test_dirty_file_is_never_discarded(self):
        fd = self.fs.open("/f.txt", os.O_RDWR)
        self.fs.write("/f.txt", b"LOCAL", 0, fd)
        self.fs.release("/f.txt", fd)
        meta = self.fs.state_manager.get_file_metadata("/f.txt")
        self.assertEqual(SyncStatus.DIRTY, meta.sync_status)
        self.rewrite_member("f.txt", b"changed-and-longer")
        self.age_checks()
        with self.count_stats() as stat:
            self.assertEqual(b"LOCALnal", self.read_through("/f.txt"))
        stat.assert_not_called()

    def test_open_writer_keeps_its_copy(self):
        writer = self.fs.open("/f.txt", os.O_RDWR)
        try:
            self.rewrite_member("f.txt", b"changed-and-longer")
            self.age_checks()
            # The writer's descriptor pins the copy: _invalidate_file_cache
            # leaves it, and the open is served from it rather than failing.
            self.assertEqual(b"original", self.read_through("/f.txt"))
            self.assertTrue((self.cache_dir / "f.txt").exists())
        finally:
            self.fs.release("/f.txt", writer)

    def test_open_reader_keeps_reading_old_copy_after_refetch(self):
        reader = self.fs.open("/f.txt", os.O_RDONLY)
        try:
            self.rewrite_member("f.txt", b"changed-and-longer")
            self.age_checks()
            self.assertEqual(b"changed-and-longer", self.read_through("/f.txt"))
            self.assertEqual(b"original", self.fs.read("/f.txt", 100, 0, reader))
        finally:
            self.fs.release("/f.txt", reader)

    def test_restart_checks_at_first_open(self):
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        self.rewrite_member("f.txt", b"changed-and-longer")
        self.fs = self._make_fs()
        self.assertEqual({}, self.fs._member_checks)
        self.assertEqual(b"changed-and-longer", self.read_through("/f.txt"))

    def test_eviction_forgets_the_check(self):
        self.assertIn("/f.txt", self.fs._member_checks)
        self.fs.cache_manager._evict_if_still_clean("/f.txt", self.cache_dir / "f.txt")
        self.assertFalse((self.cache_dir / "f.txt").exists())
        self.assertNotIn("/f.txt", self.fs._member_checks)


class TestRevalidateAfterSync(RevalidateTestBase):
    def test_sync_records_member_baseline(self):
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        self.fs = self._make_fs(start_sync=True)
        fd = self.fs.create("/new.txt", 0o644)
        self.fs.write("/new.txt", b"written here", 0, fd)
        self.fs.release("/new.txt", fd)
        self.assertTrue(self.fs.sync_manager.sync_now("/new.txt", timeout=10))
        check = self.fs._member_checks["/new.txt"]
        self.assertEqual("member", check.backend_name)
        self.assertEqual(len(b"written here"), check.member_size)
        # Stamp the member with a later mtime, as an upload that does not
        # keep it (SFTP) would, and the baseline to match: the next stale
        # open must not mistake the upload for a change on the member.
        later = os.lstat(self.member_dir / "new.txt").st_mtime + 100
        os.utime(self.member_dir / "new.txt", (later, later))
        with self.fs._member_checks_lock:
            self.fs._member_checks["/new.txt"] = dataclasses.replace(check, member_mtime=later)
        inode = self.cache_inode("new.txt")
        self.age_checks()
        self.assertEqual(b"written here", self.read_through("/new.txt"))
        self.assertEqual(inode, self.cache_inode("new.txt"))


if __name__ == "__main__":
    unittest.main()
