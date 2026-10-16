import os
import shutil
import stat
import tempfile
import unittest
from pathlib import Path

from storage.cache_metadata import (
    XATTR_CACHE_METADATA_NAME,
    cache_file_would_be_scanned,
    cache_path_to_virtual_path,
    read_cache_metadata,
    scan_cache_entries,
    write_cache_metadata,
)
from storage.models import FileMetadata, SyncStatus


class TestCacheMetadata(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_root = self.temp_dir / "cache"
        self.cache_root.mkdir()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_write_and_read_round_trip(self):
        cache_path = self.cache_root / "video.mp4"
        cache_path.write_bytes(b"abc123")
        metadata = FileMetadata(
            virtual_path="/video.mp4",
            size=6,
            checksum="sum-1",
            last_access=10.0,
            last_modified=cache_path.stat().st_mtime,
            sync_status=SyncStatus.CLEAN,
            replica_count=2,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )

        write_cache_metadata(cache_path, metadata)
        record = read_cache_metadata(cache_path)

        self.assertIsNotNone(record)
        self.assertEqual(record.virtual_path, "/video.mp4")
        self.assertEqual(record.size, 6)
        self.assertEqual(record.checksum, "sum-1")
        self.assertEqual(record.sync_status, SyncStatus.CLEAN)
        self.assertIn(XATTR_CACHE_METADATA_NAME, os.listxattr(str(cache_path)))

    def test_scan_reports_path_mismatch(self):
        nested = self.cache_root / "dir"
        nested.mkdir()
        cache_path = nested / "file.txt"
        cache_path.write_text("hello")
        metadata = FileMetadata(
            virtual_path="/wrong.txt",
            size=cache_path.stat().st_size,
            checksum=None,
            last_access=0.0,
            last_modified=cache_path.stat().st_mtime,
            sync_status=SyncStatus.DIRTY,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        write_cache_metadata(cache_path, metadata)

        entries = list(scan_cache_entries(self.cache_root))
        self.assertEqual(1, len(entries))
        self.assertEqual("/dir/file.txt", cache_path_to_virtual_path(self.cache_root, cache_path))
        self.assertTrue(any(issue.startswith("path_mismatch:") for issue in entries[0].issues))

    def test_scan_skips_internal_database_files(self):
        (self.cache_root / ".storage.db").write_bytes(b"sqlite")
        (self.cache_root / ".storage.db-wal").write_bytes(b"wal")
        (self.cache_root / ".storage.db-shm").write_bytes(b"shm")
        (self.cache_root / ".storage.db-journal").write_bytes(b"journal")

        cache_path = self.cache_root / "real.bin"
        cache_path.write_bytes(b"payload")
        metadata = FileMetadata(
            virtual_path="/real.bin",
            size=cache_path.stat().st_size,
            checksum="sum-2",
            last_access=0.0,
            last_modified=cache_path.stat().st_mtime,
            sync_status=SyncStatus.CLEAN,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        write_cache_metadata(cache_path, metadata)

        entries = list(scan_cache_entries(self.cache_root))
        self.assertEqual(["/real.bin"], [entry.virtual_path for entry in entries])

    def test_would_be_scanned_matches_scan_when_cache_root_is_unlistable(self):
        (self.cache_root / "top.bin").write_bytes(b"data")
        root = self.cache_root.resolve()
        self.assertTrue(cache_file_would_be_scanned(root, "/top.bin"))

        original_mode = stat.S_IMODE(root.stat().st_mode)
        # Searchable but not readable: a known name still lstats, but os.walk cannot list the root.
        os.chmod(root, 0o311)
        try:
            try:
                os.listdir(root)
            except PermissionError:
                pass
            else:
                # Root, CAP_DAC_READ_SEARCH, or a filesystem that ignores mode bits.
                self.skipTest("this process can list a directory without read permission")
            self.assertEqual([], [entry.virtual_path for entry in scan_cache_entries(root)])
            self.assertFalse(cache_file_would_be_scanned(root, "/top.bin"))
        finally:
            os.chmod(root, original_mode)


if __name__ == "__main__":
    unittest.main()


class TestStaleRecordDerivation(unittest.TestCase):
    """A record older than the file it describes derives DIRTY."""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_path = self.temp_dir / "f.txt"
        self.cache_path.write_bytes(b"v1")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _clean_record(self):
        st = self.cache_path.lstat()
        return write_cache_metadata(
            self.cache_path,
            FileMetadata(
                virtual_path="/f.txt", size=st.st_size, checksum="c1",
                last_access=1.0, last_modified=st.st_mtime,
                sync_status=SyncStatus.CLEAN, replica_count=1, recomputable=False,
                recompute_cmd=None, encrypted=False, encryption_key_id=None,
            ),
        )

    def test_record_matching_the_file_keeps_its_status_and_checksum(self):
        record = self._clean_record()
        st = self.cache_path.lstat()

        self.assertFalse(record.is_stale_for(st.st_size, st.st_mtime))
        derived = record.to_file_metadata(
            virtual_path="/f.txt", stat_size=st.st_size, stat_mtime=st.st_mtime, last_access=2.0,
        )
        self.assertEqual(derived.sync_status, SyncStatus.CLEAN)
        self.assertEqual(derived.checksum, "c1")

    def test_record_older_than_the_file_derives_dirty_and_keeps_the_synced_checksum(self):
        record = self._clean_record()
        # A write lands after the record, and its mirror never happens.
        self.cache_path.write_bytes(b"v2 is longer")
        st = self.cache_path.lstat()

        self.assertTrue(record.is_stale_for(st.st_size, st.st_mtime))
        derived = record.to_file_metadata(
            virtual_path="/f.txt", stat_size=st.st_size, stat_mtime=st.st_mtime, last_access=2.0,
        )
        self.assertEqual(derived.sync_status, SyncStatus.DIRTY)
        self.assertEqual(derived.checksum, "c1")  # what the members hold
        self.assertEqual(derived.size, st.st_size)

    def test_same_size_newer_mtime_is_stale(self):
        record = self._clean_record()
        os.utime(self.cache_path, (5_000_000, 5_000_000))
        st = self.cache_path.lstat()

        self.assertTrue(record.is_stale_for(st.st_size, st.st_mtime))

    def test_scan_reports_stale_clean_xattr(self):
        self._clean_record()
        self.cache_path.write_bytes(b"v2 is longer")

        [entry] = list(scan_cache_entries(self.temp_dir))

        kinds = {issue.partition(":")[0] for issue in entry.issues}
        self.assertIn("stale_clean_xattr", kinds)
        self.assertIn("size_mismatch", kinds)
        self.assertEqual(entry.build_file_metadata().sync_status, SyncStatus.DIRTY)
