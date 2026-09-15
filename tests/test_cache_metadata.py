import os
import shutil
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

    @unittest.skipIf(os.geteuid() == 0, "root lists unreadable directories anyway")
    def test_would_be_scanned_matches_scan_when_cache_root_is_unlistable(self):
        (self.cache_root / "top.bin").write_bytes(b"data")
        root = self.cache_root.resolve()
        self.assertTrue(cache_file_would_be_scanned(root, "/top.bin"))

        # Execute-only: a known name still lstats, but os.walk cannot list the root.
        os.chmod(root, 0o311)
        try:
            self.assertEqual([], [entry.virtual_path for entry in scan_cache_entries(root)])
            self.assertFalse(cache_file_would_be_scanned(root, "/top.bin"))
        finally:
            os.chmod(root, 0o755)


if __name__ == "__main__":
    unittest.main()
