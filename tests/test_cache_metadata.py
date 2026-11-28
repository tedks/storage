import errno
import json
import os
import shutil
import stat
import struct
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from storage.cache_metadata import (
    XATTR_CACHE_METADATA_NAME,
    cache_file_would_be_scanned,
    cache_path_to_virtual_path,
    MEMBER_ATTRS_XATTR,
    cached_member_attributes,
    copy_member_attributes,
    record_native_attributes,
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

    def test_import_preserves_existing_acl_group_mask(self):
        path = self.cache_root / "from-member"
        path.write_bytes(b"file")
        acl_xattr = "system.posix_acl_access"
        # Linux POSIX ACL xattr v2. A named user has rw behind a read-only
        # mask. Changing the group bits on the backing file would widen it.
        entries = (
            (0x01, 6, 0xffffffff),  # ACL_USER_OBJ
            (0x02, 6, os.geteuid()), # ACL_USER (mapped in the test sandbox)
            (0x04, 0, 0xffffffff),  # ACL_GROUP_OBJ
            (0x10, 4, 0xffffffff),  # ACL_MASK
            (0x20, 0, 0xffffffff),  # ACL_OTHER
        )
        acl = struct.pack("<I", 2) + b"".join(struct.pack("<HHI", *entry) for entry in entries)
        try:
            os.setxattr(path, acl_xattr, acl)
        except OSError as exc:
            if exc.errno == errno.ENOTSUP:
                self.skipTest("test filesystem does not support POSIX ACLs")
            raise

        source = SimpleNamespace(st_size=4, st_mode=stat.S_IFREG | 0o660)
        with path.open("rb") as handle:
            copy_member_attributes(handle.fileno(), source)

        self.assertEqual(acl, os.getxattr(path, acl_xattr))
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o640)
        self.assertEqual(cached_member_attributes(path, path.stat())["st_mode"], source.st_mode)

    def test_import_records_no_set_id_bits(self):
        path = self.cache_root / "from-member"
        path.write_bytes(b"file")
        os.chmod(path, 0o755)
        source = SimpleNamespace(st_size=4, st_mode=stat.S_IFREG | 0o7755)
        with path.open("rb") as handle:
            copy_member_attributes(handle.fileno(), source)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o755)
        # Sticky means nothing on a regular file and is kept as reported.
        self.assertEqual(cached_member_attributes(path, path.stat())["st_mode"],
                         stat.S_IFREG | 0o1755)
        # A record written with them (an earlier build) is read without them.
        record = json.loads(os.getxattr(path, MEMBER_ATTRS_XATTR))
        record["attrs"]["st_mode"] = stat.S_IFREG | 0o6755
        os.setxattr(path, MEMBER_ATTRS_XATTR, json.dumps(record).encode())
        self.assertEqual(cached_member_attributes(path, path.stat())["st_mode"],
                         stat.S_IFREG | 0o755)

    def test_agreeing_confirmation_keeps_the_probes_atime(self):
        from storage.cache_metadata import confirm_member_stat
        probe = SimpleNamespace(st_size=4, st_mode=stat.S_IFREG | 0o644, st_mtime=1_000_000_000,
                                st_atime=900_000_000, st_uid=1, st_gid=2)
        after = SimpleNamespace(st_size=4, st_mode=stat.S_IFREG | 0o644, st_mtime=1_000_000_000,
                                st_atime=1_700_000_000, st_uid=3, st_gid=4)
        backend = SimpleNamespace(stat=lambda path: after)
        confirmed, agreed = confirm_member_stat(backend, "/f", probe, 4)
        self.assertTrue(agreed)
        self.assertEqual((confirmed.st_atime, confirmed.st_uid, confirmed.st_gid),
                         (900_000_000, 3, 4))

    def test_native_record_is_empty_and_keeps_an_existing_one(self):
        path = self.cache_root / "made-here"
        path.write_bytes(b"file")
        fd = os.open(path, os.O_RDWR)
        try:
            record_native_attributes(fd)
            self.assertEqual(cached_member_attributes(path, path.stat()), {})
            source = SimpleNamespace(st_size=4, st_mode=stat.S_IFREG | 0o600, st_uid=4242)
            os.removexattr(path, MEMBER_ATTRS_XATTR)
            copy_member_attributes(fd, source)
            record_native_attributes(fd)  # O_TRUNC over an imported file
        finally:
            os.close(fd)
        self.assertEqual(cached_member_attributes(path, path.stat())["st_uid"], 4242)

    def test_scan_orders_a_fill_by_its_ctime_not_the_member_atime(self):
        path = self.cache_root / "filled"
        path.write_bytes(b"file")
        os.utime(path, (1_000_000_000, 1_000_000_000))  # the member's atime
        entry, = scan_cache_entries(self.cache_root)
        self.assertEqual(entry.stat_atime, path.stat().st_ctime)
        os.utime(path, (4_000_000_000, 1_000_000_000))  # read later
        entry, = scan_cache_entries(self.cache_root)
        self.assertEqual(entry.stat_atime, 4_000_000_000)

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


class TestCacheScansDoNotFollowLinks(unittest.TestCase):
    """A symlink in the cache tree is not a cached copy: scans skip it and
    nothing reads or writes metadata through it
    (ditz cache-scans-follow-symlinks)."""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_root = self.temp_dir / "cache"
        self.cache_root.mkdir()
        self.real = self.cache_root / "real.bin"
        self.real.write_bytes(b"real bytes")
        write_cache_metadata(self.real, self._metadata("/real.bin", self.real))
        self.link = self.cache_root / "link.bin"
        self.link.symlink_to("real.bin")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @staticmethod
    def _metadata(virtual_path, path):
        st = os.lstat(path)
        return FileMetadata(
            virtual_path=virtual_path, size=st.st_size, checksum=None,
            last_access=1.0, last_modified=st.st_mtime, sync_status=SyncStatus.CLEAN,
            replica_count=1, recomputable=False, recompute_cmd=None,
            encrypted=False, encryption_key_id=None,
        )

    def test_scan_skips_a_link(self):
        paths = [entry.virtual_path for entry in scan_cache_entries(self.cache_root)]
        self.assertEqual(paths, ["/real.bin"])

    def test_would_be_scanned_is_false_for_a_link(self):
        self.assertTrue(cache_file_would_be_scanned(self.cache_root.resolve(), "/real.bin"))
        self.assertFalse(cache_file_would_be_scanned(self.cache_root.resolve(), "/link.bin"))

    def test_write_refuses_a_link_and_leaves_the_target(self):
        with self.assertRaises(OSError) as raised:
            write_cache_metadata(self.link, self._metadata("/link.bin", self.link))
        self.assertEqual(raised.exception.errno, errno.EINVAL)
        self.assertEqual(read_cache_metadata(self.real).virtual_path, "/real.bin")

    def test_read_does_not_follow_a_link(self):
        self.assertIsNone(read_cache_metadata(self.link))

    def test_rebuild_index_does_not_rewrite_the_target_through_a_link(self):
        from storage.index_audit import rebuild_index_from_cache
        from storage.state import StateManager

        state = StateManager(str(self.temp_dir / "state.db"))
        try:
            report = rebuild_index_from_cache(self.cache_root, state)
            # The link comes back as a link row (ditz symlink-replication),
            # never as a file carrying the target's metadata.
            link_row = state.get_file_metadata("/link.bin")
            self.assertEqual(link_row.link_target, "real.bin")
            self.assertEqual(link_row.sync_status, SyncStatus.DIRTY)
            self.assertEqual(report.rebuilt_links, 1)
            self.assertIsNone(state.get_file_metadata("/real.bin").link_target)
            self.assertEqual(state.get_file_locations("/link.bin"), [])
        finally:
            state.close()
        self.assertEqual(read_cache_metadata(self.real).virtual_path, "/real.bin")
