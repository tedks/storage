"""Revalidation of cached files against their member on open.

Ruling on oq-watcher-outage-cache-policy: opening a CLEAN cached file whose
last check is older than revalidate_after stats the member holding it and
refetches the file when the member's copy changed. These tests drive
StorageFS directly (no mount) with local members, and age the in-memory
checks instead of sleeping.
"""

import dataclasses
import errno
import logging
import stat
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fuse import FuseOSError

from storage.backends.local import LocalBackend
from storage.backends.rsync import RsyncBackend, RsyncError
from storage.cache_metadata import MEMBER_ATTRS_XATTR, read_cache_metadata, regular_file_stat
from storage.checksums import fast_checksum_file
from storage.fs import StorageFS
from storage.models import FileMetadata, SyncStatus
from storage.sync_executor import SyncExecutor
from storage.sync_tasks import SyncConfig, SyncTask


class NoMtimeBackend(LocalBackend):
    """A local member whose transfers stamp the current time, as SFTP get/put do."""

    def copy_to_local(self, virtual_path, local_path):
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self._resolve_path(virtual_path), local_path)

    def copy_from_local(self, local_path, virtual_path):
        real_path = self._resolve_path(virtual_path)
        real_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, real_path)


class LocalRsyncTransport(RsyncBackend):
    """Use production rsync upload with local-only metadata transport."""

    def __init__(self, name, root_path):
        super().__init__(name, "localhost", root_path, "user", compress=False)

    def _with_sftp(self, operation, path, fn, retry=False):
        return None  # parent already exists in this local fixture

    def _remote_spec(self, virtual_path):
        return str(self._resolve_path(virtual_path))

    def is_available(self):
        return True

    def exists_or_raise(self, virtual_path):
        return Path(self._resolve_path(virtual_path)).exists()

    def stat(self, virtual_path):
        st = os.stat(self._resolve_path(virtual_path))
        # SFTP stat does not publish ctime, as on the deployed rsync member.
        return SimpleNamespace(st_mode=st.st_mode, st_size=st.st_size,
                               st_mtime=st.st_mtime)

    def get_fast_checksum(self, virtual_path, allow_download=False):
        return fast_checksum_file(self._resolve_path(virtual_path))


class LegacyLocalRsyncTransport(LocalRsyncTransport):
    """Control: old upload selection, still using the real rsync subprocess."""

    def copy_from_local(self, local_path, virtual_path):
        self._run_rsync(str(local_path), self._remote_spec(virtual_path))


class TestLocalRsyncSyncSettlement(unittest.TestCase):
    """Mount handlers, real sync settlement/callback, and local rsync upload."""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_dir = self.temp_dir / "cache"
        self.cache_dir.mkdir()
        self.member_dir = self.temp_dir / "member"
        self.member_dir.mkdir()
        self.member = LocalRsyncTransport("member", str(self.member_dir))
        self.fs = StorageFS(
            cache_root=self.cache_dir, state_db=self.temp_dir / "state.db",
            backends={"member": self.member}, cache_size=10 * 1024 * 1024,
            start_sync=False,
        )
        self.executor = SyncExecutor(
            worker_id=0, state_manager=self.fs.state_manager,
            cache_manager=self.fs.cache_manager,
            replication_manager=self.fs.replication_manager,
            config=SyncConfig(), member_written_cb=self.fs._record_member_written,
        )

    def tearDown(self):
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        self.member.close()
        shutil.rmtree(self.temp_dir)

    def _write_through_fs(self, content, previous):
        member_path = self.member_dir / "file.txt"
        member_path.write_bytes(previous)
        stamp = 1_700_000_000
        os.utime(member_path, (stamp, stamp))
        fd = self.fs.create("/file.txt", 0o644)
        self.fs.write("/file.txt", content, 0, fd)
        self.fs.release("/file.txt", fd)
        self.fs.utimens("/file.txt", (stamp, stamp))
        cache_path = self.cache_dir / "file.txt"
        self.assertEqual(SyncStatus.DIRTY,
                         self.fs.state_manager.get_file_metadata("/file.txt").sync_status)
        return cache_path, member_path

    def _sync(self, backend=None):
        backend = backend or self.member
        with patch.object(self.fs.replication_manager, "select_backends_for_file",
                          return_value=[backend]):
            self.executor.process_file_task(SyncTask("/file.txt"))
        return self.fs.state_manager.get_file_metadata("/file.txt")

    def _reverify(self, backend, persisted):
        result = self.fs._reverify_member_copy(backend, "/file.txt", persisted.basis)
        self.assertIsNotNone(result)
        with self.assertLogs("storage.fs", level="INFO") as logged:
            with self.fs._checksum_lock:
                self.fs._settle_reverify(("/file.txt", "member"), persisted, result)
        self.assertTrue(any("re-verification found a copy" in line for line in logged.output))
        return result[0]

    def test_changed_size_upload_settles_clean_with_measured_member_bytes(self):
        cache_path, member_path = self._write_through_fs(b"new longer bytes", b"old bytes")
        metadata = self._sync()
        self.assertEqual(SyncStatus.CLEAN, metadata.sync_status)
        self.assertEqual(cache_path.read_bytes(), member_path.read_bytes())
        record = self.fs.state_manager.get_member_checksum("/file.txt", "member")
        self.assertEqual(fast_checksum_file(member_path), record.checksum)

    def test_same_size_mtime_upload_does_not_falsely_settle_clean(self):
        cache_path, member_path = self._write_through_fs(b"new bytes", b"old bytes")
        self.assertEqual(cache_path.stat().st_size, member_path.stat().st_size)
        self.assertEqual(cache_path.stat().st_mtime_ns, member_path.stat().st_mtime_ns)
        self.assertNotEqual(cache_path.read_bytes(), member_path.read_bytes())
        metadata = self._sync()
        self.assertEqual(SyncStatus.CLEAN, metadata.sync_status)
        self.assertEqual(cache_path.read_bytes(), member_path.read_bytes())
        record = self.fs.state_manager.get_member_checksum("/file.txt", "member")
        self.assertEqual(fast_checksum_file(member_path), record.checksum)

    def test_legacy_skip_reports_clean_then_reverify_without_remote_rewrite(self):
        cache_path, member_path = self._write_through_fs(b"new bytes", b"old bytes")
        legacy = LegacyLocalRsyncTransport("member", str(self.member_dir))
        try:
            metadata = self._sync(legacy)
            self.assertEqual(SyncStatus.CLEAN, metadata.sync_status)
            self.assertEqual(b"old bytes", member_path.read_bytes())
            persisted = self.fs.state_manager.get_member_checksum("/file.txt", "member")
            self.assertEqual(fast_checksum_file(cache_path), persisted.checksum)
            self.assertNotEqual(fast_checksum_file(member_path), persisted.checksum)
            measured = self._reverify(legacy, persisted)
            self.assertEqual(fast_checksum_file(member_path), measured)
        finally:
            legacy.close()

    def test_genuine_same_basis_member_rewrite_is_detected(self):
        cache_path, member_path = self._write_through_fs(b"new bytes", b"old bytes")
        self.assertEqual(SyncStatus.CLEAN, self._sync().sync_status)
        persisted = self.fs.state_manager.get_member_checksum("/file.txt", "member")
        self.assertIsNotNone(persisted)
        self.assertEqual(cache_path.read_bytes(), member_path.read_bytes())
        self.assertEqual(fast_checksum_file(member_path), persisted.checksum)
        member_path.write_bytes(b"bad bytes")
        os.utime(member_path, (persisted.basis[1], persisted.basis[1]))
        measured = self._reverify(self.member, persisted)
        self.assertNotEqual(persisted.checksum, measured)
        self.assertEqual(measured,
                         self.fs.state_manager.get_member_checksum("/file.txt", "member").checksum)

    def test_failed_rsync_does_not_settle_clean(self):
        self._write_through_fs(b"new bytes", b"old bytes")
        with patch.object(self.member, "_run_rsync", side_effect=RsyncError("failed", 23)):
            metadata = self._sync()
        self.assertEqual(SyncStatus.ERROR, metadata.sync_status)
        self.assertIsNone(self.fs.state_manager.get_member_checksum("/file.txt", "member"))


class RevalidateTestBase(unittest.TestCase):
    revalidate_after = 60.0
    backend_class = LocalBackend

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_dir = self.temp_dir / "cache"
        self.cache_dir.mkdir()
        self.member_dir = self.temp_dir / "member"
        self.member_dir.mkdir()
        self.db_path = self.temp_dir / "state.db"
        self.member = self.backend_class("member", str(self.member_dir))
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

    def count_fetches(self):
        return patch.object(self.member, "copy_to_local", wraps=self.member.copy_to_local)

    def restart(self, start_sync=False):
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        self.fs = self._make_fs(start_sync=start_sync)
        self.assertEqual({}, self.fs._member_checks)


class TestMemberAttributes(RevalidateTestBase):
    backend_class = NoMtimeBackend

    def test_cached_stat_keeps_member_fields_and_reports_missing_fields(self):
        source = self.rewrite_member("f.txt", b"original")
        mtime_ns, atime_ns = 1_000_000_000_123_456_789, 1_000_000_001_234_567_890
        fake = SimpleNamespace(
            st_size=8, st_mode=stat.S_IFREG | 0o4640, st_uid=12345, st_gid=12346,
            st_mtime_ns=mtime_ns, st_atime_ns=atime_ns, st_ctime=1_100_000_000,
        )
        with patch.object(self.member, "stat", return_value=fake):
            self.assertTrue(self.fs._ensure_file_in_cache(self.fs._resolve_path("/f.txt")))

        cached = self.cache_dir / "f.txt"
        local = cached.stat()
        attrs = self.fs.getattr("/f.txt")
        self.assertEqual((local.st_mtime_ns, local.st_atime_ns), (mtime_ns, atime_ns))
        self.assertEqual((stat.S_IMODE(local.st_mode), local.st_uid, local.st_gid),
                         (0o640, os.geteuid(), os.getegid()))
        # Every member field but the setuid bit, which is never reported.
        self.assertEqual((attrs["st_mode"], attrs["st_uid"], attrs["st_gid"]),
                         (stat.S_IFREG | 0o640, fake.st_uid, fake.st_gid))
        self.assertEqual(attrs["st_ctime"], fake.st_ctime)
        self.assertEqual(attrs["st_nlink"], local.st_nlink)
        self.assertEqual(source.read_bytes(), cached.read_bytes())

        # Setting exactly the current physical mode or owner retires the
        # overlay too. chown -1 still preserves the other reported field.
        self.fs.chmod("/f.txt", 0o640)
        self.fs.chown("/f.txt", os.geteuid(), -1)
        attrs = self.fs.getattr("/f.txt")
        self.assertEqual(attrs["st_mode"], stat.S_IFREG | 0o640)
        self.assertEqual((attrs["st_uid"], attrs["st_gid"]), (os.geteuid(), fake.st_gid))
        self.assertEqual(attrs["st_ctime"], cached.stat().st_ctime)
        self.fs.chown("/f.txt", -1, os.getegid())
        self.assertEqual(self.fs.getattr("/f.txt")["st_gid"], os.getegid())

    def test_incomplete_member_stat_reports_native_fallbacks(self):
        self.rewrite_member("f.txt", b"original")
        incomplete = SimpleNamespace(
            st_mode=None, st_uid=None, st_gid=None, st_atime=None,
            st_size=8, st_mtime=1_000_000_000,
        )
        with patch.object(self.member, "lstat", return_value=incomplete), \
                patch.object(self.member, "stat", return_value=incomplete):
            cold = self.fs.getattr("/f.txt")
            self.assertEqual(cold["st_uid"], os.getuid())
            self.assertEqual(cold["st_gid"], os.getgid())
            self.assertEqual(cold["st_atime"], cold["st_mtime"])
            self.assertEqual(cold["st_mode"], stat.S_IFREG | 0o644)
            self.assertEqual(self.read_through("/f.txt"), b"original")

        st = (self.cache_dir / "f.txt").stat()
        warm = self.fs.getattr("/f.txt")
        # No member mode: the native mode, read-only until a chmod.
        self.assertEqual(warm["st_mode"], st.st_mode & ~0o222)
        self.assertEqual((warm["st_uid"], warm["st_gid"]), (st.st_uid, st.st_gid))
        self.assertEqual(warm["st_mtime"], incomplete.st_mtime)

    def test_utimens_overrides_source_ctime_on_atime_only_change(self):
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        fake = SimpleNamespace(st_size=8, st_mtime=1_000_000_000, st_atime=1_000_000_000)
        with patch.object(self.member, "stat", return_value=fake):
            self.read_through("/f.txt")

        self.assertEqual(self.fs.getattr("/f.txt")["st_ctime"], fake.st_mtime)
        self.fs.utimens("/f.txt", (1_200_000_000, fake.st_mtime))
        attrs = self.fs.getattr("/f.txt")
        self.assertEqual(attrs["st_atime"], 1_200_000_000)
        self.assertEqual(attrs["st_mtime"], fake.st_mtime)
        self.assertEqual(attrs["st_ctime"], (self.cache_dir / "f.txt").stat().st_ctime)
        self.assertEqual(self.fs.state_manager.get_file_metadata("/f.txt").sync_status,
                         SyncStatus.CLEAN)

    def test_full_fetch_with_stat_unavailable_or_size_missing(self):
        for reason in (NotImplementedError(), OSError(95, "no stat"), None):
            with self.subTest(reason=reason):
                src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
                with patch.object(self.member, "stat", side_effect=reason,
                                  return_value=SimpleNamespace()), \
                        patch.object(self.member, "get_size", return_value=src.stat().st_size):
                    self.assertEqual(self.read_through("/f.txt"), b"original")
                cache = self.cache_dir / "f.txt"
                attrs = self.fs.getattr("/f.txt")
                self.assertEqual(attrs["st_mtime"], cache.stat().st_mtime)
                self.assertEqual(attrs["st_uid"], cache.stat().st_uid)
                self.assertEqual(self.fs.state_manager.get_file_metadata("/f.txt").sync_status,
                                 SyncStatus.CLEAN)
                self.assertGreater(self.fs.cache_manager._evict_if_still_clean("/f.txt", cache), 0)

    def test_full_fetch_that_loses_install_keeps_winners_baseline(self):
        self.rewrite_member("f.txt", b"started first", mtime=1_000_000_000)
        cached = self.cache_dir / "f.txt"
        copy_to_local = self.member.copy_to_local
        winning_time = 1_700_000_000

        def copy_with_concurrent_install(path, temp):
            copy_to_local(path, temp)
            cached.write_bytes(b"winning stream")
            os.utime(cached, (winning_time, winning_time))
            st = cached.stat()
            self.fs.cache_manager.store_file_metadata(FileMetadata(
                virtual_path=path, size=st.st_size, checksum=None, last_access=0,
                last_modified=st.st_mtime, sync_status=SyncStatus.CLEAN,
                replica_count=1, recomputable=False, recompute_cmd=None,
                encrypted=False, encryption_key_id=None,
            ), cache_path=cached)
            self.fs._store_member_check(path, cached, self.fs._cache_identity(st),
                                        {"member": (st.st_size, winning_time)}, replace=True)

        with patch.object(self.member, "copy_to_local", side_effect=copy_with_concurrent_install):
            self.assertEqual(self.read_through("/f.txt"), b"winning stream")

        local = cached.stat()
        self.assertEqual(local.st_mtime, winning_time)
        identity = self.fs._cache_identity(local)
        self.assertEqual(self.fs._load_persisted_members(cached, identity),
                         {"member": (local.st_size, winning_time)})
        self.assertEqual(read_cache_metadata(cached).last_modified, winning_time)

    def make_legacy_file(self):
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        self.read_through("/f.txt")
        cached = self.cache_dir / "f.txt"
        os.removexattr(cached, MEMBER_ATTRS_XATTR)
        os.utime(cached, (1_700_000_000, 1_700_000_000))
        local = cached.stat()
        row = self.fs.state_manager.get_file_metadata("/f.txt")
        self.assertTrue(self.fs.state_manager.record_cached_stat(
            "/f.txt", row.dirty_generation, size=local.st_size, last_modified=local.st_mtime,
        ))
        self.fs.cache_manager.sync_cache_metadata(
            "/f.txt", self.fs.state_manager.get_file_metadata("/f.txt"),
        )
        self.fs._store_member_check("/f.txt", cached, self.fs._cache_identity(local),
                                    {"member": (8, 1_000_000_000)}, replace=True)
        return cached

    def test_legacy_reporting_does_not_touch_clean_inode_or_index(self):
        cached = self.make_legacy_file()
        native = cached.stat()
        record = read_cache_metadata(cached)
        self.assertEqual(self.fs.getattr("/f.txt")["st_mtime"], 1_000_000_000)
        self.assertEqual(native, cached.stat())
        self.assertEqual(record, read_cache_metadata(cached))
        self.assertEqual(self.read_through("/f.txt"), b"original")
        self.assertEqual(self.fs.state_manager.get_file_metadata("/f.txt").sync_status,
                         SyncStatus.CLEAN)

        # Explicitly setting the existing native time takes precedence.
        self.fs.utimens("/f.txt", (native.st_atime, native.st_mtime))
        self.assertEqual(self.fs.getattr("/f.txt")["st_mtime"], native.st_mtime)

    def test_legacy_reporting_skips_unproven_timestamps(self):
        for reason in ("missing", "stale inode", "dirty", "ambiguous", "size", "upload"):
            with self.subTest(reason=reason):
                cached = self.make_legacy_file()
                native = cached.stat()
                identity = self.fs._cache_identity(native)
                if reason == "missing":
                    os.removexattr(cached, StorageFS.MEMBER_STAT_XATTR)
                elif reason == "stale inode":
                    self.fs._store_member_check("/f.txt", cached, (0, *identity[1:]),
                                                {"member": (8, 1_000_000_000)}, replace=True)
                elif reason == "dirty":
                    self.fs.cache_manager.mark_file_dirty("/f.txt", cached)
                elif reason == "ambiguous":
                    self.fs._store_member_check("/f.txt", cached, identity,
                                                {"other": (8, 900_000_000)})
                elif reason == "size":
                    self.fs._store_member_check("/f.txt", cached, identity,
                                                {"member": (9, 1_000_000_000)}, replace=True)
                elif reason == "upload":
                    row = self.fs.state_manager.get_file_metadata("/f.txt")
                    self.fs.state_manager.publish_sync_status(
                        "/f.txt", row.dirty_generation, SyncStatus.CLEAN, checksum="uploaded",
                    )
                self.assertEqual(self.fs.getattr("/f.txt")["st_mtime"], native.st_mtime)
                self.fs.cache_manager.delete_cached_file("/f.txt")
                self.fs.state_manager.delete_file("/f.txt")


class TestImportedPermissions(RevalidateTestBase):
    """What a fill imports never grants more through the mount than the
    member file had: no set-ID bits, no write to a read-only file."""

    def fill_read_only(self, member_stat):
        src = self.member_dir / "f.txt"
        if src.exists():
            src.unlink()
        src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        os.chmod(src, 0o444)
        with patch.object(self.member, "stat", **member_stat):
            self.assertEqual(self.read_through("/f.txt"), b"original")
        cached = self.cache_dir / "f.txt"
        # The daemon keeps owner write on its backing file.
        self.assertTrue(cached.stat().st_mode & stat.S_IWUSR)
        return cached

    def assert_write_refused(self, cached):
        self.assertEqual(stat.S_IMODE(self.fs.getattr("/f.txt")["st_mode"]), 0o444)
        for attempt in (lambda: self.fs.open("/f.txt", os.O_WRONLY),
                        lambda: self.fs.truncate("/f.txt", 0),
                        lambda: self.fs.create("/f.txt", 0o644),
                        lambda: self.fs.setxattr("/f.txt", "user.note", b"x", 0),
                        lambda: self.fs.removexattr("/f.txt", "user.note")):
            with self.assertRaises(FuseOSError) as raised:
                attempt()
            self.assertEqual(raised.exception.errno, errno.EACCES)
        self.assertEqual(cached.read_bytes(), b"original")

    def test_read_only_member_mode_is_enforced(self):
        self.assert_write_refused(self.fill_read_only({"wraps": self.member.stat}))

    def test_read_only_without_member_stat_fails_closed(self):
        # No member mode: the downloaded file's own (copied) mode is
        # recorded instead of the backing file's owner-writable one.
        for kwargs in ({"side_effect": OSError(errno.EIO, "no stat")},
                       {"return_value": SimpleNamespace(st_size=None)}):
            with self.subTest(kwargs=kwargs):
                with patch.object(self.member, "get_size", return_value=8):
                    cached = self.fill_read_only(kwargs)
                self.assert_write_refused(cached)
                self.fs.cache_manager.delete_cached_file("/f.txt")
                self.fs.state_manager.delete_file("/f.txt")

    def test_set_id_bits_are_never_reported(self):
        src = self.rewrite_member("f.txt", b"original")
        os.chmod(src, 0o6755)
        self.assertEqual(stat.S_IMODE(self.fs.getattr("/f.txt")["st_mode"]), 0o755)  # uncached
        self.read_through("/f.txt")
        cached = self.cache_dir / "f.txt"
        self.assertEqual(stat.S_IMODE(cached.stat().st_mode), 0o755)
        self.assertEqual(stat.S_IMODE(self.fs.getattr("/f.txt")["st_mode"]), 0o755)
        # chown keeps the member mode, still without them; a chmod through
        # the mount cannot add them to what is reported either.
        self.fs.chown("/f.txt", -1, os.getegid())
        self.assertEqual(stat.S_IMODE(self.fs.getattr("/f.txt")["st_mode"]), 0o755)
        self.fs.chmod("/f.txt", 0o6750)
        self.assertEqual(stat.S_IMODE(self.fs.getattr("/f.txt")["st_mode"]), 0o750)

    def test_record_matches_after_device_number_changes(self):
        self.fill_read_only({"wraps": self.member.stat})
        cached = self.cache_dir / "f.txt"
        real = os.lstat
        remounted = lambda path, *a, **kw: _with_dev(real(path, *a, **kw), 12345)
        with patch("storage.namespace.os.lstat", side_effect=remounted),                 patch("storage.cache_metadata.os.lstat", side_effect=remounted):
            self.assertEqual(stat.S_IMODE(self.fs.getattr("/f.txt")["st_mode"]), 0o444)
        self.assertTrue(cached.exists())

    def make_copied_record(self):
        """A read-only fill whose cache file then moved to another inode
        with its xattrs (a cache migrated with cp -a)."""
        cached = self.fill_read_only({"wraps": self.member.stat})
        copy = cached.with_name("copy")
        shutil.copy2(cached, copy)
        for name in os.listxattr(cached):
            os.setxattr(copy, name, os.getxattr(cached, name))
        os.replace(copy, cached)
        return cached

    def test_copied_record_still_refuses_write(self):
        self.make_copied_record()
        with self.assertRaises(FuseOSError) as raised:
            self.fs.open("/f.txt", os.O_WRONLY)
        self.assertEqual(raised.exception.errno, errno.EACCES)
        # stat agrees with what is enforced.
        self.assertEqual(stat.S_IMODE(self.fs.getattr("/f.txt")["st_mode"]), 0o444)

    def test_chmod_clears_a_copied_records_refusal(self):
        for mode, chown_first in ((0o644, False), (0o666, False), (0o644, True)):
            with self.subTest(mode=oct(mode), chown_first=chown_first):
                cached = self.make_copied_record()
                if chown_first:
                    # Rebinding for another field keeps the refusal.
                    self.fs.chown("/f.txt", -1, os.getegid())
                    self.assertEqual(stat.S_IMODE(self.fs.getattr("/f.txt")["st_mode"]), 0o444)
                self.fs.chmod("/f.txt", mode)
                self.assertEqual(stat.S_IMODE(self.fs.getattr("/f.txt")["st_mode"]),
                                 stat.S_IMODE(cached.stat().st_mode))
                self.assertTrue(self.fs.getattr("/f.txt")["st_mode"] & stat.S_IWUSR)
                fh = self.fs.open("/f.txt", os.O_WRONLY)
                self.fs.write("/f.txt", b"O", 0, fh)
                self.fs.release("/f.txt", fh)
                self.assertEqual(cached.read_bytes(), b"Original")
                self.fs.cache_manager.delete_cached_file("/f.txt")
                self.fs.state_manager.delete_file("/f.txt")


def _with_field(st, index, value):
    fields = list(st)
    fields[index] = value
    return os.stat_result(fields, {name: getattr(st, name) for name in (
        "st_atime", "st_mtime", "st_ctime", "st_atime_ns", "st_mtime_ns", "st_ctime_ns",
        "st_blksize", "st_blocks")})


def _with_dev(st, dev):
    return _with_field(st, 2, dev)  # st_dev


def _with_ino(st, ino):
    return _with_field(st, 1, ino)  # st_ino


class TestCreatedAndChangedFiles(RevalidateTestBase):

    def test_created_file_stat_reads_no_index_row(self):
        fh = self.fs.create("/new.txt", 0o644)
        self.fs.write("/new.txt", b"made here", 0, fh)
        self.fs.release("/new.txt", fh)
        with patch.object(self.fs.state_manager, "get_file_metadata",
                          wraps=self.fs.state_manager.get_file_metadata) as rows:
            attrs = self.fs.getattr("/new.txt")
        self.assertEqual(rows.call_count, 0)
        self.assertEqual(attrs["st_size"], 9)
        self.assertEqual(attrs["st_mode"], (self.cache_dir / "new.txt").stat().st_mode)

    def test_record_update_failure_after_chmod_is_logged(self):
        self.rewrite_member("f.txt", b"original")
        self.read_through("/f.txt")
        with patch("storage.fs.forget_cached_member_fields",
                   side_effect=OSError(errno.ENOSPC, "full")), \
                self.assertLogs("storage.fs", level="WARNING"):
            self.fs.chmod("/f.txt", 0o600)
            self.fs.chown("/f.txt", -1, os.getegid())
            self.fs.utimens("/f.txt", (1_200_000_000, 1_200_000_000))
        self.assertEqual(stat.S_IMODE((self.cache_dir / "f.txt").stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.fs.getattr("/f.txt")["st_mode"]), 0o600)

    def test_explicit_utime_beats_legacy_mtime_on_read_only_backing_file(self):
        # A legacy fill copied a read-only mode onto its backing file; an
        # explicit utime that keeps the native mtime still retires recovery.
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        self.read_through("/f.txt")
        cached = self.cache_dir / "f.txt"
        os.removexattr(cached, MEMBER_ATTRS_XATTR)
        os.utime(cached, (1_700_000_000, 1_700_000_000))
        local = cached.stat()
        row = self.fs.state_manager.get_file_metadata("/f.txt")
        self.assertTrue(self.fs.state_manager.record_cached_stat(
            "/f.txt", row.dirty_generation, size=local.st_size, last_modified=local.st_mtime,
        ))
        self.fs._store_member_check("/f.txt", cached, self.fs._cache_identity(local),
                                    {"member": (8, 1_000_000_000)}, replace=True)
        os.chmod(cached, 0o444)
        self.assertEqual(self.fs.getattr("/f.txt")["st_mtime"], 1_000_000_000)
        self.fs.utimens("/f.txt", (local.st_atime, local.st_mtime))
        self.assertEqual(self.fs.getattr("/f.txt")["st_mtime"], local.st_mtime)
        self.assertEqual(stat.S_IMODE(cached.stat().st_mode), 0o444)


class TestFillConfirmsMemberVersion(RevalidateTestBase):
    """A fill records the member's newest genuine attributes, and fetches
    again when they show the member changed after its probe."""

    def rewrite_during_first_copy(self, change):
        copy_to_local = self.member.copy_to_local
        calls = []

        def copy(path, temp):
            if not calls:
                change()  # after the probe, before the transfer reads it
            calls.append(path)
            copy_to_local(path, temp)

        return patch.object(self.member, "copy_to_local", side_effect=copy), calls

    def test_member_rewritten_at_the_same_size_after_its_probe(self):
        src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        os.chmod(src, 0o644)

        def change():
            src.write_bytes(b"newbytes")
            os.utime(src, (1_100_000_000, 1_100_000_000))

        patcher, calls = self.rewrite_during_first_copy(change)
        with patcher:
            self.assertEqual(self.read_through("/f.txt"), b"newbytes")
        self.assertEqual(len(calls), 2)  # fetched again against the newer stat
        attrs = self.fs.getattr("/f.txt")
        self.assertEqual(attrs["st_mtime"], 1_100_000_000)
        self.assertEqual(stat.S_IMODE(attrs["st_mode"]), 0o644)

    def test_mode_only_change_after_the_probe_is_a_disagreement(self):
        src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        os.chmod(src, 0o644)
        patcher, calls = self.rewrite_during_first_copy(lambda: os.chmod(src, 0o444))
        with patcher:
            self.assertEqual(self.read_through("/f.txt"), b"original")
        self.assertEqual(len(calls), 2)
        self.assertEqual(stat.S_IMODE(self.fs.getattr("/f.txt")["st_mode"]), 0o444)

    def test_confirming_stat_failures_fall_back_to_the_probe(self):
        for failures in (1, 2):
            with self.subTest(failures=failures):
                src = self.member_dir / "f.txt"
                src.unlink(missing_ok=True)
                src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
                os.chmod(src, 0o644)
                real_stat, seen = self.member.stat, []

                def flaky(path):
                    seen.append(path)
                    # The first call is the probe; the next ones confirm.
                    if 1 < len(seen) <= 1 + failures:
                        raise OSError(errno.EIO, "stat failed")
                    return real_stat(path)

                with patch.object(self.member, "stat", side_effect=flaky):
                    self.assertEqual(self.read_through("/f.txt"), b"original")
                attrs = self.fs.getattr("/f.txt")
                self.assertEqual(stat.S_IMODE(attrs["st_mode"]), 0o644)  # still writable
                self.assertEqual(attrs["st_mtime"], 1_000_000_000)
                self.fs.release("/f.txt", self.fs.open("/f.txt", os.O_WRONLY))
                self.fs.cache_manager.delete_cached_file("/f.txt")
                self.fs.state_manager.delete_file("/f.txt")

    def test_second_transfer_is_the_baseline(self):
        src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)

        def change():
            src.write_bytes(b"newbytes")
            os.utime(src, (1_100_000_000, 1_100_000_000))

        patcher, calls = self.rewrite_during_first_copy(change)
        with patcher:
            self.read_through("/f.txt")
        self.assertEqual(self.fs._member_checks["/f.txt"].members, {"member": (8, 1_100_000_000)})
        self.age_checks()
        with self.count_fetches() as fetches:
            self.assertEqual(self.read_through("/f.txt"), b"newbytes")
        self.assertEqual(fetches.call_count, 0)  # the baseline matched: no needless refetch

    def test_failed_confirmations_with_another_size_keep_no_member_attributes(self):
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        probe = SimpleNamespace(st_size=99, st_mode=stat.S_IFREG | 0o644, st_mtime=1_200_000_000)
        seen = []

        def stat_once(path):
            seen.append(path)
            if len(seen) == 1:
                return probe
            raise OSError(errno.EIO, "stat failed")

        with patch.object(self.member, "stat", side_effect=stat_once), \
                patch.object(self.member, "get_size", return_value=99):
            self.assertEqual(self.read_through("/f.txt"), b"original")
        attrs = self.fs.getattr("/f.txt")
        self.assertNotEqual(attrs["st_mtime"], 1_200_000_000)
        self.assertEqual(attrs["st_mode"] & 0o222, 0)  # fails closed

    def test_transfer_of_another_size_than_the_stat_is_fetched_again(self):
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        fake = SimpleNamespace(st_size=99, st_mode=stat.S_IFREG | 0o600, st_mtime=1_200_000_000)
        with patch.object(self.member, "stat", return_value=fake), \
                patch.object(self.member, "get_size", return_value=99), \
                self.count_fetches() as fetches:
            self.assertEqual(self.read_through("/f.txt"), b"original")
        self.assertEqual(fetches.call_count, 2)
        # Still the member's own (reported) attributes, never local ones.
        attrs = self.fs.getattr("/f.txt")
        self.assertEqual((attrs["st_mtime"], stat.S_IMODE(attrs["st_mode"])), (1_200_000_000, 0o600))

    def test_unchanged_member_keeps_its_attributes(self):
        src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        os.chmod(src, 0o640)
        with self.count_fetches() as fetches:
            self.assertEqual(self.read_through("/f.txt"), b"original")
        self.assertEqual(fetches.call_count, 1)
        attrs = self.fs.getattr("/f.txt")
        self.assertEqual((attrs["st_mtime"], stat.S_IMODE(attrs["st_mode"])), (1_000_000_000, 0o640))


class TestXattrChangesHoldThePathLock(RevalidateTestBase):

    def test_check_and_change_run_under_the_path_lock(self):
        self.rewrite_member("f.txt", b"original")
        self.read_through("/f.txt")
        held = []
        real_set, real_remove = os.setxattr, os.removexattr

        def observed(real):
            def call(path, *args, **kwargs):
                if str(path).endswith("f.txt"):
                    held.append("/f.txt" in self.fs._path_locks._entries)
                return real(path, *args, **kwargs)
            return call

        with patch("storage.fs.os.setxattr", side_effect=observed(real_set)), \
                patch("storage.fs.os.removexattr", side_effect=observed(real_remove)):
            self.fs.setxattr("/f.txt", "user.note", b"x", 0)
            self.fs.removexattr("/f.txt", "user.note")
        self.assertEqual(held, [True, True])


class TestConflictViewClose(RevalidateTestBase):
    """A conflict view's close sends its copy to the member when the copy
    changed while the descriptor was open, whatever its open flags."""

    def setUp(self):
        super().setUp()
        self.member_file = self.rewrite_member("f.txt", b"member bytes")
        self.view = self.fs.cache_manager.get_cache_path("/f.txt.member")
        self.view.write_bytes(b"member bytes")
        patcher = patch.object(self.member, "copy_from_local", wraps=self.member.copy_from_local)
        self.copies = patcher.start()
        self.addCleanup(patcher.stop)

    def test_path_truncate_under_a_read_only_handle_is_sent_at_close(self):
        fh = self.fs.open("/f.txt.member", os.O_RDONLY)
        self.fs.truncate("/f.txt.member", 6)
        self.assertEqual(self.member_file.read_bytes(), b"member bytes")  # not in the callback
        self.fs.release("/f.txt.member", fh)
        self.assertEqual(self.member_file.read_bytes(), b"member")
        self.assertEqual(self.copies.call_count, 1)

    def test_unchanged_read_only_close_keeps_a_newer_member_edit(self):
        fh = self.fs.open("/f.txt.member", os.O_RDONLY)
        self.fs.truncate("/f.txt.member", len(b"member bytes"))  # no-op length
        self.member_file.write_bytes(b"newer edit")
        self.fs.release("/f.txt.member", fh)
        self.assertEqual(self.member_file.read_bytes(), b"newer edit")
        self.assertEqual(self.copies.call_count, 0)

    def test_unwritten_writable_close_sends_nothing(self):
        fh = self.fs.open("/f.txt.member", os.O_RDWR)
        self.member_file.write_bytes(b"newer edit")
        self.fs.flush("/f.txt.member", fh)
        self.fs.release("/f.txt.member", fh)
        self.assertEqual(self.member_file.read_bytes(), b"newer edit")
        self.assertEqual(self.copies.call_count, 0)

    def test_write_in_the_same_timestamp_tick_is_sent(self):
        fh = self.fs.open("/f.txt.member", os.O_WRONLY)
        st = self.view.stat()
        self.fs.write("/f.txt.member", b"MEMBER", 0, fh)
        # Same size, and the mtime of the open: only the recorded write shows it.
        os.utime(self.view, ns=(st.st_atime_ns, st.st_mtime_ns))
        self.fs.release("/f.txt.member", fh)
        self.assertEqual(self.member_file.read_bytes(), b"MEMBER bytes")
        self.assertEqual(self.copies.call_count, 1)

    def test_timestamp_change_alone_is_not_sent(self):
        fh = self.fs.open("/f.txt.member", os.O_RDONLY)
        self.fs.utimens("/f.txt.member", (1_200_000_000, 1_200_000_000))
        self.fs.chmod("/f.txt.member", 0o600)
        self.member_file.write_bytes(b"newer edit")
        self.fs.release("/f.txt.member", fh)
        self.assertEqual(self.member_file.read_bytes(), b"newer edit")
        self.assertEqual(self.copies.call_count, 0)

    def test_a_change_is_sent_by_the_first_close_only(self):
        first = self.fs.open("/f.txt.member", os.O_RDONLY)
        second = self.fs.open("/f.txt.member", os.O_RDONLY)
        self.fs.truncate("/f.txt.member", 6)
        self.fs.release("/f.txt.member", first)
        self.assertEqual(self.member_file.read_bytes(), b"member")
        self.member_file.write_bytes(b"newer edit")
        self.fs.release("/f.txt.member", second)
        self.assertEqual(self.member_file.read_bytes(), b"newer edit")
        self.assertEqual(self.copies.call_count, 1)

    def test_a_standalone_truncate_is_sent_by_a_later_close(self):
        self.fs.truncate("/f.txt.member", 6)
        self.assertEqual(self.member_file.read_bytes(), b"member bytes")
        fh = self.fs.open("/f.txt.member", os.O_RDWR)
        self.fs.release("/f.txt.member", fh)
        self.assertEqual(self.member_file.read_bytes(), b"member")
        self.assertEqual(self.copies.call_count, 1)

    def test_a_failed_send_stays_pending(self):
        fh = self.fs.open("/f.txt.member", os.O_WRONLY)
        self.fs.write("/f.txt.member", b"MEMBER", 0, fh)
        self.copies.side_effect = OSError(errno.EIO, "member down")
        with self.assertLogs("storage.fs", level="ERROR"):
            self.fs.release("/f.txt.member", fh)
        self.assertEqual(self.member_file.read_bytes(), b"member bytes")
        self.copies.side_effect = None
        self.fs.release("/f.txt.member", self.fs.open("/f.txt.member", os.O_RDONLY))
        self.assertEqual(self.member_file.read_bytes(), b"MEMBER bytes")

    def test_a_change_recorded_during_a_send_stays_pending(self):
        fh = self.fs.open("/f.txt.member", os.O_WRONLY)
        self.fs.write("/f.txt.member", b"MEMBER", 0, fh)
        real = type(self.member).copy_from_local.__get__(self.member)
        ino = self.view.stat().st_ino

        def racing(local, virtual):
            real(local, virtual)
            self.view.write_bytes(b"later bytes!")  # lands after the copy
            self.fs._mark_view_changed(self.view, ino)

        self.copies.side_effect = racing
        self.fs.release("/f.txt.member", fh)
        self.assertEqual(self.member_file.read_bytes(), b"MEMBER bytes")
        self.copies.side_effect = None
        self.fs.release("/f.txt.member", self.fs.open("/f.txt.member", os.O_RDONLY))
        self.assertEqual(self.member_file.read_bytes(), b"later bytes!")
        self.fs.release("/f.txt.member", self.fs.open("/f.txt.member", os.O_RDONLY))
        self.assertEqual(self.copies.call_count, 2)

    def test_unlink_retires_a_pending_change(self):
        self.fs.truncate("/f.txt.member", 6)  # pending, with no descriptor
        self.assertTrue(self.fs._view_pending)
        with patch.object(self.member, "delete_file"):
            self.fs.unlink("/f.txt.member")
        self.assertEqual(self.fs._view_pending, {})
        # A later copy at the name (a refetch) owes nothing, and invalidates.
        self.view.write_bytes(b"member bytes")
        self.fs.release("/f.txt.member", self.fs.open("/f.txt.member", os.O_RDONLY))
        self.assertEqual(self.copies.call_count, 0)
        self.fs._on_remote_change("/f.txt")
        self.assertFalse(self.view.exists())

    def test_a_reused_inode_number_inherits_nothing(self):
        # A copy put at the name right after the unlink reports the removed
        # inode's number: the removed key goes all the same.
        self.fs.truncate("/f.txt.member", 6)
        ((path, ino),) = self.fs._view_pending
        real_unlink = self.fs.namespace.unlink
        real_stat = regular_file_stat

        def unlink_then_refill(name):
            real_unlink(name)
            self.view.write_bytes(b"member bytes")

        def reused(target):
            st = real_stat(target)
            if st is not None and str(target) == path:
                return _with_ino(st, ino)
            return st

        with patch.object(self.fs.namespace, "unlink", side_effect=unlink_then_refill), \
                patch("storage.fs.regular_file_stat", side_effect=reused), \
                patch.object(self.member, "delete_file"):
            self.fs.unlink("/f.txt.member")
        self.assertEqual(self.fs._view_pending, {})

    def test_a_rename_failing_after_the_local_move_retires_the_target(self):
        # The local move replaced the view's copy, then the rename failed
        # (a member step): the replaced inode's pending change still goes.
        self.fs.truncate("/f.txt.member", 6)
        self.assertTrue(self.fs._view_pending)

        def moved_then_failed(old, new, on_local_moved=None):
            replacement = self.view.with_name("moved-in")
            replacement.write_bytes(b"moved bytes!")
            os.replace(replacement, self.view)
            raise OSError(errno.EIO, "member rename failed")

        with patch.object(self.fs.namespace, "rename", side_effect=moved_then_failed), \
                self.assertRaises(OSError):
            self.fs._rename("/elsewhere.txt", "/f.txt.member")
        self.assertEqual(self.fs._view_pending, {})

    def test_a_rename_failing_before_the_local_move_keeps_the_target(self):
        self.fs.truncate("/f.txt.member", 6)
        pending = dict(self.fs._view_pending)
        with patch.object(self.fs.namespace, "rename",
                          side_effect=OSError(errno.EIO, "refused")), \
                self.assertRaises(OSError):
            self.fs._rename("/elsewhere.txt", "/f.txt.member")
        self.assertEqual(self.fs._view_pending, pending)

    def test_a_replaced_name_under_an_open_descriptor_sends_nothing(self):
        fh = self.fs.open("/f.txt.member", os.O_RDONLY)
        self.fs.truncate("/f.txt.member", 6)
        replacement = self.view.with_name("replacement")
        replacement.write_bytes(b"other bytes!")
        os.replace(replacement, self.view)
        with self.assertLogs("storage.fs", level="WARNING"):
            self.fs.release("/f.txt.member", fh)
        self.assertEqual(self.copies.call_count, 0)
        self.assertEqual(self.member_file.read_bytes(), b"member bytes")
        self.assertEqual(self.fs._view_pending, {})

    def test_bookkeeping_failure_after_a_send_does_not_resend(self):
        fh = self.fs.open("/f.txt.member", os.O_WRONLY)
        self.fs.write("/f.txt.member", b"MEMBER", 0, fh)
        with patch.object(self.fs, "_schedule_checksum_task", side_effect=RuntimeError("queue")), \
                self.assertLogs("storage.fs", level="ERROR"):
            self.fs.release("/f.txt.member", fh)
        self.assertEqual(self.member_file.read_bytes(), b"MEMBER bytes")
        self.member_file.write_bytes(b"newer edit")
        self.fs.release("/f.txt.member", self.fs.open("/f.txt.member", os.O_RDONLY))
        self.assertEqual(self.member_file.read_bytes(), b"newer edit")
        self.assertEqual(self.copies.call_count, 1)

    def test_a_view_of_a_member_no_longer_configured_clears_its_change(self):
        fh = self.fs.open("/f.txt.member", os.O_WRONLY)
        self.fs.write("/f.txt.member", b"MEMBER", 0, fh)
        member = self.fs.backends.pop("member")
        self.addCleanup(self.fs.backends.__setitem__, "member", member)
        with self.assertLogs("storage.fs", level="ERROR"):
            self.fs.release("/f.txt.member", fh)
        self.assertEqual(self.fs._view_pending, {})

    def test_path_truncate_without_a_handle_stays_in_the_cached_copy(self):
        # As on master: nothing closes, so nothing is sent (a known gap).
        self.fs.truncate("/f.txt.member", 0)
        self.assertEqual(self.view.read_bytes(), b"")
        self.assertEqual(self.member_file.read_bytes(), b"member bytes")
        self.assertEqual(self.copies.call_count, 0)


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
        # The check's stat is also the refetch's baseline; one more confirms
        # the transfer read that version.
        self.assertEqual(2, stat.call_count)
        self.assertEqual(b"changed-and-longer", (self.cache_dir / "f.txt").read_bytes())
        meta = self.fs.state_manager.get_file_metadata("/f.txt")
        self.assertEqual(SyncStatus.CLEAN, meta.sync_status)
        self.assertEqual(len(b"changed-and-longer"), meta.size)

    def test_fetch_records_member_baseline(self):
        check = self.fs._member_checks["/f.txt"]
        self.assertEqual({"member": (8, 1_700_000_000.0)}, check.members)

    def test_mtime_only_change_refetches(self):
        self.rewrite_member("f.txt", b"ORIGINAL", mtime=1_700_000_100)
        self.age_checks()
        self.assertEqual(b"ORIGINAL", self.read_through("/f.txt"))

    def test_older_mtime_change_before_first_check_refetches(self):
        # Same size, mtime moved backwards (rsync -a, cp -p, a restore):
        # only the baseline taken at the fetch can see this.
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

    def test_unreachable_holders_skip_the_index_until_one_returns(self):
        self.rewrite_member("f.txt", b"changed-and-longer")
        self.member.set_available(False)
        self.age_checks()
        state = self.fs.state_manager
        with patch.object(state, "get_file_locations", wraps=state.get_file_locations) as locs:
            for _ in range(5):
                self.assertEqual(b"original", self.read_through("/f.txt"))
        # One look at the index, not five.
        self.assertEqual(1, locs.call_count)
        # The member answers again: the very next open revalidates.
        self.member.set_available(True)
        self.assertEqual(b"changed-and-longer", self.read_through("/f.txt"))
        self.assertNotIn("/f.txt", self.fs._revalidate_unreachable)

    def test_unreachable_entry_expires_with_the_age(self):
        self.member.set_available(False)
        self.age_checks()
        state = self.fs.state_manager
        with patch.object(state, "get_file_locations", wraps=state.get_file_locations) as locs:
            self.read_through("/f.txt")
            self.read_through("/f.txt")
            self.assertEqual(1, locs.call_count)
            since, names = self.fs._revalidate_unreachable["/f.txt"]
            self.fs._revalidate_unreachable["/f.txt"] = (
                since - self.revalidate_after - 1, names)
            self.read_through("/f.txt")
        self.assertEqual(2, locs.call_count)

    def test_unreachable_entry_goes_with_the_cached_copy(self):
        self.member.set_available(False)
        self.age_checks()
        self.read_through("/f.txt")
        self.assertIn("/f.txt", self.fs._revalidate_unreachable)
        self.fs.unlink("/f.txt")
        self.assertNotIn("/f.txt", self.fs._revalidate_unreachable)

    def test_revalidation_stat_runs_as_an_interactive_request(self):
        # The short per-call timeout (connect_timeout, not the bulk
        # operation timeout) applies to pooled members only for interactive
        # requests; every FUSE op is one, whatever the loop mode.
        from storage.backends.base import is_interactive_request
        seen = []
        real_stat = self.member.stat

        def stat(path):
            seen.append(is_interactive_request())
            return real_stat(path)

        self.age_checks()
        with patch.object(self.member, "stat", side_effect=stat):
            fd = self.fs("open", "/f.txt", os.O_RDONLY)
            self.fs("release", "/f.txt", fd)
        self.assertEqual([True], seen)

    def test_stalled_stat_serves_the_cached_copy_once_per_age(self):
        import socket
        self.rewrite_member("f.txt", b"changed-and-longer")
        self.age_checks()
        with patch.object(self.member, "stat", side_effect=socket.timeout("timed out")) as stat:
            self.assertEqual(b"original", self.read_through("/f.txt"))
            self.assertEqual(b"original", self.read_through("/f.txt"))
        self.assertEqual(1, stat.call_count)

    def test_refetch_stats_the_member_once(self):
        # The stat that found the change is the refetch's baseline too.
        self.rewrite_member("f.txt", b"changed-and-longer", mtime=1_700_000_500)
        self.age_checks()
        with self.count_stats() as stat:
            self.assertEqual(b"changed-and-longer", self.read_through("/f.txt"))
        # The check's stat, and one confirming the refetch's transfer.
        self.assertEqual(2, stat.call_count)
        self.assertEqual({"member": (len(b"changed-and-longer"), 1_700_000_500.0)},
                         self.fs._member_checks["/f.txt"].members)
        self.assertNotIn("/f.txt", self.fs._pinned_stats)

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
        self.restart()
        self.rewrite_member("f.txt", b"changed-and-longer")
        self.assertEqual(b"changed-and-longer", self.read_through("/f.txt"))

    def test_restart_keeps_baseline_in_xattr(self):
        # Changed while unmounted, same size, older mtime: seen only through
        # the baseline persisted on the cache file.
        self.restart()
        self.rewrite_member("f.txt", b"ORIGINAL", mtime=1_600_000_000)
        self.assertEqual(b"ORIGINAL", self.read_through("/f.txt"))

    def test_persisted_baseline_ignored_after_cache_file_changes(self):
        cache_file = self.cache_dir / "f.txt"
        os.utime(cache_file, (1_650_000_000, 1_650_000_000))
        recorded = self.fs._member_checks["/f.txt"].cache_identity
        self.assertEqual({"member": (8, 1_700_000_000.0)},
                         self.fs._load_persisted_members(cache_file, recorded))
        st = os.lstat(cache_file)
        identity = (st.st_ino, st.st_size, st.st_mtime_ns)
        self.assertNotEqual(recorded, identity)
        self.assertEqual({}, self.fs._load_persisted_members(cache_file, identity))

    def test_baselines_merge_across_members(self):
        cache_file = self.cache_dir / "f.txt"
        identity = self.fs._member_checks["/f.txt"].cache_identity
        self.fs._store_member_check("/f.txt", cache_file, identity, {"other": (8, 1.0)})
        expected = {"member": (8, 1_700_000_000.0), "other": (8, 1.0)}
        self.assertEqual(expected, self.fs._member_checks["/f.txt"].members)
        # And on disk, so a restart sees both.
        self.assertEqual(expected, self.fs._load_persisted_members(cache_file, identity))
        # Not in memory (restart): the merge starts from the xattr.
        self.fs._forget_member_check("/f.txt")
        self.fs._store_member_check("/f.txt", cache_file, identity, {"third": (8, 2.0)})
        self.assertEqual(dict(expected, third=(8, 2.0)), self.fs._member_checks["/f.txt"].members)

    def test_member_stat_xattr_is_read_only_through_the_mount(self):
        before = os.getxattr(self.cache_dir / "f.txt", StorageFS.MEMBER_STAT_XATTR)
        self.fs.setxattr("/f.txt", StorageFS.MEMBER_STAT_XATTR, b"{}", 0)
        self.fs.removexattr("/f.txt", StorageFS.MEMBER_STAT_XATTR)
        self.assertEqual(before, os.getxattr(self.cache_dir / "f.txt", StorageFS.MEMBER_STAT_XATTR))

    def test_failed_stat_keeps_baseline(self):
        baseline = self.fs._member_checks["/f.txt"].members
        self.age_checks()
        with patch.object(self.member, "stat", side_effect=OSError(5, "I/O error")), \
                self.assertLogs("storage.fs", logging.WARNING):
            self.read_through("/f.txt")
        self.assertEqual(baseline, self.fs._member_checks["/f.txt"].members)
        # Same size, older mtime: still caught against the kept baseline.
        self.rewrite_member("f.txt", b"ORIGINAL", mtime=1_600_000_000)
        self.age_checks()
        self.assertEqual(b"ORIGINAL", self.read_through("/f.txt"))

    def test_truncating_open_makes_no_stat(self):
        self.age_checks()
        with self.count_stats() as stat:
            fd = self.fs.open("/f.txt", os.O_WRONLY | os.O_TRUNC)
            self.fs.release("/f.txt", fd)
        stat.assert_not_called()

    def test_rename_keeps_baseline_and_unlink_forgets(self):
        self.fs.rename("/f.txt", "/g.txt")
        self.assertNotIn("/f.txt", self.fs._member_checks)
        self.assertEqual({"member": (8, 1_700_000_000.0)}, self.fs._member_checks["/g.txt"].members)
        # Same size, older mtime at the new name: only the moved baseline sees it.
        self.rewrite_member("g.txt", b"ORIGINAL", mtime=1_600_000_000)
        self.age_checks()
        self.assertEqual(b"ORIGINAL", self.read_through("/g.txt"))
        self.fs.unlink("/g.txt")
        self.assertNotIn("/g.txt", self.fs._member_checks)

    def test_rename_failing_before_any_move_keeps_baselines(self):
        with patch.object(self.fs.namespace, "rename", side_effect=OSError(5, "I/O error")):
            with self.assertRaises(OSError):
                self.fs.rename("/f.txt", "/g.txt")
        self.assertEqual({"member": (8, 1_700_000_000.0)}, self.fs._member_checks["/f.txt"].members)
        os.getxattr(self.cache_dir / "f.txt", StorageFS.MEMBER_STAT_XATTR)
        # The kept baseline still catches a same-size change with an older mtime.
        self.rewrite_member("f.txt", b"ORIGINAL", mtime=1_600_000_000)
        self.age_checks()
        self.assertEqual(b"ORIGINAL", self.read_through("/f.txt"))

    def test_rename_failing_after_the_cache_moved_drops_baselines(self):
        def move_cache_then_fail(old, new, on_local_moved=None):
            os.rename(self.cache_dir / "f.txt", self.cache_dir / "g.txt")
            raise OSError(5, "I/O error")

        with patch.object(self.fs.namespace, "rename", side_effect=move_cache_then_fail):
            with self.assertRaises(OSError):
                self.fs.rename("/f.txt", "/g.txt")
        with self.assertRaises(OSError):
            os.getxattr(self.cache_dir / "g.txt", StorageFS.MEMBER_STAT_XATTR)
        self.assertNotIn("/f.txt", self.fs._member_checks)
        self.assertNotIn("/g.txt", self.fs._member_checks)

    def test_failed_rename_from_a_never_cached_source_keeps_the_targets_baselines(self):
        before = os.getxattr(self.cache_dir / "f.txt", StorageFS.MEMBER_STAT_XATTR)
        with patch.object(self.fs.namespace, "rename", side_effect=OSError(5, "I/O error")):
            with self.assertRaises(OSError):
                self.fs.rename("/never-cached.txt", "/f.txt")
        self.assertEqual({"member": (8, 1_700_000_000.0)}, self.fs._member_checks["/f.txt"].members)
        self.assertEqual(before, os.getxattr(self.cache_dir / "f.txt", StorageFS.MEMBER_STAT_XATTR))

    def test_rename_to_itself_keeps_check(self):
        self.fs._move_member_checks("/f.txt", "/f.txt")
        self.assertIn("/f.txt", self.fs._member_checks)

    def test_rename_keeps_persisted_baseline_after_restart(self):
        self.fs.rename("/f.txt", "/g.txt")
        self.restart()
        self.rewrite_member("g.txt", b"ORIGINAL", mtime=1_600_000_000)
        self.assertEqual(b"ORIGINAL", self.read_through("/g.txt"))

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
        self.assertEqual(len(b"written here"), check.members["member"][0])
        inode = self.cache_inode("new.txt")
        self.age_checks()
        self.assertEqual(b"written here", self.read_through("/new.txt"))
        self.assertEqual(inode, self.cache_inode("new.txt"))


    def test_failed_post_sync_stat_drops_that_members_baseline(self):
        """After an upload the member's old stat no longer describes it; if
        the stat that would replace it fails, the baseline goes (the next
        check uses the rule for a member without one) and the others stay."""
        fd = self.fs.create("/new.txt", 0o644)
        self.fs.write("/new.txt", b"written here", 0, fd)
        self.fs.release("/new.txt", fd)
        cache_path = self.cache_dir / "new.txt"
        identity = self.fs._cache_identity(os.lstat(cache_path))
        self.fs._store_member_check("/new.txt", cache_path, identity,
                                    {"member": (12, 1.0), "other": (12, 2.0)})

        with patch.object(self.member, "stat", side_effect=OSError("connection reset")):
            self.fs._record_member_written("/new.txt", self.member, cache_path)

        self.assertEqual({"other": (12, 2.0)}, self.fs._member_checks["/new.txt"].members)
        self.assertEqual({"other": (12, 2.0)},
                         self.fs._load_persisted_members(cache_path, identity))


    def _written_file(self):
        fd = self.fs.create("/new.txt", 0o644)
        self.fs.write("/new.txt", b"written here", 0, fd)
        self.fs.release("/new.txt", fd)
        cache_path = self.cache_dir / "new.txt"
        return cache_path, self.fs._cache_identity(os.lstat(cache_path))

    def test_failed_post_sync_stat_drops_a_sole_baseline_everywhere(self):
        cache_path, identity = self._written_file()
        self.fs._store_member_check("/new.txt", cache_path, identity, {"member": (12, 1.0)})
        with patch.object(self.member, "stat", side_effect=OSError("connection reset")):
            self.fs._record_member_written("/new.txt", self.member, cache_path)
        self.assertEqual({}, self.fs._member_checks["/new.txt"].members)
        self.assertEqual({}, self.fs._load_persisted_members(cache_path, identity))
        # After a restart, the next check uses the rule without a baseline:
        # same size, member mtime not later than the cache file's.
        # (The sync itself is not run: the member gets the bytes by hand.)
        self.rewrite_member("new.txt", b"written here", mtime=1.0)
        self.fs.state_manager.publish_sync_status(
            "/new.txt", self.fs.state_manager.get_file_metadata("/new.txt").dirty_generation,
            SyncStatus.CLEAN)
        self.restart()
        inode = self.cache_inode("new.txt")
        with self.count_fetches() as fetch:
            self.assertEqual(b"written here", self.read_through("/new.txt"))
        fetch.assert_not_called()
        self.assertEqual(inode, self.cache_inode("new.txt"))

    def test_failed_post_sync_stat_keeps_a_baseline_a_check_recorded_meanwhile(self):
        cache_path, identity = self._written_file()

        # A FUSE thread's check lands while this stat is in flight. (The
        # sync publishes CLEAN only after this callback, so in use a check
        # rarely can; this tests _store_member_check's guard.) It records
        # the same values as the old baseline, as an unchanged member does.
        def observed(size, mtime):
            return (size, mtime)  # a new tuple, as a real stat gives

        for fresh in ((12, 5.0), (12, 1.0)):
            self.fs._store_member_check("/new.txt", cache_path, identity,
                                        {"member": observed(12, 1.0)})

            def check_then_fail(path, fresh=fresh):
                self.fs._store_member_check("/new.txt", cache_path, identity,
                                            {"member": observed(*fresh)})
                raise OSError("connection reset")

            with patch.object(self.member, "stat", side_effect=check_then_fail):
                self.fs._record_member_written("/new.txt", self.member, cache_path)
            self.assertEqual({"member": fresh}, self.fs._member_checks["/new.txt"].members)

    def test_failed_post_sync_stat_drops_despite_a_check_of_another_member(self):
        cache_path, identity = self._written_file()
        self.fs._store_member_check("/new.txt", cache_path, identity,
                                    {"member": (12, 1.0), "other": (12, 2.0)})

        def other_checked_then_fail(path):
            self.fs._store_member_check("/new.txt", cache_path, identity, {"other": (12, 3.0)})
            raise OSError("connection reset")

        with patch.object(self.member, "stat", side_effect=other_checked_then_fail):
            self.fs._record_member_written("/new.txt", self.member, cache_path)
        self.assertEqual({"other": (12, 3.0)}, self.fs._member_checks["/new.txt"].members)


class TestRevalidateSftpLikeMember(RevalidateTestBase):
    """Transfers stamp the current time, so member and cache mtimes differ."""

    backend_class = NoMtimeBackend

    def test_future_member_mtime_refetches_at_most_once(self):
        # A member clock ahead of ours (or touch -d tomorrow): the baseline
        # taken at the fetch means no check ever refetches.
        future = time.time() + 3600
        self.rewrite_member("f.txt", b"original", mtime=future)
        with self.count_fetches() as fetch:
            for _ in range(4):
                self.assertEqual(b"original", self.read_through("/f.txt"))
                self.age_checks()
        self.assertEqual(1, fetch.call_count)

    def test_upload_then_restart_does_not_refetch(self):
        self.restart(start_sync=True)
        fd = self.fs.create("/new.txt", 0o644)
        self.fs.write("/new.txt", b"written here", 0, fd)
        self.fs.release("/new.txt", fd)
        # Put the write seconds before the upload, whose mtime is "now":
        # without a baseline the member would look changed.
        earlier = time.time() - 5
        os.utime(self.cache_dir / "new.txt", (earlier, earlier))
        self.fs._mark_cache_file_dirty("/new.txt", self.cache_dir / "new.txt")
        self.assertTrue(self.fs.sync_manager.sync_now("/new.txt", timeout=10))
        self.restart()
        with self.count_fetches() as fetch:
            self.assertEqual(b"written here", self.read_through("/new.txt"))
        fetch.assert_not_called()


class TestRevalidateTwoMembers(RevalidateTestBase):
    def test_checks_the_member_a_refetch_reads(self):
        fast_dir = self.temp_dir / "fast"
        fast_dir.mkdir()
        fast = LocalBackend("fast", str(fast_dir))
        slow = self.member
        slow.speed_priority = 50
        fast.speed_priority = 0
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        self.fs = StorageFS(
            cache_root=self.cache_dir, state_db=self.db_path,
            backends={"member": slow, "fast": fast}, cache_size=10 * 1024 * 1024,
            num_sync_workers=1, start_sync=False, revalidate_after=self.revalidate_after,
        )
        for d in (fast_dir, self.member_dir):
            (d / "f.txt").write_bytes(b"original")
            os.utime(d / "f.txt", (1_700_000_000, 1_700_000_000))
        self.assertEqual(b"original", self.read_through("/f.txt"))
        # Both members are recorded as holders, and slow holds the baseline
        # (as the last upload would leave it).
        from storage.models import FileLocation
        self.fs.state_manager.add_file_location(FileLocation(
            virtual_path="/f.txt", backend_name="member", checksum="",
            last_synced=time.time(), is_cached=False))
        self.fs.state_manager.add_file_location(FileLocation(
            virtual_path="/f.txt", backend_name="fast", checksum="",
            last_synced=time.time(), is_cached=False))
        self.fast, self.slow, self.fast_dir = fast, slow, fast_dir
        (fast_dir / "f.txt").write_bytes(b"changed on fast")
        self.age_checks()
        with patch.object(slow, "stat", wraps=slow.stat) as slow_stat:
            self.assertEqual(b"changed on fast", self.read_through("/f.txt"))
        slow_stat.assert_not_called()

    def test_refetch_from_another_member_rechecks_the_changed_one(self):
        self.test_checks_the_member_a_refetch_reads()
        fast, fast_dir = self.fast, self.fast_dir
        (fast_dir / "f.txt").write_bytes(b"changed again on fast")
        self.age_checks()
        real_stat = fast.stat

        def stat_then_drop(path):
            # The checked member goes away between its stat and the refetch.
            result = real_stat(path)
            fast.set_available(False)
            return result

        with patch.object(fast, "stat", side_effect=stat_then_drop):
            # The slower member supplies its (older) bytes; the open works.
            self.assertEqual(b"original", self.read_through("/f.txt"))
        self.assertEqual({"fast"}, self.fs._recheck["/f.txt"])
        fast.set_available(True)
        self.age_checks()
        # Its baseline now matches nothing it supplied: the recheck refetches.
        self.assertEqual(b"changed again on fast", self.read_through("/f.txt"))
        self.assertNotIn("/f.txt", self.fs._recheck)

    def test_recheck_mark_survives_a_failed_pinned_transfer(self):
        self.test_checks_the_member_a_refetch_reads()
        fast, fast_dir = self.fast, self.fast_dir
        with self.fs._member_checks_lock:
            self.fs._recheck["/f.txt"] = {"fast"}
        self.age_checks()
        with patch.object(fast, "copy_to_local", side_effect=OSError(5, "I/O error")):
            with self.assertRaises(Exception):
                self.read_through("/f.txt")
        # Selected but never delivered: the forced refetch is still owed.
        self.assertEqual({"fast"}, self.fs._recheck["/f.txt"])
        (fast_dir / "f.txt").write_bytes(b"CHANGED ON FAST")
        self.assertEqual(b"CHANGED ON FAST", self.read_through("/f.txt"))
        self.assertNotIn("/f.txt", self.fs._recheck)

    def test_recheck_mark_survives_a_refused_invalidation(self):
        self.test_checks_the_member_a_refetch_reads()
        fast, fast_dir = self.fast, self.fast_dir
        with self.fs._member_checks_lock:
            self.fs._recheck["/f.txt"] = {"fast"}
        writer = self.fs.open("/f.txt", os.O_RDWR)
        try:
            self.age_checks()
            # The forced refetch is refused (a writer is open): the copy and
            # the mark both stay.
            self.assertEqual(b"changed on fast", self.read_through("/f.txt"))
            self.assertEqual({"fast"}, self.fs._recheck["/f.txt"])
        finally:
            self.fs.release("/f.txt", writer)
        (fast_dir / "f.txt").write_bytes(b"CHANGED ON FAST")
        self.age_checks()
        self.assertEqual(b"CHANGED ON FAST", self.read_through("/f.txt"))
        self.assertNotIn("/f.txt", self.fs._recheck)



class TestRevalidateStreamedFetch(RevalidateTestBase):
    def _make_fs(self, start_sync=False):
        return StorageFS(
            cache_root=self.cache_dir,
            state_db=self.db_path,
            backends={"member": self.member},
            cache_size=10 * 1024 * 1024,
            num_sync_workers=1,
            start_sync=start_sync,
            streaming_reads=True,
            streaming_chunk_size=4096,
            revalidate_after=self.revalidate_after,
        )

    def tearDown(self):
        self.fs.destroy("/")
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_finished_stream_records_member_baseline(self):
        payload = os.urandom(3 * 4096 + 17)
        self.rewrite_member("s.bin", payload, mtime=1_700_000_000)
        self.assertEqual(payload, self.read_through("/s.bin"))
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            check = self.fs._member_checks.get("/s.bin")
            if check is not None and check.members:
                break
            time.sleep(0.05)
        self.assertEqual({"member": (len(payload), 1_700_000_000.0)},
                         self.fs._member_checks["/s.bin"].members)
        # Same size, older mtime: caught against the streamed baseline.
        changed = os.urandom(len(payload))
        self.rewrite_member("s.bin", changed, mtime=1_600_000_000)
        self.age_checks()
        self.assertEqual(changed, self.read_through("/s.bin"))

    def _wait_for_baseline(self, path):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            check = self.fs._member_checks.get(path)
            if check is not None and check.members:
                return check
            time.sleep(0.05)
        self.fail(f"no baseline recorded for {path}")

    def test_member_change_mid_stream_is_refetched(self):
        # The member's stat at the end no longer matches the one taken when
        # the stream started, so bytes that mix the old and new versions are
        # never installed: the next open fetches the file again.
        old = os.urandom(3 * 4096 + 17)
        new = os.urandom(len(old))
        self.rewrite_member("m.bin", old, mtime=1_700_000_000)
        real_read = self.member.read_file
        state = {"changed": False}

        def read_then_change(path, offset, size):
            data = real_read(path, offset, size)
            if not state["changed"]:
                state["changed"] = True
                self.rewrite_member("m.bin", new, mtime=1_600_000_000)
            return data

        with patch.object(self.member, "read_file", side_effect=read_then_change):
            # Read it all, so the stream finishes (keep-after-close).
            mixed = self.read_through("/m.bin")
            stats = self.fs._streaming_manager._streaming_stats
            # Counted only once the session finished and its partial is gone.
            deadline = time.monotonic() + 10
            while (time.monotonic() < deadline
                   and not stats.get("streaming_member_changed_discards")):
                time.sleep(0.05)
            self.assertFalse((self.cache_dir / "m.bin.storage.partial").exists())
        # The first range came from the old version, the rest from the new.
        self.assertNotEqual(new, mixed)
        self.assertEqual(stats.get("streaming_member_changed_discards"), 1)
        self.assertFalse((self.cache_dir / "m.bin").exists())
        self.assertEqual(new, self.read_through("/m.bin"))
        self.assertEqual(self.fs.getattr("/m.bin")["st_mtime"], 1_600_000_000)


class TestRevalidateStreamBail(RevalidateTestBase):
    """A stream that selects the pinned member and then bails to a full fetch."""

    def _make_fs(self, start_sync=False):
        fast_dir = self.temp_dir / "fast"
        fast_dir.mkdir(exist_ok=True)
        self.fast_dir = fast_dir
        self.fast = LocalBackend("fast", str(fast_dir))
        self.member.speed_priority = 50
        self.fast.speed_priority = 0
        return StorageFS(
            cache_root=self.cache_dir, state_db=self.db_path,
            backends={"member": self.member, "fast": self.fast},
            cache_size=10 * 1024 * 1024, num_sync_workers=1, start_sync=start_sync,
            streaming_reads=True, streaming_chunk_size=4096,
            revalidate_after=self.revalidate_after,
        )

    def tearDown(self):
        self.fs.destroy("/")
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _cached_on_both(self):
        from storage.models import FileLocation
        for d in (self.fast_dir, self.member_dir):
            (d / "f.txt").write_bytes(b"original")
            os.utime(d / "f.txt", (1_700_000_000, 1_700_000_000))
        self.assertEqual(b"original", self.read_through("/f.txt"))
        # The first read streamed; wait for the finished stream to be cached.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            check = self.fs._member_checks.get("/f.txt")
            if check is not None and check.members:
                break
            time.sleep(0.05)
        else:
            self.fail("stream never recorded a baseline")
        for name in ("member", "fast"):
            self.fs.state_manager.add_file_location(FileLocation(
                virtual_path="/f.txt", backend_name=name, checksum="",
                last_synced=time.time(), is_cached=False))

    def test_selection_is_idempotent_within_one_open(self):
        self._cached_on_both()
        with self.fs._member_checks_lock:
            self.fs._refetch_pins["/f.txt"] = self.member
        # The stream's selection and the fallback's both see the pin, even
        # though the unpinned order would prefer "fast".
        self.assertIs(self.member, self.fs._select_backend_for_read("/f.txt"))
        self.assertIs(self.member, self.fs._select_backend_for_read("/f.txt"))
        self.fs.release("/f.txt", self.fs.open("/f.txt", os.O_RDONLY))
        self.assertNotIn("/f.txt", self.fs._refetch_pins)

    def test_a_pin_no_fetch_used_does_not_outlive_the_open(self):
        self._cached_on_both()
        # Fresh check, cached file: this open revalidates nothing and fetches
        # nothing, so only open()'s own cleanup can drop a pin left behind.
        with self.fs._member_checks_lock:
            self.fs._refetch_pins["/f.txt"] = self.fast
        self.fs.release("/f.txt", self.fs.open("/f.txt", os.O_RDONLY))
        self.assertNotIn("/f.txt", self.fs._refetch_pins)

    def test_stream_bailing_to_the_fallback_still_fetches_from_the_pinned_member(self):
        self._cached_on_both()
        (self.fast_dir / "f.txt").write_bytes(b"changed on fast")
        self.age_checks()
        # The unpinned order would now choose "member" (old bytes): only the
        # pin, carried into the fallback, brings in the checked member's.
        with patch.object(self.fast, "supports_range_reads", return_value=False), \
                patch.object(self.fs.replication_manager, "get_backend_for_read",
                             return_value=self.member):
            self.assertEqual(b"changed on fast", self.read_through("/f.txt"))
        self.assertNotIn("/f.txt", self.fs._refetch_pins)

    def test_recheck_mark_survives_a_stream_that_bails_when_the_pinned_member_drops(self):
        self._cached_on_both()
        (self.fast_dir / "f.txt").write_bytes(b"changed on fast")
        self.age_checks()
        real_stat = self.fast.stat

        def stat_then_drop(path):
            result = real_stat(path)
            self.fast.set_available(False)
            return result

        with patch.object(self.fast, "stat", side_effect=stat_then_drop), \
                patch.object(self.member, "supports_range_reads", return_value=False):
            # The slower member supplies its older bytes through the fallback.
            self.assertEqual(b"original", self.read_through("/f.txt"))
        self.assertEqual({"fast"}, self.fs._recheck["/f.txt"])
        self.assertNotIn("/f.txt", self.fs._refetch_pins)
        self.fast.set_available(True)
        self.age_checks()
        self.assertEqual(b"changed on fast", self.read_through("/f.txt"))
        # Streamed this time: the mark clears when the stream finishes.
        deadline = time.monotonic() + 10
        while "/f.txt" in self.fs._recheck and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertNotIn("/f.txt", self.fs._recheck)


class TestRevalidateFromListing(RevalidateTestBase):
    """A recent listing of the parent stands in for the stat when it shows no change."""

    def setUp(self):
        super().setUp()
        self.rewrite_member("f.txt", b"original", mtime=1_700_000_000)
        self.assertEqual(b"original", self.read_through("/f.txt"))

    def list_root(self):
        self.fs.namespace.invalidate_dir_cache_entry("/")
        self.assertIn("f.txt", list(self.fs.readdir("/", None)))
        self.assertIn("/", self.fs.namespace.entry_attrs)

    def test_unchanged_listing_replaces_the_stat(self):
        self.age_checks()
        self.list_root()
        with self.count_stats() as stat:
            self.assertEqual(b"original", self.read_through("/f.txt"))
        stat.assert_not_called()
        # It counts as a check, dated from the member read behind the listing
        # (not from now), so the next one is due a full age after that read.
        check = self.fs._member_checks["/f.txt"]
        self.assertEqual({"member": (8, 1_700_000_000.0)}, check.members)
        self.assertEqual(self.fs.namespace.entry_attrs["/"][0], check.verified_at)

    def test_check_from_a_listing_expires_with_the_listing_not_with_now(self):
        self.age_checks()
        self.list_root()
        with self.fs.namespace.listing_lock:
            at, attrs, unknown = self.fs.namespace.entry_attrs["/"]
            # The read behind the listing began just inside the age.
            self.fs.namespace.entry_attrs["/"] = (
                time.monotonic() - self.revalidate_after + 5, attrs, unknown)
        self.read_through("/f.txt")
        self.rewrite_member("f.txt", b"changed-and-longer")
        with self.count_stats() as stat:
            self.read_through("/f.txt")
            stat.assert_not_called()
            # Ten seconds on, a stat taken at the listing would be stale:
            # so are this check and the listing, and the change is seen.
            self.age_checks(10)
            with self.fs.namespace.listing_lock:
                at, attrs, unknown = self.fs.namespace.entry_attrs["/"]
                self.fs.namespace.entry_attrs["/"] = (at - 10, attrs, unknown)
            self.assertEqual(b"changed-and-longer", self.read_through("/f.txt"))
        # The check's stat, and one confirming the refetch's transfer.
        self.assertEqual(2, stat.call_count)

    def test_changed_listing_falls_back_to_a_stat_and_refetches(self):
        self.rewrite_member("f.txt", b"ORIGINAL", mtime=1_600_000_000)
        self.age_checks()
        self.list_root()
        with self.count_stats() as stat:
            self.assertEqual(b"ORIGINAL", self.read_through("/f.txt"))
        # The check's stat, and one confirming the refetch's transfer.
        self.assertEqual(2, stat.call_count)

    def test_listing_older_than_the_age_is_ignored(self):
        self.list_root()
        with self.fs.namespace.listing_lock:
            at, attrs, unknown = self.fs.namespace.entry_attrs["/"]
            self.fs.namespace.entry_attrs["/"] = (at - self.revalidate_after - 5, attrs, unknown)
        self.age_checks(self.revalidate_after + 100)
        with self.count_stats() as stat:
            self.read_through("/f.txt")
        self.assertEqual(1, stat.call_count)

    def test_dropped_name_is_not_trusted(self):
        self.age_checks()
        self.list_root()
        self.fs.namespace.forget_listed_attrs("/f.txt", removing=False)
        with self.count_stats() as stat:
            self.read_through("/f.txt")
        self.assertEqual(1, stat.call_count)

    def test_owed_recheck_is_not_answered_from_a_listing(self):
        self.age_checks()
        self.list_root()
        with self.fs._member_checks_lock:
            self.fs._recheck["/f.txt"] = {"member"}
        with self.count_stats() as stat:
            self.read_through("/f.txt")
        # The check's stat, and one confirming the refetch's transfer.
        self.assertEqual(2, stat.call_count)

    def test_member_without_a_baseline_is_stat_ed(self):
        with self.fs._member_checks_lock:
            self.fs._member_checks.pop("/f.txt")
        os.removexattr(self.cache_dir / "f.txt", StorageFS.MEMBER_STAT_XATTR)
        self.list_root()
        with self.count_stats() as stat:
            self.read_through("/f.txt")
        self.assertEqual(1, stat.call_count)


if __name__ == "__main__":
    unittest.main()


class TestRecomputeRecordsStat(unittest.TestCase):
    """A cache-miss recompute over a CLEAN size-0 row leaves its bytes evictable."""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_dir = self.temp_dir / "cache"
        self.cache_dir.mkdir()
        self.member_dir = self.temp_dir / "member"
        self.member_dir.mkdir()
        self.member = LocalBackend("member", str(self.member_dir))
        self.fs = StorageFS(
            cache_root=self.cache_dir, state_db=self.temp_dir / "state.db",
            backends={"member": self.member}, cache_size=10 * 1024 * 1024,
            start_sync=False,
        )

    def tearDown(self):
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        shutil.rmtree(self.temp_dir)

    def _recompute(self, output):
        cache_path = self.cache_dir / "derived.bin"

        def run(virtual_path, path, timeout=None):
            path.write_bytes(output)
            return SimpleNamespace(success=True, output="", error="", duration=0.0, timed_out=False)

        engine = self.fs.recompute_engine
        with patch.object(engine, "can_recompute", return_value=True), \
                patch.object(engine, "recompute_file", side_effect=run):
            self.assertTrue(self.fs._ensure_file_in_cache(self.fs._resolve_path("/derived.bin")))
        return cache_path

    def _row(self, **changes):
        self.fs.state_manager.set_file_metadata(dataclasses.replace(FileMetadata(
            virtual_path="/derived.bin", size=0, checksum=None, last_access=1.0,
            last_modified=0.0, sync_status=SyncStatus.CLEAN, replica_count=1,
            recomputable=True, recompute_cmd="make", encrypted=False, encryption_key_id=None,
        ), **changes))

    def test_cache_miss_recompute_over_a_size_zero_row_becomes_a_candidate(self):
        self._row()
        cache_path = self._recompute(b"recomputed bytes")
        st = cache_path.stat()
        row = self.fs.state_manager.get_file_metadata("/derived.bin")
        self.assertEqual((row.size, row.last_modified, row.sync_status),
                         (st.st_size, st.st_mtime, SyncStatus.CLEAN))
        self.assertEqual(read_cache_metadata(cache_path).size, st.st_size)
        self.assertEqual(list(self.fs.state_manager.iter_lru_paths(recomputable=True)), ["/derived.bin"])
        # The stat matches, so the next open does not take it for a lost write.
        self.assertFalse(self.fs.cache_manager.heal_stale_clean("/derived.bin", cache_path))

    def test_recompute_leaves_a_dirty_row_alone(self):
        self._row(sync_status=SyncStatus.DIRTY, size=5, last_modified=2.0)
        self._recompute(b"recomputed bytes")
        row = self.fs.state_manager.get_file_metadata("/derived.bin")
        self.assertEqual((row.size, row.last_modified, row.sync_status), (5, 2.0, SyncStatus.DIRTY))
