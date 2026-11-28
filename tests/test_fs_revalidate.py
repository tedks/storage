"""Revalidation of cached files against their member on open.

Ruling on oq-watcher-outage-cache-policy: opening a CLEAN cached file whose
last check is older than revalidate_after stats the member holding it and
refetches the file when the member's copy changed. These tests drive
StorageFS directly (no mount) with local members, and age the in-memory
checks instead of sleeping.
"""

import dataclasses
import errno
import json
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

    def without_descriptor_evidence(self):
        """Model an adapter that can copy bounded bytes but exposes no fstat."""
        from storage.backends.base import current_copy_bound
        copy = self.member.copy_to_local

        def transfer(path, staging):
            copy(path, staging)
            current_copy_bound().source_stats.clear()

        return patch.object(self.member, "copy_to_local", side_effect=transfer)

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
            last_access=local.st_atime,
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

    def fill_with_reported_mode(self, mode):
        """A full fill whose member reports mode (synthetic; the file stays
        readable to the transfer, as one readable through a group would)."""
        src = self.rewrite_member("g.txt", b"guarded", mtime=1_000_000_000)
        real = self.member.stat

        def reported(path):
            st = real(path)
            return SimpleNamespace(st_size=st.st_size, st_mtime=st.st_mtime,
                                   st_mtime_ns=st.st_mtime_ns, st_mode=stat.S_IFREG | mode,
                                   st_uid=54321, st_gid=54322)

        with patch.object(self.member, "stat", side_effect=reported):
            self.assertTrue(self.fs._ensure_file_in_cache(self.fs._resolve_path("/g.txt")))
        cached = self.cache_dir / "g.txt"
        # Storage's own owner read and write on the backing file.
        self.assertEqual(cached.stat().st_mode & 0o600, 0o600)
        return src, cached

    def test_synthesized_owner_read_bit_grants_no_read(self):
        for mode in (0o200, 0o044, 0o000):
            with self.subTest(mode=oct(mode)):
                src, cached = self.fill_with_reported_mode(mode)
                self.assertEqual(stat.S_IMODE(self.fs.getattr("/g.txt")["st_mode"]), mode)
                for flags in (os.O_RDONLY, os.O_RDWR):
                    with self.assertRaises(FuseOSError) as raised:
                        self.fs.open("/g.txt", flags)
                    self.assertEqual(raised.exception.errno, errno.EACCES)
                if mode & stat.S_IWUSR:  # write-only stays writable
                    self.fs.release("/g.txt", self.fs.open("/g.txt", os.O_WRONLY))
                self.fs.cache_manager.delete_cached_file("/g.txt")
                self.fs.state_manager.delete_file("/g.txt")

    def test_readable_member_modes_still_read(self):
        for mode in (0o400, 0o644, 0o600):
            with self.subTest(mode=oct(mode)):
                self.fill_with_reported_mode(mode)
                self.assertEqual(self.read_through("/g.txt"), b"guarded")
                self.fs.cache_manager.delete_cached_file("/g.txt")
                self.fs.state_manager.delete_file("/g.txt")

    def _turning_read_only(self, probe_stat):
        """Patches: the copy finds the source turned read-only (same
        length); the first stat is probe_stat(real stat), the rest fail."""
        src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        os.chmod(src, 0o644)
        copy_to_local = self.member.copy_to_local
        real_stat, seen = self.member.stat, []

        def turned_read_only(path, temp):
            os.chmod(src, 0o444)
            copy_to_local(path, temp)

        def probe_then_fail(path):
            seen.append(path)
            if len(seen) == 1:
                return probe_stat(real_stat(path))
            raise OSError(errno.EIO, "stat failed")

        return (patch.object(self.member, "copy_to_local", side_effect=turned_read_only),
                patch.object(self.member, "stat", side_effect=probe_then_fail))

    def test_unconfirmed_version_is_not_installed(self):
        # A comparable probe, then no confirmation at all: not a verified
        # generation, so nothing is published. A writable open gets EAGAIN;
        # a later open with the member answering installs its current
        # (read-only) version.
        copying, stats = self._turning_read_only(lambda st: st)
        with copying, stats:
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/f.txt", os.O_RDWR)
        self.assertEqual(raised.exception.errno, errno.EAGAIN)
        self.assertFalse((self.cache_dir / "f.txt").exists())
        self.assertEqual(self.read_through("/f.txt"), b"original")
        self.assertEqual(stat.S_IMODE(self.fs.getattr("/f.txt")["st_mode"]), 0o444)

    def test_stale_writable_probe_does_not_survive_a_failed_confirmation(self):
        # The receiver can copy bytes without a token, but failed stat I/O
        # does not classify the member as NEVER_VERSIONED.
        _, stats = self._turning_read_only(
            lambda st: SimpleNamespace(st_size=st.st_size, st_mode=stat.S_IFREG | 0o644))
        sftp_like = patch.object(self.member, "copy_to_local", side_effect=lambda path, temp: (
            os.chmod(self.member_dir / "f.txt", 0o444),
            shutil.copyfile(self.member_dir / "f.txt", temp)))
        with sftp_like, stats:
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/f.txt", os.O_RDONLY)
        self.assertEqual(raised.exception.errno, errno.EAGAIN)
        self.assertFalse((self.cache_dir / "f.txt").exists())
        self.assertEqual(self.read_through("/f.txt"), b"original")
        self.assertEqual(self.fs.getattr("/f.txt")["st_mode"] & 0o222, 0)
        with self.assertRaises(FuseOSError) as raised:
            self.fs.open("/f.txt", os.O_WRONLY)
        self.assertEqual(raised.exception.errno, errno.EACCES)

    def test_confirmation_without_mode_keeps_the_probes_read_denial(self):
        src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        real, seen = self.member.stat, []

        def probe_with_mode_then_without(path):
            st = real(path)
            seen.append(path)
            mode = stat.S_IFREG | 0o200 if len(seen) == 1 else None
            return SimpleNamespace(st_size=st.st_size, st_mtime=st.st_mtime,
                                   st_mtime_ns=st.st_mtime_ns, st_mode=mode)

        with patch.object(self.member, "stat", side_effect=probe_with_mode_then_without):
            self.assertTrue(self.fs._ensure_file_in_cache(self.fs._resolve_path("/f.txt")))
        self.assertEqual(self.fs.getattr("/f.txt")["st_mode"] & 0o400, 0)
        with self.assertRaises(FuseOSError) as raised:
            self.fs.open("/f.txt", os.O_RDONLY)
        self.assertEqual(raised.exception.errno, errno.EACCES)

    def test_never_versioned_fill_keeps_the_probes_atime(self):
        self.rewrite_member("f.txt", b"original")
        real, seen = self.member.stat, []

        def stats(path):
            st = real(path)
            seen.append(path)
            atime = 900_000_000 if len(seen) == 1 else 1_700_000_000
            return SimpleNamespace(st_size=st.st_size, st_atime=atime,
                                   st_atime_ns=atime * 1_000_000_000)

        with self.without_descriptor_evidence(), patch.object(self.member, "stat", side_effect=stats):
            self.assertTrue(self.fs._ensure_file_in_cache(self.fs._resolve_path("/f.txt")))
        self.assertEqual((self.cache_dir / "f.txt").stat().st_atime, 900_000_000)

    def test_warm_open_past_the_age_with_a_stat_lacking_mtime_keeps_the_copy(self):
        self.rewrite_member("f.txt", b"original")
        size_only = lambda path: SimpleNamespace(st_size=8)  # noqa: E731
        self.assertEqual(self.read_through("/f.txt"), b"original")
        with patch.object(self.member, "stat", side_effect=size_only):
            for limited in (SimpleNamespace(st_size=8), SimpleNamespace(st_size=8, st_mtime=None)):
                self.age_checks()
                with patch.object(self.member, "stat", return_value=limited), \
                        self.count_fetches() as fetches:
                    self.assertEqual(self.read_through("/f.txt"), b"original")
                self.assertEqual(fetches.call_count, 0)

    def test_read_only_member_mode_is_enforced(self):
        self.assert_write_refused(self.fill_read_only({"wraps": self.member.stat}))

    def test_read_only_without_member_stat_fails_closed(self):
        # No member mode: the downloaded file's own (copied) mode is
        # recorded instead of the backing file's owner-writable one.
        for kwargs in ({"side_effect": NotImplementedError("no stat")},
                       {"return_value": SimpleNamespace(st_size=None)}):
            with self.subTest(kwargs=kwargs):
                with self.without_descriptor_evidence(), patch.object(self.member, "get_size", return_value=8):
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
            last_access=local.st_atime,
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

    def test_a_second_stat_can_confirm_the_probe_after_a_transient_failure(self):
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000).chmod(0o644)
        probe = self.member.stat("/f.txt")
        with patch.object(self.member, "stat", side_effect=[
                probe, OSError(errno.EIO, "stat failed"), probe]) as stats:
            self.assertEqual(self.read_through("/f.txt"), b"original")
        self.assertEqual(stats.call_count, 3)
        attrs = self.fs.getattr("/f.txt")
        self.assertEqual(stat.S_IMODE(attrs["st_mode"]), 0o644)
        self.assertEqual(attrs["st_mtime"], 1_000_000_000)
        self.fs.release("/f.txt", self.fs.open("/f.txt", os.O_WRONLY))

    def test_a_cold_open_requires_confirmation_of_the_token(self):
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        probe = self.member.stat("/f.txt")
        for flags in (os.O_RDONLY, os.O_WRONLY):
            with self.subTest(flags=flags), patch.object(self.member, "stat", side_effect=[
                    probe, OSError(errno.EIO, "stat failed"), OSError(errno.EIO, "stat failed")
            ]) as stats:
                with self.assertRaises(FuseOSError) as raised:
                    self.fs.open("/f.txt", flags)
                self.assertEqual(raised.exception.errno, errno.EAGAIN)
                self.assertEqual(stats.call_count, 3)
            self.assertFalse((self.cache_dir / "f.txt").exists())
            self.assertIsNone(self.fs.state_manager.get_file_metadata("/f.txt"))
        self.assertEqual(self.read_through("/f.txt"), b"original")

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
            # Another size than probed, with no newer token to retry against.
            for flags in (os.O_RDONLY, os.O_WRONLY):
                seen.clear()
                with self.assertRaises(FuseOSError) as raised:
                    self.fs.open("/f.txt", flags)
                self.assertEqual(raised.exception.errno, errno.EAGAIN)
        self.assertFalse((self.cache_dir / "f.txt").exists())
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/f.txt"))

    def test_transfer_of_another_size_than_the_stat_is_fetched_again(self):
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        fake = SimpleNamespace(st_size=99, st_mode=stat.S_IFREG | 0o600, st_mtime=1_200_000_000)
        with patch.object(self.member, "stat", return_value=fake), \
                patch.object(self.member, "get_size", return_value=99), \
                self.count_fetches() as fetches:
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/f.txt", os.O_RDWR)
        self.assertEqual(raised.exception.errno, errno.EAGAIN)
        self.assertEqual(fetches.call_count, 2)
        # Still another size after the retry: never installed under a token
        # its bytes do not match.
        self.assertFalse((self.cache_dir / "f.txt").exists())

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


class TestStickyVersioning(RevalidateTestBase):
    """Once any stat seen for a fill carries a version token, the fill is
    published only on AGREED. An unstable cold fill fails with EAGAIN."""

    def partials(self):
        return sorted(p.name for p in self.cache_dir.rglob("*.storage.partial"))

    def test_token_seen_only_after_the_copy_publishes_nothing(self):
        # J1: the probe raises, the member is rewritten (same size) during the
        # copy, the confirmation succeeds with a token: unconfirmed.
        src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        real_stat, seen = self.member.stat, []
        copy = self.member.copy_to_local

        def probe_fails(path):
            seen.append(path)
            if len(seen) == 1:
                raise OSError(errno.EIO, "probe failed")
            return real_stat(path)

        def rewritten(path, temp):
            copy(path, temp)
            src.write_bytes(b"NEWBYTES")
            os.utime(src, (1_100_000_000, 1_100_000_000))

        with patch.object(self.member, "stat", side_effect=probe_fails), \
                patch.object(self.member, "copy_to_local", side_effect=rewritten):
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/f.txt", os.O_RDWR)
        self.assertEqual(raised.exception.errno, errno.EAGAIN)
        self.assertFalse((self.cache_dir / "f.txt").exists())

    def test_probe_without_mtime_then_a_token_is_unconfirmed(self):
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        real_stat, seen = self.member.stat, []

        def stats(path):
            seen.append(path)
            st = real_stat(path)
            return SimpleNamespace(st_size=st.st_size) if len(seen) == 1 else st

        with patch.object(self.member, "stat", side_effect=stats):
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/f.txt", os.O_WRONLY)
        self.assertEqual(raised.exception.errno, errno.EAGAIN)
        self.assertFalse((self.cache_dir / "f.txt").exists())

    def test_never_versioned_fill_records_no_member_mtime(self):
        # A stat with an mtime but no size carries no token: that mtime was
        # never compared, so it is not put on the copy.
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        with self.without_descriptor_evidence(), patch.object(
                self.member, "stat", return_value=SimpleNamespace(
                    st_mtime=1_234_567_890, st_uid=4242)):
            self.assertEqual(self.read_through("/f.txt"), b"original")
        record = json.loads(os.getxattr(self.cache_dir / "f.txt", MEMBER_ATTRS_XATTR))
        self.assertEqual(record["attrs"].get("st_uid"), 4242)
        self.assertEqual((self.cache_dir / "f.txt").stat().st_mtime, 1_000_000_000)  # the copy's own

    def test_genuine_no_stat_member_reuses_its_fill_warm_and_offline(self):
        # J2: no stat at all, through and past the revalidation age.
        self.rewrite_member("f.txt", b"no stat here")
        with self.without_descriptor_evidence(), patch.object(
                self.member, "stat", side_effect=NotImplementedError):
            self.assertEqual(self.read_through("/f.txt"), b"no stat here")
            self.age_checks()
            with self.count_fetches() as fetches:
                self.assertEqual(self.read_through("/f.txt"), b"no stat here")
            self.assertEqual(fetches.call_count, 0)
            self.member.set_available(False)
            try:
                with self.count_fetches() as fetches:
                    self.assertEqual(self.read_through("/f.txt"), b"no stat here")
                self.assertEqual(fetches.call_count, 0)
            finally:
                self.member.set_available(True)

    def test_revalidation_without_mtime_refetches_on_another_size(self):
        self.rewrite_member("f.txt", b"original")
        self.assertEqual(self.read_through("/f.txt"), b"original")
        self.rewrite_member("f.txt", b"longer content")
        self.age_checks()
        size_only = lambda path: SimpleNamespace(st_size=14)  # noqa: E731
        with patch.object(self.member, "stat", side_effect=size_only), \
                self.count_fetches() as fetches, self.without_descriptor_evidence():
            self.assertEqual(self.read_through("/f.txt"), b"longer content")
        self.assertEqual(fetches.call_count, 1)

    def test_revalidation_reads_st_mtime_ns(self):
        # A stat that reports its mtime only in nanoseconds: the same version
        # is kept, a newer one (same size) refetched.
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        self.assertEqual(self.read_through("/f.txt"), b"original")
        for mtime, content, fetched in ((1_000_000_000, b"original", 0),
                                        (1_100_000_000, b"REWRITE!", 1)):
            self.rewrite_member("f.txt", content, mtime=mtime)
            self.age_checks()
            ns_only = lambda path, m=mtime: SimpleNamespace(  # noqa: E731
                st_size=8, st_mtime_ns=m * 10**9)
            with patch.object(self.member, "stat", side_effect=ns_only), \
                    self.count_fetches() as fetches:
                self.assertEqual(self.read_through("/f.txt"), content)
            self.assertEqual(fetches.call_count, fetched)

    def test_absent_member_file_is_enoent(self):
        with self.assertRaises(FuseOSError) as raised:
            self.fs.open("/missing.txt", os.O_RDONLY)
        self.assertEqual(raised.exception.errno, errno.ENOENT)


class TestLiveRead(RevalidateTestBase):
    """CTO-208-LIVE-READ: a file that keeps changing."""

    def growing_copy(self, extra=b"+more"):
        """copy_to_local that lets the member grow after each copy, so the
        stat after the transfer always reports another size."""
        src = self.member_dir / "live.log"
        copy = self.member.copy_to_local

        def grows(path, temp):
            copy(path, temp)
            with open(src, "ab") as f:
                f.write(extra)

        return patch.object(self.member, "copy_to_local", side_effect=grows)

    def partials(self):
        return sorted(p.name for p in self.cache_dir.rglob("*.storage.partial"))

    def test_unstable_cold_reader_gets_eagain_and_nothing_is_published(self):
        self.rewrite_member("live.log", b"line 1\n")
        with self.growing_copy() as copies, patch.object(self.member, "copy_from_local") as uploads:
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/live.log", os.O_RDONLY)
        self.assertEqual(raised.exception.errno, errno.EAGAIN)
        self.assertEqual(copies.call_count, 2)
        uploads.assert_not_called()
        self.assertEqual(self.fs.cache_manager._reserved_bytes, 0)
        self.assertFalse((self.cache_dir / "live.log").exists())
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/live.log"))
        self.assertEqual(self.partials(), [])

    def test_retry_that_does_not_fit_reports_enospc_and_leaves_nothing(self):
        # The retry must reserve room for the larger version before copying.
        self.rewrite_member("live.log", b"line 1\n")
        with self.growing_copy(), patch.object(self.fs.cache_manager, "_reserve_more",
                                               return_value=False):
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/live.log", os.O_RDONLY)
        self.assertEqual(raised.exception.errno, errno.ENOSPC)
        self.assertEqual(self.partials(), [])
        self.assertEqual(self.fs.cache_manager._reserved_bytes, 0)

    def appending_during_copy(self, guard=4096):
        """The member appends after every chunk the bounded copy reads (8-byte
        chunks), up to guard bytes in all: a writer outrunning the transfer."""
        from storage.backends import base, local
        src = self.member_dir / "live.log"
        real_stream, appended = local.copy_bounded_stream, []

        def stream(read, write, bound):
            def read_and_grow(n):
                data = read(n)
                if sum(appended) < guard:
                    with open(src, "ab") as f:
                        f.write(b"+" * 5)
                    appended.append(5)
                return data
            real_stream(read_and_grow, write, bound)
            self.bounds.append(bound)

        self.bounds = []
        return [patch.object(base, "COPY_CHUNK", 8), patch.object(local, "copy_bounded_stream", stream)]

    def test_source_appending_during_the_copy_is_bounded_and_refused(self):
        import contextlib
        self.rewrite_member("live.log", b"0123456789" * 4)
        with contextlib.ExitStack() as stack:
            uploads = stack.enter_context(patch.object(self.member, "copy_from_local"))
            for patcher in self.appending_during_copy():
                stack.enter_context(patcher)
            for flags in (os.O_RDWR, os.O_RDONLY):
                self.bounds.clear()
                with self.assertRaises(FuseOSError) as raised:
                    self.fs.open("/live.log", flags)
                self.assertEqual(raised.exception.errno, errno.EAGAIN)
                self.assertEqual(len(self.bounds), 2)
                for bound in self.bounds:
                    self.assertTrue(bound.grew)
                    self.assertEqual(bound.written, bound.cap)
                    self.assertEqual(bound.read, bound.cap + 1)
        uploads.assert_not_called()
        self.assertFalse((self.cache_dir / "live.log").exists())
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/live.log"))
        self.assertEqual(self.partials(), [])
        self.assertEqual(self.fs.cache_manager._reserved_bytes, 0)

    def test_growth_the_stats_do_not_show_still_blocks_publication(self):
        # The member's stat keeps reporting the probed version (a lagging
        # stat), but the bounded copy saw bytes past it: not published.
        import contextlib
        self.rewrite_member("live.log", b"0123456789" * 4)
        probe = self.member.stat("/live.log")
        with contextlib.ExitStack() as stack:
            for patcher in self.appending_during_copy() + [
                    patch.object(self.member, "stat", return_value=probe)]:
                stack.enter_context(patcher)
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/live.log", os.O_RDWR)
        self.assertEqual(raised.exception.errno, errno.EAGAIN)
        self.assertTrue(self.bounds[-1].grew)
        self.assertFalse((self.cache_dir / "live.log").exists())

    def test_source_appending_during_a_refresh_keeps_serving_last_good(self):
        import contextlib
        self.rewrite_member("live.log", b"version 1", mtime=1_000_000_000)
        self.assertEqual(self.read_through("/live.log"), b"version 1")
        self.rewrite_member("live.log", b"version 2 grows", mtime=1_100_000_000)
        self.age_checks()
        with contextlib.ExitStack() as stack:
            for patcher in self.appending_during_copy():
                stack.enter_context(patcher)
            self.assertEqual(self.read_through("/live.log"), b"version 1")
        self.assertTrue(all(b.written <= b.cap for b in self.bounds))

    def test_unstable_fill_of_a_member_without_owner_read_is_refused(self):
        real = self.member.stat
        for mode in (0o200, 0o040):
            with self.subTest(mode=oct(mode)):
                self.rewrite_member("live.log", b"line 1\n")

                def reported(path, mode=mode):
                    st = real(path)
                    return SimpleNamespace(st_size=st.st_size, st_mtime_ns=st.st_mtime_ns,
                                           st_mtime=st.st_mtime, st_mode=stat.S_IFREG | mode)

                with self.growing_copy(), patch.object(self.member, "stat", side_effect=reported):
                    with self.assertRaises(FuseOSError) as raised:
                        self.fs.open("/live.log", os.O_RDONLY)
                self.assertEqual(raised.exception.errno, errno.EACCES)
                self.assertEqual(self.partials(), [])
                self.assertEqual(self.fs.cache_manager._reserved_bytes, 0)

    def test_any_read_denial_of_the_fill_wins(self):
        # R5: a size-only probe, a confirmation with 0o200, then a size-only
        # second confirmation: the read denial still applies.
        self.rewrite_member("live.log", b"line 1\n")
        real, seen = self.member.stat, []

        def stats(path):
            seen.append(path)
            st = real(path)
            if len(seen) == 2:  # same size, a token and 0o200: no retry, unconfirmed
                return SimpleNamespace(st_size=st.st_size, st_mtime=st.st_mtime,
                                       st_mtime_ns=st.st_mtime_ns, st_mode=stat.S_IFREG | 0o200)
            return SimpleNamespace(st_size=st.st_size)

        with patch.object(self.member, "stat", side_effect=stats):
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/live.log", os.O_RDONLY)
        self.assertEqual(raised.exception.errno, errno.EACCES)
        self.assertEqual(self.partials(), [])
        self.assertEqual(self.fs.cache_manager._reserved_bytes, 0)

    def test_enospc_when_the_reservation_does_not_fit(self):
        self.rewrite_member("live.log", b"line 1\n")
        with patch.object(self.fs.cache_manager, "ensure_space", return_value=False):
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/live.log", os.O_RDONLY)
        self.assertEqual(raised.exception.errno, errno.ENOSPC)

    def stale_copy(self):
        """A cached copy whose refresh failed: the last good one, marked stale."""
        self.rewrite_member("live.log", b"version 1", mtime=1_000_000_000)
        self.assertEqual(self.read_through("/live.log"), b"version 1")
        self.rewrite_member("live.log", b"version 2!", mtime=1_100_000_000)
        self.age_checks()
        with self.growing_copy():
            self.assertEqual(self.read_through("/live.log"), b"version 1")
        return self.cache_dir / "live.log"

    def test_stale_mark_follows_the_inode(self):
        from storage.cache_metadata import STALE_LAST_GOOD_XATTR
        cached = self.stale_copy()
        os.getxattr(cached, STALE_LAST_GOOD_XATTR)
        # An editor's save: a new file renamed over the name is not stale.
        replacement = cached.with_name("save.tmp")
        replacement.write_bytes(b"saved")
        os.replace(replacement, cached)
        self.fs.release("/live.log", self.fs.open("/live.log", os.O_RDWR))

    def test_stale_mark_goes_with_a_rename_through_the_mount(self):
        cached = self.stale_copy()
        self.fs.rename("/live.log", "/moved.log")
        with self.assertRaises(FuseOSError) as raised:
            self.fs.open("/moved.log", os.O_RDWR)
        self.assertEqual(raised.exception.errno, errno.EAGAIN)
        self.assertFalse(cached.exists())

    def test_a_failed_o_trunc_open_leaves_the_mark(self):
        import storage.fs as fs_module
        cached = self.stale_copy()
        real_open = os.open

        def failing(path, flags, *a, **kw):
            if flags & os.O_TRUNC and str(path) == str(cached):
                raise OSError(errno.EMFILE, "too many open files")
            return real_open(path, flags, *a, **kw)

        with patch.object(fs_module.os, "open", side_effect=failing):
            with self.assertRaises(FuseOSError):
                self.fs.open("/live.log", os.O_RDWR | os.O_TRUNC)
        with self.assertRaises(FuseOSError) as raised:
            self.fs.open("/live.log", os.O_RDWR)
        self.assertEqual(raised.exception.errno, errno.EAGAIN)
        # A successful O_TRUNC open clears it.
        self.fs.release("/live.log", self.fs.open("/live.log", os.O_RDWR | os.O_TRUNC))
        self.fs.release("/live.log", self.fs.open("/live.log", os.O_RDWR))

    def test_stale_mark_survives_a_restart(self):
        self.stale_copy()
        self.restart()
        self.member.set_available(False)
        try:
            self.assertEqual(self.read_through("/live.log"), b"version 1")
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/live.log", os.O_RDWR)
            self.assertEqual(raised.exception.errno, errno.EAGAIN)
        finally:
            self.member.set_available(True)

    def test_a_token_seen_in_the_first_confirmation_sticks(self):
        # B4: a size-only probe, a tokened first confirmation, then a
        # size-only or failing second one: never installed.
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        real = self.member.stat
        for second in ("size-only", "fails"):
            with self.subTest(second=second):
                seen = []

                def stats(path):
                    seen.append(path)
                    st = real(path)
                    if len(seen) == 2:
                        return st  # the first confirmation has a token
                    if len(seen) == 3 and second == "fails":
                        raise OSError(errno.EIO, "stat failed")
                    return SimpleNamespace(st_size=st.st_size)

                with patch.object(self.member, "stat", side_effect=stats):
                    with self.assertRaises(FuseOSError) as raised:
                        self.fs.open("/f.txt", os.O_WRONLY)
                self.assertEqual(raised.exception.errno, errno.EAGAIN)
                self.assertFalse((self.cache_dir / "f.txt").exists())

    def test_a_daemon_restarting_offline_mid_refresh_serves_the_named_copy(self):
        # The crash point: the refresh is still copying beside the copy.
        # The copy stays named throughout, so a new daemon starting offline
        # then serves it read-only.
        self.rewrite_member("live.log", b"version 1", mtime=1_000_000_000)
        self.assertEqual(self.read_through("/live.log"), b"version 1")
        self.rewrite_member("live.log", b"version 2!", mtime=1_100_000_000)
        self.age_checks()
        copy, seen = self.member.copy_to_local, {}

        def crash_point(path, temp):
            copy(path, temp)
            restarted = self._make_fs()
            try:
                self.member.set_available(False)
                fh = restarted.open("/live.log", os.O_RDONLY)
                try:
                    seen["bytes"] = restarted.read("/live.log", 64, 0, fh)
                finally:
                    restarted.release("/live.log", fh)
            finally:
                self.member.set_available(True)
                restarted.sync_manager.stop()
                restarted.state_manager.close()

        with patch.object(self.member, "copy_to_local", side_effect=crash_point):
            self.read_through("/live.log")
        self.assertEqual(seen["bytes"], b"version 1")

    def test_writable_open_retries_then_eagain_and_a_stable_version_publishes(self):
        self.rewrite_member("live.log", b"line 1\n")
        with self.growing_copy(), self.count_fetches() as fetches:
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/live.log", os.O_RDWR)
        self.assertEqual(raised.exception.errno, errno.EAGAIN)
        self.assertEqual(fetches.call_count, 2)  # one bounded retry
        self.assertEqual(self.partials(), [])
        # The member stops changing: the next open publishes normally.
        fh = self.fs.open("/live.log", os.O_RDWR)
        self.fs.release("/live.log", fh)
        self.assertEqual((self.cache_dir / "live.log").read_bytes(),
                         (self.member_dir / "live.log").read_bytes())
        self.assertEqual(self.fs.state_manager.get_file_metadata("/live.log").sync_status,
                         SyncStatus.CLEAN)

    def test_last_good_copy_is_kept_through_a_failed_refresh(self):
        self.rewrite_member("live.log", b"version 1", mtime=1_000_000_000)
        self.assertEqual(self.read_through("/live.log"), b"version 1")
        row = self.fs.state_manager.get_file_metadata("/live.log")
        self.rewrite_member("live.log", b"version 2!", mtime=1_100_000_000)
        self.age_checks()
        # The refresh never settles (the member keeps growing).
        with self.growing_copy():
            self.assertEqual(self.read_through("/live.log"), b"version 1")
            # Within the age, a writable open finds the copy known stale.
            with self.count_fetches() as fetches:
                with self.assertRaises(FuseOSError) as raised:
                    self.fs.open("/live.log", os.O_RDWR)
            self.assertEqual(raised.exception.errno, errno.EAGAIN)
            self.assertEqual(fetches.call_count, 0)
            self.age_checks()
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/live.log", os.O_RDWR)
            self.assertEqual(raised.exception.errno, errno.EAGAIN)
        self.assertEqual((self.cache_dir / "live.log").read_bytes(), b"version 1")
        kept = self.fs.state_manager.get_file_metadata("/live.log")
        self.assertEqual((kept.size, kept.last_modified, kept.sync_status),
                         (row.size, row.last_modified, row.sync_status))
        self.assertEqual(self.partials(), [])
        # Within the age the failed refresh counts as the check: no transfer.
        with self.count_fetches() as fetches:
            self.assertEqual(self.read_through("/live.log"), b"version 1")
        self.assertEqual(fetches.call_count, 0)
        # Settled: the next open past the age publishes the member's version,
        # and the stale mark goes with it (a writable open works again).
        self.age_checks()
        self.assertEqual(self.read_through("/live.log"),
                         (self.member_dir / "live.log").read_bytes())
        self.fs.release("/live.log", self.fs.open("/live.log", os.O_RDWR))


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
            # The refresh failed: a read-only open keeps the last good copy
            # (CTO-208-LIVE-READ); a writable one reports the I/O error.
            self.assertEqual(b"changed on fast", self.read_through("/f.txt"))
            self.age_checks()
            with self.assertRaises(FuseOSError) as raised:
                self.fs.open("/f.txt", os.O_RDWR)
            self.assertEqual(raised.exception.errno, errno.EIO)
        # Selected but never delivered: the forced refetch is still owed.
        self.assertEqual({"fast"}, self.fs._recheck["/f.txt"])
        (fast_dir / "f.txt").write_bytes(b"CHANGED ON FAST")
        self.age_checks()  # the failed refresh was a check: next one after the age
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


class TestRefreshRound5(RevalidateTestBase):
    """#208 round 5: the stale mark's coverage, a refreshed inode's own
    record, read denials, the copied descriptor's evidence, the refresh's
    pin, errnos and failures by exception."""

    growing_copy = TestLiveRead.growing_copy
    partials = TestLiveRead.partials
    stale_copy = TestLiveRead.stale_copy

    def stale_mark(self, name="live.log"):
        from storage.cache_metadata import STALE_LAST_GOOD_XATTR
        try:
            os.getxattr(self.cache_dir / name, STALE_LAST_GOOD_XATTR)
        except OSError:
            return False
        return True

    def assert_errno(self, code, fn, *args):
        with self.assertRaises(FuseOSError) as raised:
            fn(*args)
        self.assertEqual(raised.exception.errno, code)

    def changed_v1(self):
        """v1 cached and checked, the member at v2, the check past the age."""
        self.rewrite_member("live.log", b"version 1", mtime=1_000_000_000)
        self.assertEqual(self.read_through("/live.log"), b"version 1")
        self.rewrite_member("live.log", b"version 2!", mtime=1_100_000_000)
        self.age_checks()

    def test_discovery_preserves_unindexed_member_errors(self):
        for code in (errno.EACCES, errno.EIO):
            with self.subTest(code=code), patch.object(self.member, "exists", return_value=False), \
                    patch.object(self.member, "exists_or_raise", side_effect=OSError(code, "probe")):
                with self.assertRaises(OSError) as raised:
                    self.fs.open("/unindexed", os.O_RDONLY)
                self.assertEqual(raised.exception.errno, code)

    def test_real_confirmation_denial_cannot_serve_last_good(self):
        self.changed_v1()
        new_stat = self.member.stat("/live.log")
        with patch.object(self.member, "stat", side_effect=[
                new_stat, PermissionError(errno.EACCES, "confirmation denied")]):
            self.assert_errno(errno.EACCES, self.fs.open, "/live.log", os.O_RDONLY)
        self.assertFalse((self.cache_dir / "live.log").exists())
        self.assertEqual(self.partials(), [])

    def test_descriptor_denial_survives_metadata_copy_error(self):
        from storage.backends import local

        self.changed_v1()
        source = self.member_dir / "live.log"
        real_stream = local.copy_bounded_stream

        def revoke_after_read(read, write, bound):
            real_stream(read, write, bound)
            source.chmod(0o200)

        with patch.object(local, "copy_bounded_stream", side_effect=revoke_after_read), \
                patch.object(local, "_listxattr", side_effect=OSError(errno.EIO, "attrs")), \
                patch.object(self.fs.sync_manager, "queue_sync") as sync:
            self.assert_errno(errno.EACCES, self.read_through, "/live.log")
        sync.assert_not_called()
        self.assertFalse((self.cache_dir / "live.log").exists())
        self.assertEqual(self.partials(), [])

    def test_readable_source_metadata_copy_error_keeps_eio(self):
        from storage.backends import local

        self.changed_v1()
        with patch.object(local, "_listxattr", side_effect=OSError(errno.EIO, "attrs")):
            self.assert_errno(errno.EIO, self.fs.open, "/live.log", os.O_RDWR)
        self.assertEqual(self.read_through("/live.log"), b"version 1")
        self.assertEqual(self.partials(), [])

    def test_aged_stat_denial_is_not_an_outage(self):
        self.changed_v1()
        with patch.object(self.member, "stat", side_effect=PermissionError(errno.EACCES, "denied")):
            with self.assertRaises(OSError) as raised:
                self.fs.open("/live.log", os.O_RDONLY)
        self.assertEqual(raised.exception.errno, errno.EACCES)
        self.assertFalse((self.cache_dir / "live.log").exists())

    def test_read_mode_denial_wins_without_refresh_capacity(self):
        self.changed_v1()
        (self.member_dir / "live.log").chmod(0o200)
        with patch.object(self.fs.cache_manager, "ensure_space", return_value=False) as reserve:
            self.assert_errno(errno.EACCES, self.fs.open, "/live.log", os.O_RDONLY)
        reserve.assert_not_called()
        self.assertFalse((self.cache_dir / "live.log").exists())

    def test_larger_confirmation_denial_precedes_retry_space(self):
        path = "/live.log"
        self.changed_v1()
        copy = self.member.copy_to_local

        def grow_and_revoke(virtual_path, staging):
            copy(virtual_path, staging)
            self.rewrite_member("live.log", b"version 3 is larger", mtime=1_200_000_000).chmod(0o200)

        with patch.object(self.member, "copy_to_local", side_effect=grow_and_revoke) as copies, \
                patch.object(self.fs.cache_manager, "_reserve_more", return_value=False) as reserve, \
                patch.object(self.fs, "_record_fetched") as publish, \
                patch.object(self.member, "copy_from_local") as upload, \
                patch.object(self.fs.sync_manager, "queue_sync") as queue:
            self.assert_errno(errno.EACCES, self.read_through, path)
        copies.assert_called_once()
        reserve.assert_not_called()
        publish.assert_not_called()
        upload.assert_not_called()
        queue.assert_not_called()
        self.assertFalse((self.cache_dir / "live.log").exists())
        self.assertEqual(self.partials(), [])
        self.assertEqual(self.fs.cache_manager._reserved_bytes, 0)

    def test_readable_larger_confirmation_preserves_retry_enospc(self):
        path = "/live.log"
        self.changed_v1()
        with self.growing_copy(), patch.object(
                self.fs.cache_manager, "_reserve_more", return_value=False) as reserve:
            self.assert_errno(errno.ENOSPC, self.fs.open, path, os.O_RDWR)
            self.age_checks()
            self.assertEqual(self.read_through(path), b"version 1")
        self.assertEqual(reserve.call_count, 2)
        self.assertTrue(self.stale_mark())
        self.assertEqual(self.partials(), [])
        self.assertEqual(self.fs.cache_manager._reserved_bytes, 0)

    def test_aged_same_version_write_denial_survives_the_read_that_observed_it(self):
        src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        src.chmod(0o644)
        self.assertEqual(self.read_through("/f.txt"), b"original")
        src.chmod(0o444)
        self.age_checks()
        with self.count_fetches() as copies:
            self.assertEqual(self.read_through("/f.txt"), b"original")
            for flags in (os.O_WRONLY, os.O_RDWR):
                self.assert_errno(errno.EACCES, self.fs.open, "/f.txt", flags)
        copies.assert_not_called()
        self.fs.chmod("/f.txt", 0o644)
        self.age_checks()
        self.fs.release("/f.txt", self.fs.open("/f.txt", os.O_RDWR))

    def test_ns_only_baseline_refetches_an_older_same_size_version_after_restart(self):
        real_stat = self.member.stat

        def ns_only(path):
            st = real_stat(path)
            return SimpleNamespace(st_size=st.st_size, st_mtime_ns=st.st_mtime_ns,
                                   st_mode=st.st_mode)

        self.rewrite_member("f.txt", b"original", mtime=1_700_000_000)
        with patch.object(self.member, "stat", side_effect=ns_only):
            self.assertEqual(self.read_through("/f.txt"), b"original")
            self.restart()
            self.rewrite_member("f.txt", b"newbytes", mtime=1_600_000_000)
            with self.count_fetches() as copies:
                fh = self.fs.open("/f.txt", os.O_RDWR)
                try:
                    self.assertEqual(self.fs.read("/f.txt", 32, 0, fh), b"newbytes")
                finally:
                    self.fs.release("/f.txt", fh)
            self.assertEqual(copies.call_count, 1)

    def test_colliding_float_mtimes_do_not_clear_the_known_stale_mark(self):
        from storage.cache_metadata import STALE_LAST_GOOD_XATTR
        src = self.rewrite_member("f.txt", b"original")
        initial = 1_700_000_000_000_000_000
        os.utime(src, ns=(initial, initial))
        self.assertEqual(self.read_through("/f.txt"), b"original")
        cached = self.cache_dir / "f.txt"
        os.setxattr(cached, STALE_LAST_GOOD_XATTR, b"1")
        self.restart()
        src.write_bytes(b"newbytes")
        os.utime(src, ns=(initial + 1, initial + 1))
        self.assertEqual(src.stat().st_mtime, cached.stat().st_mtime)
        self.assertNotEqual(src.stat().st_mtime_ns, cached.stat().st_mtime_ns)
        with patch.object(self.fs.cache_manager, "ensure_space", return_value=False):
            self.assert_errno(errno.ENOSPC, self.fs.open, "/f.txt", os.O_RDWR)
        self.assertEqual(os.getxattr(cached, STALE_LAST_GOOD_XATTR), b"1")
        self.assertEqual(self.read_through("/f.txt"), b"original")

    def test_stable_descriptor_cannot_confirm_the_name_after_a_replacement(self):
        from storage.backends import local
        src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        replacement = self.rewrite_member("replacement", b"newbytes", mtime=1_100_000_000)
        real_stream = local.copy_bounded_stream

        def replace_after_open(read, write, bound):
            os.replace(replacement, src)
            real_stream(read, write, bound)
            self.assertEqual(bound.source_stats[0].st_ino, os.fstat(read.__self__.fileno()).st_ino)
            self.assertNotEqual(bound.source_stats[0].st_ino, src.stat().st_ino)

        with patch.object(self.member, "stat", return_value=SimpleNamespace(st_size=8)), \
                patch.object(local, "copy_bounded_stream", side_effect=replace_after_open):
            self.assert_errno(errno.EAGAIN, self.fs.open, "/f.txt", os.O_RDONLY)
        self.assertEqual(src.read_bytes(), b"newbytes")
        self.assertFalse((self.cache_dir / "f.txt").exists())
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/f.txt"))

    def test_bounded_fill_copies_xattrs_from_the_source_of_its_bytes(self):
        from storage.backends import local

        src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        replacement = self.rewrite_member("replacement", b"newbytes", mtime=1_000_000_000)
        aside = src.with_name("aside")
        os.setxattr(src, "user.tag", b"original attrs")
        os.setxattr(replacement, "user.tag", b"replacement attrs")
        real_list, real_stat = local._listxattr, self.member.stat
        swapped = False

        def list_during_swap(entry):
            nonlocal swapped
            if entry == src or isinstance(entry, int):
                os.rename(src, aside)
                os.replace(replacement, src)
                swapped = True
            return real_list(entry)

        def restore_before_confirmation(path):
            if swapped and aside.exists():
                os.replace(aside, src)
            return real_stat(path)

        with patch.object(local, "_listxattr", side_effect=list_during_swap), \
                patch.object(self.member, "stat", side_effect=restore_before_confirmation):
            self.assertEqual(self.read_through("/f.txt"), b"original")
        self.assertTrue(swapped)
        self.assertEqual(os.getxattr(self.cache_dir / "f.txt", "user.tag"), b"original attrs")

    def test_path_identity_can_disprove_matching_size_and_mtime(self):
        from storage.backends import local

        src = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        replacement = self.rewrite_member("replacement", b"newbytes", mtime=1_000_000_000)
        real_stream = local.copy_bounded_stream

        def replace_after_open(read, write, bound):
            os.replace(replacement, src)
            real_stream(read, write, bound)

        with patch.object(local, "copy_bounded_stream", side_effect=replace_after_open):
            self.assert_errno(errno.EAGAIN, self.fs.open, "/f.txt", os.O_RDONLY)
        self.assertFalse((self.cache_dir / "f.txt").exists())
        self.assertEqual(self.read_through("/f.txt"), b"newbytes")

    def test_the_copy_is_marked_before_the_refresh_stages(self):
        # A daemon restarting offline mid-refresh finds the copy marked: no
        # writer is admitted on bytes known to be old.
        self.changed_v1()
        copy, seen = self.member.copy_to_local, {}

        def crash_point(path, temp):
            seen["marked"] = self.stale_mark()
            copy(path, temp)
            restarted = self._make_fs()
            try:
                self.member.set_available(False)
                with self.assertRaises(FuseOSError) as raised:
                    restarted.open("/live.log", os.O_RDWR)
                seen["errno"] = raised.exception.errno
                seen["read"] = restarted.read(
                    "/live.log", 64, 0, fh := restarted.open("/live.log", os.O_RDONLY))
                restarted.release("/live.log", fh)
            finally:
                self.member.set_available(True)
                restarted.sync_manager.stop()
                restarted.state_manager.close()

        with patch.object(self.member, "copy_to_local", side_effect=crash_point):
            self.read_through("/live.log")
        self.assertEqual(seen, {"marked": True, "errno": errno.EAGAIN, "read": b"version 1"})
        # The restart swept the in-flight copy, so that refresh failed and the
        # copy stays marked (a transfer's own ENOENT is not the member's
        # absence). The next refresh's verified copy is a new inode, never
        # marked.
        self.assertTrue(self.stale_mark())
        self.age_checks()
        self.assertEqual(self.read_through("/live.log"), b"version 2!")
        self.assertFalse(self.stale_mark())

    def test_a_mark_that_cannot_be_written_takes_the_invalidation(self):
        import storage.fs as fs_module
        from storage.cache_metadata import STALE_LAST_GOOD_XATTR
        self.changed_v1()
        real = os.setxattr

        def refuse(path, name, *a, **kw):
            if name == STALE_LAST_GOOD_XATTR:
                raise OSError(errno.ENOSPC, "no xattr space")
            return real(path, name, *a, **kw)

        with patch.object(fs_module.os, "setxattr", side_effect=refuse), self.growing_copy():
            # The unmarked copy is dropped. The unstable refetch has no
            # last-good copy to fall back to.
            self.assert_errno(errno.EAGAIN, self.fs.open, "/live.log", os.O_RDONLY)
            self.assert_errno(errno.EAGAIN, self.fs.open, "/live.log", os.O_RDWR)
        self.assertFalse((self.cache_dir / "live.log").exists())

    def test_an_unreadable_stale_mark_refuses_writes(self):
        from storage.cache_metadata import STALE_LAST_GOOD_XATTR
        self.stale_copy()
        getxattr = os.getxattr

        def read(path, name, *args, **kwargs):
            if name == STALE_LAST_GOOD_XATTR:
                raise OSError(errno.EIO, "xattr could not be read")
            return getxattr(path, name, *args, **kwargs)

        with patch.object(os, "getxattr", side_effect=read):
            self.assert_errno(errno.EAGAIN, self.fs.open, "/live.log", os.O_RDWR)
            self.assertEqual(self.read_through("/live.log"), b"version 1")

    def test_older_same_size_evidence_does_not_clear_a_mark_without_a_baseline(self):
        cached = self.stale_copy()
        with self.fs._member_checks_lock:
            check = self.fs._member_checks["/live.log"]
            self.fs._member_checks["/live.log"] = dataclasses.replace(check, members={})
        self.rewrite_member("live.log", b"older!!!!", mtime=900_000_000)
        for _ in range(2):
            self.age_checks()
            self.assert_errno(errno.EAGAIN, self.fs.open, "/live.log", os.O_RDWR)
            self.assertTrue(self.stale_mark())
        self.assertEqual(cached.read_bytes(), b"version 1")

    def test_size_only_can_confirm_a_never_versioned_copy(self):
        from storage.cache_metadata import STALE_LAST_GOOD_XATTR
        self.rewrite_member("live.log", b"version 1")
        size_and_mode = SimpleNamespace(st_size=9, st_mode=stat.S_IFREG | 0o644)
        with self.without_descriptor_evidence(), patch.object(
                self.member, "stat", return_value=size_and_mode):
            self.assertEqual(self.read_through("/live.log"), b"version 1")
            os.setxattr(self.cache_dir / "live.log", STALE_LAST_GOOD_XATTR, b"1")
            self.age_checks()
            self.fs.release("/live.log", self.fs.open("/live.log", os.O_RDWR))
        self.assertFalse(self.stale_mark())

    def test_touch_of_a_marked_copy_is_refused_and_records_nothing(self):
        cached = self.stale_copy()
        before = cached.stat().st_mtime
        self.assert_errno(errno.EAGAIN, self.fs.utimens, "/live.log", (5.0, 5.0))
        self.assertEqual(cached.stat().st_mtime, before)
        self.assertEqual(self.fs.state_manager.get_file_metadata("/live.log").sync_status,
                         SyncStatus.CLEAN)
        # Read-only stays served.
        self.assertEqual(self.read_through("/live.log"), b"version 1")

    def test_a_link_of_a_marked_copy_is_refused(self):
        # The mount has no hard links at all (fusepy's default, EROFS); a
        # marked copy is no exception, and nothing is recorded.
        self.stale_copy()
        self.assert_errno(errno.EROFS, self.fs.link, "/other.log", "/live.log")
        self.assertEqual(self.fs.state_manager.get_file_metadata("/live.log").sync_status,
                         SyncStatus.CLEAN)
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/other.log"))

    def test_the_mark_clears_when_the_member_is_back_at_the_copys_version(self):
        self.stale_copy()
        self.rewrite_member("live.log", b"version 1", mtime=1_000_000_000)
        self.age_checks()
        self.assertEqual(self.read_through("/live.log"), b"version 1")
        self.assertFalse(self.stale_mark())
        self.fs.release("/live.log", self.fs.open("/live.log", os.O_RDWR))

    def test_the_mark_clears_when_the_member_no_longer_has_the_file(self):
        self.stale_copy()
        (self.member_dir / "live.log").unlink()
        self.age_checks()
        self.assertEqual(self.read_through("/live.log"), b"version 1")
        self.assertFalse(self.stale_mark())
        self.fs.release("/live.log", self.fs.open("/live.log", os.O_RDWR))

    def test_a_refresh_that_finds_the_file_gone_clears_the_mark(self):
        # Gone between the check and the fill: the copy is the one left.
        self.stale_copy()
        self.age_checks()
        with patch.object(self.member, "exists_or_raise", return_value=False):
            fh = self.fs.open("/live.log", os.O_RDWR)
            self.fs.release("/live.log", fh)
        self.assertFalse(self.stale_mark())

    def refreshed_with_checksums(self):
        from storage.models import FileLocation
        self.changed_v1()
        row = self.fs.state_manager.get_file_metadata("/live.log")
        row.checksum = "hash-of-v1"
        self.fs.state_manager.set_file_metadata(row)
        for name in ("member", "cache"):
            self.fs.state_manager.add_file_location(FileLocation(
                virtual_path="/live.log", backend_name=name, checksum="hash-of-v1",
                last_synced=time.time(), is_cached=name == "cache"))
        self.fs.cache_manager.sync_cache_metadata(
            "/live.log", self.fs.state_manager.get_file_metadata("/live.log"),
            cache_path=self.cache_dir / "live.log")
        return self.cache_inode("live.log")

    def test_an_in_place_refresh_carries_no_old_checksum(self):
        inode = self.refreshed_with_checksums()
        self.assertEqual(self.read_through("/live.log"), b"version 2!")
        self.assertNotEqual(self.cache_inode("live.log"), inode)
        row = self.fs.state_manager.get_file_metadata("/live.log")
        self.assertIsNone(row.checksum)
        self.assertEqual((row.size, row.sync_status), (10, SyncStatus.CLEAN))
        for loc in self.fs.state_manager.get_file_locations("/live.log"):
            self.assertNotEqual(loc.checksum, "hash-of-v1", loc.backend_name)
        record = read_cache_metadata(self.cache_dir / "live.log")
        self.assertIsNone(record.checksum)

    def seed_same_stat_replacement_with_a_member_digest(self):
        from storage.checksums import ChecksumRecord

        path = "/live.log"
        self.rewrite_member("live.log", b"version 1", mtime=1_000_000_000)
        self.assertEqual(self.read_through(path), b"version 1")
        state = self.fs.state_manager
        self.assertIsNone(state.get_file_metadata(path).checksum)
        state.put_member_checksum(path, "member", ChecksumRecord(
            checksum="old-member-hash", timestamp=time.time(),
            basis=(9, 1_000_000_000, None)))
        self.rewrite_member("live.log", b"version 2", mtime=1_000_000_000)
        self.fs._recheck[path] = {"member"}
        self.age_checks()

    def test_equal_stat_replacement_cannot_leave_old_digest_before_row_update(self):
        self.seed_same_stat_replacement_with_a_member_digest()
        with patch.object(self.fs, "_record_fetched", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.fs.open("/live.log", os.O_RDONLY)
        self.restart()
        self.assertEqual((self.cache_dir / "live.log").read_bytes(), b"version 2")
        self.assertIsNone(self.fs.state_manager.get_member_checksum("/live.log", "member"))

    def test_equal_stat_replacement_cannot_leave_old_digest_after_row_update(self):
        self.seed_same_stat_replacement_with_a_member_digest()
        record = self.fs.state_manager.record_cached_stat

        def crash_after_row(*args, **kwargs):
            self.assertTrue(record(*args, **kwargs))
            raise KeyboardInterrupt

        with patch.object(self.fs.state_manager, "record_cached_stat", side_effect=crash_after_row):
            with self.assertRaises(KeyboardInterrupt):
                self.fs.open("/live.log", os.O_RDONLY)
        self.restart()
        self.assertEqual(self.fs.sweep_stale_cache_metadata(), 0)
        self.assertEqual((self.cache_dir / "live.log").read_bytes(), b"version 2")
        self.assertIsNone(self.fs.state_manager.get_member_checksum("/live.log", "member"))

    def test_refresh_does_not_replace_bytes_when_old_digest_cannot_be_removed(self):
        import sqlite3

        self.seed_same_stat_replacement_with_a_member_digest()
        with patch.object(self.fs.state_manager, "delete_member_checksums",
                          side_effect=sqlite3.OperationalError("database is locked")):
            self.assertEqual(self.read_through("/live.log"), b"version 1")
            self.age_checks()
            self.assert_errno(errno.EIO, self.fs.open, "/live.log", os.O_RDWR)
        self.assertEqual((self.cache_dir / "live.log").read_bytes(), b"version 1")
        self.assertTrue(self.stale_mark())

    def test_a_crash_after_the_replace_heals_clean(self):
        self.refreshed_with_checksums()

        class Crash(BaseException):  # a crash, not a failure the open handles
            pass

        uploads = patch.object(self.member, "copy_from_local",
                               side_effect=AssertionError("upload of a refreshed copy"))
        with patch.object(self.fs, "_record_fetched", side_effect=Crash):
            with self.assertRaises(Crash):
                self.fs.open("/live.log", os.O_RDONLY)
        # Named, the row still describing v1.
        self.assertEqual((self.cache_dir / "live.log").read_bytes(), b"version 2!")
        self.assertEqual(self.fs.state_manager.get_file_metadata("/live.log").size, 9)
        self.restart()
        with uploads:
            self.assertEqual(self.fs.sweep_stale_cache_metadata(), 0)
            row = self.fs.state_manager.get_file_metadata("/live.log")
            self.assertEqual((row.size, row.sync_status, row.checksum),
                             (10, SyncStatus.CLEAN, None))
            self.assertTrue(all(not loc.checksum for loc in
                                self.fs.state_manager.get_file_locations("/live.log")))
            self.assertEqual(self.read_through("/live.log"), b"version 2!")
            self.assertEqual(self.fs.state_manager.get_file_metadata("/live.log").sync_status,
                             SyncStatus.CLEAN)

    def test_failed_invalidation_repair_cannot_admit_a_stale_writer(self):
        import sqlite3

        path = "/live.log"
        self.crash_after_installing_refresh()
        self.rewrite_member("live.log", b"version 3 is newest", mtime=1_200_000_000)
        cached = self.cache_dir / "live.log"
        state = self.fs.state_manager
        record = state.record_cached_stat
        calls = []

        def fail_once(*args, **kwargs):
            calls.append(args[0])
            if len(calls) == 1:
                raise sqlite3.OperationalError("row repair failed once")
            return record(*args, **kwargs)

        with patch.object(state, "record_cached_stat", side_effect=fail_once), \
                patch.object(self.fs.sync_manager, "queue_sync") as sync:
            self.assert_errno(errno.EIO, self.fs.open, path, os.O_RDWR)
            self.assertEqual(calls, [path])
            self.assertEqual(cached.read_bytes(), b"version 2!")
            self.assertEqual(state.get_file_metadata(path).size, 9)
            self.assertNotIn(path, self.fs._member_checks)
            sync.assert_not_called()

            # A reader can still recover the inode offline. Completing that
            # repair does not make it a safe write baseline for v3.
            self.member.set_available(False)
            try:
                self.assertEqual(self.read_through(path), b"version 2!")
                self.assert_errno(errno.EAGAIN, self.fs.open, path, os.O_RDWR)
            finally:
                self.member.set_available(True)

            fh = self.fs.open(path, os.O_RDWR)
            try:
                self.assertEqual(self.fs.read(path, 64, 0, fh), b"version 3 is newest")
            finally:
                self.fs.release(path, fh)

    def test_unpersisted_stale_mark_still_refuses_offline_writes(self):
        import sqlite3

        from storage.cache_metadata import STALE_LAST_GOOD_XATTR

        path = "/live.log"
        self.crash_after_installing_refresh()
        self.rewrite_member("live.log", b"version 3 is newest", mtime=1_200_000_000)
        cached = self.cache_dir / "live.log"
        setxattr = os.setxattr

        def fail_stale_mark(target, name, *args, **kwargs):
            if name == STALE_LAST_GOOD_XATTR and Path(target) == cached:
                raise OSError(errno.ENOSPC, "cannot persist stale mark")
            return setxattr(target, name, *args, **kwargs)

        with patch.object(self.fs.state_manager, "record_cached_stat",
                          side_effect=sqlite3.OperationalError("row repair failed once")) as repair, \
                patch.object(os, "setxattr", side_effect=fail_stale_mark):
            self.assert_errno(errno.EIO, self.fs.open, path, os.O_RDWR)
        repair.assert_called_once()
        with self.assertRaises(OSError) as absent:
            os.getxattr(cached, STALE_LAST_GOOD_XATTR)
        self.assertEqual(absent.exception.errno, errno.ENODATA)

        self.member.set_available(False)
        try:
            self.assertEqual(self.read_through(path), b"version 2!")
            for flags in (os.O_WRONLY, os.O_RDWR):
                self.assert_errno(errno.EAGAIN, self.fs.open, path, flags)
            self.assert_errno(errno.EAGAIN, self.fs.truncate, path, 1)
            self.assert_errno(errno.EAGAIN, self.fs.utimens, path, (10, 10))
            self.assertEqual(cached.read_bytes(), b"version 2!")
        finally:
            self.member.set_available(True)

        fh = self.fs.open(path, os.O_RDWR)
        try:
            self.assertEqual(self.fs.read(path, 64, 0, fh), b"version 3 is newest")
        finally:
            self.fs.release(path, fh)
        self.assertFalse(self.fs._is_stale_last_good(cached))

    def test_read_denial_survives_repair_failure_and_offline_reopen(self):
        import sqlite3

        from storage.cache_metadata import STALE_LAST_GOOD_XATTR

        path = "/live.log"
        self.crash_after_installing_refresh()
        (self.member_dir / "live.log").chmod(0o200)
        cached = self.cache_dir / "live.log"
        with patch.object(self.fs.state_manager, "record_cached_stat",
                          side_effect=sqlite3.OperationalError("row repair failed once")) as repair:
            self.assert_errno(errno.EACCES, self.fs.open, path, os.O_RDONLY)
        repair.assert_called_once()
        self.restart()

        self.member.set_available(False)
        try:
            for flags in (os.O_RDONLY, os.O_RDWR):
                self.assert_errno(errno.EACCES, self.fs.open, path, flags)
            self.assert_errno(errno.EAGAIN, self.fs.open, path, os.O_WRONLY)
            self.assert_errno(errno.EAGAIN, self.fs.truncate, path, 1)
            self.assertEqual(cached.read_bytes(), b"version 2!")
            self.assertEqual(os.getxattr(cached, STALE_LAST_GOOD_XATTR), b"read-denied")
        finally:
            self.member.set_available(True)

        # The source's new accessible version must be fetched to lift the
        # retained inode's admission restriction.
        (self.member_dir / "live.log").chmod(0o644)
        self.rewrite_member("live.log", b"version 3 is newest", mtime=1_200_000_000)
        self.assertEqual(self.read_through(path), b"version 3 is newest")
        self.assertFalse(self.fs._is_stale_last_good(cached))

    def test_unreadable_recovery_record_does_not_make_a_refresh_dirty(self):
        from storage.cache_metadata import XATTR_CACHE_METADATA_NAME

        self.crash_after_installing_refresh()
        cached = self.cache_dir / "live.log"
        state = self.fs.state_manager
        getxattr = os.getxattr

        def fail_recovery_read(path, name, *args, **kwargs):
            if name == XATTR_CACHE_METADATA_NAME and Path(path) == cached:
                raise OSError(errno.EIO, "cannot read recovery record")
            return getxattr(path, name, *args, **kwargs)

        with patch.object(os, "getxattr", side_effect=fail_recovery_read), \
                patch.object(self.fs.sync_manager, "queue_sync") as sync:
            self.assertFalse(self.fs.cache_manager.evict_lru_files(bytes_needed=10))
            self.assertEqual(self.fs.sweep_stale_cache_metadata(), 0)
            sync.assert_not_called()
            row = state.get_file_metadata("/live.log")
            self.assertEqual((row.size, row.sync_status, row.dirty_generation),
                             (9, SyncStatus.CLEAN, 0))
            self.assertEqual(cached.read_bytes(), b"version 2!")
        self.assertTrue(self.fs.cache_manager.evict_lru_files(bytes_needed=10))
        self.assertIsNone(state.get_file_metadata("/live.log").checksum)

    def test_eviction_heals_verified_replacement_before_the_sweep(self):
        self.refreshed_with_checksums()
        with patch.object(self.fs, "_record_fetched", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.fs.open("/live.log", os.O_RDONLY)
        self.restart()
        cached = self.cache_dir / "live.log"
        self.assertEqual(cached.read_bytes(), b"version 2!")
        state = self.fs.state_manager
        self.assertEqual(state.get_file_metadata("/live.log").size, 9)
        unlink = Path.unlink

        def inspect_before_unlink(path, *args, **kwargs):
            if path == cached:
                repaired = state.get_file_metadata("/live.log")
                self.assertEqual((repaired.size, repaired.checksum), (10, None))
                self.assertTrue(all(not loc.checksum for loc in state.get_file_locations("/live.log")))
            return unlink(path, *args, **kwargs)

        with patch.object(self.fs.sync_manager, "queue_sync") as sync, \
                patch.object(Path, "unlink", inspect_before_unlink):
            self.assertTrue(self.fs.cache_manager.evict_lru_files(bytes_needed=10))
        sync.assert_not_called()
        self.assertFalse(cached.exists())
        row = state.get_file_metadata("/live.log")
        self.assertEqual((row.size, row.last_modified, row.sync_status, row.checksum),
                         (10, 1_100_000_000, SyncStatus.CLEAN, None))
        self.assertEqual(row.dirty_generation, 0)
        for location in state.get_file_locations("/live.log"):
            self.assertNotEqual(location.backend_name, "cache")
            self.assertFalse(location.checksum)

    def test_startup_scan_snapshot_can_predate_a_verified_refresh(self):
        self.refreshed_with_checksums()
        cached = self.cache_dir / "live.log"
        old_stat = cached.stat()
        self.assertEqual(self.read_through("/live.log"), b"version 2!")
        self.assertFalse(self.fs.cache_manager.heal_stale_clean(
            "/live.log", cached, stat_size=old_stat.st_size, stat_mtime=old_stat.st_mtime))
        self.assertEqual(self.fs.state_manager.get_file_metadata("/live.log").sync_status,
                         SyncStatus.CLEAN)

    def test_a_crash_after_equal_stat_replacement_forgets_the_old_checksums(self):
        self.crash_after_installing_refresh(equal_stat=True)
        self.assertEqual(self.fs.sweep_stale_cache_metadata(), 0)
        row = self.fs.state_manager.get_file_metadata("/live.log")
        self.assertEqual((row.size, row.last_modified, row.sync_status, row.checksum),
                         (9, 1_000_000_000, SyncStatus.CLEAN, None))
        self.assertTrue(all(not loc.checksum for loc in
                            self.fs.state_manager.get_file_locations("/live.log")))
        self.assertEqual(self.read_through("/live.log"), b"version 2")

    def test_equal_stat_replacement_refills_without_old_checksum_after_eviction_crash(self):
        self.crash_after_installing_refresh(equal_stat=True)
        cached = self.cache_dir / "live.log"
        unlink = Path.unlink

        def crash_after_unlink(path, *args, **kwargs):
            unlink(path, *args, **kwargs)
            if path == cached:
                raise KeyboardInterrupt

        with patch.object(Path, "unlink", crash_after_unlink):
            with self.assertRaises(KeyboardInterrupt):
                self.fs.cache_manager.evict_lru_files(bytes_needed=9)
        self.assertFalse(cached.exists())
        self.restart()
        self.assertEqual(self.read_through("/live.log"), b"version 2")
        row = self.fs.state_manager.get_file_metadata("/live.log")
        self.assertEqual((row.sync_status, row.checksum, row.dirty_generation),
                         (SyncStatus.CLEAN, None, 0))

    def test_eviction_keeps_a_verified_copy_when_its_row_repair_fails(self):
        import sqlite3

        self.crash_after_installing_refresh(equal_stat=True)
        cached = self.cache_dir / "live.log"
        state = self.fs.state_manager
        inode = cached.stat().st_ino
        with patch.object(state, "record_cached_stat",
                          side_effect=sqlite3.OperationalError("row write failed")) as repair:
            self.assertFalse(self.fs.cache_manager.evict_lru_files(bytes_needed=9))
        repair.assert_called_once()
        self.assertEqual(cached.stat().st_ino, inode)
        self.assertEqual(cached.read_bytes(), b"version 2")
        self.assertEqual(state.get_file_metadata("/live.log").checksum, "hash-of-v1")
        self.assertTrue(self.fs.cache_manager.evict_lru_files(bytes_needed=9))
        self.assertIsNone(state.get_file_metadata("/live.log").checksum)

    def crash_after_installing_refresh(self, *, equal_stat=False):
        old_inode = self.refreshed_with_checksums()
        if equal_stat:
            self.rewrite_member("live.log", b"version 2", mtime=1_000_000_000)
            self.fs._recheck["/live.log"] = {"member"}
        with patch.object(self.fs, "_record_fetched", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.fs.open("/live.log", os.O_RDONLY)
        self.assertNotEqual(self.cache_inode("live.log"), old_inode)
        self.restart()
        self.assertEqual(self.fs.state_manager.get_file_metadata("/live.log").checksum, "hash-of-v1")

    def test_an_invalidation_after_a_lost_row_update_does_not_mark_dirty(self):
        # The same crash, then a change found before any sweep: the copy is
        # dropped as CLEAN, not kept as a lost write.
        self.refreshed_with_checksums()
        with patch.object(self.fs, "_record_fetched", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.fs.open("/live.log", os.O_RDONLY)
        self.restart()
        self.fs._invalidate_file_cache("/live.log")
        self.assertFalse((self.cache_dir / "live.log").exists())
        self.assertEqual(self.fs.state_manager.get_file_metadata("/live.log").sync_status,
                         SyncStatus.CLEAN)

    def test_a_warm_refresh_after_read_revocation_gives_eacces(self):
        self.changed_v1()
        real = self.member.stat

        def revoked(path):
            st = real(path)
            return SimpleNamespace(st_size=st.st_size, st_mtime=st.st_mtime,
                                   st_mtime_ns=st.st_mtime_ns, st_mode=stat.S_IFREG | 0o200)

        # The refresh does not settle (the member keeps growing), so only the
        # denial decides: never last good.
        with patch.object(self.member, "stat", side_effect=revoked), self.growing_copy():
            self.assert_errno(errno.EACCES, self.fs.open, "/live.log", os.O_RDONLY)
            # The copy is dropped, not kept for later opens.
            self.assertFalse((self.cache_dir / "live.log").exists())
            # And the next open, within the age, too: last good is not served.
            self.assert_errno(errno.EACCES, self.fs.open, "/live.log", os.O_RDONLY)

    def test_a_denial_in_any_confirmation_survives_a_mode_less_one(self):
        # Probe: a token, no mode. First confirmation: 0o200 and no mtime
        # (UNCONFIRMED). Second: a token without a mode (AGREED), so no
        # stat of the installed version reports a mode.
        self.rewrite_member("f.txt", b"secret", mtime=1_000_000_000)
        real, calls = self.member.stat, []

        def stats(path):
            calls.append(path)
            st = real(path)
            if len(calls) == 2:
                return SimpleNamespace(st_size=st.st_size, st_mode=stat.S_IFREG | 0o200)
            return SimpleNamespace(st_size=st.st_size, st_mtime_ns=st.st_mtime_ns)

        with patch.object(self.member, "stat", side_effect=stats):
            self.assert_errno(errno.EACCES, self.fs.open, "/f.txt", os.O_RDONLY)
        self.assertEqual(len(calls), 3)
        # Whatever is installed is never readable through the mount.
        self.assert_errno(errno.EACCES, self.fs.open, "/f.txt", os.O_RDONLY)

    def test_a_read_denial_does_not_import_the_earlier_write_grant(self):
        self.rewrite_member("f.txt", b"secret", mtime=1_000_000_000)
        probe = SimpleNamespace(st_size=6, st_mtime=1_000_000_000)
        denies_read = SimpleNamespace(st_size=6, st_mode=stat.S_IFREG | 0o200)
        denies_write = SimpleNamespace(st_size=6, st_mtime=1_000_000_000,
                                      st_mode=stat.S_IFREG | 0o400)
        with patch.object(self.member, "stat", side_effect=[probe, denies_read, denies_write]):
            self.assert_errno(errno.EACCES, self.fs.open, "/f.txt", os.O_WRONLY)
        self.assert_errno(errno.EACCES, self.fs.open, "/f.txt", os.O_RDONLY)

    def test_aged_check_enforces_a_read_denial_at_the_same_stat(self):
        source = self.rewrite_member("f.txt", b"secret", mtime=1_000_000_000)
        self.assertEqual(self.read_through("/f.txt"), b"secret")
        source.chmod(0o200)
        self.age_checks()
        self.assert_errno(errno.EACCES, self.fs.open, "/f.txt", os.O_RDONLY)
        self.assertFalse((self.cache_dir / "f.txt").exists())

    def test_descriptor_change_with_size_only_path_stats_cannot_publish(self):
        from storage.backends import local
        source = self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        real_stream = local.copy_bounded_stream

        def rewrite_after_copy(read, write, bound):
            real_stream(read, write, bound)
            self.rewrite_member("f.txt", b"newbytes", mtime=1_100_000_000)

        with patch.object(self.member, "stat", return_value=SimpleNamespace(st_size=8)), \
                patch.object(local, "copy_bounded_stream", side_effect=rewrite_after_copy):
            self.assert_errno(errno.EAGAIN, self.fs.open, "/f.txt", os.O_RDONLY)
        self.assertEqual(source.read_bytes(), b"newbytes")
        self.assertFalse((self.cache_dir / "f.txt").exists())
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/f.txt"))

    def test_the_copied_descriptor_contradicting_the_path_publishes_nothing(self):
        # A is renamed away and a same-size B with another mtime opened; A
        # is back before the confirmation, so the pathname stats agree.
        from storage.backends import local
        self.rewrite_member("f.txt", b"AAAAAAAA", mtime=1_000_000_000)
        src = self.member_dir / "f.txt"
        aside = self.member_dir / "f.txt.aside"
        real_open, swapped = local.os.open, []

        def swapping_open(path, flags, *a, **kw):
            if str(path) == str(src) and not swapped:
                swapped.append(path)
                os.rename(src, aside)
                src.write_bytes(b"BBBBBBBB")
                os.utime(src, (1_200_000_000, 1_200_000_000))
                try:
                    return real_open(path, flags, *a, **kw)
                finally:
                    os.replace(aside, src)
            return real_open(path, flags, *a, **kw)

        uploads = patch.object(self.member, "copy_from_local",
                               side_effect=AssertionError("upload"))
        with patch.object(local.os, "open", side_effect=swapping_open), uploads:
            self.assert_errno(errno.EAGAIN, self.fs.open, "/f.txt", os.O_RDWR)
        self.assertTrue(swapped)
        self.assertFalse((self.cache_dir / "f.txt").exists())
        self.assertEqual(self.partials(), [])
        # Unmolested, the same file installs.
        self.assertEqual(self.read_through("/f.txt"), b"AAAAAAAA")

    def test_the_local_copy_reads_no_more_than_the_cap_plus_one_at_the_descriptor(self):
        from storage.backends import base, local
        src = self.rewrite_member("big.bin", b"z" * (1 << 20))
        consumed = []
        real_stream = local.copy_bounded_stream

        def stream(read, write, bound):
            real_stream(read, write, bound)
            consumed.append(os.lseek(read.__self__.fileno(), 0, os.SEEK_CUR))

        with patch.object(local, "copy_bounded_stream", side_effect=stream), \
                base.bounded_copy(100) as bound:
            self.member.copy_to_local("/big.bin", self.temp_dir / "out")
        self.assertEqual(consumed, [101])
        self.assertTrue(bound.grew)
        del src

    def test_the_copy_being_refreshed_is_pinned_against_its_own_eviction(self):
        # Room for one copy only: the refresh's reservation could be met by
        # evicting the copy it would fall back to.
        self.changed_v1()
        self.fs.cache_manager.max_size_bytes = 15
        with self.growing_copy():
            self.assertEqual(self.read_through("/live.log"), b"version 1")
        self.assertEqual((self.cache_dir / "live.log").read_bytes(), b"version 1")
        self.assertTrue(self.stale_mark())
        self.assertEqual(self.fs._refreshing, set())

    def test_fallback_stays_pinned_until_its_descriptor_is_registered(self):
        self.changed_v1()
        cached = self.cache_dir / "live.log"
        require = self.fs._require_imported_permission
        attempts = []

        def try_evict(path, mode):
            attempts.append(self.fs.cache_manager._evict_if_still_clean("/live.log", cached))
            return require(path, mode)

        with self.growing_copy(), patch.object(
                self.fs, "_require_imported_permission", side_effect=try_evict):
            self.assertEqual(self.read_through("/live.log"), b"version 1")
        self.assertEqual(attempts, [0])
        self.assertEqual(self.fs._refreshing, set())

    def test_a_conflict_view_whose_member_has_no_file_is_enoent(self):
        self.assert_errno(errno.ENOENT, self.fs.open, "/absent.txt.member", os.O_RDONLY)

    def test_a_conflict_view_that_does_not_fit_is_enospc(self):
        self.rewrite_member("f.txt", b"member bytes")
        with patch.object(self.fs.cache_manager, "ensure_space", return_value=False), \
                patch.object(self.fs.cache_manager, "reclaim_space", None):
            self.assert_errno(errno.ENOSPC, self.fs.open, "/f.txt.member", os.O_RDONLY)

    def test_an_access_time_update_failing_after_the_install_is_not_enospc(self):
        import sqlite3
        self.rewrite_member("f.txt", b"member bytes")
        real, calls = self.fs.state_manager.update_access_time, []

        def once(path):
            calls.append(path)
            if len(calls) == 1:  # the one in cache_file_result, after the install
                raise sqlite3.OperationalError("database is locked")
            return real(path)

        with patch.object(self.fs.state_manager, "update_access_time", side_effect=once):
            self.assertEqual(self.read_through("/f.txt"), b"member bytes")
        row = self.fs.state_manager.get_file_metadata("/f.txt")
        self.assertEqual(row.sync_status, SyncStatus.CLEAN)
        # Baselined as an install: the next open within the age stats nothing.
        with self.count_stats() as stats:
            self.assertEqual(self.read_through("/f.txt"), b"member bytes")
        self.assertEqual(stats.call_count, 0)

    def test_failed_reservation_cannot_claim_a_concurrent_install(self):
        self.rewrite_member("f.txt", b"member bytes")
        cached = self.cache_dir / "f.txt"

        def concurrent_install(_size):
            cached.write_bytes(b"stream bytes")
            return False

        with patch.object(self.fs.cache_manager, "ensure_space", side_effect=concurrent_install), \
                patch.object(self.fs.cache_manager, "reclaim_space", None), \
                patch.object(self.fs, "_record_fetched") as record_fetched:
            self.assert_errno(errno.ENOSPC, self.fs.open, "/f.txt", os.O_RDWR)
        record_fetched.assert_not_called()
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/f.txt"))

    def refresh_raising(self, method, exc, writer_errno):
        self.changed_v1()
        with patch.object(self.member, method, side_effect=exc):
            self.assertEqual(self.read_through("/live.log"), b"version 1")
        self.assertTrue(self.stale_mark())
        # The failure counts as the check: within the age, the writer finds
        # the mark without asking the member.
        with self.count_fetches() as fetches:
            self.assert_errno(errno.EAGAIN, self.fs.open, "/live.log", os.O_RDWR)
        self.assertEqual(fetches.call_count, 0)
        # Past the age, a writer whose refresh raises gets its class.
        self.age_checks()
        with patch.object(self.member, method, side_effect=exc):
            self.assert_errno(writer_errno, self.fs.open, "/live.log", os.O_RDWR)
        self.assertEqual((self.cache_dir / "live.log").read_bytes(), b"version 1")

    def test_a_refresh_without_capacity_in_time_keeps_last_good(self):
        from storage.backends.ssh_pool import BulkCapacityTimeout
        self.refresh_raising("copy_to_local", BulkCapacityTimeout("no capacity"), errno.EAGAIN)

    def test_a_refresh_whose_member_is_unreachable_keeps_last_good(self):
        from storage.backends.ssh_pool import MemberUnavailable
        self.refresh_raising("exists_or_raise", MemberUnavailable("down"), errno.EHOSTUNREACH)

    def test_a_no_baseline_check_reads_st_mtime_ns_alone(self):
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        self.assertEqual(self.read_through("/f.txt"), b"original")
        with self.fs._member_checks_lock:
            for path, check in list(self.fs._member_checks.items()):
                self.fs._member_checks[path] = dataclasses.replace(
                    check, members={}, verified_at=check.verified_at - 3600)
        real = self.member.stat
        ns_only = lambda path: SimpleNamespace(  # noqa: E731
            st_size=real(path).st_size, st_mtime=None, st_mtime_ns=real(path).st_mtime_ns)
        with patch.object(self.member, "stat", side_effect=ns_only), \
                self.count_fetches() as fetches:
            self.assertEqual(self.read_through("/f.txt"), b"original")
        self.assertEqual(fetches.call_count, 0)

    def test_a_fill_makes_at_most_two_confirming_stats(self):
        self.rewrite_member("f.txt", b"original", mtime=1_000_000_000)
        real, calls = self.member.stat, []

        def stats(path):
            calls.append(path)
            if len(calls) == 1:
                return real(path)  # the probe
            raise OSError(errno.EIO, "stat failed")

        with patch.object(self.member, "stat", side_effect=stats):
            self.assert_errno(errno.EAGAIN, self.fs.open, "/f.txt", os.O_RDWR)
        self.assertEqual(len(calls), 3)  # the probe and two confirmations


if __name__ == "__main__":
    unittest.main()
