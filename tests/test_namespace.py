"""
Tests for NamespaceService.

Covers hidden-entry filtering, conflict-view logic, readdir snapshot TTL,
and directory operation propagation to backends.
"""

import logging
import os
import stat
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from storage.backends.local import LocalBackend
from storage.cache import CacheManager
from storage.checksums import ChecksumRecord
from storage.fs import StorageFS
from storage.namespace import NamespaceService
from storage.pathing import resolve_path
from storage.state import StateManager


def _make_namespace_service(
    cache_root,
    backends,
    state_db_path=None,
    readdir_snapshot_ttl=1.0,
    checksum_records=None,
    listing_max_stale=3600.0,
):
    """Build a NamespaceService with sensible test defaults.

    Uses real CacheManager and StateManager so cache-path resolution
    and directory scanning work against the actual filesystem.
    """
    if state_db_path is None:
        state_db_path = str(cache_root / ".state.db")

    state_manager = StateManager(state_db_path)
    cache_manager = CacheManager(
        cache_root=cache_root,
        state_manager=state_manager,
        max_size_bytes=100 * 1024 * 1024,
    )
    sync_manager = MagicMock()
    sync_manager.queue_dir_sync = MagicMock()

    backend_names = list(backends.keys())

    def _resolve(path):
        return resolve_path(
            path,
            backend_names=backend_names,
            cache_path_for=cache_manager.get_cache_path,
        )

    if checksum_records is None:
        checksum_records = {}

    dir_cache = {}
    readdir_snapshot = {}

    ns = NamespaceService(
        backends=backends,
        cache_manager=cache_manager,
        state_manager=state_manager,
        sync_manager=sync_manager,
        logger=logging.getLogger("test"),
        resolve_path=_resolve,
        schedule_checksum_task=lambda backend, vpath, basis=None, **kw: None,
        partial_suffix=".storage.partial",
        is_hidden_entry=StorageFS._is_hidden_entry,
        dir_cache=dir_cache,
        dir_cache_lock=threading.Lock(),
        readdir_snapshot=readdir_snapshot,
        readdir_snapshot_ttl=readdir_snapshot_ttl,
        checksum_records=checksum_records,
        checksum_lock=threading.RLock(),
        listing_max_stale=listing_max_stale,
    )
    # Attach for cleanup in tearDown
    ns._test_state_manager = state_manager
    return ns


class TestHiddenEntryFiltering(unittest.TestCase):
    """Test the is_hidden_entry callback behavior in readdir."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self.cache_root = self.root / "cache"
        self.cache_root.mkdir()
        self.backend_root = self.root / "backend0"
        self.backend_root.mkdir()
        self.backend = LocalBackend("backend0", str(self.backend_root))
        self.ns = _make_namespace_service(
            cache_root=self.cache_root,
            backends={"backend0": self.backend},
        )

    def tearDown(self):
        self.ns._test_state_manager.close()
        self._tmpdir.cleanup()

    def _populate_cache_dir(self, subdir, filenames):
        """Create subdir under cache_root and touch files in it."""
        cache_dir = self.cache_root / subdir.lstrip("/")
        cache_dir.mkdir(parents=True, exist_ok=True)
        for name in filenames:
            path = cache_dir / name
            if name.endswith("/"):
                path.mkdir(exist_ok=True)
            else:
                path.touch()

    def test_partial_files_hidden_from_cache_listing(self):
        self._populate_cache_dir("docs", [
            "readme.txt",
            "download.bin.storage.partial",
        ])
        entries = self.ns.readdir("/docs")
        self.assertIn("readme.txt", entries)
        self.assertNotIn("download.bin.storage.partial", entries)

    def test_rsync_tmp_hidden_from_cache_listing(self):
        self._populate_cache_dir("data", ["file.txt"])
        (self.cache_root / "data" / ".storage.rsync-tmp").mkdir()
        entries = self.ns.readdir("/data")
        self.assertIn("file.txt", entries)
        self.assertNotIn(".storage.rsync-tmp", entries)

    def test_control_sock_hidden(self):
        self._populate_cache_dir("root", [
            "normal.txt",
            ".storage.control.sock",
        ])
        entries = self.ns.readdir("/root")
        self.assertIn("normal.txt", entries)
        self.assertNotIn(".storage.control.sock", entries)

    def test_mount_lock_hidden(self):
        self._populate_cache_dir("root", [
            "normal.txt",
            ".storage.mount.abc123.lock",
        ])
        entries = self.ns.readdir("/root")
        self.assertIn("normal.txt", entries)
        self.assertNotIn(".storage.mount.abc123.lock", entries)

    def test_normal_dotfiles_not_hidden(self):
        self._populate_cache_dir("project", [
            ".gitignore",
            ".env",
            ".bashrc",
            "src.py",
        ])
        entries = self.ns.readdir("/project")
        self.assertIn(".gitignore", entries)
        self.assertIn(".env", entries)
        self.assertIn(".bashrc", entries)
        self.assertIn("src.py", entries)

    def test_hidden_entries_filtered_from_backend_listing(self):
        # Place hidden entries only on the backend, not in the cache dir
        backend_dir = self.backend_root / "stuff"
        backend_dir.mkdir(parents=True)
        (backend_dir / "real_file.txt").touch()
        (backend_dir / "temp.storage.partial").touch()
        (backend_dir / ".storage.rsync-tmp").mkdir()

        # Ensure cache dir exists so readdir can scan it
        (self.cache_root / "stuff").mkdir(parents=True, exist_ok=True)

        entries = self.ns.readdir("/stuff")
        self.assertIn("real_file.txt", entries)
        self.assertNotIn("temp.storage.partial", entries)
        self.assertNotIn(".storage.rsync-tmp", entries)


class TestConflictView(unittest.TestCase):
    """Test _apply_conflict_view for multi-backend conflict resolution."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self.cache_root = self.root / "cache"
        self.cache_root.mkdir()

        self.b0_root = self.root / "b0"
        self.b0_root.mkdir()
        self.b1_root = self.root / "b1"
        self.b1_root.mkdir()

        self.backend0 = LocalBackend("backend0", str(self.b0_root))
        self.backend1 = LocalBackend("backend1", str(self.b1_root))

    def tearDown(self):
        self._tmpdir.cleanup()

    def _make_ns(self, checksum_records=None):
        ns = _make_namespace_service(
            cache_root=self.cache_root,
            backends={"backend0": self.backend0, "backend1": self.backend1},
            checksum_records=checksum_records,
        )
        self._ns_state_manager = ns._test_state_manager
        return ns

    def _cleanup_ns(self):
        if hasattr(self, "_ns_state_manager"):
            self._ns_state_manager.close()

    def test_no_conflict_when_checksums_match(self):
        now = time.time()
        checksum_records = {
            ("/shared/report.txt", "backend0"): ChecksumRecord(
                checksum="aabbccdd", timestamp=now,
            ),
            ("/shared/report.txt", "backend1"): ChecksumRecord(
                checksum="aabbccdd", timestamp=now,
            ),
        }
        ns = self._make_ns(checksum_records=checksum_records)
        ns.dir_cache["/shared"] = {
            "backend0": {"report.txt"},
            "backend1": {"report.txt"},
        }

        result = ns._apply_conflict_view("/shared", {".", "..", "report.txt"})
        self.assertIn("report.txt", result)
        self.assertNotIn("report.txt.backend0", result)
        self.assertNotIn("report.txt.backend1", result)
        self._cleanup_ns()

    def test_conflict_creates_suffixed_entries(self):
        now = time.time()
        checksum_records = {
            ("/shared/data.csv", "backend0"): ChecksumRecord(
                checksum="aaaa1111", timestamp=now,
            ),
            ("/shared/data.csv", "backend1"): ChecksumRecord(
                checksum="bbbb2222", timestamp=now,
            ),
        }
        ns = self._make_ns(checksum_records=checksum_records)
        ns.dir_cache["/shared"] = {
            "backend0": {"data.csv"},
            "backend1": {"data.csv"},
        }

        result = ns._apply_conflict_view("/shared", {".", "..", "data.csv"})
        self.assertIn("data.csv.backend0", result)
        self.assertIn("data.csv.backend1", result)
        # The unsuffixed name must not appear in a real conflict
        self.assertNotIn("data.csv", result)
        self._cleanup_ns()

    def test_single_backend_no_suffix(self):
        ns = self._make_ns()
        ns.dir_cache["/solo"] = {
            "backend0": {"unique.txt"},
        }

        result = ns._apply_conflict_view("/solo", {".", "..", "unique.txt"})
        self.assertIn("unique.txt", result)
        self.assertNotIn("unique.txt.backend0", result)
        self._cleanup_ns()

    # Size-first: the listing's attributes decide before any hash does.

    @staticmethod
    def _file(size, mode=stat.S_IFREG | 0o644):
        return {"st_mode": mode, "st_size": size, "st_mtime": 1.0}

    def _view(self, attrs, records=None):
        ns = self._make_ns(checksum_records=records)
        entries = {name: set(names) for name, names in
                   ((member, members.keys()) for member, members in attrs.items())}
        splits = ns._size_splits(entries, attrs)
        scheduled = []
        ns.schedule_checksum_task = lambda backend, path, basis=None, **kw: scheduled.append(
            (backend.name, path, basis, kw.get("check_dir", False)))
        ns._schedule_directory_checksums("/d", entries, attrs, splits)
        view = ns._apply_conflict_view(
            "/d", {".", ".."} | set().union(*entries.values()), entries, splits)
        self._cleanup_ns()
        return view, splits, sorted(scheduled)

    def test_different_sizes_split_without_hashing(self):
        view, splits, scheduled = self._view({
            "backend0": {"a.bin": self._file(10)},
            "backend1": {"a.bin": self._file(11)},
        })
        self.assertEqual(splits, {"a.bin": {"backend0": 10, "backend1": 11}})
        self.assertIn("a.bin.backend0", view)
        self.assertIn("a.bin.backend1", view)
        self.assertNotIn("a.bin", view)
        self.assertEqual(scheduled, [])

    def test_equal_sizes_show_plain_name_and_hash(self):
        view, splits, scheduled = self._view({
            "backend0": {"a.bin": self._file(10)},
            "backend1": {"a.bin": self._file(10)},
        })
        self.assertEqual(splits, {})
        self.assertIn("a.bin", view)
        self.assertNotIn("a.bin.backend0", view)
        self.assertEqual([s[:2] for s in scheduled],
                         [("backend0", "/d/a.bin"), ("backend1", "/d/a.bin")])

    def test_equal_mtime_is_not_identity(self):
        # Same size and mtime, different hashes: still a conflict.
        now = time.time()
        records = {("/d/a.bin", "backend0"): ChecksumRecord(checksum="aa", timestamp=now),
                   ("/d/a.bin", "backend1"): ChecksumRecord(checksum="bb", timestamp=now)}
        view, _, _ = self._view({
            "backend0": {"a.bin": self._file(10)},
            "backend1": {"a.bin": self._file(10)},
        }, records)
        self.assertIn("a.bin.backend0", view)
        self.assertIn("a.bin.backend1", view)

    def test_unverified_copy_shows_plain_name(self):
        now = time.time()
        records = {("/d/a.bin", "backend0"): ChecksumRecord(checksum="aa", timestamp=now),
                   ("/d/a.bin", "backend1"): ChecksumRecord(checksum=None, timestamp=now)}
        view, _, _ = self._view({
            "backend0": {"a.bin": self._file(10)},
            "backend1": {"a.bin": self._file(10)},
        }, records)
        self.assertIn("a.bin", view)
        self.assertNotIn("a.bin.backend0", view)

    def test_directory_or_symlink_copy_is_never_a_size_split(self):
        for other in (self._file(4096, stat.S_IFDIR | 0o755),
                      self._file(3, stat.S_IFLNK | 0o777),
                      {}):
            with self.subTest(other=other):
                view, splits, _ = self._view({
                    "backend0": {"a": self._file(10)},
                    "backend1": {"a": other},
                })
                self.assertEqual(splits, {})
                self.assertIn("a", view)

    def test_unlisted_mode_and_symlinks_defer_the_directory_test(self):
        # No member call while scheduling: the hash worker asks isdir.
        self.backend0.isdir = self.backend1.isdir = lambda path: self.fail("isdir on the caller")
        _, _, scheduled = self._view({
            "backend0": {"a": self._file(3, stat.S_IFLNK | 0o777)},
            "backend1": {"a": {}},
        })
        self.assertEqual(scheduled, [("backend0", "/d/a", None, True),
                                     ("backend1", "/d/a", None, True)])
        self._cleanup_ns()


class TestReaddirSnapshotTTL(unittest.TestCase):
    """Test that readdir caches and expires snapshots based on TTL."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self.cache_root = self.root / "cache"
        self.cache_root.mkdir()
        self.backend_root = self.root / "backend0"
        self.backend_root.mkdir()
        self.backend = LocalBackend("backend0", str(self.backend_root))

    def tearDown(self):
        if hasattr(self, "_ns"):
            self._ns._test_state_manager.close()
        self._tmpdir.cleanup()

    def test_readdir_returns_cached_snapshot_within_ttl(self):
        self._ns = _make_namespace_service(
            cache_root=self.cache_root,
            backends={"backend0": self.backend},
            readdir_snapshot_ttl=10.0,  # long TTL so it never expires during test
        )
        cache_dir = self.cache_root / "ttl_test"
        cache_dir.mkdir()
        (cache_dir / "first.txt").touch()

        first = self._ns.readdir("/ttl_test")
        self.assertIn("first.txt", first)

        # Add a file after the snapshot is taken
        (cache_dir / "second.txt").touch()

        second = self._ns.readdir("/ttl_test")
        # Should return the cached snapshot, so second.txt is absent
        self.assertEqual(first, second)
        self.assertNotIn("second.txt", second)

    def test_readdir_refreshes_after_ttl_expires(self):
        self._ns = _make_namespace_service(
            cache_root=self.cache_root,
            backends={"backend0": self.backend},
            readdir_snapshot_ttl=0.15,
            listing_max_stale=0.15,  # expired, not stale: relisted at once
        )
        cache_dir = self.cache_root / "ttl_expire"
        cache_dir.mkdir()
        (cache_dir / "alpha.txt").touch()

        first = self._ns.readdir("/ttl_expire")
        self.assertIn("alpha.txt", first)

        # Add a file and wait past the TTL
        (cache_dir / "beta.txt").touch()
        time.sleep(0.25)

        refreshed = self._ns.readdir("/ttl_expire")
        self.assertIn("alpha.txt", refreshed)
        self.assertIn("beta.txt", refreshed)


    def test_readdir_serves_stale_snapshot_then_the_refresh(self):
        self._ns = _make_namespace_service(
            cache_root=self.cache_root,
            backends={"backend0": self.backend},
            readdir_snapshot_ttl=0.15,
        )
        self.addCleanup(self._ns.shutdown)
        cache_dir = self.cache_root / "ttl_stale"
        cache_dir.mkdir()
        (cache_dir / "alpha.txt").touch()
        self.assertIn("alpha.txt", self._ns.readdir("/ttl_stale"))
        (cache_dir / "beta.txt").touch()
        time.sleep(0.25)

        self.assertNotIn("beta.txt", self._ns.readdir("/ttl_stale"))
        deadline = time.monotonic() + 5
        while "/ttl_stale" in self._ns._background_refreshes and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertIn("beta.txt", self._ns.readdir("/ttl_stale"))

    def test_listing_state_boundaries(self):
        ns = self._ns = _make_namespace_service(
            cache_root=self.cache_root, backends={"backend0": self.backend},
            readdir_snapshot_ttl=60.0, listing_max_stale=3600.0,
        )
        self.assertEqual("fresh", ns.listing_state(1000.0, 1060.0))
        self.assertEqual("stale", ns.listing_state(1000.0, 1060.5))
        self.assertEqual("stale", ns.listing_state(1000.0, 4600.0))
        self.assertEqual("expired", ns.listing_state(1000.0, 4600.5))
        # max_stale below the fresh window: nothing is stale.
        ns.listing_max_stale = 10.0
        self.assertEqual("fresh", ns.listing_state(1000.0, 1060.0))
        self.assertEqual("expired", ns.listing_state(1000.0, 1060.5))
        ns.readdir_snapshot_ttl = float("inf")
        self.assertEqual("fresh", ns.listing_state(0.0, 10.0 ** 9))


class TestDirOperationPropagation(unittest.TestCase):
    """Test that mkdir propagates to backends correctly."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self.cache_root = self.root / "cache"
        self.cache_root.mkdir()

        self.b0_root = self.root / "b0"
        self.b0_root.mkdir()
        self.b1_root = self.root / "b1"
        self.b1_root.mkdir()

        self.backend0 = LocalBackend("backend0", str(self.b0_root))
        self.backend1 = LocalBackend("backend1", str(self.b1_root))

    def tearDown(self):
        if hasattr(self, "_ns"):
            self._ns._test_state_manager.close()
        self._tmpdir.cleanup()

    def test_mkdir_propagates_to_all_backends(self):
        self._ns = _make_namespace_service(
            cache_root=self.cache_root,
            backends={"backend0": self.backend0, "backend1": self.backend1},
        )
        self._ns.mkdir("/newdir", 0o755)

        # Verify directory created on both backends and in cache
        self.assertTrue((self.b0_root / "newdir").is_dir())
        self.assertTrue((self.b1_root / "newdir").is_dir())
        self.assertTrue((self.cache_root / "newdir").is_dir())

    def test_mkdir_failure_on_one_backend_still_succeeds(self):
        # Create a read-only directory so backend mkdir fails
        ro_root = self.root / "readonly_backend"
        ro_root.mkdir()
        backend_fail = LocalBackend("backend_fail", str(ro_root))

        self._ns = _make_namespace_service(
            cache_root=self.cache_root,
            backends={"backend0": self.backend0, "backend_fail": backend_fail},
        )

        # Remove write permission on the failing backend root
        os.chmod(ro_root, 0o555)
        try:
            self._ns.mkdir("/survived", 0o755)

            # Cache and healthy backend should have the directory
            self.assertTrue((self.cache_root / "survived").is_dir())
            self.assertTrue((self.b0_root / "survived").is_dir())
            # Failing backend should not
            self.assertFalse((ro_root / "survived").exists())

            # sync_manager.queue_dir_sync should be called for the failed backend
            self._ns.sync_manager.queue_dir_sync.assert_called()
        finally:
            os.chmod(ro_root, 0o755)


if __name__ == "__main__":
    unittest.main()
