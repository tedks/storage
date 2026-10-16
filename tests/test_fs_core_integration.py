import errno
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from fuse import FuseOSError

from storage.backends.local import LocalBackend
from storage.cache_metadata import read_cache_metadata
from storage.fs import StorageFS
from storage.models import SyncStatus


class TestFSCoreIntegration(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_dir = self.temp_dir / "cache"
        self.cache_dir.mkdir()
        self.backend1_dir = self.temp_dir / "backend1"
        self.backend2_dir = self.temp_dir / "backend2"
        self.backend1_dir.mkdir()
        self.backend2_dir.mkdir()
        self.db_path = self.temp_dir / "state.db"

        self.backends = {
            "backend1": LocalBackend("backend1", str(self.backend1_dir)),
            "backend2": LocalBackend("backend2", str(self.backend2_dir)),
        }

        self.fs = StorageFS(
            cache_root=self.cache_dir,
            state_db=self.db_path,
            backends=self.backends,
            cache_size=10 * 1024 * 1024,
            num_sync_workers=1,
        )

    def tearDown(self):
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_open_reads_remote_only_file_and_populates_state(self):
        content = "remote-data"
        (self.backend1_dir / "remote.txt").write_text(content)

        vfd = self.fs.open('/remote.txt', os.O_RDONLY)
        data = self.fs.read('/remote.txt', 1024, 0, vfd)
        self.fs.release('/remote.txt', vfd)

        self.assertEqual(data.decode(), content)
        meta = self.fs.state_manager.get_file_metadata('/remote.txt')
        self.assertIsNotNone(meta)
        locations = self.fs.state_manager.get_file_locations('/remote.txt')
        backend_names = {loc.backend_name for loc in locations}
        self.assertIn('backend1', backend_names)
        self.assertIn('cache', backend_names)

    def test_open_uses_cache_when_backends_unavailable(self):
        content = "cached-data"
        (self.backend1_dir / "cached.txt").write_text(content)

        first_fd = self.fs.open('/cached.txt', os.O_RDONLY)
        _ = self.fs.read('/cached.txt', 1024, 0, first_fd)
        self.fs.release('/cached.txt', first_fd)

        self.backends["backend1"].set_available(False)
        self.backends["backend2"].set_available(False)

        second_fd = self.fs.open('/cached.txt', os.O_RDONLY)
        reread = self.fs.read('/cached.txt', 1024, 0, second_fd)
        self.fs.release('/cached.txt', second_fd)

        self.assertEqual(reread.decode(), content)

    def test_open_uncached_returns_hostunreach_when_all_backends_unavailable(self):
        (self.backend1_dir / "offline.txt").write_text("offline")

        self.backends["backend1"].set_available(False)
        self.backends["backend2"].set_available(False)

        with self.assertRaises(FuseOSError) as ctx:
            self.fs.open('/offline.txt', os.O_RDONLY)
        self.assertEqual(ctx.exception.errno, errno.EHOSTUNREACH)

    def test_cache_state_xattr_reports_cache_presence_and_backend_availability(self):
        content = "xattr-data"
        (self.backend1_dir / "xattr.txt").write_text(content)

        raw_before = self.fs.getxattr('/xattr.txt', StorageFS.CACHE_STATE_XATTR)
        state_before = json.loads(raw_before.decode("utf-8"))
        self.assertFalse(state_before["cache_present"])
        self.assertIn("backend1", state_before["backend_available"])

        fd = self.fs.open('/xattr.txt', os.O_RDONLY)
        _ = self.fs.read('/xattr.txt', 1024, 0, fd)
        self.fs.release('/xattr.txt', fd)

        raw_cached = self.fs.getxattr('/xattr.txt', StorageFS.CACHE_STATE_XATTR)
        state_cached = json.loads(raw_cached.decode("utf-8"))
        self.assertTrue(state_cached["cache_present"])
        self.assertEqual(state_cached["cache_size"], len(content))

        self.backends["backend1"].set_available(False)
        self.backends["backend2"].set_available(False)

        raw_offline = self.fs.getxattr('/xattr.txt', StorageFS.CACHE_STATE_XATTR)
        state_offline = json.loads(raw_offline.decode("utf-8"))
        self.assertEqual(state_offline["backend_available"], [])
        self.assertIn("backend1", state_offline["backend_unavailable"])
        self.assertIn("backend2", state_offline["backend_unavailable"])

    def test_sync_start_initializes_fd_tracking_before_worker_callbacks(self):
        temp_dir = Path(tempfile.mkdtemp())
        cache_dir = temp_dir / "cache"
        cache_dir.mkdir()
        backend_dir = temp_dir / "backend"
        backend_dir.mkdir()
        state_db = temp_dir / "state.db"
        callback_results = []

        def start_side_effect(sync_manager):
            callback_results.append(sync_manager._should_sync_path_cb('/dirty.txt'))

        try:
            with unittest.mock.patch(
                'storage.fs.SyncManager.start',
                autospec=True,
                side_effect=start_side_effect,
            ):
                fs = StorageFS(
                    cache_root=cache_dir,
                    state_db=state_db,
                    backends={"backend": LocalBackend("backend", str(backend_dir))},
                    cache_size=10 * 1024 * 1024,
                    num_sync_workers=1,
                )
            fs.state_manager.close()
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        self.assertEqual([True], callback_results)

    def test_create_write_and_truncate_each_bump_the_dirty_generation(self):
        fd = self.fs.create("/gen.txt", 0o644)
        created = self.fs.state_manager.get_file_metadata("/gen.txt")
        self.assertEqual(created.sync_status, SyncStatus.DIRTY)
        self.assertEqual(created.size, 0)

        self.fs.write("/gen.txt", b"hello", 0, fd)
        written = self.fs.state_manager.get_file_metadata("/gen.txt")
        self.assertGreater(written.dirty_generation, created.dirty_generation)
        self.assertEqual(written.sync_status, SyncStatus.DIRTY)
        self.assertEqual(written.size, 5)
        self.fs.release("/gen.txt", fd)

        self.fs.truncate("/gen.txt", 2)
        truncated = self.fs.state_manager.get_file_metadata("/gen.txt")
        self.assertGreater(truncated.dirty_generation, written.dirty_generation)
        self.assertEqual(truncated.sync_status, SyncStatus.DIRTY)
        self.assertEqual(truncated.size, 2)

        record = read_cache_metadata(self.cache_dir / "gen.txt")
        if record is not None:
            self.assertEqual(record.sync_status, SyncStatus.DIRTY)
            self.assertEqual(record.size, 2)

    def test_open_heals_a_clean_row_older_than_the_file(self):
        self.fs.sync_manager.stop()  # the row states below are set by hand
        fd = self.fs.create("/heal.txt", 0o644)
        self.fs.write("/heal.txt", b"v1", 0, fd)
        self.fs.release("/heal.txt", fd)
        row = self.fs.state_manager.get_file_metadata("/heal.txt")
        self.fs.state_manager.publish_sync_status(
            "/heal.txt", row.dirty_generation, SyncStatus.CLEAN, checksum="v1",
        )
        # Bytes land behind the mount's back (or a write's row update was lost).
        (self.cache_dir / "heal.txt").write_bytes(b"v2 is longer")

        fd = self.fs.open("/heal.txt", os.O_RDONLY)
        self.fs.release("/heal.txt", fd)

        healed = self.fs.state_manager.get_file_metadata("/heal.txt")
        self.assertEqual(healed.sync_status, SyncStatus.DIRTY)
        self.assertEqual(healed.size, len(b"v2 is longer"))
        self.assertGreater(healed.dirty_generation, row.dirty_generation)

    def test_utimens_records_a_modification(self):
        fd = self.fs.create("/touch.txt", 0o644)
        self.fs.write("/touch.txt", b"v1", 0, fd)
        self.fs.release("/touch.txt", fd)
        before = self.fs.state_manager.get_file_metadata("/touch.txt")

        self.fs.utimens("/touch.txt", (5_000_000, 5_000_000))

        after = self.fs.state_manager.get_file_metadata("/touch.txt")
        self.assertGreater(after.dirty_generation, before.dirty_generation)
        self.assertEqual(after.last_modified, 5_000_000)
        record = read_cache_metadata(self.cache_dir / "touch.txt")
        self.assertEqual(record.last_modified, 5_000_000)

    def test_sweep_marks_files_written_after_their_clean_row(self):
        self.fs.sync_manager.stop()  # the row states below are set by hand
        for name in ("a.txt", "b.txt"):
            fd = self.fs.create(f"/{name}", 0o644)
            self.fs.write(f"/{name}", b"v1", 0, fd)
            self.fs.release(f"/{name}", fd)
            row = self.fs.state_manager.get_file_metadata(f"/{name}")
            self.fs.state_manager.publish_sync_status(
                f"/{name}", row.dirty_generation, SyncStatus.CLEAN, checksum="v1",
            )
        (self.cache_dir / "a.txt").write_bytes(b"v2 is longer")

        self.assertEqual(self.fs.sweep_stale_cache_metadata(), 1)

        self.assertEqual(self.fs.state_manager.get_file_metadata("/a.txt").sync_status, SyncStatus.DIRTY)
        self.assertEqual(self.fs.state_manager.get_file_metadata("/b.txt").sync_status, SyncStatus.CLEAN)

    def test_rename_rewrites_cache_metadata_virtual_path(self):
        fd = self.fs.create("/old.txt", 0o644)
        self.fs.write("/old.txt", b"hello", 0, fd)
        self.fs.release("/old.txt", fd)

        self.fs.rename("/old.txt", "/new.txt")

        cache_path = self.cache_dir / "new.txt"
        record = read_cache_metadata(cache_path)
        self.assertIsNotNone(record)
        self.assertEqual("/new.txt", record.virtual_path)
        self.assertEqual(5, record.size)


if __name__ == '__main__':
    unittest.main()
