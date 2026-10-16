import shutil
import tempfile
import time
import unittest
from unittest import mock
from pathlib import Path

from storage.models import FileLocation, FileMetadata, SyncStatus
from storage.state import StateManager


class TestStateAccessUpdates(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "state.db"
        self.state_manager = StateManager(str(self.db_path))

        now = time.time() - 100.0
        for path in ("/a.txt", "/b.txt"):
            self.state_manager.set_file_metadata(
                FileMetadata(
                    virtual_path=path,
                    size=1,
                    checksum=None,
                    last_access=now,
                    last_modified=now,
                    sync_status=SyncStatus.CLEAN,
                    replica_count=1,
                    recomputable=False,
                    recompute_cmd=None,
                    encrypted=False,
                    encryption_key_id=None,
                )
            )

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_access_updates_are_debounced(self):
        self.state_manager.ACCESS_FLUSH_INTERVAL_SECONDS = 9999.0
        self.state_manager.ACCESS_FLUSH_BATCH_SIZE = 9999
        self.state_manager.ACCESS_DEBOUNCE_SECONDS = 1.0

        self.state_manager.update_access_time("/a.txt")
        pending_first = dict(self.state_manager._pending_access_updates)
        self.assertIn("/a.txt", pending_first)

        # This should be ignored by debounce window.
        self.state_manager.update_access_time("/a.txt")
        pending_second = dict(self.state_manager._pending_access_updates)
        self.assertEqual(pending_first, pending_second)

    def test_access_updates_flush_in_batch(self):
        self.state_manager.ACCESS_FLUSH_INTERVAL_SECONDS = 9999.0
        self.state_manager.ACCESS_FLUSH_BATCH_SIZE = 2
        self.state_manager.ACCESS_DEBOUNCE_SECONDS = 0.0

        old_a = self.state_manager.get_file_metadata("/a.txt").last_access
        old_b = self.state_manager.get_file_metadata("/b.txt").last_access

        self.state_manager.update_access_time("/a.txt")
        self.assertIn("/a.txt", self.state_manager._pending_access_updates)

        # Reaching batch size should flush both updates.
        self.state_manager.update_access_time("/b.txt")
        self.assertEqual({}, self.state_manager._pending_access_updates)

        new_a = self.state_manager.get_file_metadata("/a.txt").last_access
        new_b = self.state_manager.get_file_metadata("/b.txt").last_access
        self.assertGreater(new_a, old_a)
        self.assertGreater(new_b, old_b)

    def test_close_flushes_pending_access_updates(self):
        self.state_manager.ACCESS_FLUSH_INTERVAL_SECONDS = 9999.0
        self.state_manager.ACCESS_FLUSH_BATCH_SIZE = 9999
        self.state_manager.ACCESS_DEBOUNCE_SECONDS = 0.0

        old_a = self.state_manager.get_file_metadata("/a.txt").last_access
        self.state_manager.update_access_time("/a.txt")
        self.assertIn("/a.txt", self.state_manager._pending_access_updates)

        self.state_manager.close()

        reopened = StateManager(str(self.db_path))
        try:
            new_a = reopened.get_file_metadata("/a.txt").last_access
            self.assertGreater(new_a, old_a)
        finally:
            reopened.close()
            self.state_manager = StateManager(str(self.db_path))

    def test_replace_index_rolls_back_when_insert_fails(self):
        old_metadata = self.state_manager.get_file_metadata("/a.txt")
        self.assertIsNotNone(old_metadata)
        self.state_manager.add_file_location(
            FileLocation(
                virtual_path="/a.txt",
                backend_name="cache",
                checksum="old",
                last_synced=old_metadata.last_modified,
                is_cached=True,
            )
        )
        new_metadata = FileMetadata(
            virtual_path="/new.txt",
            size=1,
            checksum="new",
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.CLEAN,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )

        with mock.patch.object(
            self.state_manager,
            "_insert_location_rows_locked",
            side_effect=RuntimeError("location insert failed"),
        ):
            with self.assertRaises(RuntimeError):
                self.state_manager.replace_index(
                    [new_metadata],
                    [
                        FileLocation(
                            virtual_path="/new.txt",
                            backend_name="cache",
                            checksum="new",
                            last_synced=new_metadata.last_modified,
                            is_cached=True,
                        )
                    ],
                )

        self.assertIsNotNone(self.state_manager.get_file_metadata("/a.txt"))
        self.assertIsNone(self.state_manager.get_file_metadata("/new.txt"))
        locations = self.state_manager.get_file_locations("/a.txt")
        self.assertEqual(["cache"], [location.backend_name for location in locations])

    def test_iter_file_locations_pages_across_rows_sharing_a_path(self):
        rows = [
            ("/a.txt", "cache", True),
            ("/a.txt", "local0", False),
            ("/a.txt", "remote1", False),
            ("/b.txt", "cache", True),
            ("/b.txt", "remote1", False),
        ]
        for virtual_path, backend_name, is_cached in rows:
            self.state_manager.add_file_location(
                FileLocation(
                    virtual_path=virtual_path,
                    backend_name=backend_name,
                    checksum="c",
                    last_synced=1.0,
                    is_cached=is_cached,
                )
            )

        def keys(**kwargs):
            return [
                (location.virtual_path, location.backend_name)
                for location in self.state_manager.iter_file_locations(batch_size=1, **kwargs)
            ]

        # batch_size=1 puts a page boundary between rows that share a virtual_path,
        # so the cursor must advance on backend_name as well.
        self.assertEqual([(path, backend) for path, backend, _ in rows], keys())
        self.assertEqual([("/a.txt", "cache"), ("/b.txt", "cache")], keys(is_cached=True))
        self.assertEqual(
            [("/a.txt", "local0"), ("/a.txt", "remote1"), ("/b.txt", "remote1")],
            keys(is_cached=False),
        )

    def test_iter_file_locations_pages_do_bounded_work_at_representative_cursors(self):
        # Each page must seek to its cursor and do bounded work past it. Choosing
        # idx_cached, sorting in a temp B-tree, or scanning from the start all cost
        # O(rows) per page and make a full iteration quadratic. Count SQLite VM
        # steps per page rather than matching EXPLAIN QUERY PLAN text, which is not
        # a stable interface (and a SCAN in primary-key order passes a plan check).
        # Blind spots: work against another table (only file_locations is filled
        # here) and work done inside a single VM step are not counted.
        paths = 5000
        rows = [
            (f"/f{index:06d}", backend_name, "c", 1.0, int(backend_name == "cache"))
            for index in range(paths)
            for backend_name in ("cache", "remote1")
        ]
        # No single cursor position catches every regression: scanning from the
        # start is cheap for an early page, and seeking then sorting the remaining
        # suffix is cheap for a late one. The known regression classes are each
        # costly at one or more of these representative positions.
        cursors = {
            "early": ("", ""),
            "middle": (f"/f{paths // 2:06d}", "remote1"),
            "late": (f"/f{paths - 5:06d}", "remote1"),
        }
        cases = [
            (f"{name} {position}", query, prefix + cursor + (10,))
            for position, cursor in cursors.items()
            for name, query, prefix in (
                ("all", StateManager.ITER_LOCATIONS_QUERY, ()),
                ("cached", StateManager.ITER_LOCATIONS_BY_CACHE_QUERY, (1,)),
            )
        ]

        conn = self.state_manager.conn
        with self.state_manager._db_lock:
            conn.executemany("INSERT INTO file_locations VALUES (?, ?, ?, ?, ?)", rows)
            conn.commit()
            for label, query, params in cases:
                with self.subTest(label):
                    steps = [0]

                    def count_step():
                        steps[0] += 1
                        return 0

                    conn.set_progress_handler(count_step, 1)
                    try:
                        fetched = conn.execute(query, params).fetchall()
                    finally:
                        conn.set_progress_handler(None, 1)

                    self.assertTrue(fetched)
                    # The real queries measured about 260 steps at most on SQLite
                    # 3.45-3.50; each regression above measured at least 45,000 at one
                    # or more of these cursors on this table.
                    self.assertLess(steps[0], 2000)


if __name__ == "__main__":
    unittest.main()


class TestDirtyGeneration(unittest.TestCase):
    """mark_dirty, publish_sync_status and the generation guard on set_file_metadata."""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.db_path = self.temp_dir / "state.db"
        self.state_manager = StateManager(str(self.db_path))

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _metadata(self, **overrides):
        values = dict(
            virtual_path="/f.txt",
            size=3,
            checksum="old",
            last_access=1.0,
            last_modified=1.0,
            sync_status=SyncStatus.CLEAN,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        values.update(overrides)
        return FileMetadata(**values)

    def test_mark_dirty_bumps_generation_and_records_stat(self):
        self.state_manager.set_file_metadata(self._metadata())

        self.assertEqual(self.state_manager.mark_dirty("/f.txt", size=10, last_modified=2.0), 1)
        self.assertEqual(self.state_manager.mark_dirty("/f.txt", size=11, last_modified=3.0), 2)

        row = self.state_manager.get_file_metadata("/f.txt")
        self.assertEqual(row.sync_status, SyncStatus.DIRTY)
        self.assertEqual(row.dirty_generation, 2)
        self.assertEqual(row.size, 11)
        self.assertEqual(row.last_modified, 3.0)

    def test_mark_dirty_without_row_returns_none(self):
        self.assertIsNone(self.state_manager.mark_dirty("/missing", size=1, last_modified=1.0))

    def test_publish_sync_status_refuses_a_stale_generation(self):
        self.state_manager.set_file_metadata(self._metadata(sync_status=SyncStatus.DIRTY))
        seen = self.state_manager.get_file_metadata("/f.txt").dirty_generation
        self.state_manager.mark_dirty("/f.txt", size=10, last_modified=2.0)

        published = self.state_manager.publish_sync_status(
            "/f.txt", seen, SyncStatus.CLEAN, checksum="new",
        )

        self.assertFalse(published)
        row = self.state_manager.get_file_metadata("/f.txt")
        self.assertEqual(row.sync_status, SyncStatus.DIRTY)
        self.assertEqual(row.checksum, "old")
        self.assertEqual(row.size, 10)

    def test_publish_sync_status_at_current_generation_sets_status_and_checksum(self):
        self.state_manager.set_file_metadata(self._metadata(sync_status=SyncStatus.DIRTY))
        generation = self.state_manager.mark_dirty("/f.txt", size=10, last_modified=2.0)

        self.assertTrue(self.state_manager.publish_sync_status(
            "/f.txt", generation, SyncStatus.CLEAN, checksum="new",
        ))
        row = self.state_manager.get_file_metadata("/f.txt")
        self.assertEqual(row.sync_status, SyncStatus.CLEAN)
        self.assertEqual(row.checksum, "new")
        self.assertEqual(row.dirty_generation, generation)

        # Without a checksum the stored one is kept.
        self.assertTrue(self.state_manager.publish_sync_status("/f.txt", generation, SyncStatus.ERROR))
        self.assertEqual(self.state_manager.get_file_metadata("/f.txt").checksum, "new")

    def test_set_file_metadata_with_stale_snapshot_keeps_newer_content_columns(self):
        self.state_manager.set_file_metadata(self._metadata())
        snapshot = self.state_manager.get_file_metadata("/f.txt")
        self.state_manager.mark_dirty("/f.txt", size=10, last_modified=2.0)

        # A read-modify-write from before the write: policy change plus a
        # stale CLEAN. The policy lands, the content columns do not move back.
        snapshot.replica_count = 3
        snapshot.sync_status = SyncStatus.CLEAN
        snapshot.size = 3
        self.state_manager.set_file_metadata(snapshot)

        row = self.state_manager.get_file_metadata("/f.txt")
        self.assertEqual(row.replica_count, 3)
        self.assertEqual(row.sync_status, SyncStatus.DIRTY)
        self.assertEqual(row.size, 10)
        self.assertEqual(row.last_modified, 2.0)
        self.assertEqual(row.dirty_generation, 1)

    def test_set_file_metadata_at_current_generation_applies_everything(self):
        self.state_manager.set_file_metadata(self._metadata())
        self.state_manager.mark_dirty("/f.txt", size=10, last_modified=2.0)
        current = self.state_manager.get_file_metadata("/f.txt")

        current.sync_status = SyncStatus.CONFLICT
        current.checksum = "resolved"
        self.state_manager.set_file_metadata(current)

        row = self.state_manager.get_file_metadata("/f.txt")
        self.assertEqual(row.sync_status, SyncStatus.CONFLICT)
        self.assertEqual(row.checksum, "resolved")
        self.assertEqual(row.dirty_generation, 1)

    def test_index_without_generation_column_is_migrated(self):
        self.state_manager.close()
        self.db_path.unlink()
        import sqlite3

        conn = sqlite3.connect(str(self.db_path))
        conn.executescript(
            '''
            CREATE TABLE files (
                virtual_path TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                checksum TEXT,
                last_access REAL NOT NULL,
                last_modified REAL NOT NULL,
                sync_status TEXT NOT NULL,
                replica_count INTEGER NOT NULL,
                recomputable INTEGER NOT NULL,
                recompute_cmd TEXT,
                encrypted INTEGER NOT NULL,
                encryption_key_id TEXT
            );
            INSERT INTO files VALUES ('/old.txt', 5, NULL, 1.0, 1.0, 'dirty', 1, 0, NULL, 0, NULL);
            '''
        )
        conn.commit()
        conn.close()

        self.state_manager = StateManager(str(self.db_path))
        row = self.state_manager.get_file_metadata("/old.txt")
        self.assertEqual(row.dirty_generation, 0)
        self.assertEqual(row.sync_status, SyncStatus.DIRTY)
        self.assertEqual(self.state_manager.mark_dirty("/old.txt", size=6, last_modified=2.0), 1)
        self.assertEqual([m.virtual_path for m in self.state_manager.get_dirty_files()], ["/old.txt"])

    def test_mark_dirty_from_cache_file_records_the_file_stat(self):
        self.state_manager.set_file_metadata(self._metadata())
        cache_path = self.temp_dir / "f.txt"
        cache_path.write_bytes(b"twelve bytes")

        self.assertEqual(self.state_manager.mark_dirty_from_cache_file("/f.txt", cache_path), 1)

        row = self.state_manager.get_file_metadata("/f.txt")
        self.assertEqual(row.sync_status, SyncStatus.DIRTY)
        self.assertEqual(row.size, 12)
        self.assertEqual(row.last_modified, cache_path.lstat().st_mtime)

    def test_reprobe_promotion_from_a_stale_snapshot_keeps_newer_content(self):
        self.state_manager.set_file_metadata(self._metadata())
        snapshot = self.state_manager.get_file_metadata("/f.txt")
        self.state_manager.mark_dirty("/f.txt", size=10, last_modified=2.0)

        snapshot.sync_status = SyncStatus.CONFLICT
        snapshot.replica_count = 2
        self.state_manager.apply_reprobe_backend_update(
            promoted_metadata=[snapshot],
            backend_name="remote0",
            virtual_paths=[],
            locations=[],
            is_cached=False,
        )

        row = self.state_manager.get_file_metadata("/f.txt")
        self.assertEqual(row.replica_count, 2)
        self.assertEqual(row.sync_status, SyncStatus.DIRTY)
        self.assertEqual(row.size, 10)
        self.assertEqual(row.dirty_generation, 1)

    def test_record_cached_stat_moves_only_the_stat_at_the_generation_read(self):
        self.state_manager.set_file_metadata(self._metadata(checksum="synced"))
        row = self.state_manager.get_file_metadata("/f.txt")

        self.assertTrue(self.state_manager.record_cached_stat(
            "/f.txt", row.dirty_generation, size=42, last_modified=7.0, last_access=8.0,
        ))

        updated = self.state_manager.get_file_metadata("/f.txt")
        self.assertEqual((updated.size, updated.last_modified, updated.last_access), (42, 7.0, 8.0))
        self.assertEqual(updated.sync_status, SyncStatus.CLEAN)
        self.assertEqual(updated.checksum, "synced")
        self.assertEqual(updated.dirty_generation, row.dirty_generation)

    def test_record_cached_stat_is_refused_after_a_write(self):
        self.state_manager.set_file_metadata(self._metadata())
        row = self.state_manager.get_file_metadata("/f.txt")
        self.state_manager.mark_dirty("/f.txt", size=10, last_modified=2.0)

        self.assertFalse(self.state_manager.record_cached_stat(
            "/f.txt", row.dirty_generation, size=42, last_modified=7.0, last_access=8.0,
        ))

        updated = self.state_manager.get_file_metadata("/f.txt")
        self.assertEqual((updated.size, updated.last_modified), (10, 2.0))
        self.assertEqual(updated.sync_status, SyncStatus.DIRTY)
