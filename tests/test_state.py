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
