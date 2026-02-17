import shutil
import tempfile
import time
import unittest
from pathlib import Path

from storage.state import StateManager, FileMetadata, SyncStatus


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


if __name__ == "__main__":
    unittest.main()
