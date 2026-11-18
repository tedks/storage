import shutil
import tempfile
import unittest
from pathlib import Path

from storage.backends.local import LocalBackend
from storage.file_config import FileConfig
from storage.replication import ReplicationManager
from storage.state import StateManager


class TestReplicationManager(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "state.db"
        self.backend1_dir = Path(self.temp_dir) / "backend1"
        self.backend2_dir = Path(self.temp_dir) / "backend2"
        self.backend1_dir.mkdir()
        self.backend2_dir.mkdir()

        self.state_manager = StateManager(str(self.db_path))
        self.backend1 = LocalBackend("backend1", str(self.backend1_dir))
        self.backend2 = LocalBackend("backend2", str(self.backend2_dir))
        self.replication_manager = ReplicationManager(
            self.state_manager,
            {"backend1": self.backend1, "backend2": self.backend2},
        )

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_replicate_file_copies_to_target_backends(self):
        path = "/replicate.txt"
        source_path = self.backend1_dir / path.lstrip("/")
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text("payload")

        results = self.replication_manager.replicate_file(
            path,
            source_backend=self.backend1,
            target_backends=[self.backend1, self.backend2],
        )

        self.assertTrue(results["backend2"])
        self.assertTrue((self.backend2_dir / path.lstrip("/")).exists())

    def test_excluded_members_are_left_out_as_if_down(self):
        """An excluded member is not selected, even when preferred, and the
        replica count is filled from the others, as for a member that is
        down."""
        for replica_count, preferred in ((1, ["backend1"]), (1, None), (-1, None)):
            config = FileConfig(replica_count=replica_count, preferred_backends=preferred)
            selected = self.replication_manager.select_backends_for_file(
                "/f.txt", config, exclude={"backend1"})
            self.assertEqual(["backend2"], [b.name for b in selected], (replica_count, preferred))
        config = FileConfig(replica_count=1)
        self.assertEqual([], self.replication_manager.select_backends_for_file(
            "/f.txt", config, exclude={"backend1", "backend2"}))


if __name__ == "__main__":
    unittest.main()
