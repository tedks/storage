import time
import unittest

from storage.models import FileLocation, FileMetadata, SyncStatus


class TestFileMetadataModel(unittest.TestCase):
    def test_to_dict_serializes_sync_status_value(self):
        now = time.time()
        metadata = FileMetadata(
            virtual_path="/docs/readme.txt",
            size=42,
            checksum="abc123",
            last_access=now,
            last_modified=now,
            sync_status=SyncStatus.DIRTY,
            replica_count=2,
            recomputable=True,
            recompute_cmd="make readme",
            encrypted=False,
            encryption_key_id=None,
        )

        payload = metadata.to_dict()

        self.assertEqual(payload["sync_status"], "dirty")
        self.assertEqual(payload["virtual_path"], "/docs/readme.txt")

    def test_from_dict_round_trips_metadata(self):
        now = time.time()
        payload = {
            "virtual_path": "/video.mp4",
            "size": 1024,
            "checksum": "deadbeef",
            "last_access": now,
            "last_modified": now,
            "sync_status": "clean",
            "replica_count": 1,
            "recomputable": False,
            "recompute_cmd": None,
            "encrypted": True,
            "encryption_key_id": "key-1",
            "dirty_generation": 7,
        }

        metadata = FileMetadata.from_dict(payload)

        self.assertEqual(metadata.sync_status, SyncStatus.CLEAN)
        self.assertEqual(metadata.to_dict(), payload)

    def test_from_dict_defaults_dirty_generation_for_rows_without_it(self):
        # Index rows and xattr records written before the column existed.
        payload = {
            "virtual_path": "/old.txt",
            "size": 1,
            "checksum": None,
            "last_access": 1.0,
            "last_modified": 1.0,
            "sync_status": "dirty",
            "replica_count": 1,
            "recomputable": False,
            "recompute_cmd": None,
            "encrypted": False,
            "encryption_key_id": None,
        }

        self.assertEqual(FileMetadata.from_dict(payload).dirty_generation, 0)


class TestFileLocationModel(unittest.TestCase):
    def test_dataclass_equality_tracks_backend_and_cache_state(self):
        left = FileLocation("/a", "member0", "abc", 1.0, False)
        right = FileLocation("/a", "member0", "abc", 1.0, False)
        other = FileLocation("/a", "member1", "abc", 1.0, False)

        self.assertEqual(left, right)
        self.assertNotEqual(left, other)


if __name__ == "__main__":
    unittest.main()
