"""
Tests for background sync workers

Tests cover:
- Unit tests: SyncWorker, SyncManager components
- Integration tests: Sync with backends
- System tests: End-to-end sync in mounted filesystem
"""

import unittest
import tempfile
import time
import os
from pathlib import Path

from storage.state import StateManager, FileMetadata, SyncStatus, FileConfig, FileLocation
from storage.cache import CacheManager
from storage.backends.local import LocalBackend
from storage.replication import ReplicationManager
from storage.sync import SyncManager, SyncTask


class TestSyncTask(unittest.TestCase):
    """Unit tests for SyncTask"""

    def test_sync_task_creation(self):
        """Test creating a sync task"""
        task = SyncTask("/test.txt", priority=50)
        self.assertEqual(task.virtual_path, "/test.txt")
        self.assertEqual(task.priority, 50)
        self.assertEqual(task.retry_count, 0)

    def test_sync_task_priority(self):
        """Test priority ordering"""
        task1 = SyncTask("/a.txt", priority=10)
        task2 = SyncTask("/b.txt", priority=50)
        task3 = SyncTask("/c.txt", priority=30)

        # Higher priority should be "less than" for min heap
        self.assertTrue(task2 < task1)
        self.assertTrue(task2 < task3)
        self.assertFalse(task1 < task2)


class TestSyncManager(unittest.TestCase):
    """Integration tests for SyncManager"""

    def setUp(self):
        """Set up test environment"""
        # Create temp directories
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend1_dir = Path(self.temp_dir) / "backend1"
        self.backend2_dir = Path(self.temp_dir) / "backend2"
        self.db_path = Path(self.temp_dir) / "state.db"

        self.cache_dir.mkdir()
        self.backend1_dir.mkdir()
        self.backend2_dir.mkdir()

        # Initialize components
        self.state_manager = StateManager(str(self.db_path))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 100 * 1024 * 1024)

        self.backends = {
            "backend1": LocalBackend("backend1", str(self.backend1_dir), speed_priority=0),
            "backend2": LocalBackend("backend2", str(self.backend2_dir), speed_priority=10),
        }

        self.replication_manager = ReplicationManager(self.state_manager, self.backends)
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            self.replication_manager,
            num_workers=2
        )

    def tearDown(self):
        """Clean up test environment"""
        self.sync_manager.stop()
        self.state_manager.close()

        # Clean up temp directory
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_sync_manager_start_stop(self):
        """Test starting and stopping sync manager"""
        self.sync_manager.start()
        self.assertTrue(len(self.sync_manager.workers) > 0)

        self.sync_manager.stop()
        # Workers should be stopped
        for worker in self.sync_manager.workers:
            self.assertFalse(worker.is_alive())

    def test_queue_sync(self):
        """Test queuing a sync task"""
        self.sync_manager.start()

        # Create a file in cache
        test_file = "/test.txt"
        cache_path = self.cache_manager.get_cache_path(test_file)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("test content")

        # Create metadata
        metadata = FileMetadata(
            virtual_path=test_file,
            size=len("test content"),
            checksum=None,
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.DIRTY,
            replica_count=2,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None
        )
        self.state_manager.set_file_metadata(metadata)

        # Set config for replication
        config = FileConfig(replica_count=2)
        self.state_manager.set_file_config(cache_path, config)

        # Queue sync
        self.sync_manager.queue_sync(test_file, priority=100)

        # Wait for sync to complete
        time.sleep(2.0)

        # Check that file was synced
        locations = self.state_manager.get_file_locations(test_file)
        self.assertGreater(len(locations), 0, "File should be synced to at least one backend")

        # Check that backends have the file
        self.assertTrue(self.backends["backend1"].exists(test_file) or
                       self.backends["backend2"].exists(test_file))

    def test_sync_with_two_replicas(self):
        """Test syncing file with 2-way replication"""
        self.sync_manager.start()

        # Create a file in cache
        test_file = "/important.txt"
        cache_path = self.cache_manager.get_cache_path(test_file)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        test_content = "important data"
        cache_path.write_text(test_content)

        # Create metadata with 2 replicas
        metadata = FileMetadata(
            virtual_path=test_file,
            size=len(test_content),
            checksum=None,
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.DIRTY,
            replica_count=2,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None
        )
        self.state_manager.set_file_metadata(metadata)

        # Set config
        config = FileConfig(replica_count=2)
        self.state_manager.set_file_config(cache_path, config)

        # Queue sync and wait
        self.sync_manager.queue_sync(test_file, priority=100)
        time.sleep(3.0)

        # Both backends should have the file
        self.assertTrue(self.backends["backend1"].exists(test_file))
        self.assertTrue(self.backends["backend2"].exists(test_file))

        # Content should match
        backend1_content = (self.backend1_dir / test_file.lstrip('/')).read_text()
        backend2_content = (self.backend2_dir / test_file.lstrip('/')).read_text()
        self.assertEqual(backend1_content, test_content)
        self.assertEqual(backend2_content, test_content)

        # Metadata should be CLEAN
        updated_metadata = self.state_manager.get_file_metadata(test_file)
        self.assertEqual(updated_metadata.sync_status, SyncStatus.CLEAN)

    def test_sync_now_synchronous(self):
        """Test synchronous sync operation"""
        self.sync_manager.start()

        # Create a file
        test_file = "/sync_now.txt"
        cache_path = self.cache_manager.get_cache_path(test_file)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("sync now test")

        # Create metadata
        metadata = FileMetadata(
            virtual_path=test_file,
            size=len("sync now test"),
            checksum=None,
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.DIRTY,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None
        )
        self.state_manager.set_file_metadata(metadata)

        # Set config
        config = FileConfig(replica_count=1)
        self.state_manager.set_file_config(cache_path, config)

        # Sync now (synchronous)
        result = self.sync_manager.sync_now(test_file, timeout=5.0)
        self.assertTrue(result, "Sync should complete successfully")

        # File should be synced
        self.assertTrue(self.backends["backend1"].exists(test_file) or
                       self.backends["backend2"].exists(test_file))

        # Status should be CLEAN
        updated_metadata = self.state_manager.get_file_metadata(test_file)
        self.assertEqual(updated_metadata.sync_status, SyncStatus.CLEAN)

    def test_dirty_file_monitoring(self):
        """Test that sync manager automatically picks up dirty files"""
        self.sync_manager.start()

        # Create a dirty file
        test_file = "/monitored.txt"
        cache_path = self.cache_manager.get_cache_path(test_file)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("monitored content")

        # Create metadata as dirty
        metadata = FileMetadata(
            virtual_path=test_file,
            size=len("monitored content"),
            checksum=None,
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.DIRTY,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None
        )
        self.state_manager.set_file_metadata(metadata)

        # Set config
        config = FileConfig(replica_count=1)
        self.state_manager.set_file_config(cache_path, config)

        # Wait for monitor to pick it up (monitor runs every 5 seconds)
        time.sleep(7.0)

        # File should eventually be synced
        updated_metadata = self.state_manager.get_file_metadata(test_file)
        # Should be either CLEAN or SYNCING
        self.assertIn(updated_metadata.sync_status, [SyncStatus.CLEAN, SyncStatus.SYNCING])


class TestSyncConflicts(unittest.TestCase):
    """Tests for conflict detection during sync"""

    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend_dir = Path(self.temp_dir) / "backend"
        self.db_path = Path(self.temp_dir) / "state.db"

        self.cache_dir.mkdir()
        self.backend_dir.mkdir()

        self.state_manager = StateManager(str(self.db_path))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 100 * 1024 * 1024)

        self.backends = {
            "backend": LocalBackend("backend", str(self.backend_dir), speed_priority=0),
        }

        self.replication_manager = ReplicationManager(self.state_manager, self.backends)
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            self.replication_manager,
            num_workers=1
        )

    def tearDown(self):
        """Clean up"""
        self.sync_manager.stop()
        self.state_manager.close()
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_conflict_detection(self):
        """Test that conflicts are detected"""
        self.sync_manager.start()

        test_file = "/conflict.txt"

        # Create file in backend with different content
        backend_path = self.backend_dir / test_file.lstrip('/')
        backend_path.parent.mkdir(parents=True, exist_ok=True)
        backend_path.write_text("backend version")

        # Record location with old checksum
        old_checksum = self.state_manager.compute_checksum(backend_path)
        location = FileLocation(
            virtual_path=test_file,
            backend_name="backend",
            checksum=old_checksum,
            last_synced=time.time(),
            is_cached=False
        )
        self.state_manager.add_file_location(location)

        # Now modify the backend file (simulating external change)
        backend_path.write_text("modified externally")

        # Create different content in cache
        cache_path = self.cache_manager.get_cache_path(test_file)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("cache version")

        # Create metadata
        metadata = FileMetadata(
            virtual_path=test_file,
            size=len("cache version"),
            checksum=None,
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.DIRTY,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None
        )
        self.state_manager.set_file_metadata(metadata)

        # Set config
        config = FileConfig(replica_count=1)
        self.state_manager.set_file_config(cache_path, config)

        # Try to sync - should detect conflict
        self.sync_manager.sync_now(test_file, timeout=3.0)

        # Should be marked as CONFLICT
        updated_metadata = self.state_manager.get_file_metadata(test_file)
        self.assertEqual(updated_metadata.sync_status, SyncStatus.CONFLICT)


if __name__ == '__main__':
    unittest.main()
