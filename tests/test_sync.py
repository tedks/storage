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
import shutil
from pathlib import Path

from storage.state import StateManager, FileMetadata, SyncStatus, FileConfig, FileLocation
from storage.cache import CacheManager
from storage.backends.local import LocalBackend
from storage.replication import ReplicationManager
from storage.sync import SyncManager, SyncTask, DirSyncTask, DirOperation


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


class TestDirSyncTask(unittest.TestCase):
    def test_dir_sync_task_creation(self):
        task = DirSyncTask("/dir", DirOperation.MKDIR, priority=100)
        self.assertEqual(task.virtual_path, "/dir")
        self.assertEqual(task.operation, DirOperation.MKDIR)
        self.assertEqual(task.priority, 100)

    def test_dir_sync_task_rename(self):
        task = DirSyncTask("/old", DirOperation.RENAME, new_path="/new", backend_hint="backend1")
        self.assertEqual(task.new_path, "/new")
        self.assertEqual(task.backend_hint, "backend1")

    def test_dir_sync_task_priority(self):
        fast = DirSyncTask("/fast", DirOperation.RMDIR, priority=50)
        slow = DirSyncTask("/slow", DirOperation.RMDIR, priority=10)
        self.assertTrue(fast < slow)


class TestDirSyncOperations(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend1_dir = Path(self.temp_dir) / "backend1"
        self.backend2_dir = Path(self.temp_dir) / "backend2"
        self.db_path = Path(self.temp_dir) / "state.db"

        self.cache_dir.mkdir()
        self.backend1_dir.mkdir()
        self.backend2_dir.mkdir()

        self.state_manager = StateManager(str(self.db_path))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 32 * 1024 * 1024)
        self.backends = {
            "backend1": LocalBackend("backend1", str(self.backend1_dir), speed_priority=0),
            "backend2": LocalBackend("backend2", str(self.backend2_dir), speed_priority=5),
        }
        self.replication_manager = ReplicationManager(self.state_manager, self.backends)
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            self.replication_manager,
            num_workers=1,
        )

    def tearDown(self):
        self.sync_manager.stop()
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _wait_for(self, predicate, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.05)
        return predicate()

    def test_mkdir_sync(self):
        self.sync_manager.start()
        test_dir = "/queued"
        self.sync_manager.queue_dir_sync(test_dir, DirOperation.MKDIR, priority=100)
        self.assertTrue(self._wait_for(lambda: (self.backend1_dir / 'queued').is_dir()))
        self.assertTrue(self._wait_for(lambda: (self.backend2_dir / 'queued').is_dir()))

    def test_rmdir_sync(self):
        self.sync_manager.start()
        test_dir = "/toremove"
        (self.backend1_dir / "toremove").mkdir()
        (self.backend2_dir / "toremove").mkdir()
        self.sync_manager.queue_dir_sync(test_dir, DirOperation.RMDIR, priority=100)
        self.assertTrue(self._wait_for(lambda: not (self.backend1_dir / 'toremove').exists()))
        self.assertTrue(self._wait_for(lambda: not (self.backend2_dir / 'toremove').exists()))

    def test_rename_file_sync(self):
        self.sync_manager.start()
        old_path = "/oldname.txt"
        new_path = "/newname.txt"
        for backend_dir in (self.backend1_dir, self.backend2_dir):
            fpath = backend_dir / old_path.lstrip('/')
            fpath.parent.mkdir(parents=True, exist_ok=True)
            fpath.write_text("payload")
        new_cache = self.cache_manager.get_cache_path(new_path)
        new_cache.parent.mkdir(parents=True, exist_ok=True)
        new_cache.write_text("payload")

        self.sync_manager.queue_dir_sync(old_path, DirOperation.RENAME, new_path=new_path, priority=100)

        self.assertTrue(self._wait_for(lambda: (self.backend1_dir / new_path.lstrip('/')).exists()))
        self.assertTrue(self._wait_for(lambda: (self.backend2_dir / new_path.lstrip('/')).exists()))
        self.assertTrue(self._wait_for(lambda: not (self.backend1_dir / old_path.lstrip('/')).exists()))


class TestStateManagerRename(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "state.db"
        self.state_manager = StateManager(str(self.db_path))

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_rename_file_metadata(self):
        meta = FileMetadata(
            virtual_path="/old.txt",
            size=10,
            checksum="abc",
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.CLEAN,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        self.state_manager.set_file_metadata(meta)
        self.state_manager.rename_file("/old.txt", "/new.txt")
        self.assertIsNone(self.state_manager.get_file_metadata("/old.txt"))
        renamed = self.state_manager.get_file_metadata("/new.txt")
        self.assertIsNotNone(renamed)
        self.assertEqual(renamed.size, 10)

    def test_rename_path_recursive(self):
        child = FileMetadata(
            virtual_path="/dir/child.txt",
            size=1,
            checksum="x",
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.CLEAN,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        self.state_manager.set_file_metadata(child)
        self.state_manager.rename_path("/dir", "/renamed")
        self.assertIsNone(self.state_manager.get_file_metadata("/dir/child.txt"))
        self.assertIsNotNone(self.state_manager.get_file_metadata("/renamed/child.txt"))


class TestPathNormalization(unittest.TestCase):
    def test_normalize_empty_path(self):
        self.assertEqual(StateManager._normalize_virtual_path(""), "/")

    def test_normalize_root(self):
        self.assertEqual(StateManager._normalize_virtual_path("/"), "/")

    def test_normalize_adds_leading_slash(self):
        self.assertEqual(StateManager._normalize_virtual_path("foo/bar"), "/foo/bar")

    def test_normalize_removes_trailing_slash(self):
        self.assertEqual(StateManager._normalize_virtual_path("/foo/bar/"), "/foo/bar")

    def test_normalize_preserves_valid_path(self):
        self.assertEqual(StateManager._normalize_virtual_path("/foo/bar"), "/foo/bar")


class TestHierarchicalRename(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "state.db"
        self.state_manager = StateManager(str(self.db_path))

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_meta(self, path: str):
        meta = FileMetadata(
            virtual_path=path,
            size=100,
            checksum="abc",
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.CLEAN,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        self.state_manager.set_file_metadata(meta)

    def test_single_file(self):
        self._write_meta("/old.txt")
        renamed = self.state_manager.rename_path("/old.txt", "/new.txt")
        self.assertEqual(len(renamed), 1)
        self.assertIsNone(self.state_manager.get_file_metadata("/old.txt"))
        self.assertIsNotNone(self.state_manager.get_file_metadata("/new.txt"))

    def test_directory_children(self):
        self._write_meta("/dir/a.txt")
        self._write_meta("/dir/b.txt")
        self._write_meta("/dir/sub/c.txt")
        renamed = self.state_manager.rename_path("/dir", "/newdir")
        self.assertEqual(len(renamed), 3)
        self.assertIsNotNone(self.state_manager.get_file_metadata("/newdir/a.txt"))
        self.assertIsNotNone(self.state_manager.get_file_metadata("/newdir/sub/c.txt"))

    def test_keep_locations_in_sync(self):
        self._write_meta("/dir/file.txt")
        location = FileLocation(
            virtual_path="/dir/file.txt",
            backend_name="backend1",
            checksum="abc",
            last_synced=time.time(),
            is_cached=False,
        )
        self.state_manager.add_file_location(location)
        self.state_manager.rename_path("/dir", "/new")
        self.assertEqual(len(self.state_manager.get_file_locations("/dir/file.txt")), 0)
        new_locs = self.state_manager.get_file_locations("/new/file.txt")
        self.assertEqual(len(new_locs), 1)
        self.assertEqual(new_locs[0].backend_name, "backend1")

    def test_path_normalization(self):
        self._write_meta("/dir/file.txt")
        self.state_manager.rename_path("dir/", "newdir")
        self.assertIsNotNone(self.state_manager.get_file_metadata("/newdir/file.txt"))


class TestBackendRename(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.backend_root = Path(self.temp_dir) / "backend"
        self.backend_root.mkdir()
        self.backend = LocalBackend("test", str(self.backend_root))

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_file_rename(self):
        old_file = self.backend_root / "old.txt"
        old_file.write_text("data")
        self.backend.rename("/old.txt", "/new.txt")
        self.assertFalse(old_file.exists())
        self.assertEqual((self.backend_root / "new.txt").read_text(), "data")

    def test_directory_rename(self):
        old_dir = self.backend_root / "olddir"
        old_dir.mkdir()
        (old_dir / "file.txt").write_text("payload")
        self.backend.rename("/olddir", "/newdir")
        self.assertFalse(old_dir.exists())
        self.assertTrue((self.backend_root / "newdir" / "file.txt").exists())

    def test_creates_parent_directories(self):
        src = self.backend_root / "file.txt"
        src.write_text("payload")
        self.backend.rename("/file.txt", "/nested/dir/file.txt")
        self.assertFalse(src.exists())
        self.assertTrue((self.backend_root / "nested/dir/file.txt").exists())


class TestDirQueueDedup(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend_dir = Path(self.temp_dir) / "backend"
        self.cache_dir.mkdir()
        self.backend_dir.mkdir()
        self.state_manager = StateManager(str(Path(self.temp_dir) / "state.db"))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 1_048_576)
        self.replication_manager = ReplicationManager(
            self.state_manager,
            {"backend": LocalBackend("backend", str(self.backend_dir))},
        )
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            self.replication_manager,
            num_workers=0,
        )

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_queue_dir_sync_dedupes_same_backend(self):
        self.sync_manager.queue_dir_sync("/dedupe", DirOperation.MKDIR, backend_hint="backend")
        size_before = self.sync_manager.task_queue.qsize()
        self.sync_manager.queue_dir_sync("/dedupe", DirOperation.MKDIR, backend_hint="backend")
        self.assertEqual(self.sync_manager.task_queue.qsize(), size_before)

    def test_queue_dir_sync_allows_different_backends(self):
        self.sync_manager.replication_manager.backends['backend2'] = LocalBackend(
            "backend2", str(self.backend_dir)
        )
        self.sync_manager.queue_dir_sync("/dir", DirOperation.MKDIR, backend_hint="backend")
        self.sync_manager.queue_dir_sync("/dir", DirOperation.MKDIR, backend_hint="backend2")
        self.assertEqual(self.sync_manager.task_queue.qsize(), 2)

    def test_task_complete_releases_dir_key(self):
        task = DirSyncTask("/dir", DirOperation.MKDIR, backend_hint="backend")
        self.sync_manager.queue_dir_sync(task.virtual_path, task.operation, backend_hint=task.backend_hint)
        self.sync_manager._task_complete(task)
        self.sync_manager.queue_dir_sync(task.virtual_path, task.operation, backend_hint=task.backend_hint)
        self.assertEqual(self.sync_manager.task_queue.qsize(), 2)


class TestHierarchicalRename(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "state.db"
        self.state_manager = StateManager(str(self.db_path))

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_metadata(self, path: str):
        meta = FileMetadata(
            virtual_path=path,
            size=1,
            checksum="x",
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.CLEAN,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        self.state_manager.set_file_metadata(meta)

    def test_rename_path_single_file(self):
        self._create_metadata("/old.txt")
        renamed = self.state_manager.rename_path("/old.txt", "/new.txt")
        self.assertEqual(len(renamed), 1)
        self.assertIsNotNone(self.state_manager.get_file_metadata("/new.txt"))
        self.assertIsNone(self.state_manager.get_file_metadata("/old.txt"))

    def test_rename_path_directory_with_children(self):
        self._create_metadata("/dir/a.txt")
        self._create_metadata("/dir/b.txt")
        self._create_metadata("/dir/sub/c.txt")
        renamed = self.state_manager.rename_path("/dir", "/newdir")
        self.assertEqual(len(renamed), 3)
        self.assertIsNotNone(self.state_manager.get_file_metadata("/newdir/a.txt"))
        self.assertIsNotNone(self.state_manager.get_file_metadata("/newdir/b.txt"))
        self.assertIsNotNone(self.state_manager.get_file_metadata("/newdir/sub/c.txt"))

    def test_rename_path_same_path_noop(self):
        self._create_metadata("/file.txt")
        renamed = self.state_manager.rename_path("/file.txt", "/file.txt")
        self.assertEqual(len(renamed), 0)
        self.assertIsNotNone(self.state_manager.get_file_metadata("/file.txt"))

    def test_rename_path_updates_locations(self):
        self._create_metadata("/dir/file.txt")
        location = FileLocation(
            virtual_path="/dir/file.txt",
            backend_name="backend1",
            checksum="abc",
            last_synced=time.time(),
            is_cached=False,
        )
        self.state_manager.add_file_location(location)
        self.state_manager.rename_path("/dir", "/newdir")
        self.assertEqual(len(self.state_manager.get_file_locations("/dir/file.txt")), 0)
        new_locations = self.state_manager.get_file_locations("/newdir/file.txt")
        self.assertEqual(len(new_locations), 1)
        self.assertEqual(new_locations[0].backend_name, "backend1")

    def test_rename_path_normalizes_inputs(self):
        self._create_metadata("/dir/file.txt")
        self.state_manager.rename_path("dir/", "newdir")
        self.assertIsNotNone(self.state_manager.get_file_metadata("/newdir/file.txt"))


class TestBackendRename(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.backend_dir = Path(self.temp_dir) / "backend"
        self.backend_dir.mkdir()
        self.backend = LocalBackend("test", str(self.backend_dir), speed_priority=0)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_rename_file(self):
        old_path = self.backend_dir / "old.txt"
        old_path.write_text("data")
        self.backend.rename("/old.txt", "/new.txt")
        self.assertFalse(old_path.exists())
        self.assertTrue((self.backend_dir / "new.txt").exists())

    def test_rename_directory(self):
        old_dir = self.backend_dir / "olddir"
        old_dir.mkdir()
        (old_dir / "file.txt").write_text("data")
        self.backend.rename("/olddir", "/newdir")
        self.assertFalse(old_dir.exists())
        self.assertTrue((self.backend_dir / "newdir" / "file.txt").exists())

    def test_rename_creates_parent_dirs(self):
        old_path = self.backend_dir / "file.txt"
        old_path.write_text("data")
        self.backend.rename("/file.txt", "/nested/dir/file.txt")
        self.assertFalse(old_path.exists())
        self.assertTrue((self.backend_dir / "nested/dir/file.txt").exists())


if __name__ == '__main__':
    unittest.main()
