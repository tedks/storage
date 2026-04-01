"""
Tests for SyncExecutor

Tests cover:
- File sync task processing (skip, success, conflict, error, partial)
- Directory sync task processing (mkdir, rmdir, rename, failure)
- Retry and failure handling
"""

import queue
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from storage.backends.base import Backend
from storage.cache import CacheManager
from storage.file_config import FileConfig
from storage.models import FileLocation, FileMetadata, SyncStatus
from storage.replication import ReplicationManager
from storage.state import StateManager
from storage.sync_executor import SyncExecutor
from storage.sync_tasks import DirOperation, DirSyncTask, SyncConfig, SyncTask


def _make_metadata(virtual_path="/test.txt", size=100, sync_status=SyncStatus.DIRTY,
                   checksum="abc123", replica_count=1):
    """Create a FileMetadata with sensible defaults for testing."""
    now = time.time()
    return FileMetadata(
        virtual_path=virtual_path,
        size=size,
        checksum=checksum,
        last_access=now,
        last_modified=now,
        sync_status=sync_status,
        replica_count=replica_count,
        recomputable=False,
        recompute_cmd=None,
        encrypted=False,
        encryption_key_id=None,
    )


def _make_mock_backend(name="test0"):
    """Create a MagicMock backend with spec=Backend and sensible defaults."""
    backend = MagicMock(spec=Backend)
    backend.name = name
    backend.is_available.return_value = True
    backend.speed_priority = 0
    backend.copy_from_local = MagicMock()
    backend.sync = MagicMock()
    backend.mkdir = MagicMock()
    backend.rmdir = MagicMock()
    backend.rename = MagicMock()
    return backend


class _ExecutorTestBase(unittest.TestCase):
    """Shared setup for SyncExecutor tests."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.cache_dir.mkdir()
        self.db_path = Path(self.temp_dir) / "state.db"

        self.state_manager = StateManager(str(self.db_path))
        self.cache_manager = CacheManager(
            self.cache_dir, self.state_manager, 100 * 1024 * 1024,
        )

        self.mock_backend = _make_mock_backend("test0")
        self.backends = {"test0": self.mock_backend}

        self.replication_manager = ReplicationManager(
            self.state_manager, self.backends,
        )

        self.config = SyncConfig(
            monitor_interval=1,
            max_retries=3,
            retry_delay=0,
        )

        self.executor = SyncExecutor(
            worker_id=0,
            state_manager=self.state_manager,
            cache_manager=self.cache_manager,
            replication_manager=self.replication_manager,
            config=self.config,
        )

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_cache_file(self, virtual_path="/test.txt", content=b"test content"):
        """Write a real file into the cache directory."""
        cache_path = self.cache_manager.get_cache_path(virtual_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(content)
        return cache_path


class TestSyncExecutorFileTask(_ExecutorTestBase):
    """Tests for SyncExecutor.process_file_task."""

    def test_sync_clean_file_skipped(self):
        """File already CLEAN -> returns True, no backend call."""
        meta = _make_metadata(sync_status=SyncStatus.CLEAN)
        self.state_manager.set_file_metadata(meta)
        self._create_cache_file()

        task = SyncTask("/test.txt")
        result = self.executor.process_file_task(task)

        self.assertTrue(result)
        self.mock_backend.copy_from_local.assert_not_called()

    def test_sync_missing_metadata_skipped(self):
        """No metadata for path -> returns True."""
        task = SyncTask("/nonexistent.txt")
        result = self.executor.process_file_task(task)

        self.assertTrue(result)
        self.mock_backend.copy_from_local.assert_not_called()

    def test_sync_missing_cache_file_skipped(self):
        """Metadata exists but cache file missing -> returns True."""
        meta = _make_metadata()
        self.state_manager.set_file_metadata(meta)
        # Do NOT create the cache file.

        task = SyncTask("/test.txt")
        result = self.executor.process_file_task(task)

        self.assertTrue(result)
        self.mock_backend.copy_from_local.assert_not_called()

    def test_no_backends_returns_true_and_marks_error(self):
        """No backends available -> returns True (task complete), marks ERROR, increments failed."""
        meta = _make_metadata(sync_status=SyncStatus.DIRTY)
        self.state_manager.set_file_metadata(meta)
        self._create_cache_file()

        with patch.object(
            self.executor.replication_manager,
            "select_backends_for_file",
            return_value=[],
        ):
            task = SyncTask("/test.txt")
            result = self.executor.process_file_task(task)

        self.assertTrue(result)
        updated = self.state_manager.get_file_metadata("/test.txt")
        self.assertEqual(updated.sync_status, SyncStatus.ERROR)
        self.assertEqual(self.executor.get_stats()["failed"], 1)

    def test_sync_success_marks_clean(self):
        """DIRTY file synced to backend -> status becomes CLEAN, location recorded."""
        meta = _make_metadata(sync_status=SyncStatus.DIRTY)
        self.state_manager.set_file_metadata(meta)
        self._create_cache_file()

        with patch.object(self.executor.conflict_detector, "check_conflict", return_value=None):
            task = SyncTask("/test.txt")
            result = self.executor.process_file_task(task)

        self.assertTrue(result)

        updated = self.state_manager.get_file_metadata("/test.txt")
        self.assertEqual(updated.sync_status, SyncStatus.CLEAN)

        locations = self.state_manager.get_file_locations("/test.txt")
        backend_names = {loc.backend_name for loc in locations}
        self.assertIn("test0", backend_names)

        self.mock_backend.copy_from_local.assert_called_once()

    def test_sync_conflict_detected(self):
        """ConflictDetector returns conflict -> status becomes CONFLICT."""
        meta = _make_metadata(sync_status=SyncStatus.DIRTY)
        self.state_manager.set_file_metadata(meta)
        self._create_cache_file()

        fake_conflict = MagicMock()
        with patch.object(self.executor.conflict_detector, "check_conflict", return_value=fake_conflict):
            task = SyncTask("/test.txt")
            result = self.executor.process_file_task(task)

        self.assertTrue(result)

        updated = self.state_manager.get_file_metadata("/test.txt")
        self.assertEqual(updated.sync_status, SyncStatus.CONFLICT)
        self.assertEqual(self.executor.get_stats()["conflicts"], 1)

        self.mock_backend.copy_from_local.assert_not_called()

    def test_sync_backend_failure_marks_error(self):
        """Backend.copy_from_local raises -> status becomes ERROR."""
        meta = _make_metadata(sync_status=SyncStatus.DIRTY)
        self.state_manager.set_file_metadata(meta)
        self._create_cache_file()

        self.mock_backend.copy_from_local.side_effect = OSError("disk full")

        with patch.object(self.executor.conflict_detector, "check_conflict", return_value=None):
            task = SyncTask("/test.txt")
            result = self.executor.process_file_task(task)

        self.assertTrue(result)

        updated = self.state_manager.get_file_metadata("/test.txt")
        self.assertEqual(updated.sync_status, SyncStatus.ERROR)
        self.assertEqual(self.executor.get_stats()["failed"], 1)

    def test_partial_sync_stays_dirty(self):
        """2 target backends, 1 fails -> status stays DIRTY (partial sync)."""
        meta = _make_metadata(sync_status=SyncStatus.DIRTY, replica_count=2)
        self.state_manager.set_file_metadata(meta)
        self._create_cache_file()

        backend1 = _make_mock_backend("b1")
        backend2 = _make_mock_backend("b2")
        backend2.copy_from_local.side_effect = OSError("network error")

        self.replication_manager.backends = {"b1": backend1, "b2": backend2}

        two_replica_config = FileConfig(replica_count=2)
        with patch.object(self.executor.conflict_detector, "check_conflict", return_value=None), \
             patch("storage.sync_executor.get_inherited_config", return_value=two_replica_config):
            task = SyncTask("/test.txt")
            result = self.executor.process_file_task(task)

        self.assertTrue(result)

        updated = self.state_manager.get_file_metadata("/test.txt")
        self.assertEqual(updated.sync_status, SyncStatus.DIRTY)

    def test_should_sync_path_cb_defers(self):
        """Callback returns False -> task deferred, returns True."""
        meta = _make_metadata(sync_status=SyncStatus.DIRTY)
        self.state_manager.set_file_metadata(meta)
        self._create_cache_file()

        executor = SyncExecutor(
            worker_id=0,
            state_manager=self.state_manager,
            cache_manager=self.cache_manager,
            replication_manager=self.replication_manager,
            config=self.config,
            should_sync_path_cb=lambda _path: False,
        )

        task = SyncTask("/test.txt")
        result = executor.process_file_task(task)

        self.assertTrue(result)
        # File should still be DIRTY -- sync was deferred, not performed.
        updated = self.state_manager.get_file_metadata("/test.txt")
        self.assertEqual(updated.sync_status, SyncStatus.DIRTY)
        self.mock_backend.copy_from_local.assert_not_called()


class TestSyncExecutorDirTask(_ExecutorTestBase):
    """Tests for SyncExecutor.process_dir_task."""

    def test_mkdir_calls_backend(self):
        """DirSyncTask with MKDIR -> backend.mkdir called."""
        task = DirSyncTask(virtual_path="/newdir", operation=DirOperation.MKDIR)
        result = self.executor.process_dir_task(task)

        self.assertTrue(result)
        self.mock_backend.mkdir.assert_called_once_with("/newdir")

    def test_rmdir_calls_backend(self):
        """DirSyncTask with RMDIR -> backend.rmdir called."""
        task = DirSyncTask(virtual_path="/olddir", operation=DirOperation.RMDIR)
        result = self.executor.process_dir_task(task)

        self.assertTrue(result)
        self.mock_backend.rmdir.assert_called_once_with("/olddir")

    def test_rename_calls_backend(self):
        """DirSyncTask with RENAME and new_path -> backend.rename called."""
        task = DirSyncTask(
            virtual_path="/oldname",
            operation=DirOperation.RENAME,
            new_path="/newname",
        )
        result = self.executor.process_dir_task(task)

        self.assertTrue(result)
        self.mock_backend.rename.assert_called_once_with("/oldname", "/newname")

    def test_dir_task_backend_failure_raises(self):
        """Backend raises -> RuntimeError propagated."""
        self.mock_backend.mkdir.side_effect = OSError("permission denied")

        task = DirSyncTask(virtual_path="/faildir", operation=DirOperation.MKDIR)
        with self.assertRaises(RuntimeError):
            self.executor.process_dir_task(task)


class TestSyncExecutorRetry(_ExecutorTestBase):
    """Tests for SyncExecutor.handle_task_failure."""

    def test_retry_requeues_task(self):
        """retry_count < max_retries -> task put back in queue with lower priority."""
        task_queue = queue.PriorityQueue()
        task = SyncTask("/retry.txt", priority=50, retry_count=0)

        done = self.executor.handle_task_failure(task, "some error", task_queue)

        self.assertFalse(done)
        self.assertFalse(task_queue.empty())

        requeued = task_queue.get_nowait()
        self.assertEqual(requeued.virtual_path, "/retry.txt")
        self.assertEqual(requeued.retry_count, 1)
        # Priority should be reduced by 10.
        self.assertEqual(requeued.priority, 40)

    def test_max_retries_exceeded_marks_error(self):
        """retry_count >= max_retries -> returns True, stats['failed'] incremented, status ERROR."""
        meta = _make_metadata(virtual_path="/doomed.txt", sync_status=SyncStatus.DIRTY)
        self.state_manager.set_file_metadata(meta)

        task_queue = queue.PriorityQueue()
        # handle_task_failure increments retry_count first, so pass max_retries - 1
        # so after increment it becomes 3 which is NOT < 3.
        task = SyncTask("/doomed.txt", priority=50, retry_count=2)

        done = self.executor.handle_task_failure(task, "persistent error", task_queue)

        self.assertTrue(done)
        self.assertTrue(task_queue.empty())
        self.assertEqual(self.executor.get_stats()["failed"], 1)

        updated = self.state_manager.get_file_metadata("/doomed.txt")
        self.assertEqual(updated.sync_status, SyncStatus.ERROR)


if __name__ == "__main__":
    unittest.main()
