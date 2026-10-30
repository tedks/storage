"""
Background sync workers for Storage

Handles asynchronous synchronization of files from cache to backing filesystems.
Monitors dirty files and syncs them according to their replication configuration.
"""

import sys
import time
import threading
import queue
import logging
from pathlib import Path
from typing import Callable, Optional, Set, Union, Tuple

from .backends.base import Backend
from .cache import CacheManager
from .models import SyncStatus
from .replication import ReplicationManager
from .state import StateManager
from .sync_executor import SyncExecutor
from .sync_tasks import DirOperation, DirSyncTask, SyncConfig, SyncTask, dir_task_key

logger = logging.getLogger(__name__)

# Posted to the task queue by SyncManager.stop() so idle workers wake at once
# instead of at the end of their poll. Sorts ahead of every real task.
_WAKE = SyncTask(virtual_path="", priority=sys.maxsize)

__all__ = [
    "DirOperation",
    "DirSyncTask",
    "SyncConfig",
    "SyncManager",
    "SyncTask",
]


class SyncWorker(threading.Thread):
    """
    Worker thread that syncs files from cache to backends

    Processes sync tasks from a queue and updates file state.
    """

    def __init__(self, worker_id: int, task_queue: queue.PriorityQueue,
                 state_manager: StateManager, cache_manager: CacheManager,
                 replication_manager: ReplicationManager, config: SyncConfig,
                 task_complete_cb=None, should_sync_path_cb: Optional[Callable[[str], bool]] = None,
                 member_written_cb: Optional[Callable[[str, Backend, Path], None]] = None):
        super().__init__(daemon=True, name=f"SyncWorker-{worker_id}")
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.state_manager = state_manager
        self.cache_manager = cache_manager
        self.replication_manager = replication_manager
        self.config = config
        self.executor = SyncExecutor(
            worker_id=worker_id,
            state_manager=state_manager,
            cache_manager=cache_manager,
            replication_manager=replication_manager,
            config=config,
            should_sync_path_cb=should_sync_path_cb,
            member_written_cb=member_written_cb,
        )
        self._stop_event = threading.Event()
        self._active_task: Optional[str] = None
        self._task_complete_cb = task_complete_cb

    def run(self):
        """Main worker loop"""
        logger.info(f"Sync worker {self.worker_id} started")

        while not self._stop_event.is_set():
            try:
                # Get task with timeout to check stop event
                try:
                    task = self.task_queue.get(timeout=1.0)
                except queue.Empty:
                    continue

                if task is _WAKE:
                    # stop() posted this to end the idle wait; nothing to run.
                    self.task_queue.task_done()
                    continue

                self._active_task = task.virtual_path

                try:
                    success = self.executor.process(task)
                    if success:
                        self._notify_task_complete(task)
                except Exception as e:
                    logger.error(f"Error processing sync task {task.virtual_path}: {e}")
                    completed = self.executor.handle_task_failure(
                        task,
                        str(e),
                        self.task_queue,
                    )
                    if completed:
                        self._notify_task_complete(task)
                finally:
                    self._active_task = None
                    self.task_queue.task_done()

            except Exception as e:
                logger.error(f"Worker {self.worker_id} error: {e}")

        logger.info(f"Sync worker {self.worker_id} stopped")

    def _notify_task_complete(self, task):
        if self._task_complete_cb:
            try:
                self._task_complete_cb(task)
            except Exception:
                logger.exception("Task completion callback failed")

    def stop(self):
        """Stop the worker thread"""
        self._stop_event.set()

    def get_active_task(self) -> Optional[str]:
        """Get currently active task"""
        return self._active_task

    def get_stats(self) -> dict:
        """Get worker statistics"""
        return self.executor.get_stats()


class SyncManager:
    """
    Manages background sync workers

    Coordinates multiple worker threads and queues sync tasks for dirty files.
    """

    def __init__(self, state_manager: StateManager, cache_manager: CacheManager,
                 replication_manager: ReplicationManager, num_workers: int = 2,
                 config: Optional[SyncConfig] = None,
                 should_sync_path_cb: Optional[Callable[[str], bool]] = None,
                 member_written_cb: Optional[Callable[[str, Backend, Path], None]] = None):
        """
        Initialize sync manager

        Args:
            state_manager: State manager instance
            cache_manager: Cache manager instance
            replication_manager: Replication manager instance
            num_workers: Number of sync worker threads
            config: Sync configuration (uses defaults if None)
            should_sync_path_cb: Whether a path may sync now (no open writer)
            member_written_cb: Called (virtual_path, member, cache_path)
                after each upload to a member
        """
        self.state_manager = state_manager
        self.cache_manager = cache_manager
        self.replication_manager = replication_manager
        self.num_workers = num_workers
        self.config = config or SyncConfig()
        self._should_sync_path_cb = should_sync_path_cb
        self._member_written_cb = member_written_cb

        # Task queue (priority queue)
        self.task_queue = queue.PriorityQueue()

        # Workers
        self.workers = []
        self._running = False
        # Set by stop() to end the monitor's sleep between scans.
        self._monitor_stop = threading.Event()
        self._monitor_thread = None
        self._queued_tasks: Set[str] = set()
        self._queued_dir_tasks: Set[Tuple[str, str, Optional[str]]] = set()
        self._queue_lock = threading.Lock()

    def start(self):
        """Start sync workers"""
        if self._running:
            logger.warning("Sync manager already running")
            return

        logger.info(f"Starting sync manager with {self.num_workers} workers")

        # Start workers
        for i in range(self.num_workers):
            worker = SyncWorker(
                i, self.task_queue, self.state_manager,
                self.cache_manager, self.replication_manager,
                self.config, self._task_complete,
                should_sync_path_cb=self._should_sync_path_cb,
                member_written_cb=self._member_written_cb,
            )
            worker.start()
            self.workers.append(worker)

        # Start monitor thread
        self._running = True
        self._monitor_stop.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

        logger.info("Sync manager started")

    def stop(self):
        """Stop sync workers"""
        if not self._running:
            return

        logger.info("Stopping sync manager")
        self._running = False
        self._monitor_stop.set()

        # Stop workers; one wake task each ends an idle worker's queue wait
        for worker in self.workers:
            worker.stop()
            self.task_queue.put(_WAKE)

        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=5.0)

        # Wait for monitor
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)

        self.workers.clear()
        logger.info("Sync manager stopped")

    def _monitor_loop(self):
        """Monitor for dirty files and queue them for sync"""
        logger.info("Sync monitor started")

        while self._running:
            try:
                # Get dirty files
                dirty_files = self.state_manager.get_dirty_files()

                # Queue new tasks
                for metadata in dirty_files:
                    if self._should_sync_path_cb and not self._should_sync_path_cb(metadata.virtual_path):
                        continue
                    self.queue_sync(metadata.virtual_path, priority=50)

                # Sleep before next check
                self._monitor_stop.wait(self.config.monitor_interval)

            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                time.sleep(1.0)

        logger.info("Sync monitor stopped")

    def queue_sync(self, virtual_path: str, priority: int = 50, force: bool = False):
        """
        Queue a file for syncing

        Args:
            virtual_path: Virtual path of file to sync
            priority: Sync priority (higher = sooner)
            force: Force sync even if already queued
        """
        with self._queue_lock:
            if not force and virtual_path in self._queued_tasks:
                return

            task = SyncTask(virtual_path=virtual_path, priority=priority)
            self.task_queue.put(task)
            self._queued_tasks.add(virtual_path)

            logger.debug(f"Queued sync for {virtual_path} (priority={priority})")

    def sync_now(self, virtual_path: str, timeout: Optional[float] = None) -> bool:
        """
        Synchronously sync a file (blocks until complete or timeout)

        Args:
            virtual_path: Virtual path of file to sync
            timeout: Maximum time to wait in seconds

        Returns:
            True if sync completed successfully
        """
        # Queue with high priority
        self.queue_sync(virtual_path, priority=1000, force=True)

        # Wait for sync to complete
        start_time = time.time()
        while True:
            metadata = self.state_manager.get_file_metadata(virtual_path)

            if not metadata:
                return False

            if metadata.sync_status == SyncStatus.CLEAN:
                return True

            if metadata.sync_status == SyncStatus.ERROR:
                return False

            if timeout and (time.time() - start_time) > timeout:
                return False

            time.sleep(0.1)

    def queue_dir_sync(self, virtual_path: str, operation: DirOperation,
                       new_path: Optional[str] = None, mode: int = 0o755,
                       priority: int = 50, backend_hint: Optional[str] = None,
                       force: bool = False):
        """Queue a directory operation for syncing."""
        if virtual_path and not virtual_path.startswith('/'):
            virtual_path = '/' + virtual_path
        if new_path and not new_path.startswith('/'):
            new_path = '/' + new_path
        key = dir_task_key(operation, virtual_path, new_path, backend_hint)
        with self._queue_lock:
            if not force and key in self._queued_dir_tasks:
                return

            task = DirSyncTask(
                virtual_path=virtual_path,
                operation=operation,
                new_path=new_path,
                mode=mode,
                backend_hint=backend_hint,
                priority=priority,
            )
            self.task_queue.put(task)
            self._queued_dir_tasks.add(key)
            logger.debug("Queued dir sync %s for %s", operation.value, virtual_path)

    def _task_complete(self, task: Union[SyncTask, DirSyncTask]):
        with self._queue_lock:
            if isinstance(task, DirSyncTask):
                key = dir_task_key(
                    task.operation,
                    task.virtual_path,
                    task.new_path,
                    task.backend_hint,
                )
                self._queued_dir_tasks.discard(key)
            elif task.requeue:
                # The file was written while this pass was in flight. Hand
                # the dedup entry straight to a fresh task, under the lock,
                # so there is never a moment with the path unregistered or
                # registered twice. (queue_sync would re-take _queue_lock.)
                # Default priority: a file written continuously must not
                # crowd out everything else by requeueing ahead of it.
                self.task_queue.put(SyncTask(virtual_path=task.virtual_path))
                logger.debug("Re-queued sync for %s after an overlapping write", task.virtual_path)
            else:
                self._queued_tasks.discard(task.virtual_path)
            logger.debug("Completed %s task for %s", getattr(task, 'operation', 'file'),
                         getattr(task, 'virtual_path', 'unknown'))

    def get_queue_size(self) -> int:
        """Get number of pending sync tasks"""
        return self.task_queue.qsize()

    def get_active_tasks(self) -> list:
        """Get list of currently active sync tasks"""
        return [w.get_active_task() for w in self.workers if w.get_active_task()]

    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for all queued tasks to complete.

        Returns:
            True if all queued work completed before timeout.
        """
        if timeout is None:
            self.task_queue.join()
            return True

        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.task_queue.unfinished_tasks == 0:
                return True
            time.sleep(0.05)

        return self.task_queue.unfinished_tasks == 0

    def get_stats(self) -> dict:
        """Get combined statistics from all workers"""
        stats = {
            'total_synced': 0,
            'total_failed': 0,
            'total_conflicts': 0,
            'queue_size': self.get_queue_size(),
            'active_tasks': len(self.get_active_tasks()),
            'workers': []
        }

        for worker in self.workers:
            worker_stats = worker.get_stats()
            stats['total_synced'] += worker_stats['synced']
            stats['total_failed'] += worker_stats['failed']
            stats['total_conflicts'] += worker_stats['conflicts']
            stats['workers'].append({
                'id': worker.worker_id,
                'stats': worker_stats,
                'active_task': worker.get_active_task()
            })

        return stats
