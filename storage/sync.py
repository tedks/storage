"""
Background sync workers for Storage

Handles asynchronous synchronization of files from cache to backing filesystems.
Monitors dirty files and syncs them according to their replication configuration.
"""

import time
import threading
import queue
import logging
from pathlib import Path
from typing import Dict, Optional, Set
from dataclasses import dataclass, field

from .backends.base import Backend
from .state import StateManager, SyncStatus, FileMetadata, FileLocation
from .replication import ReplicationManager
from .cache import CacheManager
from .conflicts import ConflictDetector

logger = logging.getLogger(__name__)


@dataclass
class SyncConfig:
    """Configuration for sync behavior"""
    monitor_interval: float = 5.0  # Seconds between dirty file checks
    max_retries: int = 3  # Maximum retry attempts per task
    retry_delay: float = 1.0  # Delay between retries
    high_priority_threshold: int = 100  # Priority for immediate syncs


@dataclass
class SyncTask:
    """Represents a file sync task"""
    virtual_path: str
    priority: int = 0  # Higher priority = sync sooner
    retry_count: int = 0
    queued_time: float = field(default_factory=time.time)

    def __lt__(self, other):
        # For priority queue - higher priority first
        return self.priority > other.priority

    def age(self) -> float:
        """Get age of task in seconds"""
        return time.time() - self.queued_time


class SyncWorker(threading.Thread):
    """
    Worker thread that syncs files from cache to backends

    Processes sync tasks from a queue and updates file state.
    """

    def __init__(self, worker_id: int, task_queue: queue.PriorityQueue,
                 state_manager: StateManager, cache_manager: CacheManager,
                 replication_manager: ReplicationManager, config: SyncConfig):
        super().__init__(daemon=True, name=f"SyncWorker-{worker_id}")
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.state_manager = state_manager
        self.cache_manager = cache_manager
        self.replication_manager = replication_manager
        self.config = config
        self.conflict_detector = ConflictDetector(state_manager)
        self._stop_event = threading.Event()
        self._active_task: Optional[str] = None
        self._stats = {
            'synced': 0,
            'failed': 0,
            'conflicts': 0
        }

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

                self._active_task = task.virtual_path

                try:
                    self._process_task(task)
                except Exception as e:
                    logger.error(f"Error processing sync task {task.virtual_path}: {e}")
                    self._handle_task_failure(task, str(e))
                finally:
                    self._active_task = None
                    self.task_queue.task_done()

            except Exception as e:
                logger.error(f"Worker {self.worker_id} error: {e}")

        logger.info(f"Sync worker {self.worker_id} stopped")

    def _process_task(self, task: SyncTask):
        """Process a single sync task"""
        logger.debug(f"Worker {self.worker_id} syncing {task.virtual_path}")

        # Get file metadata
        metadata = self.state_manager.get_file_metadata(task.virtual_path)
        if not metadata:
            logger.warning(f"No metadata for {task.virtual_path}, skipping")
            return

        # Skip if already clean
        if metadata.sync_status == SyncStatus.CLEAN:
            logger.debug(f"{task.virtual_path} already clean, skipping")
            return

        # Get cache path
        cache_path = self.cache_manager.get_cache_path(task.virtual_path)
        if not cache_path.exists():
            logger.warning(f"Cache file missing for {task.virtual_path}")
            return

        # Mark as syncing
        metadata.sync_status = SyncStatus.SYNCING
        self.state_manager.set_file_metadata(metadata)

        # Get configuration
        config = self.state_manager.get_inherited_config(cache_path)

        # Select target backends
        target_backends = self.replication_manager.select_backends_for_file(
            task.virtual_path, config
        )

        if not target_backends:
            logger.warning(f"No backends available for {task.virtual_path}")
            metadata.sync_status = SyncStatus.ERROR
            self.state_manager.set_file_metadata(metadata)
            return

        # Compute checksum
        checksum = self.state_manager.compute_checksum(cache_path)
        metadata.checksum = checksum

        # Sync to each backend
        sync_results = {}
        for backend in target_backends:
            try:
                # Check for conflicts before syncing
                conflict = self.conflict_detector.check_conflict(
                    task.virtual_path, backend, checksum
                )
                if conflict:
                    logger.warning(f"Conflict detected for {task.virtual_path} on {backend.name}")
                    metadata.sync_status = SyncStatus.CONFLICT
                    self.state_manager.set_file_metadata(metadata)
                    self._stats['conflicts'] += 1
                    return

                # Copy to backend
                backend.copy_from_local(cache_path, task.virtual_path)
                backend.sync()

                # Record location
                location = FileLocation(
                    virtual_path=task.virtual_path,
                    backend_name=backend.name,
                    checksum=checksum,
                    last_synced=time.time(),
                    is_cached=False
                )
                self.state_manager.add_file_location(location)

                sync_results[backend.name] = True
                logger.debug(f"Synced {task.virtual_path} to {backend.name}")

            except Exception as e:
                logger.error(f"Failed to sync {task.virtual_path} to {backend.name}: {e}")
                sync_results[backend.name] = False

        # Update status based on results
        successful_syncs = sum(1 for success in sync_results.values() if success)

        if successful_syncs >= config.replica_count or (config.replica_count == -1 and successful_syncs == len(target_backends)):
            # Successfully synced to required replicas
            metadata.sync_status = SyncStatus.CLEAN
            metadata.last_modified = time.time()
            self._stats['synced'] += 1
            logger.info(f"Successfully synced {task.virtual_path} to {successful_syncs} backends")
        elif successful_syncs > 0:
            # Partial success - keep as dirty to retry later
            metadata.sync_status = SyncStatus.DIRTY
            logger.warning(f"Partial sync for {task.virtual_path}: {successful_syncs}/{len(target_backends)}")
        else:
            # Complete failure
            metadata.sync_status = SyncStatus.ERROR
            self._stats['failed'] += 1
            logger.error(f"Failed to sync {task.virtual_path} to any backend")

        self.state_manager.set_file_metadata(metadata)


    def _handle_task_failure(self, task: SyncTask, error: str):
        """Handle failed sync task"""
        task.retry_count += 1

        if task.retry_count < self.config.max_retries:
            # Re-queue with lower priority and delay
            task.priority -= 10
            logger.info(f"Re-queueing {task.virtual_path} (retry {task.retry_count}/{self.config.max_retries})")
            time.sleep(self.config.retry_delay * task.retry_count)  # Exponential backoff
            self.task_queue.put(task)
        else:
            logger.error(f"Max retries exceeded for {task.virtual_path}: {error}")
            self._stats['failed'] += 1
            # Mark as error
            metadata = self.state_manager.get_file_metadata(task.virtual_path)
            if metadata:
                metadata.sync_status = SyncStatus.ERROR
                self.state_manager.set_file_metadata(metadata)

    def stop(self):
        """Stop the worker thread"""
        self._stop_event.set()

    def get_active_task(self) -> Optional[str]:
        """Get currently active task"""
        return self._active_task

    def get_stats(self) -> dict:
        """Get worker statistics"""
        return self._stats.copy()


class SyncManager:
    """
    Manages background sync workers

    Coordinates multiple worker threads and queues sync tasks for dirty files.
    """

    def __init__(self, state_manager: StateManager, cache_manager: CacheManager,
                 replication_manager: ReplicationManager, num_workers: int = 2,
                 config: Optional[SyncConfig] = None):
        """
        Initialize sync manager

        Args:
            state_manager: State manager instance
            cache_manager: Cache manager instance
            replication_manager: Replication manager instance
            num_workers: Number of sync worker threads
            config: Sync configuration (uses defaults if None)
        """
        self.state_manager = state_manager
        self.cache_manager = cache_manager
        self.replication_manager = replication_manager
        self.num_workers = num_workers
        self.config = config or SyncConfig()

        # Task queue (priority queue)
        self.task_queue = queue.PriorityQueue()

        # Workers
        self.workers = []
        self._running = False
        self._monitor_thread = None
        self._queued_tasks: Set[str] = set()
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
                self.config
            )
            worker.start()
            self.workers.append(worker)

        # Start monitor thread
        self._running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

        logger.info("Sync manager started")

    def stop(self):
        """Stop sync workers"""
        if not self._running:
            return

        logger.info("Stopping sync manager")
        self._running = False

        # Stop workers
        for worker in self.workers:
            worker.stop()

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
                    self.queue_sync(metadata.virtual_path, priority=50)

                # Sleep before next check
                time.sleep(self.config.monitor_interval)

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

    def get_queue_size(self) -> int:
        """Get number of pending sync tasks"""
        return self.task_queue.qsize()

    def get_active_tasks(self) -> list:
        """Get list of currently active sync tasks"""
        return [w.get_active_task() for w in self.workers if w.get_active_task()]

    def wait_for_completion(self, timeout: Optional[float] = None):
        """Wait for all queued tasks to complete"""
        if timeout:
            self.task_queue.join()
        else:
            start = time.time()
            while not self.task_queue.empty():
                if timeout and (time.time() - start) > timeout:
                    break
                time.sleep(0.1)

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
