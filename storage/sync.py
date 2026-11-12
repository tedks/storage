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
from typing import Callable, ContextManager, Optional, Set, Union, Tuple

from .backends.base import Backend, BackendType
from .cache import CacheManager
from .models import SyncStatus
from .replication import ReplicationManager
from .state import StateManager
from .sync_executor import SyncExecutor
from .sync_tasks import DirOperation, DirSyncTask, SyncConfig, SyncTask, dir_task_key

logger = logging.getLogger(__name__)


def sync_worker_ceiling(backends) -> int:
    """Conservative adaptive cap; rsync uploads share one SSH master per host."""
    remote = (BackendType.SSH, BackendType.RSYNC)
    return 8 if any(b.backend_type in remote for b in backends.values()) else 32

# Posted to the task queue by SyncManager.stop() so idle workers wake at once
# instead of at the end of their poll. Sorts ahead of every real task (the
# highest real priority is 1000). Never processed; compared by identity.
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
                 member_written_cb: Optional[Callable[[str, Backend, Path, Optional[str]], None]] = None,
                 upload_guard: Optional[Callable[[str], ContextManager]] = None,
                 path_guard: Optional[Callable[..., ContextManager]] = None,
                 dir_retry_guard: Optional[Callable[..., ContextManager]] = None,
                 idle_timeout: float = 30.0,
                 retire_cb: Optional[Callable[['SyncWorker'], bool]] = None,
                 demand_changed_cb: Optional[Callable[[], None]] = None):
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
            upload_guard=upload_guard,
            path_guard=path_guard,
            dir_retry_guard=dir_retry_guard,
        )
        self._stop_event = threading.Event()
        self._active_task: Optional[str] = None
        self._task_complete_cb = task_complete_cb
        self._idle_timeout = idle_timeout
        self._retire_cb = retire_cb
        self._demand_changed_cb = demand_changed_cb

    def run(self):
        """Main worker loop"""
        logger.info(f"Sync worker {self.worker_id} started")

        idle_since = time.monotonic()
        while not self._stop_event.is_set():
            try:
                # Get task with timeout to check stop event
                try:
                    task = self.task_queue.get(timeout=min(1.0, self._idle_timeout))
                except queue.Empty:
                    if (self._retire_cb and time.monotonic() - idle_since >= self._idle_timeout
                            and self._retire_cb(self)):
                        break
                    continue

                if task is _WAKE:
                    # stop() posted this to end the idle wait; nothing to run.
                    self.task_queue.task_done()
                    continue

                if self._stop_event.is_set():
                    # Leave queued work for a later start, including a task
                    # fetched concurrently with stop().
                    self.task_queue.put(task)
                    self.task_queue.task_done()
                    break

                self._active_task = task.virtual_path
                # A dequeued task is absent from qsize() until marked
                # active. Recheck demand now to close that gap. A failure to
                # add a thread must not strand this task before processing.
                self._notify_demand_change()

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
                    idle_since = time.monotonic()
                    self._notify_demand_change()

            except Exception as e:
                logger.error(f"Worker {self.worker_id} error: {e}")

        logger.info(f"Sync worker {self.worker_id} stopped")

    def _notify_demand_change(self):
        if self._demand_changed_cb:
            try:
                self._demand_changed_cb()
            except Exception:
                logger.exception("Could not adjust sync worker count")

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
                 member_written_cb: Optional[Callable[[str, Backend, Path, Optional[str]], None]] = None,
                 upload_guard: Optional[Callable[[str], ContextManager]] = None,
                 path_guard: Optional[Callable[..., ContextManager]] = None,
                 dir_retry_guard: Optional[Callable[..., ContextManager]] = None,
                 max_workers: Optional[int] = None, idle_timeout: float = 30.0):
        """
        Initialize sync manager

        Args:
            state_manager: State manager instance
            cache_manager: Cache manager instance
            replication_manager: Replication manager instance
            num_workers: Number of sync worker threads
            max_workers: Optional upper limit; None keeps the fixed pool
            idle_timeout: Seconds of idleness before excess workers retire
            config: Sync configuration (uses defaults if None)
            should_sync_path_cb: Whether a path may sync now (no open writer)
            member_written_cb: Called (virtual_path, member, cache_path,
                checksum) after each upload to a member; checksum is the
                hash of what was uploaded, or None when a write to the
                cache file may have overlapped the upload
            upload_guard: Context manager factory held across each file
                task (StorageFS's upload lock for the path)
            path_guard: Context manager factory over paths, held across
                each directory retry (StorageFS's path locks)
            dir_retry_guard: FS-owned context yielding whether a directory
                retry may mutate a member, held through the member call
        """
        self.state_manager = state_manager
        self.cache_manager = cache_manager
        self.replication_manager = replication_manager
        if num_workers < 0 or max_workers is not None and max_workers < num_workers:
            raise ValueError("sync worker limits must satisfy 0 <= workers <= max_workers")
        if max_workers is not None:
            ceiling = sync_worker_ceiling(replication_manager.backends)
            if max_workers > ceiling:
                raise ValueError(f"max sync workers cannot exceed {ceiling}")
        if idle_timeout <= 0:
            raise ValueError("sync worker idle timeout must be positive")
        self.num_workers = num_workers
        self.max_workers = num_workers if max_workers is None else max_workers
        self.idle_timeout = idle_timeout
        self.config = config or SyncConfig()
        self._should_sync_path_cb = should_sync_path_cb
        self._member_written_cb = member_written_cb
        self._upload_guard = upload_guard
        self._path_guard = path_guard
        self._dir_retry_guard = dir_retry_guard

        # Task queue (priority queue)
        self.task_queue = queue.PriorityQueue()

        # Workers
        self.workers = []
        self._lifecycle_lock = threading.RLock()
        self._workers_lock = threading.Lock()
        self._next_worker_id = 0
        self._retired_stats = {'synced': 0, 'failed': 0, 'conflicts': 0}
        self._lingering_workers = []
        self._running = False
        # Set by stop() to end the monitor's sleep between scans. Each start()
        # makes a fresh one, so a monitor that outlived stop()'s join can never
        # be revived by a later start().
        self._monitor_stop = threading.Event()
        self._monitor_thread = None
        self._queued_tasks: Set[str] = set()
        self._queued_dir_tasks: Set[Tuple[str, str, Optional[str]]] = set()
        self._queue_lock = threading.Lock()

    def start(self):
        """Start sync workers"""
        with self._lifecycle_lock:
            try:
                with self._workers_lock:
                    if self._running:
                        logger.warning("Sync manager already running")
                        return
                    self._lingering_workers = [w for w in self._lingering_workers if w.is_alive()]
                    if self._lingering_workers:
                        raise RuntimeError("previous sync workers are still finishing; cannot restart")
                    logger.info("Starting sync manager with %d-%d workers", self.num_workers, self.max_workers)
                    self._running = True
                    self._monitor_stop = threading.Event()
                    for _ in range(self.num_workers):
                        self._start_worker_locked()
                    self._monitor_thread = threading.Thread(
                        target=self._monitor_loop, args=(self._monitor_stop,), daemon=True)
                    self._monitor_thread.start()
            except Exception:
                if self._running:
                    self.stop()
                raise

            self._maybe_grow()

        logger.info("Sync manager started")

    def _start_worker_locked(self):
        worker = SyncWorker(
            self._next_worker_id, self.task_queue, self.state_manager,
            self.cache_manager, self.replication_manager,
            self.config, self._task_complete,
            should_sync_path_cb=self._should_sync_path_cb,
            member_written_cb=self._member_written_cb,
            upload_guard=self._upload_guard,
            path_guard=self._path_guard,
            dir_retry_guard=self._dir_retry_guard,
            idle_timeout=self.idle_timeout,
            retire_cb=self._retire_idle,
            demand_changed_cb=self._maybe_grow,
        )
        self._next_worker_id += 1
        worker.start()
        self.workers.append(worker)

    def _maybe_grow(self):
        with self._workers_lock:
            if not self._running:
                return
            active = sum(worker.get_active_task() is not None for worker in self.workers)
            target = min(self.max_workers, max(self.num_workers, active + self.task_queue.qsize()))
            for _ in range(target - len(self.workers)):
                try:
                    self._start_worker_locked()
                except RuntimeError:
                    logger.exception("Could not start another sync worker")
                    break

    def _retire_idle(self, worker: SyncWorker) -> bool:
        with self._workers_lock:
            if (not self._running or len(self.workers) <= self.num_workers
                    or self.task_queue.qsize() or worker not in self.workers):
                return False
            worker_stats = worker.get_stats()
            for key in self._retired_stats:
                self._retired_stats[key] += worker_stats.get(key, 0)
            self.workers.remove(worker)
            return True

    def stop(self):
        """Stop sync workers"""
        with self._lifecycle_lock:
            with self._workers_lock:
                if not self._running:
                    return

                logger.info("Stopping sync manager")
                self._running = False
                self._monitor_stop.set()
                workers = list(self.workers)

            # Stop every worker before waking any, so a woken worker cannot
            # take a sibling's wake before its own stop flag is set.
            for worker in workers:
                worker.stop()
            for _ in workers:
                self.task_queue.put(_WAKE)

            deadline = time.monotonic() + 5.0
            for worker in workers:
                worker.join(timeout=max(0.0, deadline - time.monotonic()))
            if self._monitor_thread and self._monitor_thread.ident is not None:
                self._monitor_thread.join(timeout=max(0.0, deadline - time.monotonic()))

            with self._workers_lock:
                self._lingering_workers = [w for w in workers if w.is_alive()]
                self.workers.clear()
                self._retired_stats = {'synced': 0, 'failed': 0, 'conflicts': 0}
            self._drop_wake_tasks()
            if self._lingering_workers:
                logger.warning("%d sync worker(s) still finishing after stop timeout",
                               len(self._lingering_workers))
            logger.info("Sync manager stopped")

    def _drop_wake_tasks(self):
        """Remove wake tasks no worker took (it was busy or exited first).

        They would otherwise stay in qsize() and unfinished_tasks, so a
        stopped manager would look busy and wait_for_completion would never
        see an idle queue. Real tasks are put back.
        """
        drained = []
        while True:
            try:
                drained.append(self.task_queue.get_nowait())
            except queue.Empty:
                break
        # Re-put real tasks before releasing the drained items' unfinished
        # counts, so the count never reaches zero while real work is pending.
        for task in drained:
            if task is not _WAKE:
                self.task_queue.put(task)
        for _ in drained:
            self.task_queue.task_done()

    def _monitor_loop(self, stop: threading.Event):
        """Monitor for dirty files and queue them for sync"""
        logger.info("Sync monitor started")

        while not stop.is_set():
            try:
                # Get dirty files
                dirty_files = self.state_manager.get_dirty_files()

                # Queue new tasks
                for metadata in dirty_files:
                    if self._should_sync_path_cb and not self._should_sync_path_cb(metadata.virtual_path):
                        continue
                    self.queue_sync(metadata.virtual_path, priority=50)

                # Sleep before next check
                stop.wait(self.config.monitor_interval)

            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                stop.wait(1.0)

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
        self._maybe_grow()

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
        self._maybe_grow()

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
        self._maybe_grow()

    def get_queue_size(self) -> int:
        """Get number of pending sync tasks"""
        return self.task_queue.qsize()

    def get_active_tasks(self) -> list:
        """Get list of currently active sync tasks"""
        with self._workers_lock:
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

        with self._workers_lock:
            workers = list(self.workers)
            retired = dict(self._retired_stats)
        for key, value in retired.items():
            stats['total_' + key] += value
        for worker in workers:
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
