"""Execution policy for background sync workers."""

import logging
import queue
import time
from typing import Callable, Dict, List, Optional, Union

from .backends.base import Backend
from .cache import CacheManager
from .conflicts import ConflictDetector
from .file_config import get_inherited_config
from .models import FileLocation, SyncStatus
from .replication import ReplicationManager
from .state import StateManager
from .sync_tasks import DirOperation, DirSyncTask, SyncConfig, SyncTask

logger = logging.getLogger(__name__)


class SyncExecutor:
    """Owns sync task execution and retry policy for a worker."""

    def __init__(
        self,
        *,
        worker_id: int,
        state_manager: StateManager,
        cache_manager: CacheManager,
        replication_manager: ReplicationManager,
        config: SyncConfig,
        should_sync_path_cb: Optional[Callable[[str], bool]] = None,
    ):
        self.worker_id = worker_id
        self.state_manager = state_manager
        self.cache_manager = cache_manager
        self.replication_manager = replication_manager
        self.config = config
        self.conflict_detector = ConflictDetector(state_manager)
        self.should_sync_path_cb = should_sync_path_cb
        self._stats = {
            "synced": 0,
            "failed": 0,
            "conflicts": 0,
        }

    def process(self, task: Union[SyncTask, DirSyncTask]) -> bool:
        if isinstance(task, DirSyncTask):
            return self.process_dir_task(task)
        return self.process_file_task(task)

    def process_file_task(self, task: SyncTask) -> bool:
        """Process a single file sync task."""
        logger.debug("Worker %s syncing %s", self.worker_id, task.virtual_path)

        metadata = self.state_manager.get_file_metadata(task.virtual_path)
        if not metadata:
            logger.warning("No metadata for %s, skipping", task.virtual_path)
            return True

        if metadata.sync_status == SyncStatus.CLEAN:
            logger.debug("%s already clean, skipping", task.virtual_path)
            return True

        if self.should_sync_path_cb and not self.should_sync_path_cb(task.virtual_path):
            logger.debug("Deferring sync for busy path %s", task.virtual_path)
            return True

        cache_path = self.cache_manager.get_cache_path(task.virtual_path)
        if not cache_path.exists():
            logger.warning("Cache file missing for %s", task.virtual_path)
            return True

        metadata.sync_status = SyncStatus.SYNCING
        self.state_manager.set_file_metadata(metadata)

        config = get_inherited_config(cache_path)
        target_backends = self.replication_manager.select_backends_for_file(
            task.virtual_path,
            config,
        )

        if not target_backends:
            logger.warning("No backends available for %s", task.virtual_path)
            metadata.sync_status = SyncStatus.ERROR
            self.state_manager.set_file_metadata(metadata)
            self._stats["failed"] += 1
            return True

        checksum = self.state_manager.compute_checksum(cache_path)
        metadata.checksum = checksum

        sync_results: Dict[str, bool] = {}
        for backend in target_backends:
            try:
                conflict = self.conflict_detector.check_conflict(
                    task.virtual_path,
                    backend,
                    checksum,
                )
                if conflict:
                    logger.warning(
                        "Conflict detected for %s on %s",
                        task.virtual_path,
                        backend.name,
                    )
                    metadata.sync_status = SyncStatus.CONFLICT
                    self.state_manager.set_file_metadata(metadata)
                    self._stats["conflicts"] += 1
                    return True

                backend.copy_from_local(cache_path, task.virtual_path)
                backend.sync()

                location = FileLocation(
                    virtual_path=task.virtual_path,
                    backend_name=backend.name,
                    checksum=checksum,
                    last_synced=time.time(),
                    is_cached=False,
                )
                self.state_manager.add_file_location(location)

                sync_results[backend.name] = True
                logger.debug("Synced %s to %s", task.virtual_path, backend.name)
            except Exception as exc:
                logger.error(
                    "Failed to sync %s to %s: %s",
                    task.virtual_path,
                    backend.name,
                    exc,
                )
                sync_results[backend.name] = False

        successful_syncs = sum(1 for success in sync_results.values() if success)

        if successful_syncs >= config.replica_count or (
            config.replica_count == -1 and successful_syncs == len(target_backends)
        ):
            metadata.sync_status = SyncStatus.CLEAN
            metadata.last_modified = time.time()
            self._stats["synced"] += 1
            logger.info(
                "Successfully synced %s to %s backends",
                task.virtual_path,
                successful_syncs,
            )
        elif successful_syncs > 0:
            metadata.sync_status = SyncStatus.DIRTY
            logger.warning(
                "Partial sync for %s: %s/%s",
                task.virtual_path,
                successful_syncs,
                len(target_backends),
            )
        else:
            metadata.sync_status = SyncStatus.ERROR
            self._stats["failed"] += 1
            logger.error("Failed to sync %s to any backend", task.virtual_path)

        self.state_manager.set_file_metadata(metadata)
        return True

    def handle_task_failure(
        self,
        task: Union[SyncTask, DirSyncTask],
        error: str,
        task_queue: queue.PriorityQueue,
    ) -> bool:
        """Handle failed sync task execution."""
        task.retry_count += 1

        if task.retry_count < self.config.max_retries:
            task.priority -= 10
            logger.info(
                "Re-queueing %s (retry %s/%s)",
                task.virtual_path,
                task.retry_count,
                self.config.max_retries,
            )
            time.sleep(self.config.retry_delay * task.retry_count)
            task_queue.put(task)
            return False

        logger.error("Max retries exceeded for %s: %s", task.virtual_path, error)
        self._stats["failed"] += 1
        if isinstance(task, SyncTask):
            metadata = self.state_manager.get_file_metadata(task.virtual_path)
            if metadata:
                metadata.sync_status = SyncStatus.ERROR
                self.state_manager.set_file_metadata(metadata)
        return True

    def process_dir_task(self, task: DirSyncTask) -> bool:
        """Process a single directory operation."""
        logger.debug(
            "Worker %s processing dir op %s on %s",
            self.worker_id,
            task.operation.value,
            task.virtual_path,
        )

        backends = self._select_dir_backends(task.backend_hint)
        if not backends:
            raise RuntimeError(
                f"No backends available for dir operation {task.operation.value}"
            )

        success = True
        for backend in backends:
            if not backend:
                continue
            try:
                if task.operation == DirOperation.MKDIR:
                    backend.mkdir(task.virtual_path)
                elif task.operation == DirOperation.RMDIR:
                    backend.rmdir(task.virtual_path)
                elif task.operation == DirOperation.RENAME:
                    if not task.new_path:
                        raise ValueError("Rename dir task missing new_path")
                    backend.rename(task.virtual_path, task.new_path)
                logger.debug(
                    "Dir op %s succeeded for %s on %s",
                    task.operation.value,
                    task.virtual_path,
                    backend.name,
                )
            except Exception as exc:
                success = False
                logger.warning(
                    "Dir op %s failed for %s on %s: %s",
                    task.operation.value,
                    task.virtual_path,
                    backend.name,
                    exc,
                )

        if not success:
            raise RuntimeError(
                f"Dir op {task.operation.value} failed for {task.virtual_path}"
            )

        self._stats["synced"] += 1
        return True

    def get_stats(self) -> dict:
        return self._stats.copy()

    def _select_dir_backends(self, backend_hint: Optional[str]) -> List[Backend]:
        if backend_hint:
            backend = self.replication_manager.backends.get(backend_hint)
            return [backend] if backend else []
        return self.replication_manager.get_available_backends()
