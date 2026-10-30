"""Execution policy for background sync workers."""

import contextlib
import logging
import os
import queue
import time
from pathlib import Path
from typing import Callable, ContextManager, Dict, List, Optional, Union

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
        member_written_cb: Optional[Callable[[str, Backend, Path], None]] = None,
        upload_guard: Optional[Callable[[str], ContextManager]] = None,
        path_guard: Optional[Callable[..., ContextManager]] = None,
    ):
        self.worker_id = worker_id
        self.state_manager = state_manager
        self.cache_manager = cache_manager
        self.replication_manager = replication_manager
        self.config = config
        self.conflict_detector = ConflictDetector(state_manager)
        self.should_sync_path_cb = should_sync_path_cb
        # Told (virtual_path, member, cache_path) after each upload, so the
        # filesystem can record the member's new stat (StorageFS revalidation).
        self.member_written_cb = member_written_cb
        # Held across a file task, from reading its row to publishing the
        # outcome: a rename or unlink of the path waits for it, so the
        # member never gets the bytes under a name the namespace has just
        # given up (StorageFS: UploadLocks.hold_for_upload).
        self.upload_guard = upload_guard
        # Held across a directory retry (StorageFS: the namespace lock shared
        # and the paths' locks), so the check that the namespace still wants
        # the operation and the member call are one step against handlers.
        self.path_guard = path_guard
        self._stats = {
            "synced": 0,
            "failed": 0,
            "conflicts": 0,
        }

    def process(self, task: Union[SyncTask, DirSyncTask]) -> bool:
        if isinstance(task, DirSyncTask):
            return self.process_dir_task(task)
        return self.process_file_task(task)

    def _upload_guard(self, virtual_path: str) -> ContextManager:
        return self.upload_guard(virtual_path) if self.upload_guard else contextlib.nullcontext()

    def process_file_task(self, task: SyncTask) -> bool:
        """Process a single file sync task under the path's upload guard.

        A failed attempt is settled before the guard is released: once it
        is, a rename could move the row (as SYNCING, which nothing would
        queue again) or put another file with the same generation count at
        the path, and handle_task_failure runs later without the guard.
        """
        with self._upload_guard(task.virtual_path):
            try:
                return self._process_file_task(task)
            except Exception:
                self._settle_failed_attempt(task)
                raise

    def _settle_failed_attempt(self, task: SyncTask):
        """Give a failed attempt's row back: DIRTY for a retry, ERROR after the last.

        Conditioned on the attempt's generation like every sync-side status,
        so a write since then keeps its DIRTY. Caller holds the upload guard.
        """
        if task.generation is None:
            return
        final = task.retry_count + 1 >= self.config.max_retries
        status = SyncStatus.ERROR if final else SyncStatus.DIRTY
        try:
            cache_path = self.cache_manager.get_cache_path(task.virtual_path)
            published = self._publish(task, task.generation, status, cache_path)
        except Exception:
            logger.exception("Could not record the failed sync of %s", task.virtual_path)
            return
        if final:
            task.final_error = published

    def _process_file_task(self, task: SyncTask) -> bool:
        logger.debug("Worker %s syncing %s", self.worker_id, task.virtual_path)
        # A retry's generation is its own: one left from the previous
        # attempt, read under a guard since released, must not be settled.
        task.generation = None

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

        # Every status change below is conditioned on this generation. A
        # write() that lands while the copy is in flight bumps it, so the
        # change is refused and the file stays DIRTY (#35, #57).
        generation = metadata.dirty_generation
        task.generation = generation
        if not self._publish(task, generation, SyncStatus.SYNCING, cache_path):
            return True

        config = get_inherited_config(cache_path)
        target_backends = self.replication_manager.select_backends_for_file(
            task.virtual_path,
            config,
        )

        if not target_backends:
            logger.warning("No backends available for %s", task.virtual_path)
            if self._publish(task, generation, SyncStatus.ERROR, cache_path):
                self._stats["failed"] += 1
            return True

        checksum = self.state_manager.compute_checksum(cache_path)

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
                    if self._publish(task, generation, SyncStatus.CONFLICT, cache_path):
                        self._stats["conflicts"] += 1
                    return True

                backend.copy_from_local(cache_path, task.virtual_path)
                backend.sync()
                if self.member_written_cb:
                    try:
                        self.member_written_cb(task.virtual_path, backend, cache_path)
                    except Exception:
                        logger.exception("member_written_cb failed for %s", task.virtual_path)

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
            status = SyncStatus.CLEAN
        elif successful_syncs > 0:
            status = SyncStatus.DIRTY
        else:
            status = SyncStatus.ERROR

        # Counted only once the outcome is on record: a refused publish means
        # the content that was copied is already superseded.
        if not self._publish(task, generation, status, cache_path, checksum=checksum):
            return True
        if status == SyncStatus.CLEAN:
            self._stats["synced"] += 1
            logger.info(
                "Successfully synced %s to %s backends",
                task.virtual_path,
                successful_syncs,
            )
        elif status == SyncStatus.DIRTY:
            logger.warning(
                "Partial sync for %s: %s/%s",
                task.virtual_path,
                successful_syncs,
                len(target_backends),
            )
        else:
            self._stats["failed"] += 1
            logger.error("Failed to sync %s to any backend", task.virtual_path)
        return True

    def _publish(
        self,
        task: SyncTask,
        generation: int,
        status: SyncStatus,
        cache_path: Path,
        *,
        checksum: Optional[str] = None,
    ) -> bool:
        """Record a sync-side status unless the file was written since `generation`.

        When the generation has moved, nothing is written: the row already
        carries the newer write's DIRTY, which must stand. The task is flagged
        for requeue so the newer content gets its own pass once the manager
        retires this one, and False is returned so the caller stops treating
        this sync as current.
        """
        # The publish and its mirror are one critical section, so a writer's
        # mirror cannot land between them and then be overwritten by ours.
        with self.state_manager.metadata_lock:
            published = self.state_manager.publish_sync_status(
                task.virtual_path, generation, status, checksum=checksum,
            )
            if published:
                metadata = self.state_manager.get_file_metadata(task.virtual_path)
                if metadata:
                    self.cache_manager.sync_cache_metadata(
                        task.virtual_path, metadata, cache_path=cache_path,
                    )
        if not published:
            logger.info(
                "%s was written during sync; leaving it dirty for another pass",
                task.virtual_path,
            )
            task.requeue = True
            return False
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
            # The retry reads the row afresh; a refused publish of this
            # attempt needs no extra pass after it.
            task.requeue = False
            task_queue.put(task)
            return False

        logger.error("Max retries exceeded for %s: %s", task.virtual_path, error)
        if isinstance(task, DirSyncTask) or task.generation is None:
            # A file task that never read its row (the index itself failed,
            # say) has no generation to publish against; the row stays DIRTY
            # for the monitor. The failure still counts.
            self._stats["failed"] += 1
        elif task.final_error is not None:
            # The attempt published (or was refused) its ERROR under its guard.
            if task.final_error:
                self._stats["failed"] += 1
        else:
            # The failure belongs to the attempt that read task.generation. A
            # write since then owns the row, and its DIRTY must stand; a
            # failure it refuses is not counted, the file is simply dirty.
            with self._upload_guard(task.virtual_path):
                cache_path = self.cache_manager.get_cache_path(task.virtual_path)
                if self._publish(task, task.generation, SyncStatus.ERROR, cache_path):
                    self._stats["failed"] += 1
        return True

    def process_dir_task(self, task: DirSyncTask) -> bool:
        """Retry a directory operation on a member, if the namespace still wants it.

        A retry queued for a member that missed the operation can run after
        the namespace has moved on: a mkdir retry after the directory was
        renamed away would recreate the old name on that member (ditz
        sync-namespace-orphans). The cache holds every directory made, and
        drops every directory removed or renamed, through the mount, so it
        says whether the operation still applies; the check and the member
        call run under the path guard. The guard includes the namespace
        lock, which a directory rename waits for, so a member known to be
        down is not tried under it: the task fails and is retried later.
        """
        down = [backend.name for backend in self._select_dir_backends(task.backend_hint)
                if backend and not backend.is_available()]
        if down:
            raise RuntimeError(
                f"Dir op {task.operation.value} for {task.virtual_path} waits for {', '.join(down)}"
            )
        paths = [task.virtual_path] + ([task.new_path] if task.new_path else [])
        guard = self.path_guard(*paths) if self.path_guard else contextlib.nullcontext()
        with guard:
            if not self._dir_task_current(task):
                logger.info(
                    "Dropping %s retry for %s: the namespace no longer has it",
                    task.operation.value,
                    task.virtual_path,
                )
                return True
            return self._process_dir_task(task)

    def _dir_task_current(self, task: DirSyncTask) -> bool:
        cache_path = self.cache_manager.get_cache_path(task.virtual_path)
        if task.operation == DirOperation.MKDIR:
            return cache_path.is_dir()
        if task.operation == DirOperation.RMDIR:
            return not os.path.lexists(cache_path)
        # RENAME: while the destination exists. The old name may be in use
        # again (a new file or directory there): the member still holds the
        # renamed copy under it, and only the rename moves it. A destination
        # gone since (renamed on, which queues its own retry, or removed)
        # must not be created on the member. What the member holds after a
        # chain of failed operations is not fully settled by single retries
        # (ditz sync-member-reconcile).
        return os.path.lexists(self.cache_manager.get_cache_path(task.new_path))

    def _process_dir_task(self, task: DirSyncTask) -> bool:
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
