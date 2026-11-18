"""Task and configuration models for background sync."""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Tuple


@dataclass
class SyncConfig:
    """Configuration for sync behavior."""

    monitor_interval: float = 5.0
    max_retries: int = 3
    retry_delay: float = 1.0
    high_priority_threshold: int = 100


@dataclass
class SyncTask:
    """Represents a file sync task."""

    virtual_path: str
    priority: int = 0
    retry_count: int = 0
    queued_time: float = field(default_factory=time.time)
    # The dirty_generation the current attempt read before claiming the row.
    # Every status it publishes, including the ERROR after the last retry, is
    # conditioned on it. None until an attempt has read the row.
    generation: Optional[int] = None
    # Set when a publish was refused because the file was written during the
    # attempt. The manager queues another pass when it completes this task.
    requeue: bool = False
    # Whether the failed last attempt's ERROR was published, set by the
    # attempt itself under its upload guard; None until then.
    final_error: Optional[bool] = None

    def __lt__(self, other):
        return self.priority > other.priority

    def age(self) -> float:
        return time.time() - self.queued_time


class DirOperation(Enum):
    """Directory-level operations that need syncing."""

    MKDIR = "mkdir"
    RMDIR = "rmdir"
    RENAME = "rename"


@dataclass
class DirSyncTask:
    """Represents a directory sync task."""

    virtual_path: str
    operation: DirOperation
    new_path: Optional[str] = None
    mode: int = 0o755
    backend_hint: Optional[str] = None
    priority: int = 0
    retry_count: int = 0
    queued_time: float = field(default_factory=time.time)

    def __lt__(self, other):
        return self.priority > other.priority

    def age(self) -> float:
        return time.time() - self.queued_time


def dir_task_key(
    operation: DirOperation,
    virtual_path: str,
    new_path: Optional[str],
    backend_hint: Optional[str],
) -> Tuple[str, str, str, str]:
    """Build the stable dedupe key for a queued directory operation."""

    return (operation.value, virtual_path, new_path or "", backend_hint or "*")


def record_member_rename_repair(
    state_manager,
    backend_name: str,
    old_path: str,
    new_path: str,
    reason: str,
    logger: logging.Logger,
) -> None:
    """Record a rename backend_name missed, for repair; never replay it.

    The member may hold the moved object at old_path still, or the move may
    have taken effect there before its failure was seen. A replay could move
    a new object at old_path or replace one at new_path, so the record is
    the whole response. A record that cannot be written is logged: the
    member is left as it is either way.
    """
    try:
        repair_id = state_manager.record_member_rename_repair(
            backend_name, old_path, new_path, reason
        )
    except Exception:
        logger.exception(
            "Could not record the rename %s -> %s that %s missed; the member "
            "is left as it is",
            old_path, new_path, backend_name,
        )
        return
    logger.warning(
        "Member %s missed the rename %s -> %s and it is not replayed; repair %d "
        "(storage debug member-repairs): %s",
        backend_name, old_path, new_path, repair_id, reason,
    )
