"""Data models for storage state and placement."""

from dataclasses import asdict, dataclass
from enum import Enum
from fractions import Fraction
from typing import Dict, Optional, Tuple


class SyncStatus(Enum):
    """Status of file synchronization."""

    CLEAN = "clean"  # File is synced to all required members.
    DIRTY = "dirty"  # File has unsynced changes.
    SYNCING = "syncing"  # File is currently being synced.
    ERROR = "error"  # Sync error occurred.
    CONFLICT = "conflict"  # Conflict detected.


@dataclass
class FileMetadata:
    """Metadata for a file in the storage filesystem."""

    virtual_path: str
    size: int
    checksum: Optional[str]
    last_access: float
    last_modified: float
    sync_status: SyncStatus
    replica_count: int
    recomputable: bool
    recompute_cmd: Optional[str]
    encrypted: bool
    encryption_key_id: Optional[str]
    # Bumped by every local modification (StateManager.mark_dirty). Sync-side
    # status changes are conditioned on it, so a write that overlaps an
    # in-flight sync cannot be reported as synced.
    dirty_generation: int = 0

    def to_dict(self) -> Dict:
        """Convert to a SQLite-friendly dictionary."""
        payload = asdict(self)
        payload["sync_status"] = self.sync_status.value
        return payload

    @classmethod
    def from_dict(cls, data: Dict) -> "FileMetadata":
        """Build metadata from stored row data."""
        payload = dict(data)
        payload["sync_status"] = SyncStatus(payload["sync_status"])
        return cls(**payload)


@dataclass
class FileLocation:
    """Tracks where a file currently exists."""

    virtual_path: str
    backend_name: str
    checksum: str
    last_synced: float
    is_cached: bool


@dataclass(frozen=True)
class StreamingKeepAfterClose:
    """When read-ahead outlives the last close of a streaming session.

    Read-ahead continues once the longest contiguous span the readers asked
    for reaches any of the thresholds: a byte count, or a percentage of the
    file rounded down. 0 (bytes or percent) means any read qualifies.
    """

    byte_thresholds: Tuple[int, ...] = ()
    percent_thresholds: Tuple[Fraction, ...] = ()

    def __post_init__(self):
        if not self.byte_thresholds and not self.percent_thresholds:
            raise ValueError("at least one threshold is required")
        if any(value < 0 for value in self.byte_thresholds):
            raise ValueError("byte thresholds must not be negative")
        if any(not 0 <= value <= 100 for value in self.percent_thresholds):
            raise ValueError("percent thresholds must be between 0 and 100")

    def threshold(self, file_size: int) -> int:
        """The span in bytes a reader must reach on a file of file_size bytes."""
        return min(
            list(self.byte_thresholds)
            + [int(file_size * percent // 100) for percent in self.percent_thresholds]
        )
