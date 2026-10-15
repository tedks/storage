"""Data models for storage state and placement."""

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Dict, Optional


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
