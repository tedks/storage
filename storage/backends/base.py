"""
Base backend abstraction

Defines the interface all backend implementations must follow
"""

from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Optional, List
import os
from dataclasses import dataclass


class BackendType(Enum):
    """Type of backend filesystem"""
    LOCAL = "local"
    SSH = "ssh"
    RSYNC = "rsync"
    S3 = "s3"


@dataclass
class BackendInfo:
    """Information about a backend filesystem"""
    name: str
    backend_type: BackendType
    is_remote: bool
    speed_priority: int  # Lower is faster (0=fastest local, 100=slowest remote)
    encrypt_all: bool  # Encrypt all files written to this backend
    available: bool  # Is backend currently available/connected


class Backend(ABC):
    """Base class for all backend implementations"""

    def __init__(self, name: str, backend_type: BackendType, is_remote: bool, speed_priority: int):
        self.name = name
        self.backend_type = backend_type
        self.is_remote = is_remote
        self.speed_priority = speed_priority
        self.encrypt_all = False
        self._available = True

    def get_info(self) -> BackendInfo:
        """Get backend information"""
        return BackendInfo(
            name=self.name,
            backend_type=self.backend_type,
            is_remote=self.is_remote,
            speed_priority=self.speed_priority,
            encrypt_all=self.encrypt_all,
            available=self._available
        )

    @abstractmethod
    def exists(self, virtual_path: str) -> bool:
        """Check if a file exists on this backend"""
        pass

    @abstractmethod
    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        """Read data from a file"""
        pass

    @abstractmethod
    def write_file(self, virtual_path: str, data: bytes, offset: int):
        """Write data to a file"""
        pass

    @abstractmethod
    def create_file(self, virtual_path: str):
        """Create a new empty file"""
        pass

    @abstractmethod
    def delete_file(self, virtual_path: str):
        """Delete a file"""
        pass

    @abstractmethod
    def truncate_file(self, virtual_path: str):
        """Truncate a file to zero length (preserve config)"""
        pass

    @abstractmethod
    def get_size(self, virtual_path: str) -> int:
        """Get file size"""
        pass

    @abstractmethod
    def mkdir(self, virtual_path: str):
        """Create a directory"""
        pass

    @abstractmethod
    def rmdir(self, virtual_path: str):
        """Remove a directory"""
        pass

    @abstractmethod
    def listdir(self, virtual_path: str) -> List[str]:
        """List directory contents"""
        pass

    def listdir_attrs(self, virtual_path: str):
        """
        List directory contents with stat-like attributes in one call.

        Returns an iterable of (name, attr_dict) where attr_dict contains
        st_mode, st_size, st_mtime, st_gid, st_uid, st_nlink.

        Returns None to fall back to listdir + per-entry stat calls.
        Backends should override this to avoid N+1 round trips.
        """
        return None

    @abstractmethod
    def isdir(self, virtual_path: str) -> bool:
        """Check if path is a directory"""
        pass

    @abstractmethod
    def copy_to_local(self, virtual_path: str, local_path: Path):
        """Copy a file from backend to local path (for caching)"""
        pass

    @abstractmethod
    def copy_from_local(self, local_path: Path, virtual_path: str):
        """Copy a local file to backend (for syncing)"""
        pass

    @abstractmethod
    def rename(self, old_virtual_path: str, new_virtual_path: str):
        """Rename or move a path on this backend."""
        pass

    @abstractmethod
    def get_free_space(self) -> Optional[int]:
        """Get free space in bytes, None if unknown"""
        pass

    def stat(self, virtual_path: str) -> os.stat_result:
        """Get full stat info for a path"""
        raise NotImplementedError

    @abstractmethod
    def sync(self):
        """Sync any buffered writes to storage"""
        pass

    def get_dir_mtime(self, virtual_path: str) -> Optional[float]:
        """
        Return directory modification time if available.

        Used for cache validation - if remote mtime is newer than cached,
        a refresh is needed. Default implementation uses stat(); backends
        may override for efficiency.
        """
        try:
            st = self.stat(virtual_path)
            return getattr(st, 'st_mtime', None)
        except Exception:
            return None

    def is_available(self) -> bool:
        """Check if backend is currently available"""
        return self._available

    def set_available(self, available: bool):
        """Set backend availability status"""
        self._available = available

    def supports_range_reads(self) -> bool:
        """Return whether this backend supports offset+size reads."""
        return False

    # Optional capabilities -------------------------------------------------
    def get_fast_checksum(self, virtual_path: str) -> str:
        """
        Compute a checksum on the backend side without routing through the cache.
        Implementations should avoid downloading the full file to the caller.
        """
        raise NotImplementedError

    def set_xattr(self, virtual_path: str, name: str, value: bytes):
        """Set an extended attribute on the backend file (best-effort)."""
        raise NotImplementedError

    def get_xattr(self, virtual_path: str, name: str):
        """Get an extended attribute from the backend file (best-effort)."""
        raise NotImplementedError
