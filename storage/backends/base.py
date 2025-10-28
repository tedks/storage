"""
Base backend abstraction

Defines the interface all backend implementations must follow
"""

from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Optional, List
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
    def get_free_space(self) -> Optional[int]:
        """Get free space in bytes, None if unknown"""
        pass

    @abstractmethod
    def sync(self):
        """Sync any buffered writes to storage"""
        pass

    def is_available(self) -> bool:
        """Check if backend is currently available"""
        return self._available

    def set_available(self, available: bool):
        """Set backend availability status"""
        self._available = available
