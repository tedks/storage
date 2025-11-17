"""
Local filesystem backend implementation
"""

import os
import shutil
from pathlib import Path
from typing import List, Optional

from .base import Backend, BackendType


class LocalBackend(Backend):
    """Backend for local filesystems"""

    def __init__(self, name: str, root_path: str, speed_priority: int = 0):
        """
        Initialize local backend

        Args:
            name: Backend identifier
            root_path: Root directory for this backend
            speed_priority: Speed priority (default 0 for local)
        """
        super().__init__(name, BackendType.LOCAL, is_remote=False, speed_priority=speed_priority)
        self.root_path = Path(root_path).resolve()

        if not self.root_path.exists():
            raise ValueError(f"Root path does not exist: {root_path}")
        if not self.root_path.is_dir():
            raise ValueError(f"Root path is not a directory: {root_path}")

    def _resolve_path(self, virtual_path: str) -> Path:
        """Convert virtual path to real path on this backend"""
        # Remove leading slash for path joining
        relative_path = virtual_path.lstrip('/')
        return self.root_path / relative_path

    def exists(self, virtual_path: str) -> bool:
        """Check if a file exists on this backend"""
        return self._resolve_path(virtual_path).exists()

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        """Read data from a file"""
        real_path = self._resolve_path(virtual_path)
        with open(real_path, 'rb') as f:
            f.seek(offset)
            return f.read(size)

    def write_file(self, virtual_path: str, data: bytes, offset: int):
        """Write data to a file"""
        real_path = self._resolve_path(virtual_path)

        # Ensure parent directory exists
        real_path.parent.mkdir(parents=True, exist_ok=True)

        # Create file if it doesn't exist
        if not real_path.exists():
            real_path.touch()

        with open(real_path, 'r+b') as f:
            f.seek(offset)
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

    def create_file(self, virtual_path: str):
        """Create a new empty file"""
        real_path = self._resolve_path(virtual_path)
        real_path.parent.mkdir(parents=True, exist_ok=True)
        real_path.touch()

    def delete_file(self, virtual_path: str):
        """Delete a file"""
        real_path = self._resolve_path(virtual_path)
        if real_path.exists():
            real_path.unlink()

    def truncate_file(self, virtual_path: str):
        """Truncate a file to zero length (preserve extended attributes)"""
        real_path = self._resolve_path(virtual_path)
        if real_path.exists():
            with open(real_path, 'wb') as f:
                f.truncate(0)

    def get_size(self, virtual_path: str) -> int:
        """Get file size"""
        real_path = self._resolve_path(virtual_path)
        return real_path.stat().st_size

    def mkdir(self, virtual_path: str):
        """Create a directory"""
        real_path = self._resolve_path(virtual_path)
        real_path.mkdir(parents=True, exist_ok=True)

    def rmdir(self, virtual_path: str):
        """Remove a directory"""
        real_path = self._resolve_path(virtual_path)
        if real_path.exists() and real_path.is_dir():
            real_path.rmdir()

    def listdir(self, virtual_path: str) -> List[str]:
        """List directory contents"""
        real_path = self._resolve_path(virtual_path)
        if not real_path.exists() or not real_path.is_dir():
            return []
        return os.listdir(real_path)

    def isdir(self, virtual_path: str) -> bool:
        """Check if path is a directory"""
        real_path = self._resolve_path(virtual_path)
        return real_path.exists() and real_path.is_dir()

    def copy_to_local(self, virtual_path: str, local_path: Path):
        """Copy a file from backend to local path"""
        real_path = self._resolve_path(virtual_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(real_path, local_path)

    def copy_from_local(self, local_path: Path, virtual_path: str):
        """Copy a local file to backend"""
        real_path = self._resolve_path(virtual_path)
        real_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, real_path)

    def get_free_space(self) -> Optional[int]:
        """Get free space in bytes"""
        try:
            stat = os.statvfs(self.root_path)
            return stat.f_bavail * stat.f_frsize
        except Exception:
            return None

    def sync(self):
        """Sync any buffered writes to storage"""
        # On Linux, sync specific directory
        try:
            fd = os.open(self.root_path, os.O_RDONLY)
            os.fsync(fd)
            os.close(fd)
        except Exception:
            pass

    def __repr__(self):
        return f"LocalBackend(name={self.name}, root={self.root_path})"
