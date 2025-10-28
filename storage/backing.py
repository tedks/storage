"""
Backing filesystem manager for Storage
Manages multiple underlying filesystems that back the abstract filesystem
"""

import os
from typing import List, Optional, Dict
from pathlib import Path


class BackingFilesystem:
    """Represents a single backing filesystem"""

    def __init__(self, name: str, path: str):
        self.name = name
        self.path = Path(path).resolve()

        if not self.path.exists():
            raise ValueError(f"Backing filesystem path does not exist: {path}")
        if not self.path.is_dir():
            raise ValueError(f"Backing filesystem path is not a directory: {path}")

    def get_real_path(self, virtual_path: str) -> Path:
        """Convert a virtual path to a real path on this backing filesystem"""
        # Remove leading slash for path joining
        relative_path = virtual_path.lstrip('/')
        return self.path / relative_path

    def __repr__(self):
        return f"BackingFilesystem(name={self.name}, path={self.path})"


class BackingManager:
    """Manages multiple backing filesystems"""

    def __init__(self):
        self.filesystems: Dict[str, BackingFilesystem] = {}
        self.default_filesystem: Optional[str] = None

    def add_filesystem(self, name: str, path: str, is_default: bool = False):
        """Add a backing filesystem"""
        backing = BackingFilesystem(name, path)
        self.filesystems[name] = backing

        if is_default or self.default_filesystem is None:
            self.default_filesystem = name

        return backing

    def get_filesystem(self, name: str) -> Optional[BackingFilesystem]:
        """Get a backing filesystem by name"""
        return self.filesystems.get(name)

    def get_default_filesystem(self) -> Optional[BackingFilesystem]:
        """Get the default backing filesystem"""
        if self.default_filesystem:
            return self.filesystems.get(self.default_filesystem)
        return None

    def list_filesystems(self) -> List[BackingFilesystem]:
        """List all backing filesystems"""
        return list(self.filesystems.values())

    def resolve_path(self, virtual_path: str, filesystem_name: Optional[str] = None) -> Path:
        """
        Resolve a virtual path to a real path on a backing filesystem
        If filesystem_name is not provided, uses the default filesystem
        """
        if filesystem_name:
            backing = self.get_filesystem(filesystem_name)
            if not backing:
                raise ValueError(f"Unknown backing filesystem: {filesystem_name}")
        else:
            backing = self.get_default_filesystem()
            if not backing:
                raise ValueError("No default backing filesystem configured")

        return backing.get_real_path(virtual_path)

    def __len__(self):
        return len(self.filesystems)
