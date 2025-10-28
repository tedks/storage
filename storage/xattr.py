"""
Extended attribute YAML storage system
Stores YAML documents in extended attributes to control storage distribution
"""

import os
import yaml
from typing import Dict, Optional, List
from pathlib import Path


class StorageMetadata:
    """Represents storage metadata for a directory or file"""

    XATTR_NAME = "user.storage.config"

    def __init__(self):
        self.backing_filesystems: List[str] = []
        self.replication_count: int = 1
        self.preferred_filesystem: Optional[str] = None
        self.custom_config: Dict = {}

    def to_dict(self) -> Dict:
        """Convert metadata to dictionary"""
        return {
            'backing_filesystems': self.backing_filesystems,
            'replication_count': self.replication_count,
            'preferred_filesystem': self.preferred_filesystem,
            'custom_config': self.custom_config,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'StorageMetadata':
        """Create metadata from dictionary"""
        metadata = cls()
        metadata.backing_filesystems = data.get('backing_filesystems', [])
        metadata.replication_count = data.get('replication_count', 1)
        metadata.preferred_filesystem = data.get('preferred_filesystem')
        metadata.custom_config = data.get('custom_config', {})
        return metadata

    def to_yaml(self) -> str:
        """Convert metadata to YAML string"""
        return yaml.dump(self.to_dict(), default_flow_style=False)

    @classmethod
    def from_yaml(cls, yaml_str: str) -> 'StorageMetadata':
        """Create metadata from YAML string"""
        data = yaml.safe_load(yaml_str)
        return cls.from_dict(data)


class XAttrManager:
    """Manages extended attributes for storage metadata"""

    def __init__(self):
        pass

    def get_metadata(self, path: Path) -> Optional[StorageMetadata]:
        """
        Get storage metadata from a file's extended attributes
        Returns None if no metadata is found
        """
        try:
            yaml_data = os.getxattr(str(path), StorageMetadata.XATTR_NAME)
            return StorageMetadata.from_yaml(yaml_data.decode('utf-8'))
        except (OSError, FileNotFoundError, KeyError):
            # No extended attribute or file doesn't exist
            return None
        except (yaml.YAMLError, UnicodeDecodeError) as e:
            # Corrupted metadata
            print(f"Warning: Corrupted metadata for {path}: {e}")
            return None

    def set_metadata(self, path: Path, metadata: StorageMetadata) -> bool:
        """
        Set storage metadata in a file's extended attributes
        Returns True on success, False on failure
        """
        try:
            yaml_data = metadata.to_yaml().encode('utf-8')
            os.setxattr(str(path), StorageMetadata.XATTR_NAME, yaml_data)
            return True
        except OSError as e:
            print(f"Warning: Failed to set metadata for {path}: {e}")
            return False

    def remove_metadata(self, path: Path) -> bool:
        """
        Remove storage metadata from a file's extended attributes
        Returns True on success, False on failure
        """
        try:
            os.removexattr(str(path), StorageMetadata.XATTR_NAME)
            return True
        except (OSError, FileNotFoundError):
            return False

    def get_inherited_metadata(self, path: Path) -> Optional[StorageMetadata]:
        """
        Get metadata for a path, inheriting from parent directories if not set
        """
        # Try to get metadata for this path
        metadata = self.get_metadata(path)
        if metadata:
            return metadata

        # Try parent directories
        current = path.parent
        while current != current.parent:  # Stop at root
            metadata = self.get_metadata(current)
            if metadata:
                return metadata
            current = current.parent

        return None
