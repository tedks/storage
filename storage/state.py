"""
File state tracking using SQLite database and extended attributes

This module manages the state of all files in the Storage filesystem, including:
- File checksums and sizes
- Replication status across backing filesystems
- Last access and modification times
- Sync status (clean, dirty, syncing)
- Conflict information
"""

import os
import sqlite3
import hashlib
import time
import yaml
from pathlib import Path
from typing import Optional, List, Dict, Set, Tuple
from enum import Enum
from dataclasses import dataclass, asdict


class SyncStatus(Enum):
    """Status of file synchronization"""
    CLEAN = "clean"  # File is synced to all required backends
    DIRTY = "dirty"  # File has unsynced changes
    SYNCING = "syncing"  # File is currently being synced
    ERROR = "error"  # Sync error occurred
    CONFLICT = "conflict"  # Conflict detected


@dataclass
class FileMetadata:
    """Metadata for a file in the Storage filesystem"""
    virtual_path: str
    size: int
    checksum: Optional[str]  # SHA256 hash
    last_access: float
    last_modified: float
    sync_status: SyncStatus
    replica_count: int  # Configured replica count (-1 = all backends)
    recomputable: bool
    recompute_cmd: Optional[str]
    encrypted: bool
    encryption_key_id: Optional[str]

    def to_dict(self) -> Dict:
        """Convert to dictionary for storage"""
        d = asdict(self)
        d['sync_status'] = self.sync_status.value
        return d

    @classmethod
    def from_dict(cls, data: Dict) -> 'FileMetadata':
        """Create from dictionary"""
        data['sync_status'] = SyncStatus(data['sync_status'])
        return cls(**data)


@dataclass
class FileLocation:
    """Tracks where a file is stored"""
    virtual_path: str
    backend_name: str
    checksum: str
    last_synced: float
    is_cached: bool  # True if this is the local cache


@dataclass
class FileConfig:
    """Configuration for a file/directory from extended attributes"""
    replica_count: int = 1  # -1 for all backends
    preferred_backends: List[str] = None  # Preferred backing filesystems
    encrypted: bool = False
    encryption_backends: List[str] = None  # Which backends to encrypt for
    recomputable: bool = False
    recompute_cmd: Optional[str] = None
    custom: Dict = None  # Custom configuration

    def __post_init__(self):
        if self.preferred_backends is None:
            self.preferred_backends = []
        if self.encryption_backends is None:
            self.encryption_backends = []
        if self.custom is None:
            self.custom = {}

    def to_yaml(self) -> str:
        """Convert to YAML for storage in xattrs"""
        data = {
            'replica_count': self.replica_count,
            'preferred_backends': self.preferred_backends,
            'encrypted': self.encrypted,
            'encryption_backends': self.encryption_backends,
            'recomputable': self.recomputable,
            'recompute_cmd': self.recompute_cmd,
            'custom': self.custom,
        }
        return yaml.dump(data, default_flow_style=False)

    @classmethod
    def from_yaml(cls, yaml_str: str) -> 'FileConfig':
        """Create from YAML"""
        data = yaml.safe_load(yaml_str) or {}
        return cls(**data)


class StateManager:
    """Manages file state in SQLite database"""

    XATTR_CONFIG_NAME = "user.storage.config"

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        """Initialize database schema"""
        self.conn.executescript('''
            CREATE TABLE IF NOT EXISTS files (
                virtual_path TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                checksum TEXT,
                last_access REAL NOT NULL,
                last_modified REAL NOT NULL,
                sync_status TEXT NOT NULL,
                replica_count INTEGER NOT NULL,
                recomputable INTEGER NOT NULL,
                recompute_cmd TEXT,
                encrypted INTEGER NOT NULL,
                encryption_key_id TEXT
            );

            CREATE TABLE IF NOT EXISTS file_locations (
                virtual_path TEXT NOT NULL,
                backend_name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                last_synced REAL NOT NULL,
                is_cached INTEGER NOT NULL,
                PRIMARY KEY (virtual_path, backend_name),
                FOREIGN KEY (virtual_path) REFERENCES files(virtual_path)
            );

            CREATE INDEX IF NOT EXISTS idx_last_access ON files(last_access);
            CREATE INDEX IF NOT EXISTS idx_sync_status ON files(sync_status);
            CREATE INDEX IF NOT EXISTS idx_recomputable ON files(recomputable);
            CREATE INDEX IF NOT EXISTS idx_cached ON file_locations(is_cached);
        ''')
        self.conn.commit()

    def get_file_metadata(self, virtual_path: str) -> Optional[FileMetadata]:
        """Get metadata for a file"""
        cursor = self.conn.execute(
            'SELECT * FROM files WHERE virtual_path = ?',
            (virtual_path,)
        )
        row = cursor.fetchone()
        if not row:
            return None

        return FileMetadata(
            virtual_path=row['virtual_path'],
            size=row['size'],
            checksum=row['checksum'],
            last_access=row['last_access'],
            last_modified=row['last_modified'],
            sync_status=SyncStatus(row['sync_status']),
            replica_count=row['replica_count'],
            recomputable=bool(row['recomputable']),
            recompute_cmd=row['recompute_cmd'],
            encrypted=bool(row['encrypted']),
            encryption_key_id=row['encryption_key_id']
        )

    def set_file_metadata(self, metadata: FileMetadata):
        """Store or update file metadata"""
        self.conn.execute('''
            INSERT OR REPLACE INTO files
            (virtual_path, size, checksum, last_access, last_modified,
             sync_status, replica_count, recomputable, recompute_cmd,
             encrypted, encryption_key_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            metadata.virtual_path,
            metadata.size,
            metadata.checksum,
            metadata.last_access,
            metadata.last_modified,
            metadata.sync_status.value,
            metadata.replica_count,
            int(metadata.recomputable),
            metadata.recompute_cmd,
            int(metadata.encrypted),
            metadata.encryption_key_id
        ))
        self.conn.commit()

    def get_file_locations(self, virtual_path: str) -> List[FileLocation]:
        """Get all locations where a file is stored"""
        cursor = self.conn.execute(
            'SELECT * FROM file_locations WHERE virtual_path = ?',
            (virtual_path,)
        )
        return [
            FileLocation(
                virtual_path=row['virtual_path'],
                backend_name=row['backend_name'],
                checksum=row['checksum'],
                last_synced=row['last_synced'],
                is_cached=bool(row['is_cached'])
            )
            for row in cursor.fetchall()
        ]

    def add_file_location(self, location: FileLocation):
        """Record that a file exists on a backend"""
        self.conn.execute('''
            INSERT OR REPLACE INTO file_locations
            (virtual_path, backend_name, checksum, last_synced, is_cached)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            location.virtual_path,
            location.backend_name,
            location.checksum,
            location.last_synced,
            int(location.is_cached)
        ))
        self.conn.commit()

    def remove_file_location(self, virtual_path: str, backend_name: str):
        """Remove a file location"""
        self.conn.execute(
            'DELETE FROM file_locations WHERE virtual_path = ? AND backend_name = ?',
            (virtual_path, backend_name)
        )
        self.conn.commit()

    @staticmethod
    def _normalize_virtual_path(path: str) -> str:
        """Ensure virtual paths consistently start with '/' and have no trailing slash."""
        if not path:
            return '/'
        if not path.startswith('/'):
            path = '/' + path
        if len(path) > 1 and path.endswith('/'):
            path = path.rstrip('/')
        return path

    def rename_path(self, old_virtual_path: str, new_virtual_path: str) -> List[Tuple[str, str]]:
        """Rename metadata entries from old path (and descendants) to new path."""
        old_path = self._normalize_virtual_path(old_virtual_path)
        new_path = self._normalize_virtual_path(new_virtual_path)

        if old_path == new_path:
            return []

        like_pattern = f"{old_path}/%"
        cursor = self.conn.execute(
            'SELECT virtual_path FROM files WHERE virtual_path = ? OR virtual_path LIKE ?',
            (old_path, like_pattern)
        )
        rename_map: List[Tuple[str, str]] = []

        for row in cursor.fetchall():
            current_path = row['virtual_path']
            if old_path == '/':
                suffix = current_path
            else:
                suffix = current_path[len(old_path):]
            new_entry = new_path + suffix
            rename_map.append((current_path, new_entry))

        for original, updated in rename_map:
            self.conn.execute(
                'UPDATE files SET virtual_path = ? WHERE virtual_path = ?',
                (updated, original)
            )
            self.conn.execute(
                'UPDATE file_locations SET virtual_path = ? WHERE virtual_path = ?',
                (updated, original)
            )

        if rename_map:
            self.conn.commit()

        return rename_map

    def rename_file(self, old_virtual_path: str, new_virtual_path: str):
        """Rename a single file entry (alias for rename_path)."""
        self.rename_path(old_virtual_path, new_virtual_path)

    def update_access_time(self, virtual_path: str):
        """Update last access time (for LRU)"""
        now = time.time()
        self.conn.execute(
            'UPDATE files SET last_access = ? WHERE virtual_path = ?',
            (now, virtual_path)
        )
        self.conn.commit()

    def get_lru_files(self, limit: int = 100) -> List[FileMetadata]:
        """Get least recently used files"""
        cursor = self.conn.execute('''
            SELECT * FROM files
            WHERE sync_status = ?
            ORDER BY last_access ASC
            LIMIT ?
        ''', (SyncStatus.CLEAN.value, limit))

        return [FileMetadata.from_dict(dict(row)) for row in cursor.fetchall()]

    def get_recomputable_files(self) -> List[FileMetadata]:
        """Get all recomputable files"""
        cursor = self.conn.execute(
            'SELECT * FROM files WHERE recomputable = 1 ORDER BY size ASC'
        )
        return [FileMetadata.from_dict(dict(row)) for row in cursor.fetchall()]

    def delete_file(self, virtual_path: str):
        """Delete file from state tracking"""
        self.conn.execute('DELETE FROM file_locations WHERE virtual_path = ?', (virtual_path,))
        self.conn.execute('DELETE FROM files WHERE virtual_path = ?', (virtual_path,))
        self.conn.commit()

    def get_dirty_files(self) -> List[FileMetadata]:
        """Get all files that need syncing"""
        cursor = self.conn.execute(
            'SELECT * FROM files WHERE sync_status = ?',
            (SyncStatus.DIRTY.value,)
        )
        return [FileMetadata.from_dict(dict(row)) for row in cursor.fetchall()]

    @staticmethod
    def get_file_config(real_path: Path) -> Optional[FileConfig]:
        """Get configuration from file's extended attributes"""
        try:
            yaml_data = os.getxattr(str(real_path), StateManager.XATTR_CONFIG_NAME)
            return FileConfig.from_yaml(yaml_data.decode('utf-8'))
        except (OSError, KeyError):
            return None

    @staticmethod
    def set_file_config(real_path: Path, config: FileConfig):
        """Set configuration in file's extended attributes"""
        try:
            yaml_data = config.to_yaml().encode('utf-8')
            os.setxattr(str(real_path), StateManager.XATTR_CONFIG_NAME, yaml_data)
        except OSError as e:
            print(f"Warning: Failed to set xattr on {real_path}: {e}")

    @staticmethod
    def get_inherited_config(real_path: Path) -> FileConfig:
        """Get configuration, inheriting from parent directories"""
        # Start with defaults
        config = FileConfig()

        # Walk up directory tree, applying configurations
        # Child configs override parent configs, but we take the max replica_count
        current = real_path
        configs = []

        while current != current.parent:
            file_config = StateManager.get_file_config(current)
            if file_config:
                configs.insert(0, file_config)
            current = current.parent

        # Apply configurations (parents first, then children)
        for cfg in configs:
            # Take maximum replica count as per design
            if cfg.replica_count == -1 or config.replica_count == -1:
                config.replica_count = -1
            else:
                config.replica_count = max(config.replica_count, cfg.replica_count)

            # Override other settings (children win)
            if cfg.preferred_backends:
                config.preferred_backends = cfg.preferred_backends
            if cfg.encrypted:
                config.encrypted = cfg.encrypted
            if cfg.encryption_backends:
                config.encryption_backends = cfg.encryption_backends
            if cfg.recomputable:
                config.recomputable = cfg.recomputable
                config.recompute_cmd = cfg.recompute_cmd
            if cfg.custom:
                config.custom.update(cfg.custom)

        return config

    @staticmethod
    def compute_checksum(file_path: Path) -> str:
        """Compute SHA256 checksum of a file"""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        return sha256.hexdigest()

    def close(self):
        """Close database connection"""
        self.conn.close()
