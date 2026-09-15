"""
File state tracking using SQLite database and extended attributes

This module manages the state of all files in the Storage filesystem, including:
- File checksums and sizes
- Replication status across backing filesystems
- Last access and modification times
- Sync status (clean, dirty, syncing)
- Conflict information
"""

import threading
import sqlite3
import time
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple

from .checksums import fast_checksum_file
from .file_config import (
    XATTR_CONFIG_NAME as FILE_CONFIG_XATTR_NAME,
    FileConfig,
    get_file_config,
    get_inherited_config,
    set_file_config,
)
from .models import FileLocation, FileMetadata, SyncStatus


class StateManager:
    """Manages file state in SQLite database"""

    XATTR_CONFIG_NAME = FILE_CONFIG_XATTR_NAME
    ACCESS_DEBOUNCE_SECONDS = 0.25
    ACCESS_FLUSH_INTERVAL_SECONDS = 0.5
    ACCESS_FLUSH_BATCH_SIZE = 256
    WRITE_BATCH_SIZE = 1000

    # Keyset paging over file_locations. The cursor is a row-value comparison on
    # the (virtual_path, backend_name) primary key so each page is an index range
    # seek. The unary + on is_cached stops SQLite from choosing idx_cached, which
    # would re-sort all remaining cached rows for every page (quadratic).
    # tests/test_state.py bounds the work each page does.
    ITER_LOCATIONS_QUERY = """
        SELECT * FROM file_locations
        WHERE (virtual_path, backend_name) > (?, ?)
        ORDER BY virtual_path ASC, backend_name ASC
        LIMIT ?
    """
    ITER_LOCATIONS_BY_CACHE_QUERY = """
        SELECT * FROM file_locations
        WHERE +is_cached = ?
          AND (virtual_path, backend_name) > (?, ?)
        ORDER BY virtual_path ASC, backend_name ASC
        LIMIT ?
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._db_lock = threading.RLock()
        self._access_lock = threading.RLock()
        self._pending_access_updates: Dict[str, float] = {}
        self._recent_access_updates: Dict[str, float] = {}
        self._last_access_flush_mono = time.monotonic()
        self._configure_connection()
        self._init_db()

    def _execute_write(self, query: str, params: tuple = ()):
        """Execute a single write statement and commit it atomically."""
        with self._db_lock:
            self.conn.execute(query, params)
            self.conn.commit()

    def _commit_locked(self):
        """Commit the current transaction while the caller holds _db_lock."""
        self.conn.commit()

    def _configure_connection(self):
        """Configure SQLite pragmas for lower write amplification."""
        pragmas = (
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
            "PRAGMA temp_store=MEMORY",
            "PRAGMA busy_timeout=5000",
        )
        for pragma in pragmas:
            try:
                self.conn.execute(pragma)
            except sqlite3.DatabaseError:
                # In-memory DBs and some filesystems may not support all pragmas.
                continue

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

    def _flush_access_updates(self, force: bool = False):
        """Flush pending access-time updates in a single commit."""
        with self._access_lock:
            if not self._pending_access_updates:
                return

            now_mono = time.monotonic()
            should_flush = (
                force
                or len(self._pending_access_updates) >= self.ACCESS_FLUSH_BATCH_SIZE
                or (now_mono - self._last_access_flush_mono) >= self.ACCESS_FLUSH_INTERVAL_SECONDS
            )
            if not should_flush:
                return

            rows = [(ts, path) for path, ts in self._pending_access_updates.items()]
            with self._db_lock:
                self.conn.executemany(
                    'UPDATE files SET last_access = ? WHERE virtual_path = ?',
                    rows
                )
                self.conn.commit()
            self._pending_access_updates.clear()
            self._last_access_flush_mono = now_mono

    def get_file_metadata(self, virtual_path: str) -> Optional[FileMetadata]:
        """Get metadata for a file"""
        with self._db_lock:
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
        self._execute_write('''
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

    def get_file_locations(self, virtual_path: str) -> List[FileLocation]:
        """Get all locations where a file is stored"""
        with self._db_lock:
            cursor = self.conn.execute(
                'SELECT * FROM file_locations WHERE virtual_path = ?',
                (virtual_path,)
            )
            rows = cursor.fetchall()
        return [
            FileLocation(
                virtual_path=row['virtual_path'],
                backend_name=row['backend_name'],
                checksum=row['checksum'],
                last_synced=row['last_synced'],
                is_cached=bool(row['is_cached'])
            )
            for row in rows
        ]

    def get_all_file_metadata(self) -> List[FileMetadata]:
        """Return all file metadata rows."""
        self._flush_access_updates(force=True)
        with self._db_lock:
            cursor = self.conn.execute('SELECT * FROM files ORDER BY virtual_path ASC')
            rows = cursor.fetchall()
        return [FileMetadata.from_dict(dict(row)) for row in rows]

    def iter_file_metadata(self, batch_size: int = WRITE_BATCH_SIZE) -> Iterator[FileMetadata]:
        """Yield file metadata rows in bounded batches."""
        self._flush_access_updates(force=True)
        last_path = ""
        while True:
            with self._db_lock:
                cursor = self.conn.execute(
                    '''
                    SELECT * FROM files
                    WHERE virtual_path > ?
                    ORDER BY virtual_path ASC
                    LIMIT ?
                    ''',
                    (last_path, batch_size),
                )
                rows = cursor.fetchall()

            if not rows:
                break

            for row in rows:
                yield FileMetadata.from_dict(dict(row))
            last_path = rows[-1]['virtual_path']

    def count_file_metadata(self) -> int:
        """Return the number of file metadata rows."""
        self._flush_access_updates(force=True)
        with self._db_lock:
            cursor = self.conn.execute('SELECT COUNT(*) AS count FROM files')
            return int(cursor.fetchone()['count'])

    def get_all_file_locations(self, is_cached: Optional[bool] = None) -> List[FileLocation]:
        """Return all file location rows, optionally filtered by cache membership."""
        query = 'SELECT * FROM file_locations'
        params: tuple = ()
        if is_cached is not None:
            query += ' WHERE is_cached = ?'
            params = (int(is_cached),)
        query += ' ORDER BY virtual_path ASC, backend_name ASC'

        with self._db_lock:
            cursor = self.conn.execute(query, params)
            rows = cursor.fetchall()

        return [
            FileLocation(
                virtual_path=row['virtual_path'],
                backend_name=row['backend_name'],
                checksum=row['checksum'],
                last_synced=row['last_synced'],
                is_cached=bool(row['is_cached'])
            )
            for row in rows
        ]

    def iter_file_locations(
        self,
        is_cached: Optional[bool] = None,
        batch_size: int = WRITE_BATCH_SIZE,
    ) -> Iterator[FileLocation]:
        """Yield file location rows in bounded batches (see ITER_LOCATIONS_QUERY)."""
        last_path = ""
        last_backend = ""
        while True:
            params: tuple
            if is_cached is None:
                query = self.ITER_LOCATIONS_QUERY
                params = (last_path, last_backend, batch_size)
            else:
                query = self.ITER_LOCATIONS_BY_CACHE_QUERY
                params = (int(is_cached), last_path, last_backend, batch_size)

            with self._db_lock:
                cursor = self.conn.execute(query, params)
                rows = cursor.fetchall()

            if not rows:
                break

            for row in rows:
                yield FileLocation(
                    virtual_path=row['virtual_path'],
                    backend_name=row['backend_name'],
                    checksum=row['checksum'],
                    last_synced=row['last_synced'],
                    is_cached=bool(row['is_cached'])
                )
            last_path = rows[-1]['virtual_path']
            last_backend = rows[-1]['backend_name']

    def get_location_backends(self, virtual_path: str, recursive: bool = False) -> Set[str]:
        """
        Return backend names that currently hold the path or subtree.

        Cache-only entries are excluded because callers use this to decide which
        remote backends need a path transform such as rename.
        """
        path = self._normalize_virtual_path(virtual_path)
        params: tuple
        query = 'SELECT DISTINCT backend_name FROM file_locations WHERE is_cached = 0 AND virtual_path = ?'
        params = (path,)
        if recursive:
            query = (
                'SELECT DISTINCT backend_name FROM file_locations '
                'WHERE is_cached = 0 AND (virtual_path = ? OR virtual_path LIKE ?)'
            )
            params = (path, f"{path}/%")

        with self._db_lock:
            cursor = self.conn.execute(query, params)
            rows = cursor.fetchall()

        return {row['backend_name'] for row in rows}

    def add_file_location(self, location: FileLocation):
        """Record that a file exists on a backend"""
        self._execute_write('''
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

    @staticmethod
    def _metadata_row(metadata: FileMetadata) -> tuple:
        return (
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
            metadata.encryption_key_id,
        )

    @staticmethod
    def _location_row(location: FileLocation) -> tuple:
        return (
            location.virtual_path,
            location.backend_name,
            location.checksum,
            location.last_synced,
            int(location.is_cached),
        )

    def _insert_metadata_rows_locked(self, rows: List[tuple], *, table_name: str = "files"):
        if not rows:
            return
        if table_name not in {"files", "replacement_files"}:
            raise ValueError(f"unsupported metadata table: {table_name}")
        self.conn.executemany(
            f'''
            INSERT INTO {table_name}
            (virtual_path, size, checksum, last_access, last_modified,
             sync_status, replica_count, recomputable, recompute_cmd,
             encrypted, encryption_key_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            rows,
        )

    def _insert_location_rows_locked(self, rows: List[tuple], *, table_name: str = "file_locations"):
        if not rows:
            return
        if table_name not in {"file_locations", "replacement_file_locations"}:
            raise ValueError(f"unsupported location table: {table_name}")
        self.conn.executemany(
            f'''
            INSERT INTO {table_name}
            (virtual_path, backend_name, checksum, last_synced, is_cached)
            VALUES (?, ?, ?, ?, ?)
            ''',
            rows,
        )

    def replace_backend_locations_for_paths(
        self,
        backend_name: str,
        virtual_paths: List[str],
        locations: List[FileLocation],
        *,
        is_cached: bool,
    ):
        """Replace location rows for one backend, scoped to the provided paths only."""
        normalized_paths = [self._normalize_virtual_path(path) for path in virtual_paths]

        with self._db_lock:
            if normalized_paths:
                self.conn.executemany(
                    'DELETE FROM file_locations WHERE backend_name = ? AND is_cached = ? AND virtual_path = ?',
                    [
                        (backend_name, int(is_cached), path)
                        for path in normalized_paths
                    ],
                )

            if locations:
                self.conn.executemany(
                    '''
                    INSERT INTO file_locations
                    (virtual_path, backend_name, checksum, last_synced, is_cached)
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    [
                        (
                            location.virtual_path,
                            location.backend_name,
                            location.checksum,
                            location.last_synced,
                            int(location.is_cached),
                        )
                        for location in locations
                    ],
                )

            self.conn.commit()

    def apply_reprobe_backend_update(
        self,
        *,
        promoted_metadata: List[FileMetadata],
        backend_name: str,
        virtual_paths: List[str],
        locations: List[FileLocation],
        is_cached: bool,
    ):
        """Atomically apply reprobe metadata promotions and backend location replacement."""
        normalized_paths = [self._normalize_virtual_path(path) for path in virtual_paths]

        with self._db_lock:
            try:
                if promoted_metadata:
                    self.conn.executemany(
                        '''
                        INSERT OR REPLACE INTO files
                        (virtual_path, size, checksum, last_access, last_modified,
                         sync_status, replica_count, recomputable, recompute_cmd,
                         encrypted, encryption_key_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''',
                        [
                            (
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
                                metadata.encryption_key_id,
                            )
                            for metadata in promoted_metadata
                        ],
                    )

                if normalized_paths:
                    self.conn.executemany(
                        'DELETE FROM file_locations WHERE backend_name = ? AND is_cached = ? AND virtual_path = ?',
                        [
                            (backend_name, int(is_cached), path)
                            for path in normalized_paths
                        ],
                    )

                if locations:
                    self.conn.executemany(
                        '''
                        INSERT INTO file_locations
                        (virtual_path, backend_name, checksum, last_synced, is_cached)
                        VALUES (?, ?, ?, ?, ?)
                        ''',
                        [
                            (
                                location.virtual_path,
                                location.backend_name,
                                location.checksum,
                                location.last_synced,
                                int(location.is_cached),
                            )
                            for location in locations
                        ],
                    )
                self._commit_locked()
            except Exception:
                self.conn.rollback()
                raise

    def remove_file_location(self, virtual_path: str, backend_name: str):
        """Remove a file location"""
        self._execute_write(
            'DELETE FROM file_locations WHERE virtual_path = ? AND backend_name = ?',
            (virtual_path, backend_name)
        )

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
        rename_map: List[Tuple[str, str]] = []
        with self._db_lock:
            cursor = self.conn.execute(
                'SELECT virtual_path FROM files WHERE virtual_path = ? OR virtual_path LIKE ?',
                (old_path, like_pattern)
            )

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
        with self._access_lock:
            recent = self._recent_access_updates.get(virtual_path)
            if recent is not None and (now - recent) < self.ACCESS_DEBOUNCE_SECONDS:
                return

            self._recent_access_updates[virtual_path] = now
            self._pending_access_updates[virtual_path] = now
        self._flush_access_updates(force=False)

    def get_lru_files(self, limit: int = 100) -> List[FileMetadata]:
        """Get least recently used files"""
        self._flush_access_updates(force=True)
        with self._db_lock:
            cursor = self.conn.execute('''
                SELECT * FROM files
                WHERE sync_status = ?
                ORDER BY last_access ASC
                LIMIT ?
            ''', (SyncStatus.CLEAN.value, limit))
            rows = cursor.fetchall()
        return [FileMetadata.from_dict(dict(row)) for row in rows]

    def get_recomputable_files(self) -> List[FileMetadata]:
        """Get all recomputable files"""
        with self._db_lock:
            cursor = self.conn.execute(
                'SELECT * FROM files WHERE recomputable = 1 ORDER BY size ASC'
            )
            rows = cursor.fetchall()
        return [FileMetadata.from_dict(dict(row)) for row in rows]

    def delete_file(self, virtual_path: str):
        """Delete file from state tracking"""
        with self._db_lock:
            self.conn.execute('DELETE FROM file_locations WHERE virtual_path = ?', (virtual_path,))
            self.conn.execute('DELETE FROM files WHERE virtual_path = ?', (virtual_path,))
            self.conn.commit()

    def replace_index(
        self,
        metadata_rows: Iterable[FileMetadata],
        locations: Iterable[FileLocation],
    ):
        """Replace the local index contents atomically."""
        with self._access_lock:
            self._pending_access_updates.clear()
            self._recent_access_updates.clear()

        with self._db_lock:
            try:
                self.conn.execute('DELETE FROM file_locations')
                self.conn.execute('DELETE FROM files')

                batch: List[tuple] = []
                for metadata in metadata_rows:
                    batch.append(self._metadata_row(metadata))
                    if len(batch) >= self.WRITE_BATCH_SIZE:
                        self._insert_metadata_rows_locked(batch)
                        batch.clear()
                self._insert_metadata_rows_locked(batch)

                batch = []
                for location in locations:
                    batch.append(self._location_row(location))
                    if len(batch) >= self.WRITE_BATCH_SIZE:
                        self._insert_location_rows_locked(batch)
                        batch.clear()
                self._insert_location_rows_locked(batch)

                self._commit_locked()
            except Exception:
                self.conn.rollback()
                raise

    def replace_index_from_entries(
        self,
        entries: Iterable[Tuple[FileMetadata, FileLocation]],
    ):
        """Replace the index from a streaming metadata/location entry source."""
        with self._access_lock:
            self._pending_access_updates.clear()
            self._recent_access_updates.clear()

        with self._db_lock:
            try:
                self.conn.execute('DROP TABLE IF EXISTS replacement_files')
                self.conn.execute('DROP TABLE IF EXISTS replacement_file_locations')
                self.conn.execute(
                    '''
                    CREATE TEMP TABLE replacement_files (
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
                    )
                    '''
                )
                self.conn.execute(
                    '''
                    CREATE TEMP TABLE replacement_file_locations (
                        virtual_path TEXT NOT NULL,
                        backend_name TEXT NOT NULL,
                        checksum TEXT NOT NULL,
                        last_synced REAL NOT NULL,
                        is_cached INTEGER NOT NULL,
                        PRIMARY KEY (virtual_path, backend_name)
                    )
                    '''
                )

                metadata_batch: List[tuple] = []
                location_batch: List[tuple] = []
                for metadata, location in entries:
                    metadata_batch.append(self._metadata_row(metadata))
                    location_batch.append(self._location_row(location))
                    if len(metadata_batch) >= self.WRITE_BATCH_SIZE:
                        self._insert_metadata_rows_locked(metadata_batch, table_name="replacement_files")
                        self._insert_location_rows_locked(location_batch, table_name="replacement_file_locations")
                        metadata_batch.clear()
                        location_batch.clear()
                self._insert_metadata_rows_locked(metadata_batch, table_name="replacement_files")
                self._insert_location_rows_locked(location_batch, table_name="replacement_file_locations")

                self.conn.execute('DELETE FROM file_locations')
                self.conn.execute('DELETE FROM files')
                self.conn.execute(
                    '''
                    INSERT INTO files
                    SELECT * FROM replacement_files
                    '''
                )
                self.conn.execute(
                    '''
                    INSERT INTO file_locations
                    SELECT * FROM replacement_file_locations
                    '''
                )
                self.conn.execute('DROP TABLE replacement_file_locations')
                self.conn.execute('DROP TABLE replacement_files')
                self._commit_locked()
            except Exception:
                self.conn.rollback()
                raise

    def get_dirty_files(self) -> List[FileMetadata]:
        """Get all files that need syncing"""
        with self._db_lock:
            cursor = self.conn.execute(
                'SELECT * FROM files WHERE sync_status = ?',
                (SyncStatus.DIRTY.value,)
            )
            rows = cursor.fetchall()
        return [FileMetadata.from_dict(dict(row)) for row in rows]

    @staticmethod
    def get_file_config(real_path: Path) -> Optional[FileConfig]:
        """Compatibility wrapper for xattr-backed file configuration."""
        return get_file_config(real_path)

    @staticmethod
    def set_file_config(real_path: Path, config: FileConfig):
        """Compatibility wrapper for xattr-backed file configuration."""
        set_file_config(real_path, config)

    @staticmethod
    def get_inherited_config(real_path: Path) -> FileConfig:
        """Compatibility wrapper for inherited xattr-backed configuration."""
        return get_inherited_config(real_path)

    @staticmethod
    def compute_checksum(file_path: Path) -> str:
        """Compute a fast non-cryptographic checksum of a file (XXH3-128)."""
        return fast_checksum_file(file_path)

    def close(self):
        """Close database connection"""
        self._flush_access_updates(force=True)
        with self._db_lock:
            self.conn.close()
