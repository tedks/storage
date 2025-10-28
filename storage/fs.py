"""
FUSE filesystem implementation for Storage

Main filesystem layer that provides:
- Read path: local cache → remote backends → recomputation
- Write path: local cache first → background sync to backends
- LRU cache management
- N-way replication
"""

import os
import errno
import threading
from fuse import FUSE, FuseOSError, Operations
from pathlib import Path
from typing import Dict, Optional

from .backends.base import Backend
from .cache import CacheManager
from .state import StateManager, FileMetadata, FileConfig, SyncStatus
from .replication import ReplicationManager


class StorageFS(Operations):
    """
    Storage FUSE filesystem

    Implements a filesystem that multiplexes over multiple backing filesystems
    with local caching, replication, and background sync.
    """

    def __init__(self, cache_root: Path, state_db: Path, backends: Dict[str, Backend],
                 cache_size: int):
        """
        Initialize Storage filesystem

        Args:
            cache_root: Root directory for local cache
            state_db: Path to SQLite state database
            backends: Dictionary of backend name -> Backend instance
            cache_size: Maximum cache size in bytes
        """
        self.state_manager = StateManager(str(state_db))
        self.cache_manager = CacheManager(cache_root, self.state_manager, cache_size)
        self.replication_manager = ReplicationManager(self.state_manager, backends)
        self.backends = backends

        # File descriptor tracking
        self._fd_counter = 0
        self._fd_map = {}  # fd -> (virtual_path, cache_path, flags)
        self._fd_lock = threading.Lock()

    def _get_config(self, virtual_path: str) -> FileConfig:
        """Get configuration for a file"""
        cache_path = self.cache_manager.get_cache_path(virtual_path)
        return self.state_manager.get_inherited_config(cache_path)

    def _ensure_file_in_cache(self, virtual_path: str) -> bool:
        """
        Ensure a file is in the cache, downloading if necessary

        Returns:
            True if file is available in cache, False otherwise
        """
        # Check if already cached
        if self.cache_manager.is_cached(virtual_path):
            self.cache_manager.mark_accessed(virtual_path)
            return True

        # Try to download from a backend
        backend = self.replication_manager.get_backend_for_read(virtual_path)

        if backend:
            # Download to cache
            if self.cache_manager.cache_file(virtual_path, backend):
                return True

        # File not available
        return False

    # Filesystem methods

    def getattr(self, path, fh=None):
        """Get file attributes"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            st = os.lstat(str(cache_path))
            return dict((key, getattr(st, key)) for key in (
                'st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime',
                'st_nlink', 'st_size', 'st_uid'))
        except OSError as e:
            raise FuseOSError(e.errno)

    def readdir(self, path, fh):
        """Read directory contents"""
        cache_path = self.cache_manager.get_cache_path(path)

        dirents = ['.', '..']

        # Get from cache
        if cache_path.exists() and cache_path.is_dir():
            dirents.extend(os.listdir(str(cache_path)))

        # Also merge from backends (union view)
        # TODO: Implement proper directory merging across backends

        for r in dirents:
            yield r

    def mkdir(self, path, mode):
        """Create a directory"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            cache_path.mkdir(parents=True, exist_ok=False)
            os.chmod(cache_path, mode)

            # TODO: Propagate to backends in background

        except OSError as e:
            raise FuseOSError(e.errno)

    def rmdir(self, path):
        """Remove a directory"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            cache_path.rmdir()

            # TODO: Propagate to backends in background

        except OSError as e:
            raise FuseOSError(e.errno)

    def unlink(self, path):
        """Remove a file"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            cache_path.unlink()

            # Remove from state tracking
            self.state_manager.delete_file(path)

            # TODO: Propagate to backends in background

        except OSError as e:
            raise FuseOSError(e.errno)

    def rename(self, old, new):
        """Rename a file or directory"""
        old_cache = self.cache_manager.get_cache_path(old)
        new_cache = self.cache_manager.get_cache_path(new)

        try:
            new_cache.parent.mkdir(parents=True, exist_ok=True)
            old_cache.rename(new_cache)

            # TODO: Update state tracking and propagate to backends

        except OSError as e:
            raise FuseOSError(e.errno)

    def chmod(self, path, mode):
        """Change file mode"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            os.chmod(cache_path, mode)
        except OSError as e:
            raise FuseOSError(e.errno)

    def chown(self, path, uid, gid):
        """Change file owner"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            os.chown(cache_path, uid, gid)
        except OSError as e:
            raise FuseOSError(e.errno)

    def truncate(self, path, length, fh=None):
        """Truncate a file"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            with open(cache_path, 'r+b') as f:
                f.truncate(length)

            # Mark as dirty
            metadata = self.state_manager.get_file_metadata(path)
            if metadata:
                metadata.sync_status = SyncStatus.DIRTY
                self.state_manager.set_file_metadata(metadata)

        except OSError as e:
            raise FuseOSError(e.errno)

    # File operations

    def open(self, path, flags):
        """Open a file"""
        # Ensure file is in cache
        if not self._ensure_file_in_cache(path):
            # File doesn't exist
            raise FuseOSError(errno.ENOENT)

        cache_path = self.cache_manager.get_cache_path(path)

        try:
            real_fd = os.open(str(cache_path), flags)

            # Allocate virtual fd
            with self._fd_lock:
                vfd = self._fd_counter
                self._fd_counter += 1
                self._fd_map[vfd] = (path, cache_path, real_fd, flags)

            # Mark as accessed
            self.cache_manager.mark_accessed(path)

            return vfd

        except OSError as e:
            raise FuseOSError(e.errno)

    def create(self, path, mode, fi=None):
        """Create a file"""
        cache_path = self.cache_manager.create_cached_file(path)

        try:
            real_fd = os.open(str(cache_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)

            # Allocate virtual fd
            with self._fd_lock:
                vfd = self._fd_counter
                self._fd_counter += 1
                self._fd_map[vfd] = (path, cache_path, real_fd, os.O_WRONLY)

            # Create metadata
            import time
            config = self._get_config(path)
            metadata = FileMetadata(
                virtual_path=path,
                size=0,
                checksum=None,
                last_access=time.time(),
                last_modified=time.time(),
                sync_status=SyncStatus.DIRTY,
                replica_count=config.replica_count,
                recomputable=config.recomputable,
                recompute_cmd=config.recompute_cmd,
                encrypted=config.encrypted,
                encryption_key_id=None
            )
            self.state_manager.set_file_metadata(metadata)

            return vfd

        except OSError as e:
            raise FuseOSError(e.errno)

    def read(self, path, size, offset, fh):
        """Read from a file"""
        with self._fd_lock:
            if fh not in self._fd_map:
                raise FuseOSError(errno.EBADF)
            _, _, real_fd, _ = self._fd_map[fh]

        try:
            os.lseek(real_fd, offset, os.SEEK_SET)
            data = os.read(real_fd, size)

            # Mark as accessed
            self.cache_manager.mark_accessed(path)

            return data

        except OSError as e:
            raise FuseOSError(e.errno)

    def write(self, path, data, offset, fh):
        """Write to a file"""
        with self._fd_lock:
            if fh not in self._fd_map:
                raise FuseOSError(errno.EBADF)
            _, _, real_fd, _ = self._fd_map[fh]

        try:
            os.lseek(real_fd, offset, os.SEEK_SET)
            written = os.write(real_fd, data)

            # Mark as dirty and accessed
            self.cache_manager.mark_accessed(path)

            metadata = self.state_manager.get_file_metadata(path)
            if metadata:
                metadata.sync_status = SyncStatus.DIRTY
                import time
                metadata.last_modified = time.time()
                self.state_manager.set_file_metadata(metadata)

            return written

        except OSError as e:
            raise FuseOSError(e.errno)

    def release(self, path, fh):
        """Release (close) a file"""
        with self._fd_lock:
            if fh not in self._fd_map:
                return 0
            _, _, real_fd, _ = self._fd_map[fh]
            del self._fd_map[fh]

        try:
            os.close(real_fd)

            # Flush cache
            self.cache_manager.flush_file(path)

            # TODO: Trigger background sync if dirty

            return 0

        except OSError as e:
            raise FuseOSError(e.errno)

    def fsync(self, path, datasync, fh):
        """Sync file to disk"""
        with self._fd_lock:
            if fh not in self._fd_map:
                return 0
            _, _, real_fd, _ = self._fd_map[fh]

        try:
            if datasync:
                os.fdatasync(real_fd)
            else:
                os.fsync(real_fd)
            return 0

        except OSError as e:
            raise FuseOSError(e.errno)

    # Extended attributes

    def getxattr(self, path, name, position=0):
        """Get extended attribute"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            return os.getxattr(str(cache_path), name)
        except OSError as e:
            raise FuseOSError(e.errno)

    def setxattr(self, path, name, value, options, position=0):
        """Set extended attribute"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            os.setxattr(str(cache_path), name, value)
        except OSError as e:
            raise FuseOSError(e.errno)

    def listxattr(self, path):
        """List extended attributes"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            return os.listxattr(str(cache_path))
        except OSError:
            return []

    def removexattr(self, path, name):
        """Remove extended attribute"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            os.removexattr(str(cache_path), name)
        except OSError as e:
            raise FuseOSError(e.errno)

    # Symbolic links

    def readlink(self, path):
        """Read symbolic link"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            return os.readlink(str(cache_path))
        except OSError as e:
            raise FuseOSError(e.errno)

    def symlink(self, target, source):
        """Create symbolic link"""
        cache_path = self.cache_manager.get_cache_path(source)

        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            os.symlink(target, str(cache_path))
        except OSError as e:
            raise FuseOSError(e.errno)

    # Time operations

    def utimens(self, path, times=None):
        """Update file timestamps"""
        cache_path = self.cache_manager.get_cache_path(path)

        try:
            os.utime(str(cache_path), times)
        except OSError as e:
            raise FuseOSError(e.errno)

    def destroy(self, path):
        """Cleanup on unmount"""
        self.state_manager.close()


def mount_storage(mountpoint: str, cache_root: Path, state_db: Path,
                 backends: Dict[str, Backend], cache_size: int,
                 foreground: bool = False):
    """
    Mount a Storage filesystem

    Args:
        mountpoint: Where to mount the filesystem
        cache_root: Root directory for cache
        state_db: Path to state database
        backends: Dictionary of backends
        cache_size: Maximum cache size in bytes
        foreground: Run in foreground (default: False)
    """
    fs = StorageFS(cache_root, state_db, backends, cache_size)
    FUSE(fs, mountpoint, nothreads=True, foreground=foreground, allow_other=True)
