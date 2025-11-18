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
import logging
import fcntl
import stat
import time
import hashlib
from fuse import FUSE, FuseOSError, Operations
from pathlib import Path
from typing import Dict, Optional, Set

from .backends.base import Backend
from .cache import CacheManager
from .state import StateManager, FileMetadata, FileConfig, SyncStatus
from .replication import ReplicationManager
from .sync import SyncManager
from .recompute import RecomputeEngine


class StorageFS(Operations):
    """
    Storage FUSE filesystem

    Implements a filesystem that multiplexes over multiple backing filesystems
    with local caching, replication, and background sync.
    """

    def __init__(self, cache_root: Path, state_db: Path, backends: Dict[str, Backend],
                 cache_size: int, num_sync_workers: int = 2, start_sync: bool = True):
        """
        Initialize Storage filesystem

        Args:
            cache_root: Root directory for local cache
            state_db: Path to SQLite state database
            backends: Dictionary of backend name -> Backend instance
            cache_size: Maximum cache size in bytes
            num_sync_workers: Number of background sync worker threads
        """
        self.state_manager = StateManager(str(state_db))
        self.cache_manager = CacheManager(cache_root, self.state_manager, cache_size)
        self.replication_manager = ReplicationManager(self.state_manager, backends)
        self.backends = backends

        # Background sync manager
        self.sync_manager = SyncManager(
            self.state_manager,
            self.cache_manager,
            self.replication_manager,
            num_workers=num_sync_workers
        )
        self._sync_started = False
        if start_sync:
            self.sync_manager.start()
            self._sync_started = True

        # Recomputation engine
        self.recompute_engine = RecomputeEngine(self.state_manager)

        # File descriptor tracking
        self._fd_counter = 0
        self._fd_map = {}  # fd -> (virtual_path, cache_path, flags)
        self._fd_lock = threading.Lock()

        # Directory listing cache (per directory, per backend) for fallback when backend unavailable
        self._dir_cache: Dict[str, Dict[str, Set[str]]] = {}
        self._dir_cache_lock = threading.Lock()

        self.logger = logging.getLogger(__name__)

    def init(self, path):
        """FUSE init hook (runs after mount, post-fork in background mode)."""
        if not self._sync_started:
            self.sync_manager.start()
            self._sync_started = True

    def _get_config(self, virtual_path: str) -> FileConfig:
        """Get configuration for a file"""
        cache_path = self.cache_manager.get_cache_path(virtual_path)
        return self.state_manager.get_inherited_config(cache_path)

    def _ensure_file_in_cache(self, virtual_path: str) -> bool:
        """
        Ensure a file is in the cache, downloading or recomputing if necessary

        Returns:
            True if file is available in cache, False otherwise
        """
        cache_path = self.cache_manager.get_cache_path(virtual_path)

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

        # File not in cache or backends - try recomputation
        if self.recompute_engine.can_recompute(virtual_path, cache_path):
            # Recompute the file synchronously (blocks until complete)
            result = self.recompute_engine.recompute_file(virtual_path, cache_path, timeout=30.0)

            if result.success:
                # File was recomputed successfully
                self.cache_manager.mark_accessed(virtual_path)
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
            # Fallback: query backends for non-cached entries (e.g., remote-only files)
            for backend_name, backend in self.backends.items():
                try:
                    st = backend.stat(path)
                    return {
                        'st_atime': getattr(st, 'st_atime', time.time()),
                        'st_ctime': getattr(st, 'st_ctime', getattr(st, 'st_mtime', time.time())),
                        'st_gid': getattr(st, 'st_gid', os.getgid()),
                        'st_mode': getattr(st, 'st_mode', stat.S_IFREG | 0o644),
                        'st_mtime': getattr(st, 'st_mtime', time.time()),
                        'st_nlink': getattr(st, 'st_nlink', 1),
                        'st_size': getattr(st, 'st_size', 0),
                        'st_uid': getattr(st, 'st_uid', os.getuid()),
                    }
                except Exception as be:
                    self.logger.debug("getattr backend fallback failed for %s on %s: %s", path, backend_name, be)

            raise FuseOSError(e.errno)

    def readdir(self, path, fh):
        """Read directory contents"""
        cache_path = self.cache_manager.get_cache_path(path)

        entries: Set[str] = {'.', '..'}

        # Get from cache
        if cache_path.exists() and cache_path.is_dir():
            entries.update(os.listdir(str(cache_path)))

        # Merge listings from backends; cache for fallback on connection issues
        with self._dir_cache_lock:
            dir_cache = self._dir_cache.setdefault(path, {})

            for backend_name, backend in self.backends.items():
                try:
                    if backend.isdir(path):
                        backend_entries = set(backend.listdir(path))
                    else:
                        backend_entries = set()
                    entries.update(backend_entries)
                    dir_cache[backend_name] = backend_entries
                except Exception as exc:
                    cached = dir_cache.get(backend_name, set())
                    if cached:
                        entries.update(cached)
                    self.logger.debug(
                        "readdir fallback for %s on backend %s due to %s", path, backend_name, exc
                    )

        for r in sorted(entries):
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

            # Trigger background sync if dirty
            metadata = self.state_manager.get_file_metadata(path)
            if metadata and metadata.sync_status == SyncStatus.DIRTY:
                self.sync_manager.queue_sync(path, priority=100)

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
        # Stop sync workers
        self.sync_manager.stop()

        # Close state manager
        self.state_manager.close()


def _acquire_mount_lock(lock_path: Path, mountpoint: str):
    """Take an exclusive, non-blocking lock to prevent duplicate mounts."""
    # Clean up stale lock files (pid not running)
    if lock_path.exists():
        try:
            existing_pid = int(lock_path.read_text().strip())
            # kill(pid, 0) checks for existence without signal delivery
            os.kill(existing_pid, 0)
        except (ValueError, ProcessLookupError, OSError):
            try:
                lock_path.unlink()
            except OSError:
                pass

    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT)
    lock_file = os.fdopen(fd, 'r+')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        try:
            existing = lock_file.read().strip()
        finally:
            lock_file.close()
        raise RuntimeError(
            f"Mount already active for {mountpoint}" + (f" (pid {existing})" if existing else "")
        )

    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(os.getpid()))
    lock_file.flush()
    return lock_file


def mount_storage(mountpoint: str, cache_root: Path, state_db: Path,
                 backends: Dict[str, Backend], cache_size: int,
                 foreground: bool = False, num_sync_workers: int = 2,
                 allow_other: Optional[bool] = None):
    """
    Mount a Storage filesystem

    Args:
        mountpoint: Where to mount the filesystem
        cache_root: Root directory for cache
        state_db: Path to state database
        backends: Dictionary of backends
        cache_size: Maximum cache size in bytes
        foreground: Run in foreground (default: False)
        num_sync_workers: Number of background sync workers
        allow_other: Allow other users to access the mount (requires root or fuse.conf setting).
                     If None, auto-detects based on whether running as root.

    Raises:
        PermissionError: If allow_other=True but not running as root and user_allow_other not set
    """
    logger = logging.getLogger(__name__)

    is_root = os.geteuid() == 0

    # Auto-detect allow_other if not specified
    if allow_other is None:
        allow_other = is_root
        if not is_root:
            logger.warning("Not running as root - mounting with allow_other=False (only you can access)")

    # Validate allow_other setting
    if allow_other and not is_root:
        # Check if user_allow_other is set in /etc/fuse.conf
        try:
            with open('/etc/fuse.conf', 'r') as f:
                fuse_conf = f.read()
                if 'user_allow_other' not in fuse_conf:
                    raise PermissionError(
                        "allow_other=True requires either root privileges or 'user_allow_other' "
                        "to be set in /etc/fuse.conf"
                    )
        except FileNotFoundError:
            raise PermissionError(
                "allow_other=True requires either root privileges or 'user_allow_other' "
                "to be set in /etc/fuse.conf (file not found)"
            )

    # Prevent duplicate mounts on the same cache/mountpoint
    mount_hash = hashlib.sha256(str(mountpoint).encode('utf-8')).hexdigest()[:12]
    lock_path = cache_root / f'.storage.mount.{mount_hash}.lock'
    try:
        lock_file = _acquire_mount_lock(lock_path, mountpoint)
        logger.info("Acquired mount lock at %s for %s", lock_path, mountpoint)
    except RuntimeError as e:
        logger.error(str(e))
        raise

    fs = StorageFS(cache_root, state_db, backends, cache_size, num_sync_workers, start_sync=False)
    try:
        FUSE(fs, mountpoint, nothreads=True, foreground=foreground, allow_other=allow_other)
    finally:
        try:
            lock_file.close()
        except Exception:
            pass
