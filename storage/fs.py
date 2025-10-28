"""
FUSE filesystem implementation for Storage
Main filesystem layer that multiplexes over backing filesystems
"""

import os
import errno
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from pathlib import Path
from typing import Optional

from .backing import BackingManager
from .xattr import XAttrManager, StorageMetadata


class StorageFS(LoggingMixIn, Operations):
    """
    FUSE filesystem that multiplexes over multiple backing filesystems
    """

    def __init__(self, backing_manager: BackingManager):
        self.backing_manager = backing_manager
        self.xattr_manager = XAttrManager()

    def _resolve_backing_path(self, path: str) -> Path:
        """
        Resolve a virtual path to a real path on a backing filesystem
        Uses metadata if available, otherwise uses default filesystem
        """
        # Get metadata for this path (may inherit from parent)
        default_backing = self.backing_manager.get_default_filesystem()
        if not default_backing:
            raise FuseOSError(errno.EIO)

        real_path = default_backing.get_real_path(path)

        # Try to get metadata to determine which backing filesystem to use
        if real_path.exists():
            metadata = self.xattr_manager.get_inherited_metadata(real_path)
            if metadata and metadata.preferred_filesystem:
                backing = self.backing_manager.get_filesystem(metadata.preferred_filesystem)
                if backing:
                    real_path = backing.get_real_path(path)

        return real_path

    # Filesystem methods

    def getattr(self, path, fh=None):
        """Get file attributes"""
        real_path = self._resolve_backing_path(path)
        try:
            st = os.lstat(str(real_path))
            return dict((key, getattr(st, key)) for key in (
                'st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime',
                'st_nlink', 'st_size', 'st_uid'))
        except OSError as e:
            raise FuseOSError(e.errno)

    def readdir(self, path, fh):
        """Read directory contents"""
        real_path = self._resolve_backing_path(path)
        dirents = ['.', '..']

        if real_path.exists() and real_path.is_dir():
            dirents.extend(os.listdir(str(real_path)))

        for r in dirents:
            yield r

    def mkdir(self, path, mode):
        """Create a directory"""
        real_path = self._resolve_backing_path(path)
        try:
            os.mkdir(str(real_path), mode)
        except OSError as e:
            raise FuseOSError(e.errno)

    def rmdir(self, path):
        """Remove a directory"""
        real_path = self._resolve_backing_path(path)
        try:
            os.rmdir(str(real_path))
        except OSError as e:
            raise FuseOSError(e.errno)

    def unlink(self, path):
        """Remove a file"""
        real_path = self._resolve_backing_path(path)
        try:
            os.unlink(str(real_path))
        except OSError as e:
            raise FuseOSError(e.errno)

    def rename(self, old, new):
        """Rename a file or directory"""
        old_real = self._resolve_backing_path(old)
        new_real = self._resolve_backing_path(new)
        try:
            os.rename(str(old_real), str(new_real))
        except OSError as e:
            raise FuseOSError(e.errno)

    def chmod(self, path, mode):
        """Change file mode"""
        real_path = self._resolve_backing_path(path)
        try:
            os.chmod(str(real_path), mode)
        except OSError as e:
            raise FuseOSError(e.errno)

    def chown(self, path, uid, gid):
        """Change file owner"""
        real_path = self._resolve_backing_path(path)
        try:
            os.chown(str(real_path), uid, gid)
        except OSError as e:
            raise FuseOSError(e.errno)

    def truncate(self, path, length, fh=None):
        """Truncate a file"""
        real_path = self._resolve_backing_path(path)
        try:
            with open(str(real_path), 'r+') as f:
                f.truncate(length)
        except OSError as e:
            raise FuseOSError(e.errno)

    # File operations

    def open(self, path, flags):
        """Open a file"""
        real_path = self._resolve_backing_path(path)
        try:
            return os.open(str(real_path), flags)
        except OSError as e:
            raise FuseOSError(e.errno)

    def create(self, path, mode, fi=None):
        """Create a file"""
        real_path = self._resolve_backing_path(path)
        try:
            return os.open(str(real_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
        except OSError as e:
            raise FuseOSError(e.errno)

    def read(self, path, size, offset, fh):
        """Read from a file"""
        try:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.read(fh, size)
        except OSError as e:
            raise FuseOSError(e.errno)

    def write(self, path, data, offset, fh):
        """Write to a file"""
        try:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.write(fh, data)
        except OSError as e:
            raise FuseOSError(e.errno)

    def release(self, path, fh):
        """Release (close) a file"""
        try:
            return os.close(fh)
        except OSError as e:
            raise FuseOSError(e.errno)

    def fsync(self, path, datasync, fh):
        """Sync file to disk"""
        try:
            if datasync:
                os.fdatasync(fh)
            else:
                os.fsync(fh)
        except OSError as e:
            raise FuseOSError(e.errno)

    # Extended attributes

    def getxattr(self, path, name, position=0):
        """Get extended attribute"""
        real_path = self._resolve_backing_path(path)
        try:
            return os.getxattr(str(real_path), name)
        except OSError as e:
            raise FuseOSError(e.errno)

    def setxattr(self, path, name, value, options, position=0):
        """Set extended attribute"""
        real_path = self._resolve_backing_path(path)
        try:
            os.setxattr(str(real_path), name, value)
        except OSError as e:
            raise FuseOSError(e.errno)

    def listxattr(self, path):
        """List extended attributes"""
        real_path = self._resolve_backing_path(path)
        try:
            return os.listxattr(str(real_path))
        except OSError as e:
            raise FuseOSError(e.errno)

    def removexattr(self, path, name):
        """Remove extended attribute"""
        real_path = self._resolve_backing_path(path)
        try:
            os.removexattr(str(real_path), name)
        except OSError as e:
            raise FuseOSError(e.errno)

    # Symbolic links

    def readlink(self, path):
        """Read symbolic link"""
        real_path = self._resolve_backing_path(path)
        try:
            target = os.readlink(str(real_path))
            return target
        except OSError as e:
            raise FuseOSError(e.errno)

    def symlink(self, target, source):
        """Create symbolic link"""
        real_path = self._resolve_backing_path(source)
        try:
            os.symlink(target, str(real_path))
        except OSError as e:
            raise FuseOSError(e.errno)

    def link(self, target, source):
        """Create hard link"""
        target_real = self._resolve_backing_path(target)
        source_real = self._resolve_backing_path(source)
        try:
            os.link(str(target_real), str(source_real))
        except OSError as e:
            raise FuseOSError(e.errno)

    # Time operations

    def utimens(self, path, times=None):
        """Update file timestamps"""
        real_path = self._resolve_backing_path(path)
        try:
            os.utime(str(real_path), times)
        except OSError as e:
            raise FuseOSError(e.errno)


def mount_storage(mountpoint: str, backing_manager: BackingManager, foreground: bool = False):
    """
    Mount a Storage filesystem

    Args:
        mountpoint: Where to mount the filesystem
        backing_manager: Configured backing filesystem manager
        foreground: Run in foreground (default: False)
    """
    fs = StorageFS(backing_manager)
    FUSE(fs, mountpoint, nothreads=True, foreground=foreground, allow_other=True)
