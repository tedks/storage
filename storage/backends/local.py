"""Local filesystem storage-member implementation."""

import errno
import os
import shutil
import stat
from pathlib import Path
from typing import List, Optional

from .base import Backend, BackendType, StorageMemberCapabilities
from ..checksums import fast_checksum_file
from ..file_config import XATTR_CONFIG_NAME

# The mount's own state on a cache file (metadata, member baselines, stream
# state): never copied to a member, nor back from one. The config policy
# is the user's and travels with the file.
_INTERNAL_XATTR_PREFIX = "user.storage."


def _is_internal_xattr(name: str) -> bool:
    return name.startswith(_INTERNAL_XATTR_PREFIX) and name != XATTR_CONFIG_NAME


# Errors copy2 ignores per xattr: the name cannot be read or written here.
_XATTR_SKIP_ERRNOS = (errno.EPERM, errno.ENOTSUP, errno.ENODATA, errno.EINVAL)


def _listxattr(path) -> List[str]:
    try:
        return os.listxattr(path)
    except OSError as exc:
        if exc.errno in (errno.ENOTSUP, errno.ENODATA, errno.EINVAL):
            return []
        raise


def _copy_file(src: Path, dst: Path):
    """shutil.copy2 without the mount's internal xattrs.

    Bytes, xattrs, mode and times, in copy2's order: the xattrs go before
    the chmod, since changing a user xattr on a file needs write permission
    on it. Internal user.storage.* names are never copied. An existing
    destination keeps its inode, and so its xattrs: its user.* names are
    made to match the source's (internal ones an older copy2 left, and
    names the source no longer has, such as a config policy since removed);
    other namespaces (ACLs, security labels) are only copied over, as copy2
    does. Errors copy2 ignores per name are ignored; others raise.
    """
    shutil.copyfile(src, dst)
    st = os.stat(src)
    keep = {name for name in _listxattr(src) if not _is_internal_xattr(name)}
    for name in _listxattr(dst):
        if name.startswith("user.") and name not in keep:
            try:
                os.removexattr(dst, name)
            except OSError as exc:
                if exc.errno not in _XATTR_SKIP_ERRNOS:
                    raise
    for name in sorted(keep):
        try:
            os.setxattr(dst, name, os.getxattr(src, name))
        except OSError as exc:
            if exc.errno not in _XATTR_SKIP_ERRNOS:
                raise
    os.chmod(dst, stat.S_IMODE(st.st_mode))
    os.utime(dst, ns=(st.st_atime_ns, st.st_mtime_ns))


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
        super().__init__(
            name,
            BackendType.LOCAL,
            is_remote=False,
            speed_priority=speed_priority,
            capabilities=StorageMemberCapabilities(range_reads=True),
        )
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

    def listdir_attrs(self, virtual_path: str):
        """List directory with attributes using scandir for efficiency."""
        real_path = self._resolve_path(virtual_path)
        if not real_path.exists() or not real_path.is_dir():
            return []
        result = []
        with os.scandir(real_path) as it:
            for entry in it:
                try:
                    st = entry.stat(follow_symlinks=False)
                    result.append((entry.name, {
                        'st_mode': st.st_mode,
                        'st_size': st.st_size,
                        'st_mtime': st.st_mtime,
                        'st_atime': st.st_atime,
                        'st_ctime': st.st_ctime,
                        'st_gid': st.st_gid,
                        'st_uid': st.st_uid,
                        'st_nlink': st.st_nlink,
                    }))
                except FileNotFoundError:
                    # Entry disappeared between scandir and stat
                    continue
        return result

    def isdir(self, virtual_path: str) -> bool:
        """Check if path is a directory"""
        real_path = self._resolve_path(virtual_path)
        return real_path.exists() and real_path.is_dir()

    def copy_to_local(self, virtual_path: str, local_path: Path):
        """Copy a file from backend to local path"""
        real_path = self._resolve_path(virtual_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        _copy_file(real_path, local_path)

    def copy_from_local(self, local_path: Path, virtual_path: str):
        """Copy a local file to backend"""
        real_path = self._resolve_path(virtual_path)
        real_path.parent.mkdir(parents=True, exist_ok=True)
        _copy_file(local_path, real_path)

    def rename(self, old_virtual_path: str, new_virtual_path: str):
        """Rename a file or directory on the backend."""
        old_path = self._resolve_path(old_virtual_path)
        new_path = self._resolve_path(new_virtual_path)
        new_path.parent.mkdir(parents=True, exist_ok=True)
        if not old_path.exists():
            return
        os.replace(old_path, new_path)

    def get_free_space(self) -> Optional[int]:
        """Get free space in bytes"""
        try:
            stat = os.statvfs(self.root_path)
            return stat.f_bavail * stat.f_frsize
        except Exception:
            return None

    def stat(self, virtual_path: str) -> os.stat_result:
        return os.lstat(self._resolve_path(virtual_path))

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

    # ------------------------------------------------------------------ #
    # Optional capabilities used by conflict detection
    # ------------------------------------------------------------------ #
    def get_fast_checksum(self, virtual_path: str) -> str:
        """Compute checksum directly from the real file without caching."""
        real_path = self._resolve_path(virtual_path)
        return fast_checksum_file(real_path)

    def set_xattr(self, virtual_path: str, name: str, value: bytes):
        try:
            os.setxattr(str(self._resolve_path(virtual_path)), name, value)
        except OSError:
            # Best effort; some filesystems may not support xattrs
            pass

    def get_xattr(self, virtual_path: str, name: str):
        try:
            return os.getxattr(str(self._resolve_path(virtual_path)), name)
        except OSError:
            return None
