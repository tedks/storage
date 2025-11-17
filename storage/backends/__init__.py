"""
Backend abstraction layer for Storage

Supports multiple types of backing filesystems:
- Local filesystems
- SSH/SFTP remote filesystems
- Other remote storage (future: S3, rsync, etc.)
"""

from .base import Backend, BackendType, BackendInfo
from .local import LocalBackend
from .ssh import SSHBackend

__all__ = ['Backend', 'BackendType', 'BackendInfo', 'LocalBackend', 'SSHBackend']
