"""
Backend abstraction layer for Storage

Supports multiple types of backing filesystems:
- Local filesystems
- SSH/SFTP remote filesystems
- Rsync-over-SSH remote filesystems (rsync for transfers, SSH for metadata)
"""

from .base import Backend, BackendType, BackendInfo
from .local import LocalBackend
from .ssh import SSHBackend
from .rsync import RsyncBackend

__all__ = ['Backend', 'BackendType', 'BackendInfo', 'LocalBackend', 'SSHBackend', 'RsyncBackend']
