"""
Storage member abstraction layer for Storage.

Supports multiple transports beneath one logical storage fabric:
- Local filesystems
- SSH/SFTP remote filesystems
- Rsync-over-SSH remote filesystems (rsync for transfers, SSH for metadata)
"""

from .base import Backend, BackendType, BackendInfo, StorageMemberCapabilities, WatcherSpec
from .local import LocalBackend
from .ssh import SSHBackend
from .rsync import RsyncBackend

__all__ = [
    'Backend',
    'BackendType',
    'BackendInfo',
    'StorageMemberCapabilities',
    'WatcherSpec',
    'LocalBackend',
    'SSHBackend',
    'RsyncBackend',
]
