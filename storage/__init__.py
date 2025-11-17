"""
Storage - A meta-filesystem that multiplexes over multiple backing filesystems

Storage provides a unified filesystem view backed by multiple local and remote
filesystems with:
- Local cache with LRU management
- N-way replication
- Background sync
- Transparent encryption
- File recomputation
- Conflict resolution
"""

__version__ = '0.2.0'
