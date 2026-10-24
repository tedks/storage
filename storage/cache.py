"""
Local cache manager with LRU eviction

Manages the local cache of files from remote backing filesystems.
Files are cached on first access and evicted using LRU when space is needed.
"""

import os
import time
import threading
from pathlib import Path
from contextlib import nullcontext
from typing import Callable, ContextManager, Optional, List, Set

from .backends.base import Backend
from .cache_metadata import write_cache_metadata
from .models import FileMetadata, SyncStatus
from .state import StateManager


class CacheManager:
    """
    Manages local cache with LRU eviction

    The cache stores complete files downloaded from remote backends.
    Access (read or write) updates the LRU timestamp.
    """

    def __init__(self, cache_root: Path, state_manager: StateManager, max_size_bytes: int):
        """
        Initialize cache manager

        Args:
            cache_root: Root directory for cache storage
            state_manager: State manager instance
            max_size_bytes: Maximum cache size in bytes
        """
        self.cache_root = cache_root
        # Set by StorageFS: a context manager that holds the descriptor
        # table lock and yields whether the path has an open descriptor.
        # Eviction stats and unlinks inside it, so no descriptor can be
        # opened on the file in between, and none open can be missed: bytes
        # written through one after the stat would die with the inode.
        # None (no StorageFS attached: tests, CLI tools) means no descriptors.
        self.descriptor_guard: Optional[Callable[[str], ContextManager[bool]]] = None
        # Set by StorageFS: NamespaceService.forget_listed_attrs. Called
        # before a cached file is evicted and when one is recorded dirty, so
        # getattr never answers for a vanished copy from a directory listing
        # that may predate its last write. It takes only a leaf lock and is
        # safe under every lock here. None (no StorageFS) means no listings.
        self.forget_listed_attrs: Optional[Callable[[str], None]] = None
        self.state_manager = state_manager
        self.max_size_bytes = max_size_bytes
        self._lock = threading.RLock()

        # Ensure cache root exists
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self._current_size_bytes = self._scan_total_cache_size()
        self._size_dirty = False

    def _scan_total_cache_size(self) -> int:
        """Calculate total size of cache in bytes by scanning the filesystem."""
        total = 0
        for root, dirs, files in os.walk(self.cache_root):
            for name in files:
                filepath = Path(root) / name
                try:
                    total += filepath.stat().st_size
                except OSError:
                    pass
        return total

    def _ensure_size_current_locked(self):
        """Refresh tracked size if it has been marked dirty by direct writes."""
        if self._size_dirty:
            self._current_size_bytes = self._scan_total_cache_size()
            self._size_dirty = False

    def get_cache_path(self, virtual_path: str) -> Path:
        """Get the local cache path for a virtual file"""
        # Remove leading slash and resolve within cache
        relative_path = virtual_path.lstrip('/')
        return self.cache_root / relative_path

    def is_cached(self, virtual_path: str) -> bool:
        """Check if a file is in the cache"""
        cache_path = self.get_cache_path(virtual_path)
        return cache_path.exists() and cache_path.is_file()

    def get_cached_size(self, virtual_path: str) -> int:
        """Get size of cached file"""
        cache_path = self.get_cache_path(virtual_path)
        if cache_path.exists():
            return cache_path.stat().st_size
        return 0

    def get_total_cache_size(self) -> int:
        """Calculate total size of cache in bytes"""
        with self._lock:
            self._ensure_size_current_locked()
            return self._current_size_bytes

    def mark_cache_size_dirty(self):
        """Mark cache size accounting as stale after direct external file mutations."""
        with self._lock:
            self._size_dirty = True

    def get_free_space_needed(self, required_bytes: int) -> int:
        """
        Calculate how much space needs to be freed

        Returns:
            Bytes to free (0 if enough space available)
        """
        with self._lock:
            self._ensure_size_current_locked()
            available = self.max_size_bytes - self._current_size_bytes

        if available >= required_bytes:
            return 0

        return required_bytes - available

    def evict_lru_files(self, bytes_needed: int) -> bool:
        """
        Evict least recently used files to free space

        Only evicts files that are:
        - Clean (fully synced)
        - Not recomputable (those handled separately)

        Args:
            bytes_needed: Amount of space to free in bytes

        Returns:
            True if enough space was freed, False otherwise
        """
        with self._lock:
            freed = 0
            lru_files = self.state_manager.get_lru_files(limit=1000)

            for metadata in lru_files:
                if freed >= bytes_needed:
                    break

                # Skip files that aren't clean
                if metadata.sync_status != SyncStatus.CLEAN:
                    continue

                # Skip recomputable files (handled separately)
                if metadata.recomputable:
                    continue

                # Evict this file
                cache_path = self.get_cache_path(metadata.virtual_path)
                if cache_path.exists():
                    try:
                        freed += self._evict_if_still_clean(metadata.virtual_path, cache_path)
                    except OSError as e:
                        print(f"Warning: Failed to evict {metadata.virtual_path}: {e}")

            return freed >= bytes_needed

    def evict_recomputable_files(self, bytes_needed: int) -> bool:
        """
        Evict recomputable files to free space

        Args:
            bytes_needed: Amount of space to free in bytes

        Returns:
            True if enough space was freed, False otherwise
        """
        with self._lock:
            freed = 0
            recomputable = self.state_manager.get_recomputable_files()

            for metadata in recomputable:
                if freed >= bytes_needed:
                    break

                cache_path = self.get_cache_path(metadata.virtual_path)
                if cache_path.exists():
                    try:
                        file_size = cache_path.stat().st_size
                        # Truncate to preserve xattrs
                        with open(cache_path, 'wb') as f:
                            f.truncate(0)
                        freed += file_size
                        self._current_size_bytes = max(0, self._current_size_bytes - file_size)

                        # Update metadata to show file needs recomputation
                        metadata.size = 0
                        metadata.checksum = None
                        self.store_file_metadata(metadata, cache_path=cache_path)

                    except OSError as e:
                        print(f"Warning: Failed to evict recomputable {metadata.virtual_path}: {e}")

            return freed >= bytes_needed

    def ensure_space(self, bytes_needed: int) -> bool:
        """
        Ensure enough cache space is available

        Attempts eviction in this order:
        1. LRU non-recomputable files
        2. Recomputable files (smallest first)

        Args:
            bytes_needed: Amount of space needed in bytes

        Returns:
            True if space was made available, False if insufficient space
        """
        space_needed = self.get_free_space_needed(bytes_needed)

        if space_needed <= 0:
            return True  # Already have enough space

        # Try evicting LRU files first
        if self.evict_lru_files(space_needed):
            return True

        # Try evicting recomputable files
        if self.evict_recomputable_files(space_needed):
            return True

        # Could not free enough space
        return False

    def cache_file(self, virtual_path: str, backend: Backend, source_virtual_path: str = None) -> bool:
        """
        Download a file from backend into cache

        Args:
            virtual_path: Virtual path of file
            backend: Backend to download from
            source_virtual_path: Virtual path to read from backend (defaults to virtual_path)

        Returns:
            True if successfully cached, False otherwise
        """
        source_virtual_path = source_virtual_path or virtual_path
        with self._lock:
            cache_path = self.get_cache_path(virtual_path)
            existing_size = cache_path.stat().st_size if cache_path.exists() else 0

            # Check if backend has the file
            if not backend.exists(source_virtual_path):
                return False

            # Get file size to ensure we have space
            file_size = backend.get_size(source_virtual_path)

            # Ensure we have space
            if not self.ensure_space(file_size):
                print(f"Warning: Insufficient cache space for {virtual_path}")
                return False

            # Download file
            try:
                backend.copy_to_local(source_virtual_path, cache_path)
                new_size = cache_path.stat().st_size if cache_path.exists() else 0
                self._current_size_bytes += (new_size - existing_size)
                self._size_dirty = False

                # Update access time in state
                self.state_manager.update_access_time(virtual_path)

                return True

            except Exception as e:
                self._size_dirty = True
                print(f"Error caching {virtual_path} from {backend.name}: {e}")
                return False

    def mark_accessed(self, virtual_path: str):
        """Mark a file as accessed (update LRU)"""
        self.state_manager.update_access_time(virtual_path)

    def mark_file_dirty(self, virtual_path: str, cache_path: Path) -> Optional[int]:
        """Record a modification of a cached file: DIRTY, its size and mtime, a new generation.

        One UPDATE on the index row, so a sync in flight for the old content
        sees the generation move and does not report it as synced. The stat,
        the update and the mirror of the row into the cache file's metadata
        xattr run under the index lock, so concurrent writers record their
        stats in update order and mirror their rows in the same order.
        The path's listing attributes are forgotten first: they describe the
        member copy, which no longer matches. Returns the new generation, or
        None when the path has no row.
        """
        with self.state_manager.metadata_lock:
            if self.forget_listed_attrs:
                self.forget_listed_attrs(virtual_path)
            generation = self.state_manager.mark_dirty_from_cache_file(virtual_path, cache_path)
            if generation is None:
                return None
            metadata = self.state_manager.get_file_metadata(virtual_path)
            if metadata:
                self.sync_cache_metadata(virtual_path, metadata, cache_path=cache_path)
        return generation

    def _evict_if_still_clean(self, virtual_path: str, cache_path: Path) -> int:
        """Unlink a cached file if its row is still CLEAN and describes the file.

        The LRU list was read earlier; by now a write or the post-crash
        sweep may have made the row DIRTY, or a write's row update may have
        been lost (a CLEAN row older than the file). Both mean the file is
        the only copy of something. The re-check and the unlink are one
        critical section under the index lock, which every mark_file_dirty
        also takes, so no write can be recorded between them. The stat and
        the unlink also sit inside descriptor_guard, which holds the
        descriptor table lock: a path with an open descriptor is skipped,
        and none can be opened on the file before the unlink (open() takes
        the same lock across its os.open and registration), since bytes
        written through one after the stat would die with the unlinked
        inode. A stale CLEAN is marked and kept. The listing attributes for
        the path are forgotten before the unlink (forget_listed_attrs). Lock
        order: the caller's
        self._lock, the index lock, then the descriptor lock; nothing under
        the descriptor lock takes the other two. Returns the bytes freed.
        """
        with self.state_manager.metadata_lock:
            row = self.state_manager.get_file_metadata(virtual_path)
            if row is None or row.sync_status != SyncStatus.CLEAN:
                return 0
            guard = self.descriptor_guard(virtual_path) if self.descriptor_guard else nullcontext(False)
            with guard as is_open:
                if is_open:
                    return 0
                st = os.lstat(str(cache_path))
                stale = st.st_size != row.size or st.st_mtime != row.last_modified
                if not stale:
                    if self.forget_listed_attrs:
                        self.forget_listed_attrs(virtual_path)
                    cache_path.unlink()
            if stale:
                self.mark_file_dirty(virtual_path, cache_path)
                return 0
            self._current_size_bytes = max(0, self._current_size_bytes - st.st_size)
            self.state_manager.remove_file_location(virtual_path, "cache")
            return st.st_size

    def heal_stale_clean(
        self,
        virtual_path: str,
        cache_path: Path,
        *,
        stat_size: Optional[int] = None,
        stat_mtime: Optional[float] = None,
    ) -> bool:
        """Mark a CLEAN row DIRTY when the cache file was written after it.

        The row's size and mtime are the stat recorded by the last
        mark_file_dirty (or the last fetch). A CLEAN row whose stat differs
        from the file means a write landed and its row update was lost (a
        crash between the two), or something wrote to the cache directory
        behind the mount; in that second case a sync may publish CLEAN for
        a half-written file and the next open heals it again. One row read
        and one lstat, unless the caller passes a stat it already has. The
        read and the stat are not under the index lock: a writer racing
        them bumps the generation, and mark_file_dirty's own bump keeps the
        row at the newer write. Returns True when the file was marked.
        """
        row = self.state_manager.get_file_metadata(virtual_path)
        if row is None or row.sync_status != SyncStatus.CLEAN:
            return False
        if stat_size is None or stat_mtime is None:
            try:
                st = os.lstat(str(cache_path))
            except OSError:
                return False
            stat_size, stat_mtime = st.st_size, st.st_mtime
        if stat_size == row.size and stat_mtime == row.last_modified:
            return False
        return self.mark_file_dirty(virtual_path, cache_path) is not None

    def store_file_metadata(
        self,
        metadata: FileMetadata,
        *,
        cache_path: Optional[Path] = None,
    ) -> Optional[FileMetadata]:
        """Store a metadata snapshot and mirror the row as stored into the xattr.

        set_file_metadata keeps content columns a write recorded after the
        snapshot was read, so the xattr is written from a re-read of the row,
        never from the caller's object, and the write, re-read and mirror are
        one critical section under the index lock so no other writer's
        mirror can land between them. Returns the stored row.
        """
        with self.state_manager.metadata_lock:
            self.state_manager.set_file_metadata(metadata)
            stored = self.state_manager.get_file_metadata(metadata.virtual_path)
            if stored is not None:
                self.sync_cache_metadata(metadata.virtual_path, stored, cache_path=cache_path)
        return stored

    def sync_cache_metadata(
        self,
        virtual_path: str,
        metadata: FileMetadata,
        *,
        cache_path: Optional[Path] = None,
    ) -> bool:
        """Mirror file metadata into the cache file xattr when the cache entry exists."""
        target_path = cache_path or self.get_cache_path(virtual_path)
        if not target_path.exists() or not target_path.is_file():
            return False

        try:
            write_cache_metadata(target_path, metadata, virtual_path=virtual_path)
            return True
        except OSError:
            return False

    def flush_file(self, virtual_path: str):
        """Flush a cached file to ensure writes are synced"""
        cache_path = self.get_cache_path(virtual_path)
        if cache_path.exists():
            try:
                # Open and sync file
                with open(cache_path, 'rb') as f:
                    os.fsync(f.fileno())
            except OSError:
                pass

    def create_cached_file(self, virtual_path: str) -> Path:
        """
        Create a new file in cache

        Args:
            virtual_path: Virtual path of file

        Returns:
            Path to created cache file
        """
        with self._lock:
            cache_path = self.get_cache_path(virtual_path)
            existing_size = cache_path.stat().st_size if cache_path.exists() else 0
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.touch()
            new_size = cache_path.stat().st_size if cache_path.exists() else 0
            self._current_size_bytes += (new_size - existing_size)
            self._size_dirty = False

            # Mark as accessed
            self.mark_accessed(virtual_path)

            return cache_path

    def delete_cached_file(self, virtual_path: str):
        """Delete a file from cache"""
        with self._lock:
            cache_path = self.get_cache_path(virtual_path)
            if cache_path.exists():
                try:
                    file_size = cache_path.stat().st_size
                    cache_path.unlink()
                    self._current_size_bytes = max(0, self._current_size_bytes - file_size)
                    self._size_dirty = False
                except OSError as e:
                    self._size_dirty = True
                    print(f"Warning: Failed to delete cache file {virtual_path}: {e}")
