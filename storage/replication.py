"""
Replication manager for Storage

Handles N-way replication of files across multiple backing filesystems.
Supports hierarchical configuration where child directories inherit parent
configurations with maximum replica count.
"""

import hashlib
import time
from typing import List, Dict, Optional, Set
from pathlib import Path

from .backends.base import Backend
from .state import StateManager, FileConfig, FileMetadata, FileLocation, SyncStatus


class ReplicationManager:
    """
    Manages file replication across backends

    Handles:
    - Selecting which backends to use for a file
    - N-way replication (including -1 for all backends)
    - Preference for faster/local backends
    - Respecting user-configured preferences
    """

    def __init__(self, state_manager: StateManager, backends: Dict[str, Backend]):
        """
        Initialize replication manager

        Args:
            state_manager: State manager instance
            backends: Dictionary of backend name -> Backend instance
        """
        self.state_manager = state_manager
        self.backends = backends

    def get_available_backends(self) -> List[Backend]:
        """Get list of currently available backends"""
        return [b for b in self.backends.values() if b.is_available()]

    def select_backends_for_file(self, virtual_path: str, config: FileConfig) -> List[Backend]:
        """
        Select which backends should store a file

        Args:
            virtual_path: Virtual path of file
            config: File configuration

        Returns:
            List of backends to use for this file
        """
        available = self.get_available_backends()

        if not available:
            return []

        # Determine replica count
        replica_count = config.replica_count

        if replica_count == -1:
            # Replicate to all backends
            return available

        if replica_count <= 0:
            replica_count = 1

        # Start with preferred backends if specified
        selected = []

        if config.preferred_backends:
            for name in config.preferred_backends:
                if name in self.backends and self.backends[name].is_available():
                    selected.append(self.backends[name])
                    if len(selected) >= replica_count:
                        return selected

        # Use consistent hashing to deterministically select backends
        # This ensures the same file always maps to the same backends
        # (assuming backend list doesn't change)
        path_hash = hashlib.md5(virtual_path.encode()).hexdigest()
        hash_int = int(path_hash, 16)

        # Sort available backends by (speed_priority, name) for consistency
        available_sorted = sorted(available, key=lambda b: (b.speed_priority, b.name))

        # Select backends using hash-based rotation
        while len(selected) < replica_count and len(selected) < len(available_sorted):
            idx = (hash_int + len(selected)) % len(available_sorted)
            backend = available_sorted[idx]

            if backend not in selected:
                selected.append(backend)

        return selected

    def get_backend_for_read(self, virtual_path: str) -> Optional[Backend]:
        """
        Select best backend to read a file from

        Prefers:
        1. Local backends over remote
        2. Faster backends over slower

        Args:
            virtual_path: Virtual path of file

        Returns:
            Backend to read from, or None if file not found
        """
        locations = self.state_manager.get_file_locations(virtual_path)

        if not locations:
            return None

        # Get backends that have this file and are available
        available_backends = []
        for location in locations:
            if location.backend_name in self.backends:
                backend = self.backends[location.backend_name]
                if backend.is_available() and backend.exists(virtual_path):
                    available_backends.append(backend)

        if not available_backends:
            return None

        # Sort by speed priority (lower is faster)
        available_backends.sort(key=lambda b: b.speed_priority)

        return available_backends[0]

    def replicate_file(self, virtual_path: str, source_backend: Backend,
                      target_backends: List[Backend]) -> Dict[str, bool]:
        """
        Replicate a file from source to target backends

        Args:
            virtual_path: Virtual path of file
            source_backend: Backend to copy from
            target_backends: Backends to copy to

        Returns:
            Dictionary of backend_name -> success status
        """
        results = {}

        if not source_backend.exists(virtual_path):
            return results

        # Get file checksum from source
        try:
            # Create temporary path for reading
            temp_path = Path(f"/tmp/storage_repl_{hash(virtual_path)}")

            # Copy from source to temp
            source_backend.copy_to_local(virtual_path, temp_path)
            checksum = self.state_manager.compute_checksum(temp_path)

            # Replicate to each target
            for target in target_backends:
                if target == source_backend:
                    results[target.name] = True
                    continue

                try:
                    # Copy to target
                    target.copy_from_local(temp_path, virtual_path)
                    target.sync()

                    # Record location
                    location = FileLocation(
                        virtual_path=virtual_path,
                        backend_name=target.name,
                        checksum=checksum,
                        last_synced=time.time(),
                        is_cached=False
                    )
                    self.state_manager.add_file_location(location)

                    results[target.name] = True

                except Exception as e:
                    print(f"Error replicating {virtual_path} to {target.name}: {e}")
                    results[target.name] = False

            # Clean up temp file
            if temp_path.exists():
                temp_path.unlink()

        except Exception as e:
            print(f"Error reading {virtual_path} from {source_backend.name}: {e}")

        return results

    def check_replication_status(self, virtual_path: str, config: FileConfig) -> bool:
        """
        Check if file is properly replicated according to its configuration

        Args:
            virtual_path: Virtual path of file
            config: File configuration

        Returns:
            True if file meets replication requirements, False otherwise
        """
        locations = self.state_manager.get_file_locations(virtual_path)
        required_backends = self.select_backends_for_file(virtual_path, config)

        if config.replica_count == -1:
            # Should be on all available backends
            available = self.get_available_backends()
            return len(locations) >= len(available)

        # Should be on at least replica_count backends
        return len(locations) >= config.replica_count

    def sync_file_to_backends(self, virtual_path: str, cache_path: Path,
                              target_backends: List[Backend]) -> Dict[str, bool]:
        """
        Sync a file from cache to target backends

        Args:
            virtual_path: Virtual path of file
            cache_path: Path to file in cache
            target_backends: Backends to sync to

        Returns:
            Dictionary of backend_name -> success status
        """
        results = {}

        if not cache_path.exists():
            return results

        # Compute checksum
        checksum = self.state_manager.compute_checksum(cache_path)

        for backend in target_backends:
            try:
                # Copy to backend
                backend.copy_from_local(cache_path, virtual_path)
                backend.sync()

                # Record location
                location = FileLocation(
                    virtual_path=virtual_path,
                    backend_name=backend.name,
                    checksum=checksum,
                    last_synced=time.time(),
                    is_cached=False
                )
                self.state_manager.add_file_location(location)

                results[backend.name] = True

            except Exception as e:
                print(f"Error syncing {virtual_path} to {backend.name}: {e}")
                results[backend.name] = False

        return results

    def find_over_replicated_files(self) -> List[str]:
        """
        Find files that are replicated more than required

        Returns:
            List of virtual paths of over-replicated files
        """
        # This would require iterating through all files
        # For now, return empty list (to be implemented when needed)
        return []

    def get_slowest_backend_for_file(self, virtual_path: str) -> Optional[Backend]:
        """
        Get the slowest backend that has this file

        Useful for space management - can delete from slow backends first

        Args:
            virtual_path: Virtual path of file

        Returns:
            Slowest backend that has the file, or None
        """
        locations = self.state_manager.get_file_locations(virtual_path)

        backends_with_file = []
        for location in locations:
            if location.backend_name in self.backends:
                backend = self.backends[location.backend_name]
                if backend.is_available():
                    backends_with_file.append(backend)

        if not backends_with_file:
            return None

        # Sort by speed priority (higher is slower)
        backends_with_file.sort(key=lambda b: b.speed_priority, reverse=True)

        return backends_with_file[0]
