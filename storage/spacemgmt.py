"""
Advanced space management for Storage

Handles:
- Defragmentation (moving files between backends)
- Over-replication cleanup
- Automatic space reclamation
"""

import logging
from typing import List, Dict, Optional
from pathlib import Path

from .state import StateManager, FileMetadata, SyncStatus
from .backends.base import Backend
from .replication import ReplicationManager
from .cache import CacheManager

logger = logging.getLogger(__name__)


class SpaceManager:
    """
    Manages space across backends

    Handles defragmentation, over-replication cleanup, and space reclamation.
    """

    def __init__(self, state_manager: StateManager, cache_manager: CacheManager,
                 replication_manager: ReplicationManager, backends: Dict[str, Backend]):
        """
        Initialize space manager

        Args:
            state_manager: State manager instance
            cache_manager: Cache manager instance
            replication_manager: Replication manager instance
            backends: Dictionary of backends
        """
        self.state_manager = state_manager
        self.cache_manager = cache_manager
        self.replication_manager = replication_manager
        self.backends = backends

    def get_backend_usage(self) -> Dict[str, Dict]:
        """
        Get space usage statistics for each backend

        Returns:
            Dictionary of backend_name -> {free: bytes, total: bytes}
        """
        usage = {}

        for name, backend in self.backends.items():
            free = backend.get_free_space()
            usage[name] = {
                'free': free,
                'available': backend.is_available(),
                'speed_priority': backend.speed_priority,
                'is_remote': backend.is_remote
            }

        return usage

    def find_over_replicated_files(self) -> List[FileMetadata]:
        """
        Find files that have more replicas than required

        Returns:
            List of file metadata for over-replicated files
        """
        over_replicated = []

        # This would require iterating all files
        # For now, return empty list
        # TODO: Implement efficient over-replication detection

        return over_replicated

    def cleanup_over_replication(self, dry_run: bool = True) -> int:
        """
        Remove excess replicas from over-replicated files

        Args:
            dry_run: If True, only report what would be done

        Returns:
            Number of replicas that would be/were removed
        """
        removed = 0

        # Find over-replicated files
        over_replicated = self.find_over_replicated_files()

        for metadata in over_replicated:
            # Get current locations
            locations = self.state_manager.get_file_locations(metadata.virtual_path)

            # Get required replica count
            cache_path = self.cache_manager.get_cache_path(metadata.virtual_path)
            config = self.state_manager.get_inherited_config(cache_path)

            required = config.replica_count
            if required == -1:
                # All backends - skip
                continue

            if len(locations) > required:
                # Over-replicated - remove from slowest backend
                excess = len(locations) - required

                # Sort by speed priority (higher = slower)
                sorted_locations = sorted(
                    locations,
                    key=lambda l: self.backends[l.backend_name].speed_priority
                    if l.backend_name in self.backends else 999,
                    reverse=True
                )

                for i in range(excess):
                    location = sorted_locations[i]

                    if dry_run:
                        logger.info(f"Would remove {metadata.virtual_path} from {location.backend_name}")
                    else:
                        # Remove from backend
                        backend = self.backends.get(location.backend_name)
                        if backend:
                            try:
                                backend.delete_file(metadata.virtual_path)
                                self.state_manager.remove_file_location(
                                    metadata.virtual_path,
                                    location.backend_name
                                )
                                logger.info(f"Removed {metadata.virtual_path} from {location.backend_name}")
                            except Exception as e:
                                logger.error(f"Error removing {metadata.virtual_path} from {location.backend_name}: {e}")

                    removed += 1

        return removed

    def defragment(self, source_backend: str, target_backend: str, bytes_to_move: int,
                   dry_run: bool = True) -> int:
        """
        Move files from one backend to another to free space

        Args:
            source_backend: Backend to move files from
            target_backend: Backend to move files to
            bytes_to_move: Amount of data to move
            dry_run: If True, only report what would be done

        Returns:
            Bytes actually moved (or would be moved if dry_run)
        """
        moved = 0

        if source_backend not in self.backends or target_backend not in self.backends:
            logger.error("Invalid backend names")
            return 0

        source = self.backends[source_backend]
        target = self.backends[target_backend]

        # Find files on source backend
        # For now, this is simplified - would need proper tracking
        # TODO: Implement efficient file location queries

        logger.info(f"Defragmentation from {source_backend} to {target_backend}")

        if dry_run:
            logger.info(f"Would move {bytes_to_move} bytes")
        else:
            logger.info(f"Moving {bytes_to_move} bytes")

        return moved

    def reclaim_space(self, bytes_needed: int, prefer_recomputable: bool = True) -> bool:
        """
        Reclaim space by deleting recomputable files and over-replicated files

        Args:
            bytes_needed: Amount of space needed
            prefer_recomputable: Try recomputable files first

        Returns:
            True if enough space was reclaimed
        """
        freed = 0

        if prefer_recomputable:
            # Try evicting recomputable files from cache first
            if self.cache_manager.evict_recomputable_files(bytes_needed):
                return True

        # Try cleaning up over-replication
        # This would remove files from backends, not cache
        # TODO: Implement backend-specific over-replication cleanup

        return freed >= bytes_needed

    def get_space_report(self) -> Dict:
        """
        Generate a space usage report

        Returns:
            Dictionary with space usage statistics
        """
        backend_usage = self.get_backend_usage()

        cache_size = self.cache_manager.get_total_cache_size()
        cache_max = self.cache_manager.max_size_bytes

        report = {
            'cache': {
                'used': cache_size,
                'max': cache_max,
                'available': cache_max - cache_size,
                'usage_percent': (cache_size / cache_max * 100) if cache_max > 0 else 0
            },
            'backends': backend_usage,
            'total_backends': len(self.backends),
            'available_backends': sum(1 for b in backend_usage.values() if b['available'])
        }

        return report


def optimize_backend_distribution(space_manager: SpaceManager, dry_run: bool = True) -> Dict:
    """
    Optimize file distribution across backends

    Moves files to balance space usage and optimize access patterns.

    Args:
        space_manager: SpaceManager instance
        dry_run: If True, only report what would be done

    Returns:
        Dictionary with optimization statistics
    """
    stats = {
        'files_moved': 0,
        'bytes_moved': 0,
        'replicas_removed': 0,
        'recomputable_deleted': 0
    }

    # Cleanup over-replication
    replicas_removed = space_manager.cleanup_over_replication(dry_run=dry_run)
    stats['replicas_removed'] = replicas_removed

    # TODO: Implement more sophisticated optimization
    # - Move hot files to faster backends
    # - Move cold files to slower backends
    # - Balance space usage across backends

    return stats
