"""
Conflict detection and handling for Storage

Handles detection of conflicts when files are modified in multiple locations.
Per design, conflicts are exposed as separate files with timestamps rather
than attempting automatic resolution.
"""

import time
from pathlib import Path
from typing import Optional, Tuple
from dataclasses import dataclass

from .backends.base import Backend
from .state import StateManager


@dataclass
class Conflict:
    """Represents a detected conflict"""
    virtual_path: str
    local_checksum: str
    remote_checksum: str
    backend_name: str
    detected_time: float


class ConflictDetector:
    """
    Detects conflicts between local cache and remote backends

    A conflict occurs when:
    1. File exists on backend with recorded checksum
    2. Backend file is modified (different checksum)
    3. Local file is also modified
    """

    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager

    def check_conflict(self, virtual_path: str, backend: Backend,
                      local_checksum: str) -> Optional[Conflict]:
        """
        Check if a conflict exists for a file on a specific backend

        Args:
            virtual_path: Virtual path of file
            backend: Backend to check
            local_checksum: Checksum of local version

        Returns:
            Conflict object if conflict detected, None otherwise
        """
        if not backend.exists(virtual_path):
            # No conflict if file doesn't exist on backend
            return None

        # Get recorded locations
        locations = self.state_manager.get_file_locations(virtual_path)

        # Find this backend's recorded state
        backend_location = None
        for location in locations:
            if location.backend_name == backend.name:
                backend_location = location
                break

        if not backend_location:
            # File exists on backend but we have no record of it
            # This could be a new file or external modification
            # For now, treat as no conflict (will sync)
            return None

        # Compute current backend checksum
        try:
            # Download to temp location and compute checksum
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_path = Path(tmp.name)

            backend.copy_to_local(virtual_path, tmp_path)
            remote_checksum = self.state_manager.compute_checksum(tmp_path)
            tmp_path.unlink()

        except Exception as e:
            # Can't compute checksum, assume no conflict
            return None

        # Check for conflict
        if backend_location.checksum != remote_checksum:
            # Backend file was modified
            if local_checksum != backend_location.checksum:
                # Local file was also modified - CONFLICT
                return Conflict(
                    virtual_path=virtual_path,
                    local_checksum=local_checksum,
                    remote_checksum=remote_checksum,
                    backend_name=backend.name,
                    detected_time=time.time()
                )

        return None

    def create_conflict_files(self, conflict: Conflict, cache_path: Path,
                            backend: Backend) -> Tuple[str, str]:
        """
        Create conflict files with timestamps

        Per design:
        - Remote file: {filename}.remote.{timestamp}
        - Local file: {filename}.local.{timestamp}

        Args:
            conflict: Conflict information
            cache_path: Path to local cached file
            backend: Backend with conflicting version

        Returns:
            Tuple of (local_conflict_path, remote_conflict_path)
        """
        timestamp = int(conflict.detected_time)
        base_path = Path(conflict.virtual_path)
        base_name = base_path.stem
        suffix = base_path.suffix

        # Create conflict file names
        local_name = f"{base_name}.local.{timestamp}{suffix}"
        remote_name = f"{base_name}.remote.{timestamp}{suffix}"

        local_conflict_path = str(base_path.parent / local_name)
        remote_conflict_path = str(base_path.parent / remote_name)

        return (local_conflict_path, remote_conflict_path)


class ConflictResolver:
    """
    Helper for conflict resolution

    Provides utilities for the storage-resolve command to handle conflicts.
    """

    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager

    def list_conflicts(self, directory: Path) -> list:
        """
        List all conflict files in a directory

        Returns:
            List of tuples (original_path, local_conflict, remote_conflict)
        """
        conflicts = []

        # Find .local.* and .remote.* files
        local_files = list(directory.glob("*.local.*"))
        remote_files = list(directory.glob("*.remote.*"))

        # Group by timestamp and original name
        conflict_groups = {}

        for local_file in local_files:
            # Parse: basename.local.timestamp.ext
            parts = local_file.name.split('.')
            if len(parts) >= 3:
                timestamp = parts[-2]
                base = '.'.join(parts[:-3])
                ext = parts[-1]
                key = (base, timestamp)

                if key not in conflict_groups:
                    conflict_groups[key] = {}
                conflict_groups[key]['local'] = local_file

        for remote_file in remote_files:
            parts = remote_file.name.split('.')
            if len(parts) >= 3:
                timestamp = parts[-2]
                base = '.'.join(parts[:-3])
                ext = parts[-1]
                key = (base, timestamp)

                if key not in conflict_groups:
                    conflict_groups[key] = {}
                conflict_groups[key]['remote'] = remote_file

        # Build conflict list
        for (base, timestamp), files in conflict_groups.items():
            if 'local' in files and 'remote' in files:
                conflicts.append((
                    base,
                    files['local'],
                    files['remote'],
                    timestamp
                ))

        return conflicts

    def resolve_with_local(self, local_file: Path, remote_file: Path,
                          original_name: str):
        """
        Resolve conflict by keeping local version

        Renames local conflict file to original name and deletes remote.
        """
        original_path = local_file.parent / original_name

        # Use local version
        if original_path.exists():
            original_path.unlink()
        local_file.rename(original_path)

        # Delete remote version
        if remote_file.exists():
            remote_file.unlink()

    def resolve_with_remote(self, local_file: Path, remote_file: Path,
                           original_name: str):
        """
        Resolve conflict by keeping remote version

        Renames remote conflict file to original name and deletes local.
        """
        original_path = remote_file.parent / original_name

        # Use remote version
        if original_path.exists():
            original_path.unlink()
        remote_file.rename(original_path)

        # Delete local version
        if local_file.exists():
            local_file.unlink()

    def resolve_with_both(self, local_file: Path, remote_file: Path,
                         original_name: str):
        """
        Keep both versions with descriptive names

        Keeps .local and .remote files, removes original.
        """
        original_path = local_file.parent / original_name
        if original_path.exists():
            original_path.unlink()

        # Both conflict files remain
        # User can manually decide what to do with them
