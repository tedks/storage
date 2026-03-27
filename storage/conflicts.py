"""
Conflict detection for Storage

Detects when a file has been modified both locally and on a remote backend.
Conflicts are surfaced through the FUSE readdir conflict view (name.backend)
and resolved via the ``storage conflicts`` CLI command.
"""

import time
from pathlib import Path
from typing import Optional
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

