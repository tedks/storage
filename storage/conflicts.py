"""
Conflict detection for Storage

Detects when a file has been modified both locally and on a remote backend.
Conflicts are surfaced through the FUSE readdir conflict view (name.backend)
and resolved via the ``storage conflicts`` CLI command.
"""

import logging
import time
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

from .backends.base import Backend
from .state import StateManager

logger = logging.getLogger(__name__)


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
        logger.debug(
            "conflict check: path=%s backend=%s local_checksum=%s",
            virtual_path, backend.name, local_checksum[:12] if local_checksum else None,
        )

        # Unknown is not absent: if the member cannot be asked, this raises
        # and the sync to it fails and is retried, instead of overwriting a
        # file whose remote state was never checked.
        if not backend.exists_or_raise(virtual_path):
            return None

        locations = self.state_manager.get_file_locations(virtual_path)

        backend_location = None
        for location in locations:
            if location.backend_name == backend.name:
                backend_location = location
                break

        if not backend_location:
            return None

        # Compute current backend checksum.
        # WARNING: this downloads the ENTIRE remote file to /tmp to hash it.
        # For large files this is extremely expensive.  See issue #74.
        try:
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_path = Path(tmp.name)

            logger.info(
                "conflict check downloading remote file: path=%s backend=%s "
                "recorded_checksum=%s (this downloads the entire file)",
                virtual_path, backend.name,
                backend_location.checksum[:12] if backend_location.checksum else None,
            )
            backend.copy_to_local(virtual_path, tmp_path)
            remote_checksum = self.state_manager.compute_checksum(tmp_path)
            tmp_path.unlink()
            logger.info(
                "conflict check complete: path=%s backend=%s remote_checksum=%s",
                virtual_path, backend.name, remote_checksum[:12],
            )

        except Exception as e:
            logger.warning(
                "conflict check failed (assuming no conflict): path=%s backend=%s error=%s",
                virtual_path, backend.name, e,
            )
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

