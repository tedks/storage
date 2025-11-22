"""
Fast checksum helpers used for conflict detection.

We prefer BLAKE2b for speed while keeping collisions extremely unlikely.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path


def fast_checksum_file(path: Path) -> str:
    """
    Compute a fast but strong checksum for a local file.

    Uses BLAKE2b with a 256-bit digest for speed and security.
    """
    digest = hashlib.blake2b(digest_size=32)
    with open(path, "rb", buffering=1024 * 1024) as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class ChecksumRecord:
    """Checksum value plus the time it was produced."""

    checksum: str
    timestamp: float
