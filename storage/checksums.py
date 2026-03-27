"""
Fast checksum helpers for data integrity verification.

Uses xxHash (XXH3) for speed.  This is a non-cryptographic hash --
we don't need collision resistance against adversaries, only against
accidental data corruption.  XXH3-128 processes data at ~10 GB/s,
roughly 40x faster than SHA256 and 35x faster than BLAKE2b.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import xxhash

# Read buffer: 1 MB chunks for streaming large files.
_CHUNK_SIZE = 1024 * 1024


def fast_checksum_file(path: Path) -> str:
    """
    Compute a fast checksum for a local file.

    Returns a 128-bit XXH3 hex digest (32 hex chars).
    """
    digest = xxhash.xxh3_128()
    with open(path, "rb", buffering=_CHUNK_SIZE) as fh:
        for chunk in iter(lambda: fh.read(_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fast_checksum_bytes(data: bytes) -> str:
    """Compute a fast checksum for an in-memory buffer."""
    return xxhash.xxh3_128(data).hexdigest()


@dataclass(frozen=True)
class ChecksumRecord:
    """Checksum value plus the time it was produced."""

    checksum: str
    timestamp: float
