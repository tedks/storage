"""
Shared fixtures for storage benchmarks.

Provides temp directories, backends, and file populations that all
benchmark modules can reuse.
"""

import tempfile
from pathlib import Path

import pytest

from storage.backends.local import LocalBackend
from .bench_utils import FileFactory, LatencyBackend, FILE_SIZES


# ---------------------------------------------------------------------------
# Temp directory fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def bench_tmp(tmp_path):
    """Root temp directory for a benchmark run."""
    return tmp_path


@pytest.fixture
def source_dir(bench_tmp):
    """Directory holding source files for upload benchmarks."""
    d = bench_tmp / "source"
    d.mkdir()
    return d


@pytest.fixture
def cache_dir(bench_tmp):
    """Cache directory for download benchmarks."""
    d = bench_tmp / "cache"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# Backend fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def local_backend(bench_tmp):
    """A plain LocalBackend rooted in a temp directory."""
    root = bench_tmp / "backend_local"
    root.mkdir()
    return LocalBackend(name="bench-local", root_path=str(root))


@pytest.fixture
def slow_backend(local_backend):
    """
    A LocalBackend wrapped with 5ms latency and 1ms jitter.
    Simulates a low-latency LAN SSH connection.
    """
    return LatencyBackend(local_backend, latency_s=0.005, jitter_s=0.001)


@pytest.fixture
def remote_backend(local_backend):
    """
    A LocalBackend wrapped with 50ms latency and 10ms jitter,
    plus bandwidth ceiling of ~10 MB/s (1e-7 s/byte).
    Simulates a typical WAN SSH connection.
    """
    return LatencyBackend(
        local_backend,
        latency_s=0.050,
        jitter_s=0.010,
        per_byte_s=1e-7,
    )


# ---------------------------------------------------------------------------
# File factory
# ---------------------------------------------------------------------------

@pytest.fixture
def file_factory():
    """Seeded FileFactory for reproducible file generation."""
    return FileFactory(seed=0xBEEF)
