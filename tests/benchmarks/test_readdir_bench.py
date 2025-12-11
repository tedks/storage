"""
Benchmarks for readdir and directory listing performance.

Run with: pytest tests/benchmarks/ --benchmark-only
"""

import os
import tempfile
import time
from pathlib import Path

import pytest

from storage.fs import StorageFS
from storage.backends.base import Backend, BackendType


class FakeBackend(Backend):
    """
    Minimal backend that serves a fixed set of directory entries and simulates latency.
    """

    def __init__(self, name: str, entries: list[str], latency: float = 0.0, mtime: float | None = None):
        super().__init__(name, BackendType.LOCAL, is_remote=False, speed_priority=10)
        self.entries = list(entries)
        self.latency = latency
        self._dir_mtime = mtime or time.time()

    def _sleep(self):
        if self.latency > 0:
            time.sleep(self.latency)

    # Backend API
    def exists(self, virtual_path: str) -> bool:
        return virtual_path == "/" or virtual_path.lstrip("/") in self.entries

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        raise NotImplementedError

    def write_file(self, virtual_path: str, data: bytes, offset: int):
        raise NotImplementedError

    def create_file(self, virtual_path: str):
        raise NotImplementedError

    def delete_file(self, virtual_path: str):
        raise NotImplementedError

    def truncate_file(self, virtual_path: str):
        raise NotImplementedError

    def get_size(self, virtual_path: str) -> int:
        raise NotImplementedError

    def mkdir(self, virtual_path: str):
        pass

    def rmdir(self, virtual_path: str):
        pass

    def listdir(self, virtual_path: str):
        self._sleep()
        return list(self.entries)

    def isdir(self, virtual_path: str) -> bool:
        return virtual_path == "/"

    def copy_to_local(self, virtual_path: str, local_path: Path):
        raise NotImplementedError

    def copy_from_local(self, local_path: Path, virtual_path: str):
        raise NotImplementedError

    def rename(self, old_virtual_path: str, new_virtual_path: str):
        raise NotImplementedError

    def get_free_space(self):
        return None

    def stat(self, virtual_path: str):
        class _S:
            st_mtime = self._dir_mtime
        return _S()

    def sync(self):
        pass

    def get_dir_mtime(self, virtual_path: str):
        return self._dir_mtime


@pytest.fixture
def fs_with_fake_backend():
    """Create a StorageFS with a fake backend for benchmarking."""
    temp_dir = tempfile.TemporaryDirectory()
    base = Path(temp_dir.name)
    cache_dir = base / "cache"
    cache_dir.mkdir()
    db_path = base / "state.db"

    entries = [f"file_{i}.txt" for i in range(200)]
    backend = FakeBackend("fake", entries=entries, latency=0.05)
    fs = StorageFS(
        cache_root=cache_dir,
        state_db=db_path,
        backends={"fake": backend},
        cache_size=10 * 1024 * 1024,
        num_sync_workers=0,
        start_sync=False,
    )
    yield fs, backend
    fs.destroy("/")
    temp_dir.cleanup()


def test_readdir_warm_vs_cold_latency(benchmark, fs_with_fake_backend):
    """Warm cache reads should be faster than cold reads."""
    fs, backend = fs_with_fake_backend

    def cold_call():
        list(fs.readdir("/", None))
        fs._invalidate_dir_cache_entry("/")

    # Measure cold path
    cold_durations = []
    for _ in range(3):
        start = time.perf_counter()
        cold_call()
        cold_durations.append(time.perf_counter() - start)
    cold_median = sorted(cold_durations)[len(cold_durations) // 2]

    def warm_call():
        list(fs.readdir("/", None))

    # Measure warm path
    warm_durations = []
    for _ in range(10):
        start = time.perf_counter()
        warm_call()
        warm_durations.append(time.perf_counter() - start)
    warm_median = sorted(warm_durations)[len(warm_durations) // 2]

    benchmark(warm_call)

    # Warm should be substantially faster than cold
    assert warm_median < cold_median / 2


@pytest.fixture
def fs_with_large_dir():
    """Create a StorageFS with a large directory for benchmarking."""
    temp_dir = tempfile.TemporaryDirectory()
    base = Path(temp_dir.name)
    cache_dir = base / "cache"
    cache_dir.mkdir()
    db_path = base / "state.db"

    entries = [f"seed_{i}.txt" for i in range(3000)]
    backends = {
        "b1": FakeBackend("b1", entries=entries, latency=0.0, mtime=time.time()),
        "b2": FakeBackend("b2", entries=[], latency=0.0, mtime=time.time()),
    }

    fs = StorageFS(
        cache_root=cache_dir,
        state_db=db_path,
        backends=backends,
        cache_size=100 * 1024 * 1024,
        num_sync_workers=0,
        start_sync=False,
    )
    yield fs, backends
    fs.destroy("/")
    temp_dir.cleanup()


def test_readdir_large_dir_warm(benchmark, fs_with_large_dir):
    """Large directory warm reads should stay under 500ms."""
    fs, backends = fs_with_large_dir

    # Warm the cache
    list(fs.readdir("/", None))

    def warm_call():
        list(fs.readdir("/", None))

    warm_durations = []
    for _ in range(5):
        start = time.perf_counter()
        warm_call()
        warm_durations.append(time.perf_counter() - start)
    warm_median = sorted(warm_durations)[len(warm_durations) // 2]

    benchmark(warm_call)

    # Should stay under a modest bound
    assert warm_median < 0.5
