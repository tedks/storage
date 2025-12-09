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

    # Helpers
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

    def bump_mtime(self):
        self._dir_mtime = time.time()


@pytest.fixture
def fs_with_fake_backend():
    temp_dir = tempfile.TemporaryDirectory()
    base = Path(temp_dir.name)
    cache_dir = base / "cache"
    cache_dir.mkdir()
    db_path = base / "state.db"

    entries = [f"file_{i}.txt" for i in range(200)]
    backend = FakeBackend("fake", entries=entries, latency=0.05)
    fs = StorageFS(cache_root=cache_dir, state_db=db_path, backends={"fake": backend},
                   cache_size=10 * 1024 * 1024, num_sync_workers=0, start_sync=False)
    yield fs, backend
    fs.destroy("/")
    temp_dir.cleanup()


def test_readdir_warm_vs_cold_latency(benchmark, fs_with_fake_backend):
    fs, backend = fs_with_fake_backend

    # Cold path (fills cache)
    def cold_call():
        list(fs.readdir("/", None))
        fs._invalidate_dir_cache_entry("/")

    # Manual timing for cold to avoid reusing benchmark fixture
    cold_durations = []
    for _ in range(3):
        start = time.perf_counter()
        cold_call()
        cold_durations.append(time.perf_counter() - start)
    cold_median = sorted(cold_durations)[len(cold_durations) // 2]

    def warm_call():
        list(fs.readdir("/", None))

    warm_durations = []
    for _ in range(10):
        start = time.perf_counter()
        warm_call()
        warm_durations.append(time.perf_counter() - start)
    warm_median = sorted(warm_durations)[len(warm_durations) // 2]
    benchmark(warm_call)  # record sample for benchmark report

    # Warm should be substantially faster than cold; allow generous ratio
    assert warm_median < cold_median / 5


def test_readdir_refresh_detects_mtime(benchmark, fs_with_fake_backend):
    fs, backend = fs_with_fake_backend

    # Warm cache
    list(fs.readdir("/", None))

    # Trigger backend change
    backend.entries.append("new_file.txt")
    backend.bump_mtime()

    def await_refresh():
        # First call should be quick (cache) but schedule refresh
        list(fs.readdir("/", None))
        # Poll until new file appears
        deadline = time.time() + 2.0
        while time.time() < deadline:
            if "new_file.txt" in set(fs.readdir("/", None)):
                return True
            time.sleep(0.05)
        return False

    result = benchmark(await_refresh)
    assert result is True


@pytest.fixture
def fs_with_real_backends():
    temp_dir = tempfile.TemporaryDirectory()
    base = Path(temp_dir.name)
    cache_dir = base / "cache"
    cache_dir.mkdir()
    backend1 = base / "backend1"
    backend2 = base / "backend2"
    backend1.mkdir()
    backend2.mkdir()
    db_path = base / "state.db"

    # seed many files
    for i in range(3000):
        (backend1 / f"seed_{i}.txt").write_text("x")

    backends = {
        "b1": FakeBackend("b1", entries=[f"seed_{i}.txt" for i in range(3000)], latency=0.0, mtime=time.time()),
        "b2": FakeBackend("b2", entries=[], latency=0.0, mtime=time.time()),
    }

    fs = StorageFS(cache_root=cache_dir, state_db=db_path, backends=backends,
                   cache_size=100 * 1024 * 1024, num_sync_workers=0, start_sync=False)
    yield fs, backends
    fs.destroy("/")
    temp_dir.cleanup()


def test_readdir_large_dir_warm(benchmark, fs_with_real_backends):
    fs, backends = fs_with_real_backends

    list(fs.readdir("/", None))  # warm

    def warm_call():
        list(fs.readdir("/", None))

    warm_durations = []
    for _ in range(5):
        start = time.perf_counter()
        warm_call()
        warm_durations.append(time.perf_counter() - start)
    warm_median = sorted(warm_durations)[len(warm_durations) // 2]
    benchmark(warm_call)  # record sample for benchmark report
    # Should stay under a modest bound on local machine; allow headroom to avoid flakes
    assert warm_median < 0.3
