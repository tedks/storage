"""
Backend operation benchmarks.

Measures isolated backend operations across file sizes and backend types
(local vs. simulated-remote).  Each test uses pytest-benchmark for
automated statistical analysis (min 5 rounds, adaptive warmup).

Run:
    pytest tests/benchmarks/test_backend_bench.py --benchmark-only -v
    pytest tests/benchmarks/test_backend_bench.py --benchmark-only --benchmark-json=results.json
"""

import random
import tempfile
from pathlib import Path

import pytest

from storage.backends.local import LocalBackend
from .bench_utils import (
    FILE_SIZES,
    FileFactory,
    LatencyBackend,
    stats_summary,
    timed,
)


# ---------------------------------------------------------------------------
# Parametrize over file sizes
# ---------------------------------------------------------------------------

FILE_SIZE_PARAMS = list(FILE_SIZES.items())


@pytest.fixture(params=FILE_SIZE_PARAMS, ids=[p[0] for p in FILE_SIZE_PARAMS])
def file_size(request):
    """Yields (name, size_bytes) for each standard file size."""
    return request.param


# ---------------------------------------------------------------------------
# copy_from_local (upload) benchmarks
# ---------------------------------------------------------------------------

class TestCopyFromLocal:
    """Benchmark copy_from_local across file sizes and backend types."""

    def test_local_backend(self, benchmark, local_backend, file_factory,
                           source_dir, file_size):
        """Baseline: local copy_from_local throughput."""
        name, nbytes = file_size
        src = file_factory.make_file(source_dir / f"upload_{name}.bin", nbytes)

        def do_copy():
            vpath = f"/bench_upload_{name}.bin"
            local_backend.copy_from_local(src, vpath)

        benchmark.extra_info["file_size"] = name
        benchmark.extra_info["file_bytes"] = nbytes
        benchmark.extra_info["backend"] = "local"
        benchmark(do_copy)

    def test_slow_backend(self, benchmark, slow_backend, file_factory,
                          source_dir, file_size):
        """Simulated LAN (5ms latency): copy_from_local throughput."""
        name, nbytes = file_size
        src = file_factory.make_file(source_dir / f"upload_slow_{name}.bin", nbytes)

        def do_copy():
            vpath = f"/bench_upload_slow_{name}.bin"
            slow_backend.copy_from_local(src, vpath)

        benchmark.extra_info["file_size"] = name
        benchmark.extra_info["file_bytes"] = nbytes
        benchmark.extra_info["backend"] = "latency:5ms"
        benchmark(do_copy)

    def test_remote_backend(self, benchmark, remote_backend, file_factory,
                            source_dir, file_size):
        """Simulated WAN (50ms + bandwidth cap): copy_from_local throughput."""
        name, nbytes = file_size
        # Skip 16MB on simulated WAN to keep bench time reasonable
        if nbytes > 1024 * 1024:
            pytest.skip("Skipping large file on simulated WAN backend")

        src = file_factory.make_file(source_dir / f"upload_remote_{name}.bin", nbytes)

        def do_copy():
            vpath = f"/bench_upload_remote_{name}.bin"
            remote_backend.copy_from_local(src, vpath)

        benchmark.extra_info["file_size"] = name
        benchmark.extra_info["file_bytes"] = nbytes
        benchmark.extra_info["backend"] = "latency:50ms+bw"
        benchmark(do_copy)


# ---------------------------------------------------------------------------
# copy_to_local (download) benchmarks
# ---------------------------------------------------------------------------

class TestCopyToLocal:
    """Benchmark copy_to_local across file sizes and backend types."""

    def test_local_backend(self, benchmark, local_backend, file_factory,
                           cache_dir, file_size):
        """Baseline: local copy_to_local throughput."""
        name, nbytes = file_size
        # Pre-populate backend with source file
        vpaths = file_factory.populate_backend(local_backend, 1, nbytes,
                                               prefix=f"dl_{name}")
        vpath = vpaths[0]
        dest = cache_dir / f"download_{name}.bin"

        def do_copy():
            local_backend.copy_to_local(vpath, dest)

        benchmark.extra_info["file_size"] = name
        benchmark.extra_info["file_bytes"] = nbytes
        benchmark.extra_info["backend"] = "local"
        benchmark(do_copy)

    def test_slow_backend(self, benchmark, slow_backend, file_factory,
                          cache_dir, file_size):
        """Simulated LAN: copy_to_local throughput."""
        name, nbytes = file_size
        # Populate via inner backend to avoid latency during setup
        vpaths = file_factory.populate_backend(slow_backend._inner, 1, nbytes,
                                               prefix=f"dl_slow_{name}")
        vpath = vpaths[0]
        dest = cache_dir / f"download_slow_{name}.bin"

        def do_copy():
            slow_backend.copy_to_local(vpath, dest)

        benchmark.extra_info["file_size"] = name
        benchmark.extra_info["file_bytes"] = nbytes
        benchmark.extra_info["backend"] = "latency:5ms"
        benchmark(do_copy)


# ---------------------------------------------------------------------------
# listdir benchmarks
# ---------------------------------------------------------------------------

class TestListdir:
    """Benchmark directory listing at various directory sizes."""

    DIR_SIZES = [10, 100, 1000, 5000]

    @pytest.fixture(params=DIR_SIZES, ids=[f"{n}files" for n in DIR_SIZES])
    def dir_size(self, request):
        return request.param

    def test_local_listdir(self, benchmark, local_backend, dir_size):
        """Local backend listdir scaling with directory size."""
        # Create files directly on backend
        for i in range(dir_size):
            local_backend.create_file(f"/listbench/file_{i:05d}.txt")

        benchmark.extra_info["dir_entries"] = dir_size
        benchmark.extra_info["backend"] = "local"
        benchmark(local_backend.listdir, "/listbench")

    def test_local_listdir_attrs(self, benchmark, local_backend, dir_size):
        """Local backend listdir_attrs scaling (avoids N+1 stat calls)."""
        for i in range(dir_size):
            local_backend.create_file(f"/listbench_attrs/file_{i:05d}.txt")

        benchmark.extra_info["dir_entries"] = dir_size
        benchmark.extra_info["backend"] = "local"
        benchmark(local_backend.listdir_attrs, "/listbench_attrs")

    def test_slow_listdir(self, benchmark, slow_backend, dir_size):
        """Simulated LAN listdir (shows per-call latency cost)."""
        for i in range(dir_size):
            slow_backend._inner.create_file(f"/listbench_slow/file_{i:05d}.txt")

        benchmark.extra_info["dir_entries"] = dir_size
        benchmark.extra_info["backend"] = "latency:5ms"
        benchmark(slow_backend.listdir, "/listbench_slow")


# ---------------------------------------------------------------------------
# stat / metadata benchmarks
# ---------------------------------------------------------------------------

class TestMetadata:
    """Benchmark metadata operations (stat, exists, get_size)."""

    N_FILES = 100

    @pytest.fixture
    def populated_backend(self, local_backend):
        """Backend with N_FILES for metadata benchmarks."""
        for i in range(self.N_FILES):
            local_backend.create_file(f"/meta/file_{i:04d}.txt")
        return local_backend

    def test_stat(self, benchmark, populated_backend):
        """Stat a random file."""
        rng = random.Random(42)
        files = [f"/meta/file_{i:04d}.txt" for i in range(self.N_FILES)]

        def do_stat():
            f = rng.choice(files)
            populated_backend.stat(f)

        benchmark(do_stat)

    def test_exists(self, benchmark, populated_backend):
        """Check existence of a random file."""
        rng = random.Random(42)
        files = [f"/meta/file_{i:04d}.txt" for i in range(self.N_FILES)]
        # Mix existing and non-existing paths
        all_paths = files + [f"/meta/nonexist_{i}.txt" for i in range(50)]

        def do_exists():
            f = rng.choice(all_paths)
            populated_backend.exists(f)

        benchmark(do_exists)

    def test_get_size(self, benchmark, populated_backend):
        """Get size of a random file."""
        rng = random.Random(42)
        files = [f"/meta/file_{i:04d}.txt" for i in range(self.N_FILES)]

        def do_get_size():
            f = rng.choice(files)
            populated_backend.get_size(f)

        benchmark(do_get_size)


# ---------------------------------------------------------------------------
# Manual stats collection (for comparison without pytest-benchmark)
# ---------------------------------------------------------------------------

class TestManualStats:
    """
    Runs benchmarks with explicit statistical reporting.
    Useful when you want the full BenchResult output (p50/p95/p99/CI)
    or need to compare backends programmatically.
    """

    def test_copy_from_local_stats(self, local_backend, file_factory,
                                   source_dir, capsys):
        """Print detailed stats for copy_from_local across file sizes."""
        n_trials = 20
        results = []

        for size_name, size_bytes in FILE_SIZES.items():
            src = file_factory.make_file(
                source_dir / f"stats_{size_name}.bin", size_bytes
            )
            durations = []
            for i in range(n_trials):
                vpath = f"/stats_{size_name}_{i}.bin"
                dur = timed(lambda: local_backend.copy_from_local(src, vpath))
                durations.append(dur)

            result = stats_summary(
                f"copy_from_local/{size_name}",
                durations,
                bytes_per_op=size_bytes,
            )
            results.append(result)

        # Print summary table
        print("\n--- copy_from_local detailed stats ---")
        for r in results:
            print(r.summary_line())

        # Basic sanity: larger files should generally take longer
        assert results[-1].p50_s >= results[0].p50_s
