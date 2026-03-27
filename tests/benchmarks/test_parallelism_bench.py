"""
Concurrency scaling benchmarks.

Measures how backend operations scale with thread count.
This directly answers the question "does SFTP have bad parallelism?"
by showing throughput curves as we add threads.

Inspired by Berger's emphasis on understanding scaling behavior:
if throughput doesn't grow with threads, there's a serialization
bottleneck (lock contention, protocol limitation, or Amdahl's law).

Run:
    pytest tests/benchmarks/test_parallelism_bench.py -v -s
"""

import tempfile
import threading
import time
from pathlib import Path

import pytest

from storage.backends.local import LocalBackend
from .bench_utils import (
    FILE_SIZES,
    FileFactory,
    LatencyBackend,
    run_concurrent,
    stats_summary,
    timed,
)

# Thread counts to test scaling
THREAD_COUNTS = [1, 2, 4, 8]

# File size for parallelism tests (64KB is a reasonable middle ground)
PARALLEL_FILE_SIZE = 64 * 1024
PARALLEL_FILE_NAME = "64KB"


class TestCopyFromLocalScaling:
    """
    Measure copy_from_local throughput as thread count increases.

    For a local backend, we expect near-linear scaling (I/O bound).
    For a latency-injected backend, we expect better-than-linear scaling
    because threads overlap wait time.

    This is the key test for the SFTP deprecation question: if the
    simulated-remote backend shows poor scaling, that matches the
    observed SFTP behavior and motivates rsync (which batches transfers).
    """

    OPS_PER_THREAD = 10

    @pytest.fixture(params=THREAD_COUNTS, ids=[f"{n}T" for n in THREAD_COUNTS])
    def n_threads(self, request):
        return request.param

    def _make_source_files(self, factory, source_dir, n_threads):
        """Create one source file per thread to avoid contention on reads."""
        files = []
        for i in range(n_threads):
            p = factory.make_file(
                source_dir / f"par_src_{i}.bin", PARALLEL_FILE_SIZE
            )
            files.append(p)
        return files

    def test_local_scaling(self, local_backend, file_factory, n_threads,
                           bench_tmp, capsys):
        """Local backend copy_from_local thread scaling."""
        source_dir = bench_tmp / "par_src_local"
        source_dir.mkdir(exist_ok=True)
        src_files = self._make_source_files(file_factory, source_dir, n_threads)
        counter = [0]
        lock = threading.Lock()

        def do_copy():
            with lock:
                idx = counter[0]
                counter[0] += 1
            tid = idx % n_threads
            src = src_files[tid]
            vpath = f"/par_upload/t{tid}_{idx}.bin"
            return timed(lambda: local_backend.copy_from_local(src, vpath))

        durations = run_concurrent(do_copy, n_threads, self.OPS_PER_THREAD)
        result = stats_summary(
            f"copy_from_local/local/{n_threads}T",
            durations,
            bytes_per_op=PARALLEL_FILE_SIZE,
        )
        print(f"\n{result.summary_line()}")

        # Should complete all operations
        assert len(durations) == n_threads * self.OPS_PER_THREAD

    def test_remote_scaling(self, bench_tmp, file_factory, n_threads, capsys):
        """
        Simulated remote backend (50ms latency) thread scaling.

        With 50ms latency and 10 ops/thread:
        - 1 thread:  ~500ms total (serialized waits)
        - 8 threads: ~62ms per thread if perfectly parallel

        Poor scaling here = serialization bottleneck (the SFTP problem).
        """
        backend_root = bench_tmp / "par_backend_remote"
        backend_root.mkdir(exist_ok=True)
        inner = LocalBackend(name="par-inner", root_path=str(backend_root))
        remote = LatencyBackend(inner, latency_s=0.050, jitter_s=0.005)

        source_dir = bench_tmp / "par_src_remote"
        source_dir.mkdir(exist_ok=True)
        src_files = self._make_source_files(file_factory, source_dir, n_threads)
        counter = [0]
        lock = threading.Lock()

        def do_copy():
            with lock:
                idx = counter[0]
                counter[0] += 1
            tid = idx % n_threads
            src = src_files[tid]
            vpath = f"/par_upload_remote/t{tid}_{idx}.bin"
            return timed(lambda: remote.copy_from_local(src, vpath))

        durations = run_concurrent(do_copy, n_threads, self.OPS_PER_THREAD)
        result = stats_summary(
            f"copy_from_local/remote/{n_threads}T",
            durations,
            bytes_per_op=PARALLEL_FILE_SIZE,
        )
        print(f"\n{result.summary_line()}")

        assert len(durations) == n_threads * self.OPS_PER_THREAD


class TestCopyToLocalScaling:
    """Measure copy_to_local (download) thread scaling."""

    OPS_PER_THREAD = 10

    @pytest.fixture(params=THREAD_COUNTS, ids=[f"{n}T" for n in THREAD_COUNTS])
    def n_threads(self, request):
        return request.param

    def test_local_scaling(self, local_backend, file_factory, n_threads,
                           bench_tmp, capsys):
        """Local backend copy_to_local thread scaling."""
        # Pre-populate backend with files
        vpaths = file_factory.populate_backend(
            local_backend, n_threads * self.OPS_PER_THREAD,
            PARALLEL_FILE_SIZE, prefix="par_dl"
        )
        cache_dir = bench_tmp / "par_cache_local"
        cache_dir.mkdir(exist_ok=True)

        counter = [0]
        lock = threading.Lock()

        def do_copy():
            with lock:
                idx = counter[0]
                counter[0] += 1
            vpath = vpaths[idx % len(vpaths)]
            dest = cache_dir / f"dl_{idx}.bin"
            return timed(lambda: local_backend.copy_to_local(vpath, dest))

        durations = run_concurrent(do_copy, n_threads, self.OPS_PER_THREAD)
        result = stats_summary(
            f"copy_to_local/local/{n_threads}T",
            durations,
            bytes_per_op=PARALLEL_FILE_SIZE,
        )
        print(f"\n{result.summary_line()}")

        assert len(durations) == n_threads * self.OPS_PER_THREAD


class TestScalingSummary:
    """
    Aggregate scaling test: runs all thread counts in one test and
    prints a scaling efficiency table.

    This is the most useful single test for the SFTP question:
    it shows exactly how throughput scales (or doesn't) with concurrency.
    """

    OPS_PER_THREAD = 15

    def test_upload_scaling_table(self, bench_tmp, file_factory, capsys):
        """Print throughput scaling table for copy_from_local."""
        backend_root = bench_tmp / "scale_backend"
        backend_root.mkdir()
        inner = LocalBackend(name="scale-inner", root_path=str(backend_root))

        # Test both "local" (no latency) and "remote" (50ms latency)
        configs = [
            ("local", LatencyBackend(inner, latency_s=0.0)),
            ("LAN_5ms", LatencyBackend(inner, latency_s=0.005)),
            ("WAN_50ms", LatencyBackend(inner, latency_s=0.050)),
        ]

        source_dir = bench_tmp / "scale_src"
        source_dir.mkdir()
        src_files = [
            file_factory.make_file(source_dir / f"s_{i}.bin", PARALLEL_FILE_SIZE)
            for i in range(max(THREAD_COUNTS))
        ]

        print("\n\n=== Upload (copy_from_local) Scaling Table ===")
        print(f"{'Backend':<12} {'Threads':>7} {'p50(ms)':>9} {'p95(ms)':>9} "
              f"{'ops/s':>8} {'MB/s':>8} {'efficiency':>10}")
        print("-" * 75)

        for config_name, backend in configs:
            baseline_ops_s = None

            for n_threads in THREAD_COUNTS:
                counter = [0]
                lock = threading.Lock()

                def do_copy(be=backend, srcs=src_files, nt=n_threads):
                    with lock:
                        idx = counter[0]
                        counter[0] += 1
                    src = srcs[idx % nt]
                    vpath = f"/scale/{config_name}/t{idx}.bin"
                    return timed(lambda: be.copy_from_local(src, vpath))

                durations = run_concurrent(
                    do_copy, n_threads, self.OPS_PER_THREAD
                )
                result = stats_summary(
                    f"{config_name}/{n_threads}T",
                    durations,
                    bytes_per_op=PARALLEL_FILE_SIZE,
                )

                if baseline_ops_s is None:
                    baseline_ops_s = result.throughput_ops_per_s

                efficiency = (
                    result.throughput_ops_per_s / (baseline_ops_s * n_threads)
                    if baseline_ops_s > 0
                    else 0
                )

                mb_s = result.throughput_mb_per_s or 0
                print(
                    f"{config_name:<12} {n_threads:>7} "
                    f"{result.p50_s * 1000:>9.2f} {result.p95_s * 1000:>9.2f} "
                    f"{result.throughput_ops_per_s:>8.1f} {mb_s:>8.2f} "
                    f"{efficiency:>9.0%}"
                )

        print()
