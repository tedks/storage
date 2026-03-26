"""
End-to-end sync pipeline benchmarks.

Measures the full sync path: file written to cache -> sync worker picks
it up -> copy_from_local to backend(s).  This captures the real overhead
of the sync machinery (queue dispatch, state management, conflict checks,
checksum computation) on top of raw backend transfer time.

Run:
    pytest tests/benchmarks/test_sync_bench.py -v -s
"""

import tempfile
import time
from pathlib import Path

import pytest

from storage.backends.local import LocalBackend
from storage.cache import CacheManager
from storage.replication import ReplicationManager
from storage.state import StateManager, SyncStatus, FileMetadata
from storage.sync import SyncManager, SyncConfig
from .bench_utils import (
    FILE_SIZES,
    FileFactory,
    LatencyBackend,
    stats_summary,
    timed,
)


# ---------------------------------------------------------------------------
# Fixtures for full-stack sync benchmarks
# ---------------------------------------------------------------------------

class SyncBenchHarness:
    """
    Encapsulates the full sync stack for benchmarking.

    Sets up: state DB, cache manager, replication manager, sync manager,
    and one or more backends.  Provides helpers to inject files and
    measure sync completion time.
    """

    def __init__(self, tmp_path: Path, backends: dict, num_workers: int = 2):
        self.tmp_path = tmp_path
        self.cache_dir = tmp_path / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = tmp_path / "state.db"

        self.state_manager = StateManager(str(self.db_path))
        self.cache_manager = CacheManager(
            self.cache_dir, self.state_manager, max_size_bytes=512 * 1024 * 1024
        )
        self.replication_manager = ReplicationManager(
            backends=backends,
            state_manager=self.state_manager,
        )

        self.sync_config = SyncConfig(
            monitor_interval=0.1,  # Fast polling for benchmarks
            max_retries=1,
            retry_delay=0.1,
        )
        self.sync_manager = SyncManager(
            state_manager=self.state_manager,
            cache_manager=self.cache_manager,
            replication_manager=self.replication_manager,
            num_workers=num_workers,
            config=self.sync_config,
        )

    def start(self):
        self.sync_manager.start()

    def stop(self):
        self.sync_manager.stop()

    def inject_file(self, virtual_path: str, data: bytes, replica_count: int = 1):
        """
        Write a file into cache and mark it dirty, as if the FUSE layer
        had written it.  Returns the cache path.
        """
        cache_path = self.cache_manager.get_cache_path(virtual_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(data)

        now = time.time()
        metadata = FileMetadata(
            virtual_path=virtual_path,
            size=len(data),
            checksum=None,
            last_access=now,
            last_modified=now,
            sync_status=SyncStatus.DIRTY,
            replica_count=replica_count,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        self.state_manager.set_file_metadata(metadata)
        # Set replication config via xattr on cache file so
        # get_inherited_config picks it up.
        if replica_count != 1:
            from storage.state import FileConfig
            config = FileConfig(replica_count=replica_count)
            StateManager.set_file_config(cache_path, config)
        return cache_path

    def inject_and_queue(self, virtual_path: str, data: bytes,
                         replica_count: int = 1, priority: int = 50):
        """Inject a file and immediately queue it for sync."""
        self.inject_file(virtual_path, data, replica_count)
        self.sync_manager.queue_sync(virtual_path, priority=priority, force=True)

    def wait_all_clean(self, vpaths: list, timeout: float = 30.0) -> float:
        """
        Wait until all given vpaths reach CLEAN status.
        Returns elapsed wall-clock time in seconds.
        """
        start = time.perf_counter()
        deadline = time.monotonic() + timeout
        remaining = set(vpaths)

        while remaining and time.monotonic() < deadline:
            for vp in list(remaining):
                meta = self.state_manager.get_file_metadata(vp)
                if meta and meta.sync_status == SyncStatus.CLEAN:
                    remaining.discard(vp)
            if remaining:
                time.sleep(0.01)

        elapsed = time.perf_counter() - start
        if remaining:
            raise TimeoutError(
                f"{len(remaining)} files did not reach CLEAN within {timeout}s"
            )
        return elapsed


# ---------------------------------------------------------------------------
# Sync pipeline throughput tests
# ---------------------------------------------------------------------------

class TestSyncPipelineThroughput:
    """
    Measure how fast the sync pipeline processes a batch of files.

    This answers: "What is the overhead of the sync machinery on top
    of raw backend transfer time?"
    """

    def test_batch_sync_local(self, bench_tmp, file_factory, capsys):
        """Sync a batch of files to a local backend, measure total time."""
        backend_root = bench_tmp / "sync_backend"
        backend_root.mkdir()
        backend = LocalBackend(name="sync-local", root_path=str(backend_root))

        n_files = 50
        file_size = 64 * 1024  # 64KB each

        for n_workers in [1, 2, 4]:
            harness = SyncBenchHarness(
                bench_tmp / f"sync_local_{n_workers}w",
                backends={"sync-local": backend},
                num_workers=n_workers,
            )
            harness.start()

            try:
                # Generate deterministic file content
                rng_data = file_factory._rng.randbytes(file_size)
                vpaths = []
                for i in range(n_files):
                    vp = f"/syncbench/batch_{i:04d}.bin"
                    harness.inject_and_queue(vp, rng_data)
                    vpaths.append(vp)

                elapsed = harness.wait_all_clean(vpaths, timeout=30.0)
                total_bytes = n_files * file_size
                mb_per_s = (total_bytes / (1024 * 1024)) / elapsed
                files_per_s = n_files / elapsed

                print(
                    f"\nSync pipeline (local, {n_workers}w): "
                    f"{n_files} files x {file_size // 1024}KB "
                    f"in {elapsed:.2f}s "
                    f"({files_per_s:.1f} files/s, {mb_per_s:.2f} MB/s)"
                )
            finally:
                harness.stop()

    def test_batch_sync_simulated_remote(self, bench_tmp, file_factory, capsys):
        """
        Sync a batch of files to a simulated remote backend (50ms latency).

        This is the key motivating benchmark: it shows how sync throughput
        degrades when each copy_from_local call pays 50ms of protocol
        overhead.  With N workers, ideal throughput = N * (1 / 0.05) = 20N files/s
        minus overhead.
        """
        backend_root = bench_tmp / "sync_remote_backend"
        backend_root.mkdir()
        inner = LocalBackend(name="sync-inner", root_path=str(backend_root))
        remote = LatencyBackend(inner, latency_s=0.050, jitter_s=0.005)

        n_files = 30
        file_size = 64 * 1024

        for n_workers in [1, 2, 4]:
            harness = SyncBenchHarness(
                bench_tmp / f"sync_remote_{n_workers}w",
                backends={"sync-remote": remote},
                num_workers=n_workers,
            )
            harness.start()

            try:
                rng_data = file_factory._rng.randbytes(file_size)
                vpaths = []
                for i in range(n_files):
                    vp = f"/syncbench_remote/batch_{i:04d}.bin"
                    harness.inject_and_queue(vp, rng_data)
                    vpaths.append(vp)

                elapsed = harness.wait_all_clean(vpaths, timeout=60.0)
                total_bytes = n_files * file_size
                mb_per_s = (total_bytes / (1024 * 1024)) / elapsed
                files_per_s = n_files / elapsed

                # Theoretical max: n_workers * 20 files/s (at 50ms/file)
                theoretical_max = n_workers * 20.0
                efficiency = files_per_s / theoretical_max if theoretical_max > 0 else 0

                print(
                    f"\nSync pipeline (remote 50ms, {n_workers}w): "
                    f"{n_files} files x {file_size // 1024}KB "
                    f"in {elapsed:.2f}s "
                    f"({files_per_s:.1f} files/s, {mb_per_s:.2f} MB/s, "
                    f"efficiency={efficiency:.0%} of theoretical)"
                )
            finally:
                harness.stop()

    def test_sync_overhead_isolation(self, bench_tmp, file_factory, capsys):
        """
        Measure sync machinery overhead by comparing:
        1. Raw copy_from_local time (no sync stack)
        2. Full sync pipeline time (with state, queue, checksums)

        The difference is pure sync overhead.
        """
        backend_root = bench_tmp / "overhead_backend"
        backend_root.mkdir()
        backend = LocalBackend(name="overhead-local", root_path=str(backend_root))

        n_files = 30
        file_size = 64 * 1024
        rng_data = file_factory._rng.randbytes(file_size)

        # 1. Raw copy_from_local
        source_dir = bench_tmp / "overhead_src"
        source_dir.mkdir()
        raw_durations = []
        for i in range(n_files):
            src_path = source_dir / f"raw_{i}.bin"
            src_path.write_bytes(rng_data)
            vpath = f"/raw_copy/file_{i:04d}.bin"
            dur = timed(lambda s=src_path, v=vpath: backend.copy_from_local(s, v))
            raw_durations.append(dur)

        raw_stats = stats_summary("raw_copy_from_local", raw_durations,
                                  bytes_per_op=file_size)

        # 2. Full sync pipeline
        harness = SyncBenchHarness(
            bench_tmp / "overhead_sync",
            backends={"overhead-local": backend},
            num_workers=2,
        )
        harness.start()

        try:
            vpaths = []
            for i in range(n_files):
                vp = f"/sync_overhead/file_{i:04d}.bin"
                harness.inject_and_queue(vp, rng_data)
                vpaths.append(vp)

            sync_elapsed = harness.wait_all_clean(vpaths, timeout=30.0)
        finally:
            harness.stop()

        sync_per_file = sync_elapsed / n_files
        raw_per_file = raw_stats.mean_s
        overhead_ms = (sync_per_file - raw_per_file) * 1000

        print(f"\n--- Sync overhead isolation ---")
        print(f"Raw copy_from_local:  {raw_stats.summary_line()}")
        print(f"Sync pipeline total:  {sync_elapsed:.3f}s for {n_files} files "
              f"({sync_per_file * 1000:.2f}ms/file)")
        print(f"Overhead per file:    {overhead_ms:.2f}ms "
              f"({overhead_ms / (raw_per_file * 1000) * 100:.0f}% of raw transfer)")


# ---------------------------------------------------------------------------
# File size impact on sync pipeline
# ---------------------------------------------------------------------------

class TestSyncFileSizeImpact:
    """
    Measure how file size affects sync pipeline throughput.

    Small files: overhead-dominated (checksum, state updates, queue dispatch)
    Large files: transfer-dominated (copy_from_local)
    """

    # Use smaller subset to keep test time reasonable
    SIZES = {"1KB": 1024, "64KB": 64 * 1024, "1MB": 1024 * 1024}

    def test_throughput_by_size(self, bench_tmp, file_factory, capsys):
        """Print throughput table by file size."""
        backend_root = bench_tmp / "size_backend"
        backend_root.mkdir()
        backend = LocalBackend(name="size-local", root_path=str(backend_root))

        n_files = 20
        n_workers = 2

        print(f"\n\n=== Sync Pipeline Throughput by File Size ({n_workers} workers) ===")
        print(f"{'Size':<8} {'Files':>6} {'Total(s)':>9} {'files/s':>8} {'MB/s':>8}")
        print("-" * 50)

        for size_name, size_bytes in self.SIZES.items():
            harness = SyncBenchHarness(
                bench_tmp / f"size_{size_name}",
                backends={"size-local": backend},
                num_workers=n_workers,
            )
            harness.start()

            try:
                data = file_factory._rng.randbytes(size_bytes)
                vpaths = []
                for i in range(n_files):
                    vp = f"/sizebench/{size_name}/file_{i:04d}.bin"
                    harness.inject_and_queue(vp, data)
                    vpaths.append(vp)

                elapsed = harness.wait_all_clean(vpaths, timeout=60.0)
                total_bytes = n_files * size_bytes
                mb_per_s = (total_bytes / (1024 * 1024)) / elapsed
                files_per_s = n_files / elapsed

                print(
                    f"{size_name:<8} {n_files:>6} {elapsed:>9.3f} "
                    f"{files_per_s:>8.1f} {mb_per_s:>8.2f}"
                )
            finally:
                harness.stop()

        print()
