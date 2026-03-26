"""
Critical path analysis benchmarks.

Answers three questions:
1. How much sync pipeline overhead can be moved off the critical path?
2. How much could inotify/push-based invalidation save vs polling?
3. Are we measuring cache hit rates? How do cold vs warm reads compare?

Run:
    pytest tests/benchmarks/test_critical_path_bench.py -v -s --benchmark-disable
"""

import os
import tempfile
import time
from pathlib import Path

import pytest

from storage.backends.local import LocalBackend
from storage.cache import CacheManager
from storage.replication import ReplicationManager
from storage.state import (
    StateManager, SyncStatus, FileMetadata, FileLocation, FileConfig,
)
from storage.sync import SyncManager, SyncConfig
from .bench_utils import (
    FILE_SIZES,
    FileFactory,
    LatencyBackend,
    stats_summary,
    timed,
)


# ---------------------------------------------------------------------------
# Q1: Sync pipeline overhead decomposition
# ---------------------------------------------------------------------------

class TestSyncOverheadDecomposition:
    """
    Break down where time goes in the sync pipeline to identify
    what can be moved off the critical path.

    Current critical path for write:
      write() -> local cache write (fast)
      release() -> fsync cache file + queue sync task (fast)
      [async] -> sync worker: checksum + state lookup + conflict check
                 + copy_from_local + state update

    The async portion has these components we can measure independently:
    1. Checksum computation (SHA256 of file)
    2. State DB operations (metadata lookup, status update, location record)
    3. Conflict detection (backend checksum comparison)
    4. Backend transfer (copy_from_local)
    5. Queue dispatch overhead
    """

    SIZES = {"1KB": 1024, "64KB": 64 * 1024, "1MB": 1024 * 1024}
    N_TRIALS = 30

    def test_overhead_decomposition(self, bench_tmp, file_factory, capsys):
        """Decompose sync overhead into individual components."""
        backend_root = bench_tmp / "decomp_backend"
        backend_root.mkdir()
        backend = LocalBackend(name="decomp", root_path=str(backend_root))

        db_path = bench_tmp / "decomp_state.db"
        state_mgr = StateManager(str(db_path))

        print("\n\n=== Sync Pipeline Overhead Decomposition ===")
        print(f"{'Size':<8} {'checksum':>10} {'state_w':>10} {'state_r':>10} "
              f"{'transfer':>10} {'total':>10} {'overhead%':>10}")
        print("-" * 72)

        for size_name, size_bytes in self.SIZES.items():
            # Create test file
            src_dir = bench_tmp / f"decomp_src_{size_name}"
            src_dir.mkdir(exist_ok=True)
            src_path = file_factory.make_file(
                src_dir / "test.bin", size_bytes
            )

            # 1. Checksum computation
            checksum_durs = []
            for _ in range(self.N_TRIALS):
                dur = timed(lambda: state_mgr.compute_checksum(src_path))
                checksum_durs.append(dur)

            # 2. State DB write (set_file_metadata)
            state_w_durs = []
            for i in range(self.N_TRIALS):
                now = time.time()
                meta = FileMetadata(
                    virtual_path=f"/decomp/{size_name}/trial_{i}.bin",
                    size=size_bytes,
                    checksum="abc123",
                    last_access=now,
                    last_modified=now,
                    sync_status=SyncStatus.DIRTY,
                    replica_count=1,
                    recomputable=False,
                    recompute_cmd=None,
                    encrypted=False,
                    encryption_key_id=None,
                )
                dur = timed(lambda m=meta: state_mgr.set_file_metadata(m))
                state_w_durs.append(dur)

            # 3. State DB read (get_file_metadata)
            state_r_durs = []
            for i in range(self.N_TRIALS):
                vp = f"/decomp/{size_name}/trial_{i}.bin"
                dur = timed(lambda v=vp: state_mgr.get_file_metadata(v))
                state_r_durs.append(dur)

            # 4. Backend transfer (copy_from_local)
            transfer_durs = []
            for i in range(self.N_TRIALS):
                vp = f"/decomp_upload/{size_name}/trial_{i}.bin"
                dur = timed(
                    lambda s=src_path, v=vp: backend.copy_from_local(s, v)
                )
                transfer_durs.append(dur)

            cksum = stats_summary("checksum", checksum_durs)
            st_w = stats_summary("state_write", state_w_durs)
            st_r = stats_summary("state_read", state_r_durs)
            xfer = stats_summary("transfer", transfer_durs, bytes_per_op=size_bytes)

            total_overhead = cksum.p50_s + st_w.p50_s + st_r.p50_s
            total_with_xfer = total_overhead + xfer.p50_s
            overhead_pct = (total_overhead / total_with_xfer * 100
                           if total_with_xfer > 0 else 0)

            print(
                f"{size_name:<8} "
                f"{cksum.p50_s * 1000:>9.2f}ms "
                f"{st_w.p50_s * 1000:>9.2f}ms "
                f"{st_r.p50_s * 1000:>9.2f}ms "
                f"{xfer.p50_s * 1000:>9.2f}ms "
                f"{total_with_xfer * 1000:>9.2f}ms "
                f"{overhead_pct:>9.1f}%"
            )

        print("\nchecksum = SHA256 of file")
        print("state_w = write FileMetadata to SQLite")
        print("state_r = read FileMetadata from SQLite")
        print("transfer = copy_from_local to backend")
        print("overhead% = (checksum + state_w + state_r) / total")

    def test_overhead_with_latency(self, bench_tmp, file_factory, capsys):
        """
        Same decomposition but with simulated 50ms remote latency.
        Shows how overhead fraction changes when transfer is slow.
        """
        backend_root = bench_tmp / "decomp_remote_backend"
        backend_root.mkdir()
        inner = LocalBackend(name="decomp-inner", root_path=str(backend_root))
        remote = LatencyBackend(inner, latency_s=0.050, jitter_s=0.005)

        db_path = bench_tmp / "decomp_remote_state.db"
        state_mgr = StateManager(str(db_path))

        print("\n\n=== Overhead Decomposition (50ms simulated remote) ===")
        print(f"{'Size':<8} {'checksum':>10} {'state_w':>10} {'state_r':>10} "
              f"{'transfer':>10} {'total':>10} {'overhead%':>10}")
        print("-" * 72)

        for size_name, size_bytes in self.SIZES.items():
            src_dir = bench_tmp / f"decomp_remote_src_{size_name}"
            src_dir.mkdir(exist_ok=True)
            src_path = file_factory.make_file(src_dir / "test.bin", size_bytes)

            n = 15  # fewer trials for latency tests

            cksum_durs = [
                timed(lambda: state_mgr.compute_checksum(src_path))
                for _ in range(n)
            ]
            state_w_durs = []
            for i in range(n):
                now = time.time()
                meta = FileMetadata(
                    virtual_path=f"/decomp_r/{size_name}/t_{i}.bin",
                    size=size_bytes, checksum="abc", last_access=now,
                    last_modified=now, sync_status=SyncStatus.DIRTY,
                    replica_count=1, recomputable=False, recompute_cmd=None,
                    encrypted=False, encryption_key_id=None,
                )
                state_w_durs.append(
                    timed(lambda m=meta: state_mgr.set_file_metadata(m))
                )
            state_r_durs = [
                timed(lambda v=f"/decomp_r/{size_name}/t_{i}.bin":
                      state_mgr.get_file_metadata(v))
                for i in range(n)
            ]
            xfer_durs = [
                timed(lambda s=src_path, v=f"/decomp_r_up/{size_name}/t_{i}.bin":
                      remote.copy_from_local(s, v))
                for i in range(n)
            ]

            cksum = stats_summary("checksum", cksum_durs)
            st_w = stats_summary("state_write", state_w_durs)
            st_r = stats_summary("state_read", state_r_durs)
            xfer = stats_summary("transfer", xfer_durs, bytes_per_op=size_bytes)

            total_overhead = cksum.p50_s + st_w.p50_s + st_r.p50_s
            total_with_xfer = total_overhead + xfer.p50_s
            overhead_pct = (total_overhead / total_with_xfer * 100
                           if total_with_xfer > 0 else 0)

            print(
                f"{size_name:<8} "
                f"{cksum.p50_s * 1000:>9.2f}ms "
                f"{st_w.p50_s * 1000:>9.2f}ms "
                f"{st_r.p50_s * 1000:>9.2f}ms "
                f"{xfer.p50_s * 1000:>9.2f}ms "
                f"{total_with_xfer * 1000:>9.2f}ms "
                f"{overhead_pct:>9.1f}%"
            )


# ---------------------------------------------------------------------------
# Q2: Cache invalidation strategies
# ---------------------------------------------------------------------------

class TestCacheInvalidation:
    """
    Measure cost of different cache invalidation strategies:
    1. Current: 1-second readdir TTL, no file-level invalidation
    2. Polling: periodic stat() of remote files to check mtime
    3. Push-based: inotify on remote → instant invalidation (simulated as zero-cost)

    The key metric is: how much time does the system spend on
    invalidation-related backend calls that could be eliminated?
    """

    def test_readdir_invalidation_cost(self, bench_tmp, file_factory, capsys):
        """
        Measure the cost of readdir cache misses at various TTLs.
        Current TTL is 1s. What if we used 5s? 30s? Push-based (infinite)?
        """
        backend_root = bench_tmp / "inval_backend"
        backend_root.mkdir()
        inner = LocalBackend(name="inval", root_path=str(backend_root))

        # Create directory with files
        n_files = 200
        for i in range(n_files):
            inner.create_file(f"/testdir/file_{i:04d}.txt")

        # Simulate different latencies
        latencies = [
            ("local", 0.0),
            ("LAN_5ms", 0.005),
            ("WAN_50ms", 0.050),
            ("WAN_200ms", 0.200),
        ]

        print("\n\n=== Readdir Invalidation Cost by Backend Latency ===")
        print(f"{'Backend':<12} {'listdir':>10} {'listdir_attrs':>14} "
              f"{'savings_5s':>11} {'savings_30s':>12} {'savings_push':>13}")
        print("-" * 78)

        for lat_name, lat_s in latencies:
            backend = LatencyBackend(inner, latency_s=lat_s)

            # Measure single listdir call
            listdir_durs = [
                timed(lambda: backend.listdir("/testdir"))
                for _ in range(20)
            ]
            listdir_attrs_durs = [
                timed(lambda: backend.listdir_attrs("/testdir"))
                for _ in range(20)
            ]

            ld = stats_summary("listdir", listdir_durs)
            lda = stats_summary("listdir_attrs", listdir_attrs_durs)

            # Current: 1 call/second (1s TTL)
            calls_per_min_1s = 60
            # 5s TTL: 12 calls/min
            calls_per_min_5s = 12
            # 30s TTL: 2 calls/min
            calls_per_min_30s = 2
            # Push-based: 0 calls/min (only on change)
            # Assume 1 change/min for push-based
            calls_per_min_push = 1

            cost_1s = ld.p50_s * calls_per_min_1s
            savings_5s = cost_1s - ld.p50_s * calls_per_min_5s
            savings_30s = cost_1s - ld.p50_s * calls_per_min_30s
            savings_push = cost_1s - ld.p50_s * calls_per_min_push

            print(
                f"{lat_name:<12} "
                f"{ld.p50_s * 1000:>9.2f}ms "
                f"{lda.p50_s * 1000:>13.2f}ms "
                f"{savings_5s * 1000:>10.1f}ms/m "
                f"{savings_30s * 1000:>11.1f}ms/m "
                f"{savings_push * 1000:>12.1f}ms/m"
            )

        print("\nsavings = time saved per minute vs current 1s TTL")

    def test_stat_polling_cost(self, bench_tmp, file_factory, capsys):
        """
        If we polled remote file mtime to detect changes, how much
        would it cost?  Measure stat() at various scales.
        """
        backend_root = bench_tmp / "poll_backend"
        backend_root.mkdir()
        inner = LocalBackend(name="poll", root_path=str(backend_root))

        file_counts = [10, 100, 500]

        print("\n\n=== Stat Polling Cost (per poll cycle) ===")
        print(f"{'Files':<8} {'local':>10} {'LAN_5ms':>10} {'WAN_50ms':>10}")
        print("-" * 40)

        for n_files in file_counts:
            for i in range(n_files):
                inner.create_file(f"/poll_{n_files}/file_{i:04d}.txt")

            results = {}
            for lat_name, lat_s in [("local", 0.0), ("LAN_5ms", 0.005),
                                     ("WAN_50ms", 0.05)]:
                backend = LatencyBackend(inner, latency_s=lat_s)
                paths = [f"/poll_{n_files}/file_{i:04d}.txt"
                         for i in range(n_files)]

                # Stat all files (single pass — extrapolate for more)
                dur = timed(
                    lambda ps=paths: [backend.stat(p) for p in ps]
                )

                results[lat_name] = dur

            print(
                f"{n_files:<8} "
                f"{results['local'] * 1000:>9.1f}ms "
                f"{results['LAN_5ms'] * 1000:>9.1f}ms "
                f"{results['WAN_50ms'] * 1000:>9.1f}ms"
            )

        print("\nThese are per-cycle costs. At 5s polling interval, multiply by 12/min.")
        print("WAN_200ms extrapolated: 10 files=2s, 100 files=20s, 500 files=100s per cycle.")
        print("Push-based (inotify) eliminates this entirely.")


# ---------------------------------------------------------------------------
# Q3: Cache hit/miss and repeated read benchmarks
# ---------------------------------------------------------------------------

class TestCacheEffectiveness:
    """
    Measure cache behavior on repeated reads.
    Cold read = download from backend + cache.
    Warm read = serve from local cache.
    """

    SIZES = {"1KB": 1024, "64KB": 64 * 1024, "1MB": 1024 * 1024}
    N_READS = 20

    def test_cold_vs_warm_read(self, bench_tmp, file_factory, capsys):
        """
        Compare cold read (cache miss → backend fetch) vs warm read
        (cache hit → local file read) across file sizes.
        """
        backend_root = bench_tmp / "cache_backend"
        backend_root.mkdir()
        inner = LocalBackend(name="cache-be", root_path=str(backend_root))

        cache_dir = bench_tmp / "cache_cache"
        cache_dir.mkdir()
        db_path = bench_tmp / "cache_state.db"
        state_mgr = StateManager(str(db_path))
        cache_mgr = CacheManager(cache_dir, state_mgr, 512 * 1024 * 1024)

        print("\n\n=== Cold vs Warm Read Latency ===")
        print(f"{'Size':<8} {'cold_p50':>10} {'warm_p50':>10} {'speedup':>8} "
              f"{'cold_p95':>10} {'warm_p95':>10}")
        print("-" * 60)

        for size_name, size_bytes in self.SIZES.items():
            # Populate backend
            vpaths = file_factory.populate_backend(
                inner, self.N_READS, size_bytes, prefix=f"cache_{size_name}"
            )

            # Cold reads (cache miss)
            cold_durs = []
            for vp in vpaths:
                # Ensure not cached
                cp = cache_mgr.get_cache_path(vp)
                if cp.exists():
                    cp.unlink()
                dur = timed(lambda v=vp: cache_mgr.cache_file(v, inner))
                cold_durs.append(dur)

            # Warm reads (cache hit → local pread)
            warm_durs = []
            for vp in vpaths:
                cp = cache_mgr.get_cache_path(vp)
                dur = timed(lambda p=cp: p.read_bytes())
                warm_durs.append(dur)

            cold = stats_summary("cold", cold_durs, bytes_per_op=size_bytes)
            warm = stats_summary("warm", warm_durs, bytes_per_op=size_bytes)

            speedup = cold.p50_s / warm.p50_s if warm.p50_s > 0 else float("inf")

            print(
                f"{size_name:<8} "
                f"{cold.p50_s * 1000:>9.2f}ms "
                f"{warm.p50_s * 1000:>9.2f}ms "
                f"{speedup:>7.1f}x "
                f"{cold.p95_s * 1000:>9.2f}ms "
                f"{warm.p95_s * 1000:>9.2f}ms"
            )

    def test_cold_vs_warm_with_latency(self, bench_tmp, file_factory, capsys):
        """Same test but with simulated remote latency."""
        backend_root = bench_tmp / "cache_remote_backend"
        backend_root.mkdir()
        inner = LocalBackend(name="cache-remote", root_path=str(backend_root))
        remote = LatencyBackend(inner, latency_s=0.050, jitter_s=0.005)

        cache_dir = bench_tmp / "cache_remote_cache"
        cache_dir.mkdir()
        db_path = bench_tmp / "cache_remote_state.db"
        state_mgr = StateManager(str(db_path))
        cache_mgr = CacheManager(cache_dir, state_mgr, 512 * 1024 * 1024)

        # Populate via inner (no latency during setup)
        vpaths = file_factory.populate_backend(
            inner, self.N_READS, 64 * 1024, prefix="cache_remote"
        )

        print("\n\n=== Cold vs Warm Read (50ms simulated remote) ===")

        # Cold reads through latency backend
        cold_durs = []
        for vp in vpaths:
            cp = cache_mgr.get_cache_path(vp)
            if cp.exists():
                cp.unlink()
            dur = timed(lambda v=vp: cache_mgr.cache_file(v, remote))
            cold_durs.append(dur)

        # Warm reads (local)
        warm_durs = []
        for vp in vpaths:
            cp = cache_mgr.get_cache_path(vp)
            dur = timed(lambda p=cp: p.read_bytes())
            warm_durs.append(dur)

        cold = stats_summary("cold_remote", cold_durs, bytes_per_op=64 * 1024)
        warm = stats_summary("warm_cached", warm_durs, bytes_per_op=64 * 1024)
        speedup = cold.p50_s / warm.p50_s if warm.p50_s > 0 else float("inf")

        print(f"Cold (remote):  {cold.summary_line()}")
        print(f"Warm (cached):  {warm.summary_line()}")
        print(f"Cache speedup:  {speedup:.0f}x")

    def test_cache_hit_ratio_simulation(self, bench_tmp, file_factory, capsys):
        """
        Simulate access patterns and measure effective cache hit ratio.
        Uses Zipf distribution (realistic: most accesses hit few files).
        """
        import random
        rng = random.Random(42)

        backend_root = bench_tmp / "hitrate_backend"
        backend_root.mkdir()
        inner = LocalBackend(name="hitrate", root_path=str(backend_root))

        cache_dir = bench_tmp / "hitrate_cache"
        cache_dir.mkdir()
        db_path = bench_tmp / "hitrate_state.db"
        state_mgr = StateManager(str(db_path))

        n_files = 100
        file_size = 64 * 1024  # 64KB each
        total_file_bytes = n_files * file_size

        # Populate backend
        vpaths = file_factory.populate_backend(
            inner, n_files, file_size, prefix="hitrate"
        )

        print("\n\n=== Cache Hit Ratio by Cache Size (Zipf access pattern) ===")
        print(f"Total working set: {n_files} files x 64KB = "
              f"{total_file_bytes / (1024 * 1024):.1f}MB")
        print(f"{'Cache %':<10} {'Cache MB':>9} {'Hits':>6} {'Misses':>8} "
              f"{'Hit Rate':>9} {'Evictions':>10}")
        print("-" * 58)

        n_accesses = 500

        for cache_pct in [10, 25, 50, 75, 100]:
            cache_bytes = int(total_file_bytes * cache_pct / 100)
            cache_mgr = CacheManager(
                cache_dir / f"c_{cache_pct}",
                state_mgr,
                max_size_bytes=cache_bytes,
            )

            # Generate Zipf-distributed access pattern
            # Lower indices accessed more frequently
            accesses = []
            for _ in range(n_accesses):
                # Zipf: P(k) ~ 1/k
                idx = int(rng.paretovariate(1.0)) % n_files
                accesses.append(vpaths[idx])

            hits = 0
            misses = 0
            evictions_before = 0  # approximate

            for vp in accesses:
                if cache_mgr.is_cached(vp):
                    hits += 1
                    cache_mgr.mark_accessed(vp)
                else:
                    misses += 1
                    cache_mgr.cache_file(vp, inner)

            hit_rate = hits / n_accesses * 100

            print(
                f"{cache_pct:>5}%    "
                f"{cache_bytes / (1024 * 1024):>8.1f} "
                f"{hits:>6} {misses:>8} "
                f"{hit_rate:>8.1f}% "
                f"{'(no eviction)' if cache_pct >= 100 else ''}"
            )

        print(f"\n{n_accesses} accesses with Zipf distribution")
