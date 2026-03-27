"""
Rsync vs SFTP: controlled A/B comparison benchmarks.

Methodology (after Berger):
- Same file content (seeded RNG), same sizes, same operation counts
- Randomized trial order to defeat systematic bias
- Multiple trials with statistical summary (p50, p95, CI)
- Controlled latency injection for reproducible "remote" simulation
- Report effect size, not just significance

Run:
    pytest tests/benchmarks/test_rsync_vs_sftp_bench.py -v -s --benchmark-disable
"""

import os
import random
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import pytest

from storage.backends.local import LocalBackend
from storage.backends.base import BackendType
from storage.cache import CacheManager
from storage.replication import ReplicationManager
from storage.state import StateManager, SyncStatus, FileMetadata
from storage.sync import SyncManager, SyncConfig
from .bench_utils import (
    FILE_SIZES,
    FileFactory,
    LatencyBackend,
    run_concurrent,
    stats_summary,
    timed,
    BenchResult,
)


def _has_rsync() -> bool:
    """Check if rsync is available on this system."""
    try:
        subprocess.run(["rsync", "--version"], capture_output=True, timeout=5)
        return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


HAS_RSYNC = _has_rsync()


# ---------------------------------------------------------------------------
# RsyncLocalBackend: uses rsync subprocess for local-to-local transfers
# ---------------------------------------------------------------------------

class RsyncLocalBackend(LocalBackend):
    """
    LocalBackend that uses rsync for copy_to_local and copy_from_local.

    This lets us benchmark rsync's transfer path against shutil.copy2
    (the SFTP-equivalent local path) without requiring SSH.  The overhead
    of rsync's checksumming and delta algorithm is real; only the network
    transport is eliminated.
    """

    def __init__(self, name: str, root_path: str, speed_priority: int = 0,
                 compress: bool = False):
        super().__init__(name, root_path, speed_priority)
        self.backend_type = BackendType.RSYNC
        self._compress = compress

    def copy_to_local(self, virtual_path: str, local_path: Path):
        real_path = self._resolve_path(virtual_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        cmd = ["rsync", "-a", "--partial"]
        if self._compress:
            cmd.append("-z")
        cmd.extend([str(real_path), str(local_path)])
        result = subprocess.run(cmd, capture_output=True, timeout=60)
        if result.returncode != 0:
            raise RuntimeError(f"rsync failed: {result.stderr.decode()}")

    def copy_from_local(self, local_path: Path, virtual_path: str):
        real_path = self._resolve_path(virtual_path)
        real_path.parent.mkdir(parents=True, exist_ok=True)
        cmd = ["rsync", "-a", "--partial"]
        if self._compress:
            cmd.append("-z")
        cmd.extend([str(local_path), str(real_path)])
        result = subprocess.run(cmd, capture_output=True, timeout=60)
        if result.returncode != 0:
            raise RuntimeError(f"rsync failed: {result.stderr.decode()}")


# ---------------------------------------------------------------------------
# Controlled A/B: SFTP-equivalent vs rsync, same file, same conditions
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not HAS_RSYNC, reason="rsync not installed")
class TestUploadComparison:
    """
    A/B comparison of copy_from_local: shutil.copy2 (SFTP path) vs rsync.

    Both operate on local files to isolate the transfer mechanism from
    network variability.  We add latency injection on top to simulate
    remote conditions.
    """

    N_TRIALS = 30

    def _run_comparison(self, bench_tmp, file_factory, size_bytes, size_name,
                        latency_s=0.0, jitter_s=0.0):
        """Run paired comparison and return (sftp_result, rsync_result)."""
        # Shared backend root
        backend_root = bench_tmp / f"cmp_{size_name}"
        backend_root.mkdir(parents=True, exist_ok=True)

        sftp_root = backend_root / "sftp"
        sftp_root.mkdir(exist_ok=True)
        rsync_root = backend_root / "rsync"
        rsync_root.mkdir(exist_ok=True)

        sftp_inner = LocalBackend("sftp", str(sftp_root))
        rsync_inner = RsyncLocalBackend("rsync", str(rsync_root))

        if latency_s > 0:
            sftp_be = LatencyBackend(sftp_inner, latency_s=latency_s, jitter_s=jitter_s)
            rsync_be = LatencyBackend(rsync_inner, latency_s=latency_s, jitter_s=jitter_s)
        else:
            sftp_be = sftp_inner
            rsync_be = rsync_inner

        # Same source file for both
        src_dir = bench_tmp / f"src_{size_name}"
        src_dir.mkdir(exist_ok=True)
        src = file_factory.make_file(src_dir / "data.bin", size_bytes)

        # Interleaved trials (randomized order to defeat systematic effects)
        rng = random.Random(42)
        sftp_durs = []
        rsync_durs = []

        for i in range(self.N_TRIALS):
            order = rng.choice(["sftp_first", "rsync_first"])
            vpath_sftp = f"/trial_{i}_sftp.bin"
            vpath_rsync = f"/trial_{i}_rsync.bin"

            if order == "sftp_first":
                sftp_durs.append(timed(lambda: sftp_be.copy_from_local(src, vpath_sftp)))
                rsync_durs.append(timed(lambda: rsync_be.copy_from_local(src, vpath_rsync)))
            else:
                rsync_durs.append(timed(lambda: rsync_be.copy_from_local(src, vpath_rsync)))
                sftp_durs.append(timed(lambda: sftp_be.copy_from_local(src, vpath_sftp)))

        sftp_stats = stats_summary(f"sftp/{size_name}", sftp_durs, bytes_per_op=size_bytes)
        rsync_stats = stats_summary(f"rsync/{size_name}", rsync_durs, bytes_per_op=size_bytes)

        return sftp_stats, rsync_stats

    def test_upload_comparison_local(self, bench_tmp, file_factory, capsys):
        """Direct local comparison (no latency) -- measures raw mechanism overhead."""
        sizes = [("1KB", 1024), ("64KB", 64*1024), ("1MB", 1024*1024), ("16MB", 16*1024*1024)]

        print("\n\n" + "=" * 78)
        print("UPLOAD (copy_from_local): shutil.copy2 vs rsync -- LOCAL (no latency)")
        print("=" * 78)
        print(f"{'Size':<8} {'SFTP p50':>10} {'rsync p50':>10} {'speedup':>8} "
              f"{'SFTP MB/s':>10} {'rsync MB/s':>10}")
        print("-" * 62)

        for size_name, size_bytes in sizes:
            sftp, rsync = self._run_comparison(
                bench_tmp / f"local_{size_name}", file_factory,
                size_bytes, size_name
            )
            speedup = sftp.p50_s / rsync.p50_s if rsync.p50_s > 0 else float("inf")
            sftp_mb = sftp.throughput_mb_per_s or 0
            rsync_mb = rsync.throughput_mb_per_s or 0

            print(
                f"{size_name:<8} "
                f"{sftp.p50_s * 1000:>9.2f}ms "
                f"{rsync.p50_s * 1000:>9.2f}ms "
                f"{speedup:>7.2f}x "
                f"{sftp_mb:>9.1f} "
                f"{rsync_mb:>9.1f}"
            )

    def test_upload_comparison_simulated_wan(self, bench_tmp, file_factory, capsys):
        """Simulated 50ms WAN -- where rsync's delta advantage shows."""
        sizes = [("1KB", 1024), ("64KB", 64*1024), ("1MB", 1024*1024)]

        print("\n\n" + "=" * 78)
        print("UPLOAD (copy_from_local): shutil.copy2 vs rsync -- WAN (50ms latency)")
        print("=" * 78)
        print(f"{'Size':<8} {'SFTP p50':>10} {'rsync p50':>10} {'speedup':>8} "
              f"{'SFTP p95':>10} {'rsync p95':>10}")
        print("-" * 62)

        for size_name, size_bytes in sizes:
            sftp, rsync = self._run_comparison(
                bench_tmp / f"wan_{size_name}", file_factory,
                size_bytes, size_name,
                latency_s=0.050, jitter_s=0.005,
            )
            speedup = sftp.p50_s / rsync.p50_s if rsync.p50_s > 0 else float("inf")

            print(
                f"{size_name:<8} "
                f"{sftp.p50_s * 1000:>9.2f}ms "
                f"{rsync.p50_s * 1000:>9.2f}ms "
                f"{speedup:>7.2f}x "
                f"{sftp.p95_s * 1000:>9.2f}ms "
                f"{rsync.p95_s * 1000:>9.2f}ms"
            )


@pytest.mark.skipif(not HAS_RSYNC, reason="rsync not installed")
class TestDownloadComparison:
    """A/B comparison of copy_to_local."""

    N_TRIALS = 30

    def test_download_comparison_local(self, bench_tmp, file_factory, capsys):
        sizes = [("1KB", 1024), ("64KB", 64*1024), ("1MB", 1024*1024), ("16MB", 16*1024*1024)]

        print("\n\n" + "=" * 78)
        print("DOWNLOAD (copy_to_local): shutil.copy2 vs rsync -- LOCAL")
        print("=" * 78)
        print(f"{'Size':<8} {'SFTP p50':>10} {'rsync p50':>10} {'speedup':>8} "
              f"{'SFTP MB/s':>10} {'rsync MB/s':>10}")
        print("-" * 62)

        rng = random.Random(42)

        for size_name, size_bytes in sizes:
            be_root = bench_tmp / f"dl_{size_name}"
            be_root.mkdir(exist_ok=True)

            sftp_root = be_root / "sftp"
            sftp_root.mkdir()
            rsync_root = be_root / "rsync"
            rsync_root.mkdir()

            sftp_be = LocalBackend("sftp-dl", str(sftp_root))
            rsync_be = RsyncLocalBackend("rsync-dl", str(rsync_root))

            # Populate both backends with same files
            vpaths = file_factory.populate_backend(
                sftp_be, self.N_TRIALS, size_bytes, prefix="dlbench"
            )
            file_factory.populate_backend(
                rsync_be, self.N_TRIALS, size_bytes, prefix="dlbench"
            )

            cache_sftp = be_root / "cache_sftp"
            cache_sftp.mkdir()
            cache_rsync = be_root / "cache_rsync"
            cache_rsync.mkdir()

            sftp_durs = []
            rsync_durs = []
            for i, vp in enumerate(vpaths):
                dst_sftp = cache_sftp / f"dl_{i}.bin"
                dst_rsync = cache_rsync / f"dl_{i}.bin"

                if rng.random() < 0.5:
                    sftp_durs.append(timed(lambda: sftp_be.copy_to_local(vp, dst_sftp)))
                    rsync_durs.append(timed(lambda: rsync_be.copy_to_local(vp, dst_rsync)))
                else:
                    rsync_durs.append(timed(lambda: rsync_be.copy_to_local(vp, dst_rsync)))
                    sftp_durs.append(timed(lambda: sftp_be.copy_to_local(vp, dst_sftp)))

            sftp = stats_summary(f"sftp-dl/{size_name}", sftp_durs, bytes_per_op=size_bytes)
            rsync = stats_summary(f"rsync-dl/{size_name}", rsync_durs, bytes_per_op=size_bytes)
            speedup = sftp.p50_s / rsync.p50_s if rsync.p50_s > 0 else float("inf")

            print(
                f"{size_name:<8} "
                f"{sftp.p50_s * 1000:>9.2f}ms "
                f"{rsync.p50_s * 1000:>9.2f}ms "
                f"{speedup:>7.2f}x "
                f"{(sftp.throughput_mb_per_s or 0):>9.1f} "
                f"{(rsync.throughput_mb_per_s or 0):>9.1f}"
            )


# ---------------------------------------------------------------------------
# Delta transfer efficiency: the rsync killer feature
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not HAS_RSYNC, reason="rsync not installed")
class TestDeltaTransfer:
    """
    Measure rsync's delta transfer advantage: modify a small portion
    of a large file and re-sync.  SFTP re-sends the whole file;
    rsync should only send the changed blocks.

    This is the single most important benchmark for the SFTP deprecation
    question.
    """

    def test_delta_efficiency(self, bench_tmp, file_factory, capsys):
        """Modify 1% of a file and measure re-sync time."""
        file_sizes = [("1MB", 1024*1024), ("16MB", 16*1024*1024)]
        modify_fractions = [0.01, 0.10, 0.50]  # 1%, 10%, 50%

        print("\n\n" + "=" * 78)
        print("DELTA TRANSFER: re-sync after partial modification")
        print("SFTP always re-sends entire file; rsync sends only changed blocks")
        print("=" * 78)
        print(f"{'Size':<8} {'Modified':>8} {'SFTP p50':>10} {'rsync p50':>10} "
              f"{'speedup':>8} {'rsync saved':>12}")
        print("-" * 66)

        for size_name, size_bytes in file_sizes:
            for frac in modify_fractions:
                be_root = bench_tmp / f"delta_{size_name}_{int(frac*100)}"
                be_root.mkdir(exist_ok=True)

                sftp_root = be_root / "sftp"
                sftp_root.mkdir()
                rsync_root = be_root / "rsync"
                rsync_root.mkdir()

                sftp_be = LocalBackend("sftp-delta", str(sftp_root))
                rsync_be = RsyncLocalBackend("rsync-delta", str(rsync_root))

                # Create identical source file
                src = file_factory.make_file(be_root / "original.bin", size_bytes)

                # Initial sync to both backends (baseline)
                vpath = "/delta_test.bin"
                sftp_be.copy_from_local(src, vpath)
                rsync_be.copy_from_local(src, vpath)

                n_trials = 15
                sftp_durs = []
                rsync_durs = []

                for trial in range(n_trials):
                    # Modify a fraction of the file
                    modified = be_root / f"modified_{trial}.bin"
                    data = src.read_bytes()
                    data_arr = bytearray(data)
                    modify_bytes = int(size_bytes * frac)
                    offset = random.Random(trial).randint(0, size_bytes - modify_bytes)
                    data_arr[offset:offset + modify_bytes] = os.urandom(modify_bytes)
                    modified.write_bytes(bytes(data_arr))

                    # Re-sync with SFTP (whole file)
                    sftp_durs.append(
                        timed(lambda m=modified: sftp_be.copy_from_local(m, vpath))
                    )
                    # Re-sync with rsync (delta)
                    rsync_durs.append(
                        timed(lambda m=modified: rsync_be.copy_from_local(m, vpath))
                    )

                sftp = stats_summary("sftp-delta", sftp_durs, bytes_per_op=size_bytes)
                rsync = stats_summary("rsync-delta", rsync_durs, bytes_per_op=size_bytes)
                speedup = sftp.p50_s / rsync.p50_s if rsync.p50_s > 0 else float("inf")
                saved_pct = (1 - rsync.p50_s / sftp.p50_s) * 100 if sftp.p50_s > 0 else 0

                print(
                    f"{size_name:<8} "
                    f"{frac:>7.0%} "
                    f"{sftp.p50_s * 1000:>9.2f}ms "
                    f"{rsync.p50_s * 1000:>9.2f}ms "
                    f"{speedup:>7.2f}x "
                    f"{saved_pct:>10.1f}%"
                )

        print("\n'rsync saved' = percentage of transfer time eliminated by delta algorithm")


# ---------------------------------------------------------------------------
# Thread scaling with rsync vs SFTP
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not HAS_RSYNC, reason="rsync not installed")
class TestParallelScaling:
    """Compare thread scaling: SFTP vs rsync."""

    THREAD_COUNTS = [1, 2, 4, 8]
    OPS_PER_THREAD = 10
    FILE_SIZE = 64 * 1024

    def test_scaling_comparison(self, bench_tmp, file_factory, capsys):
        """Side-by-side scaling table."""
        sftp_root = bench_tmp / "scale_sftp"
        sftp_root.mkdir()
        rsync_root = bench_tmp / "scale_rsync"
        rsync_root.mkdir()

        sftp_be = LocalBackend("sftp-scale", str(sftp_root))
        rsync_be = RsyncLocalBackend("rsync-scale", str(rsync_root))

        src_dir = bench_tmp / "scale_src"
        src_dir.mkdir()
        src_files = [
            file_factory.make_file(src_dir / f"s_{i}.bin", self.FILE_SIZE)
            for i in range(max(self.THREAD_COUNTS))
        ]

        print("\n\n" + "=" * 78)
        print("THREAD SCALING: SFTP vs rsync (64KB files)")
        print("=" * 78)
        print(f"{'Threads':>7} {'SFTP ops/s':>11} {'rsync ops/s':>12} "
              f"{'SFTP eff%':>10} {'rsync eff%':>11} {'winner':>8}")
        print("-" * 65)

        sftp_baseline = None
        rsync_baseline = None

        for nt in self.THREAD_COUNTS:
            for label, backend in [("sftp", sftp_be), ("rsync", rsync_be)]:
                counter = [0]
                lock = threading.Lock()

                def do_copy(be=backend, srcs=src_files, n=nt):
                    with lock:
                        idx = counter[0]
                        counter[0] += 1
                    src = srcs[idx % n]
                    vpath = f"/scale_{label}/t{idx}.bin"
                    return timed(lambda: be.copy_from_local(src, vpath))

                durs = run_concurrent(do_copy, nt, self.OPS_PER_THREAD)

                result = stats_summary(f"{label}/{nt}T", durs, bytes_per_op=self.FILE_SIZE)

                if label == "sftp":
                    if sftp_baseline is None:
                        sftp_baseline = result.throughput_ops_per_s
                    sftp_ops = result.throughput_ops_per_s
                    sftp_eff = sftp_ops / (sftp_baseline * nt) * 100
                else:
                    if rsync_baseline is None:
                        rsync_baseline = result.throughput_ops_per_s
                    rsync_ops = result.throughput_ops_per_s
                    rsync_eff = rsync_ops / (rsync_baseline * nt) * 100

            winner = "rsync" if rsync_ops > sftp_ops else "sftp"

            print(
                f"{nt:>7} "
                f"{sftp_ops:>11.1f} "
                f"{rsync_ops:>12.1f} "
                f"{sftp_eff:>9.0f}% "
                f"{rsync_eff:>10.0f}% "
                f"{winner:>8}"
            )


# ---------------------------------------------------------------------------
# Sync pipeline end-to-end: full stack with rsync backend
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not HAS_RSYNC, reason="rsync not installed")
class TestSyncPipelineComparison:
    """
    Full sync pipeline comparison: same files through SFTP-path vs rsync-path.
    Measures total wall-clock time including all overhead.
    """

    def _make_harness(self, tmp_path, backend, n_workers=2):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        db_path = tmp_path / "state.db"
        state_mgr = StateManager(str(db_path))
        cache_mgr = CacheManager(cache_dir, state_mgr, 512 * 1024 * 1024)
        repl_mgr = ReplicationManager(
            backends={backend.name: backend},
            state_manager=state_mgr,
        )
        sync_cfg = SyncConfig(monitor_interval=0.1, max_retries=1, retry_delay=0.1)
        sync_mgr = SyncManager(
            state_manager=state_mgr,
            cache_manager=cache_mgr,
            replication_manager=repl_mgr,
            num_workers=n_workers,
            config=sync_cfg,
        )
        return state_mgr, cache_mgr, sync_mgr

    def _inject(self, state_mgr, cache_mgr, sync_mgr, vpath, data):
        cp = cache_mgr.get_cache_path(vpath)
        cp.parent.mkdir(parents=True, exist_ok=True)
        cp.write_bytes(data)
        now = time.time()
        meta = FileMetadata(
            virtual_path=vpath, size=len(data), checksum=None,
            last_access=now, last_modified=now, sync_status=SyncStatus.DIRTY,
            replica_count=1, recomputable=False, recompute_cmd=None,
            encrypted=False, encryption_key_id=None,
        )
        state_mgr.set_file_metadata(meta)
        sync_mgr.queue_sync(vpath, priority=50, force=True)

    def _wait_clean(self, state_mgr, vpaths, timeout=30):
        start = time.perf_counter()
        remaining = set(vpaths)
        deadline = time.monotonic() + timeout
        while remaining and time.monotonic() < deadline:
            for vp in list(remaining):
                m = state_mgr.get_file_metadata(vp)
                if m and m.sync_status == SyncStatus.CLEAN:
                    remaining.discard(vp)
            if remaining:
                time.sleep(0.01)
        return time.perf_counter() - start

    def test_pipeline_comparison(self, bench_tmp, file_factory, capsys):
        """End-to-end sync pipeline: SFTP-path vs rsync-path."""
        n_files = 30
        file_size = 64 * 1024

        print("\n\n" + "=" * 78)
        print("SYNC PIPELINE: end-to-end (30 files x 64KB, 2 workers)")
        print("=" * 78)
        print(f"{'Backend':<14} {'Total(s)':>9} {'files/s':>8} {'MB/s':>8}")
        print("-" * 42)

        data = file_factory._rng.randbytes(file_size)

        for label, backend_cls in [("shutil(SFTP)", LocalBackend),
                                    ("rsync", RsyncLocalBackend)]:
            be_root = bench_tmp / f"pipe_{label}"
            be_root.mkdir(exist_ok=True)

            if backend_cls == RsyncLocalBackend:
                backend = RsyncLocalBackend(f"pipe-{label}", str(be_root))
            else:
                backend = LocalBackend(f"pipe-{label}", str(be_root))

            state_mgr, cache_mgr, sync_mgr = self._make_harness(
                bench_tmp / f"pipe_harness_{label}", backend
            )
            sync_mgr.start()
            try:
                vpaths = []
                for i in range(n_files):
                    vp = f"/pipe/{label}/file_{i:04d}.bin"
                    self._inject(state_mgr, cache_mgr, sync_mgr, vp, data)
                    vpaths.append(vp)

                elapsed = self._wait_clean(state_mgr, vpaths)
                total_bytes = n_files * file_size
                mb_s = (total_bytes / (1024 * 1024)) / elapsed
                files_s = n_files / elapsed

                print(f"{label:<14} {elapsed:>9.3f} {files_s:>8.1f} {mb_s:>8.2f}")
            finally:
                sync_mgr.stop()
                state_mgr.close()

        print()


# ---------------------------------------------------------------------------
# Checksum improvement (xxhash vs sha256 baseline)
# ---------------------------------------------------------------------------

class TestChecksumImprovement:
    """Measure the xxhash improvement over SHA256 baseline."""

    def test_checksum_speedup(self, bench_tmp, file_factory, capsys):
        """Compare xxhash (current) vs SHA256 (old) checksum speed."""
        import hashlib
        import xxhash

        sizes = [("1KB", 1024), ("64KB", 64*1024), ("1MB", 1024*1024),
                 ("16MB", 16*1024*1024)]

        print("\n\n" + "=" * 78)
        print("CHECKSUM: XXH3-128 (current) vs SHA256 (old)")
        print("=" * 78)
        print(f"{'Size':<8} {'SHA256':>10} {'XXH3':>10} {'speedup':>8} "
              f"{'SHA256 MB/s':>12} {'XXH3 MB/s':>10}")
        print("-" * 64)

        for size_name, size_bytes in sizes:
            src = file_factory.make_file(bench_tmp / f"cksum_{size_name}.bin", size_bytes)
            data = src.read_bytes()

            n = 50 if size_bytes <= 1024*1024 else 20

            sha_durs = []
            xxh_durs = []
            for _ in range(n):
                sha_durs.append(timed(lambda: hashlib.sha256(data).hexdigest()))
                xxh_durs.append(timed(lambda: xxhash.xxh3_128(data).hexdigest()))

            sha = stats_summary("sha256", sha_durs, bytes_per_op=size_bytes)
            xxh = stats_summary("xxh3", xxh_durs, bytes_per_op=size_bytes)
            speedup = sha.p50_s / xxh.p50_s if xxh.p50_s > 0 else float("inf")

            print(
                f"{size_name:<8} "
                f"{sha.p50_s * 1000:>9.3f}ms "
                f"{xxh.p50_s * 1000:>9.3f}ms "
                f"{speedup:>7.1f}x "
                f"{(sha.throughput_mb_per_s or 0):>11.0f} "
                f"{(xxh.throughput_mb_per_s or 0):>9.0f}"
            )
