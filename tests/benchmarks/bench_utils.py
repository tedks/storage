"""
Benchmark utilities for the storage project.

Provides controlled measurement infrastructure:
- LatencyBackend: decorator that adds configurable delay to any backend
- FileFactory: deterministic random file generation (seeded)
- Statistical helpers: p50/p95/p99, confidence intervals, throughput

Design principles (after Berger):
- Randomize to defeat caching/layout effects
- Report distributions, not just means
- Control confounds via explicit latency injection
- Reproducible via seeded RNG
"""

import math
import os
import random
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional

from storage.backends.base import Backend, BackendType


# ---------------------------------------------------------------------------
# LatencyBackend: wraps any Backend, injecting configurable delay
# ---------------------------------------------------------------------------

class LatencyBackend(Backend):
    """
    Decorator that wraps a real backend and injects configurable latency
    before each operation.  Lets us simulate SSH/network overhead on a
    local backend so we can isolate protocol cost from network variance.

    Parameters
    ----------
    inner : Backend
        The real backend to delegate to.
    latency_s : float
        Fixed latency in seconds added before each operation.
    jitter_s : float
        Uniform random jitter added on top of latency_s (0 = deterministic).
    per_byte_s : float
        Per-byte transfer cost (simulates bandwidth limit).
        E.g. 1e-7 means ~10 MB/s throughput ceiling.
    """

    def __init__(self, inner: Backend, latency_s: float = 0.0,
                 jitter_s: float = 0.0, per_byte_s: float = 0.0):
        super().__init__(
            name=f"latency:{inner.name}",
            backend_type=inner.backend_type,
            is_remote=inner.transport_is_remote,
            speed_priority=inner.speed_priority,
            capabilities=inner.capabilities,
        )
        self._inner = inner
        self.latency_s = latency_s
        self.jitter_s = jitter_s
        self.per_byte_s = per_byte_s
        self._rng = random.Random(42)

    def _delay(self, nbytes: int = 0):
        """Sleep to simulate latency + bandwidth cost."""
        delay = self.latency_s
        if self.jitter_s > 0:
            delay += self._rng.uniform(0, self.jitter_s)
        if nbytes > 0 and self.per_byte_s > 0:
            delay += nbytes * self.per_byte_s
        if delay > 0:
            time.sleep(delay)

    # -- Delegated operations with latency injection -----------------------

    def exists(self, virtual_path: str) -> bool:
        self._delay()
        return self._inner.exists(virtual_path)

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        self._delay(size)
        return self._inner.read_file(virtual_path, offset, size)

    def write_file(self, virtual_path: str, data: bytes, offset: int):
        self._delay(len(data))
        self._inner.write_file(virtual_path, data, offset)

    def create_file(self, virtual_path: str):
        self._delay()
        self._inner.create_file(virtual_path)

    def delete_file(self, virtual_path: str):
        self._delay()
        self._inner.delete_file(virtual_path)

    def truncate_file(self, virtual_path: str):
        self._delay()
        self._inner.truncate_file(virtual_path)

    def get_size(self, virtual_path: str) -> int:
        self._delay()
        return self._inner.get_size(virtual_path)

    def mkdir(self, virtual_path: str):
        self._delay()
        self._inner.mkdir(virtual_path)

    def rmdir(self, virtual_path: str):
        self._delay()
        self._inner.rmdir(virtual_path)

    def listdir(self, virtual_path: str) -> list:
        self._delay()
        return self._inner.listdir(virtual_path)

    def listdir_attrs(self, virtual_path: str):
        self._delay()
        return self._inner.listdir_attrs(virtual_path)

    def isdir(self, virtual_path: str) -> bool:
        self._delay()
        return self._inner.isdir(virtual_path)

    def copy_to_local(self, virtual_path: str, local_path: Path):
        size = self._inner.get_size(virtual_path)
        self._delay(size)
        self._inner.copy_to_local(virtual_path, local_path)

    def copy_from_local(self, local_path: Path, virtual_path: str):
        size = local_path.stat().st_size if local_path.exists() else 0
        self._delay(size)
        self._inner.copy_from_local(local_path, virtual_path)

    def rename(self, old_virtual_path: str, new_virtual_path: str):
        self._delay()
        self._inner.rename(old_virtual_path, new_virtual_path)

    def get_free_space(self) -> Optional[int]:
        return self._inner.get_free_space()

    def stat(self, virtual_path: str):
        self._delay()
        return self._inner.stat(virtual_path)

    def sync(self):
        self._inner.sync()

    def get_dir_mtime(self, virtual_path: str):
        self._delay()
        return self._inner.get_dir_mtime(virtual_path)

    def supports_range_reads(self) -> bool:
        return self._inner.supports_range_reads()

    def get_fast_checksum(self, virtual_path: str, allow_download: bool = True) -> str:
        self._delay()
        return self._inner.get_fast_checksum(virtual_path, allow_download=allow_download)

    def set_xattr(self, virtual_path: str, name: str, value: bytes):
        self._delay()
        self._inner.set_xattr(virtual_path, name, value)

    def get_xattr(self, virtual_path: str, name: str):
        self._delay()
        return self._inner.get_xattr(virtual_path, name)


# ---------------------------------------------------------------------------
# FileFactory: deterministic random file generation
# ---------------------------------------------------------------------------

# Standard file sizes for benchmarking.  Named for readability in test output.
FILE_SIZES = {
    "1KB": 1024,
    "64KB": 64 * 1024,
    "1MB": 1024 * 1024,
    "16MB": 16 * 1024 * 1024,
}


class FileFactory:
    """
    Generate deterministic random files for benchmarking.

    Uses a seeded RNG so content is reproducible across runs, defeating
    any content-aware optimizations (dedup, compression) that could skew
    measurements.
    """

    def __init__(self, seed: int = 0xBEEF):
        self._rng = random.Random(seed)

    def make_file(self, path: Path, size_bytes: int) -> Path:
        """Write a file of exactly size_bytes with pseudo-random content."""
        path.parent.mkdir(parents=True, exist_ok=True)
        # Generate in 64KB chunks to avoid huge memory allocation
        chunk_size = 64 * 1024
        remaining = size_bytes
        with open(path, "wb") as f:
            while remaining > 0:
                n = min(chunk_size, remaining)
                f.write(self._rng.randbytes(n))
                remaining -= n
        return path

    def populate_backend(self, backend: Backend, file_count: int,
                         size_bytes: int, prefix: str = "bench") -> List[str]:
        """
        Create files directly on a backend for read benchmarks.

        Returns list of virtual paths created.
        """
        paths = []
        with tempfile.TemporaryDirectory() as tmpdir:
            for i in range(file_count):
                vpath = f"/{prefix}/file_{i:04d}.bin"
                local_path = Path(tmpdir) / f"file_{i:04d}.bin"
                self.make_file(local_path, size_bytes)
                backend.mkdir(f"/{prefix}")
                backend.copy_from_local(local_path, vpath)
                paths.append(vpath)
        return paths

    def populate_directory(self, directory: Path, file_count: int,
                           size_bytes: int, prefix: str = "bench") -> List[Path]:
        """Create files in a local directory.  Returns list of paths."""
        created = []
        subdir = directory / prefix
        subdir.mkdir(parents=True, exist_ok=True)
        for i in range(file_count):
            p = subdir / f"file_{i:04d}.bin"
            self.make_file(p, size_bytes)
            created.append(p)
        return created


# ---------------------------------------------------------------------------
# Statistical helpers
# ---------------------------------------------------------------------------

@dataclass
class BenchResult:
    """Summary statistics for a set of duration measurements."""
    name: str
    n_trials: int
    mean_s: float
    std_s: float
    p50_s: float
    p95_s: float
    p99_s: float
    min_s: float
    max_s: float
    ci95_low_s: float
    ci95_high_s: float
    throughput_ops_per_s: float
    throughput_mb_per_s: Optional[float]  # Only if bytes_per_op provided

    def summary_line(self) -> str:
        ci = f"[{self.ci95_low_s * 1000:.1f}, {self.ci95_high_s * 1000:.1f}]"
        parts = [
            f"{self.name}:",
            f"n={self.n_trials}",
            f"p50={self.p50_s * 1000:.2f}ms",
            f"p95={self.p95_s * 1000:.2f}ms",
            f"p99={self.p99_s * 1000:.2f}ms",
            f"95%CI={ci}ms",
            f"ops/s={self.throughput_ops_per_s:.1f}",
        ]
        if self.throughput_mb_per_s is not None:
            parts.append(f"MB/s={self.throughput_mb_per_s:.2f}")
        return "  ".join(parts)


def percentile(sorted_vals: List[float], pct: float) -> float:
    """Compute percentile from a pre-sorted list."""
    if not sorted_vals:
        return 0.0
    idx = (len(sorted_vals) - 1) * pct / 100.0
    lo = int(math.floor(idx))
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def stats_summary(name: str, durations: List[float],
                  bytes_per_op: Optional[int] = None) -> BenchResult:
    """
    Compute summary statistics from a list of duration measurements.

    Parameters
    ----------
    name : str
        Human-readable label for this measurement.
    durations : list of float
        Raw duration measurements in seconds.
    bytes_per_op : int, optional
        If provided, computes MB/s throughput.
    """
    n = len(durations)
    if n == 0:
        return BenchResult(
            name=name, n_trials=0,
            mean_s=0, std_s=0, p50_s=0, p95_s=0, p99_s=0,
            min_s=0, max_s=0, ci95_low_s=0, ci95_high_s=0,
            throughput_ops_per_s=0, throughput_mb_per_s=None,
        )

    sorted_d = sorted(durations)
    mean = sum(sorted_d) / n
    variance = sum((d - mean) ** 2 for d in sorted_d) / max(n - 1, 1)
    std = math.sqrt(variance)

    # 95% confidence interval using t-distribution approximation
    # For n >= 30, z ~= 1.96; for smaller n, use conservative 2.0
    z = 1.96 if n >= 30 else 2.0
    se = std / math.sqrt(n)
    ci_low = mean - z * se
    ci_high = mean + z * se

    total_time = sum(sorted_d)
    ops_per_s = n / total_time if total_time > 0 else 0

    mb_per_s = None
    if bytes_per_op is not None and total_time > 0:
        total_bytes = bytes_per_op * n
        mb_per_s = (total_bytes / (1024 * 1024)) / total_time

    return BenchResult(
        name=name,
        n_trials=n,
        mean_s=mean,
        std_s=std,
        p50_s=percentile(sorted_d, 50),
        p95_s=percentile(sorted_d, 95),
        p99_s=percentile(sorted_d, 99),
        min_s=sorted_d[0],
        max_s=sorted_d[-1],
        ci95_low_s=ci_low,
        ci95_high_s=ci_high,
        throughput_ops_per_s=ops_per_s,
        throughput_mb_per_s=mb_per_s,
    )


# ---------------------------------------------------------------------------
# Concurrent runner
# ---------------------------------------------------------------------------

def run_concurrent(fn: Callable[[], float], n_threads: int,
                   ops_per_thread: int) -> List[float]:
    """
    Run fn() concurrently across n_threads, each calling it ops_per_thread
    times.  fn() should return its own duration in seconds.

    Returns a flat list of all durations across all threads.
    """
    all_durations: List[List[float]] = [[] for _ in range(n_threads)]
    barrier = threading.Barrier(n_threads)

    def worker(thread_idx: int):
        barrier.wait()  # Synchronize start
        for _ in range(ops_per_thread):
            dur = fn()
            all_durations[thread_idx].append(dur)

    threads = []
    for i in range(n_threads):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Flatten
    return [d for thread_durs in all_durations for d in thread_durs]


def timed(fn: Callable) -> float:
    """Call fn() and return wall-clock duration in seconds."""
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start
