# Storage Performance Benchmarks

## Overview

These benchmarks measure the performance characteristics of the Storage
filesystem's backend operations, sync pipeline, cache effectiveness, and
the rsync-vs-SFTP transfer comparison.  They were used to guide the
decision to add xxHash checksums, inotify-based invalidation, and rsync
as a transfer backend.

## Baseline Commits

The benchmarks were originally run at these commits:

- **Benchmarking framework**: `31283d5` (test_backend_bench, test_parallelism_bench, test_sync_bench)
- **Critical path analysis**: `fa40b18` (test_critical_path_bench, generate_graphs.py)
- **rsync-vs-SFTP comparison**: `2431677` (test_rsync_vs_sftp_bench, generate_comparison_graphs.py)

The final merged state on master includes all fixes from 8 review rounds.

## Key Results

### Checksum: XXH3-128 vs SHA256

| File Size | SHA256 | XXH3-128 | Speedup | XXH3 MB/s |
|-----------|--------|----------|---------|-----------|
| 64KB | 0.27ms | 0.008ms | 35x | 7,645 |
| 1MB | 4.40ms | 0.13ms | 33x | 7,233 |
| 16MB | 68.8ms | 2.07ms | 33x | 7,780 |

### Sync Pipeline Overhead (1MB file, local backend)

| Component | Before (SHA256) | After (XXH3) |
|-----------|----------------|--------------|
| Checksum | 5.00ms | 0.90ms |
| State DB write | 0.06ms | 0.06ms |
| State DB read | 0.02ms | 0.02ms |
| Transfer | 1.20ms | 1.16ms |
| **Overhead %** | **80.9%** | **45.9%** |

### Cache Effectiveness (50ms simulated remote)

| Metric | Cold (miss) | Warm (hit) | Speedup |
|--------|-------------|------------|---------|
| 64KB read | 158ms | 0.06ms | 2,634x |

### rsync vs SFTP (local, no network)

| File Size | shutil (SFTP) | rsync | Notes |
|-----------|--------------|-------|-------|
| 1KB | 0.41ms | 48.0ms | rsync fork overhead dominates |
| 64KB | 0.46ms | 48.3ms | rsync fork overhead dominates |
| 1MB | 1.30ms | 50.2ms | approaching crossover |
| 16MB | 14.2ms | 73.6ms | rsync overhead still visible |

### rsync Delta Transfer (16MB file)

| Modification | shutil | rsync | Savings |
|-------------|--------|-------|---------|
| 1% modified | 68.9ms | 48.9ms | 29% |
| 50% modified | 96.6ms | 51.1ms | 47% |

### Conclusion

rsync wins only for large files with incremental modifications.
SFTP wins for small files, batch operations, and full syncs.
The hybrid approach (SFTP for metadata + small files, rsync for
large deltas) is optimal.

## How to Run

```bash
# Setup
bash setup-venv.sh
source .venv/bin/activate

# Run all benchmarks (fast mode, no pytest-benchmark warmup)
pytest tests/benchmarks/ --benchmark-disable -v -s

# Run with full pytest-benchmark statistics
pytest tests/benchmarks/ --benchmark-only -v

# Export benchmark data as JSON
pytest tests/benchmarks/ --benchmark-only --benchmark-json=results.json

# Generate graphs (outputs to tests/benchmarks/graphs/)
python tests/benchmarks/generate_graphs.py
python tests/benchmarks/generate_comparison_graphs.py
```

## Benchmark Modules

| Module | What it measures |
|--------|-----------------|
| `test_backend_bench.py` | Isolated backend ops (copy, listdir, stat) across file sizes |
| `test_parallelism_bench.py` | Thread scaling (1/2/4/8 threads) with efficiency tables |
| `test_sync_bench.py` | End-to-end sync pipeline throughput and overhead isolation |
| `test_critical_path_bench.py` | Sync overhead decomposition, invalidation cost, cache hit ratios |
| `test_rsync_vs_sftp_bench.py` | A/B comparison of shutil vs rsync, delta transfer efficiency |
| `test_readdir_bench.py` | Directory listing warm/cold cache performance |

## Graph Generators

The PNG graphs are not checked into the repository.  To regenerate:

```bash
python tests/benchmarks/generate_graphs.py           # overhead, scaling, invalidation, cache
python tests/benchmarks/generate_comparison_graphs.py  # rsync vs SFTP, checksum improvement
```

Output goes to `tests/benchmarks/graphs/` (gitignored).
Requires `matplotlib` (`pip install matplotlib`).
