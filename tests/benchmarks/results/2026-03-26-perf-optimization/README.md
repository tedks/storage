# Benchmark Results: Performance Optimization (2026-03-26)

## Context

These benchmarks were run during the SFTP deprecation analysis to evaluate
xxHash checksums, inotify-based cache invalidation, and rsync as a transfer
backend.

**Branch**: `rsync-backend` (merged to master as PRs #45, #46, #47)
**Baseline commit**: `86c27be` (master before changes)
**Final commit**: `541f51a` (master after all three PRs merged)
**Machine**: Linux 6.8.0-50-generic, Python 3.12

## Graphs

| File | What it shows |
|------|---------------|
| `1_sync_overhead_decomposition.png` | Breakdown of sync time: checksum, state DB, transfer (local vs remote) |
| `2_overhead_percentage.png` | Overhead as % of total sync time by file size |
| `3_thread_scaling.png` | Throughput and efficiency curves at 1/2/4/8 threads |
| `4_invalidation_savings.png` | Time saved by longer TTL or push-based invalidation |
| `5_stat_polling_cost.png` | Cost of stat-polling at various file counts and latencies |
| `6_cold_vs_warm.png` | Cache miss vs hit latency (local and 50ms remote) |
| `7_cache_hit_ratio.png` | Hit rate by cache size under Zipf access pattern |
| `8_sync_pipeline_efficiency.png` | Sync workers vs theoretical throughput (50ms remote) |
| `9_delta_transfer_16mb.png` | rsync delta efficiency on 16MB files |
| `10_rsync_overhead.png` | Per-file transfer cost: shutil vs rsync subprocess |
| `11_crossover_analysis.png` | When to use rsync vs SFTP across scenarios |
| `12_checksum_improvement.png` | XXH3-128 vs SHA256 throughput |

## Key Findings

- XXH3-128 is 33x faster than SHA256 (7.7 GB/s vs 232 MB/s)
- rsync has ~48ms fork overhead per invocation; wins only for files >50MB with incremental changes
- Cache provides 2,634x speedup for 64KB files over 50ms remote
- Push-based inotify eliminates 97% of WAN polling cost vs 1s TTL

## How to Regenerate

```bash
source .venv/bin/activate
python tests/benchmarks/generate_graphs.py
python tests/benchmarks/generate_comparison_graphs.py
# Output goes to tests/benchmarks/graphs/ (gitignored)
```
