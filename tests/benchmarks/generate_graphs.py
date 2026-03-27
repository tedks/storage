#!/usr/bin/env python3
"""
Generate benchmark visualization graphs.

Run: .venv/bin/python tests/benchmarks/generate_graphs.py
Outputs PNGs to tests/benchmarks/graphs/
"""

import os
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

GRAPH_DIR = Path(__file__).parent / "graphs"
GRAPH_DIR.mkdir(exist_ok=True)

# Consistent style
plt.rcParams.update({
    'figure.facecolor': '#1a1a2e',
    'axes.facecolor': '#16213e',
    'axes.edgecolor': '#e94560',
    'axes.labelcolor': '#eee',
    'text.color': '#eee',
    'xtick.color': '#ccc',
    'ytick.color': '#ccc',
    'grid.color': '#333',
    'grid.alpha': 0.5,
    'font.size': 11,
    'axes.titlesize': 14,
    'figure.titlesize': 16,
})

COLORS = ['#0f3460', '#e94560', '#53a8b6', '#f5a623', '#7b68ee']


def graph_sync_overhead_decomposition():
    """Q1: Where does sync pipeline time go?"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle('Sync Pipeline Overhead Decomposition', fontweight='bold')

    # Local backend data
    sizes = ['1KB', '64KB', '1MB']
    checksum_local = [0.03, 0.33, 5.00]
    state_w_local = [0.06, 0.06, 0.07]
    state_r_local = [0.02, 0.02, 0.02]
    transfer_local = [0.20, 0.24, 1.20]

    x = np.arange(len(sizes))
    width = 0.2

    ax1.bar(x - 1.5*width, checksum_local, width, label='SHA256 checksum', color=COLORS[0])
    ax1.bar(x - 0.5*width, state_w_local, width, label='State DB write', color=COLORS[1])
    ax1.bar(x + 0.5*width, state_r_local, width, label='State DB read', color=COLORS[2])
    ax1.bar(x + 1.5*width, transfer_local, width, label='Backend transfer', color=COLORS[3])

    ax1.set_xlabel('File Size')
    ax1.set_ylabel('Latency (ms)')
    ax1.set_title('Local Backend')
    ax1.set_xticks(x)
    ax1.set_xticklabels(sizes)
    ax1.legend(fontsize=9)
    ax1.grid(axis='y')
    ax1.set_yscale('log')
    ax1.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.2f'))

    # Remote backend (50ms) data
    checksum_remote = [0.03, 0.32, 4.98]
    state_w_remote = [0.06, 0.06, 0.06]
    state_r_remote = [0.02, 0.02, 0.02]
    transfer_remote = [51.94, 53.55, 54.41]

    ax2.bar(x - 1.5*width, checksum_remote, width, label='SHA256 checksum', color=COLORS[0])
    ax2.bar(x - 0.5*width, state_w_remote, width, label='State DB write', color=COLORS[1])
    ax2.bar(x + 0.5*width, state_r_remote, width, label='State DB read', color=COLORS[2])
    ax2.bar(x + 1.5*width, transfer_remote, width, label='Backend transfer', color=COLORS[3])

    ax2.set_xlabel('File Size')
    ax2.set_ylabel('Latency (ms)')
    ax2.set_title('Remote Backend (50ms latency)')
    ax2.set_xticks(x)
    ax2.set_xticklabels(sizes)
    ax2.legend(fontsize=9)
    ax2.grid(axis='y')
    ax2.set_yscale('log')
    ax2.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.1f'))

    fig.tight_layout()
    fig.savefig(GRAPH_DIR / '1_sync_overhead_decomposition.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPH_DIR / '1_sync_overhead_decomposition.png'}")


def graph_overhead_percentage():
    """Q1 continued: What % of sync time is overhead vs actual transfer?"""
    fig, ax = plt.subplots(figsize=(10, 6))
    fig.suptitle('Sync Overhead as % of Total Time', fontweight='bold')

    sizes = ['1KB', '64KB', '1MB']

    # Local: overhead% from benchmark
    overhead_local = [36.6, 62.9, 80.9]
    # Remote 50ms: overhead%
    overhead_remote = [0.2, 0.8, 8.5]

    x = np.arange(len(sizes))
    width = 0.35

    bars1 = ax.bar(x - width/2, overhead_local, width, label='Local backend',
                   color=COLORS[1], edgecolor='white', linewidth=0.5)
    bars2 = ax.bar(x + width/2, overhead_remote, width, label='Remote (50ms)',
                   color=COLORS[2], edgecolor='white', linewidth=0.5)

    ax.set_xlabel('File Size')
    ax.set_ylabel('Overhead %')
    ax.set_title('(checksum + state ops) / total sync time')
    ax.set_xticks(x)
    ax.set_xticklabels(sizes)
    ax.legend()
    ax.grid(axis='y')

    # Add value labels
    for bar in bars1:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., h + 1,
                f'{h:.1f}%', ha='center', va='bottom', fontsize=10, color='#eee')
    for bar in bars2:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., h + 1,
                f'{h:.1f}%', ha='center', va='bottom', fontsize=10, color='#eee')

    ax.set_ylim(0, 100)

    fig.tight_layout()
    fig.savefig(GRAPH_DIR / '2_overhead_percentage.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPH_DIR / '2_overhead_percentage.png'}")


def graph_thread_scaling():
    """Thread scaling from parallelism benchmarks."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle('Thread Scaling: copy_from_local Throughput', fontweight='bold')

    threads = [1, 2, 4, 8]

    # Data from test_upload_scaling_table output
    local_ops = [3279.9, 1762.9, 694.6, 286.1]
    lan_ops = [177.3, 171.0, 170.3, 140.6]
    wan_ops = [19.6, 6.2, 19.4, 19.5]

    local_eff = [100, 27, 5, 1]
    lan_eff = [100, 48, 24, 10]
    wan_eff = [100, 16, 25, 12]

    # Throughput
    ax1.plot(threads, local_ops, 'o-', color=COLORS[2], label='Local', linewidth=2, markersize=8)
    ax1.plot(threads, lan_ops, 's-', color=COLORS[3], label='LAN (5ms)', linewidth=2, markersize=8)
    ax1.plot(threads, wan_ops, '^-', color=COLORS[1], label='WAN (50ms)', linewidth=2, markersize=8)

    ax1.set_xlabel('Thread Count')
    ax1.set_ylabel('Operations/sec')
    ax1.set_title('Absolute Throughput')
    ax1.set_xticks(threads)
    ax1.legend()
    ax1.grid(True)
    ax1.set_yscale('log')

    # Efficiency
    ax2.plot(threads, local_eff, 'o-', color=COLORS[2], label='Local', linewidth=2, markersize=8)
    ax2.plot(threads, lan_eff, 's-', color=COLORS[3], label='LAN (5ms)', linewidth=2, markersize=8)
    ax2.plot(threads, wan_eff, '^-', color=COLORS[1], label='WAN (50ms)', linewidth=2, markersize=8)

    # Ideal scaling line
    ax2.plot(threads, [100, 100, 100, 100], '--', color='gray', alpha=0.5, label='Ideal (100%)')

    ax2.set_xlabel('Thread Count')
    ax2.set_ylabel('Scaling Efficiency %')
    ax2.set_title('Efficiency (throughput / threads / baseline)')
    ax2.set_xticks(threads)
    ax2.set_ylim(0, 110)
    ax2.legend()
    ax2.grid(True)

    fig.tight_layout()
    fig.savefig(GRAPH_DIR / '3_thread_scaling.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPH_DIR / '3_thread_scaling.png'}")


def graph_invalidation_savings():
    """Q2: Cache invalidation strategy comparison."""
    fig, ax = plt.subplots(figsize=(10, 6))
    fig.suptitle('Cache Invalidation: Time Saved vs Current 1s TTL', fontweight='bold')

    backends = ['Local', 'LAN\n(5ms)', 'WAN\n(50ms)', 'WAN\n(200ms)']

    # ms saved per minute (from benchmark output)
    savings_5s = [7.8, 259.5, 2419.6, 9619.8]
    savings_30s = [9.4, 313.5, 2923.7, 11623.9]
    savings_push = [9.6, 318.9, 2974.1, 11824.3]

    x = np.arange(len(backends))
    width = 0.25

    ax.bar(x - width, savings_5s, width, label='5s TTL', color=COLORS[2])
    ax.bar(x, savings_30s, width, label='30s TTL', color=COLORS[3])
    ax.bar(x + width, savings_push, width, label='Push-based (inotify)', color=COLORS[1])

    ax.set_xlabel('Backend Type')
    ax.set_ylabel('Time saved (ms/minute)')
    ax.set_title('Savings from longer TTL or push-based invalidation\n(readdir calls saved per minute)')
    ax.set_xticks(x)
    ax.set_xticklabels(backends)
    ax.legend()
    ax.grid(axis='y')
    ax.set_yscale('log')
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.0f'))

    fig.tight_layout()
    fig.savefig(GRAPH_DIR / '4_invalidation_savings.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPH_DIR / '4_invalidation_savings.png'}")


def graph_stat_polling_cost():
    """Q2: Cost of mtime-polling invalidation."""
    fig, ax = plt.subplots(figsize=(10, 6))
    fig.suptitle('Cost of Stat-Polling Cache Invalidation (per cycle)', fontweight='bold')

    file_counts = [10, 100, 500]

    # From benchmark (ms)
    local = [0.2, 2.6, 13.0]  # extrapolated 500
    lan = [52.0, 519.8, 2599.0]
    wan = [502.1, 5020.8, 25104.0]

    ax.plot(file_counts, local, 'o-', color=COLORS[2], label='Local', linewidth=2, markersize=8)
    ax.plot(file_counts, lan, 's-', color=COLORS[3], label='LAN (5ms)', linewidth=2, markersize=8)
    ax.plot(file_counts, wan, '^-', color=COLORS[1], label='WAN (50ms)', linewidth=2, markersize=8)

    # Add horizontal line for "unacceptable" threshold
    ax.axhline(y=5000, color='red', linestyle='--', alpha=0.5, label='5s budget (unusable)')

    ax.set_xlabel('Number of Files')
    ax.set_ylabel('Poll Cycle Time (ms)')
    ax.set_title('Stat-polling is O(n) per file -- scales terribly over WAN')
    ax.set_yscale('log')
    ax.legend()
    ax.grid(True)

    fig.tight_layout()
    fig.savefig(GRAPH_DIR / '5_stat_polling_cost.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPH_DIR / '5_stat_polling_cost.png'}")


def graph_cold_vs_warm():
    """Q3: Cache effectiveness - cold vs warm reads."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle('Cache Effectiveness: Cold vs Warm Reads', fontweight='bold')

    # Local backend
    sizes = ['1KB', '64KB', '1MB']
    cold_local = [0.30, 0.35, 1.29]
    warm_local = [0.04, 0.06, 0.27]

    x = np.arange(len(sizes))
    width = 0.35

    ax1.bar(x - width/2, cold_local, width, label='Cold (cache miss)', color=COLORS[1])
    ax1.bar(x + width/2, warm_local, width, label='Warm (cache hit)', color=COLORS[2])

    ax1.set_xlabel('File Size')
    ax1.set_ylabel('Latency (ms)')
    ax1.set_title('Local Backend')
    ax1.set_xticks(x)
    ax1.set_xticklabels(sizes)
    ax1.legend()
    ax1.grid(axis='y')

    # Speedup annotations
    for i, (c, w) in enumerate(zip(cold_local, warm_local)):
        speedup = c / w
        ax1.text(i, max(c, w) + 0.05, f'{speedup:.1f}x',
                ha='center', va='bottom', fontsize=10, fontweight='bold', color='#f5a623')

    # Remote backend (50ms)
    cold_remote = 158.03
    warm_remote = 0.06
    speedup_remote = cold_remote / warm_remote

    categories = ['Cold\n(remote fetch)', 'Warm\n(cached)']
    values = [cold_remote, warm_remote]

    bars = ax2.bar(categories, values, color=[COLORS[1], COLORS[2]],
                   edgecolor='white', linewidth=0.5, width=0.5)
    ax2.set_ylabel('Latency (ms)')
    ax2.set_title(f'Remote Backend (50ms) -- 64KB files\n{speedup_remote:.0f}x speedup')
    ax2.set_yscale('log')
    ax2.grid(axis='y')

    # Value labels
    for bar, val in zip(bars, values):
        ax2.text(bar.get_x() + bar.get_width()/2., val * 1.3,
                f'{val:.2f}ms', ha='center', va='bottom', fontsize=11, color='#eee')

    fig.tight_layout()
    fig.savefig(GRAPH_DIR / '6_cold_vs_warm.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPH_DIR / '6_cold_vs_warm.png'}")


def graph_cache_hit_ratio():
    """Q3: Cache hit ratio by cache size."""
    fig, ax = plt.subplots(figsize=(10, 6))
    fig.suptitle('Cache Hit Ratio by Cache Size (Zipf Access Pattern)', fontweight='bold')

    cache_pct = [10, 25, 50, 75, 100]
    hit_rates = [83.6, 90.0, 93.4, 92.4, 91.2]

    ax.plot(cache_pct, hit_rates, 'o-', color=COLORS[1], linewidth=2, markersize=10)
    ax.fill_between(cache_pct, hit_rates, alpha=0.2, color=COLORS[1])

    ax.set_xlabel('Cache Size (% of working set)')
    ax.set_ylabel('Cache Hit Rate (%)')
    ax.set_title('100 files x 64KB, 500 accesses, Zipf(1.0) distribution\n'
                 'Even 10% cache achieves 84% hit rate due to heavy-tail access')
    ax.set_ylim(75, 100)
    ax.set_xticks(cache_pct)
    ax.set_xticklabels([f'{p}%' for p in cache_pct])
    ax.grid(True)

    # Annotate key points
    ax.annotate(f'{hit_rates[0]}%', (cache_pct[0], hit_rates[0]),
                textcoords="offset points", xytext=(15, -15),
                fontsize=12, fontweight='bold', color=COLORS[3])
    ax.annotate(f'{hit_rates[2]}%', (cache_pct[2], hit_rates[2]),
                textcoords="offset points", xytext=(15, 5),
                fontsize=12, fontweight='bold', color=COLORS[3])

    fig.tight_layout()
    fig.savefig(GRAPH_DIR / '7_cache_hit_ratio.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPH_DIR / '7_cache_hit_ratio.png'}")


def graph_sync_pipeline_efficiency():
    """Sync pipeline efficiency from sync bench."""
    fig, ax = plt.subplots(figsize=(10, 6))
    fig.suptitle('Sync Pipeline: Workers vs Efficiency (50ms Remote)', fontweight='bold')

    workers = [1, 2, 4]

    # From test_batch_sync_simulated_remote
    files_per_s = [8.6, 17.3, 28.0]
    efficiency = [43, 43, 35]
    theoretical = [20, 40, 80]

    x = np.arange(len(workers))
    width = 0.3

    ax.bar(x - width/2, files_per_s, width, label='Actual files/s',
           color=COLORS[1], edgecolor='white')
    ax.bar(x + width/2, theoretical, width, label='Theoretical max',
           color=COLORS[2], alpha=0.4, edgecolor='white')

    # Add efficiency labels
    for i, (actual, eff) in enumerate(zip(files_per_s, efficiency)):
        ax.text(i - width/2, actual + 1, f'{eff}%\neff.',
                ha='center', va='bottom', fontsize=10, color=COLORS[3])

    ax.set_xlabel('Sync Workers')
    ax.set_ylabel('Files/second')
    ax.set_title('Sync overhead consumes 57-65% of theoretical throughput\n'
                 '(checksum + state DB + conflict check per file)')
    ax.set_xticks(x)
    ax.set_xticklabels(workers)
    ax.legend()
    ax.grid(axis='y')

    fig.tight_layout()
    fig.savefig(GRAPH_DIR / '8_sync_pipeline_efficiency.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPH_DIR / '8_sync_pipeline_efficiency.png'}")


if __name__ == '__main__':
    graph_sync_overhead_decomposition()
    graph_overhead_percentage()
    graph_thread_scaling()
    graph_invalidation_savings()
    graph_stat_polling_cost()
    graph_cold_vs_warm()
    graph_cache_hit_ratio()
    graph_sync_pipeline_efficiency()
    print(f"\nAll graphs saved to {GRAPH_DIR}/")
