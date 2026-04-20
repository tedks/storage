# Storage Refactor Roadmap (Stacked PR Plan)

## Assumptions

1. Storage semantics remain stable: cache-first writes, async replica sync, xattr policy inheritance.
2. Performance focus is syscall/IO latency and throughput under large caches and many files.
3. Availability and debuggability matter more than maximal novelty.
4. Any database is an acceleration index, not the source of truth for policy.

## Current System Decomposition

The current implementation combines four primitives in one layer (`StorageFS`):

1. Namespace and policy resolution (path parsing, inherited xattrs, replica decisions)
2. Operational indexing (dirty set, locations, LRU, sync state)
3. Data movement (cache/backend copies, checksums)
4. Scheduling and convergence (sync queues, retries, conflict projection)

The refactor separates these concerns so each can be optimized independently.

## Holistic Refactor Phases

### Phase 1: State and Sync Hot Path Stabilization

Goals:

1. Reduce write amplification from metadata updates.
2. Fix known correctness edge cases in sync/state utility code.
3. Preserve current external behavior.

Scope:

1. SQLite PRAGMA tuning and safer transactional defaults.
2. Batched/debounced access-time updates.
3. Fix `replicate_file()` missing `time` import and sync wait timeout semantics.

Success criteria:

1. Fewer commits under read-heavy workloads.
2. Existing tests pass.
3. New unit tests cover debounced access updates and timeout behavior.

### Phase 2: O(1) Cache Accounting and Eviction Control

Goals:

1. Remove repeated full-cache `os.walk` scans from hot path.
2. Keep cache accounting correct through create, cache, unlink, truncate, and eviction.

Scope:

1. Introduce tracked cache byte counter (state + in-memory reconciliation).
2. Replace per-request full scan checks with counter-based checks.
3. Add consistency checks/rebuild path for crash recovery.

Success criteria:

1. `cache_file()` avoids full cache walk on steady state.
2. Cache size remains accurate after mixed file operations.

### Phase 3: Directory Listing and Conflict Projection Efficiency

Goals:

1. Reduce remote listing latency and checksum overhead.
2. Keep conflict surfacing behavior equivalent.

Scope:

1. Prefer `listdir_attrs()` backends when available.
2. Schedule checksums only for entries with multi-backend collisions.
3. Cache directory snapshots with freshness checks.

Success criteria:

1. Lower warm/cold `readdir` latency in benchmarks.
2. Conflict view tests still pass.

### Phase 4: Event-Driven Sync Scheduler

Goals:

1. Eliminate periodic full dirty-file scans.
2. Make sync queueing deterministic and lower overhead.

Scope:

1. Queue on transitions to `DIRTY`.
2. Recovery scan only on startup.
3. Explicit backpressure and retry buckets.

Success criteria:

1. No monitor-loop full-table polling in steady state.
2. Fewer redundant queue operations.

### Phase 5: Module Boundary Extraction

Goals:

1. Break `StorageFS` into composable services.
2. Improve testability and pave language-migration path.

Scope:

1. Extract `PolicyResolver`, `IndexStore`, `ConflictEngine`, `DirViewService`, `SyncOrchestrator`.
2. Keep wire-level FUSE contract unchanged.

Success criteria:

1. Smaller `StorageFS` with explicit dependencies.
2. Per-service unit tests with minimal FUSE coupling.

### Phase 6: Metadata Architecture Hardening (xattr + local index)

Goals:

1. Keep xattrs as durable policy source of truth.
2. Keep local index as rebuildable operational accelerator.

Scope:

1. Define authoritative fields in xattrs vs index.
2. Add index rebuild tooling from cache/backends.
3. Add invariants and verification tooling.

Success criteria:

1. System can recover if local DB is deleted/corrupted.
2. Policy behavior remains xattr-defined.

Authority model:

1. Cache file path and file stat are authoritative for cache presence, file identity, byte size, and mtime.
2. `user.storage.config` remains the durable policy source of truth for placement/recompute/encryption policy.
3. `user.storage.metadata` on cached files is the durable snapshot for cache-local file metadata such as checksum, sync status, and policy-derived file flags.
4. SQLite remains authoritative only for operational acceleration: LRU/access ordering, queue scans, and remembered remote locations.

Recovery behavior:

1. `storage debug rebuild-index` reconstructs `files` rows and cache `file_locations` rows from cache files plus `user.storage.metadata`.
2. If a cache file is missing `user.storage.metadata` but the current local index still has a row for that path, rebuild backfills the xattr from the existing row and current file stat.
3. If a cache file is missing `user.storage.metadata` and there is no surviving local row, rebuild skips it rather than guessing and risking indexing derived cache artifacts.
4. `storage debug verify-index` reports divergence between cache files, cache metadata xattrs, and SQLite rows without mutating state.

### Phase 7: Language Migration Spikes (Rust, Go, OCaml)

Goals:

1. Replace assumptions with measured feasibility.
2. Decide migration boundary by evidence.

Scope:

1. Implement identical micro-daemon spikes for one hot path primitive in Python, Rust, Go, OCaml.
2. Compare latency/throughput, complexity, deployability, maintenance burden.

Success criteria:

1. Benchmark + implementation complexity report.
2. Final recommendation with migration path and rollback plan.

## Language Tradeoff Baseline

### Python

Strengths:

1. Fast iteration and strong ecosystem.
2. Easy orchestration and CLI development.

Weaknesses:

1. Hot-path overhead in syscall-heavy loops.
2. Concurrency model limits under high FUSE request rates.

### Rust

Strengths:

1. Highest performance ceiling with memory-safety guarantees.
2. Excellent for daemon/data-plane correctness under concurrency.

Weaknesses:

1. Highest implementation complexity and migration cost.
2. Slower team iteration if Rust maturity is low.

### Go

Strengths:

1. Strong practical concurrency and deployment ergonomics.
2. Good performance with lower migration cost than Rust.

Weaknesses:

1. Less low-level control than Rust for certain zero-copy patterns.
2. Type system less expressive for some invariants.

### OCaml

Strengths:

1. Strong type-driven design and native compilation.
2. Good correctness ergonomics for complex logic.

Weaknesses:

1. Smaller ecosystem for this exact storage/FUSE domain.
2. Highest operational/tooling adoption risk for most teams.

## Decision on Local Database

Recommendation: retain a local DB as a rebuildable index while using xattrs as policy source of truth.

Rationale:

1. xattrs alone are poor for global queries (`dirty`, LRU, location fanout).
2. DB gives efficient set/ordering queries and queue coordination.
3. Rebuildability prevents DB from becoming irreplaceable state.

## Planned PR Stack

Base chain:

1. `main` <- `refactor/phase-1-state-hot-path`
2. `refactor/phase-1-state-hot-path` <- `refactor/phase-2-cache-accounting`
3. `refactor/phase-2-cache-accounting` <- `refactor/phase-3-readdir-conflict-efficiency`
4. `refactor/phase-3-readdir-conflict-efficiency` <- `refactor/phase-4-event-driven-sync`
5. `refactor/phase-4-event-driven-sync` <- `refactor/phase-5-service-boundaries`
6. `refactor/phase-5-service-boundaries` <- `refactor/phase-6-metadata-hardening`
7. `refactor/phase-6-metadata-hardening` <- `refactor/phase-7-language-spikes`

Each PR remains reviewable and mergeable independently.
