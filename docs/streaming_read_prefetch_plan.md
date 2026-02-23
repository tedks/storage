# Streaming Read + Background Prefetch Plan

## Problem Statement

Current uncached reads block until a full file download finishes.

- `StorageFS.open()` calls `_ensure_file_in_cache()` before returning a file descriptor.
- `_ensure_file_in_cache()` downloads entire files via `CacheManager.cache_file()`.
- `CacheManager.cache_file()` calls backend `copy_to_local()`, which is whole-file transfer (`shutil.copy2` for local, `sftp.get` for SSH).

Impact:

1. Time-to-first-byte equals full-file transfer time.
2. Large remote files make filesystem access feel stalled.
3. Parallel readers duplicate cold-start penalty.

## Goal

Provide immediate, streaming reads for uncached files while prefetching the rest of the file into cache in the background.

Target behavior:

1. `open()` returns quickly on cold files.
2. `read(offset, size)` returns requested data as soon as that range can be fetched.
3. Background prefetch fills the remaining file into cache.
4. Once complete, file becomes a normal cached file with existing LRU/sync metadata semantics.

## Non-Goals

1. No protocol-level resume across process restarts in initial implementation.
2. No write-through changes in this phase (write path remains cache-first as-is).
3. No backend protocol redesign beyond additive capability hooks.

## Constraints and Assumptions

1. FUSE callbacks currently run with `nothreads=True`; this means streaming implementation must use internal threads safely without expecting parallel FUSE calls.
2. Existing backend API already exposes range reads via `Backend.read_file(path, offset, size)`.
3. Cache size limits must still be respected before streaming starts.
4. Integrity/conflict machinery should continue to work with final cached files.

## High-Level Design

## Primitive Split

1. `RangeFetcher`: fetches exact byte ranges from selected backend.
2. `PrefetchSession`: tracks partial-cache file state, available ranges, background prefetch progress, and errors.
3. `SessionRegistry`: deduplicates concurrent readers for same path.
4. `CacheFinalizer`: atomically promotes partial file into normal cache path and updates state metadata/locations.

## Chosen Strategy

Use a **partial local cache file + range-availability map**:

1. For uncached opens, create or attach to `PrefetchSession`.
2. Session creates `cache_path + ".storage.partial"`, pre-sizes/truncates to full remote size.
3. Background prefetch thread streams chunks sequentially (e.g., 1 MiB).
4. Foreground `read()` ensures requested range exists:
   - if already available: read locally
   - else synchronously fetch missing chunks via `Backend.read_file`, write to partial file, mark range available
5. When full file is available, atomically rename partial file to final cache path and mark cache/state locations.

This avoids waiting for full transfer while preserving eventual full-cache behavior.

## Detailed Execution Plan

### Phase 0: Baseline and Safety Scaffolding

1. Add read-path instrumentation counters:
   - open latency
   - first-read latency
   - bytes served from remote-range-fetch vs local cache
   - background-prefetch completion time
2. Add config toggles in CLI/env (default off first):
   - `--streaming-reads`
   - chunk size
   - max concurrent prefetch sessions
3. Add structured logs for session lifecycle.

Deliverables:

1. No behavior change.
2. Metrics + logs ready for A/B comparison.

### Phase 1: Backend Capability and Range Fetch Layer

1. Add backend capability check: `supports_range_reads()` (default true where `read_file` is implemented).
2. Add thin `RangeFetcher` utility in `storage/fs.py` or new module:
   - normalizes retries and errors
   - enforces bounded chunk size
3. Keep fallback to legacy whole-file cache path when capability absent.

Deliverables:

1. Deterministic range fetch API.
2. No user-visible changes yet.

### Phase 2: Prefetch Session Model

1. Introduce `PrefetchSession` dataclass:
   - canonical path, backend name, file size
   - partial cache path, final cache path
   - `intervals` map for available byte ranges
   - condition variable + lock
   - in-flight ranges map
   - error state and completion flag
2. Add `self._prefetch_sessions` registry and lock in `StorageFS`.
3. Implement attach/create session semantics for concurrent opens.

Edge handling:

1. Multiple readers share one session.
2. Session survives until finalization or terminal error.
3. Refcount tracks active FDs.

### Phase 3: Open/Read Integration

1. Modify `open()`:
   - if fully cached: unchanged
   - if uncached and streaming enabled:
     - choose backend
     - initialize/attach session
     - open partial file locally and return fd immediately
2. Modify `read()` for streaming FDs:
   - ensure requested range is available; trigger sync range fetch if needed
   - then read bytes from partial file and return
3. Ensure backend-suffixed conflict views still work correctly (legacy path fallback for first cut if needed).

### Phase 4: Background Prefetch Worker + Finalization

1. Start session prefetch thread after first open.
2. Thread reads sequential chunks until EOF or error.
3. On completion:
   - flush/fsync partial file
   - atomic rename to final cache path
   - add/update metadata and cache/backend locations in state
4. Release path behavior:
   - decref session
   - if no readers and prefetch failed: cleanup partial file + registry
   - if no readers and prefetch running: allow continuation (configurable)

### Phase 5: Cache Accounting and Eviction Compatibility

1. Ensure partial files count against cache size accounting from first byte.
2. Prevent eviction/finalization races with active session files.
3. Pin active partial files from eviction.
4. Add explicit cleanup policy for abandoned partial files on mount init.

### Phase 6: Recovery and Failure Semantics

1. Startup scan for stale `*.storage.partial` files:
   - either delete or resume (initially delete + log)
2. Mid-stream backend failure:
   - serve available already-fetched ranges
   - return `EIO` when an unfetched requested range cannot be retrieved
3. Timeout/retry strategy for range fetches.
4. Close/unmount cancellation barriers to avoid background writer races.

### Phase 7: Rollout

1. Keep behind feature flag through full validation.
2. Enable in staging/system tests first.
3. Promote to default only after benchmark and reliability gates pass.

## Concurrency and Correctness Invariants

1. A byte range is marked available only after successful local write + flush.
2. No read returns bytes from an unavailable range.
3. Only one background prefetch thread per `(canonical_path, backend)` session.
4. Finalization (`partial -> final`) is atomic and occurs only when full range `[0, size)` is available.
5. Session registry state remains consistent through open/read/release/destroy.

## Security and Abuse Considerations

1. Prevent path traversal by reusing existing canonical path resolution.
2. Bound memory and thread usage:
   - max active sessions
   - bounded in-flight range requests per session
3. Defend against read-amplification abuse via random-seek workload:
   - coalesce nearby ranges
   - clamp max read chunk size
4. Avoid backend starvation:
   - reserve capacity for foreground range fetches
   - enforce a global prefetch concurrency limit

## Claude Review (ask-agent) and Plan Deltas

Review method:

1. Used `~/.codex/skills/ask-agent/scripts/agent-query.sh claude` in this worktree.
2. Focused prompts succeeded for concrete risk and missing-test feedback.
3. Longer prompts intermittently timed out, so feedback was captured in smaller focused queries.

Captured Claude risks adopted into this plan:

1. **Memory pressure from unbounded prefetch**: add strict session + buffer limits and back-pressure.
2. **Prefetch/eviction race**: pin active partial files from eviction until finalization/error cleanup.
3. **False sequential detection/prefetch waste**: add re-anchoring on seeks and dynamic prefetch suppression for random patterns.
4. **FUSE/backend starvation**: foreground reads get priority over background prefetch.
5. **Teardown/use-after-free races**: explicit cancellation/join barriers for close, unmount, and destroy paths.

Captured Claude missing tests adopted into this plan:

1. Seek past prefetched region and verify synchronous fallback + prefetch re-anchor.
2. Close/cancel semantics: no leaks, no dangling writers.
3. Back-pressure behavior under constrained memory/buffer budget.
4. Backend error in prefetch path surfaces at user read boundary.
5. Final short-read and EOF correctness when file size is not chunk-aligned.

## Exhaustive Test Plan

### A) Unit Tests

1. Range interval merge/split logic.
2. Session state transitions: created -> prefetching -> complete/error -> finalized/cleaned.
3. In-flight dedup: concurrent reads for same missing range trigger one remote fetch.
4. Error propagation from background and foreground fetch paths.
5. Prefetch suppression/re-anchor policy for random/strided reads.
6. Session teardown and cancellation join behavior (close/unmount/destroy).

### B) Filesystem Integration Tests (pytest)

1. Cold open returns before full copy completes (inject slow backend).
2. First read returns partial data while total file still downloading.
3. Sequential reads hit prefetched data after warmup.
4. Random seek reads fetch targeted ranges correctly.
5. Concurrent opens/readers share one session and do not duplicate full download.
6. Final cache promotion and state metadata correctness.
7. Backward compatibility with feature flag off (legacy behavior unchanged).
8. Close during in-flight prefetch leaves no orphaned worker or corrupt partial file.
9. Seek beyond prefetched window re-anchors and serves correct bytes.
10. Final short chunk/EOF correctness on non-aligned file size.

### C) Benchmark Tests

1. Time-to-first-byte baseline vs streaming mode for 10 MiB / 100 MiB / 1 GiB files.
2. Total transfer completion time impact.
3. CPU overhead and lock contention under 1/4/16 concurrent readers.

### D) Docker System Tests

Use docker-compose-based testbed with:

1. Storage mount container
2. One or more SSH backend containers (`linuxserver/openssh-server` or equivalent)
3. Optional `tc netem` shaping for latency/bandwidth control

Scenarios:

1. High-latency remote backend, verify first-byte improvement.
2. Packet loss/intermittent connectivity during prefetch.
3. Parallel readers and large-file stress.
4. Enforced buffer/session limits under synthetic pressure (no OOM/leaks).

Automate via `scripts/system/test_streaming_docker.sh` and archive logs/metrics artifacts.

### E) Real SSH System Tests

Use real hosts in your environment:

1. `drynwyn` and `framework0`
2. test mount under `~/Downloads/storage-test-streaming`
3. generate large files remotely (`dd if=/dev/urandom ...`)

Scenarios:

1. First read of uncached large file returns quickly and continues streaming.
2. Compare elapsed time to first byte vs full-download legacy mode.
3. Mid-transfer disconnect/reconnect behavior.
4. Multi-client read contention.
5. Partial final chunk/EOF correctness with non-aligned file sizes.

### F) Regression Matrix

1. Existing sync/conflict tests unchanged.
2. Existing cache accounting tests unchanged.
3. Existing mount/unmount and lock tests unchanged.
4. Repeat full Bazel test suite and CLI build in Nix shell.

## Implementation Order (Strict) with Go/No-Go Gates

1. Add scaffolding + feature flag + metrics.
Gate 0 (baseline): collect legacy first-byte and total-read metrics on representative files.
2. Add session/range internals with isolated unit tests.
Gate 1: all new unit tests green, no race warnings in repeated stress loops.
3. Integrate open/read path for streaming mode (feature-flagged).
Gate 2: integration tests prove first-byte improvement and byte correctness on cold reads.
4. Add background prefetch + finalization + eviction pinning.
Gate 3: no corruption/orphan files after close/unmount/restart scenarios.
5. Handle cleanup/recovery/failure edges, including backend dropouts and cancellation.
Gate 4: deterministic error semantics (`EIO` only when requested range unavailable).
6. Run full automated tests + benchmark suite in CI/local.
Gate 5: no regressions versus current test suite and acceptable overhead on warm reads.
7. Run docker system tests (latency/loss shaping + containerized sshd backends).
Gate 6: all docker scenarios pass with reproducible logs/artifacts.
8. Run real SSH system tests on `drynwyn` and `framework0`.
Gate 7: first-byte benefit confirmed on real network; no regressions in correctness.
9. Promote rollout in stages (flag default off -> optional on -> default on only after soak).

## Acceptance Criteria

1. First uncached read returns data without waiting for full-file download.
2. Final cached file matches remote content checksum.
3. No regression in existing test suite.
4. Docker and real-SSH system tests pass.
5. Observability shows first-byte improvement on remote large files.

## Rollback Plan

1. Keep legacy path available via feature flag.
2. On incident, disable streaming reads at runtime/CLI and revert to whole-file cache path.
3. Preserve logs/metrics for root-cause analysis.

