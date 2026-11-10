# Foreground responsiveness under bounded demand

State: draft on `66c640a`; Ted's 2026-11-10 ruling includes reads and browsing.

## Intent and limits

Serve DESIGN's large-file streaming and offline-cache goals while background
sync and cold full-file copies are busy. A request waiting to browse metadata
or read bytes must have an admission path and, for SSH, a channel path.
Grow work with demand up to explicit caps. CPU load is diagnostic only: a
blocked member operation may use almost no CPU. Keep landed writes dirty or
synced, preserve returned bytes and remote member identity, and stop cleanly.

This is an opt-in mount feature so existing single-threaded mounts retain
their observed behavior. `max_foreground_operations` enables the multithreaded
FUSE loop and caps **active Python callbacks**, with protected capacity for
metadata, demand I/O, and remaining operations. It does not cap libfuse 2
native threads: blocked callbacks can still cause libfuse to create more.
The default foreground cap when opted in is 32, with one protected active
slot for each class and the rest shared; a configured cap from 3 to 128 is
allowed. Directory handles and listing use the metadata slot; warm cached
opens and reads use the demand slot; cold full-file opens use ordinary capacity.
A cached open whose entry disappears during freshness checking or eviction
releases its demand slot before waiting for ordinary admission to fetch.
At cap 3, each class has exactly one slot and progress still
depends on the active call finishing or its member timeout; admission does
not interrupt it. Explicit gate close cancels queued callbacks. During a
normal unmount libfuse joins handlers before calling `destroy`, so queued
callbacks may drain before `destroy` closes the gate; unmount latency depends
on active member operations and their timeouts. Sync may grow from its configured minimum up to a separately
configured maximum, capped at 8 with SSH members or 32 local-only. Neither
pool may silently expand without its configured limit.

No live profile changes, production mount, real cache inspection, deploy, or
claim of a hard process-thread bound is in scope. No new CPU-load policy or
streaming range pipeline is needed for this mission.

## Firewalled entities and boundaries

```mermaid
flowchart LR
    F[FUSE callback] -->|foreground-admission v1| A[Admission gate]
    F -->|member-io-class v1| M[Member transport]
    S[Sync queue] -->|sync-pool v1| W[Sync workers]
    W -->|member-io-class v1| M
```

The FUSE callback keeps the exact operation and return value; the gate only
decides when it may run. Full cache copies are bulk for the duration of the
copy, even when started by a foreground open. Metadata listing and demand
range reads retain interactive classification. The SSH pool's existing
interactive channel reserve applies. The sync queue remains durable across
stop/restart; no task is dropped because worker growth or shutdown raced it.

## Milestones and acceptance

1. Tracer: an executable cold-open/listing classification test fails on the
   old head, then passes without changing bytes. It must expose deferred
   `readdir` generator execution as well as full-copy classification.
2. Foreground gate: a synthetic red test first shows cold opens occupying
   callback capacity that browsing and demand reads need. Then saturation
   proves metadata, demand read, and ordinary work each progress when a
   class slot becomes free; cap, FIFO within class, cancellation, close,
   and no lost callback or leaked slot. Assert exact bytes and unchanged
   exceptions. Use FUSE callbacks directly, without a manual mount. Native
   libfuse thread growth remains outside this test's claim.
3. Sync demand growth: reuse #167's reviewed logic where compatible with
   current master. Prove cap, idle retirement, stop/restart, queue
   preservation and partial startup cleanup. Keep display labels separate
   from stable `remote0` identity and retain `UpdateHostKeys=no`.
4. On a clean pushed head run CLI build and `scripts/run-gates.sh` (all four
   gates), formal council to fixpoint and independent lead review. Root owns
   acceptance and eventual scripted normal merge; no lane here merges.

## Stop conditions

Escalate a product tradeoff, live-data requirement, keys/money, unresolved
third approach failure, or a change to a published boundary. If fleet gate
capacity is unavailable, continue source and synthetic work and identify
the exact blocked gate. The separate #167 draft remains intact; record its
succession explicitly before this PR can be accepted.

