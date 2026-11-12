# Proposed durable freshness and restart boundary for PR #171

Status: Stage 1 implementation boundary amended from root/warm Sol readiness review of frozen `391936df7e1ea7204fb2fab787e58a4377a1214a` (contract `5100e166127be5f2591dc4349dae09d7b9199b6e`), reconciled against clean pushed `c23bacf2c4d4a8468801fa9c4c6f01bdd5542c83`. Luna PROCEED only for the bounded scratch-root prototype after this amendment. The implementation and tests remain separate evidence.
Root/warm Sol scoped CLEAN at code/test `bcf062a` closes the narrow retry
finding, not this boundary. No implementation or runtime evidence exists for
the proposal below. Decision keys remain `foreground-open-pin-ownership` and
`namespace-subtree-uncertainty-coverage`; member-name reconciliation is separate.

## Current path and missing authority

| Boundary | Current source and order | Crash gap |
| --- | --- | --- |
| Changed member A | `StorageFS._revalidate_cached_file` (`fs.py:2208-2250`) stats A, calls `_observe_changed_member` (`:1497`) and then `_invalidate_file_cache`. `_freshness` holds A's generation/debt and path revision (`:1367-1507`). | Debt is memory only. An invalidated/evicted cache inode cannot carry an xattr for the path. |
| Baseline and B fallback | `_store_member_check` (`fs.py:1811-1886`) writes `user.storage.member_stat` keyed to `(ino,size,mtime_ns)`, swallowing `setxattr` errors; `_record_fetched` (`:1888-1917`) can clear only matching in-memory debt. Full fetch stages under `CacheManager.fetch_into_cache` (`cache.py:365-435`), links no-replace, then `_publish_full_fetch` (`fs.py:2377-2435`) writes row/xattr/baseline. Stream finalization has a separate guarded link (`fs.py:1460-1480`, `streaming.py`). | Baseline xattr is not a versioned debt marker. B's CLEAN inode or a lost stage may be visible after restart without A's owed recheck. SQLite location/status cannot substitute for source lineage. |
| Local bytes | `_local_mutation` (`fs.py:1386-1432`) announces in memory before `pwrite`, O_TRUNC or create; `_publish_local_dirty` (`:2814-2832`) calls `CacheManager.mark_file_dirty` (`cache.py:442-463`), which bumps SQLite `dirty_generation` and mirrors `user.storage.metadata`. `CacheMetadataRecord` lacks that generation (`cache_metadata.py:86-153`). | A crash between bytes and DIRTY publication can leave an old CLEAN row/xattr; size/mtime healing misses same-size same-tick writes. Active/failed mutation uncertainty is memory only. |
| Rename | `StorageFS.rename/_rename` (`fs.py:2604-2772`) takes the retry serializer and existing namespace/path/upload locks; `NamespaceService.rename` (`namespace.py:1479-1590`) moves cache, index and callback before member operations. `RenameProgress` and `_namespace_uncertainties` (`fs.py:118-147,1711-1779`) are memory only; an intersecting ancestor move adds conservative coverage. | A crash before callback, after callback, or during member propagation loses negative old/new prefix coverage and origin-distinct debts. A queued `DirSyncTask` is neither durable nor lineage proof. |
| Startup and maintenance | `StateManager` opens/creates SQLite, `CacheManager` creates/scans cache, and `StorageFS.__init__` may start sync at `fs.py:360` before namespace/streaming initialization and `_cleanup_stale_partials` (`:483`). Mount construction uses `start_sync=False`, but `init` starts streaming and sync before the unclean sweep and watchers (`:486-505`); mount lock triggers that sweep at `:4050-4090`. `heal_stale_clean`, LRU/recomputable eviction and rebuild-index read rows/xattrs (`cache.py:467-543`, `index_audit.py:226-326`). | No startup replay precedes open, worker, watcher, heal or sweep. A rebuild can normalize a row from `user.storage.metadata` without discovering absent-path obligations. |

`user.storage.metadata` on cached files is the authority for rebuildable file
metadata and local DIRTY evidence subject to live stat; SQLite accelerates it
and generation-guards sync while running. `user.storage.member_stat` is a
best-effort baseline, not debt authority. Today a missing member-stat xattr
returns an empty baseline, and missing in-memory debt/guard means no obligation.
Neither absence is safe evidence after a crash. The cache root has no durable
namespace-intent record; an xattr on a moved directory cannot cover the old
name or a future/lazy child by itself.

## Smallest defensible persistent authority

| Candidate using current local storage | What it can certify | Why it is insufficient alone |
| --- | --- | --- |
| Existing `user.storage.member_stat` and `user.storage.metadata` cache xattrs | A baseline and local metadata for a surviving inode, subject to identity/stat checks. | Neither carries a versioned per-member debt/dirty generation; eviction and absent/lazy descendants remove the carrier. Baseline writes currently fail open. |
| SQLite rows or a new SQLite table | Efficient path lookup and bounded transactions. | The project explicitly rebuilds the index from cache xattrs; making only SQLite retain a negative subtree or evicted-path debt would silently change that authority contract. |
| Parent-directory xattrs | A small local subtree hint that moves with its directory. | A move loses the old-name carrier, nested/overlapping records grow without a dependable xattr size bound, and missing/lazy parent names cannot hold one. |
| Versioned inode marker plus one root-local manifest | Inode byte/debt identity and independent path/subtree denial, including absent descendants. | Requires ordered dual publication and conservative startup on missing/corrupt authority; the manifest replacement cost needs later performance evidence. |

Keep v3's **versioned marker on every committed CLEAN cache inode**, including
an empty debt map: root/file identity, revision, local epoch, stable member-ID
generations/debts, observed stat and actual inode identity. It is distinct
from the best-effort baseline xattr. Current DIRTY local proof additionally
needs the same inode, settled mutation group and authoritative dirty
generation; `user.storage.metadata` alone lacks that generation. This augments
live stat healing. A durable *pre-byte uncertainty intent* and a later
*completion checkpoint* are different transitions; ordinary `pwrite` is not
promised synchronous data durability.

A **single versioned cache-root freshness/namespace manifest**, excluded from
cache scans and the mounted view, is the minimum additional authority for
path debts when no inode survives and for exact/file or segment-bounded
old/new subtree guards. It stores a cache-root UUID, monotonic revision,
path/file identities, origin-separated member debts, outstanding local
mutation intents, and per-move records
(`move ID,old,new,kind,source/target identity,progress,outcome`), plus optional
completed-local-move inode/generation witnesses. A serialized atomic
replacement (private temp, fsync file, replace, fsync cache-root directory)
is a candidate, not measured implementation. One purpose-specific file avoids
an authoritative SQLite table or generic transaction engine. A prepare failure
before effects prevents those effects. A terminal persistence failure *after*
local/member effects retains the prepared record, reports the existing
partial/error outcome and denies uncertified access; it cannot claim no bytes
changed. The manifest and inode markers combine by
**conservative union**: a matching CLEAN marker restores local identity/debts,
but cannot erase an owed path debt or covering negative guard. A marker from
another root, version, inode or revision is not proof. Clearing a debt requires
matching bytes/baseline or generation-checked sync *and* a durable newer
manifest/marker state; an interrupted clear leaves extra debt, which is safe.
This is a narrow amendment to v3's sentence naming the CLEAN inode xattr as
*the* durable authority: it remains authoritative for that surviving inode,
but a root-local manifest is also required for obligations with no inode and
for negative subtree coverage. It does not change SQLite's rebuildable role.
Serialize every manifest read/modify/write with one root-local publication
mutex and revision check so one writer cannot replace another writer's debt,
guard or intent. Acquire it **outside and before** metadata, descriptor and
freshness leaf locks; never acquire it while holding one of those locks.
Rename may hold its existing outer serializer and namespace/path locks before
this mutex, but must release the manifest mutex before member I/O and before
entry into cache/index callbacks that hold leaf locks. Mutation takes the
manifest mutex before its short descriptor/freshness snapshots. No writer
holds a freshness leaf across file/manifest fsync or metadata wait. The
single-file replacement cost and first-write latency need future evidence.

Order under existing path/namespace serialization: persist observed A debt
in the root manifest before invalidating/removing its cache inode or returning
from the observation; persist it on any surviving CLEAN inode before serving
it. If a process dies after a stat but before that durable publication, no
cache mutation may have occurred; unclean startup must conservatively force
that member's recheck from prior holder/baseline evidence, or treat the path
uncertain if the holder cannot be identified. For B fallback, durably retain A
in the manifest, set/fsync A's marker on B's private staged inode, no-replace
link and fsync the parent, then publish CLEAN row and B baseline. If these
steps fail, reject the stage before it is certified; never let an index row
or B baseline clear A. A normal first-open member revalidation still runs:
restoring a local marker never forges `verified_at`. The local marker check
remains demand class; an uncertain cached open drops namespace/path locks and
reclassifies to ordinary only for member probe/bulk fetch, as v3 requires.

### Stage 1 restricted scratch-root execution boundary

This is an internal opt-in mode of **real** `StorageFS`, `StateManager` and
`CacheManager`, not a fake persistence service, mount/CLI setting, or default
behavior change. A scratch-only initializer must claim a previously absent
path: create a deterministic sibling provenance record with exclusive create
and a fresh root UUID, sync it and its parent, then create the root exclusively
and commit its empty versioned manifest with the same UUID and root
`(dev,ino)` (no symlink). Creating the
provenance record first ensures a crash during initialization cannot leave an
apparently legacy root at that path. It may leave an unusable scratch claim;
the initializer must not silently adopt or overwrite it. Reopen requires the
explicit internal Stage 1 mode and expected UUID, and accepts only matching
provenance, manifest version/revision and live root identity. Missing/corrupt data
fails before effects. Total loss of both independent records remains locally
indistinguishable and requires separate authority; the scratch tracer does
not claim to solve that or select Ted's pending legacy migration policy.

At the *first instruction* of `StorageFS` construction, before today's
`StateManager(str(state_db))` opens/creates SQLite and before `CacheManager`
mkdir/scans, perform a non-mutating mode/provenance check. An ordinary/default
constructor aimed at a Stage 1 path with either sibling provenance or a root
manifest must fail before constructor effects, so it cannot silently bypass
the unfinished contract. A Stage 1 reopen parses and validates authority
before those constructors, then restores debts before any partial cleanup,
worker, watcher, open, sweep or maintenance effect. Initializing an existing
root is unavailable. Default handling of existing roots lacking Stage 1
authority remains **unchanged** in this phase; no deployed refusal, CLI option
or migration is proposed. Direct constructor, `init` and `destroy` paths
must obey the same gate and teardown only owned scratch resources. The
internal reopen rejects constructor options that would start sync, streaming,
watchers, checksum/recompute work or a sweep; it does not silently turn a
requested effect off and claim the caller's requested mode ran.

Stage 1's only positive operations are read-only canonical file open/read/
release, local marker validation, required member stat/revalidation, guarded
full-copy fetch, and cache accounting/eviction **after** debt is durable and
held through the destructive action. `open` accepts only exact `O_RDONLY`
without `O_CREAT`/`O_TRUNC` or other write intent; no writable descriptor can
exist. Metadata lookup needed by those reads may run only if it cannot
publish an uncertified CLEAN row/baseline. Public `getattr`/`readdir` and
explicit member-suffix paths are unavailable until their publication paths
are audited; Stage 1 tests use canonical file opens. All other callbacks
that can mutate local bytes/names/policy, including `write`, `truncate`,
`create`, `unlink`, `rename`, `mkdir`, `rmdir`, `chmod`, `chown`, `setxattr`,
`removexattr`, `symlink`, `utimens` and writable `open`, reject at their
entry **before any cache, index or backend effect**. Sync/upload/retry workers,
watchers, streaming/prefix promotion, checksum/recompute workers, sweep,
partial cleanup, rebuild/debug and direct cache mutators that bypass the
guard are unavailable. The constructor cannot start them; `init` cannot
enable them later. A release/fsync of a read-only descriptor cannot enqueue
sync. `heal_stale_clean` cannot manufacture DIRTY from an unverified marker:
an inode/row/stat mismatch stays uncertain until the Stage 1 marker rule
proves it. An LRU eviction may remove only a path whose surviving debt is
already in the manifest and whose path guard is held through unlink. Any
private/direct callback route around this narrow allowlist must
also reject, including a direct `CacheManager` install/eviction without the
Stage 1 publication guard. A state produced by an unimplemented mutator is
never accepted as certified on reopen. If the existing constructor/callback
surfaces cannot enforce this without a broad mode framework, the first
implementation package must shrink to pre-constructor authority validation
and refusal tests, then return for boundary review; do not implement a
half-gated read mode.

### Stage 1 full-fetch and debt-clear order

Today `CacheManager.fetch_into_cache` copies to a private partial, then under
its cache lock enters `install_guard` (index -> descriptor -> freshness),
links no-replace, and calls `on_installed` **after** the link inside those
locks. `StorageFS._publish_full_fetch` then writes the CLEAN row/baseline and
calls `_record_fetched`. That callback cannot supply a pre-link durable
marker and must not fsync inside the leaf locks. A minimal Stage 1 split is:

1. After the member copy and prefix-marker removal, a new `prepare_stage`
   hook writes a versioned **pending** marker with the latest manifest debt
   set (including A for B fallback) on the private stage and fsyncs that
   file **before** the cache install lock/guard. The caller holds the canonical
   path lock, but no cache/index/descriptor/freshness leaf. Manifest RMW uses
   the root publication mutex and matching revision; its A debt was already
   durable before invalidation. The marker itself grants no CLEAN authority.
2. Existing install guard checks token, path revision, active mutation/DIRTY
   exclusion and no-replace ownership, then links the staged inode. Exit the
   guard and cache lock without calling the old `on_installed`; retain the
   caller's path lock. Fsync the parent directory outside all those leaf
   locks. A pending marker plus absent CLEAN row denies an unrelated direct
   observer; ordinary same-path opens wait on the path lock. Direct cache
   access in Stage 1 is disallowed as above. If the link loses, leave the
   winner intact, discard only this stage, and neither publish its source
   baseline nor clear its debt.
3. Reenter the existing guarded publication boundary; verify the **exact**
   staged inode owns the final path, the pending marker/root UUID, and the
   same source generation/file revision token. On stale token or mismatch,
   do not certify it. Conditionally remove only this linked inode if safe;
   otherwise keep it pending/uncertain for replay, never overwrite a local
   writer or another winner. Leave the guard and cache/index/descriptor/
   freshness leaves while retaining the path lock. Write and fsync one common
   **PENDING -> COMMITTED** marker on that exact inode before *any* CLEAN row
   or baseline, including first no-debt fetch and B fallback. The B marker
   retains every owed generation. For checked A, the marker removes only its
   matching A generation and advances file revision. Reenter the guard at
   final publication and revalidate exact inode, token, marker and debt.
   A failed or stale transition remains pending or conservatively owed.
   A late B stage cannot clear newer A/C obligations.
4. For a checked A fetch, after the common committed marker transition in
   step 3 and still under the held path lock, under the root
   publication mutex, read the latest manifest revision and conditionally
   remove only that matching `(file ID,member A,generation)` debt; preserve
   B/C and any newer A, other roots and guards. Revalidate token/current
   inode again at final guarded row/baseline publication. If the manifest
   write fails, retain extra A debt (conservative); it does not resurrect A
   forever after a **successful** matching clear, because replay unions the
   advanced inode revision with the manifest's conditional generation state.
   Replay considers only the current exact inode's highest valid marker for
   that file ID; an older staged/orphan inode marker cannot re-add A. When
   both current marker and manifest record the matching clear revision, A is
   settled; if either still owes A, a later checked A read may repeat the
   conditional clear rather than treating the debt as permanent.
   A newer A generation wins over an older marker or clear. Manifest updates
   are serialized RMW, never unconditional map replacement.

Crash cuts: before marker fsync, private copy is disposable; after marker but
before link, no canonical bytes changed; after link but before parent fsync,
the name may or may not survive and remains pending; after parent fsync but
before CLEAN publication, the exact inode is present but untrusted; a
committed marker before row/baseline publication proves only that inode's
durable stage and debts, subject to replay validation; after
CLEAN publication with B, A remains owed in both authorities; after A marker
clear but before manifest clear, A remains owed conservatively; after both
matching clears, A bytes and revision are durable and only A's observed
generation is settled. A stale token or lost no-replace race never publishes
the loser's row/baseline. Stage 1 uses the normal read admission rule and
keeps remote copy outside freshness locks; it does not claim physical
power-loss qualification from process-restart tests.

Before `NamespaceService.rename` makes its **first local destructive move**,
persist a provisional old/new intent, its operation ID and source/target
origins. It covers exact file names or both directory roots and lazy
descendants. For an intersecting ancestor/descendant move, persist translated
coverage of *older* records before its first move; retain those original
records and their origin debts. A proved pre-move failure may persist ABORTED;
crash before that terminal stays provisional. The callback's current-inode
DIRTY witness is only local read proof, never remote-name proof.

Under the existing rename serializer and namespace/path/upload serialization,
after the local callback and **every intended member return is confirmed with
failed=unknown=empty**, durably mark this *new operation's* provisional record
COMPLETED before success returns. This is the live operation's already
accepted normal disposition, not a timeless or linearizable member identity:
later external changes still use watcher/revalidation. The terminal removes
only that operation's provisional denial. It cannot erase older intersecting
records, translated negative domains, independent origin debts or concurrent
local mutation intents. A crash before durable COMPLETED, including after all
backend returns, replays provisional denial. A returned failure or exception
retains its ambiguity. A queued retry, its later success or a post-hoc stat
cannot turn a *prior* unknown/partial move into COMPLETED; that remains the
separate member-name reconciliation problem. Thus a fully successful *new*
rename need not become permanently quarantined solely because of restart,
while this tracer remains unshippable for incomplete renames.

### Mutation prepare, live publication and durable checkpoint

The existing `write` takes `_fd_lock` only for ownership/announcement, then
releases it for `pwrite`; rename can repoint that descriptor meanwhile.
`truncate` and `create` have path serialization, but create may have no inode
before `create_cached_file`/`os.open(O_TRUNC)`. A proposed narrow protocol is:

1. Under the root publication mutex, briefly snapshot the descriptor's
   logical file ID, current canonical path, `(dev,ino)`, revision and next
   mutation epoch under descriptor/freshness leaves, then drop those leaves.
   Persist an unresolved root manifest intent for that file ID/inode/epoch
   **before** bytes change. For absent-path create, persist a path-level
   create/replacement intent and prospective file ID before inode creation;
   it cannot rely on an inode xattr that does not exist yet. A failed prepare
   prevents the byte operation. All manifest RMWs merge/revalidate the
   latest revision, never overwrite an independently added debt/guard.
2. Still under the publication mutex, reacquire descriptor/freshness leaves
   and compare path mapping, file ID, inode, epoch and replacement ownership.
   On mismatch, leave the harmless prepared intent and reprepare current
   identity or fail before bytes. On match, announce the live active mutation
   and revision, then release leaves and the mutex before `pwrite`/truncate.
   A rename occurring after announcement carries that file ID's prepared
   intent and active group to the moved inode. Its terminal manifest RMW
   translates any outstanding source intents (or retains old/new aliases on
   partial failure) under the same root mutex; it never drops them merely
   because the rename itself completed. A replaced target FD fails current
   inode ownership instead of inheriting source authority.
3. Publish the live current-inode DIRTY generation after bytes, using today's
   metadata -> descriptor -> freshness order, and retire the active group
   only after every participant's byte change is covered by a successful
   current-inode DIRTY mark. Do **not** acquire the manifest mutex inside
   that critical section. A failed mark, stale FD or terminal persistence
   failure leaves the durable intent and conservative uncertainty. A later
   successful old finalizer cannot clear a newer failed epoch.
4. Only a later explicit checkpoint may retire uncertainty: after no active
   mutation, verify the same inode/file ID, latest epoch, authoritative DIRTY
   generation and all obligations; order file data, required xattr and index
   durability, then atomically publish the matching manifest terminal under
   the root mutex with a final revalidation. The existing `StorageFS.fsync`
   currently calls only `fsync`/`fdatasync` on the file, so implementation
   would need to prove/extend metadata durability before using that callback
   as the checkpoint. A generation-checked sync is another later candidate,
   not proof by itself. Ordinary writes do **not** fsync data per pwrite:
   a durable intent may be reused for later writes to the same still-uncertain
   inode, but never across replacement, checkpoint retirement or a lost
   generation. Without a proven checkpoint, restart stays conservative and
   may deny local reads; this is a latency/availability tradeoff for review,
   not a change to the current POSIX `write`/`fsync` guarantee.

| Interleaving | Required invariant |
| --- | --- |
| Writer prepares old name; rename moves/repoints before pwrite | The prepared file ID/inode follows the move or both aliases stay covered on partial failure. Writer marks only the current moved inode; old target FD cannot earn source proof. |
| Rename completes between writer's snapshot and revalidation | The writer re-prepares the new mapping before bytes. A persisted obsolete prepare is extra denial, never CLEAN authority. |
| W1 and W2 prepare/finish in opposite orders | Serialized manifest RMW retains both epochs; live settlement needs every current-inode byte change marked DIRTY. A checkpoint validates the latest epoch and no active writer, not whichever callback finishes last. |
| Later W2 DIRTY mark or terminal fsync fails after W1 succeeds | W1's completion cannot erase W2's intent. If effects already occurred, retain prepared uncertainty and report failure/partial effect; do not claim untouched bytes. |

Replay must finish **before** any `SyncManager.start`, `StreamingReadManager.init`,
watcher, stale partial cleanup, sweep, FUSE open or debug rebuild action that
could expose, upload, heal or destroy bytes. `StorageFS.__init__` currently
starts sync early when requested; a future implementation must make bootstrap
an explicit prerequisite for both direct construction and `mount_storage`.
Keep the restored prefix domain held through sync admission and destructive
cache/heal/rebuild checks, not an unlocked precheck. A valid current-inode
DIRTY proof permits local read only; it never permits upload or member CLEAN.
The root's existing mount lock excludes a second mount; it does not replace
the freshness authority or protect against unrelated member writers.

## Crash/restart matrix

| Interruption | Safe restart state |
| --- | --- |
| Before/after A debt publication; before cache invalidation | Before durable commit, old cache is intact but an unclean restart must force holder recheck/uncertainty, not certify no debt. After commit, replay A generation before any opener; invalidation can remove bytes but not the path debt. |
| B staged/marked, linked, row or baseline publication interrupted | Manifest retains A. Unlinked stage is scratch; linked but unindexed/mismatched marker is uncertain, not CLEAN. Only matching marked B inode with completed ordered publication can serve B while A stays owed. |
| Byte write/O_TRUNC/create before or after DIRTY publication | Root pre-byte intent excludes stale fetch/CLEAN, including absent-path create. A live matching DIRTY mark can prove current local bytes in process; restart retains uncertainty until an ordered matching data/xattr/index checkpoint. Failed mark or overlapping unresolved epoch stays uncertain, bytes retained and no upload. |
| Rename intent before move; cache-only move; callback; member result | Before durable COMPLETED/ABORTED, old/new exact or prefix denial replays; callback witness proves only current local DIRTY bytes. If callback and **all** intended member returns complete, durable COMPLETED ends only this new operation's provisional record before success; older guards/debts and translated domains survive. Unknown/partial result never inherits that terminal. |
| Intersecting ancestor move | Original guard and origin debts survive; translated conservative prefix is durable before later move. No finite row enumeration or successful later return removes lazy-descendant coverage. |
| Missing cache/eviction, corrupt or unknown-version marker/manifest, index rebuild | Path debt/guard survives inode loss. Invalid inode marker denies its CLEAN claim. Missing/corrupt root authority on a preexisting cache cannot be treated as empty: stop startup without mutating cache; preserve local bytes. Rebuild must replay authority first and not certify covered rows. |

First initialization requires an **explicit creation provenance**: a
dedicated initializer atomically creates a previously nonexistent, exclusively
owned root with a versioned root anchor/UUID and empty manifest **before**
`StateManager` or `CacheManager` constructors create directories/index rows.
A scratch tracer may establish this provenance itself. Merely finding an
existing cache and index empty is not evidence: a previously used root may
have lost its manifest, and total loss of both anchor and manifest is
indistinguishable locally from an old empty root. The initializer must not
silently re-adopt an existing root. Existing/legacy roots with absent
authority need Ted's still-open `oq-durable-freshness-legacy-cache` decision
(explicit migration before new-format mount versus restricted recovery);
this proposal does **not** impose a new live-root refusal or select migration.
Until that ruling/implementation, no existing-root startup is certified. A corrupt
record, unknown version or torn atomic replacement likewise cannot be skipped
as an empty set. Process-restart and fault-injection tests prove ordering at
injected points, **not** physical power-loss durability; filesystem/fsync and
storage-device behavior need separate qualification.

## Proposed real-path tracer, not authorized implementation

Stage 1, **for explicitly initialized scratch roots only**, would prove real
debt replay across a surviving CLEAN inode and across cache inode loss. The
smallest coherent package is a pre-constructor provenance/manifest gate and
replay in `storage/fs.py`; versioned marker parse/write in
`storage/cache_metadata.py`; a pre-link stage hook and split post-link
publication in `storage/cache.py` and `storage/fs.py`; and direct-constructor,
callback and restart schedules in existing `tests/test_fs_threading.py` and
`tests/test_fs_revalidate.py`. It must keep the existing no-replace and
token/inode guard. The package is conditional on a small auditable refusal
boundary; if direct callbacks or cache helpers bypass it, stop after the
pre-constructor gate rather than expanding to a broad mode framework. No
backend adapter, CLI, BUILD file or new persistence service is proposed.
This is one bounded boundary, not the prior nine-module persistence plan.
Namespace intent/COMPLETED records, local-write prepare/checkpoint,
existing-root migration, physical durability qualification and member-name
reconciliation remain unimplemented and unshippable. Root boundary/necessity
review precedes any implementation/runtime grant.

Deterministic scratch schedules (proposed, **unwritten/unrun**):

1. Activate `test_restart_of_b_fallback_restores_a_debt_before_freshness_shortcut`
   with explicit scratch provenance: changed A, stale B fallback, process
   restart and A return. Assert B bytes only while A unavailable, exact A
   generation replay before first-open selection, then A bytes/baseline and
   only matching A debt clear. B/C and newer A remain owed.
2. Activate `test_restart_with_missing_cache_rejects_ambiguous_a_b_copies`:
   cancel ordinary admission after A observation invalidates the inode,
   restart and assert manifest A debt prevents stale B CLEAN adoption without
   a cache xattr. Verify demand/ordinary slot cleanup and later A recovery.
3. Directly call construction and public/private mutation callbacks in
   Stage 1: writable open, O_TRUNC, create, write, rename/unlink/mkdir,
   xattr/policy mutation, streaming, sync/retry and direct cache install/
   eviction must refuse before inode, index, member or queue effects. A
   read-only open/read/release and safe cache accounting still work; `init`
   cannot start disabled workers, and `destroy` closes only owned resources.
4. Corrupt/remove/unknown-version manifest while retaining sibling
   provenance; then point a default constructor at a valid Stage 1 root.
   Each must fail before SQLite create/open, cache mkdir/scan, partial
   cleanup or worker effects. A separately explicitly initialized fresh
   scratch root reopens. Existing empty root without provenance is not
   adopted by Stage 1; default legacy behavior is unchanged.
5. Event-gate `prepare_stage` marker fsync, no-replace link, parent fsync,
   checked A marker clear and conditional manifest clear. Inject failed
   pre-link publication, stale token and lost link races, and a crash between
   A marker and manifest clear. Assert exact canonical inode bytes, no
   loser's baseline/CLEAN, A extra debt after interrupted clear, preservation
   of B/C/new A debts and eventual matching A clear after retry. No marker
   or manifest I/O runs under index/descriptor/freshness leaves.

These scratch schedules need same-schedule pre-fix behavioral RED
(stale bytes, false upload/CLEAN, lost debt/guard, or unsafe startup), then
GREEN after separately authorized implementation. The existing two restart
tests are currently excluded from the narrow fence suite. Even if Stage 1
passes, PR #171 remains unshippable until namespace and local-mutation
durability, existing-root policy, member-name reconciliation, full gates/CLI,
formal council and root acceptance.
Open Ted questions on collision, caller-visible partial result and external
writer ownership remain unchanged; this proposal grants no retirement rule.
