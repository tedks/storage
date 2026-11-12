# Proposed durable freshness and restart boundary for PR #171

Status: SOURCE-ONLY revision of the audit at clean pushed `31c55cfeaf8038e7e9bfa8760d0d31b48925d673`, based on root/warm Sol FINDINGS at frozen `355e41b`; Luna STOP_AND_ESCALATE for implementation on existing roots. Nothing here is implemented or tested.
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
debt replay across a surviving CLEAN inode and across cache inode loss through
the root manifest. It would wire a bootstrap barrier before open/sync/watcher/
heal/sweep, changed-A publication before invalidation, B-stage marker before
no-replace install, and generation-specific replay. Candidate source paths:
`storage/fs.py`, `storage/cache.py`, `storage/cache_metadata.py`, and existing
`tests/test_fs_threading.py`/`tests/test_fs_revalidate.py`; startup callers in
`storage/fs.py` must not start effects until replay. This is one bounded
boundary, not the prior nine-module persistence plan. It cannot ship alone:
namespace intent/COMPLETED records, local-write prepare/checkpoint, existing
root migration, physical durability qualification and member-name
reconciliation remain unimplemented. Stage 2 would separately prove
pre-move intent/terminal/replay, with source likely in `storage/fs.py` and
`storage/namespace.py`; Stage 3 would separately prove the mutation protocol.
Neither later stage is authorized here. Root boundary and necessity review
precede any implementation/runtime grant.

Deterministic scratch schedules (proposed, **unwritten/unrun**):

1. On an explicitly initialized scratch root, activate the two deferred
   `test_restart_of_b_fallback_restores_a_debt_before_freshness_shortcut` and
   `test_restart_with_missing_cache_rejects_ambiguous_a_b_copies` schedules in
   `tests/test_fs_threading.py`. Changed A, stale B fallback and a cancelled
   missing-cache open must replay A's exact owed generation before demand
   selection; B bytes never settle A debt or install a false baseline. A
   matching later A fetch may settle only A. Fail marker publication before
   install and assert no certified B CLEAN bytes.
2. In `tests/test_fs_namespace_integration.py`, complete a real new rename
   with callback and the explicit intended member set all confirmed. Restart
   after durable COMPLETED: normal path access returns exact source bytes and
   no new guard. Inject a crash before terminal fsync after returns: replay
   provisional denial. Seed an independent older intersecting guard/debt and
   prove it survives the new COMPLETED record. This is not a retry proof.
3. There, fail/raise a member after a directory callback, then perform an
   intersecting ancestor move and restart. Exact and translated old/new
   prefixes still deny an absent `_freshness` lazy descendant, stale B fetch,
   upload and destructive eviction; only a matching DIRTY child inode may be
   read locally. Queue completion does not retire any record.
4. In scratch startup/rebuild fixtures (`tests/test_fs_threading.py` and
   `tests/test_index_audit.py`), corrupt/remove the root anchor/manifest or
   supply an unknown version, including an otherwise empty existing root.
   Assert zero background/member effects and preserved local bytes, rather
   than inferred first use. Separately create a root with explicit provenance
   and assert its initial empty authority is accepted. Physical power loss
   remains outside this evidence.
5. In `tests/test_fs_threading.py`, Event-gate W1 prepare, rename/repoint,
   W2 write and opposite DIRTY completions; checkpoint or inject terminal
   failure. Assert actual current-inode bytes, generation/debt, no stale CLEAN
   or false upload, and that a failed later mark survives an older finalizer.
   Create without a prior inode must have path intent before O_TRUNC/bytes.
   This schedule belongs to later Stage 3, not Stage 1.

These process-restart schedules need same-schedule pre-fix behavioral RED
(stale bytes, false upload/CLEAN, lost debt/guard, or unsafe startup), then
GREEN after separately authorized implementation. The existing two restart
tests are currently excluded from the narrow fence suite. Even if Stage 1
passes, PR #171 remains unshippable until namespace and local-mutation
durability, existing-root policy, member-name reconciliation, full gates/CLI,
formal council and root acceptance.
Open Ted questions on collision, caller-visible partial result and external
writer ownership remain unchanged; this proposal grants no retirement rule.
