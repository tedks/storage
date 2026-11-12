# Proposed durable freshness and restart boundary for PR #171

Status: SOURCE-ONLY audit at clean pushed `c6fc1382bbd58042cd01c3f2cc9057ad6041d249`.
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
an empty debt map: cache-root UUID, logical file UUID, publication revision,
local epoch, stable member-ID generations/debts and observed stat, plus the
actual inode/size/mtime identity. It is distinct from the best-effort baseline
xattr. A current local DIRTY proof also needs the root/file UUID, inode,
settled mutation epoch and authoritative dirty generation; `user.storage.metadata`
alone lacks the generation. A pre-byte `uncertain-local` marker must be durable
before pwrite/O_TRUNC/create changes bytes; on successful publication fsync
bytes, DIRTY xattr and generation before clearing uncertainty. Failure leaves
the inode uncertain, never CLEAN or uploadable. An old finalizer cannot clear
a newer epoch. This augments, rather than replaces, live stat healing.

A **single versioned cache-root freshness/namespace manifest**, excluded from
cache scans and the mounted view, is the minimum additional authority for
path debts when no inode survives and for exact/file or segment-bounded
old/new subtree guards. It stores a cache-root UUID, monotonic revision,
path/file identities, origin-separated member debts, and per-move intent
(`old,new,kind,source/target identity,unknown member outcome`), plus optional
completed-local-move inode/generation witnesses. An atomic replacement of this
one purpose-specific file (private temp, fsync file, replace, fsync cache-root
directory) is sufficient for a first tracer; no generic transaction engine or
authoritative SQLite table. A failed write/fsync leaves the operation blocked
before cache/member mutation. Capacity or marker I/O failure fails closed and
preserves existing bytes. The manifest and inode markers combine by
**conservative union**: a matching CLEAN marker restores local identity/debts,
but cannot erase an owed path debt or covering negative guard. A marker from
another root, version, inode or revision is not proof. Clearing a debt requires
matching bytes/baseline or generation-checked sync *and* a durable newer
manifest/marker state; an interrupted clear leaves extra debt, which is safe.
This is a narrow amendment to v3's sentence naming the CLEAN inode xattr as
*the* durable authority: it remains authoritative for that surviving inode,
but a root-local manifest is also required for obligations with no inode and
for negative subtree coverage. It does not change SQLite's rebuildable role.
Serialize manifest replacements with a small root-local writer mutex acquired
after the existing namespace/path or rename-serializer boundary, never the
reverse; release the freshness leaf before local fsync. No foreground admission
wait or member I/O occurs under that mutex. The single-file replacement cost
could harm warm-open latency at scale and needs measured design review before
shipping; safety does not justify silently claiming a throughput bound.

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

Before `NamespaceService.rename` can make its **first local destructive move**,
persist a prepared old/new intent covering both roots and their future/lazy
descendants, with source and target debts kept by origin even for the same
member ID. If a later ancestor move intersects a guard in either segment
direction, persist the conservative translated/additional old/new coverage
before that move. A proved pre-move failure may persist an aborted outcome and
restore prior identities; crash before that outcome retains over-quarantine.
The local callback may persist a positive current-inode DIRTY witness after
cache+index move; before it, no stale target row/status/stat can earn local
read proof. Member returns update *observations*, not lineage proof. A queued
retry, all returned successes, or a post-hoc stat do not retire a guard under
the current external-writer contract. This means even fully returned renames
may be conservatively blocked after restart until separate reconciliation;
the tracer is not shippable as a recovered namespace service.
The prepared intent is provisional while the originating rename holds its
current locks: a fully returned in-process rename can retain today's normal
access path, but the durable intent cannot be erased as if those returns
proved post-crash lineage. Startup activates its conservative coverage unless
separately approved reconciliation evidence exists. A failed or unknown
member result activates coverage before releasing rename serialization.

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
| Byte write/O_TRUNC/create before or after DIRTY publication | Durable pre-byte uncertain marker excludes stale fetch/CLEAN; a matching fsynced DIRTY inode/generation can be read locally. A failed mark or ambiguous overlap stays uncertain, with bytes retained and no upload. |
| Rename intent before move; cache-only move; callback; member result | Pre-move aborted proof restores old state only if durably recorded. Otherwise old/new exact or prefix denial survives; callback witness can prove only current local DIRTY bytes. Member outcome remains unknown without lineage evidence. |
| Intersecting ancestor move | Original guard and origin debts survive; translated conservative prefix is durable before later move. No finite row enumeration or successful later return removes lazy-descendant coverage. |
| Missing cache/eviction, corrupt or unknown-version marker/manifest, index rebuild | Path debt/guard survives inode loss. Invalid inode marker denies its CLEAN claim. Missing/corrupt root authority on a preexisting cache cannot be treated as empty: stop startup without mutating cache; preserve local bytes. Rebuild must replay authority first and not certify covered rows. |

First initialization requires positive proof that the cache root and index
were empty **before** constructors create them; a mount-lock file created by
the mounting path is not user content. A valid empty manifest then commits a
new root UUID. A legacy root with files/rows but no manifest and a previously
initialized root whose manifest is lost are indistinguishable from local
evidence alone. Both must stop before effects, pending an explicit migration
policy; guessing that no debt or failed rename existed is unsafe. A corrupt
record, unknown version or torn atomic replacement likewise cannot be skipped
as an empty set. Process-restart and fault-injection tests prove ordering at
injected points, **not** physical power-loss durability; filesystem/fsync and
storage-device behavior need separate qualification.

## Proposed real-path tracer, not authorized implementation

Use existing `StorageFS`/`CacheManager`/`NamespaceService` paths and scratch
LocalBackend members. First close debt replay on a CLEAN inode and a missing
cache while establishing the root manifest/bootstrap; then add prepared
namespace intent and prefix replay without changing member reconciliation.
Likely owned source touchpoints: `storage/fs.py` (bootstrap, publication and
guards), `storage/cache.py` (stage/eviction and dirty marker order),
`storage/cache_metadata.py` (marker parse/internal artifact),
`storage/namespace.py` (pre-move intent and progress),
`storage/streaming.py` (stage marker before link), `storage/sync.py` and
`storage/sync_executor.py` (worker startup/guard), `storage/index_audit.py`
and `storage/cli_debug.py` (rebuild refuses or replays authority). No backend
API or generic transaction module is proposed. Exact implementation scope
requires root review before edits.

Deterministic scratch schedules (proposed, **unwritten/unrun**):

1. Activate `test_restart_of_b_fallback_restores_a_debt_before_freshness_shortcut`
   in `tests/test_fs_threading.py`: change A, make it unavailable after the
   checked stat, fetch B, restart `StorageFS`, assert B bytes plus durable A
   generation before any shortcut, then A return gives A bytes/baseline and
   clears only matching A debt. Gate the marker write to fail and show no
   falsely certified CLEAN B.
2. Activate `test_restart_with_missing_cache_rejects_ambiguous_a_b_copies`
   there: cancel ordinary admission after A observation removes cache,
   restart with differing A/B and no inode, assert EIO/no B baseline, retained
   A recheck/uncertainty and no leaked admission slot. A later proved A fetch
   may settle only its generation.
3. Real incomplete directory rename with failed member after local callback;
   restart, then request cached DIRTY child, CLEAN sibling and an absent-state
   lazy descendant. Assert only matching DIRTY inode/generation reads exact
   bytes; others return EIO, actual queued sync makes zero upload/rename,
   sweep/LRU/recomputable actions preserve covered bytes and origin debts.
4. Gate manifest fsync before cache move, after cache-only move, after callback
   and after a later intersecting ancestor move; reconstruct `StorageFS` each
   time. Assert exact old/new segment coverage (including `/a` versus `/ab`),
   no false CLEAN/baseline, no stale B bytes and unchanged unrelated warm
   browse. A proved pre-move failure retains original access/debts.
5. Inject corrupt/missing/unknown-version root authority and cache inode
   marker, plus rebuild-index on scratch copies. Assert no silent empty-state
   adoption, no upload or deletion, preserved DIRTY bytes, and a first-ever
   empty-root initialization succeeds. Observe exact startup refusal or
   quarantined path, never merely an internal diagnostic flag.

These process-restart schedules need same-schedule pre-fix behavioral RED
(stale bytes, false upload/CLEAN, lost debt/guard, or unsafe startup), then
GREEN after a separately authorized implementation. The existing two restart
tests are currently excluded from the narrow fence suite. Even if the tracer
passes, PR #171 remains unshippable until member-name reconciliation,
legacy/lost-state policy, full gates/CLI, formal council and root acceptance.
Open Ted questions on collision, caller-visible partial result and external
writer ownership remain unchanged; this proposal grants no retirement rule.
