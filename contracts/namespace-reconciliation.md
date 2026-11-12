# Proposed member-name reconciliation boundary for PR #171

Status: source-only design at `a2f0268`; root review required. This does not
make the in-memory guard shippable. Stable member IDs, never display names,
key receipts and debts.

## Assumptions and current path

A member rename may fail after taking effect. `LocalBackend.rename` returns
normally if old is absent and uses `os.replace`; SSH can also replace a
target. An external writer may race a stat. Thus return, queue completion,
path existence, size and mtime are not lineage proof. The small tracer below
assumes a synthetic member can give stable object identity and an atomic
conditional rename under its member-side lock. Production adapters lack that
capability; ambiguous cases remain quarantined.

1. `StorageFS.rename/_rename` (`storage/fs.py:2561-2720`) holds directory
   namespace exclusion or sorted file path locks plus upload serialization.
   Its local callback rekeys identity; incomplete progress publishes old/new
   negative prefix coverage, preserving distinct origin debt snapshots.
2. `NamespaceService.rename` (`storage/namespace.py:1479-1590`) moves cache,
   index and xattrs before member I/O. `RenameProgress` reports local phases
   and intended/attempted/confirmed/failed/unknown stable member IDs.
   `_propagate_dir_operation` (`:525-563`) records backend return/exception,
   but no remote object identity or conditional postcondition.
3. Failed members get `DirSyncTask` (`namespace.py:1575-1587`,
   `storage/sync.py:452-477`). Its fields (`storage/sync_tasks.py:51-64`)
   have paths and member hint, no move ID or generation. The executor
   (`storage/sync_executor.py:472-551`) filters on destination presence in
   cache/index, then calls unconditional backend rename. A guarded move must
   **fence this legacy retry before it can mutate a member**; merely adding
   a proof callback after it would leave the destructive replay race. Completion only
   removes queue dedupe (`storage/sync.py:478-502`); it never settles a
   specific `StorageFS` guard. Dropped/exhausted retries prove nothing.
4. `NamespaceUncertainty` (`fs.py:118-145`) covers unknown and lazy children.
   `fs.py:554-585,1668-1750` denies upload/CLEAN and permits only exact
   current-inode/generation DIRTY local proof. Intersecting later moves add
   coverage (`fs.py:2623-2713`), not reconciliation.

The indispensable extension is a move ID/guard revision on the task and a
per-member lineage receipt `(move ID, member ID, expected source object ID,
old/new names, observed identities, conditional outcome)`. A directory root
object identity covers its subtree without enumerating leaves. Proof must
come from an atomic member-side conditional operation or durable operation
ID service; preflight and postflight stats alone cannot close the external
writer race. Legacy untagged retries stay best effort and cannot retire a
guard.

## Transition and limits

`PENDING -> MEMBER_PROVED -> READY -> RETIRED`: proofs are per intended
member. READY requires every intended member's source object proved at new
and absent at old, or an explicitly approved resolution; target origin and
all overlapping guards still need independent disposition. Under rename's
namespace/path serialization, compare this record's revision, intersecting
record revisions, current local identity/epoch, and relevant member debt
generations. Retire only the matching record, keeping every other record and
origin-specific debt. A source proof cannot settle target debt even if both
origins owe the same member. An owed generation settles only by its matching
checked fetch or generation-checked sync. Lifting the name guard permits
normal read/upload admission; normal revalidation, byte-stage and sync guards
still decide bytes, baseline and CLEAN publication.

| Case | Proposed result |
| --- | --- |
| B misses, then recovers | A's initial **lineage-checked** conditional confirmation plus B's tagged conditional old-object -> free-new-name proof can make this record READY. A plain initial `RenameProgress.confirmed` is insufficient. B unavailable leaves it PENDING. |
| Both old/new exist, or neither | No lineage inference. Preserve both origins/debts and guard; never blind-replace an unrelated target. |
| Target has distinct A debt | Keep its A debt separate from source A, even after source proof; require target disposition. |
| Nested/ancestor moves | Retain all records and conservative translated prefixes; an old retry cannot clear a newer overlap. Lazy children stay covered. |
| Write/unlink/create races late retry | Mutation/namespace epoch changes before publication; old receipt fails revision/identity check. Exact current DIRTY inode stays local-readable, never overwritten or uploaded by stale retry. |

The first tracer uses two scratch synthetic members. Both source-root IDs are
recorded before the local move. A completes a lineage-checked conditional
directory rename; B gives a forced **pre-operation** failure and later
atomically renames only if its recorded ID remains at old and
new is absent. Tag the returned postcondition with the move ID and member ID.
Reuse `DirSyncTask` for scheduling, fence its unconditional retry for this
move, and add only a result callback to the matching `StorageFS` record; no
general transaction queue. Check revision
before member I/O and again before guard retirement. Keep existing directory
namespace exclusion or file path locks across member call and publication;
member I/O is allowed there, never under the freshness leaf. Capacity waits
occur before namespace/path locks. Upload drain protects names, and active
writers are checked by inode/epoch without leaf-lock waiting. This tracer
can restore useful reads/uploads for the proved case, but cannot certify
production SSH, external races, collided names or unresolved chains.

## Durable prerequisite and acceptance schedules

Before the first local destructive move, persist and sync a versioned intent:
move ID, old/new segment-bounded roots, source and target identities, separate
origin debt generations, intended member IDs, observed member source IDs and
unknown outcome. An unavailable member whose source ID cannot be proved
remains blocked. Persist
local phase and each tagged proof before treating it as settled; durably
retire the record before admitting CLEAN/upload. On restart reconstruct all
unretired/translated negative prefixes before opens, sweep, heal or sync;
revalidate local inode/generation and member proofs. Cache-file xattrs cannot
cover absent or lazy descendants; SQLite alone is rebuildable acceleration.
A missing/corrupt intent with possible partial move fails closed. Current
code has no durable store, so this prerequisite blocks shipping.

Future deterministic tests, all **UNRUN**, must assert behavior, not only flags:

1. A confirms, B fails before operation; CLEAN child EIO and zero upload
   before repair. After B tagged conditional retry, assert exact fresh bytes,
   DIRTY source upload, old B name absent and independent A debt retained
   until its generation settles.
2. Gate B retry; put unrelated bytes at B's target and local DIRTY bytes at
   new. Assert conditional refusal, both exact byte sets, zero unsafe upload,
   no CLEAN and both origin debts. A forced guard deletion control exposes
   wrong read/upload on the same schedule.
3. Source and target both owe member A. Finish source proof; assert target A
   debt and target bytes remain, with no source receipt claiming target CLEAN.
4. Fail child move, move ancestor, expose lazy descendant, then finish old
   retry late. Assert EIO or proved DIRTY bytes, no stale full/stream commit
   or upload; a current tagged chain may restore read.
5. Gate retry then write/unlink/create at new. Stale result cannot overwrite
   new inode, retire newer guard, clear debt or publish CLEAN. Crash/restart
   variant is reserved for the durable phase.

`sync-member-reconcile` already tracks missed operation chains and
`oq-unlink-member-copies` tracks deletion propagation. Two distinct product
choices remain: when a failed rename's member target now has unrelated data,
recommend **refuse replacement, keep both and expose conflict**; Ted decides
whether automatic overwrite or conflict-suffix placement is permitted. This
blocks collision recovery, not the empty-target tracer. The second choice is
what a caller should observe when a member misses a rename and recovery may
be indefinitely impossible: recommend an explicit partial-failure result
instead of success followed by hidden permanent EIO; Ted decides the API
contract. Unavailable members remain quarantined under existing byte-safety
rules meanwhile. If SSH cannot provide conditional lineage, that is a
technical release blocker, never a license to retire by path/stat alone.
