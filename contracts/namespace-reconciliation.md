# Proposed member-name reconciliation boundary for PR #171

Status: narrow-fence design `c35b8e7` was scoped CLEAN by root/warm Sol and
fresh Luna necessity PROCEED. The bounded pre-I/O implementation is at
`54a7812`, with strengthened debt/local-DIRTY assertions at `4c77188`.
The six new schedules pass at pushed `83b26ab`, but its eight-target affected
nonmount run has three older test failures (threading path-lock timing and two
retry-success assumptions); issue `storage171-retry-fence-suite-migration`
tracks root review and repair. Root REJECTED the earlier global namespace-exclusive background-I/O
fence. This contract neither retires a current guard nor makes the in-memory
tracer shippable. Stable member IDs, never display names, key receipts and
debts. Full PR gates, council and independent acceptance remain outstanding.

## Assumptions and current path

A member rename may fail after taking effect. The existing namespace locks
serialize this mount's handlers, not unrelated writers on a local/SSH member;
watchers exist precisely because members can change behind the mount. We
therefore cannot upgrade a return, two stats, path existence, size, mtime or
a queue completion into lineage proof. A directory root identity would prove
only its name movement, **not** descendant byte freshness or debt settlement.

| Boundary | Current guarantee | Stronger guarantee a recovery path would need |
| --- | --- | --- |
| `Backend.rename` (`storage/backends/base.py:256`) | `None` or exception; no operation ID, source identity, conditional outcome or postcondition. | One generation-bound, member-authoritative source/target lineage receipt for initial propagation **and** retry. Do not add an optional Backend API until a concrete adapter can provide it. |
| Local (`storage/backends/local.py:216-230`) | `lstat` can observe `(dev, ino)` at an instant; absent old returns success; `os.replace` can destroy an occupied target. | Atomic source-identity check plus target disposition from actual filesystem authority. `renameat2` no-replace alone does not atomically test source identity; a service mutex around separate stat/rename calls cannot exclude unrelated POSIX writers. |
| SSH (`storage/backends/ssh.py:550-580`) | `posix_rename` replaces; fallback SFTP rename may fail on occupied target; missing old is swallowed. Standard SFTP stat has no portable stable inode/operation ID. | Server-side operation with filesystem-enforced identity/target preconditions and a durable receipt, or another authority that excludes unrelated writers. SFTP pre/post stat cannot supply it. |

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
   a proof callback after it leaves the destructive replay race. Completion
   only removes queue dedupe (`storage/sync.py:478-502`); it never settles a
   specific `StorageFS` guard. Dropped/exhausted retries prove nothing.
4. `NamespaceUncertainty` (`fs.py:118-145`) covers unknown and lazy children.
   `fs.py:554-585,1668-1750` denies upload/CLEAN and permits only exact
   current-inode/generation DIRTY local proof. Intersecting later moves add
   coverage (`fs.py:2623-2713`), not reconciliation.

For **recovery**, a move ID/guard revision and per-member lineage receipt
would be indispensable for both the initial operation and later retries:
`(move ID, member ID, expected source object ID, old/new identities,
conditional outcome)`. A synthetic member with an in-process lock can test
that state machine but is not a production vertical tracer: its lock excludes
only synthetic writers. Current adapters supply neither an atomic source-ID
precondition nor a durable operation receipt. No new protocol or synthetic
experiment is proposed for this phase. Legacy untagged retries are still
capable of remote mutation and must be fenced before backend I/O.

## Proposed narrow retry fence: pre-I/O safety containment

The immediate realizable slice is **pre-I/O containment**, not recovery. At
`SyncExecutor.process_dir_task` (`storage/sync_executor.py:455-487`), before
`_dir_task_current` and before `_process_dir_task` can call a backend, ask
`StorageFS` whether this task intersects **any** unresolved old/new record.
Check every queued, legacy untagged and stale-tagged task; a matching tag is
not permission to bypass. RENAME's old and new are both mutation domains.
Because `DirSyncTask` carries no file/directory bit, conservatively treat
both as possible subtree roots. MKDIR and RMDIR may mutate their named
directory and its descendants, so check those domains too. Compare each
task domain with **both** roots of every guard in **both** segment-bounded
directions: equal, task ancestor of guard root, or guard ancestor of task.
`/a` and `/ab` never intersect. This catches old `/a -> /b` after newer
unresolved `/b -> /c`, and an ancestor retry over `/parent/child`.

The existing `path_guard=self._path_locked` (`storage/fs.py:352,511-514`)
holds namespace **shared** plus exact endpoint locks. By itself it cannot
serialize a directory retry at `/b` with a file rename beneath `/b/child`.
Root rejects the earlier proposal to take namespace exclusive during every
background retry's member I/O: repeated retries would stall unrelated
browsing. The revised candidate uses **one outer retry-versus-rename
serializer** (a per-StorageFS reentrant mutex) acquired at two public entry
points, before either side takes namespace, path, upload or freshness locks:

| Entry | Proposed order, held through |
| --- | --- |
| `StorageFS.rename` (`fs.py:2561-2607`), file **and** directory paths | outer serializer -> existing file `NamespaceLock.shared -> sorted PathLocks -> UploadLocks.hold`, or existing directory `UploadLocks.drain -> NamespaceLock.exclusive`. Hold through `_rename`, local cache/index callback, all member propagation and negative-record publication at `fs.py:2692-2713`. Acquire before its cache/getattr classification and `while True` retry, not inside `_rename`. |
| `SyncExecutor.process_dir_task` (`sync_executor.py:455-487`) for MKDIR/RMDIR/RENAME | outer serializer -> affected `UploadLocks.drain` -> existing `_path_locked` (`NamespaceLock.shared -> sorted endpoint PathLocks`) -> coverage/currency check -> `_process_dir_task` and member operation/result. `SyncManager` passes the FS-owned guarded callback through its existing `path_guard` wiring (`fs.py:352`, `sync.py:70-79,282-285`) to every worker. The current `hinted.is_available()` fast check may remain before the outer mutex because it performs no member mutation; any availability/currency decision used to authorize I/O must be rechecked inside. |

The FS callback would yield an explicit allowed/blocked result while keeping
the ordered locks; `process_dir_task` treats blocked as deferred before its
existing current-path check. Standalone executor fixtures can still supply
their simple guard, but a production FS-bound executor must have this
callback and may not silently fall back to `nullcontext`.

No nested acquisition is needed: `StorageFS._rename` and
`NamespaceService.rename` (`namespace.py:1479-1590`) are only reached from
the public FS wrapper in production; the local callback does not reacquire
the outer mutex. `SyncExecutor._process_dir_task` (`:507-551`) is reached
only from `process_dir_task`; the guard callback owns one outer acquisition.
The reentrant mutex permits same-thread instrumentation inside the boundary,
but a nested **public** rename from a member callback while inner locks are
held must fail fast at entry, rather than try to upgrade namespace shared to
exclusive. No helper acquires the serializer after an inner lock. A direct
private-helper call cannot claim this safety boundary: FS-bound production
helpers should assert the outer owner, while standalone `SyncExecutor` unit
fixtures without an FS callback remain primitive tests and must not be used
as fence evidence. Existing direct `Backend.rename` and external member
writers are outside StorageFS coordination. Source search found no other
production callers of `_rename`, `NamespaceService.rename`, or
`_process_dir_task`; `NamespaceService.mkdir/rmdir` enqueue retries but do
not publish rename guards. They remain outside this narrow serializer and
cannot be claimed as general remote namespace reconciliation.

When retry owns the serializer first, later `StorageFS.rename` waits **before**
its namespace/path/upload locks, so it cannot publish a new guard during
retry member I/O. If rename owns it first, its guard is published before
retry can enter; retry checks every record immediately before I/O while
holding the serializer and existing shared namespace/path guard. The
affected upload drain begins before namespace shared, as in the existing
directory-rename order; file rename's `UploadLocks.hold` is inside its outer
serializer and ignores drains, so no opposing serializer holder can hold
one while waiting for the other. Sync queue locks are released before a
worker enters this guard; a rename may queue its retry while holding the
serializer, but the worker waits outside the queue lock. Member I/O remains
outside cache/index/freshness leaf locks. A foreground rename enters through
ordinary admission before this serializer; its wait uses ordinary capacity,
leaving protected demand and metadata slots available. A retry holds only
namespace shared plus its endpoint path locks during member I/O, so an
unrelated warm cached read and metadata browsing can progress. Same-path
work and other renames may wait; that limit needs deterministic evidence.

A blocked overlap returns an explicit deferred/failure result **before**
`_dir_task_current` or any backend I/O; it may not use that helper's
successful-drop path. Under existing `handle_task_failure` max retries the
queue item may eventually leave the queue, but that must be an explicit
terminal unresolved failure, never a success or silent task drop. The guard
and both origin debts stay.
Neither unknown nor matching tags bypass the check. No retirement, baseline,
CLEAN, upload or remote deletion is inferred from fencing. An old-code worker
already in a backend call would need a quiescent handoff before deployment;
this audit authorizes no worker interruption or deployment. This slice is
**non-shipping safety containment**, not useful read/upload recovery.

## Recovery feasibility and state

| Approach | Real-adapter boundary and conclusion |
| --- | --- |
| Read-only reconciliation | A proved pre-move failure needs no new guard. For an incomplete move, current `RenameProgress`, cache/index rows, per-file hashes and member listings cannot certify a directory's old/new lineage or all lazy descendants. Read-only checks could retire a guard only if a **previously durable, member-authoritative operation ID/identity receipt** already exists; none does today. |
| Conditional retry | Local Linux could add no-replace semantics, but that alone does not atomically compare the source identity; current `os.replace` does neither. SSH SFTP's plain rename can refuse an occupied target but has no portable source identity or operation receipt. A real production recovery path needs an actual member-side compare-and-move service, including the initial A operation, not a synthetic-only Backend option. |
| Coordinated writer ownership | A hypothetical exclusive member namespace could let a Local adapter order cooperating writes with durable member/local lineage. Ted has **not** authorized that restriction and root retains external-writer support. Bare inode numbers can be reused after deletion, so crash recovery still needs a persistent unique member marker/operation ID. A service or helper mutex around stat/rename does not bind unrelated POSIX writers. |

Thus **no useful production guard-retirement tracer is justified on the
current Local/SSH API and external-writer contract**. Root retains external
writer support; no writer restriction or remote service is authorized. A
hypothetical future implementation would need filesystem-enforced conditional
lineage or a separately approved change in writer authority, plus durable
pre-mutation evidence. If unavailable, leave ambiguous moves guarded and
expose partial completion under Ted's pending result ruling.
Do not implement a new optional Backend capability merely to make synthetic
tests pass.

If a future adapter supplies genuine lineage proof, a move record can go
`PENDING -> MEMBER_PROVED -> READY -> RETIRED`, per intended member and
generation. Initial A propagation needs exactly the same proof as B retry;
plain `RenameProgress.confirmed` is insufficient. READY requires source
lineage at new and absence of that source at old on every intended member,
plus explicit target-origin disposition. Under namespace/path serialization,
compare move and every intersecting guard revision, local inode/mutation
epoch and each origin's member debt generation. Retire only that matching
record. A directory root identity proves name movement only; descendants'
byte freshness and debts remain governed by their own checked fetch or
generation-checked sync. Never merge source and target debts by member ID.
Lifting one guard does not lift another or manufacture a CLEAN baseline.

| Situation | Safe boundary |
| --- | --- |
| B missed and returns | Without a real initial-A and B lineage receipt, retain guard. With both and settled target origin, only their matching record may retire. |
| Both old/new exist, or neither | No inference from names alone; retain both origins/debts. Collision/deletion policy remains open. |
| Replaced target owes A independently | Source A proof cannot settle target A. Preserve both generations and any separate overlapping record. |
| Nested/ancestor move | Old and translated negative prefixes survive; a late old receipt cannot clear newer overlap or certify lazy descendants. |
| Local write/unlink/create races retry | Identity/epoch change rejects old proof. Exact current DIRTY inode may stay readable by local witness; no stale upload/byte overwrite. |

## Durable prerequisite and future evidence

Before the first destructive local cache/index move, persist and sync an
intent containing move ID, old/new segment-bounded roots, source and target
local identities, separate origin debt generations, intended member IDs,
member lineage evidence **when actually available**, and unknown outcomes.
Persist local progress and each proven member result before guard retirement;
durably retire before CLEAN/upload admission. On restart, reconstruct all
unretired and translated negative prefixes before open/sweep/heal/sync.
Cache-file xattrs miss absent/lazy descendants; SQLite alone is rebuildable.
Missing/corrupt intent with possible partial move fails closed. Current code
has no such durable store, so even a safe in-memory retry fence is unshippable.

Deterministic **nonmount** schedules, implemented in
`TestNarrowRetryFence` and all six GREEN at pushed `83b26ab`, use the actual
FS-wired `SyncManager -> SyncExecutor.process_dir_task -> LocalBackend`
path with scratch members. A missing helper or a timeout is setup failure;
RED must be wrong remote bytes/names, false publication, or blocked unrelated
request on the identical Event-gated schedule. The final affected-suite run
executed all eight targets at that head, with six targets passing and three
older assertions failing in two targets; no full-suite pass is claimed:

1. `test_old_retry_blocked_by_newer_guard`: queue an untagged `/a -> /b` retry,
   then make `/b -> /c` incomplete and create a distinct local `/b` before
   it executes, so the existing `_dir_task_current` destination check really
   passes; repeat with a stale tag. The old retry must make zero backend
   mutation calls after the new guard, preserve exact bytes/names at A and B,
   both origin debt generations
   and no new CLEAN/baseline. The pre-fence executor's destination-presence
   check can allow the destructive old call.
2. `test_retry_prefix_intersections_both_directions`: make an unresolved
   `/parent/child` move; queue ancestor and descendant RENAME roots on either side,
   including a root that contains the guard root. Give each retry a current
   destination so the old executor would reach member I/O. All intersecting
   tasks make zero backend calls; `/parentish` and another nonintersecting task
   still call the real member. Assert exact local DIRTY bytes, lazy-child EIO
   and unchanged debts/baselines.
3. `test_retry_first_keeps_unrelated_reads_responsive`: pause a real member
   MKDIR retry at backend I/O;
   start a file rename below its destination and separately a directory
   rename. Both wait *outside* namespace/path locks. Before releasing the
   backend, an unrelated warm read and `readdir` complete with correct bytes
   and names. After release, rename publishes any required guard; check
   exact member/local bytes, no false CLEAN and no lost debt. The rejected
   global-exclusive design would fail the unrelated-progress assertion.
4. `test_rename_first_denies_queued_retry`: gate a real file and then directory rename
   before their incomplete-move guard publication; queue an older retry.
   Once the rename releases, retry rechecks coverage and makes zero backend
   mutation calls. Assert old/new canonical bytes or EIO, origin debts and
   absence of stale baseline. Include an already-queued task with no tag.
5. `test_mkdir_rmdir_retry_covered_by_guard`: queue MKDIR and RMDIR retries whose path
   is inside or an ancestor of an unresolved old/new domain; assert no
   unexpected member creation/deletion, no CLEAN/UPLOAD or debt clearing.
   Nonintersecting operations still run. Use a current-inode DIRTY child to
   prove the local read exception remains intact.

`sync-member-reconcile` tracks missed chains; `oq-unlink-member-copies` the
deletion policy. `oq-namespace-rename-collision` and
`oq-namespace-partial-rename-result` are pending Ted rulings, not authority
for overwrite or success-with-EIO. `oq-member-namespace-writer-ownership`
remains a filed question, but root's current boundary retains external-writer
support. A service mutex does not change that contract or supply linearizable
lineage; watcher/revalidation only detect later changes. No ruling on the
other questions is inferred.
