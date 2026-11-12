# Proposed member-name reconciliation boundary for PR #171

Status: source-only feasibility correction to proposal `88c3982`, reconciled
against clean pushed `f5f9330`. Root/warm Sol review: FINDINGS; fresh Luna
necessity critique: SIMPLIFY. No implementation or runtime clearance. This
contract neither retires a current guard nor makes the in-memory tracer
shippable. Stable member IDs, never display names, key receipts and debts.

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
| Local (`storage/backends/local.py:216-230`) | `lstat` can observe `(dev, ino)` at an instant; absent old returns success; `os.replace` can destroy an occupied target. | Atomic source-identity check plus target disposition under a stated writer-ownership regime, or member-side service. `renameat2` no-replace alone does not atomically test source identity. |
| SSH (`storage/backends/ssh.py:550-580`) | `posix_rename` replaces; fallback SFTP rename may fail on occupied target; missing old is swallowed. Standard SFTP stat has no portable stable inode/operation ID. | Server-side compare-and-move receipt or equivalent durable operation service. SFTP pre/post stat cannot supply it against unrelated writers. |

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

## Production retry fence: the next bounded safety slice

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
holds namespace **shared** plus exact endpoint locks. It cannot serialize a
directory retry at `/b` with a file rename beneath `/b/child`, because the
two exact locks differ. For a minimal conservative fence, make directory-task
admission acquire `UploadLocks.drain` on task roots **before** the namespace
exclusive lock, as foreground directory rename does (`fs.py:2598-2601`),
then check all covering records and local task currency while holding
exclusive through any backend call and its result. This orders an already
admitted retry before a future directory **or file** guard publication:
every path-locked handler needs namespace shared, so it waits for the retry;
if publication won first, retry sees the guard and never enters backend I/O.
The drain blocks concurrent uploads at either subtree without waiting while
holding namespace. No foreground admission wait occurs inside locks; member
I/O remains outside the freshness leaf. A blocked overlap returns an
explicit deferred/failure result, **not** `_dir_task_current`'s successful
drop. Existing retry exhaustion may still lose the queued attempt; the
negative record and debts persist and must be reported unresolved. No
quarantine retirement, baseline change or member deletion follows a fence.
The fence can govern only tasks that enter through the new guard; any
already-running old-code worker would need a quiescent handoff before a
deployment. This audit authorizes no worker interruption or deployment.

This coarse exclusive lock can hold browsing behind a slow background member
call and violates the final responsiveness aim if used as a release policy.
It is acceptable only as a bounded **non-shipping safety tracer** with a
specified member timeout and latency measurement in a later authorized
phase. A later narrow in-flight subtree-domain registry could avoid a global
exclusive call, but that is additional machinery requiring its own lock-order
review; this audit does not design or authorize it. The fence must not be
described as restored read/upload responsiveness.

## Recovery feasibility and state

| Approach | Real-adapter boundary and conclusion |
| --- | --- |
| Read-only reconciliation | A proved pre-move failure needs no new guard. For an incomplete move, current `RenameProgress`, cache/index rows, per-file hashes and member listings cannot certify a directory's old/new lineage or all lazy descendants. Read-only checks could retire a guard only if a **previously durable, member-authoritative operation ID/identity receipt** already exists; none does today. |
| Conditional retry | Local Linux could add no-replace semantics, but that alone does not atomically compare the source identity; current `os.replace` does neither. SSH SFTP's plain rename can refuse an occupied target but has no portable source identity or operation receipt. A real production recovery path needs an actual member-side compare-and-move service, including the initial A operation, not a synthetic-only Backend option. |
| Coordinated writer ownership | If Ted explicitly made a member namespace exclusively writable by this mount, a real Local adapter could use pre-move `(dev, ino)` observations and the existing namespace lock to order all *cooperating* writes; a guarded no-replace move and postcondition could then be a candidate end-to-end slice. Durable recovery would also need a persistent unique member marker/operation ID: bare inode numbers can be reused after deletion. Current external-member changes/watchers contradict assuming exclusive ownership silently. An in-process helper lock or two stats do not bind unrelated writers. SSH would still need reliable member identity or a server-side service. |

Thus **no useful production guard-retirement tracer is justified on the
current Local/SSH API and external-writer contract**. The smallest real
candidate is Local under an explicit exclusive-writer ruling plus durable
pre-mutation member/local lineage and an inode-reuse-safe marker; otherwise a
member-side conditional operation service is needed. If neither is available,
leave ambiguous moves guarded and expose partial completion under Ted's
pending result ruling.
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

Focused future schedules, all **UNRUN**; the first three fit the proposed
fence slice and must fail pre-fix on observable remote/local effects, not a
missing seam or timeout:

1. Gate an old untagged `/a -> /b` retry; complete newer incomplete
   `/b -> /c`, then release it. Existing path-current check can allow the
   old member operation; assert exact B member bytes/names are unchanged by
   the fenced retry, no false CLEAN/upload and both origin debts/guards stay.
2. Repeat with a retry whose root is an ancestor of `/parent/child` guard,
   then reverse ancestor/descendant roles; include `/parentish` nonintersecting
   control. Assert actual member rename call count zero only for overlap,
   current local DIRTY bytes preserved and lazy child still denied.
3. Pause an already-admitted directory retry immediately before backend I/O;
   race file rename under its destination subtree. Assert exclusive ordering:
   either retry completes before guard publication or the guard wins and no
   member I/O occurs. No stale remote bytes, false baseline or lost debt.
4. Recovery phase only: two real Local members with proved initial and retry
   lineage under an approved ownership contract; B first fails, then returns.
   Assert exact child bytes and useful read/upload after guard retirement,
   with independent source/target debts still owed until generation proof.
5. Durable phase only: crash after local move and after partial member result;
   restart before workers, assert lazy descendants remain guarded and current
   DIRTY witness bytes survive. No xattr-only or row-only certification.

`sync-member-reconcile` tracks missed chains; `oq-unlink-member-copies` the
deletion policy. `oq-namespace-rename-collision` and
`oq-namespace-partial-rename-result` are pending Ted rulings, not authority
for overwrite or success-with-EIO. `oq-member-namespace-writer-ownership`
records whether member namespace writers may be restricted; recommend retaining
external-writer support and requiring a real conditional member service (or
keeping ambiguous moves blocked). No ruling is inferred from this audit.
