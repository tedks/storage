# Adjudicated in-memory tracer: namespace-subtree-uncertainty-coverage

Decision key: `namespace-subtree-uncertainty-coverage`. Root adjudicated the
bounded in-memory approach based on source proposal `63f22f8`, after
`proof-review-75b2efa.md`, warm Sol review and a Luna necessity critique.
The owner reconciled that decision against clean `2a9c88a` before editing.
This is implementation clearance for the in-memory tracer only, not full-PR
acceptance or a remote reconciliation policy.
The v3 `path-freshness-obligation` contract remains the higher-level byte and
debt boundary.

## Release barrier and atomicity amendment

The negative record covers every descendant regardless of whether a freshness
object, row, inode or prior open exists. Its positive DIRTY witness is only
local inode and generation evidence. The record stays even if an existing
queued rename retry runs; no retry presently proves both member namespaces or
settles source and target debts. Consequently a normal rename return with a
member failure can leave canonical CLEAN opens at either prefix returning EIO
and can defer uploads indefinitely. **That behavior is unshippable.** Before
PR #171 can be accepted, namespace reconciliation must resolve every intended
member and origin debt, and a durable restart marker/recovery rule must prevent
lost in-memory denial after a crash. The tracer does not claim bounded lifetime
or memory for unresolved records.

The rename holds its existing directory-exclusive namespace lock or sorted
file path locks through local cache/index move, member propagation and record
publication. An upload drain or file upload lock keeps queued sync from
starting in that interval. Eviction takes its existing nonblocking namespace
and path guard and checks coverage while holding it through unlink or
recomputable truncation. Healing takes that guard across check and DIRTY mark;
the stale sweep also takes it and re-stats after acquisition. Thus no sweep or
LRU mutation can act between the first cache move and failure publication.
The member operations remain outside the freshness leaf; no admission waits
inside namespace/path locks. A narrow rename receipt records local cache,
index and callback phases plus intended, attempted, confirmed, failed and
unknown stable member IDs. Callback completion alone cannot certify a member.

## Assumption and smallest complete domain

A failed or incomplete **directory** rename can affect every canonical name
at or below both its old and new roots, including a name with no current
freshness object, index row, cache inode, or prior open. The authoritative
quarantine domain must therefore be two segment-bounded prefixes, not a set
of known children. A file rename uses two exact names. Query `p == root or
p.startswith(root + "/")` after canonicalization; `/a` does not cover `/ab`.
Every applicable unresolved record blocks CLEAN interpretation and member
publication, even when a leaf object is absent or a newer record overlaps it.
A finite list of moved rows, cache entries, descriptors, and freshness keys
is useful only to attach *positive local inode evidence* and preserve existing
obligations. It cannot define the negative safety domain: an evicted file, an
unindexed prefix, an unlisted remote child, or a child created later defeats
any such enumeration. A prefix check covers future and lazy descendants
without enumerating them. Records are per failed move, not a new transaction
framework; memory grows with unresolved failures, so this proposal makes no
hard memory-bound claim.

One unresolved record carries `(old root, new root, file-or-directory,
revision, local outcome, source/target obligation snapshots)` and optional
per-destination witnesses from the completed local cache+index callback:
`(canonical path, source identity, dev, ino, DIRTY generation if stable)`.
The prefix record is the authority for *denial*. A witness is a separate,
positive exception for reading the current local DIRTY inode only after its
row, generation, inode and active-mutation state match. It never certifies a
member name, permits upload, clears a debt, or makes CLEAN bytes safe. Keep
source and target obligations by origin identity while names are ambiguous;
merging two different identities' debts into one per-member dictionary can
lose one debt when both name the same member. A path-local freshness object
may still carry a moved writer's active group, but it is not the coverage
index. No prior `_freshness_snapshot()` result proves object absence because
that diagnostic returns `None` for an object with no debt or uncertainty.

## Outcome and coverage matrix

| Rename outcome | Domain before next opener or worker | Local byte proof | Debt / identity rule |
| --- | --- | --- | --- |
| Successful cache+index move and confirmed propagation to every intended member | No new quarantine. Rekey source subtree, retire replaced target identity, invalidate old tokens. | Existing normal DIRTY/clean rules. | Preserve each source member debt at its new name; target identity retires. |
| Failure proven before cache move | No new quarantine; old and target names retain their original states. | Their existing evidence remains usable. | Do not rekey, merge, or clear either debt. |
| Cache move, then index rewrite fails or callback does not complete | Old and new prefixes (or exact file names), including cached, uncached, indexed, unindexed and future descendants. | No moved-source DIRTY witness; old target row, stat coincidence and xattr are insufficient. Canonical open EIO until independent proof. | Keep source/target obligations separately, invalidate old attempts; never heal old row into authority. |
| Cache+index callback completes, then propagation raises or reports one or more failed members | Same two-prefix domain; callback's moved rows may carry inode witnesses. | Only matching current local DIRTY row/generation and a settled mutation group may be read. CLEAN sibling and missing-cache child remain unproved. | Keep every debt and quarantine all uploads/CLEAN publication; a queued member rename is not proof of completion. |
| Later file or directory move intersects an unresolved domain in either direction, including an old/new record root nested below the moved directory | Retain the original record and conservatively add a full old/new root record before releasing namespace serialization, even if every intended member confirms the later move. This may quarantine unaffected siblings under the moved parent; it does not claim minimal coverage. Segment boundaries exclude `/parentish` from `/parent`. A proven pre-move failure adds no record. | A carried witness remains usable only for the same current inode and updated path identity. | Original origin-distinct debt snapshots remain on the old record; the later move is not reconciliation and stale callbacks remain invalid. |
| Local unlink/create under an unresolved prefix | Prefix domain remains for future lookup and member names. | Confirmed unlink retires only that local inode. A new create can earn its own exact current-inode DIRTY proof after successful publication; failed publication stays unproved. | Retire only the proved removed identity; preserve unresolved member obligations and do not authorize sync of the new inode. |

A target directory with cached children cannot be replaced by the current
`rename` path (`os.rename` rejects a nonempty destination); an empty target
whose *index* has descendants is rejected by `has_rows_below`. Those are
pre-move failures, not invented successful replacements. The new-prefix
guard still covers target member children that were never present locally.
An existing file target can be replaced; cache-only failure must not reuse its
old CLEAN or DIRTY row as proof for the moved source inode.

## Publication and guard touchpoints

Register the two-prefix record under the existing directory namespace
exclusive lock (or sorted file path locks) before releasing it after any
ambiguous move. The cache+index callback records moved-row witnesses under
metadata -> descriptor -> freshness order. A failed rename after cache move
must publish quarantine even if the callback never ran. Do not wait for
foreground admission under namespace/path locks or perform member I/O under
the freshness leaf. Existing rename propagation keeps namespace/path
serialization across member I/O; reconciliation must retain that ordering.
A normal backend failure returned by `_propagate_dir_operation` is an
incomplete outcome, even when `NamespaceService.rename` queues a retry and
returns normally. The tracer now passes `RenameProgress` to
`StorageFS._rename`, recording local phases and intended, attempted,
confirmed, failed and unknown member IDs, including exception progress.
It does not prove member object lineage or settle a later retry. A queued
`DirSyncTask` or its later success does not by itself settle all prior
old/new names or generations. See `namespace-reconciliation` for the
proposed additional proof boundary.

| Path | Check required at the existing boundary |
| --- | --- |
| `open` and demand retry | Resolve current canonical path after admission; query all covering prefixes before revalidation or `os.open`. Under metadata/descriptor/freshness validate an exact local DIRTY witness, current inode and generation with no active/failed mutation; otherwise EIO. A direct conflict-suffixed open is not permission to publish a canonical baseline. |
| `_revalidate_cached_file` | Before stat/TTL shortcut and before invalidation, refuse a covered canonical CLEAN path. A member stat must not convert ambiguous names into proof. |
| `_ensure_file_in_cache`, `_full_fetch_guard`, `_publish_full_fetch` | Check coverage before selecting/fetching and again under the existing locked no-replace publication guard; an evicted or unindexed child cannot fetch B, install, index or clear debt while covered. The guard, not an early check alone, closes the rename race. |
| `_stream_freshness_valid`, `_stream_commit_guard`, failed-prefix promotion | Check coverage at final path-locked commit before no-replace link and baseline publication. Retired sessions may clean up only their own partial; they cannot install a prefix or full row inside the domain. |
| `_should_sync_path` and actual `SyncExecutor` admission/status publication | Reject every covered child, including one with no freshness object, before `copy_from_local`, SYNCING and CLEAN. Existing upload locks serialize a rename against an in-flight upload, but a prefix check must be revalidated at executor admission; a queued retry cannot bypass it. |
| `heal_stale_clean`, post-crash sweep, normal LRU and recomputable eviction | Refuse covered paths before stat mismatch promotes an old target row to DIRTY, before deletion of a proven local inode, and before recomputable in-place truncation. The cache/index/descriptor -> freshness lock order permits a short coverage query; no freshness leaf may wait on metadata. |

`getattr` and listing may still reveal candidate names for browsing, but their
memoized member attributes cannot be treated as byte/source proof or allowed
to seed a CLEAN row for a covered child. The exact user-visible metadata
error policy is outside this byte-safety proposal; no listing result retires
quarantine.

## Retirement and regression plan

An explicit confirmed local unlink retires only the removed local identity;
the old/new prefix record continues to guard a later lookup. A confirmed
local create/replacement supplies a new local identity and may earn local
DIRTY read proof, but does not retire the record or permit upload. The whole
record may retire only after a separately specified reconciliation verifies
both old and new member namespaces for every intended member, preserves or
settles each origin's debts with generation checks, and publishes the result
under namespace serialization. Current best-effort directory retries do not
provide that receipt. A confirmed `rmdir` could retire the domain only if it
also proves no descendant and no outstanding member name/debt on both sides;
current `rmdir` does not make that proof. Durable restart recovery remains a
separate unimplemented boundary.

Original audit test plan, retained for comparison with tracer evidence:

1. Materialize a cached indexed source child through normal operations,
   restart the scratch `StorageFS` so its in-memory freshness map is actually
   empty, then inject propagation failure after a real directory cache+index
   callback. Assert the child is covered despite no prior object; CLEAN open,
   full fetch, stream completion, queued sync and heal/eviction cannot serve
   stale B bytes, upload, or publish CLEAN. This restart is fixture setup for
   an empty in-memory map, not acceptance of durable debt recovery.
2. With the same absent-state child, fail index rewrite immediately after
   cache move over an existing target file row, once CLEAN and once DIRTY,
   including matching size/mtime. Assert both names remain covered, no old
   target row earns local proof, no upload or CLEAN publication occurs.
3. Include an indexed evicted child, cache-only unindexed child and a nested
   path absent from all tables at failure time. Let a member later expose the
   nested name. Every canonical open/commit/sync path must still find the
   prefix record; no ranked fallback or TTL certifies bytes.
4. Complete local callback for two DIRTY descendants (one nested), a CLEAN
   sibling and an active writer; fail propagation by both a raised exception
   and the normal returned failed-member list. Check exact local bytes,
   inode/generation proof, retained independent debts, failed-mark EIO,
   no upload and no CLEAN claim. The corrected 75b2efa assertion by itself
   did not establish an absent-state child; the tracer checks map membership.
5. Force pre-move failure with a nonempty target directory or injected early
   error. Assert old/target identities, debts and normal access unchanged.
   Force a nested move intersecting a quarantined prefix, then late fetch and
   stream callbacks from both identities; they must remain inert. After a
   confirmed unlink/create at one child, assert only its local identity
   changes, new DIRTY bytes require exact proof, and lazy sibling lookup
   remains quarantined.

Each new test needs a same-schedule behavioral pre-fix failure in bytes,
`copy_from_local`, row status or inode ownership, then post-fix success.
`_freshness_snapshot(path) is None` alone is not the absent-object setup;
inspect the actual map under its lock after constructing scratch fixtures.
No tests or application edits were authorized or run during the source-only
audit. The later adjudicated tracer has separate, recorded test receipts.

## Root decision and interface limit

Root selected two-prefix negative coverage plus a narrow rename progress
receipt and exact local DIRTY witness for the tracer. Enumerating current
rows or cache files alone cannot cover lazy descendants. The progress receipt
exposes returned failed members and partial exception progress; the separate
record preserves two origins' debts even if they share a member ID. The
tracer does not decide remote conflict/deletion policy or when queued remote
operations count as reconciliation.
