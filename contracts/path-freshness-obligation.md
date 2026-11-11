# Contract: path-freshness-obligation v2 (proposed)

Decision key: `foreground-open-pin-ownership`. Root adjudicated the intent at
`0631a73bff98b7649b9d65c181b5fe04dcf72ff9`; this source contract is the
responsiveness lead's application of that ruling at the `a95f6d6` checkpoint.
It is review input, not authority to repair `storage/` code or run tests.

## Boundary and identity

An observation that CLEAN cached bytes differ from checked member **A** adds
one shared debt for that member and logical file before the cache entry is
dropped. The canonical path maps to a never-reused file identity and a map of
debts keyed by stable member ID; each member's observation generation increases
monotonically. Each debt carries its observed size/mtime. Display labels are
never identity. Every opener, full fetch, stream and rename sees the same map.
An attempt token is `(file identity, member ID, that member's generation,
local-content epoch)`. A later A observation supersedes only older A evidence;
an observation of changed **C** cannot erase A's debt. An old attempt cannot
publish bytes, baseline or cleanup against a newer generation or local epoch.

Each debt lasts until (1) bytes from its checked member are installed for the
matching file identity/generation and their matching baseline is published,
(2) a generation-checked local sync proves that the member now has the local
bytes, or (3) an explicit namespace operation proves that identity no longer
exists at that path. Completing C does not satisfy A. Revalidation's cache
invalidation, eviction, an open's exit, cancellation, failed transfer and
member unavailability are not
such proof. A fallback fetch from **B** can serve its bytes when A is
unavailable, but records A as owed recheck and cannot represent B as proof
that A's observation was satisfied. The debt forces A's check when it returns,
even if B's cached copy is inside its normal freshness window. If A is
available and the cache is CLEAN, selection must prefer A over the ordinary
member ranking; tests force that ranking to choose stale B so this rule cannot
pass accidentally.
Multiple available debts require a deterministic member choice, but all other
members' debts remain owed. A local DIRTY inode wins over all member debts:
serve local bytes, never overwrite them with a fetch, and retain debts until
the matching member sync or a later clean recheck resolves them.

## State transitions

| Event | Required transition |
| --- | --- |
| Changed A observed | Publish A's `(identity, A-generation+1, stat, owed)` under the path lock before cache invalidation. Persist the debt on any surviving cache inode before returning; never hide it in an opener's local variable while admission waits. Do not remove C's debt. |
| Demand to ordinary transfer | Exit namespace/path locks, release demand slot, wait for ordinary, then reacquire locks and re-read current path identity and latest obligation. No member selection from an old local snapshot. |
| A fetch succeeds | Transfer into a private staging file. At commit, compare file identity, A generation, local-content epoch, inode identity, active mutations, open writers and DIRTY status before an atomic **no-replace** install. Never unlink or overwrite an intervening create/write/truncate. Publish A's baseline against the installed inode and clear only matching A debt. A mismatch discards or retries the stage; it may not install bytes first and validate afterward. |
| A fetch fails or admission is cancelled | Release only that attempt's resources and admission slot. The shared obligation stays owed. |
| A unavailable, B used | Put A's durable debt marker on B's staged inode before its no-replace install. Publish B's byte/baseline identity without clearing A's debt; keep A visible to subsequent openers and restart recovery. |
| A returns | Check A despite a fresh B cache; fetch A if its observed change still applies, then clear only the matching debt after A bytes and baseline commit. |
| Later observation or late stream | A new generation for the **same member** wins. A stream's token is captured before range reads and checked before partial-to-final installation and again before baseline/debt publication. An older stream may not replace newer bytes, baseline or any member debt. |
| Local write/truncate/create | Announce a mutation of the target inode before the first byte change (or O_TRUNC), advance its local-content epoch, then publish DIRTY and its generation before retiring the active-mutation flag. The byte-to-DIRTY interval is never a clean fetch-commit window. Create/replacement advances file identity. A failed mark cannot turn changed local bytes into CLEAN evidence; the live-stat/xattr dirty-healing rule applies. |
| Confirmed rename | Move the source identity and its obligation atomically with the successful namespace move; retire any replaced target identity. An opener waiting on the old name must re-resolve and cannot clear the moved debt. |
| Rename fails before move | Preserve both path identities and debts. If a partial move leaves membership or cache location uncertain, invalidate both affected path identities conservatively and retain a forced reprobe obligation for every still possible name; do not publish a clean stale baseline. |
| Confirmed unlink or replacement | Advance the removed name's identity and retire only that identity's debt. A late token for the old identity is inert. |

The cache helper's "path exists afterward" result does not prove this
attempt installed it: a no-replace link may have lost to another creator.
Fetch success requires the exact staged inode to own the final name before
the member baseline or debt can change.

## Lock and publication order

`NamespaceLock` (shared, or exclusive for directory rename) precedes sorted
`PathLocks`; cache lock precedes index metadata lock, then descriptor lock;
the short content-epoch/freshness lock is a leaf. A write through an existing
descriptor need not take a path lock: it announces its inode mutation under
the leaf lock, releases it during `pwrite`, then publishes DIRTY under the
index lock before retiring the active flag under the leaf lock. Truncate and
create use the same announcement around their actual byte/O_TRUNC boundary.
Fetch commit takes the leaf lock inside its existing path/cache/index/descriptor
critical section, rejects active mutations and changed epochs, and holds it
through no-replace link and baseline/debt publication. No path lock waits for
an active writer; a contested stage is discarded or retried. The writer never
holds the leaf while waiting for the index lock, avoiding a lock cycle with
rename and fetch. A failed byte or dirty publication leaves conservative
DIRTY/uncertain state, never permission to install over the inode.
Publish observation and test token identity while holding the relevant path
lock. Do not perform member I/O while holding the freshness-map lock. Never
wait for foreground admission while holding namespace or path locks. A stream
worker must leave its session/cache locks before acquiring the path lock for
final installation and baseline publication. File replacement, index state,
cache xattr baseline and debt clearing must have one ordered publication
point; a reader cannot observe new clean bytes paired with an older baseline
or a cleared newer debt.

Mount-lifetime debts survive callback exits and worker failures. The durable
authority is a versioned cache-file xattr containing file identity, member-ID
debt map and generations; SQLite is rebuildable acceleration only. Before a
changed observation can invalidate a surviving inode, persist and sync its
marker. Before B fallback can become visible, place and sync A's marker on
B's staged inode; no-replace link and parent-directory sync precede a CLEAN
index/baseline claim. A crash between these steps leaves either a marked cache
or an uncertain path. On mount restart, the first open of **every** CLEAN
cached path is ordinary recovery work until its xattr/row/inode identity is
verified. A valid marker restores all debts before a freshness-window shortcut.
Missing/invalid marker or missing cache triggers forced reprobe of every known
holder using persisted member baselines. Any changed holder remains owed; if
baselines do not disambiguate differing copies, compare content hashes or fail
the canonical open conservatively. No cached B copy is certified by elapsed
TTL or the absence of an in-memory debt. If A is unavailable, B may serve as
offline fallback with A debt preserved in a newly durable marker; returning A
is rechecked before B can be called fresh. No network/member I/O occurs while
the freshness-map lock is held.

## Acceptance observations

Tests use scratch two-member local fixtures, bounded `Event` barriers, and
the existing direct FUSE callback path. They assert returned bytes, selected
member, published baseline, remaining A recheck debt, admission count, and
absence of transient per-open pins. The regression source proposes two
diagnostic seams: `_freshness_snapshot(path)` returns either `None` or an
immutable view with `path_identity`, `debts` (member ID to generation/stat),
`owed_member` (current selected debt for the two-member tests),
`recheck_members`, `local_epoch`, and `state`;
`_freshness_token(path, member_id)` captures the identity/member generation
and local epoch associated with a stream at start. These are internal
test observations, not user APIs or a second authority. Root may choose an
equivalent internal representation when reviewing the boundary, provided the
same observable transitions remain testable.

The cap still bounds active Python callbacks, not libfuse 2 native threads.
The cap-3 slow cached-open freshness stat remains a documented product limit
under the current warm-open demand ruling.
