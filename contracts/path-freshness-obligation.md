# Contract: path-freshness-obligation v1 (proposed)

Decision key: `foreground-open-pin-ownership`. Root adjudicated the intent at
`0631a73bff98b7649b9d65c181b5fe04dcf72ff9`; this source contract is the
responsiveness lead's application of that ruling at the `a95f6d6` checkpoint.
It is review input, not authority to repair `storage/` code or run tests.

## Boundary and identity

An observation that CLEAN cached bytes differ from checked member **A** creates
one shared obligation for the logical file, before the cache entry is dropped.
The obligation is keyed by canonical path and carries a never-reused path
identity, a monotonically increasing observation generation, A's stable member
ID, and the observed size/mtime. Display labels are never identity. Every
opener, full fetch, stream and rename sees the same current obligation. A
caller may hold an attempt token `(path identity, generation, member ID)`, but
never owns or removes the shared obligation. A later observation supersedes an
older generation; an old attempt cannot publish bytes, baseline or cleanup
against the newer one.

The obligation lasts until (1) bytes from the checked member are installed
for the matching path identity/generation and their matching baseline is
published, or (2) an explicit namespace operation proves that identity no
longer exists at that path. Revalidation's cache invalidation, eviction, an
open's exit, cancellation, failed transfer and member unavailability are not
such proof. A fallback fetch from **B** can serve its bytes when A is
unavailable, but records A as owed recheck and cannot represent B as proof
that A's observation was satisfied. The debt forces A's check when it returns,
even if B's cached copy is inside its normal freshness window. If A is
available, selection must prefer A over the ordinary member ranking; tests
force that ranking to choose stale B so this rule cannot pass accidentally.

## State transitions

| Event | Required transition |
| --- | --- |
| Changed A observed | Publish `(identity, generation+1, A, stat, owed)` under the path lock before cache invalidation. Never hide it in an opener's local variable while admission waits. |
| Demand to ordinary transfer | Exit namespace/path locks, release demand slot, wait for ordinary, then reacquire locks and re-read current path identity and latest obligation. No member selection from an old local snapshot. |
| A fetch succeeds | Transfer into a private staging file. Under the path lock, compare identity/generation and source A before replacing cache bytes and publishing A's baseline. Clear only the matching obligation and recheck debt. A mismatched completion is discarded or retried; it may not install bytes first and validate afterward. |
| A fetch fails or admission is cancelled | Release only that attempt's resources and admission slot. The shared obligation stays owed. |
| A unavailable, B used | Publish B's byte/baseline identity without clearing A's obligation; keep an A recheck debt visible to subsequent openers and restart recovery. |
| A returns | Check A despite a fresh B cache; fetch A if its observed change still applies, then clear only the matching debt after A bytes and baseline commit. |
| Later observation or late stream | A new generation wins. A stream's token is captured before range reads and checked before partial-to-final installation and again before baseline/debt publication. An older stream may not replace newer bytes, baseline or debt. |
| Confirmed rename | Move the source identity and its obligation atomically with the successful namespace move; retire any replaced target identity. An opener waiting on the old name must re-resolve and cannot clear the moved debt. |
| Rename fails before move | Preserve both path identities and debts. If a partial move leaves membership or cache location uncertain, invalidate both affected path identities conservatively and retain a forced reprobe obligation for every still possible name; do not publish a clean stale baseline. |
| Confirmed unlink or replacement | Advance the removed name's identity and retire only that identity's debt. A late token for the old identity is inert. |

## Lock and publication order

`NamespaceLock` (shared, or exclusive for directory rename) precedes sorted
`PathLocks`; cache/index locks follow; the freshness map's lock is a leaf.
Publish observation and test token identity while holding the relevant path
lock. Do not perform member I/O while holding the freshness-map lock. Never
wait for foreground admission while holding namespace or path locks. A stream
worker must leave its session/cache locks before acquiring the path lock for
final installation and baseline publication. File replacement, index state,
cache xattr baseline and debt clearing must have one ordered publication
point; a reader cannot observe new clean bytes paired with an older baseline
or a cleared newer debt.

Mount-lifetime obligations must survive all callback exits and worker
failures. If fallback B's cache can survive process restart, A's debt must be
recoverable from a cache xattr or a conservative first-open reprobe of all
holders; SQLite alone is rebuildable and cannot be the only authority. A
missing cache after a crash must not be treated as proof that A's changed
bytes were fetched.

## Acceptance observations

Tests use scratch two-member local fixtures, bounded `Event` barriers, and
the existing direct FUSE callback path. They assert returned bytes, selected
member, published baseline, remaining A recheck debt, admission count, and
absence of transient per-open pins. The regression source proposes two
diagnostic seams: `_freshness_snapshot(path)` returns either `None` or an
immutable view with `path_identity`, `generation`, `owed_member`,
`recheck_members`, and `state`; `_freshness_token(path, member_id)` captures
the identity/generation associated with a stream at start. These are internal
test observations, not user APIs or a second authority. Root may choose an
equivalent internal representation when reviewing the boundary, provided the
same observable transitions remain testable.

The cap still bounds active Python callbacks, not libfuse 2 native threads.
The cap-3 slow cached-open freshness stat remains a documented product limit
under the current warm-open demand ruling.
