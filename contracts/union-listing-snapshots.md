# Contract: union listing snapshots v0 (proposed)

- **Owner:** Storage namespace owner. **Consumers:** FUSE enumeration and
  desktop change delivery. No consumer implementation acknowledged yet.
- **Inputs:** member ID/configuration generation, canonical directory identity,
  refresh generation, local namespace revision, and either a complete set of
  validated immediate children or a typed unavailable/timeout/cancelled result.
  Absence confirmed by a reachable backend is successful empty. Partial reads
  and permission failures are not confirmed absence. Do not follow a supplied
  child name outside its parent; reject invalid or colliding backend entries
  according to existing conflict policy rather than fabricating completeness.
- **Outputs:** immutable snapshot with revision, per-member observation status,
  observation ages, local-overlay revision and merged names/available metadata.
  A snapshot can be incomplete or stale; the internal status cannot say fresh
  while any required member's observation is old or missing.
- **Invariants:** union successful per-member observations and the local
  namespace under existing conflict rules. Success replaces only that member's
  prior set, including empty. Failure preserves last-known evidence with its
  age/status; it never renews freshness or proves deletion. Enumeration handles
  pin one revision/offset mapping. Successful results publish independently;
  incomplete pages never remove unseen names. Cached requests do not wait on
  remote I/O. At most one current refresh per directory/member; newer local
  mutations/configuration/refresh generations reject stale publication.
- **Failure semantics:** no evidence plus all failures returns an explicit
  error. A failed member cannot veto healthy member results. Retained stale
  names may fail at open; listing is not proof of readable bytes. Deadline and
  cancellation do not allow abandoned workers to publish later. If transport
  cancellation is unavailable, account for the still-running worker and keep
  concurrency bounded; never spawn replacements without admission.
- **Resource policy:** limits on queued/running refreshes, active directories,
  snapshot bytes/entries and backoff are mandatory before implementation
  acceptance. Do not hold global/namespace locks over remote I/O or admission.
  Cache eviction cannot erase dirty files or alter the independent byte cache.
- **Intentionally unspecified:** numeric freshness/visibility policy pending
  the existing open question; order across member completions; content
  equality; persistence across restart. POSIX readdir cannot encode a stale
  badge; internal observability is not a claimed Nautilus indicator.
- **Compatibility:** v0 is a proposal. Breaking changes to observations or
  revision semantics require coordinated edits of both consumers; no hidden
  dependence on unmerged PR171 freshness structures.
- **Boundary tests (planned, UNWRITTEN):** empty A + populated B; failed A
  retaining stale evidence; partial page failure; all-failed/no-cache; stale
  response after local rename/unlink; slow A while B publishes; cancelled old
  refresh; capacity saturation; stable offsets under background publication;
  cache eviction and file/directory/type conflicts.
- **Interpretation register:** [spec](../docs/specs/live-union-listings.md),
  `oq-listing-stale-serve-dir-mtime`.
