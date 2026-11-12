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
  observation ages, local-overlay revision and merged entries. Each entry
  carries its name, type/available metadata, contributing member IDs and a
  coordinator-issued incarnation token plus entry revision. These tokens fence
  observations/events inside one mount generation; they do not claim a stable
  remote inode identity. Observed disappearance/recreation or type replacement
  gets a new incarnation; an unchanged observation preserves it. A detectable
  ambiguous identity change invalidates conservatively. Same-metadata unseen
  remote replacement remains an explicit detection limitation.
  A snapshot can be incomplete or stale; the internal status cannot say fresh
  while any required member's observation is old or missing.
- **Invariants:** union successful per-member observations and the local
  namespace under existing conflict rules. Success replaces only that member's
  prior set, including empty. Failure preserves last-known evidence with its
  age/status; it never renews freshness or proves deletion. Enumeration handles
  pin one revision/offset mapping. Successful results publish independently;
  incomplete pages never remove unseen names. Cached requests do not wait on
  remote I/O. At most one current refresh per directory/member; newer
  configuration/refresh generations reject stale publication. A directory
  identity change rejects its old scan. Local mutations are fenced per name:
  merge a valid member observation with the current local overlay and its
  create/rename/deletion revisions, preserving newer local authority while
  publishing unaffected remote names. Unrelated local churn must not force
  whole-directory refresh rejection forever.
- **Failure semantics:** no evidence plus all failures returns an explicit
  error. Empty A plus unavailable B with no prior cache is an incomplete
  candidate, not certified union emptiness. Public cold-partial behavior is
  unresolved under `oq-listing-cold-partial-result`; integration cannot choose
  success-empty by default. A failed member cannot veto collection or internal
  publication of healthy member observations. Retained stale
  names may fail at open; listing is not proof of readable bytes. Deadline and
  cancellation do not allow abandoned workers to publish later. If transport
  cancellation is unavailable, account for the still-running worker and keep
  concurrency bounded; never spawn replacements without admission.
- **Resource policy:** limits on queued/running refreshes, active directories,
  snapshot bytes/entries (including revisions pinned by open directory
  handles) and backoff are mandatory before implementation
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
  retaining stale evidence; empty A + failed/slow B without cache; partial page
  failure; all-failed/no-cache; continuous local creates while a remote add
  still publishes; event identity after delete/recreate; stale
  response after local rename/unlink; slow A while B publishes; cancelled old
  refresh; capacity saturation; stable offsets under background publication;
  cache eviction and file/directory/type conflicts.
- **Interpretation register:** [spec](../docs/specs/live-union-listings.md),
  `oq-listing-stale-serve-dir-mtime`, `oq-listing-cold-partial-result`.
