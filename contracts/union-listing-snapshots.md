# Contract: union listing snapshots v0 (proposed)

- **Owner:** Storage namespace owner. **Consumers:** FUSE enumeration and
  desktop change delivery. No consumer implementation acknowledged yet.
- **Inputs:** member ID/configuration generation, canonical directory identity,
  refresh generation, local namespace revision, and either a complete set of
  validated immediate children or a typed unavailable/timeout/cancelled/
  unverified-root result.
  Absence confirmed by a reachable backend under a verified member root is
  successful empty; a missing or replaced member root (for example a failed
  disk leaving an empty mountpoint) is an unverified-root result, not empty. Partial reads
  and permission failures are not confirmed absence. Do not follow a supplied
  child name outside its parent; reject invalid or colliding backend entries
  according to existing conflict policy rather than fabricating completeness.
- **Outputs:** immutable snapshot with revision, per-member observation status,
  observation ages, local-overlay revision and merged entries. Each entry
  carries its name, type/available metadata, contributing member IDs and a
  coordinator-issued incarnation token plus entry revision. The incarnation
  belongs to the merged entry, not an individual member contribution; losing
  one contributor alone does not recreate an entry still present in the union.
  Revisions strictly increase within an incarnation on each committed visible
  entry change, including remote metadata changes and local overlay mutations.
  Unchanged observations keep the revision; an old revision is never reused.
  Publication serializes revision assignment with the visible state change.
  A committed removal advances the removed incarnation's revision and emits
  an explicit removal observation. New creation uses a new incarnation.
  Events carry (mount generation, incarnation, revision); a consumer rejects
  old generations/incarnations and revisions older than the current entry or
  removal observation,
  and handles a duplicate current revision idempotently. Incarnations are
  never reused within a mount generation. If removal/history evidence was
  evicted, require a current resnapshot instead of trusting a delayed event.
  These tokens fence
  observations/events inside one mount generation; they do not claim a stable
  remote inode identity. Observed disappearance/recreation or type replacement
  gets a new incarnation; an unchanged observation preserves it. A detectable
  ambiguous identity change invalidates conservatively. Same-metadata unseen
  remote replacement remains an explicit detection limitation.
  A snapshot can be incomplete or stale; the internal status cannot say fresh
  while any required member's observation is old or missing. This per-member
  completeness status is separate from the serving TTL: it does not change
  the fresh/stale/expired serving schedule or trigger per-access refresh,
  and it leaves the `oq-listing-negative-lookup` ruling unchanged.
- **Invariants:** union successful per-member observations and the local
  namespace under existing conflict rules. Success replaces only that member's
  prior set, including empty. Failure preserves last-known evidence with its
  age/status; it never renews freshness or proves deletion. Enumeration handles
  pin one revision/offset mapping. Successful results publish independently;
  a member whose enumeration fails midway publishes none of its staged
  pages and removes no names. Cached requests do not wait on
  remote I/O. At most one current refresh per directory/member; newer
  configuration/refresh generations reject stale publication. A directory
  identity change rejects its old scan. Local mutations are fenced per name:
  merge a valid member observation with the current local overlay and its
  create/rename/deletion revisions, preserving newer local authority while
  publishing unaffected remote names. Unrelated local churn must not force
  whole-directory refresh rejection forever.
- **Failure semantics:** a missing member is any non-success result
  (unavailable, timeout, cancelled, unverified root). The no-success case is
  narrow: every configured member is missing, none has a retained
  observation of the directory, and the local namespace contributes no
  names. It returns an
  explicit unavailable error, never success-empty. Otherwise (ruled
  2026-11-17, `oq-listing-cold-partial-result`) a listing returns the
  available union of successful members, retained stale observations of
  missing members and local names, even when empty:
  empty A plus unavailable B with nothing local is a successful empty
  readdir. The snapshot stays internally partial, B's status stays missing
  (its failure class) rather than empty, and it is never complete for B. B
  is retried on its own bounded backoff, independent of the serving TTL. A
  remote member's observation that would remove retained names is accepted
  only after a root verification made after it was read. Each
  missing member is logged at warning level (member ID, failure class,
  directory), rate limited per member, on cold and warm listings alike. The
  foreground wait for members is bounded, also for an expired directory
  with retained observations; until the numeric ruling it uses the interim
  reference values named in the spec. No public partial flag or notification transport is
  implied; a client may present the result as complete (risk accepted by
  the ruling). A failed member cannot veto collection or publication of healthy
  member observations. Retained stale
  names may fail at open; listing is not proof of readable bytes. Deadline and
  cancellation do not allow abandoned workers to publish later. If transport
  cancellation is unavailable, account for the still-running worker and keep
  concurrency bounded; never spawn replacements without admission.
- **Resource policy:** limits on queued/running refreshes, active directories,
  snapshot bytes/entries (including revisions pinned by open directory
  handles) and backoff are mandatory before implementation
  acceptance; the cold-partial slice may use the spec's interim values
  (whole-request deadline, retry backoff, root recheck, per-member worker,
  listing cache limit) until the numeric ruling. Do not hold global/namespace locks over remote I/O or admission.
  Cache eviction cannot erase dirty files or alter the independent byte cache.
- **Intentionally unspecified:** numeric freshness/visibility policy pending
  the existing open question; order across member completions; content
  equality; persistence across restart. POSIX readdir cannot encode a stale
  badge; internal observability is not a claimed Nautilus indicator.
- **Compatibility:** v0 is a proposal. Breaking changes to observations or
  revision semantics require coordinated edits of both consumers; no hidden
  dependence on unmerged PR171 freshness structures.
- **Boundary tests (planned, UNWRITTEN):** empty A + populated B; failed A
  retaining stale evidence; empty A + failed/slow B without cache (success
  empty, B logged, status partial, not fresh, B's later empty or populated
  result replaces only B); the same with a local dirty name (listed); partial
  page failure; all-failed/no-cache (error) and all-failed with local names
  (listed); all-failed with retained observations (stale names listed);
  all members cancelled/no-cache (error, not success-empty); member root
  unmounted or replaced by an empty directory (unavailable, retained names
  kept, warning logged) versus ENOENT subdirectory on a verified root
  (successful empty); single-member failure; continuous local creates while a remote add
  still publishes; event identity after delete/recreate or contributor loss;
  older remote-change event after newer local mutation and duplicate events; stale
  response after local rename/unlink; slow A while B publishes; cancelled old
  refresh; capacity saturation; stable offsets under background publication;
  cache eviction and file/directory/type conflicts.
- **Interpretation register:** [spec](../docs/specs/live-union-listings.md),
  `oq-listing-stale-serve-dir-mtime`, `oq-listing-cold-partial-result`.
