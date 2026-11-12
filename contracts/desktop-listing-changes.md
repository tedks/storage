# Contract: desktop listing changes v0 (proposed)

- **Owner:** Storage desktop integration owner. **Consumers:** GIO/Nautilus
  monitor and the filesystem's metadata/content invalidation path.
- **Inputs:** committed previous/new union revisions and local mutation
  observations; per-entry incarnation token and revision from the snapshot
  contract, scoped to the mount generation, not bare reusable filenames or an
  inferred stable remote inode.
- **Outputs:** detected added/removed/metadata-changed observations and a
  capability result for the actual client URI/versions. Notification must
  follow snapshot commit: a consumer reacting immediately can enumerate the
  corresponding or newer revision. One member's deletion does not mean union
  deletion when another member/local state still contributes the name.
- **Invariants:** snapshots/metadata and content freshness are separate.
  Refresh never overwrites local dirty bytes, replaces an open writer's inode,
  advances sync baseline, or equates metadata equality with byte equality.
  Type/identity replacement is not merely attribute change. Revision checks
  prevent an old event from invalidating newer local state. Notifications may
  coalesce but must preserve the need to reconcile the latest state.
- **Local writes:** use normal filesystem operations and kernel monitoring;
  propagate current local size/mtime and a final update when writers finish.
  Never infer remote durability or CLEAN from close. Preserve existing open
  descriptors across rename/replacement under their filesystem contract.
- **Remote changes:** watcher hints and bounded metadata reconciliation detect
  changes; the safe content path decides refetch/conflict, not this adapter.
  No silent freshness promise for same-metadata remote rewrites without a
  stronger signal. Do not download contents to generate a directory event.
- **Failure semantics:** unsupported notification transport is explicit, never
  reported as live updates working. Overflow, dropped connection and sequence
  gaps require bounded resnapshot/reconciliation, not speculative deletions.
  Notification callbacks run on separate delivery work, never on a FUSE
  request-handler thread, outside namespace/path locks, and may not recurse
  into a blocking refresh. Unmount cancels subscription work and prevents
  late delivery to a new mount generation.
- **Resource policy:** cap subscribers/active directories, queued events and
  coalescing latency. Overflow may collapse events into a rescan requirement
  only if that requirement can actually reach this client; otherwise mark
  degraded transport. Never keep an unbounded per-write event log.
- **Intentionally unspecified:** a transport before capability proof; UI
  repaint cadence, thumbnails/content sniffing and client-specific badges.
  A GIO backend for another URI is not evidence that file:// monitoring works.
- **Compatibility:** no kernel/GLib patch, fake filesystem mutation or client
  URI switch is authorized by v0. Any such scope change requires a proposed
  amendment with evidence. Existing attr_timeout=0 stays in force.
- **Boundary tests (planned, UNWRITTEN):** independent producer remote add,
  remove, overwrite, truncate, chmod and atomic rename, with an already-open
  GIO monitor and Nautilus window; no F5/focus workaround. Slow local copy plus
  remote refresh must preserve bytes and DIRTY. Test old-event rejection,
  delete/recreate, overlapping writers, completion coalescing, queue overflow,
  reconnect and unmount. Disabling delivery must fail the notification test.
- **Interpretation register:** [spec](../docs/specs/live-union-listings.md).
