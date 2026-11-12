# Channel reserve tracer

Package: `STORAGE-CHANNEL-RESERVE-TRACER-20261112`. Original source base:
`81793ac27b70f82c231bf607b55e775d2b905974` (accepted release
baseline); current PR merge-base: published master `66c640aec005feccd0bd585f3355295c3bb772ba`.
This contract describes the independent draft PR #175, not PR #171 or its
later freshness and namespace machinery.

## Assumptions and need

An already threaded FUSE mount can dispatch concurrent callbacks. The existing
SSH pool has four SFTP channels and withholds one from bulk leases. Before this
change, a foreground cold full-file copy inherited `interactive_request` and
could use that reserve. `StorageFS.readdir` returned a generator, delaying its
concrete `NamespaceService.readdir` call until after FUSE dispatch cleared the
interactive context. Without this change, four cold copies can occupy every
channel and an unrelated member listing can wait for a copy or its timeout.

No new scheduler or pool is needed: the pool already distinguishes interactive
and bulk leases, and `NamespaceService.readdir` already returns a list.

## Narrow behavior

- `bulk_request` temporarily sets the existing request context to false and
  restores its exact prior token on success or exception. A nested interactive
  scope may still override it; a lease acquired before a scope change keeps its
  class until released.
- Only `CacheManager.fetch_into_cache`'s `backend.copy_to_local` call is bulk.
  Its temp path, no-replace link, accounting, return values and exceptions stay
  in their original order. Metadata and range reads remain interactive.
- `StorageFS.readdir` evaluates the concrete namespace list while its existing
  FUSE dispatch context is active. Listing errors now pass through the existing
  `StorageFS.__call__` error path before a generator is returned. Background
  listing context remains as it was.

This introduces no new lock acquisition. A copy may still wait for bulk
capacity while its existing FS path lock is held; no new opposite-order lock
edge is added. No byte freshness, mutation, namespace, or upload repair is
claimed here.

## Evidence boundary and limits

The test drives real `StorageFS.__call__`, inline one-member
`NamespaceService` listing, `SSHBackend` and `MemberConnection` with a fake
local transport. Three cold opens are gated at SFTP `get`; a fourth reaches
the pool's real bulk-capacity denial before a member listing uses the reserve.
It checks returned bytes, pool state, context reset and worker cleanup. A
control disabling only the copy's bulk context failed on `bulk_leased=0`
versus 3 before timeout. The corrected test passed in the forced, unfiltered
fast gate on the earlier clean pushed source head. The rebased head's forced
fast gate ran 40 targets with one unrelated, unchanged SSH mux test failing;
the CLI build passed. Exact commands and hashes are in PR #175's evidence
comment. This one-member test does not show namespace-executor
contention; the separate context test checks error restoration.

This does not enable FUSE threads or cap libfuse native threads. A single
SFTP channel has no reserve; other interactive work can fill it; reentrant
leases are not reclassified; member timeouts still bound waits. A cold open
now queues behind sync and other bulk leases for up to `bulk_wait`; when that
wait expires, the existing fetch-failure path can fail the open even though
the member remains connected. Local cached
reads usually need no SSH channel. Rsync and local transfers have no new SFTP
capacity. Background worker growth is separate (#167) and does not fulfill
Ted's broader foreground request. PR #170's build-merge incident and all
integration gates remain open. This draft is not accepted for release.
