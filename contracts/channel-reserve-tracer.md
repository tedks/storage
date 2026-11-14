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
  `StorageFS.__call__` error path rather than during later iteration. Background
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

At `7a5c3db`, the synthetic timeout test observed `ENOENT` for an existing
file on a healthy member after bulk capacity expired. Ted subsequently ruled
that expiry should be opt-in per mount, default off, and report `EAGAIN`.
The prior observation is evidence of the defect, not the desired behavior.

## Opt-in capacity expiry implementation boundary

`bulk_capacity_wait_timeout` is absent by default; explicit zero also means
off, and a positive finite number of seconds enables expiry. CLI and TOML
profile values resolve before mountpoint/cache creation, then
`create_storage_members` passes the value only to SSH backends. The SSH pool
uses it only while a bulk SFTP request waits for channel capacity. Its
existing connection, SFTP operation and exec deadlines remain finite and
separate. An available channel wakes a queued waiter; closing the pool wakes
all waiters. A capacity-only expiry raises a distinct member exception,
which cache fetch must preserve through its current catch and `StorageFS.open`
maps to `EAGAIN`. Missing files, down members, failed operations and other
fetch failures keep their current error paths. No new admission class,
worker count or pool cap is introduced.

This is the smallest needed addition: without a distinct capacity result,
`CacheManager.cache_file` turns the failed fetch into false and `open` reports
`ENOENT` for an existing file. With no capacity expiry, queue drain must
eventually release a lease; pool close is the explicit cancellation path.
The nonmount proof will exercise it. Mounted unmount behavior is reserved
for the later mounted gate and must not be inferred from synthetic tests.

### Unmount boundary found during implementation

The nonmount test proves that `MemberConnection.close()` wakes a default
capacity waiter, including one reached through `StorageFS.destroy()`. It does
not prove that a mounted unmount reaches `destroy()`: `mount_storage` documents
that libfuse joins active callbacks first. If all bulk holders continue to
hold leases, a queued cold-open callback has no capacity deadline by default
and can delay that join indefinitely. SSH channel inactivity timeouts do not
bound an otherwise progressing whole-file transfer. This is open as
`pr175-default-capacity-unmount-cancel`. The approved follow-up adds a
thread-local probe only inside fusepy's native callback wrapper. The pinned
libfuse2 exports `fuse_interrupted()` and `fuse_exited()`; a capacity waiter
polls those while the callback is alive, returns `EINTR` on cancellation, and
does not turn an ordinary capacity wait into an elapsed timeout. A copied
background context and a direct `StorageFS` call do not carry that probe.
No native context pointer is kept after a callback. The source boundary is
[libfuse 2.9.9 fuse.h](https://github.com/libfuse/libfuse/blob/fuse-2.9.9/include/fuse.h),
[fuse_loop_mt.c](https://github.com/libfuse/libfuse/blob/fuse-2.9.9/lib/fuse_loop_mt.c)
and the pinned fusepy 3.0.1 wrapper.

The mounted explicit-`fuse_exit()` positive and disabled-probe control pass on
`55065aa`: the control holds the callback until lease release, while the
positive reaches `destroy()` with all three holders gated. A later scratch
test uses an isolated FUSE daemon and a separate caller. A fatal signal to
that caller yields a real `fuse_interrupted()` observation and releases its
callback while the holders remain gated. SIGTERM to the exact owned daemon
yields a real `fuse_exited()` observation; the queued callback exits before
`destroy()`, also while holders remain gated. These are distinct cancellation
causes, not ordinary capacity drain. The final committed-head module run is
still required to bind the test source to a pushed revision.

The earlier unmount fixture tried `fusermount -u` and then `-uz` after a
failure; it did not retain the individual return codes. That observation
cannot be called a successful plain unmount. On Linux, `MNT_DETACH` removes
the busy mount from the namespace immediately but completes unmount only when
references drain ([umount(2)](https://man7.org/linux/man-pages/man2/umount.2.html)).
A lazy detach is therefore not evidence of session exit or request
interruption. The revised fixture records each exact attempt, verifies the
owned mount and processes are gone, and retains scratch evidence if cleanup
fails. No universal unmount completion guarantee or release acceptance follows.

This does not enable FUSE threads or cap libfuse native threads. A single
SFTP channel has no reserve; other interactive work can fill it; reentrant
leases are not reclassified; member timeouts still bound waits. A cold open
now queues behind sync and other bulk leases. Local cached
reads usually need no SSH channel. Rsync and local transfers have no new SFTP
capacity. Background worker growth is separate (#167) and does not fulfill
Ted's broader foreground request. PR #170's build-merge incident and all
integration gates remain open. This draft is not accepted for release.
