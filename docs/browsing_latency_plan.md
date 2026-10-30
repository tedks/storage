# Browsing latency over SSH: request budget, policy and plan

Status: draft for the commander's review, 2026-10-29. Written by the CTO
from four read-only design audits of master at 033d8ac (listing and
attribute caching, SSH connections, the FUSE loop's threading, conflict
detection) and two measurements on a live mount (counts only).

## 1. Goal

A browse through the mount should cost what the same browse costs on a
native sftp mount, at the same network latency:

- one listing per member per directory per freshness window, and no
  per-entry round trip for a name the listing knew;
- nothing fetched until a file is opened, and a small read fetches what it
  reads;
- requests from different processes overlap instead of queueing behind
  one slow member call;
- one SSH connection per member, not one per thread.

The intended use cases are media archives and backups: large files, rarely
written, often several members on one host. Coherence can be lenient for
directories, the way it already is for file content since #127: a change
made behind the mount's back appears within the revalidation age, and a
change made through the mount appears at once.

## 2. Where the round trips go today

Per request, M members, watchers off, nothing cached. Every row runs on the
single FUSE thread and blocks every other process on the mount.

| Action | Member round trips today | Target |
|---|---|---|
| First view of a directory | 1 listing per member, in parallel; the kernel then asks for each entry's attributes, answered from the listing while it is under 5 s old | same, and the listing serves attributes for the whole window |
| Any revisit, or a stat, more than 1 s later | the kernel re-asks after its 1 s cache; past 5 s each entry costs 1 to M stats, and a readdir re-lists every member | 0 inside the window; one background refresh per directory after it |
| A path walk | 1 to M stats per component whose parent listing is older than 5 s | 0 inside the window |
| A name no listing has (file managers probe one per directory) | M stats, even with a fresh complete listing | 0 from a fresh complete listing |
| A sniff read of an uncached file | before #128: one chunk plus read-ahead, then discarded; files under a chunk kept whole | since #128: the kernel's request, no read-ahead until a chunk is consumed |
| A sequential read | one member request per 32 KiB inside each chunk, serialized on the thread's channel | unchanged; see section 4.5 |
| Any of the above while another process waits on a member | queued behind it | overlapped |
| A new thread's first request to a member | a full SSH handshake, plus name resolution | one connection per member for the mount's life |

The kernel keeps its own cache in front of all of this and is at the
libfuse defaults: attributes and entries expire after 1 s, failed lookups
are never cached, and the page cache is dropped at every open.

## 3. Decisions proposed

- **D1. Listing freshness follows content revalidation.** With watchers
  off, a listing is fresh for `revalidate_after` (60 s by default) and
  serves readdir and every entry's attributes for that long. Past it, the
  listing is still served and a refresh runs in the background, one per
  directory, up to a maximum staleness (1 h by default). Both per
  profile. With watchers on, the listing stays until invalidated and the 5 s
  attribute cap goes away. Writes through the mount keep invalidating at
  once; the symlink path, which does not today, is fixed.
- **D2. The kernel cache matches.** Attribute and entry timeouts equal the
  fresh window (1 s stays when watchers are on, since the mount cannot
  evict kernel entries), negative lookups cached for 10 s, and the
  mtime-checked page cache on so that a refetch drops stale pages. Per
  profile.
  As built (PR #133): entry timeouts equal the fresh window, but
  attributes are never kept (attr_timeout 0, no override). The kernel
  refreshes a file's size only from expired attributes, so any kept
  attributes let a stat taken just before an open that refetches a grown
  file stop the reads at the old size. Each stat reaches the mount and is
  answered from the cache file or the listing, or by a member stat when
  neither knows the name.
- **D3. Not-found answered from the listing.** A name absent from a fresh
  listing of every available member is ENOENT without a stat. Product
  consequence: a file added directly on a member and opened by its typed
  name is invisible for up to the window. Needs the commander's ruling
  (filed as an open question).
- **D4. Conflict detection leaves the browse path.** Copies of different
  size split without hashing. Copies of equal size show the plain name and
  are hashed in the background by one worker per member, lowest priority,
  with no download fallback on the view path; an unverified copy is
  marked as such in the checksums attribute. The hash on release of a
  conflict copy and the directory test for attribute-less entries move
  off the FUSE thread. Equal mtime is not identity: SSH uploads stamp the
  upload time, and the members that preserve mtime are exactly where a
  hidden divergence would sit. Persisting hash records so a remount does
  not rehash is a second step, gated on the same-second rewrite ruling.
- **D5. One SSH transport per member.** Channels are leased from a small
  pool per member (4, one reserved for interactive requests, at most 2
  exec channels), because paramiko's SFTP client cannot be shared between
  threads and sshd allows about 10 channels per connection. Connects are
  single-flight, the name-resolution fallback runs once per connect, a
  refused channel shrinks the pool instead of marking the member down,
  metadata operations use the short timeout and transfers the long one.
  Members on one host keep separate transports so one stall does not
  freeze the others. Mounting several members on the same host prints a
  warning.
- **D6. The FUSE loop goes multithreaded**, landed as a stack behind a
  mount option that defaults off, then on after a field test. Locking,
  outer to inner: a namespace read-write lock exclusive only for directory
  rename and rmdir; a per-path lock held across open, create, unlink,
  truncate, utimens and the release upload, with file rename taking both
  paths in order and eviction only try-locking; the cache lock never held
  across member I/O, with fetches landing in a temp file and renamed into
  place; existing leaf locks unchanged. SQLite is already thread-safe. A
  readers-writer scheme at the FUSE boundary was rejected because read-only
  opens mutate state. Concurrency is bounded by the connection pool, since
  libfuse 2 has no thread cap.
- **D7. Reads fetch what they read.** Done in #128. Defaults unchanged;
  the profile tuning applied on 2026-10-29 (256K chunks, 512K bound) is
  reverted once #128 is deployed, since 1M chunks make fewer member
  requests for sequential reads.

## 4. Lanes, in landing order

Each lane is a Lead-tier mission with merge authority, council to fixpoint,
and the four gates on the exact head. Overlaps are serialized through
master by rebase.

### 4.1 Lane listing (D1, D2, D3; lands first)
- PR A (S): kernel options through one `fuse_mount_options()` helper used
  by the mount and by the mounted tests; per-profile fields; CLI echo.
- PR B (M): fresh window and background refresh in the namespace, the
  symlink invalidation fix, not-found from a fresh listing (behind D3's
  ruling), tests for write visibility through every write path.
- PR C (S): the same-host warning.
- Evidence: a remote-gate case that counts member requests for a browse
  (list, stat every entry, re-list after the window) before and after.

### 4.2 Lane connections (D5; independent, overlaps nothing above)
- PR (L): per-member transport and leased pool, retry loops and exec
  paths rewritten, rsync's SFTP use moved onto it, the socket-timeout
  override dropped. Unit tests on fakes; remote-gate cases that count
  connections under parallel list, read and write, run a MaxSessions=3
  sshd variant, and kill the session to check reconnect.
- Prerequisite probe: whether libfuse worker threads keep Python
  thread-local state across callbacks (the current per-thread client
  design may reconnect per request under threads).

### 4.3 Lane conflicts (D4; starts after lane listing's PR B lands, same file)
- PR 1 (M): size-first split, per-member single hash worker, no download
  fallback for view hashes, release hash and directory test off the FUSE
  thread, unverified marker. Four existing tests change deliberately.
- PR 2 (M/L, later): persisted records, slow re-verify, an explicit verify
  command, sync recording what it uploads. Waits for the same-second ruling.

### 4.4 Lane threading (D6; stage 1 now, the rest after lane connections)
1. (M) Fetch into a temp file and rename; no cache lock across transfers;
   positional writes. Groundwork, no behaviour change.
2. (L) Path locks, namespace lock, eviction try-lock, the watcher taking
   the path lock; fix finalize-over-writer and the two streaming races
   filed earlier. Behaviour unchanged with threads off; deterministic
   unit races added.
3. (S/M) The `fuse_threads` option, a shared test mount helper, both modes
   in the mounted and remote gates, a remote-gate case where a slow read
   on one file must not delay a listing of another.
4. (S) Default on, after the commander's field test.

### 4.5 Not in scope now
Paramiko issues one 32 KiB request at a time per channel, so a chunk is
many round trips; the sequential-read measurement in #128 was flat on a
local bridge and the true cost appears only at latency. Pipelined range
reads are a follow-up once connections are pooled. OpenSSH connection
sharing for rsync and the watcher is filed as a follow-up.

## 5. Open questions raised by this spec
- D3, not-found from a fresh listing (new).
- oq-listing-cache-lifetime: D1 is option B of that question.
- oq-fuse-single-threaded: D6 is option B of that question, staged.
- The profile tuning revert and the deploy of #127 and #128.
