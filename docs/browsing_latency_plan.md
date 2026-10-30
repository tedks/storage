# Browsing latency over SSH: request budget, policy and plan

Status: draft for the commander's review, 2026-10-29. Written by the CTO
from four read-only design audits of master at 033d8ac (listing and
attribute caching, SSH connections, the FUSE loop's threading, conflict
detection) and two measurements on a live mount (counts only). Section 6,
added 2026-10-30, records a field test of the landed wave.

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
local bridge and the true cost appears only at latency. (Section 6
measured otherwise on a LAN: about 5.5 ms per request at a 0.18 ms round
trip.) Pipelined range
reads are a follow-up once connections are pooled. OpenSSH connection
sharing for rsync and the watcher is filed as a follow-up.

## 5. Open questions raised by this spec
- D3, not-found from a fresh listing (new).
- oq-listing-cache-lifetime: D1 is option B of that question.
- oq-fuse-single-threaded: D6 is option B of that question, staged.
- The profile tuning revert and the deploy of #127 and #128.

## 6. Measured on 2026-10-30

A read-only field test of master at 39e4b79 (the whole wave: #128, #130 to
#138) on a desktop on the members' wired network (member round trip
0.18 ms), against a profile of three SSH members on one host with the
default listing settings (60 s fresh, 1 h stale), streaming reads with the
default 1M chunks (the 2026-10-29 tuning is reverted) and watchers off.

How it was measured. Request counts are member SFTP requests (listings,
stats, opens) from paramiko's debug log, which was on throughout and may
add to every time below. Small byte counts (the sniff) are the mount's own
socket counters. The large download volumes are estimated from paramiko
rekeys, one per about 512 MiB received on a connection; with three
member connections a total can be off by up to about 1.5 GiB, and it
includes the session's own reads (a few GiB at most). Times are medians unless a range or a maximum is
given. Directories viewed had 64, 69 and 87 entries (threads off) and 127
(threads on).

The table counts the requests a browse waits for (the foreground). A
listing also schedules background checksums, counted separately below.
Targets marked "derived" are not rows of section 2 but follow from it.

| Action | Target | Measured, threads off | Threads on |
|---|---|---|---|
| First view of a directory (readdir, stat every entry) | 1 listing per member | 1 listing per member, no per-entry request; 18 ms | same; 47 ms |
| Revisit inside the window | 0 | 1 member stat when the directory was reached by path and its parent never listed, 0 otherwise; 5 ms | 3 member stats in the by-path case; 24 ms |
| A name no listing has, from a fresh listing | 0 | 0; ENOENT in under 1 ms | 0 |
| Revisit past the window | served, one background refresh per directory | served in 17 ms; one background listing per member for each stale directory touched; no per-entry request | same; 30 ms |
| A 4-level path walk past the window | derived: 0 in the foreground | 0 in the foreground, 2.3 ms; background refreshes of the stale ancestors | not measured |
| Sniff: open, read 4 KiB, close of an uncached 3 to 19 MiB file | the kernel's request | 2 member read calls (see below) and 16.7 KiB received (SSH and SFTP framing included; the kernel's request size was not observed); nothing kept in the cache | same |
| SSH connections | one per member | one per member after mount, during browsing and during reads | same |
| A listing while another file is read sequentially | overlapped | 175 ms (idle: 4 to 38 ms); a stat of the mount root 158 ms (idle: 0.1 ms) | 25 ms; 0.3 to 0.9 ms |
| Ten minutes of mixed listings, sniffs and stats | derived: no errors, no listing waiting seconds | not run | not met: 0 errors, 2157 listings at a median of 5 ms, but 6 over 10 s, the longest 105 s |

The two columns used different directories and different files (the
threads-on directory was larger, and its sequential read was a 1.8 GiB
file against 57 and 108 MiB with threads off), so the differences
between them, including 1 against 3 stats on a by-path revisit, were not
isolated or investigated.

What sections 1 and 2 do not yet meet on this profile:

- Background checksums. A listing still schedules a checksum of every
  name with equal-size copies on several members (D4, not yet landed). A
  checksum runs `xxh128sum` on the member and, when that fails, downloads
  the whole file to a temporary file and hashes it locally. These members
  took the download path (presumably they have no `xxh128sum`; not
  checked on the host): about 5 GiB for a mount that only listed
  directories, 11 to 19 GiB per test session (most of it checksum
  downloads), still running 142 s and
  188 s after unmount, during which a remount fails as already active.
  ditz `field-listing-downloads-to-hash`, `field-daemon-outlives-unmount`.
- Listing stalls under load. With threads on, 6 of 2157 listings in ten
  minutes of mixed load took over 10 s, the longest 105 s. Checksum
  downloads were running at the time and hold member channels, so they are
  the suspected cause, but the cause was not isolated
  (`field-listing-stalls-under-load`). The threading target holds for a
  single sequential read; it is not shown to hold under this load.
- A directory reached by path whose parent was never listed answers its
  own stat from a member on every visit
  (`field-dir-by-path-stat-per-visit`).

Below expectations, outside section 2's targets (section 4.5):

- Uncached sequential reads ran at 4.4 to 5.8 MiB/s with threads off and
  7.8 MiB/s with threads on. Each 1 MiB chunk is one member read call (an
  open, about 32 serialized 32 KiB requests, a close), and 97% of the time
  is inside those calls: with threads off, about 5.5 ms per request, some 30 times the
  round trip, so the cost is per-request overhead on the member or in
  paramiko, not network latency. The same connection carried about
  90 MiB/s for a pipelined transfer (the checksum download). This changes
  section 4.5's premise that the cost appears only at latency: pipelined
  range reads would help on a LAN too. Connections are now pooled (#132),
  so that work is unblocked (`field-seq-read-serialized`).

Also seen: a desktop session probes a new mount within a second (stats and
a root listing), adding member requests before any user action.
