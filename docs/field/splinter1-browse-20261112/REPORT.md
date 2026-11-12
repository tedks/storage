# splinter1 browse latency and missing-entry field report (2026-11-12)

Lane: field-browse (STORAGE-SPLINTER1-BROWSE-20261112). Base 81793ac.
Aggregated and anonymized: directories are labelled by subtree and
selection order; no file names or private paths. Raw per-name evidence
stays in the lane's private scratch.

## What was running

- Mount PID 357705 on splinter1, started 13:55:17 local, running the
  flake-built package whose `storage/*.py` are sha256-identical to
  81793ac. It runs in an interactive terminal (no tmux), so its log was
  not readable.
- Profile: three SSH members (raid, chunk1, chunk0) on one drynwyn host by
  LAN IP; `workers = 2`; `fuse_threads = true`; streaming reads, 1 MiB
  chunks, 8 sessions; cache 50G. `enable_watchers` and `readdir_ttl` are
  unset.
- Host: about 2 GiB available RAM, 1.8 GiB swap in use, load 1 to 3. The
  wired NIC is down; traffic goes over Wi-Fi.

### Thread and channel model (three separate things)

1. **FUSE request threads**: `fuse_threads = true` runs libfuse's
   multithreaded loop. It starts request threads on demand (27 threads
   observed), so this part is already dynamic.
2. **Sync workers**: fixed at 2 (`workers = 2`). 81793ac has no adaptive
   sync workers, no foreground-priority scheduling and no
   foreground-channel reservation feature. `grep -ri adaptive storage/`
   is empty, and the adaptive-workers and foreground-channel-reserve work
   is not in 81793ac.
3. **SSH channels**: one paramiko transport per member (three TCP
   connections to the same host). Each has 4 SFTP channels, 1 of them
   reserved for interactive requests, and 2 exec channels
   (`ssh_pool.MemberConnection` defaults). Every member also gets a
   single-thread listing executor.

## Method

- `scripts/field/browse_probe.py` is fed over ssh stdin, so nothing is
  written on splinter1. It ran serially and took 37 s.
- It selected 9 directories under Videos and 8 under Music on chunk0 (via
  the GVFS POSIX bridge), up to depth 2:
  - each subtree root;
  - the 4 most recently modified children, from a spread sample of at
    most 200 entries;
  - the 3 densest of 8 sampled children;
  - a most-recent descendant chain.
- Three runs per directory, alternating which side goes first. Run 1
  starts with GIO on Storage.
- Operations on each side:
  - `gio list -n -h` with listing-level attributes (`standard::name,type,size,is-hidden,is-symlink,fast-content-type`,
    `time::modified`, `unix::mode`, `access::can-read`). Storage is
    queried through `file://`, chunk0 through `sftp://` (what a GUI uses).
  - POSIX `os.listdir`.
  - `os.lstat` of the same common names, at most 200.
- **"Run 1" means first observed in this probe, not proven cold.** The
  mount had been up about 18 minutes and the subtree roots had been
  listed earlier. Selection warmed chunk0's GVFS side before run 1.
- `scripts/field/cold_phase_probe.py` split one visit into lookup,
  readdir, lstat and access for 2 more untouched Music directories.
- Metadata only. No content-read throughput was measured.

## Results (milliseconds)

| dir | entries S/M | GIO S run1 | GIO M run1 | GIO S warm med | GIO M warm med | listdir S run1 / warm | listdir M warm | lstat S warm (n) | lstat M warm |
|---|---|---|---|---|---|---|---|---|---|
| Videos#0(depth 0) | 12/12 | 84 | 42 | 25 | 28 | 2.5 / 1.9 | 15 | 5.2 (12) | 2.8 |
| Videos#1(depth 1) | 179/176 | 2115 | 77 | 223 | 57 | 4.7 / 5.8 | 51 | 94 (176) | 25 |
| Videos#2(depth 1) | 36/33 | 258 | 44 | 41 | 33 | 3.0 / 2.6 | 22 | 12 (33) | 0.2 |
| Videos#3(depth 1) | 41/40 | 94 | 48 | 62 | 45 | 3.7 / 3.3 | 44 | 21 (40) | 2.8 |
| Videos#4(depth 1) | 153/153 | 2007 | 68 | 166 | 87 | 11 / 11 | 164 | 115 (153) | 2.2 |
| Videos#5(depth 1) | 12/12 | 177 | 38 | 25 | 42 | 2.8 / 2.2 | 23 | 5.6 (12) | 0.2 |
| Videos#6(depth 1) | 7/6 | 64 | 35 | 17 | 23 | 1.8 / 1.8 | 15 | 3.3 (6) | 2.1 |
| Videos#7(depth 1) | 2/2 | 36 | 25 | 14 | 21 | 2.1 / 1.3 | 12 | 0.9 (2) | 6.8 |
| Videos#8(depth 2) | 5/5 | 66 | 23 | 20 | 27 | 1.1 / 1.6 | 20 | 2.9 (5) | 0.1 |
| Music#0(depth 0) | 700/700 | 1230 | 195 | 930 | 194 | 32 / 29 | 182 | 159 (200) | 862 |
| Music#1(depth 1) | 1/1 | 66 | 38 | 10 | 17 | 2.3 / 1.1 | 9.1 | 0.6 (1) | 0.0 |
| Music#2(depth 1) | 23/23 | 806 | 352 | 50 | 42 | 2.9 / 3.0 | 20 | 14 (23) | 16 |
| Music#3(depth 1) | 2/2 | 748 | 38 | 13 | 20 | 2.1 / 1.8 | 13 | 1.2 (2) | 0.1 |
| Music#4(depth 1) | 4/4 | 280 | 39 | 24 | 33 | 2.3 / 2.3 | 12 | 2.3 (4) | 0.1 |
| Music#5(depth 1) | 15/15 | 142 | 40 | 16 | 18 | 2.7 / 1.7 | 12 | 7.1 (15) | 0.2 |
| Music#6(depth 1) | 6/6 | 74 | 39 | 20 | 21 | 2.3 / 1.7 | 12 | 2.8 (6) | 0.1 |
| Music#7(depth 1) | 3/3 | 346 | 35 | 20 | 31 | 3.6 / 1.6 | 12 | 1.1 (3) | 0.1 |

Totals over 17 dirs, ms: GIO Storage run1 8594, GIO member run1 1176; GIO Storage run3 1701, GIO member run3 793
absent total: 0 conflict total: 0 hidden: 0 dirs with storage-only names: 4

Run 2 and 3 medians are "warm". S = Storage, M = chunk0 over GVFS SFTP.

### Missing entries

No repro. Every name chunk0 listed was in Storage's listing, in all 17
directories, all 3 runs, by both GIO and POSIX: 0 absent, 0 conflict
splits (`name.remoteN`), 0 hidden internal names. Storage listed 1 to 3
extra names in 4 directories; that is expected, since Storage shows the
union of all three members. Scope searched: the 17 directories above
plus 2 more for phase timing, all at most depth 2. Ted's example path was
not available. This does not show the problem is fixed, only that it did
not reproduce in this scope. No remount was done.

## Diagnosis

### 1. Cold directory listing costs 0.1 to 2 s, independent of size

- Phase split on untouched directories:
  - 1 entry: lookup 3.6 ms, **readdir 1667 ms**, lstat 3.7 ms.
  - 2 entries: lookup 1.6 ms, readdir 72 ms, lstat 5.3 ms.
- chunk0 itself: readdir 11 to 13 ms.
- In run 1, GIO on Storage took 0.75 s for a 2-entry directory and 2.1 s
  for 179 entries, against 38 to 77 ms over GIO SFTP to chunk0.
- From source (`NamespaceService._list_members`), a foreground listing:
  - lists the cache directory and all three members concurrently;
  - waits for every member with no deadline (only background refreshes
    use the 60 s member timeout);
  - then merges the results and queues directory checksum tasks.
- So a cold listing costs as much as the slowest of raid, chunk1 and
  chunk0, plus the wait for that member's single-thread listing executor
  and an SFTP channel on its transport.
- chunk0 alone answers in about 10 to 50 ms. The excess is therefore in
  raid or chunk1 (possibly spun-down disks, or disks busy with server-side
  hashing that the checksum queue starts), or in channel and executor
  queueing behind streaming, sync and checksum work on the same
  transports.
- This lane was not authorized to inspect raid or chunk1 and could not
  read the mount's log, so it cannot say which. **This attribution is
  unverified.**

### 2. Warm GIO on Storage pays one FUSE round trip per entry, twice

- On the 700-entry Music root, warm GIO took 0.87 to 1.23 s on Storage
  against 0.19 s on chunk0.
- Under strace, GIO on `file://` issues one `statx` and one `access`
  per entry: 700 and 703 calls, 5 `getdents64`.
- `attr_timeout = 0` (by design: `fs.KERNEL_ATTR_TIMEOUT`, see
  `fuse_mount_options`), so every `statx` reaches the Python daemon.
  Warm `lstat` costs about 0.5 to 0.8 ms per entry there.
- GVFS SFTP gets the attributes in the READDIR replies and answers
  `access::can-read` from the mode bits, so it does one batched exchange.
- POSIX `listdir` on Storage is fast (2 to 31 ms warm): the listing
  snapshot is fine. The cost is per-entry getattr and access.

### 3. A Nautilus-like attribute set is far slower (one measurement, unplanned)

- One `gio list` of the Music root with `standard::*,time::*,unix::*,access::*`
  took **94 s** warm on Storage.
- `standard::*` includes `standard::content-type`. Over `file://`, GIO
  may sniff the first bytes of each regular file to get it. About 16% of
  the sampled entries in that directory are regular files.
- Each sniff is an open plus a small read of a file that is probably not
  cached. Through Storage that means an open-time member stat and a
  streaming session.
- The same request over `sftp://` guesses from the file name. It was not
  measured here.
- This run exceeded the lane's content-read bound; see the deviation in
  the event shard. It is the strongest lead for the GUI latency, but it is
  one sample. It has not been repeated, and which part dominates is not
  known.

### 4. Freshness: no directory-mtime check

- Watchers are off (default), so a listing is fresh for 60 s
  (`revalidate_after` default; `readdir_ttl` is unset).
- After that it is **served stale** while one background refresh runs,
  until `listing_max_stale` (3600 s); past that it is relisted in the
  foreground.
- Nothing compares a directory's mtime before serving a stale listing.
  The first visit to a directory more than 60 s after its last listing
  shows the old contents, and the next visit shows the refreshed ones.
- A file added on a member within the last hour can therefore be missing
  from one view of a directory last listed more than 60 s earlier. This
  fits "fresh files not shown" in a mount that is new but has already been
  browsed; it did not reproduce in this probe.
- A brand-new mount starts with empty listing state, so the very first
  listing of each directory is complete, unless a member's listing fails.
  A failed member falls back to that member's earlier names, which a
  fresh mount does not have.
- Nautilus also keeps its own directory model and gets no file-monitor
  events for changes made on a member, so an open window does not update
  by itself.

## Remedy

No restart was done. The running build is the accepted latest, and
nothing showed that a same-version restart would change any measurement.

Recommended follow-ups. These are code changes, so root decides release.

1. **Find out where the cold readdir time goes.** Log per-member listing
   duration and the wait for the executor and channel at debug level, or
   take a one-off read-only `ps` on drynwyn during a cold listing (for
   example, for checksum processes). Then decide between a foreground
   member deadline with fallback, deferring directory checksum scheduling
   while foreground listings wait, or a dedicated listing channel.
2. **Cut the per-entry cost of GIO and Nautilus.** First confirm, with a
   bounded and authorized test, whether content-type sniffing is what
   makes Nautilus slow on Storage. If it is, options are: make small
   opens of uncached files cheap, or check whether a FUSE subtype or
   mount option makes GIO treat the mount as remote and guess from file
   names. Separately, profile the warm per-call cost of getattr and
   access in the daemon.
3. **Freshness** is a product tradeoff and is filed as an open question:
   revalidating a stale listing against the members' directory mtime
   costs one stat per member per visit, in exchange for never serving a
   stale listing. Another option is a shorter `readdir_ttl` for this
   profile, or watchers.

## Limits

- One host, one time window; only a sample of each subtree was covered.
- Not true cold. The mount log was not visible. raid and chunk1 were not
  inspected. No content throughput was measured. Run counts are small,
  so the numbers are indicative, not a benchmark.
