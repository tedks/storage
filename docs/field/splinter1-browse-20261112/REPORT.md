# splinter1 browse latency and missing-entry field report (2026-11-12)

Lane: field-browse (STORAGE-SPLINTER1-BROWSE-20261112). Base 81793ac.
Corrected at root review of 93ed39f (one feedback round); see Corrections.
Aggregated and anonymized: directories are labelled by subtree and
selection order; no file names or private paths. Raw per-name evidence
and the historical probe scripts are kept private with a sha256 manifest.

## What was running

- Mount PID 357705 on splinter1, started 13:55:17 local, running the
  flake-built package whose `storage/*.py` are sha256-identical to
  81793ac. It runs in an interactive terminal (no tmux); its log was not
  readable.
- Profile: three SSH members (raid, chunk1, chunk0) on one drynwyn host
  by LAN IP; `workers = 2`; `fuse_threads = true`; streaming reads,
  1 MiB chunks, 8 sessions; cache 50G. `enable_watchers` and
  `readdir_ttl` are unset.
- Host: about 2 GiB available RAM, 1.8 GiB swap in use, load 1 to 3. The
  wired NIC is down; traffic goes over Wi-Fi.

### Thread and channel model (three separate things)

1. **FUSE request threads**: `fuse_threads = true` runs libfuse's
   multithreaded loop, which starts request threads on demand (27 threads
   observed).
2. **Sync workers**: fixed at 2. 81793ac has no adaptive sync-worker
   growth; that and foreground request admission are in unaccepted #171.
3. **SSH channels**: one paramiko transport per member (three TCP
   connections to one host). Each has 4 SFTP channels and 2 exec
   channels; one listing executor thread runs per member.
   - **Configured:** 81793ac keeps 1 SFTP channel per member reserved for
     interactive requests (`ssh_pool.MemberConnection`,
     `interactive_reserve=1`).
   - **Effective, for ordinary foreground readdir: not interactive.**
     - `StorageFS.__call__` (`storage/fs.py:126-130`) sets the interactive
       context only around the call that returns `StorageFS.readdir`'s
       generator (`storage/fs.py:2076-2079`).
     - The listing runs when fusepy iterates that generator, after the
       context has ended.
     - `_submit_member_listings` (`storage/namespace.py:1105-1108`) then
       copies that now non-interactive context into the member's listing
       executor.
     - So a foreground listing competes for channels as background work
       and does not use the reserve.
   - **Cold whole-file copies:** they keep the interactive class of the
     FUSE request that starts them, so they can hold interactive leases.
   - Unaccepted #175 fixes both: it evaluates readdir eagerly inside the
     request, and it classifies cold full-file copies as bulk.
   - Whether either effect contributed to the field timings is
     unverified.

## Method

The historical helpers were removed from this branch after review. They
did not enforce the bounds they advertised:

- the 10000-entry cap was checked only after a full enumeration, and only
  skipped lstat;
- explicit directory lists bypassed the depth, count and symlink checks;
- the phase probe's `os.access` follows symlinks;
- the summarizer would have shown a missing operation as 0 ms.

None of these affected this data set: no run exceeded 700 names, and no
operation errored or was missing. The scripts and raw evidence are kept
privately, with hashes. What was run, as commands:

- **GIO listing**: `gio list -n -h -a standard::name,standard::type,standard::size,standard::is-hidden,standard::is-symlink,standard::fast-content-type,time::modified,unix::mode,access::can-read URI`.
  Storage is queried as `file://MOUNT/DIR`, chunk0 as
  `sftp://HOST/MEMBER_ROOT/DIR`.
- **POSIX**: `os.listdir(DIR)` on each tree, then `os.lstat` of the
  same common names (at most 200, an evenly spaced sample in sorted
  order).
- **Per-operation child processes**: each POSIX operation ran in its own
  `python3 -I -S -c` child, and each GIO listing ran `gio` directly as a
  subprocess; both had a 30 s timeout. The driver was fed over
  ssh stdin, so nothing was written on splinter1. The run was serial and
  took 37 s.
- **Selection**: on chunk0 via the GVFS POSIX bridge, from each subtree
  root:
  - the root itself;
  - the 4 most recently modified children, from a spread sample of at
    most 200 entries;
  - the 3 densest of 8 sampled children;
  - one most-recent descendant chain.

  This gave 9 directories under Videos and 8 under Music, at depth 2 or
  less.
- **Runs**: three per directory, alternating which side goes first; run 1
  starts with GIO on Storage.
- **Phase split**: for 2 Music directories not otherwise touched, one
  child each measured `lstat(dir)`, `listdir(dir)`, then `lstat` and
  `access(R_OK)` of each entry. Each directory was visited in the order
  Storage, chunk0, Storage.
- **Syscall count**: `strace -f -c` of the GIO listing above on the
  700-entry Music root of Storage.
- **"Run 1" is first observed in this probe, not proven cold.** The mount
  had been up about 18 minutes and the subtree roots had been listed
  before. Selection warmed chunk0's GVFS side before run 1. The same
  applies to the phase split's "first" visit: those directories were not
  touched by this lane before, but may have been by others.
- The bounded scripted probes were metadata only; the 94 s wide-attribute
  run in Diagnosis 3 was outside them. No content-read throughput was
  measured.

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

Run 2 and 3 medians are "warm" (later observations, not a proven steady state). S = Storage, M = chunk0 over GVFS SFTP.

### Missing entries

No repro. Every name chunk0 listed was in Storage's listing in all 17
directories, all 3 runs, by both GIO and POSIX: 102 paired checks, with 0
absent, 0 conflict splits (`name.remoteN`) and 0 hidden internal names.
Storage listed 1 to 3 extra names in 4 directories; that is expected,
since it shows the union of three members.

Scope searched: the 17 directories above plus 2 more for phase timing,
all at most depth 2. No example path from Ted was available. This does
not show that anything is fixed, only that it did not reproduce in this
scope. No remount was done.

## Diagnosis

### 1. A first-observed listing can take 0.1 to 2 s, and can be large even for small directories (not a controlled size comparison)

- Phase split, Storage first visit:
  - 1 entry: lookup 3.6 ms, **readdir 1667 ms**, lstat 3.7 ms.
  - 2 entries: lookup 1.6 ms, readdir 72 ms.
- The same directories on chunk0: readdir 11 to 13 ms.
- Storage second visit: readdir 3.4 ms.
- In run 1, GIO on Storage took up to 2.1 s, against at most 0.35 s on
  chunk0.
- The time is in readdir, and the next visit is fast. Beyond that, this
  lane did not isolate where it goes.
- From source, a foreground listing (`NamespaceService._list_members`):
  - lists the cache directory and all three members concurrently;
  - waits for every member with no deadline (only background refreshes use
    the 60 s member timeout);
  - then merges the results, queues directory checksum tasks and computes
    the conflict view.
- Hypotheses, none measured here:
  - member-side listing time on raid or chunk1;
  - waiting for a member's listing executor or an SFTP channel. In the
    deployed build a foreground listing is not classified interactive, and
    cold copies can hold interactive leases (see the channel model);
  - checksum scheduling, the merge, or Python and FUSE overhead.
- chunk0's GVFS answer time does not bound Storage's chunk0 listing,
  which goes over a different client and connection.

### 2. A later GIO listing on Storage costs more per entry than on chunk0

- On the 700-entry Music root, GIO took 0.87 to 1.23 s on Storage
  against 0.19 s on chunk0 (later runs).
- strace of the Storage listing: 700 `statx`, 703 `access` and 5
  `getdents64` calls.
- `attr_timeout` is 0 by design (`fs.KERNEL_ATTR_TIMEOUT`; see
  `fuse_mount_options`), so each `statx` reaches the daemon. Measured
  later-run `lstat` costs about 0.5 to 0.8 ms per entry there.
  `access` cost was not measured separately on the 700-entry root.
- POSIX `listdir` on Storage is fast in later runs (2 to 31 ms).
- **Qualified, not verified in this lane:** the SFTP protocol returns
  attributes with each READDIR reply, and GVFS's SFTP backend is
  expected to answer this attribute set from those replies, without a
  round trip per entry. Neither GVFS's source nor its traffic was
  checked here.

### 3. A wider, Nautilus-like attribute set took 94 s (unplanned, one sample)

- One `gio list` of the Music root on Storage with
  `standard::*,time::*,unix::*,access::*` took 94 s.
- It was run outside the harness. It exceeded both the 30 s per-operation
  bound and the content-prefix bound (at most one 64 KiB prefix per
  directory); see the deviation in the event shard.
- **What it read, wrote or cached is unknown.** A flat `write_bytes` on
  the mount process over 5 s afterwards does not bound the I/O during the
  94 s.
- **Qualified hypothesis:** `standard::*` includes
  `standard::content-type`, which GIO's local backend may compute by
  reading the start of each regular file; about 16% of the sampled
  entries there are regular files. Neither GLib's behaviour on this mount
  nor the number of opens was verified.
- The same attribute set was not run over `sftp://`, so there is no
  comparison.
- This is a lead for the reported Nautilus delay before a directory shows
  anything, together with section 1. It is not a diagnosis.

### 4. Freshness: a stale listing is served without checking the directory

- Watchers are off (default), so a listing is fresh for 60 s
  (`revalidate_after` default; `readdir_ttl` is unset).
- After that, an access serves the old listing and schedules one
  background refresh, until `listing_max_stale` (3600 s); past that the
  directory is relisted in the foreground.
- Nothing compares the members' directory mtime first. So a visit more
  than 60 s after the last listing can lack names added since.
- **The refresh is asynchronous.** The next visit sees new names only if
  the refresh has finished and published. It may still be running,
  waiting on a member, or have failed, so the next visit is not
  guaranteed to be current.
- A brand-new mount starts with empty listing state, so its first
  listing of a directory comes from the members. A member whose listing
  fails falls back to its earlier names, which a fresh mount does not
  have.
- Nautilus keeps its own directory model; whether it gets change events
  for this FUSE mount was not checked.

## Remedy

No restart: the running build is the accepted latest, and no measurement
showed that a same-version restart would change anything. The follow-ups
below are code or policy changes, so release disposition belongs to
root, and policy to Ted.

1. **Find where first-observed readdir time goes.** Per-member listing
   duration plus executor and channel wait, at debug level, under a
   separately authorized runtime budget.
2. **Measure the wide attribute set safely** (opens and bytes per
   listing, time to first batch, the same set over `sftp://`) before
   choosing a remedy for GIO and Nautilus.
3. **Freshness is an open product question** (ditz
   `oq-listing-stale-serve-dir-mtime`). The candidate options trade
   latency for fewer stale views; none guarantees a listing is never
   stale:
   - a directory-mtime check has a check-to-use race, coarse timestamp
     granularity on some filesystems, sees only direct entries, and
     cannot check a member that is failing;
   - a shorter fresh window keeps the same 3600 s stale window, so a
     stale first view remains;
   - the per-member stat latency such a check would add is unmeasured.

Tracked: ditz `field-gio-content-sniff-per-entry`,
`oq-listing-stale-serve-dir-mtime`, and comments on
`field-listing-stalls-under-load` and `field-listing-downloads-to-hash`.

## Corrections (root review of 93ed39f)

- Run 1 and the phase split's first visits are "first observed", not
  cold.
- The first-observed readdir excess is no longer attributed to raid,
  chunk1 or channel queueing; those are listed as unmeasured hypotheses.
- The channel model separates the configured interactive reserve from
  effective classification. In 81793ac, foreground readdir runs outside
  the interactive context, and cold copies keep it. #175 fixes both.
- The 94 s run exceeded both the 30 s bound and the prefix bound. Its
  I/O is unknown, and the "no bulk fetch" claim is withdrawn.
- The next visit after a stale serve is not guaranteed fresh, and no
  freshness option guarantees a listing is never stale.
- GIO, GVFS and SFTP implementation claims are qualified as unverified.
- The runnable helpers were removed and are preserved privately with
  hashes; their bound defects did not affect this data set.

## Limits

- One host, one time window; only a sample of each subtree was covered.
- Nothing was proven cold. The mount log was not visible, and raid and
  chunk1 were not inspected.
- No content throughput was measured. Run counts are small, so the
  numbers are indicative, not a benchmark.
