# Durable member-rename repair records

Status: design for PR #182 under CTO ruling RRS-CTO-20261118C. That ruling
lifts the no-xattr restriction for this metadata only: durable member-rename
repair records. It is not a general xattr migration, not a journal, and
unrelated to future duplication configuration.

## Purpose

When a storage member misses a rename made through the mount, PR #182 never
replays the move on that member later. Nothing on the member could prove
that the object at the old name is still the one renamed, or that the new
name is free. Instead the missed move becomes a repair. While a repair is
open, three protections apply:

- the member is fenced from renames, mkdir, rmdir, file and link uploads,
  and writes and deletes through a conflict view (refused with `EBUSY`
  when the view is opened for writing or unlinked), at names
  overlapping either path;
- cached files under the new name are pinned against eviction;
- the repair is listed for someone to settle by hand.

The SQLite index is rebuildable acceleration state. The repo rule is that it
must not be the only record of anything that cannot be recovered from xattrs
or a member reprobe, and a missed move cannot be recovered from a reprobe. So
the record must survive in the cache, on a carrier that exists even when
nothing is cached at either name and the member is offline. A fresh or
rebuilt index must lose none of the protections.

## Carrier and format

The cache root holds one cache-internal directory, `.storage.repairs`. It is
created on first use with mode 0700. Each repair record is one empty regular
file in it, named by the record id (32 lowercase hex characters, a random
UUID). The record itself is the file's xattr `user.storage.member_repair`.

The cache root's own inode is not used as the carrier. On ext4 without the
`ea_inode` feature, all xattrs of one inode share a single block of about
4 KiB, so roughly ten open repairs would exhaust it, and every later rename
would then have to fail. With one inode per record, the cap on the number of
records is the filesystem's inode count.

The directory is hidden from listings and skipped by every cache scan
(`is_internal_cache_artifact`, so rebuild-index never indexes a carrier). It
is refused through the mount: any path whose canonical first component is
`.storage.repairs` gets ENOENT. That also protects it from other users of an
`allow_other` mount. Anyone who can write the cache directory directly can
still damage the records, as with every cache-internal artifact.

The value is UTF-8 JSON with these fields:

- `v`: 1;
- `id`: the record id, equal to the file name;
- `old` and `new`: the canonical virtual paths;
- `members`: a sorted list of stable member IDs (the backend names, never
  display labels);
- `state`: `intent` or `open`;
- `reason`: at most 256 characters;
- `recorded`: Unix seconds.

A record larger than 3584 bytes is refused before it is written, with
ENAMETOOLONG. That keeps the bound independent of the filesystem and inside
ext4's per-inode xattr block. The cost is that a rename whose two paths
together exceed about 3.3 KiB of encoded JSON fails, on any mount with at
least one member. The record escapes non-ASCII characters (6 bytes each, 12
outside the Basic Multilingual Plane), so for such names the bound in
characters is several times lower. Resolving rewrites the state as
`resolved`, 4 bytes longer than `open`, which stays well under the 4 KiB
block. This is a stated bound, not widened scope. A filesystem that refuses `user.*`
xattrs (ENOTSUP) fails the rename the same way. The cache already depends on
`user.storage.metadata`.

## Crash ordering

`NamespaceService.rename` runs four steps. Every effect it has comes after a
durable record of it.

1. **Intent.** This step runs before the local move and before any member
   call. It computes the members the rename will be sent to (every member
   for a directory, or the location members of the file, or the member named
   by a conflict suffix). It writes an `intent` record naming all of them:
   - create `<id>.new` with `O_EXCL`, set the xattr, fsync the file;
   - rename the file to `<id>`, fsync the directory.

   If any of this fails, the rename returns that errno and nothing has
   changed: no cache move, no index change, no member call. A rename with no
   target members writes no record.
2. **Local move.** The cache rename, the index rewrite and the metadata
   xattrs, as before. If the cache rename itself fails with an error
   meaning it refused (ENOENT, ENOTEMPTY, EXDEV and the like), nothing
   moved, and the intent is dropped. Any other failure or interruption
   from the rename on, an I/O error included, keeps the intent. Then the cache tree's part of the move is made
   durable:
   - every directory from the cache root down to the old and the new
     parent is fsynced. That includes directories this move created
     (`mkdir` of the new path's parents) and ones made earlier and never
     synced.

   So no power loss can undo the cache move after a member has taken it
   and its intent has been dropped. The cache tree is the authority. The
   index commit (`synchronous=NORMAL`) and the metadata xattrs are not
   fsynced here, and can be rebuilt. If an fsync fails, no member is
   called and the intent is kept. The rename still returns the error,
   even though the local move has already happened.
3. **Member renames.** These skip any member that already had an open
   repair overlapping either path. Those members are added to the failed
   set, and none of them is called.
4. **Settle.**
   - If some members missed the move, they first get a new `open` record
     under a new id, written durably like any record, with the reasons.
   - Then the intent's carrier is unlinked and the directory fsynced.

   The paths come from the rename itself, not from the registry, which a
   concurrent reload may have changed.

   If settling fails, the `intent` record stays. It names every target
   member, so it over-fences, but nothing is lost. The rename still returns
   success, because its evidence is durable.

   The rename holds an exclusive `flock` on its intent from `begin` until
   settle has finished. The lock is taken on the temp file, before the
   carrier has its final name. Resolution takes the same lock with a
   bounded wait, so an intent can never be resolved while its rename is
   running: the resolve fails as busy and changes nothing. That makes the
   miss record durable before the intent can go. When the process dies,
   its locks go with it, so an intent left by a crash can be resolved.

   If the rename fails after its local move, the intent is kept and its
   lock released.

   Because the miss always lands under a new id, a late resolve of the
   intent id names nothing and fails. Settle on an intent it does not hold
   (another instance's) waits at most 5 seconds for the lock, since it
   runs under the rename's path and namespace locks, and then leaves the
   intent in place, which over-fences.

What a crash at each point leaves:

- **Before step 1 completes:** at most a `<id>.new` file, which loading
  deletes. No effect had happened yet.
- **Between steps 1 and 4:** an `intent` record naming every target member.
  It is treated as open for all of them: whether the local move or any
  member call happened is unknown, and the record says so.
- **After step 4:** exactly the members that missed the move.

A queued `DirSyncTask` RENAME, from a direct caller, never runs either. The
worker writes an `open` record for its member, or for every member when the
task has no hint. It retries without limit while the write fails.

## The in-process registry

`MemberRepairs` (`storage/member_repairs.py`) is created by `CacheManager`
when it opens the cache. That happens before the sync workers, the startup
sweep, eviction or the FUSE loop can act, so the protections are in force
from the first operation, whatever the state of the index, including a fresh
or rebuilt one.

At load, it reads every carrier:

- a carrier that cannot be parsed, or whose name and id disagree, stops the
  mount with a message naming the file, because a fence nothing can read
  must not be silently dropped;
- a `*.new` file is removed.

Lookups (`backends_for(paths)` for the fences, `covers(path)` for the pin)
use the in-memory records. They reload when the directory's stat changes,
which is how a resolution made by the CLI reaches a live mount. Every write
by the mount is followed by a full reload, so the mount's own new stamp
never hides an external change made before or during that write. A change
a lookup misses (two changes within one timestamp tick) can only be a
removal by another process, so missing it keeps a protection longer and
never drops one.

Two locks are involved. One guards the in-memory records and is never held
across an fsync. The other orders this process's writers across their I/O,
and is taken under the rename's path and namespace locks. A writer then
takes a carrier's `flock` only for a bounded wait. Opening the cache with
more than 1000 open repairs logs a warning, since every lookup scans them
all.

Cost: a rename with member targets makes two fsyncs for the intent, one per
cache directory from the root down to each of the two parents (shared
levels once), and one for the settle, or three when a member missed the
move. It also makes two reloads of
the carrier directory, each one listdir and one getxattr per open repair.
A rename with no member target pays nothing.

Members are identified by their stable member IDs. A member whose ID
changes in the configuration no longer matches its repairs: settle those
repairs before renaming a member.

There is no SQLite projection. The table added earlier in this PR is
removed, so no index row and no numeric id stands for a repair.

## Resolution

`storage debug member-repairs -c CACHE` lists the carriers. `--resolve ID`
works in two steps, under an exclusive `flock` on the carrier:

1. It rewrites the record with `state` `resolved` and fsyncs it. Readers
   still enforce a carrier that exists, whatever its state.
2. It unlinks the carrier and fsyncs the directory. The unlink is the only
   change readers act on, and it becomes visible only after the resolution
   is durable.

If a crash comes between the two steps, the carrier is left `resolved`. It
stays enforced until the cache's owner (the mount) next opens the cache.
The owner finishes the resolution under the same `flock`, fsyncing the
record first, and leaves a carrier it cannot lock in place. A resolve
that finds its carrier already finished by the owner still succeeds. A
mount sees a completed resolution at its next lookup.

The id is the carrier's random name, so it survives index loss. A stale or
mistyped id names no carrier and is an error. Resolving the same id twice is
an error the second time and changes nothing. No id taken from a rebuilt
index can name a different repair.

## Validation

All of these run on scratch Local members. The system cases are in
`tests/test_sync_races.py` (classes `TestRepairsOutliveTheIndex` and
`TestDirectoryRetries`), the unit cases in `tests/test_member_repairs.py`,
and the CLI cases in `tests/test_cli_debug.py`:

- **Offline member with no cached leaf.** A member holds the only bytes of
  `/d/f.txt`, the file is not cached, and the member is offline during
  `/d -> /e`. Then the database is discarded (and separately rebuilt with
  rebuild-index), and the cache is reopened. The repair must still be
  listed, and both endpoints fenced on that member: no rename, rmdir or
  upload at `/d` or `/e`.
- **Pinned-cache variant.** The same, with the file cached at the new name:
  it is not evicted after the reopen.
- **Replay-safe resolution.** Resolving twice, a mistyped id, and an id
  after a rebuild.
- **Injected persistence failures:**
  - the intent cannot be written (ENOSPC, ENOTSUP, or the record is too
    large): the rename fails with that errno, and the cache, index and
    member are unchanged;
  - the settle rewrite fails: the intent stays and fences every target;
  - the unlink after full success fails: the record stays;
  - a forked child exits right after its member call, before settling: the
    intent is there after a reopen;
  - a corrupt carrier: the cache refuses to open.
- **The carrier is protected.** Hidden from the root listing; refused
  through the mount, including chmod, chown, setxattr and removexattr; and
  not indexed by rebuild-index.
- **Races and ordering:**
  - a resolution made by another process during a rename's member call
    still reaches the mount;
  - a resolve of an in-flight intent fails as busy, and the miss is still
    recorded, including when a concurrent lookup reloads in between;
  - an intent left by a move that failed after the local move is released
    and can be resolved;
  - a late resolve of the intent id fails, and the miss survives it;
  - a busy carrier: settle leaves the intent in place, and the owner
    leaves a resolved carrier it cannot lock;
  - every cache directory down to both parents is fsynced before any
    member call;
  - an interrupted resolution stays enforced until the next open;
  - a failed fsync of the local move calls no member.
