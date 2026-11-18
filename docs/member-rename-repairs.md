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

- the member is fenced from renames, mkdir, rmdir and uploads at names
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
together exceed about 3.3 KiB fails, on any mount with at least one member.
This is a stated bound, not widened scope. A filesystem that refuses `user.*`
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
   xattrs, as before.
3. **Member renames.** These skip any member that already had an open
   repair overlapping either path. Those members are added to the failed
   set, and none of them is called.
4. **Settle.**
   - If every member succeeded, the carrier is unlinked and the directory
     fsynced.
   - Otherwise the xattr is replaced, in one setxattr, by an `open` record
     naming only the failed members, with the reasons, and the file is
     fsynced.

   If settling fails, the `intent` record stays. It names every target
   member, so it over-fences, but nothing is lost. The rename still returns
   success, because its evidence is durable.

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
use the in-memory records, and reload when the directory's stat changes. That
is how a resolution made by the CLI reaches a live mount. A change the reload
misses (two changes within one timestamp tick) can only be a removal by
another process, so missing it keeps a protection longer and never drops one.

There is no SQLite projection. The table added earlier in this PR is
removed, so no index row and no numeric id stands for a repair.

## Resolution

`storage debug member-repairs -c CACHE` lists the carriers. `--resolve ID`
unlinks `.storage.repairs/ID` and fsyncs the directory. Until that removal is
durable, nothing drops a protection, and a mount picks the removal up on its
next lookup.

The id is the carrier's random name, so it survives index loss. A stale or
mistyped id names no carrier and is an error. Resolving the same id twice is
an error the second time and changes nothing. No id taken from a rebuilt
index can name a different repair.

## Validation

All of these run on scratch Local members, as unit and system tests in
`tests/test_sync_races.py`, `tests/test_member_repairs.py` and
`tests/test_cli_debug.py`:

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
- **The carrier is protected.** Hidden from the root listing, refused
  through the mount, and not indexed by rebuild-index.
