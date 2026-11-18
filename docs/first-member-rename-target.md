# First member rename: do not replace a target the namespace did not intend

Status: implemented (PR #183) under Ted's ruling on #183
(2026-11-18, CTO-183-IMPLEMENT-GO). Tracking: GitHub #181, ditz
`gh-181-sync-rename-noreplace`.

## Purpose

When a file or directory is renamed through the mount, `NamespaceService.rename`
(`storage/namespace.py`) moves the cache copy and the index rows, then renames
the path on each storage member at once, through `Backend.rename`. On a local
member that is `os.replace`, and on an SSH member it is `posix-rename`. Both
replace whatever is at the new name. That is right when the namespace itself
had an object at the new name: the user is replacing it, as `rename(2)` does,
which is how editors save a file. It is wrong when the namespace had nothing
there. Members can be written directly at any time, so an object can appear
at the new name on a member after the mount decided the target was free and
before the member call. An object can also have been there all along without
the mount ever listing it. The member rename then destroys an object the
namespace never knew about.

After this work, a member rename onto a name the namespace did not hold will
leave any object it finds there intact. The move on that member is then
recorded for repair, as PR #182 already does for a member that missed the
move. A rename that replaces a namespace object keeps replacing it.

PR #182 makes sure a missed member rename is never replayed later. This plan
covers the first attempt, which PR #182 leaves as it was.

## What the mount knows at decision time

There are three cases, distinguished before any member call and before the
rename's intent is written, while the rename holds its path locks (or, for
a directory, the namespace lock exclusive).

A **known intended replacement**: the namespace had an object at the new name
before the move, meaning a cache entry or an index row there. For a given
member, the replacement counts as intended only if that member has a location
row for the new name, so the object there is the namespace's own copy, as far
as the index knows. The member rename replaces, as now. It is still not proof:
another writer can have changed that copy since. But replacing the object the
user chose to replace is the requested operation, not a destruction of an
unknown object.

A **known absent target**: the namespace had nothing at the new name, and the
parent directory's listing (`NamespaceService.dir_cache[parent][member]`) is
within its fresh window for this member and does not contain the name. This
is the common case. A listing is a snapshot, so this still does not prove the
target is absent at the instant of the member call. It only rules out an
object the mount could have known about.

An **unknown target**: the namespace had nothing at the new name, and this
member's listing of the parent is missing, stale, or shows the name. The
member may hold an object there that the namespace never adopted. This
includes a namespace target that this member has no location row for.

In the last two cases the member call must not replace. The difference
between them matters only for what is reported. A refusal in the known
absent case means another writer created the target during the rename. A
refusal in the unknown case may mean an object was already there.

## Primitives, and what they do not prove

A no-replace rename protects the target atomically on the member. It does not
prove the identity of the source. If another writer replaced the source just
before the call, the member moves that object instead. No bytes are
destroyed, since the object is moved, not overwritten, so this is outside
the hazard this plan closes. The source is stated as unproven, not as safe.

On a local member, Linux `renameat2` with `RENAME_NOREPLACE` is atomic on the
filesystems that support it (`storage/backends/local.py`, `rename_noreplace`).
Where the filesystem does not support it (`EINVAL`, `ENOSYS`, `EOPNOTSUPP`),
the member is skipped and recorded. Two fallbacks are rejected (CTO):
- `link` followed by `unlink`: a concurrent writer can replace the source
  name between them, and the blind unlink then removes that new object;
- checking with `stat` and then renaming, which is not atomic.

On an SSH member there is no portable proof. SFTP version 3 says a plain
rename fails if the target exists, and OpenSSH implements that for regular
files with `link` and `unlink`. For other types it checks with `stat` and then
renames, inside the server, which has the same race. Other servers differ, and
paramiko cannot tell which server it is talking to. So plain SFTP rename is
never treated as proof, for any server or type. Nor is the
`hardlink@openssh.com` extension, since it is a link that a blind unlink
must then follow. An SSH member is therefore always skipped and recorded
when replacement was not intended.

## The ruling, and what is built

Ted approved the conservative fallback. When replacement was not the
intent on a member, the move uses an operation with verified destination
protection. Without that guarantee, the member move is skipped and
reported as a partial failure, with a durable repair naming both paths.
That covers ordinary SSH directory moves on servers without a suitable
atomic operation. The namespace's own move, already committed, stays
visible. A missing or stale member listing cannot establish absence, or tell when a
destination appeared.

`NamespaceService.rename` decides each target member before any effect.

- **Intended replacement**, on member-specific evidence only. The
  namespace holds a file at the new name, and that member's copy of it
  is recorded as a location row. The member rename is `Backend.rename`,
  which replaces, as before. A directory at the new name is never
  evidence (CTO-183-DIR-PROVENANCE). Directories have no per-member
  record, so a namespace directory there says nothing about what each
  member holds. A directory move always refuses rather than replace.
- **Every other target, absent or unknown.** The member rename is
  `Backend.rename_noreplace`. It must be one operation that the member
  itself refuses when the new name is taken. A check followed by a
  rename does not qualify, and neither does a link followed by an
  unlink: a concurrent writer can get between them.
  - `LocalBackend` uses `renameat2(RENAME_NOREPLACE)`. Where the
    filesystem or libc lacks it (`EINVAL`, `ENOSYS`, `EOPNOTSUPP`), it
    raises `ENOTSUP`, with no fallback.
  - `SSHBackend` and the others inherit the base method, which always
    raises `ENOTSUP`. SFTP offers no operation the client can verify as
    atomic and non-replacing for every server and type.
  - A missing source (`FileNotFoundError`) is never reported as a move.
    It is recorded too, with its own reason, since the member may hold an
    unrelated object at the new name that must stay fenced.
  - A refusal (`EEXIST`) or `ENOTSUP` is a missed member. The member
    keeps the source and any object at the new name, and the rename's
    intent settles into an open repair for it (docs/member-rename-
    repairs.md), with the reason.

In practice a rename onto a fresh name on an SSH member is never made
there. It is recorded for repair instead, which is the cost of the ruling.

## Validation

`tests/test_sync_races.py::TestFirstMemberRenameTarget`, on a real local
member:
- the former synthetic REDs, now passing: an object created at the
  target in the window before the member call, and one the mount never
  listed;
- a directory onto an occupied member directory;
- a free target moved by the no-replace rename;
- a filesystem without `RENAME_NOREPLACE`, where the member is skipped
  and recorded;
- the intended-replacement control.

`tests/test_remote_system.py` covers SSH: an occupied file name, a free
directory and a free file name, which are not moved and are recorded,
and the intended-replacement control.

    cd <worktree>
    nix develop --command bazel test //tests:storage_test_sync_races --test_output=errors

## Progress

- [x] Classify the target cases and state what each primitive does not prove (2026-11-18).
- [x] Synthetic RED for the known absent and unknown cases, and a control case for the intended replacement (2026-11-18).
- [x] Ted's ruling on the fallback when no protection is available (2026-11-18).
- [x] No-replace member rename for non-intended targets, with a repair record on refusal or when unsupported (2026-11-18).
- [x] SSH coverage in the remote gate: an occupied file, free directory and file targets, and intended replacement (2026-11-18).

## Decision log

- 2026-11-18, CTO RRS-CTO-20261118B: kept out of PR #182, which stays the
  missed-rename retry fence. Stacked follow-up, design and synthetic RED
  first. No blanket change to the foreground rename.
- 2026-11-18, Ted on #183 and CTO-183-IMPLEMENT-GO: the conservative
  fallback. A verified non-replacing primitive is used, or else the member
  is skipped and recorded. Intended replacement is preserved. The absent
  and unknown cases are handled the same way, since a listing cannot prove
  absence.
