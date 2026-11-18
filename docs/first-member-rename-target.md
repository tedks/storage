# First member rename: do not replace a target the namespace did not intend

Status: bounded design and synthetic RED only (CTO RRS-CTO-20261118B).
The externally visible fallback when no protection is available waits for
Ted's ruling; nothing here changes the first member rename yet. Tracking:
GitHub #181, ditz `gh-181-sync-rename-noreplace`, stacked on PR #182.

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

There are three cases, distinguished before any member call, inside the
critical section that moves the cache and the index.

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
filesystems that support it. `storage/streaming.py` already wraps it
(`_rename_noreplace`). Where the filesystem does not support it (`EINVAL`,
`ENOSYS`, `EOPNOTSUPP`), a regular file can be moved with `link` and then
`unlink`, since `link` fails if the target exists. A directory has no such
fallback. Checking with stat and then renaming is not atomic and is not used.

On an SSH member there is no portable proof. SFTP version 3 says a plain
rename fails if the target exists, and OpenSSH implements that for regular
files with `link` and `unlink`. For other types it checks with `stat` and then
renames, inside the server, which has the same race. Other servers differ, and
paramiko cannot tell which server it is talking to. So plain SFTP rename is a
reasonable attempt for regular files on OpenSSH, and is never treated as
proof for every server or type. The `hardlink@openssh.com` extension gives
the same no-replace link for regular files when the server advertises it.

## The open ruling

When neither primitive is available (a directory on SSH, a local filesystem
without `renameat2`, an SFTP server without the extension), the mount has to
choose. It can refuse the member rename and record it for repair, so the
member keeps the old name and the user's rename still succeeds locally. Or it
can replace on a best-effort basis, which is today's behavior. That choice is
visible to users, so Ted decides it. Until he does, nothing in
`NamespaceService.rename` or the backends changes.

## Validation

`tests/test_sync_races.py::TestFirstMemberRenameTarget` holds the synthetic
RED. One member is a real local directory, and its `rename` is wrapped so
that an unrelated object appears at the target just before the real call:
the window between the namespace decision and the member operation. The
cases for a known absent target and an unknown target are
`unittest.expectedFailure` today. They fail because `os.replace` destroys the
object, and they will turn into unexpected successes, which fail the suite,
once the fix lands; remove the markers then. The case for a known intended
replacement passes today and must keep passing.

    cd <worktree>
    nix develop --command bazel test //tests:storage_test_sync_races --test_output=errors

## Progress

- [x] Classify the target cases and state what each primitive does not prove (2026-11-18).
- [x] Synthetic RED for the known absent and unknown cases, and a control case for the intended replacement (2026-11-18).
- [ ] Ted's ruling on the fallback when no protection is available.
- [ ] Implement no-replace member rename per case, with a repair record on refusal (after the ruling).
- [ ] SSH coverage in the remote gate: a regular file and a directory onto an occupied target.

## Decision log

- 2026-11-18, CTO RRS-CTO-20261118B: kept out of PR #182, which stays the
  missed-rename retry fence. Stacked follow-up, design and synthetic RED
  first. No blanket change to the foreground rename.
