# Foreground responsiveness: exact-base slice plan (STORAGE-RESUME-171)

State: source/test planning only, 2026-11-17. No code was ported, and no
gates, mounts, worktrees or host probes were run. This is a proposal for the
CTO to review before any rebase, extraction or force-push.

## Bases read

| Ref | SHA | Notes |
| --- | --- | --- |
| `origin/master` | `0953369` | Includes #175 (`5f3dbb3`), #179 (`1811024`), #177 and the Bazel 8 / bzlmod work (#173/#174) |
| PR #171 head | `7284ad1` | 115 commits on `66c640a`, 60 behind master |
| PR #167 head | `d61bdb1` | 4 commits on `a7467c6` |

Merge conflicts, from `git merge-tree` against `0953369`:

- **#171**: 10 files: README, storage.example.toml, `backends/base.py`,
  `cache.py`, `cli_mount.py`, `fs.py`, `mount_profiles.py`,
  `test_cli_mount.py`, `test_fs_revalidate.py`, `test_ssh_backend.py`.
- **#167**: 3 files: `cli_mount.py`, `fs.py`, `mount_profiles.py`. These are
  wiring conflicts only. `sync.py` and `test_sync_manager.py` merge cleanly,
  including over master's `ea7763f` display-label change.

## What is already on master

| Guarantee from the #171 spec | On master? | Where |
| --- | --- | --- |
| A whole-file cold copy is classified as bulk, so it cannot take the reserved interactive SFTP channel | Yes, with `fuse_threads=true` only | #175: `cache.py:381` (`bulk_cold_copies`), `fs.py:133` (`interactive_request` per callback) |
| `readdir` lists the member inside the interactive context (the generator runs eagerly) | Yes | #175 (`0d3a468`) |
| An opt-in deadline on waits for bulk capacity, failing with `EAGAIN`; native cancel gives `EINTR` | Yes (#171 has none) | #175 `bulk_capacity_wait_timeout` |
| Multithreaded FUSE loop as an opt-in | Yes, `fuse_threads`, with no cap | gh-111 |
| Rsync uploads cannot skip a changed file | Yes | #179 |
| Active-callback cap with protected metadata, demand and ordinary classes | **No** | #171 `2a5490c` + `05401f8` only |
| Sync workers that grow with demand up to a cap | **No** | #167 `d61bdb1` (#171 carries a port plus a 14-line `dir_retry_guard` hook) |

So #171's channel tracer (`48b5ea0` and the channel half of `2a5490c`) is
superseded by #175 and must not be ported. #175's per-mode conditioning
(`bulk_cold_copies` only when threads are on) is the base to keep. What
remains of the foreground mission is the admission gate and sync growth.

## Where #171's scope grew, and why that matters

The separation audit (`contracts/responsiveness-separation.md`) found no small
cut, because the admission commits depend on the freshness, namespace and
durable-restart work that followed them. The dependency starts at a single
design choice:

- `e53cb50` moves a callback from the demand class to the ordinary class when
  a warm open finds it must refetch.
- `22379e2` drops the path and namespace locks for that move, because waiting
  for an ordinary slot while holding the path lock can deadlock.
- That lock-free window broke the private refetch pin (changed A lost, stale B
  selected). Repairing it took `34c584a`'s shared per-member debt, then
  no-replace stream and full-fetch publication, the local-mutation epochs, the
  namespace quarantine, and finally durable restart. Roughly 100 of the 115
  commits follow from it.

`2a5490c` + `05401f8` without the move keep master's locking exactly: `open`
still revalidates and fetches under its path lock, as it does today with
`fuse_threads=true`. The gate only decides when a callback starts. It adds no
new interleaving between a freshness decision and publication.

The namespace problems the audit lists (a partial rename returning success,
a queued rename retry overwriting an occupied member target) **already exist
on master** at the current fixed worker count. They are recorded in
`sync-member-reconcile`, and Ted's 2026-11-17 rulings now set their policy.
They are real, but they do not depend on foreground admission.

## Proposed smallest shippable slice: S1, foreground admission without the class move

A new branch from `0953369`. #171 stays untouched as evidence.

1. Port `storage/foreground_admission.py` and the gate in `StorageFS.__call__`
   from `2a5490c`/`05401f8`. Do not port the channel tracer, `e53cb50`,
   `22379e2` or anything after them.
2. The class is fixed at entry: metadata (getattr, opendir, readdir,
   releasedir, access, readlink, getxattr, listxattr, statfs); demand (read,
   write, flush, fsync, release, and an open whose cache file exists before
   admission); ordinary for everything else. A warm open that turns out to
   need a refetch keeps its demand slot and its locks. This is master's
   behaviour, and the cost is bounded by the cap.
   - Optional, and only if review wants it: take an ordinary slot with a
     try-acquire (never a wait) and release the demand slot. It never blocks
     while holding locks, so it can neither deadlock nor open a window.
3. Setting `max_foreground_operations` requires and enables `fuse_threads`.
   When the setting is absent, behaviour is byte-for-byte today's. This keeps
   #175's bulk classification active whenever the gate is on.
4. Ted's cap-three ruling. **Recommendation:** a minimum cap of 4 that
   protects two demand slots, with a test where two reads compete. Fallback:
   keep the minimum at 3 and document the one-demand-slot limit as
   prototype-only. The CTO picks.
5. Never cancel cleanup operations. If an explicit gate close cancels a queued
   `release` or `releasedir`, the descriptor leaks and, for a member copy with
   a conflict suffix, the upload on close is skipped (master `fs.py` `release`).
   These must bypass the gate, or be exempt from close-cancellation. A test
   must prove that no dirty write loses its queued sync.
6. Tests, on master's fixtures (`run_fuse`, `FUSE_THREAD_MODES`, and #175's
   `test_fuse_thread_state.py`):
   - Port `TestForegroundAdmission` (browsing and reads keep moving; warm opens
     and directory handles progress while a cold open waits; per-class FIFO
     and queued cancellation).
   - Drop the transfer tests (`test_changed_open_drops_path_lock_...`,
     `test_reclassification_cancel_releases_both_slots`) and the
     shared-obligation suite. They test the removed design.
   - Add a test where a warm-turned-cold open keeps its demand slot and path
     lock to completion, and a competing read still progresses (protected
     second demand slot, or shared capacity).
   - Add the cleanup-operation close test from item 5.
   - RED on master first. Mutation controls: remove the class reserve; let
     the gate cancel release.
   - Assert exact bytes and unchanged exceptions throughout.
7. Docs: README Status, `storage.example.toml`, and the existing
   `threading-fuse-worker-cap` limit: libfuse native threads stay uncapped.
8. Close-out: `scripts/run-gates.sh` on the exact head, `//:storage_cli`
   build, council to fixpoint, then `merge-gated.sh`.

Estimated size: about 250 lines of source and 400 of tests. Most of the
conflicts are in `fs.py` `__init__`/`__call__` and the mount/profile option
plumbing beside #175's `bulk_capacity_wait_timeout`.

## #167 disposition

**Recommendation: land #167 as the sync-growth half, rebased by a normal
merge of master or rebased on the CTO's word, as a separate PR alongside S1.
Do not take #171's port of it.**

- #171's `sync.py` is #167's plus `dir_retry_guard`, which only means
  something with #171's namespace-uncertainty records. Leave it out.
- Its 3 conflicts are option wiring, adjacent to #175's new options in the
  same files. Order the PRs: S1 first, then rebase #167 on it, or the reverse.
  Both touch `cli_mount.py`, `mount_profiles.py` and the `StorageFS.__init__`
  signature, so whichever goes second resolves about 30 lines.
- Before it merges, one safety point bears on Ted's collision ruling. More
  workers let a stale `DirOperation.RENAME` retry run sooner and more often.
  Master's `_process_dir_task` calls `backend.rename`, which replaces an
  occupied target (Local `os.rename`; SSH `posix_rename` since #149). Ted
  ruled that reconciliation must refuse to replace an unrelated target. The
  hazard is on master today; growth makes it likelier. Proposed small fix,
  separate and ahead of #167 or folded into it: the retry refuses when the
  member target exists (a member stat inside the existing path guard; a
  no-replace rename where the member supports one), keeps both objects and
  records a conflict or repair entry. It replays nothing destructive. This
  carries out the collision and external-writer rulings for the retry path
  without #171's lineage machinery. A stat-then-rename still races an
  external writer, so the PR must state that limit. An atomic guarantee
  needs a member-side conditional move, which Ted deferred as future
  direction (xattr duplication).
- Keep #167's caps: 8 with SSH or rsync members, 32 local-only. Sync uploads
  run non-interactive, so they draw on bulk capacity and the interactive
  reserve still holds. A test that 8 workers plus a cold copy leave the
  reserved channel free would make this explicit.
- Close `adaptive-sync-cleanup-error-context` in the same PR, or leave it
  filed.

## #171 disposition

**Recommendation: keep #171 open as a draft evidence and design record. Do
not rebase it, force-push it or merge it. Re-scope it as the namespace and
freshness safety track, which no longer gates foreground responsiveness.**

- After S1 and #167 land, the foreground request (reads and browsing under
  demand, sync growth to bounded caps) is met for opt-in mounts, with the
  documented limits.
- Ted's rulings now define the release semantics #171 was blocked on:
  - Partial rename: report the failure, keep the local move, expose repair.
  - Collision: keep both objects, refuse replacement.
  - External writers: stay supported; ambiguous cases go to repair.
  - Legacy cache: refuse startup without mutating it.
- Those apply to master's existing rename path, so they belong in their own
  PRs from master: `sync-member-reconcile`, then the
  `storage171-namespace-*` issues re-homed off #171. Mine #171's contracts
  (`namespace-reconciliation`, `durable-freshness-restart`,
  `path-freshness-obligation`) and its tests for them. Do not carry its code
  forward wholesale.
- The Stage1 scratch-authority work (`37d671b`, `b0c0c4f`, with S1 FINDINGS
  open) goes into the durable-restart track. It is not part of S1.
- Retire #171 once its contracts and tests are mined into those PRs. Until
  then it stays as a record.

## Open decisions for the CTO

1. Approve S1 as scoped, specifically that the warm-to-cold class move stays
   out.
2. Cap three: minimum 4 with two demand slots (recommended), or 3 documented
   as prototype-only.
3. #167: S1 first or #167 first. Is the stale-retry no-replace fix a
   prerequisite or a parallel PR?
4. Accept the re-scoping of #171 to the namespace and freshness safety track.

## Ownership limits

Data and external writers: S1 changes when callbacks start, not what they do.
The retry fix only ever refuses an operation and never deletes. Nothing in
this plan mutates a member tree or a cache root that is not a test scratch
area.
