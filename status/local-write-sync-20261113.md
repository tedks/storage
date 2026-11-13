# Local write sync diagnosis

State: BLOCKED
Owner: sole Lead-tier Sol diagnostic/repair lane
Worktree: `/home/tedks/Projects/storage/local-write-sync-20261113`
Branch: `diag/local-write-sync-20261113`
Base: `66c640aec005feccd0bd585f3355295c3bb772ba`; last test commit: `270931c`; last record commit: this status file's commit.
Current task: preserve the traced path and unrun local rsync regression after the cached-input prerequisite failed.
Intent: DESIGN.md offline writes and no silent loss; whole-file sync may use rsync delta transfer.
Assumptions: a Storage mount calls `copy_from_local` for an already selected upload; the destination exists with different bytes but identical size and mtime; no live profile or field file is assumed. Direct cache edits are outside supported writes. The local synthetic transport is evidence of rsync selection, not proof of the field cause.
Failure modes to test: ordinary changed metadata, equal size and mtime, transfer failure, and caller-supplied rsync arguments that may reverse selection.
Owned edits: `tests/test_rsync_backend.py`, `tests/test_fs_revalidate.py` (selected for real StorageFS callback and SyncExecutor settlement), and `storage/backends/rsync.py` only for a focused fix.
Repair authority: local synthetic diagnosis and focused nonmount tests, at most four Bazel invocations under the stated systemd and flock bounds. No live data or broader sync semantics change.
Stop conditions: product/contract change, no cached test route, runtime or scratch bound, third unresolved implementation failure, unsafe cleanup, or conflicting owner.
Runtime invocations: 1/4 focused Bazel attempt; it stopped during dependency analysis before tests. Incremental owned scratch: 133 MiB/2 GiB. Both owned systemd scopes are inactive/dead; the scoped flock holder exited. Blocked on: cached PyPI simple metadata for the rules_python extension, or explicit changed runtime authority. No source fix or mutation check was attempted.
Next checkpoint: root disposition of the prerequisite and scope; no further runtime attempts under the present package.
