# Local write sync diagnosis

State: BLOCKED
Owner: sole Lead-tier Sol diagnostic/repair lane
Worktree: `/home/tedks/Projects/storage/local-write-sync-20261113`
Branch: `diag/local-write-sync-20261113`
Base: `66c640aec005feccd0bd585f3355295c3bb772ba`; last test commit: `00ffdb9`; last record commit: this status file's commit.
Current task: checkpoint the direct local-rsync RED and the unresolved repeatable Bazel prerequisite. No production fix was attempted.
Intent: DESIGN.md offline writes and no silent loss; whole-file sync may use rsync delta transfer.
Assumptions: a Storage mount calls `copy_from_local` for an already selected upload; the destination exists with different bytes but identical size and mtime; no live profile or field file is assumed. Direct cache edits are outside supported writes. The local synthetic transport is evidence of rsync selection, not proof of the field cause.
Failure modes to test: ordinary changed metadata, equal size and mtime, transfer failure, and caller-supplied rsync arguments that may reverse selection.
Owned edits: `tests/test_rsync_backend.py`, `tests/test_fs_revalidate.py` (selected for real StorageFS callback and SyncExecutor settlement), and `storage/backends/rsync.py` only for a focused fix.
Repair authority: local synthetic diagnosis and focused nonmount tests, at most four Bazel invocations under the stated systemd and flock bounds. No live data or broader sync semantics change.
Stop conditions: product/contract change, no cached test route, runtime or scratch bound, third unresolved implementation failure, unsafe cleanup, or conflicting owner.
Runtime invocations: 3/4 focused Bazel attempts. Invocation 2 resolved locked dependencies in 24 seconds through shared content cache `/home/tedks/.cache/bazel/_bazel_tedks/cache/repos/v1`; Nix stayed offline and no peer output base was used. Direct real-rsync tests failed behaviorally with stale destination bytes; four integration cases hit a corrected local fixture error and one failure case passed. Invocation 3 restored cache-only and stopped before tests on the same missing PyPI simple metadata. Network bytes: UNKNOWN. One invocation remains. Owned scratch after invocation 3: 393 MiB (<2 GiB); host free >10 GiB. All owned scopes inactive/dead and flock holder exited. Blocked on: repeatable cached metadata route or explicit further dependency-resolution authority. No production fix or mutation check.
Next checkpoint: root disposition of the concrete prerequisite; no further test attempt under the current allowance.
