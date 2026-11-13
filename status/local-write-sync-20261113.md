# Local write sync diagnosis

State: STARTED (rsync_args product ruling pending)
Owner: sole Lead-tier Sol diagnostic/repair lane
Worktree: `/home/tedks/Projects/storage/local-write-sync-20261113`
Branch: `diag/local-write-sync-20261113`
Base: `66c640aec005feccd0bd585f3355295c3bb772ba`; last test commit: `00ffdb9`; pushed production-change SHA: `f294de9` (this checkpoint); last record commit: this status file's commit.
Current task: hold a draft upload-only quick-check fix; obtain the caller-argument policy ruling before changing its contract or claiming full GREEN.
Intent: DESIGN.md offline writes and no silent loss; whole-file sync may use rsync delta transfer.
Assumptions: a Storage mount calls `copy_from_local` for an already selected upload; the destination exists with different bytes but identical size and mtime; no live profile or field file is assumed. Direct cache edits are outside supported writes. The local synthetic transport is evidence of rsync selection, not proof of the field cause.
Failure modes to test: ordinary changed metadata, equal size and mtime, transfer failure, and caller-supplied rsync arguments that may reverse selection.
Owned edits: `tests/test_rsync_backend.py`, `tests/test_fs_revalidate.py` (selected for real StorageFS callback and SyncExecutor settlement), and `storage/backends/rsync.py` only for a focused fix.
Repair authority: local synthetic diagnosis, narrow rsync upload selection, and at most seven lifetime focused nonmount Bazel invocations under the stated systemd and flock bounds; two remain held pending ruling. Locked dependency resolution may occur in those invocations; Nix stays offline. No live data or broader sync semantics change.
Stop conditions: product/contract change without a ruling, runtime or scratch bound, third unresolved implementation failure, unsafe cleanup, or conflicting owner. The earlier cache-only route stop was replaced by root.
Runtime invocations: 5/7 lifetime, two remain held unused by root disposition. Invocation 4 on unchanged rsync source proved integrated false-CLEAN with stale member bytes; legacy skip and failure controls passed. Invocation 5 with upload-only `--ignore-times` passed all five integration cases and the default direct case, but caller `--size-only` remained RED. The original-source RED is the mutation check for the default fix. Combined remaining runtime used: 20/1200 seconds; each scope <=10 minutes. Nix offline, shared content cache `/home/tedks/.cache/bazel/_bazel_tedks/cache/repos/v1`, owned output base, 2 CPU/4 GiB/swap0, <=2 GiB owned scratch, >=10 GiB host free. Network bytes: UNKNOWN. Scopes 4 and 5 inactive/dead, flock holder exited. Pending product ruling: `oq-rsync-upload-selection-args`, Ted decides the `RsyncBackend.__init__(rsync_args)`/`copy_from_local` contract. No full GREEN or exact pushed-head module run.
Next checkpoint: Ted's policy ruling, then focused GREEN and full two-module run if authorized. Root retains this exact context for the fix delta.
