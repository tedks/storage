# Local write sync diagnosis

State: STARTED (startup validation design checkpoint)
Owner: sole Lead-tier Sol diagnostic/repair lane
Worktree: `/home/tedks/Projects/storage/local-write-sync-20261113`
Branch: `diag/local-write-sync-20261113`
Base: `66c640aec005feccd0bd585f3355295c3bb772ba`; last test commit: `00ffdb9`; pushed production-change SHA: `f294de9` (this checkpoint); last record commit: this status file's commit.
Current task: implement Ted's behavioral startup validation ruling after the bounded design/source checkpoint.
Intent: DESIGN.md offline writes and no silent loss; whole-file sync may use rsync delta transfer.
Assumptions: a Storage mount calls `copy_from_local` for an already selected upload; the destination exists with different bytes but identical size and mtime; no live profile or field file is assumed. Direct cache edits are outside supported writes. The local synthetic transport is evidence of rsync selection, not proof of the field cause.
Failure modes to test: ordinary changed metadata, equal size and mtime, transfer failure, and caller-supplied rsync arguments that may reverse selection.
Owned edits this phase: `storage/backends/rsync.py`, `storage/fs.py` startup hook, `tests/test_rsync_backend.py`, `tests/test_allow_other.py`, existing `tests/test_fs_revalidate.py` only if the prior regression requires repair, and unique docs/status/events. No build graph, sibling, live profile, or remote edits.
Repair authority: Ted ruled behavioral startup validation of configured rsync options. Six new focused nonmount Bazel invocations total for this phase, each <=10 minutes, combined <=30 minutes, under the existing flock, owned batch output base, 2 CPU/4 GiB/no swap, <=2 GiB total owned scratch and >=10 GiB host free. Nix offline; unchanged locked public dependencies may resolve. No live data, SSH, FUSE, Docker, deployment, merge, or full gates.
Stop conditions: new product boundary, unsafe probe confinement, runtime/scratch bound, third unresolved implementation failure, or conflicting owner.
Runtime invocations this phase: 0/6. Prior phase: 5/7 with two unused, superseded by this phase's envelope. Network bytes: UNKNOWN. Last known owned scratch: 453 MiB; prior scopes inactive/dead and flock exited. Ted's decision is in closed `oq-rsync-upload-selection-args` (answered, not implemented). No startup validation code yet.
Next checkpoint: push the small design/source trace, then behavioral RED and implementation.
