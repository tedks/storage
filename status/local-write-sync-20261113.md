# Local write sync diagnosis

State: STARTED (startup validator implemented; pushed-head verification pending)
Owner: sole Lead-tier Sol diagnostic/repair lane
Worktree: `/home/tedks/Projects/storage/local-write-sync-20261113`
Branch: `diag/local-write-sync-20261113`
Base: `66c640aec005feccd0bd585f3355295c3bb772ba`; last pushed design checkpoint: `87cc102`; last tested source: uncommitted validator after `87cc102`.
Current task: implement Ted's behavioral startup validation ruling after the bounded design/source checkpoint.
Intent: DESIGN.md offline writes and no silent loss; whole-file sync may use rsync delta transfer.
Assumptions: a Storage mount calls `copy_from_local` for an already selected upload; the destination exists with different bytes but identical size and mtime; no live profile or field file is assumed. Direct cache edits are outside supported writes. The local synthetic transport is evidence of rsync selection, not proof of the field cause.
Failure modes to test: ordinary changed metadata, equal size and mtime, transfer failure, and caller-supplied rsync arguments that may reverse selection.
Owned edits this phase: `storage/backends/rsync.py`, `storage/fs.py` startup hook, `tests/test_rsync_backend.py`, `tests/test_allow_other.py`, existing `tests/test_fs_revalidate.py` only if the prior regression requires repair, and unique docs/status/events. No build graph, sibling, live profile, or remote edits.
Repair authority: Ted ruled behavioral startup validation of configured rsync options. Six new focused nonmount Bazel invocations total for this phase, each <=10 minutes, combined <=30 minutes, under the existing flock, owned batch output base, 2 CPU/4 GiB/no swap, <=2 GiB total owned scratch and >=10 GiB host free. Nix offline; unchanged locked public dependencies may resolve. No live data, SSH, FUSE, Docker, deployment, merge, or full gates.
Stop conditions: new product boundary, unsafe probe confinement, runtime/scratch bound, third unresolved implementation failure, or conflicting owner.
Runtime invocations this phase: 4/6; cumulative 52/1800 seconds. Invocation 1 gave unchanged-code boundary RED; invocation 2 passed new startup cases but caught the older direct `--size-only` expectation; invocation 3 passed the three full affected modules; invocation 4 removed the destination-byte check temporarily and seven assertions failed, then the check was restored. Active samples verified 2 CPU/4 GiB/no swap, maximum owned scratch 482 MiB and host free above 10 GiB. All four scopes inactive/dead, empty control groups; flock wrappers exited. Network bytes: UNKNOWN; shared content-addressable repository cache used, no peer output base. `oq-rsync-upload-selection-args` is answered; `docs-rsync-upload-contract` records the documentation gap. `gh-112-rsync-quickcheck` is in progress.
Next checkpoint: push coherent implementation, run focused check on pushed source, record results, push report, run full affected modules on exact final pushed head, then root delta review. Two scoped invocations remain; no release or merge claim.
