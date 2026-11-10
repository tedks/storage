# Foreground lead events

## foreground-lead:1

- kind: state-change
- state: STARTED
- based_on: `66c640a`
- refs: none
- evidence: Clean assigned worktree and branch confirmed; ROLES, DOCTRINE, repository instructions, DESIGN and committed integration roster read before source change.
- intent: Foreground reads and browsing progress under bounded demand-scaled work; #167's sync-only scope does not fulfill Ted's ruling.
- authority: Implement and push reviewable work here; root retains acceptance and merge authority.
- next: trace live code boundaries with scratch-only synthetic evidence, then cut packages after a necessity critique.

## foreground-lead:2

- kind: state-change
- state: TRACER_VERIFIED
- based_on: `48b5ea0`
- refs: foreground-lead:1
- evidence: `nix develop --command bazel test //tests:storage_test_fs_threading --test_arg=-k --test_arg=cold_full_copy --test_output=errors` failed on the pre-fix tree: copy interactive=true, listing interactive=false. The same command passed after the fix. Full-file copy now has a nested bulk scope; readdir eagerly resolves under the FUSE interactive context.
- limit: This tracer proves classification and bytes on a local synthetic member. It does not prove channel fairness or native FUSE thread bounds.
- next: Two sequential packages cut at `org/work-packages/`; fresh necessity critique pending.

## foreground-lead:3

- kind: state-change
- state: READY for FR-foreground and FR-sync, sequentially
- based_on: `48b5ea0`
- refs: foreground-lead:2
- necessity: Fresh OpenAI Luna/Utility critique returned SIMPLIFY. Retain the two tracer fixes; require a red admission saturation test before its machinery. Clarified cap-3 reserve, progress conditional on active completion, queued cancellation versus active member I/O, and the one-channel SSH limit in spec/package. No additional scheduler or load policy justified.
- next: implement FR-foreground first; review its diff and publish the config interface before FR-sync.

## foreground-lead:4

- kind: state-change
- state: FR-foreground implemented, focused verification; not accepted
- based_on: `edc9736` plus uncommitted foreground diff
- refs: foreground-lead:3
- evidence: Admission synthetic test was red before implementation and green afterward. Four focused Bazel targets passed. Fake SSH pool held three foreground-initiated bulk transfers with `bulk_leased=3` and `sftp_open=3` while metadata and demand interactive acquisitions completed; its targeted Bazel test passed. No live member or mount used.
- limitation: Active Python callback cap only. Libfuse 2 native threads can still grow with waiting callbacks. This is documented in CLI, README and spec. Full gates and review pending.
- control: tmux message attempt to root `%87` returned `mixed-input-after-paste`, no new receipt; its pending payload is not replayed. Root delivery remains unknown and will be retried only from a verified empty composer.

## foreground-lead:5

- kind: state-change
- state: FR-foreground pushed, review pending
- based_on: `2a5490c`
- refs: foreground-lead:4
- evidence: Exact branch push succeeded; `git diff --check` clean. Focused Bazel commands cited in FR-foreground package ran on the pre-commit tree, not the final head; exact-head gates remain pending. Self-review found no changed byte/write path: admission wraps callbacks and the cold-copy scope only changes SSH lease classification.
- next: FR-sync reconciliation, then final gate and independent review.

## foreground-lead:6

- kind: state-change
- state: FR-sync reconciled, focused verification; not accepted
- based_on: `effadd7` plus uncommitted compatibility fix
- refs: foreground-lead:5
- evidence: Four #167 commits cherry-picked in order into this branch without touching its sibling. Conflicts in `fs.py`, `cli_mount.py`, and `mount_profiles.py` retained both cap interfaces and current-master display names. `UpdateHostKeys=no` remains in rsync and watcher. Five focused targets passed; full SSH module initially exposed the no-init probe subclass and passed after the optional gate lookup fix. No live member used.
- limitation: Original #167 all-gates/council evidence does not transfer to this union tree. Exact pushed-head gates and fresh review required.
- control: BUILD one-line source addition delivered to integration lead `%93` with submitted receipt. Root `%87` message still lacks a receipt after mixed input; do not count it delivered.
