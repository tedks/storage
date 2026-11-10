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
