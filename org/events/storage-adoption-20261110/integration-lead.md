# Integration lead events

## integration-lead:1

- kind: state-change
- state: STARTED
- based_on: `a7467c6862b08588df0fb451e4e3eac89e87394b`
- refs: none
- evidence: Assigned worktree and branch confirmed; inherited PR heads and
  constraints entered in `org/ROSTER.md`.
- assumption: Existing PR branches are quiescent; verify this before treating
  a frozen review as valid. Source branches remain owned by prior lanes.
- failure modes: missing gate receipts, stale master, missing formal review,
  `gh` authentication, and cross-branch semantic conflicts. Any such condition
  blocks a stronger acceptance or merge claim until resolved.

## integration-lead:2

- kind: deviation
- refs: integration-lead:1
- based_on: `a7467c6862b08588df0fb451e4e3eac89e87394b`
- bend: Adopt already completed work without retroactively running spec,
  decomposition, standup or ongoing Crystal. Rollback: leave PRs unmerged and
  return to the full workflow for new work. Reason: acceptance and integration
  of actual work are this mission's Schwerpunkt.

## integration-lead:3

- kind: lead-review
- refs: integration-lead:2
- based_on: `64c04d6cde73ca5bd3815ed896ad9e71aa9582dc`
- package: A165
- verdict: CLEAN on full `a7467c6..64c04d6` diff. Both command builders
  explicitly pass one `UpdateHostKeys=no`; tests inspect parsed rsync options
  and watcher argv; README describes the lost automatic-rotation behavior.
  No member identity, cache path, or host-key file is changed by this code.
- gates: `.gates/64c04d6cde73ca5bd3815ed896ad9e71aa9582dc.json`
  matches exact SHA and tree and attests `//:storage_tests_all_gates`.
- council: formal helper `check 165 --expect-head 64c04d6...` returned
  `clean: true`; OpenAI and Anthropic CLEAN, Google EMPTY due usage limit.
- status: ACCEPTED for integration, subject to the scripted live PR/head/base
  checks and required merge record. The receipt is local evidence, not an
  independently observed gate rerun by this lead.

## integration-lead:4

- kind: state-change
- refs: integration-lead:3
- based_on: `84bb8d6210ee03aaec6845c16fe63cd8becd2f0a`
- package: A166
- state: REVIEWING, not accepted. Exact SHA/tree all-gates receipt exists,
  but formal helper `check 166 --expect-head 84bb8d6...` returned
  `clean: false`: no round at this head and a standing Important finding from
  the prior name-as-suffix design. The revised display-only design requires
  formal frozen review and a new outcome record. Google is a missing seat,
  not a clean seat.

## integration-lead:5

- kind: crystal-conflict
- refs: integration-lead:4
- based_on: `a7467c6862b08588df0fb451e4e3eac89e87394b`
- check: `git merge-tree --write-tree` on frozen branch pairs.
- result: #165+#166 clean; #165+#167 clean; #166+#167 has textual conflicts in
  `storage/cli_mount.py` and `storage/mount_profiles.py`. #167 is held for a
  product ruling, so no repair or merge is authorized here. A clean
  merge-tree alone does not prove semantic compatibility or passed union gates.

## integration-lead:6

- kind: state-change
- refs: integration-lead:3, integration-lead:5
- based_on: `64c04d6cde73ca5bd3815ed896ad9e71aa9582dc`
- package: A165
- state: INTEGRATED. `scripts/merge-gated.sh 165` exited 0 after confirming
  PR head, exact gate receipt, and base ancestry; PR #165 reports MERGED at
  `38c3bdb9562cade82411751acca058b04c974b5c`. Fetched `origin/master`
  equals that commit. The PR contains the lead acceptance and gate comment.
  The tactical `gh-107-preserve-host-keys` issue was already closed; the
  broader GitHub #107 remains open by design. No deployment claim.

## integration-lead:7

- kind: state-change
- refs: integration-lead:6
- based_on: `84bb8d6210ee03aaec6845c16fe63cd8becd2f0a`
- package: A166
- state: REVIEWING. #165 advanced master, so #166 now needs a rebase,
  exact-head gates and renewed formal review before the scripted merge.
  The lead requested branch repair authority or reassignment from the parent
  over `tmux-message`, with a submitted receipt; no source branch edit yet.

## integration-lead:8

- kind: state-change
- refs: integration-lead:7
- based_on: `4ca26cd8b1a041a6308bf4b77fb847e3fea033e1`
- package: A166
- state: REVIEWING. Parent assigned prior native Sol lane as sole rebase,
  gates and council owner. It rebased/pushed to this head over #165's merge
  commit `38c3bdb...`; five commits are patch-equivalent by `git range-diff`.
  GitHub reports this exact head and base. No duplicate repair lane spawned.

## integration-lead:9

- kind: lead-review
- refs: integration-lead:8
- based_on: `4ca26cd8b1a041a6308bf4b77fb847e3fea033e1`
- package: A166
- verdict: CLEAN on the full `38c3bdb..4ca26cd` diff. Optional
  `{spec, name}` parsing retains local/remote ordinal IDs; the labels flow
  only into mount output, diagnostics and `BackendInfo.display_name`.
  Persistence, policies, conflict suffixes, and watcher callback paths still
  use numbered identities. Synthetic tests cover mixed profile forms,
  duplicate/invalid labels, legacy CLI overrides, conflict routing, watcher
  labels, and stable sync location keys. `git diff --check` passed.
- limitation: No new-head gates or formal council verdict yet. CLEAN lead
  review alone is not acceptance. Re-review any subsequent fix delta.

## integration-lead:10

- kind: interpretation-filed
- refs: integration-lead:9
- based_on: `4ca26cd8b1a041a6308bf4b77fb847e3fea033e1`
- issue: `oq-streaming-fleet-keep-threshold`, filed and `ditz sync` succeeded.
  Ted chooses a higher byte or percentage threshold and rollout scope before
  profile edits. No live profile or host inspection was performed.

## integration-lead:11

- kind: lead-review
- refs: integration-lead:9
- based_on: `4ca26cd8b1a041a6308bf4b77fb847e3fea033e1`
- package: A166
- verdict: ACCEPTED. The exact SHA/tree all-gates receipt attests
  `//:storage_tests_all_gates`; the assigned lane reported a CLI build on
  this clean head. Formal helper `check 166 --expect-head 4ca26cd...`
  returned `clean: true`, Anthropic and OpenAI CLEAN, Google EMPTY due usage
  limit. Presentation nits are in `display-name-ux-followups`. The earlier
  full-diff lead review remains clean on this unchanged head.

## integration-lead:12

- kind: state-change
- refs: integration-lead:11
- based_on: `4ca26cd8b1a041a6308bf4b77fb847e3fea033e1`
- package: A166
- state: INTEGRATED. `scripts/merge-gated.sh 166` exited 0 after checking
  PR head, receipt and ancestry at master `38c3bdb...`. GitHub PR #166 is
  MERGED at `81793ac27b70f82c231bf607b55e775d2b905974`; merged tree
  `33a8d2b...` equals the gated source tree. PR acceptance comment posted;
  `descriptive-remote-names` closed and `ditz sync` succeeded. No deploy.

## integration-lead:13

- kind: crystal-conflict
- refs: integration-lead:12
- based_on: `81793ac27b70f82c231bf607b55e775d2b905974`
- package: A167
- result: `git merge-tree --write-tree origin/master d61bdb...` reports
  textual conflicts in `storage/cli_mount.py` and
  `storage/mount_profiles.py`. #167 is held for Ted's scope ruling; no
  rebasing, union gates, review refresh or merge is authorized here.

## integration-lead:14

- kind: state-change
- refs: integration-lead:13
- based_on: `e2d2e127e077e124038f0dc057021f6642c476d8`
- package: performance audit peer
- state: DONE/IDLE. Draft PR #169 contains five ranked proposals and
  boundary tests only; no code changes, gates, acceptance or merge.
  `oq-conflict-no-remote-hash` was filed and synced by that lane. No new
  implementation authority was assigned by this integration lead.

## integration-lead:15

- kind: state-change
- refs: integration-lead:14
- based_on: `81793ac27b70f82c231bf607b55e775d2b905974`
- state: DONE/IDLE. #165 and #166 integrated with exact-head gate,
  council, lead review and scripted merge evidence. #167 held for Ted's
  ruling and a future rebase. Performance audit proposals remain draft.
  The consolidated record is on draft PR #168 for parent review. No new
  implementation package is assigned; no mission-owned process or lease
  remains. Open product questions are in ditz, not solely this handoff.

## integration-lead:16

- kind: state-change
- refs: integration-lead:15
- based_on: `b080975285001d6860e88e0a916b8c6223222f5c`
- state: STARTED bounded ruling/status record update on draft PR #168,
  assigned by parent. No implementation or live-system work is in scope.

## integration-lead:17

- kind: interpretation-filed
- refs: integration-lead:16
- based_on: `81793ac27b70f82c231bf607b55e775d2b905974`
- rulings: Verified in ditz: `oq-dynamic-workers-scope` closed with
  foreground reads/browsing in scope, so #167 background-only is incomplete;
  `oq-conflict-no-remote-hash` closed with bounded download fallback and
  local hash allowed, but bounds/failures still need implementation;
  `oq-streaming-fleet-keep-threshold` commented with fleetwide scope while
  the numeric threshold remains open. No branch acceptance follows from
  these policy decisions.

## integration-lead:18

- kind: state-change
- refs: integration-lead:17
- based_on: `81793ac27b70f82c231bf607b55e775d2b905974`
- state: Parent assigned a separate Sol responsiveness lead in
  `foreground-responsiveness-20261110`; worktree observed at `66c640a`.
  This lane made no sibling edits. Parent also relayed that splinter1's
  installed/running `pq8kn2` rsync, watcher and mount-profile hashes match
  `a7467c6` and differ from `81793ac`; #165/#166 are not rolled out there.
  That host observation was not independently repeated by this lane. PR #170
  remains under onboarding-root incident remediation without waiver or
  acceptance. No merge, deployment or live inspection was performed here.

## integration-lead:19

- kind: state-change
- refs: integration-lead:18
- based_on: `81793ac27b70f82c231bf607b55e775d2b905974`
- state: DONE/IDLE for the bounded roster/ledger update. No new integration
  or implementation assignment is held. Draft PR #168 remains the reviewable
  record; the parent owns future assignments and any acceptance decisions.

## integration-lead:20

- kind: state-change
- refs: integration-lead:19
- based_on: `d6bdb39438d427906802b48d3c27586a0e51b134`
- state: STARTED bounded amendment to the consolidated roster. GitHub PR
  #169 confirms this exact draft head on master `66c640a`; ditz confirms
  `oq-conflict-fallback-limits` is open and blocks
  `gh-74-no-hash-policy`. No implementation, acceptance or merge is assigned.

## integration-lead:21

- kind: state-change
- refs: integration-lead:20
- based_on: `d6bdb39438d427906802b48d3c27586a0e51b134`
- state: Peer performance audit DONE/IDLE after GH #74 amendment. The peer
  reports docs-only diff check and 44 valid links; no tests or live access.
  Its 64 MiB, 60 second and one-active-per-mount limits remain proposals.
  The peer reports a QUEUED receipt to responsiveness lead `%106` for the
  cold-open finding, not acknowledgment or execution. Its send to parent
  `%87` is ambiguous after mixed composer input, so this record does not
  claim that attempt delivered. No source review or merge claim follows.

## integration-lead:22

- kind: state-change
- refs: integration-lead:21
- based_on: `d6bdb39438d427906802b48d3c27586a0e51b134`
- state: DONE/IDLE for this roster amendment. Draft PR #168 is the
  reviewable record; no implementation package or integration action is
  assigned to this lane.

## integration-lead:23

- kind: state-change
- refs: integration-lead:22
- based_on: `2a5490c8d50b436843710fa698d9cd326221b33a`
- state: STARTED bounded BUILD coordination record. Responsiveness lead
  reported adding only `foreground_admission.py` to `storage/BUILD` explicit
  srcs and passing `//:storage_build_srcs_check`. This lane inspected that
  BUILD diff: one added src line, no changed label or tag. GitHub PR #171
  was observed draft/open at `3f982b14e7e901af0823f2b9765e1d82be620f1b`;
  its intervening change from `2a5490c` touched only lane records and left
  BUILD bytes identical. PR #167 remains draft/open at `d61bdb1`. No
  independent gate or review of #171 is claimed.

## integration-lead:24

- kind: state-change
- refs: integration-lead:23
- based_on: `3f982b14e7e901af0823f2b9765e1d82be620f1b`
- state: DONE/IDLE for this roster note. No implementation branch, live
  storage resource, merge or deployment was touched by this lane.
