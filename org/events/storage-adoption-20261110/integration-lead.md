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
