# Storage Orgs adoption roster, 2026-11-10

## Current state

This is an adoption audit of three already implemented PRs, not a
retroactive claim that the full Orgs sprint ran. The integration lead owns
acceptance and integration evidence for #165 and #166. #165 merged at
`38c3bdb9562cade82411751acca058b04c974b5c`; #166 merged at
`81793ac27b70f82c231bf607b55e775d2b905974`. #167 is held for Ted's
foreground-worker scope ruling. No new implementation lane is authorized by
this mission. The sibling performance audit completed a draft proposal report.

| Lane | Worktree / branch | Hat and state | Next bounded action |
| --- | --- | --- | --- |
| Parent coordinator | `storage:0`, storage coordinator | CTO/root; active | Receive consolidated evidence and resolve rulings |
| Integration lead | `storage:1` pane `%93`, `orgs-integration-20261110`, `chore/orgs-integration-20261110`, Sol | Lead; DONE/IDLE after handoff | Draft record PR #168 awaits parent review; no new implementation mission |
| Performance audit | `storage:2` pane `%94`, `perf-triage-20261110`, `docs/perf-triage-20261110`, Sol | Peer lead; DONE/IDLE | Draft PR #169 at `e2d2e127`; no implementation assignment |
| #166 repair owner | `descriptive-remote-names`, `feat/descriptive-remote-names`, native Sol | Implementer; bounded evidence delivered | No further branch change assigned |
| Other prior implementation lanes | #165 and #167 worktrees | Implementers; DONE/idle | No additional task assigned; preserve branches |

## Bounded adoption packages

- **A165:** Audit #165 at `64c04d6cde73ca5bd3815ed896ad9e71aa9582dc`, including actual diff, tests, formal council, gate receipt, and base ancestry. Accept only with a clean lead review and exact-head evidence; merge through `scripts/merge-gated.sh` only if every required condition holds. Repair authority: evidence and record work, no code edits in the sibling worktree. Stop at an authentication or review gate that cannot be satisfied.
- **A166:** Audit #166, now rebased from `84bb8d6210ee03aaec6845c16fe63cd8becd2f0a` to `4ca26cd8b1a041a6308bf4b77fb847e3fea033e1`. Respect the ruling that optional display names do not change stable backend IDs. The prior native Sol lane solely owns source-branch repair, exact-head gates and formal council; this lead owns independent acceptance and normal merge. No sibling worktree edits.
- **A167:** Audit and record #167 at `d61bdb17601aba8918fe552bd456442e4a69b412`, but do not merge or settle foreground-worker behavior. Stop at `oq-dynamic-workers-scope`. After #166 merged, #167 needs a rebase that resolves `storage/cli_mount.py` and `storage/mount_profiles.py`, renewed gates and review. No repair owner assigned.

## Inherited process debt

The prior native Sol implementation dispatch used separate worktrees but did
not run a full Orgs roster, packed contexts, standups, Crystal cadence, or
one-rung-up acceptance review. Their absence is recorded here, not fabricated.
This adoption uses a fixed cross-branch compatibility check before integration
and lead review on frozen heads. Any missing formal council or exact-head gate
remains a real blocker.

## Bazel onboarding coordination

Ted authorized a separate project-by-project package-graph harmonization
mission. This lane is the local integration contact, not the implementer.
The published compatibility contract is storage module `0.1.0`, Bazel 7.6,
Python 3.11, rules_python 1.6.3; preserve `//:storage_lib`, `//:storage_cli`,
the current gate labels, explicit source coverage, local FUSE/Docker tags,
and the manual remote all-gates target. `//...` does not cover that target.
Heads #165/#166/#167 do not edit `BUILD`, `MODULE.bazel`, or `flake.nix`;
#166 and #167 change test files that any package split must include. The
performance audit lane proposes further test additions in streaming, remote,
sync, cache, rsync, namespace and conflict suites; any new top-level module
requires manifest coordination. #17 scheduler design waits for #167's
interface. Exact heads and this contract were delivered to the parent coordinator at pane
`%87` with a submitted receipt before any merge. No onboarding code is in
this package.

## Safety and authority

No inspection of live mounted files or backing directories, SSH to live
storage hosts, real profiles, cache, known_hosts, deployment, restart, live
benchmarks, money, keys, installs, or broad cleanup. Only local synthetic
gates and read-only repository inspection are authorized. Normal PR merges
for accepted #165 and #166 are authorized; #167 is not.

## Open product questions

- `oq-dynamic-workers-scope`: Ted decides whether #167's background-only
  scaling satisfies the request or foreground work is part of the milestone.
  #167 is held.
- `oq-streaming-fleet-keep-threshold`: Ted chooses the raised
  `streaming_keep_after_close` value and splinter1-only versus fleet scope.
  This blocks profile edits/deployment, not the synthetic PR reviews. Filed
  and synchronized to `ditz-metadata` on 2026-11-10.
- `oq-conflict-no-remote-hash`: Ted chooses fail-closed versus bounded
  download when a member lacks a remote hash. Filed/synced by the performance
  audit; blocks the no-download pre-sync conflict package.

## Draft performance backlog

The peer audit is a proposal at pinned `a7467c6`, pushed as draft PR #169
(`e2d2e127e077e124038f0dc057021f6642c476d8`). It makes no test or
acceptance claim. Ranked bounded candidates:

1. `streaming-prefix-write-open`: protect dirty sync state when write-open
   encounters a promoted partial prefix. Correctness first; no ruling needed
   if offline reads remain available and unsafe writes fail.
2. `gh-112-rsync-quickcheck`: synthetic same-size, same-mtime upload test and
   targeted rsync fix; the Docker remote gate is essential.
3. `field-seq-read-serialized`: bounded SFTP range pipelining with short-read,
   cancellation and channel-budget tests; no live speedup claim.
4. `threading-cold-open-pool-share`: reserve a metadata channel while full
   cold fetches occupy others; deterministic concurrency test first.
5. GitHub #74 / `gh-74-no-hash-policy`: remove full remote conflict-check
   downloads only after `oq-conflict-no-remote-hash` is ruled.

These are not authorized implementation packages in this lane. #17 scheduler
work waits for #167's interface and scope ruling. The full source and boundary
test proposals live in the peer audit document, not this summary.

## Handoff

The adoption record is on draft PR #168. #165 and #166 are integrated,
not deployed. #167 remains a draft held for `oq-dynamic-workers-scope` and
has a known textual merge conflict with current master. The performance
backlog is draft PR #169. Parent coordinator owns any new lane assignment;
the integration and performance audit lanes are idle. No mission-owned
process, mount, remote host session, or cache lease is retained.
