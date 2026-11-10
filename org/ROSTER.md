# Storage Orgs adoption roster, 2026-11-10

## Current state

This is an adoption audit of three already implemented PRs, not a
retroactive claim that the full Orgs sprint ran. The integration lead owns
acceptance and integration evidence for #165 and #166. #165 merged at
`38c3bdb9562cade82411751acca058b04c974b5c`. #167 is held for Ted's
foreground-worker scope ruling. No new implementation lane is authorized by
this mission. The sibling performance lane owns read-only triage.

| Lane | Worktree / branch | Hat and state | Next bounded action |
| --- | --- | --- | --- |
| Parent coordinator | `storage:0`, storage coordinator | CTO/root; active | Receive consolidated evidence and resolve rulings |
| Integration lead | `storage:1` pane `%93`, `orgs-integration-20261110`, `chore/orgs-integration-20261110`, Sol | Lead; active | #165 landed; review #166 and coordinate its rebase/gates |
| Performance audit | `perf-triage-20261110`, `docs/perf-triage-20261110`, Sol | Peer lead; active | Read-only triage; reports to parent |
| Prior implementation lanes | #165, #166, #167 worktrees | Implementers; DONE/idle | No additional task assigned; preserve branches |

## Bounded adoption packages

- **A165:** Audit #165 at `64c04d6cde73ca5bd3815ed896ad9e71aa9582dc`, including actual diff, tests, formal council, gate receipt, and base ancestry. Accept only with a clean lead review and exact-head evidence; merge through `scripts/merge-gated.sh` only if every required condition holds. Repair authority: evidence and record work, no code edits in the sibling worktree. Stop at an authentication or review gate that cannot be satisfied.
- **A166:** Same audit for #166 at `84bb8d6210ee03aaec6845c16fe63cd8becd2f0a`. Respect the ruling that optional display names do not change stable backend IDs. Repair and stop authority as A165.
- **A167:** Audit and record #167 at `d61bdb17601aba8918fe552bd456442e4a69b412`, but do not merge or settle foreground-worker behavior. Stop at `oq-dynamic-workers-scope`.

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
interface. Exact
heads and this contract were delivered to the parent coordinator at pane
`%87` with a submitted receipt before any merge. No onboarding code is in
this package.

## Safety and authority

No inspection of live mounted files or backing directories, SSH to live
storage hosts, real profiles, cache, known_hosts, deployment, restart, live
benchmarks, money, keys, installs, or broad cleanup. Only local synthetic
gates and read-only repository inspection are authorized. Normal PR merges
for accepted #165 and #166 are authorized; #167 is not.
