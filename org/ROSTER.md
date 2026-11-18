# Storage Orgs adoption roster, 2026-11-10

## Current state (2026-11-18, against master `4989ba7`)

This record is documentation only. It audits the adoption of three PRs that
were implemented before the Orgs workflow was used here (#165, #166, #167),
records how #165 and #166 were accepted and integrated, and lists related
later PRs (not every later PR). It does not claim that the full Orgs sprint
ran (see Inherited process debt). Lane identities, session and pane ids,
host names and host details are kept in a private roster outside this public
repo.

Heads of open drafts are as observed at 2026-11-18 06:03Z; they may have
moved since, so check GitHub before relying on one.

| PR | State on 2026-11-18 | Notes |
| --- | --- | --- |
| #165 | MERGED 2026-11-10 at `38c3bdb` | Accepted at source head `64c04d6` (A165) |
| #166 | MERGED 2026-11-10 at `81793ac` | Accepted at source head `4ca26cd` (A166) |
| #167 | OPEN, draft, head `d61bdb1` (unchanged since 2026-11-09) | Not accepted (A167). Background sync only; `git merge-tree` against `4989ba7` conflicts in `storage/cli_mount.py`, `storage/fs.py`, `storage/mount_profiles.py` |
| #169 | MERGED 2026-11-18 at `ebcbdf0` (second parent `cee0a13`) | Performance and correctness backlog, `docs/performance_backlog_2026-11-10.md` |
| #170 | MERGED 2026-11-10 at `66c640a` | Bazel source/test package split |
| #171 | OPEN, draft, head `01a7edf` | Foreground responsiveness planning; kept as namespace/freshness evidence |
| #175 | MERGED 2026-11-15 at `5f3dbb3` | Reserves an SSH channel for foreground listing during threaded cold copies; supersedes #171's channel tracer |
| #179 | MERGED 2026-11-15 at `1811024` | Rsync upload `--ignore-times` guard; closes `gh-112-rsync-quickcheck` |
| #176 | MERGED 2026-11-18 at `4989ba7` | Rejects substituted SSH mux directory types; touches only `storage/backends/ssh_mux.py` and its test |
| #180 | OPEN, draft, head `edebc55` (under active development) | Slice S1 of #171: opt-in foreground admission cap |

Successor order, as recorded in #180's description: #180 (S1) first; #167
after it, with its retry-safety prerequisite handled separately; #171 stays
draft. #171's own description, which says it will succeed #167, predates
that ordering. Nothing in this record accepts, merges or deploys #167, #171
or #180.

Bazel moved after the 2026-11-10 coordination note below: #173 pinned Bazel
8.7.0 and #174 removed `WORKSPACE` (bzlmod only). Master now uses
rules_python 2.3.2 and per-file test targets in `tests/BUILD`. The
2026-11-10 contract (Bazel 7.6, rules_python 1.6.3) is historical.

Open questions and rulings in ditz, as of 2026-11-18:

- `oq-dynamic-workers-scope`: closed 2026-11-10. Foreground reads and
  browsing are in the responsiveness scope; #167 alone does not cover it.
- `oq-conflict-no-remote-hash`: closed 2026-11-10. Bounded download
  fallback with local hashing is allowed when a member has no remote hash.
- `oq-conflict-fallback-limits`: closed 2026-11-18 (Ted's 2026-11-17
  ruling). The proposed initial bounds were adopted with no user override:
  64 MiB of actual downloaded bytes per member file; 60 seconds total,
  including slot wait, transfer and local hash; one active fallback per
  mount and per member; at most one second waiting for the slot; reads of
  at most 1 MiB. On unknown or oversize input, growth beyond the byte
  budget, timeout, cancellation or failed verification, the member stays
  unverified and its upload is blocked. Implementation
  (`gh-74-no-hash-policy`, GitHub #74) is still unstarted and still
  blocked by `gh-74-fast-checksum`.
- `oq-streaming-fleet-keep-threshold`: OPEN. Fleetwide scope was ruled on
  2026-11-10. On 2026-11-18 Ted chose the form: the greater of a byte floor
  above 8 MiB and about 30% of file size, applied to contiguous reads,
  fleetwide; the open read-ahead window stays coupled to that threshold
  (`oq-streaming-read-ahead-window`), with no independent cap. The exact
  byte floor is still pending. No rollout.

Backlog items from #169, as of 2026-11-18: `gh-112-rsync-quickcheck` is
closed (#179 merged); `streaming-prefix-write-open`,
`field-seq-read-serialized`, `threading-cold-open-pool-share` and
`gh-74-no-hash-policy` are unstarted in ditz. #175 implements the
cold-copy channel reserve that `threading-cold-open-pool-share` proposed,
but that ditz item is still open.

Deployment: this record claims no rollout of any PR to any host.

The sections below are the 2026-11-10 record. Their PR heads and states
are as of that date; the table above is current.

## Lanes (2026-11-10)

Lanes are named by role and by the PR they own; their worktrees and
session identities are in the private roster.

| Lane | PR | Hat and state then | Next bounded action then |
| --- | --- | --- | --- |
| Parent coordinator | - | CTO/root; active | Receive consolidated evidence and resolve rulings |
| Integration lead | #168 | Lead; DONE/IDLE after bounded update | Draft PR #168 awaits parent review; no implementation assignment |
| Responsiveness lead | #171 | Active on draft PR #171; last observed head `3f982b1` | Own foreground reads/browsing design and evidence; may reuse #167 on its own branch; no merge/deploy authority inferred |
| Performance audit | #169 | Peer lead; DONE/IDLE | Draft PR #169 at `d6bdb394`; GH #74 amendment is proposal only |
| #166 repair owner | #166 | Implementer; bounded evidence delivered | No further branch change assigned |
| Other prior implementation lanes | #165, #167 | Implementers; DONE/idle | No additional task assigned; preserve branches |

## Bounded adoption packages

- **A165:** Audit #165 at `64c04d6cde73ca5bd3815ed896ad9e71aa9582dc`, including actual diff, tests, formal council, gate receipt, and base ancestry. Accept only with a clean lead review and exact-head evidence; merge through `scripts/merge-gated.sh` only if every required condition holds. Repair authority: evidence and record work, no code edits in the sibling worktree. Stop at an authentication or review gate that cannot be satisfied.
- **A166:** Audit #166, now rebased from `84bb8d6210ee03aaec6845c16fe63cd8becd2f0a` to `4ca26cd8b1a041a6308bf4b77fb847e3fea033e1`. Respect the ruling that optional display names do not change stable backend IDs. The prior native Sol lane solely owns source-branch repair, exact-head gates and formal council; this lead owns independent acceptance and normal merge. No sibling worktree edits.
- **A167:** Audit and record #167 at `d61bdb17601aba8918fe552bd456442e4a69b412`, but do not merge it. Ted closed `oq-dynamic-workers-scope` with foreground reads and browsing included; the background-only branch does not complete that scope. The separate responsiveness lead owns any reuse on its own branch. After #166 merged, #167 also needs a rebase that resolves `storage/cli_mount.py` and `storage/mount_profiles.py`, renewed gates and review. No edit authority over #167 is assigned to this integration lane.

## Inherited process debt

The prior native Sol implementation dispatch used separate worktrees but did
not run a full Orgs roster, packed contexts, standups, Crystal cadence, or
one-rung-up acceptance review. Their absence is recorded here, not fabricated.
This adoption uses a fixed cross-branch compatibility check before integration
and lead review on frozen heads. Any missing formal council or exact-head gate
remains a real blocker.

## Bazel onboarding coordination (2026-11-10)

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
interface. Exact heads and this contract were sent to the parent
coordinator before any merge (transport details redacted; see
records-lead:2-4). No onboarding code is in this package.

Responsiveness PR #171 added one explicit `storage/BUILD` source entry,
`foreground_admission.py`, at reported pushed head `2a5490c8d50b436843710fa698d9cd326221b33a`.
The lead reports `//:storage_build_srcs_check` passed there. This lane
verified the one-line BUILD diff against base `66c640a`; no label or tag
change appears in that BUILD diff. PR #171 then advanced to `3f982b14`
through a records-only commit, with identical BUILD bytes. This is source
coordination, not a quality-gate, review or acceptance claim for #171. The
responsiveness lead owns the branch; #167 remains open/draft at `d61bdb1`
and was not edited by this lane.

## Safety and authority

No inspection of live mounted files or backing directories, SSH to live
storage hosts, real profiles, cache, known_hosts, deployment, restart, live
benchmarks, money, keys, installs, or broad cleanup. Only local synthetic
gates and read-only repository inspection are authorized. Normal PR merges
for accepted #165 and #166 were authorized and completed; #167 and the new
responsiveness lane have no acceptance, merge or deployment claim here.

## Product rulings and open questions (2026-11-10)

- `oq-dynamic-workers-scope`: CLOSED with Ted's 2026-11-10 ruling: foreground
  reads and browsing are in scope for responsiveness. #167 background sync
  alone does not complete that mission.
- `oq-streaming-fleet-keep-threshold`: Ted chooses the raised
  `streaming_keep_after_close` value. Fleetwide scope was ruled on
  2026-11-10; the numeric threshold remains OPEN and blocks profile edits
  or deployment for this request.
- `oq-conflict-no-remote-hash`: CLOSED with Ted's 2026-11-10 ruling: allow
  bounded download fallback and local hashing when remote hashing is
  unavailable. The implementation must enforce bounds and leave exhaustion
  or failure unverified, never silently no-conflict. GitHub #74 remains
  implementation work.
- `oq-conflict-fallback-limits`: OPEN on 2026-11-10 (closed 2026-11-18; see
  Current state) and synced by the performance audit; blocked
  `gh-74-no-hash-policy`. The proposed 64 MiB byte limit, 60 second
  deadline, and one active fallback per mount were not yet Ted-approved
  values on 2026-11-10.

## Draft performance backlog (2026-11-10)

The peer audit began at pinned `a7467c6` and its GH #74 amendment was pushed
on master `66c640a` as draft PR #169
(`d6bdb39438d427906802b48d3c27586a0e51b134`). The peer reports a
docs-only diff check and 44 valid relative links at that head; no tests,
live access, acceptance or merge are claimed. (#169 later merged at
`ebcbdf0`; see Current state.) Ranked bounded candidates:

1. `streaming-prefix-write-open`: protect dirty sync state when write-open
   encounters a promoted partial prefix. Correctness first; no ruling needed
   if offline reads remain available and unsafe writes fail.
2. `gh-112-rsync-quickcheck`: synthetic same-size, same-mtime upload test and
   targeted rsync fix; the Docker remote gate is essential.
3. `field-seq-read-serialized`: bounded SFTP range pipelining with short-read,
   cancellation and channel-budget tests; no live speedup claim.
4. `threading-cold-open-pool-share`: reserve a metadata channel while full
   cold fetches occupy others; deterministic concurrency test first.
5. GitHub #74 / `gh-74-no-hash-policy`: implement the ruled bounded download
   fallback and local hashing, with explicit bounds and unverified failures.
   Numeric limits and override scope await `oq-conflict-fallback-limits`
   (ruled 2026-11-17, closed 2026-11-18; see Current state).

These are not authorized implementation packages in this lane. #17 scheduler
work waits for #167's implementation interface; the scope ruling is settled.
The full source and boundary test proposals live in the peer audit document,
not this summary. The peer reports sending its cold-open channel finding to
the responsiveness lead; that is not acknowledgment or execution. Delivery
of its message to the parent is unconfirmed and not claimed.

## Handoff (2026-11-10)

The adoption record is on draft PR #168. #165 and #166 are integrated,
not deployed. #167 remains a draft, incomplete for the ruled foreground
scope, and has a known textual merge conflict with current master. The
performance backlog is draft PR #169 at `d6bdb394`. The parent assigned the new
responsiveness lead; this integration lane has no implementation assignment.
PR #170's incident remains under the onboarding root's remediation, with no
waiver or acceptance from this lane.

The parent relayed a deployment observation for one field host: #165 and
#166 were not rolled out there. This lane did not inspect the host, and the
details are in the private roster. No rollout was performed. No
mission-owned process, mount, remote host session, or cache lease is
retained.
