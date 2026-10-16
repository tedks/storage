# Roster: storage / sprint 1

Instantiated 2026-10-16 from `orgs/protocol/templates/roster.md`. STATES.md
owner references resolve here: a cold-started agent finds *who* is
authorized for a transition in this file, and *what* transition is next in
`orgs/protocol/STATES.md` plus the ledger under `events/`.

## Current state

Rewritten in place at every task boundary; dated evidence lives in the
ledger, never here.

- **Head:** master @ 00048dc (after #95, #97, #99).
- **Lanes:** none active. The CTO session holds every hat until the first
  lane is briefed.
- **Awaiting the CEO:** ratify the Goals and non-goals and the sprint-1
  schwerpunkt in DESIGN.md; rule on `open-questions/flip-repo-to-public`.
- **Next bounded action:** brief a Lead-tier lane on `WP-074` (packages are
  READY; see `org/packages/`).
- **Provider budget:** agy seat empty until its quota resets (~2026-10-18).
  Anthropic and OpenAI windows: phase not yet recorded.

## Hats

| Hat | Held by (agent/session) | Notes |
|---|---|---|
| CEO | Ted Smith | rulings; product scope; money, keys, outward acts (visibility flip, releases); loosening below doctrine defaults |
| CTO / spec doc owner | Claude Code interactive session, Fable 5.1 (Frontier), worktree `storage/master` | DESIGN.md merges; roster; mission allocation; merges of gated CLEAN work; provider budget |
| Accountable lead — storage | unassigned; CTO acts until a lane is spawned | one entity (the repo); decomposition, worker briefs, diff review at every task boundary, council and lead-review to fixpoint |
| Integration owner | CTO | normal merges only, after council CLEAN and the three Bazel gates on the exact head |
| Escalation owner | CTO | `NEEDS RULING` routes to the CEO through the CTO; everything else the CTO decides and logs |
| Reviewer seats | Claude native subagent at `model: opus`; Codex via `ask-agent codex -m gpt-6-sol`; agy via `ask-agent agy` | one seat per provider, Lead tier; an unavailable seat is recorded empty, never backfilled |

## Standup parameters

- **Heartbeat:** every 45 minutes of active lane work. Material events
  (block, unblock, ruling request, landed head, resource release, control
  failure) get immediate attention; a task report triggers an immediate
  diff review.
- **Blocked threshold:** longer than one heartbeat → convene.
- **Monitor:** the accountable lead for its packages; the CTO across lanes.
- **Forms:** `LANDED #<n> <sha>`; `BLOCKED: <one line>`; `NEEDS RULING:
  <one line>`; `DONE` with remaining gaps named; the standup reply.

## Lane authority and resources

- **Entity and scope:** one entity, this repository. Each package names the
  files it owns; anything outside is a deviation to log or an escalation.
- **Tiers:** Lead = Claude Opus 5.5 / gpt-6-sol; Implementer = Claude
  Sonnet 5 / gpt-6-luna; Utility = Claude Haiku 4.5. Record the model
  actually selected in the status entry. The CTO session is Frontier and
  does not implement beyond a tracer bullet.
- **Worker bound:** at most two concurrent tactical workers per lane; the
  lane reviews each task's diff before briefing the next.
- **Gates:** the three Bazel gates in AGENTS.md, on the exact head to be
  merged, rerun after any rebase. The mounted gate needs `/dev/fuse` and a
  setuid `fusermount`; framework0 is the gate host. Benchmarks are not a
  gate.
- **Review:** council to fixpoint on every non-trivial PR (full diff in
  round 1, fix delta thereafter), then the one-rung-up lead review in a
  fresh context. Trivial PRs (docs, renames) get a one-line "trivial;
  skipping council" note.
- **Repair authority (default per package):** a lane may repair its own
  worktree, Bazel cache, dev-shell entry and test fixtures, and may add
  tests, with an attempt budget of three per obstacle. It may not change
  `flake.nix` inputs, the BUILD gate structure, or anything a real host
  depends on without a ruling.
- **Standing authority:** the CTO merges gated CLEAN work, spawns and
  re-tasks lanes toward the schwerpunkt, and decides everything inside the
  CEO's envelope, flagging each decision in the ledger. Pauses only for
  money, keys, product scope and rulings, and irreversible or outward acts.
- **Hosts and leases:** framework0 runs gates. drynwyn is the only real
  remote member; nothing is pointed at it (mounts, reprobe, rebuild) without
  a CEO ruling, and never at a live cache. No leases outstanding.
- **Artifact paths:** this file; `org/packages/<id>.md` (work packages);
  `status/<id>.md` (one per claimed package, worker-owned); `events/<sprint-id>/<actor-id>.md`
  (ledger shards, append-only, one per actor); open product questions in
  ditz under `open-questions`; handoff shards in the private off-duty repo,
  never here.
