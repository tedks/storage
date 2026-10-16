# org/ — this repository as an office

The storage project runs on the org doctrine in `~/Projects/orgs/master`
(`doctrine/ROLES.md` first, then `doctrine/DOCTRINE.md`, then
`bindings/claude-code.md` or `bindings/codex-cli.md`). Durable state is files
in git; a cold-started agent on any harness reconstructs the current state
from these files alone:

1. `DESIGN.md` — the commander's intent: goals, non-goals and the current
   schwerpunkt. Every package names which part it serves.
2. `org/ROSTER.md` — who holds which hat, lane authority, resources, and
   the current-state header (rewritten in place at every task boundary).
3. `org/packages/<id>.md` — work packages, one per bounded mission, in the
   states of `orgs/protocol/STATES.md`.
4. `status/<id>.md` — one status entry per claimed package, owned by its
   worker, overwritten in place.
5. `events/<sprint-id>/<actor-id>.md` — the ledger: append-only shards,
   one per actor; the union is the sprint's memory. Projections (review
   findings, the roster header) cite ledger ids and are never a second
   source of truth.
6. ditz, component `open-questions` — product questions for the CEO, with
   options, a recommendation, who decides and what each blocks.

Handoff shards are private (the off-duty repo), never here. GitHub issues
hold problem statements and design discussion; PRs carry the council
record as a comment, in the form used on #97 and #99.

The gates, environment and repo-specific rules stay in `AGENTS.md`.
