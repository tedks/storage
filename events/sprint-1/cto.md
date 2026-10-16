# Ledger shard: sprint-1 / cto

Append-only. Conventions in `orgs/protocol/templates/event-log.md`. Ids are
`cto:<n>`; `based_on` is the git sha reasoned from.

## cto:1 · 2026-10-16T04:00:00Z · state-change
- actor: CTO (Claude Fable 5.1, Claude Code)
- based_on: 00048dc
- refs: org/ROSTER.md
Sprint 1 sized: one entity, hats collapsed onto the CTO session until the
first lane is briefed. Roster instantiated. Prior work this session (PRs
#95, #97, #99; issues #96, #98; the sitrep of 2026-09-20) predates the
ledger and is recorded on the PRs and in ditz, not replayed here.

## cto:2 · 2026-10-16T04:00:00Z · necessity-challenge
- actor: CTO (Claude Fable 5.1)
- based_on: 00048dc
- refs: WP-074
PROCEED. What fails if we don't: every sync pass downloads the whole file
from each member to compare a hash; observed in production as multi-GB
transfers and timeouts (#74). The backend already exposes
`get_fast_checksum`; the fix is to call it.

## cto:3 · 2026-10-16T04:00:00Z · necessity-challenge
- actor: CTO (Claude Fable 5.1)
- based_on: 00048dc
- refs: WP-098
PROCEED. What fails if we don't: a crash or a rename mid-sync leaves a row
at SYNCING forever, and the monitor only requeues DIRTY, so that write
never reaches its replicas (#98).

## cto:4 · 2026-10-16T04:00:00Z · necessity-challenge
- actor: CTO (Claude Fable 5.1)
- based_on: 00048dc
- refs: WP-REMOTE
PROCEED, held at DRAFT. What fails if we don't: the main use case (SSH
members) has no automated coverage; the open desktop-workload bugs cannot
be reproduced. Needs a CEO ruling on hosts before READY.

## cto:5 · 2026-10-16T04:00:00Z · escalation
- actor: CTO (Claude Fable 5.1)
- based_on: 00048dc
- refs: DESIGN.md, ditz open-questions/flip-repo-to-public, open-questions/real-remote-gate-hosts
NEEDS RULING (CEO): ratify the Goals and non-goals and the sprint-1
schwerpunkt drafted into DESIGN.md; rule on the public flip; rule on
whether drynwyn may be a gate target.
