# org/

How this project is run, in the terms of the org doctrine at
`~/Projects/orgs/master` (`doctrine/ROLES.md`, then `doctrine/DOCTRINE.md`,
then the binding for your harness). A cold-started agent reconstructs the
state from committed files:

1. `DESIGN.md`, Goals and non-goals: the commander's intent. Every package
   names which goal it serves.
2. `org/ROSTER.md`: who holds which hat, standing authority, hosts, and the
   current-state header, rewritten in place at every task boundary.
3. `org/packages/<id>.md`: one work package per bounded mission, the brief
   a lane is spawned from (template: `orgs/protocol/templates/work-package.md`).
4. ditz: the backlog, and product questions for the CEO under the
   `open-questions` component.
5. GitHub PRs: the council record for each change, as a comment.

Ledger shards, status entries and leases are added when their triggers
fire (a deviation, a second lane, a resource claim), not before.
