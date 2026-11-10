# Foreground responsiveness mission

- State: STARTED; current task: trace foreground, cache, SSH, and sync admission on published `66c640a` before decomposition.
- Owner: Sol/OpenAI lead, tmux pane `%106`, worktree `foreground-responsiveness-20261110`, branch `feat/foreground-responsiveness-20261110`.
- Last commit: `66c640a`; budget burned: initial orientation; blocked on: none.
- Acceptance: scratch-only deterministic regression evidence, bounded active I/O, foreground progress under saturated bulk work, cancellation and shutdown, exact pushed-head four gates and CLI build, formal council, independent lead review; root alone accepts or merges.
- Repair authority: source, tests, docs and bounded prerequisites in this branch; no live mount, host, profile, deployment or sibling-branch edits. Escalate product scope, live-data need, keys/money, third repeated failure, or contract collision.
- Assumptions: the published base is the correct integration point; resource bounds concern active operations and channel use, not fusepy native thread count; read and browsing work must receive admission when bulk work is saturated.
- Failure modes to test: interactive channels exhausted by cold full-file opens, unbounded native FUSE threads, stale or lost queued requests, cap overshoot, starvation, cancellation leaks, shutdown deadlock, and write/byte semantic regressions.

