# Claude Review Notes (ask-agent)

Query mechanism:

- `~/.codex/skills/ask-agent/scripts/agent-query.sh claude`
- Working directory: `<worktree>/streaming-read-prefetch`

## Successful response: key risks

Prompt summary: key risks for streaming reads + background prefetch in FUSE.

Claude response:

1. Memory pressure from unbounded prefetch.
2. Prefetch/eviction race.
3. False sequential detection causing cache pollution.
4. FUSE/backend starvation from prefetch I/O.
5. Teardown ordering and use-after-free races on close/unmount.

## Successful response: missing tests

Prompt summary: must-have tests for streaming read + prefetch.

Claude response emphasized:

1. Sequential read correctness against source bytes.
2. Prefetch window advancement behavior.
3. Seek beyond prefetched region with synchronous fallback.
4. Concurrent readers correctness and isolation.
5. Prefetch cancellation on close/seek with leak checks.
6. Back-pressure under constrained memory.
7. Backend error propagation at read boundary.
8. Final short read + EOF correctness for non-aligned sizes.

## Notes

- Longer, multi-part prompts intermittently timed out in this environment.
- Action taken: translated successful risk/test feedback into `docs/streaming_read_prefetch_plan.md` as explicit controls and go/no-go gates.
