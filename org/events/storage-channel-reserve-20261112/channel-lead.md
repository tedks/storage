# Channel reserve lead event shard

## channel-lead:1

- kind: state-change
- based_on: `81793ac27b70f82c231bf607b55e775d2b905974`
- applied_at: `cb9e66a8fba8523bd2e0270f4f7499efafaeb41c`
- refs: root `STORAGE-CHANNEL-RESERVE-TRACER-20261112`
- state: Started the isolated package after confirming exact clean worktree,
  reading its AGENTS, packed spec/member-I/O contract and separation review.
  No prior PR #171 scope remained active. Assumed only already threaded mounts
  can exploit concurrent channel reserve. No unrequested scheduling machinery:
  the existing SSH pool and concrete namespace list supply the mechanism.

## channel-lead:2

- kind: state-change
- based_on: `81793ac27b70f82c231bf607b55e775d2b905974`
- applied_at: `cb9e66a8fba8523bd2e0270f4f7499efafaeb41c`
- refs: `channel-lead:1`
- state: Baseline synthetic test RED, first from a listing wait and then,
  after tightening the assertion, from actual bulk lease count 0 instead of
  3 before timeout. Focused repaired run selected 3 tests and passed 3.
  Source/test commit pushed, draft PR #175 opened. RED/GREEN logs and scoped
  resource monitors are in the owned scratch path named in status. This is
  verification of the tracer, not release acceptance.
