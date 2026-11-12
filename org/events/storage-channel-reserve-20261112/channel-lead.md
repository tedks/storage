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

## channel-lead:3

- kind: state-change
- based_on: `8f1bc3d3a3923ccde1d166d1ffb6dc38ccb12e65`
- applied_at: evidence-only successor to the same source/test tree
- refs: `channel-lead:2`
- state: Clean pushed head ran `//:storage_tests_fast_gate` without test
  filters and with forced test execution. All three Bazel targets passed:
  986 sandboxed tests, 8 resilience tests and build source coverage. Four of
  four authorized focused invocations are spent; scope cgroups verified and
  all owned scopes inactive. Monitored combined scratch/reused cache growth
  stayed below 2 GiB; acquisition bytes unknown. Self-review found only the
  owned three application hunks, one existing test module and own documents.
  No full gate, CLI, council, root acceptance, merge or deploy is claimed.

## channel-lead:4

- kind: review-finding
- based_on: `844684bc02cdd66b7bf4a60deada64acdec6cc19`
- applied_at: pending test-only correction
- refs: `channel-lead:3`, root/warm Sol `channel-reserve-review-844684b.md`
- state: Production three hunks scoped CLEAN, package FINDINGS T1. The old
  four-at-once schedule overconstrained total channel count: the fourth
  opener can hold a prior metadata lease. The test now stages three real
  SFTP get gates, then observes the fourth thread denied by the pool's
  actual bulk capacity decision, without replacing that decision. It allows
  total channels up to the configured cap and attempts every join before
  reporting leaks. This is test evidence correction, not code acceptance.

## channel-lead:5

- kind: state-change
- based_on: `844684bc02cdd66b7bf4a60deada64acdec6cc19`
- applied_at: pending test-only correction
- refs: `channel-lead:4`
- state: Two of three renewed invocations spent. First used a target label
  absent on this baseline and stopped in analysis; no tests ran. Second
  selected one corrected test on `//:storage_tests_sandboxed` after a
  reversible `storage/cache.py` mutation disabling only its bulk context;
  behavioral RED was `bulk_leased=0` rather than 3 before timeout. The
  control source was restored and hashes match the pre-control production
  blobs. Both owned scopes are inactive. One invocation remains for the
  clean exact-head fast gate; no focused GREEN is claimed yet.
