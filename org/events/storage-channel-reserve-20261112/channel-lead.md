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

## channel-lead:6

- kind: state-change
- based_on: `844684bc02cdd66b7bf4a60deada64acdec6cc19`
- applied_at: `4fe8fb14d715ba080cb756bcd58c8ff3dd053ec0`
- refs: `channel-lead:5`
- state: Test-only correction pushed; production blobs remain byte-identical
  to `cb9e66a`. Unfiltered `//:storage_tests_fast_gate` was forced on clean
  pushed `4fe8fb1`, executed and passed 986 sandboxed plus 8 resilience tests
  and build source check, including corrected cold-open schedule. All three
  renewed invocations spent; owned control/control2/fast2 scopes inactive.
  Control RED and GREEN use the same test blob; the reversible control only
  disabled the copy's `bulk_request`, then restored production bytes. T1
  evidence is ready for root delta review, not formal council, full gates,
  CLI, merge or deployment.

## channel-lead:7

- kind: lead-review
- based_on: `a006e6b97f9bf9be3a0a82480b172b908c8d074c`
- applied_at: pending rebased checkpoint
- refs: `channel-lead:6`, root/warm Sol `channel-reserve-t1-review-a006e6b.md`
- state: T1 test delta scoped CLEAN; three production hunks unchanged. This
  closes the schedule finding only. Nit disposition: extend the gated copy's
  test-only safety wait from 5 to 30 seconds so it cannot expire before the
  sequential readiness and browser bounds; finally still releases the gate.
  Published master remains `66c640a`; branch rebase from accepted `81793ac`
  gives a five-commit range-diff with equal patches and no conflict. New-base
  fast gate, CLI and formal council are pending; PR170 incident and full
  release gates remain open.

## channel-lead:8

- kind: review-finding
- based_on: `50418327de1af6822955d6d72b138753c591ec34`
- applied_at: pending council fix delta
- refs: `channel-lead:7`, PR #175 formal round `50418327de1a-20261112T040721Z-589e74`
- state: Frozen formal round received OpenAI native gpt-6-sol CLEAN;
  Anthropic Opus FINDINGS (one Important: bulk timeout on a foreground cold
  open lacked contract/test evidence; four Nits: stale base/gate wording,
  eager-list error path wording, private pool test coupling, deadline relation).
  Google seat EMPTY at usage limit; no substitute. This is not council
  convergence. In-scope test/contract repairs are underway, preserving the
  three production blobs. New-base fast gate ran 40 targets, 39 passed and
  unchanged SSH mux test failed; CLI build passed. The gate failure is filed
  as `pr175-base-ssh-mux-fastgate`, not waived or repaired outside scope.

## channel-lead:9

- kind: review-finding
- based_on: `16fa46dda7e608a22f2c4eb9d3576cf0a0977d94`
- applied_at: pending causal-proof checkpoint
- refs: `channel-lead:8`, PR #175 formal round 2, root/warm Sol disposition
- state: Round 2 remains FINDINGS: Anthropic requested positive proof that a
  healthy existing file reached bulk-capacity denial, plus a same-test
  disabled-classification negative control. Google was EMPTY. The narrow
  test now observes a successful stat and invokes the original pool decision
  unchanged, then reads exact bytes through the spare interactive channel
  while three holders remain. Contract names current ENOENT as observed and
  unruled; eager-listing error prose was corrected. No production edit or
  runtime outcome is claimed by this event. Three-invocation evidence budget
  starts at zero; the shared mux flock is mandatory.

## channel-lead:10

- kind: state-change
- based_on: `9c3e2aa8e0c179c9543e80d1bfe50aeb3adc19d9`
- applied_at: runtime-blocked checkpoint; final receipts on PR #175
- refs: `channel-lead:9`, `pr175-bulk-timeout-proof`, PR #175 comment 6814629125
- state: The corrected test and two documentation nits are pushed without
  production changes. Two attempts waited 60 seconds each on the required
  mux flock; both timed out before `systemd-run` or Bazel. An exact-path
  read-only holder check found peer Bazel Java PID 3482627 from the isolated
  mux worktree retaining the lock descriptor. No peer process was stopped or
  lock bypassed. The temporary disabled-classification cache edit was
  restored byte-for-byte to blob `d0cd68b`; causal RED/GREEN and whole-module
  evidence remain UNRUN, 0/3 invocations spent. Owned causalred scope is
  inactive; no owned Bazel client, fixture or lease remains. Formal council
  remains FINDINGS, and observed ENOENT remains an open product question.

## channel-lead:11

- kind: state-change
- based_on: `7a5c3db38ad69525509630c4509a9536af1fbd89`
- applied_at: opt-in timeout design checkpoint
- refs: `channel-lead:10`, PR #175 comment 6814714217, Ted's timeout ruling
- state: The mux owner released the inherited lock holder; our shared-lock
  control failed behaviorally before timeout, and the clean SSH backend
  target executed 117/117 pass. Both owned scopes ended; 2/3 causal-proof
  invocations were used. Ted then selected an opt-in per-mount capacity wait
  timeout, default off, returning EAGAIN on expiry. The proposed path is
  CLI/profile resolution -> SSH member factory/backend -> pool capacity
  wait -> distinct expiry -> cache pass-through -> FUSE open EAGAIN. Existing
  connection and operation timeouts remain bounded. This is a proposed
  implementation boundary, not a test or acceptance of new code; the new
  six-invocation nonmount budget starts at zero. Physical unmount is outside
  this phase and requires mounted gate evidence later.
