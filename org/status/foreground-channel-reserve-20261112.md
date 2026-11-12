# Foreground channel reserve: current state

- State: implementation delivered as draft PR #175; focused synthetic tests
  verified, exact-head fast gate pending; scoped review and release acceptance
  pending. No deploy or merge.
- Owner: channel Sol lead; worktree
  `/home/tedks/Projects/storage/foreground-channel-reserve-20261112`, branch
  `fix/foreground-channel-reserve-20261112`.
- Source base: `81793ac27b70f82c231bf607b55e775d2b905974`; code/test
  commit: `cb9e66a8fba8523bd2e0270f4f7499efafaeb41c`.
- PR #171 remains frozen at `7284ad15` docs / `b0c0c4f` application.
  PR #167 is sync-only; neither is edited by this package.
- Runtime budget: 3/4 focused Bazel invocations used, each under an owned
  2 CPU / 4 GiB / swap 0 scope; under 30 minutes cumulative. One invocation
  remains for the exact clean pushed fast gate. Scratch:
  `/tmp/storage-channel-reserve-5Ra6HI`; only the prior owned pinned
  repository cache was reused. No prior PR #171 process or scope was active.
- RED: first baseline run failed because listing waited for a cold copy;
  strengthened same-schedule baseline on `81793ac` plus test blob
  `e7e02f6a` failed on actual `bulk_leased=0` versus expected 3 before
  timeout. GREEN: three selected tests passed on repaired source, 983
  deselected. Wrapper SHA256 `9b978124a64216acb46107d9133c8e885a41b9b5115b19e3e4eb3c5ee0be76e3`.
- Remaining: force `//:storage_tests_fast_gate` on final clean pushed head;
  four full local gates, CLI build, formal council fixpoint, root review,
  PR #170 incident clearance and merge-gated normal merge are later gates.
  Known baseline freshness/namespace defects are unchanged and unaccepted.
- Tracker: `storage-channel-reserve-tracer` (ditz); metadata sync pending.
- Resource state: RED/GREEN scopes inactive after exit; no fixture, socket,
  daemon, mount or lease acquired. New fast-gate scope is not yet started.
