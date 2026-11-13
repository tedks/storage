# Foreground channel reserve: current state

- Current option-package checkpoint (2026-11-13): draft implementation and
  focused tests are green but **BLOCKED for review** at the mounted unmount
  boundary. Ted's ruled default waits for capacity to drain; an opt-in finite
  timeout maps an existing cold file's capacity expiry to `EAGAIN`.
  `storage/backends/ssh_pool.py` waits indefinitely by default and pool close
  wakes it. `storage/fs.py:mount_storage` documents that libfuse joins workers
  before `destroy()` closes the pool, so the synthetic close test cannot prove
  an unmount finishes when a holder keeps a lease. Root `%91` received the
  blocker; `pr175-default-capacity-unmount-cancel` tracks it. No new runtime
  or application repair starts before adjudication. PR #175 remains draft,
  with no mounted/full gates, CLI build, council convergence, acceptance,
  merge or deployment on this option delta.
- Option package resources/evidence: 2/6 focused nonmount invocations used.
  First actual run: 2 old tests failed because they assumed the former
  implicit timeout; 118/120 SSH passed, profile and CLI targets passed.
  Second after explicit opt-in correction: SSH 120/120, profiles 59/59, CLI
  32/32 passed. Commands used one batch Bazel client via offline Nix under
  the shared mux flock and owned 2 CPU/4 GiB/swap 0 scopes. Scope IDs
  `storage-channel-option-focus` and `storage-channel-option-focus2` exited;
  second monitor reports +65,536 bytes owned growth, no abort, >150 GiB free.
  Wrapper SHA256 `8b44672260c56fe5bfd0d8dc79fe3866db261d2d388f9ac9e0f3eda21e605ce5`.
  Logs: `/tmp/storage-channel-reserve-5Ra6HI/optionfocus{,2}.log` and
  matching `-monitor.log`. Network bytes unobserved; no acquisition was
  reported. No peer scope or fixture was touched.

- State: draft PR #175, source head `7a5c3db` before the new option package.
  Its causal timeout control failed as intended and the clean SSH backend
  module passed 117/117; exact receipts are on PR #175 comment 6814714217.
  Formal council remains FINDINGS, Google EMPTY. Ted ruled that bulk capacity
  expiry is a mount option, default off, with opt-in expiry returning EAGAIN.
  This checkpoint records the concrete CLI/profile -> SSH member -> pool ->
  cache -> FUSE open path before implementation. No new code outcome is
  claimed. PR #171 remains frozen; no merge or deploy.
- Owner: channel Sol lead; worktree
  `/home/tedks/Projects/storage/foreground-channel-reserve-20261112`, branch
  `fix/foreground-channel-reserve-20261112`.
- Source base: `81793ac27b70f82c231bf607b55e775d2b905974`; code/test
  commit: `cb9e66a8fba8523bd2e0270f4f7499efafaeb41c`.
- PR #171 remains frozen at `7284ad15` docs / `b0c0c4f` application.
  PR #167 is sync-only; neither is edited by this package.
- Runtime budget: 4/4 focused Bazel invocations used, each under an owned
  2 CPU / 4 GiB / swap 0 scope; about 105 seconds total workload, under the
  30-minute limit. No invocation remains. Scratch:
  `/tmp/storage-channel-reserve-5Ra6HI`; only the prior owned pinned
  repository cache was reused. No prior PR #171 process or scope was active.
- Causal-proof budget: 2/3 spent; unused third slot did not carry forward.
  The peer lock was released by its owner, and our RED/GREEN scopes exited.
- New option-package budget: 0/6 focused nonmount invocations spent, up to
  30 minutes total. Own batch base and shared mux flock remain mandatory;
  no full gates, Docker, FUSE mount or live data in this phase.
- T1 repair budget: 3/3 new invocations spent. First stopped at Bazel analysis
  because this baseline has no `//tests:storage_test_ssh_backend` target;
  it ran no test. Second ran the corrected same-schedule test with only the
  `bulk_request` use disabled in `storage/cache.py`: one selected test failed
  on actual `bulk_leased=0` versus 3 before timeout (985 deselected). The
  owned source control was restored byte-for-byte. Third forced the unfiltered
  `//:storage_tests_fast_gate` on clean pushed `4fe8fb1`: all three Bazel
  targets passed, 986 sandboxed tests, 8 resilience tests and build source
  check. This includes focused GREEN for the corrected schedule, but there
  was no separate focused GREEN invocation.
- RED: first baseline run failed because listing waited for a cold copy;
  strengthened same-schedule baseline on `81793ac` plus test blob
  `e7e02f6a` failed on actual `bulk_leased=0` versus expected 3 before
  timeout. GREEN: three selected tests passed on repaired source, 983
  deselected. Wrapper SHA256 `9b978124a64216acb46107d9133c8e885a41b9b5115b19e3e4eb3c5ee0be76e3`.
- Fast gate: `//:storage_tests_fast_gate` with `--nocache_test_results`,
  no test selection, executed on clean pushed `8f1bc3d` (tree
  `9a6a8d731ffc565e54f4c1cbf92b4717c4d9b511`): three Bazel targets
  passed; 986 sandboxed tests, 8 resilience tests, build source check.
  The later evidence-only commit does not alter application or test blobs.
  All logs are under the owned scratch directory; final monitored scratch
  plus reused repository cache was 755,671,040 bytes, about 665 MiB above
  the first baseline, below the 2 GiB abort threshold. No download was
  reported in these logs; network byte count is unknown.
- Remaining: unchanged SSH mux fast-gate failure, four full local gates,
  formal council fixpoint, root review, PR #170 incident clearance and
  merge-gated normal merge are later gates. Ted has not ruled on the observed
  ENOENT mapping for bulk-wait expiry; `oq-pr175-bulk-timeout-errno` is open.
  Known baseline freshness/namespace defects are unchanged and unaccepted.
- Tracker: `storage-channel-reserve-tracer` (ditz); metadata sync succeeded.
- Resource state: RED, RED2, GREEN and FAST scopes inactive after exit;
  no Bazel client, fixture, socket, daemon, mount or lease remains.
- T1 receipts: test blob `4893a78c313d9c4693539d3fb894c0e3f04c3cf2`;
  control cache SHA256 `f1474a6a02942d680ccbeec6de42b880a712c645dc9b5a53abb06b3023d8aa34`,
  restored production cache blob `d0cd68b72bc3a5492d22d93d6ad1e0cc1dc6bce0`;
  control log SHA256 `c7577531f25e6af793dbe2fbda8d165f2bd92287185b291b3f2955ae18987906`,
  fast log SHA256 `ef296993cc6f2c823c42e81a7a20f82d1ba7f72a0b2f21209116212f78449aac`,
  unchanged wrapper SHA256 `9b978124a64216acb46107d9133c8e885a41b9b5115b19e3e4eb3c5ee0be76e3`.
  Both control scopes and fast scope inactive; final monitored owned scratch
  plus reused cache 775,176,192 bytes (up 19,505,152 bytes from this
  repair's first baseline), under +2 GiB. No download reported; bytes
  unknown. `nix develop --offline`, batch Bazel and 2 CPU/4 GiB/swap 0
  systemd scopes were used. No live or remote fixture ran.
