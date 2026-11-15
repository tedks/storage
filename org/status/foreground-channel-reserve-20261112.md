# Foreground channel reserve: current state

- 2026-11-15 formal fix-delta council at pushed `2b1d1eb`: OpenAI CLEAN
  (0/0/0), Anthropic no Critical/Important with four Nits, Google EMPTY
  (usage limit). Anthropic I1-I4 were re-examined; I2's replica-loop premise
  was withdrawn. The exact no-args all-four gate ran 45/45 at `2b1d1eb`
  with matching tree receipt, scope inactive/dead. Nits N-c/N-d led to a
  further test-only delta: discard channel closure is asserted, native
  init/destroy probe exclusion is tested, and threaded scratch `run_fuse`
  exercises the matching copy class. N-a/N-b have specific open follow-ups
  `pr175-conflict-suffix-capacity-race` and
  `ssh-pool-connect-deadline-after-capacity-wait`; neither is claimed as an
  accepted behavior change. The test-only delta and final exact head remain
  UNTESTED/UNPUSHED at this line; fresh gates, CLI and council are required.
- 2026-11-15 threaded-mode repair checkpoint: code/test head `a3d5223`
  passed all six affected nonmount/mounted synthetic targets in owned
  `storage175-finish-affected5-20261115.scope` (34 seconds, 2 CPU/8 GiB/2 GiB
  swap/1024 tasks; inactive/dead afterward). A temporary one-line source
  control made the whole-file bulk scope unconditional; the real CLI profile
  test failed on three false-mode observations (`True` expected, `False`
  observed) while the threaded case passed. The committed cache source blob
  `d8269572d10f83992f13efc7f90f935d4cdccb25` was restored and matched
  HEAD afterward. Full all-four gates, CLI build and formal fix-delta council
  are PENDING on the eventual final head. PR remains draft and unaccepted.
- 2026-11-15 formal review fix delta in progress: no-args all four gates
  passed 45/45 targets and CLI build passed on pushed `5f45cc6` (tree
  `eeac92b5545be78b203d7284ade7cf4ec9bd1622`); exact commands, wrapper
  settings, fixture IDs and limits are PR #175 comment 6900303138. Formal
  round `5f45cc660a92-20261115T195127Z-e81f3b` recorded Anthropic
  4 Important/7 Nits, OpenAI 0 Important/1 Nit, Google EMPTY (usage limit),
  with two older Anthropic findings re-examined. Root adjudicated I1:
  whole-file copy reclassification is enabled only by resolved
  `fuse_threads=True`, preserving the default single-threaded incoming
  class. Narrow source/tests/contract repairs for that boundary and an
  error-path lease wake test are in the worktree but UNTESTED/UNPUSHED at
  this line. I2/I3 are being checked against the unique cache_file/open
  call path; I4 against pool notify/lock order. No new head is yet attested
  by gates, CLI or council. PR stays draft and unaccepted; root owns
  independent acceptance/normal merge/deployment. PR171/#167 untouched.
- 2026-11-15 release validation checkpoint: tested pushed code head
  `2f273d582656a25f741daa19c4826d09d1af7169` is based on published
  master `181102496b37240d5cef4496e21b48c46359037f`.
  The corrected namespace retry test passed its focused run. Exact-head,
  no-args `scripts/run-gates.sh` then executed all 45 Bazel targets with
  45 passes, including the mounted, resilience and remote gates; receipt
  `.gates/2f273d582656a25f741daa19c4826d09d1af7169.json` records tree
  `52298a92cdbc09a6a84f200a195f86f8bca319a8`. The second run's
  three capped synthetic containers and exact network are absent by Docker
  inspect; its client scope is inactive/dead, pids.peak 318 with no cap
  event, sampled named scratch peaked 1,210 MiB. The preceding failed gate
  and monitor abort are retained below as distinct evidence. This status
  commit will advance HEAD, so these gates do not attest the new head:
  rerun exact-head gates, CLI and formal council after pushing. PR #175
  remains draft, unaccepted, unmerged and undeployed. PR #171/#167 are
  untouched; root owns independent review, merge and deployment.
- 2026-11-15 historical first gate failure: exact no-args all-four run on `0040e0f`
  executed 45 targets; 44 passed, including remote and mounted gates. The
  one failure was `test_mkdir_retry_eventually_replicates`: the asynchronous
  retry completed before its immediate absence assertion. The test now gates
  the real second `mkdir` attempt, asserts absence before release, then
  confirms eventual replication and exactly two calls. Focused proof and
  rerun on the new committed head are PENDING; no gate receipt exists for
  `0040e0f`. Owned Docker fixture ID trio and network were verified absent.
  Gate scope exited cleanly after 402 seconds, sampled named scratch peaked
  1,108 MiB, pids.peak 325 with no cap event. Cumulative bounded workload
  is 459/3600 seconds. No mount, container, client or lock holder remains.
  This test-only bend is necessary to make the required full gate reliable;
  rollback is this one test change.
- 2026-11-15 current finish mission: rebased the sole PR #175 branch onto
  fetched published master `181102496b37240d5cef4496e21b48c46359037f`.
  Seventeen old commits replayed; range-diff shows fifteen equal patches and
  only two `fs.py` import-context changes, manually combining the inherited
  rsync import with this package's pool imports. The six new-base affected
  targets executed/pass on source head `683a0ec`: 241 pytest cases, including
  the real caller/session cancellation controls. The first attempt was
  monitor-aborted at 25.3 seconds because the owned scratch scanner omitted
  the existing `_ProbeOperations` FUSE mount source. Its exact connection 64
  was aborted only after matching `/proc` fd ownership; owned PID 3592804 and
  scope exited. The corrected monitor allowed that source, then the second
  attempt passed in 30.8 seconds. Total counted workload 57/3600 seconds;
  named scratch peaked 463 MiB, pids.peak 296, pids.events max 0, no second
  abort, all owned scopes inactive and no mount remains. Docker endpoint is
  local Unix and exact cached synthetic image ID `sha256:0f7e761...` exists
  (95,680,856 bytes); the owned shim forbids builds and caps each fixture
  container. The prior old-base 238-case receipt is PR comment 6865400265.
  Exact-head all four gates and CLI, formal council fixpoint and independent
  root review remain PENDING. The prose below is historical. PR remains
  draft, unaccepted and undeployed; PR #171 and #167 are untouched. The
  current flake pins Bazel 8 through merged master.
- Cancellation evidence package (2026-11-13): root narrowed the test to
  actual caller interruption and isolated daemon session exit; lazy detach
  alone is not cancellation. Two of four new Bazel invocations used under the
  shared lock and owned 2 CPU/4 GiB/swap 0 scopes. Frozen `55065aa` mounted
  positive plus disabled-probe control passed. A focused source test then
  passed real caller SIGKILL and isolated daemon SIGTERM schedules: native
  request/session reason observed, callback finished before holder release,
  daemon `destroy()` followed callback while holders stayed gated. That test
  source is not yet committed or run as a full affected module. Cleanup now
  records exact `fusermount -u`/`-uz` attempts, checks mount/process exit and
  retains scratch on failure. Both scopes exited; no owned mount remains;
  monitor growth +65,536 and +159,744 bytes, no abort, >150 GiB free.
  Final affected modules on a clean pushed head, all four gates, CLI and
  council/root acceptance remain pending. PR #175 is draft and unaccepted.
- Latest cancellation checkpoint (2026-11-13): root narrowed the unmount
  blocker to callback lifetime cancellation. A draft libfuse2 callback probe
  now lets pool waits detect a real request interrupt or session exit and
  report `EINTR` without changing the default unlimited capacity wait.
  Native `fuse_interrupted`/`fuse_exited` symbols were verified in the pinned
  libfuse2; fusepy 3.0.1 enters the new thread-local scope only from its
  native callback wrapper. Direct synthetic FS calls and copied background
  contexts do not enter this native path. Code remains unaccepted.
- Exact new runtime: all 6/6 option-package invocations spent, under the
  same shared flock and owned 2 CPU/4 GiB/swap 0 scopes. Third run passed
  `storage_test_ssh_backend`, `storage_test_allow_other` and
  `storage_test_mount_sigpipe`; the new pool cancellation control passed.
  Fourth mounted run failed at collection because this target does not list
  `tests/fuse_mount.py`; it ran no mounted test. The fixture was made
  self-contained without a BUILD edit. Fifth mounted run executed two cases:
  the disabled-probe control passed; plain mount detach did **not** reach
  `destroy()` with holders held (1 fail/1 pass). Sixth mounted run used an
  explicit native `fuse_exit()` from a second real callback: the positive
  session-exit case passed while all three holders remained gated; the
  disabled-probe control failed at an early assertion because the triggering
  stat itself stayed blocked. That assertion was corrected after the budget
  and is UNRUN. No caller-interruption case or full mounted suite ran.
  All six scopes exited; the latest monitored growth was +86,016 bytes,
  no abort, >150 GiB free. No mount, fixture, Bazel client or owned lock
  holder remains. Network bytes unobserved. Wrapper SHA256 remains
  `8b44672260c56fe5bfd0d8dc79fe3866db261d2d388f9ac9e0f3eda21e605ce5`.
- Remaining gate: plain/lazy unmount did not expose a session exit to the
  callback in the tested schedule. Session-exit cancellation is narrower
  than a general unmount guarantee; request interruption, corrected negative
  control, exact-head affected suites, all four gates, CLI build, council
  fixpoint and root review remain pending. PR #175 stays draft. Root owns
  the next disposition; no runtime or broader lifecycle change is inferred.

## Earlier option checkpoint

- Prior option-package checkpoint (2026-11-13): draft implementation and
  focused tests are green but **BLOCKED for review** at the mounted unmount
  boundary. Ted's ruled default waits for capacity to drain; an opt-in finite
  timeout maps an existing cold file's capacity expiry to `EAGAIN`.
  `storage/backends/ssh_pool.py` waits indefinitely by default and pool close
  wakes it. `storage/fs.py:mount_storage` documents that libfuse joins workers
  before `destroy()` closes the pool, so the synthetic close test cannot prove
  an unmount finishes when a holder keeps a lease. Root `%91` received the
  blocker; `pr175-default-capacity-unmount-cancel` tracks it. The subsequent
  root adjudication authorized the narrow callback repair above. PR #175 remains draft,
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
