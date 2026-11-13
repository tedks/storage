# Local write sync events

- id: lead-sol:1
  kind: state-change
  refs: []
  based_on: 66c640aec005feccd0bd585f3355295c3bb772ba
  state: STARTED
  note: Claimed the local write sync diagnostic package; assumptions, scope, runtime bound, and stop conditions are in the status entry.

- id: lead-sol:2
  kind: state-change
  refs: [lead-sol:1]
  based_on: 66c640aec005feccd0bd585f3355295c3bb772ba
  state: BLOCKED
  note: Focused Bazel invocation 1/4 stopped in rules_python analysis before tests because cache-only inputs lacked PyPI simple metadata. Owned scope is inactive/dead; 133 MiB scratch. Root was sent this blocker with a submitted receipt on pane %91. No field attribution or test result is claimed.

- id: lead-sol:3
  kind: state-change
  refs: [lead-sol:2]
  based_on: fd8de3b
  state: STARTED
  note: Root authorized same mission to resume with exactly three remaining focused Bazel invocations. First may resolve unchanged locked dependencies within a ten-minute, 2-CPU/4-GiB/no-swap, 2-GiB-owned-scratch envelope; subsequent runs return to cache-only. Tightened test to use the real no-baseline conflict guard and to separate a real-rsync legacy skip control from the desired fixed behavior.

- id: lead-sol:4
  kind: state-change
  refs: [lead-sol:3]
  based_on: fd8de3b
  state: STARTED
  note: Scoped Bazel invocation 2/4 resolved unchanged locked dependencies in 24 seconds; active scope showed CPUQuota=2s, MemoryMax=4294967296, MemorySwapMax=0. Peak sampled owned scratch 336 MiB, post-run 453 MiB; host free exceeded 10 GiB; scope ended inactive/dead. Shared content-addressable repository cache path was used, not peer output bases; network bytes UNKNOWN. Direct real-rsync tests failed on stale destination bytes, while four integration cases hit a fixture PurePosixPath.exists error and one failure case passed. Corrected that fixture; two cache-only invocations remain.

- id: lead-sol:5
  kind: state-change
  refs: [lead-sol:4]
  based_on: fd8de3b
  state: BLOCKED
  note: Invocation 3/4 returned to cache-only and stopped in rules_python analysis before tests on the same PyPI simple metadata list. Scope ended inactive/dead in 8 seconds; owned scratch 393 MiB. One invocation remains, but further acquisition is required to test the corrected fixture. Root received this blocker on pane %91 with a submitted receipt. No production fix or field attribution.

- id: lead-sol:6
  kind: state-change
  refs: [lead-sol:5]
  based_on: 484c923
  state: STARTED
  note: Root accepted the checkpoint and replaced the cache-only stop for this package. Four focused invocations remain (lifetime ceiling seven), each may resolve unchanged locked dependencies with Nix offline; each scope <=10 minutes and combined remaining wall time <=20 minutes. Existing single-flight, CPU/memory/swap/scratch/free-space bounds persist. Proceeding to corrected original-source behavioral integration RED; no production fix yet.

- id: lead-sol:7
  kind: state-change
  refs: [lead-sol:6]
  based_on: 484c923
  state: VERIFIED
  note: Invocation 4/7 on unchanged production rsync source proved real local-rsync quick-check skip and integrated false-CLEAN with stale member bytes through the real no-baseline conflict guard and callback. Legacy skip reverify, changed-size upload, and failure controls passed. Bazel exit 3; wrapper exit 90 came from a post-completion scope sampling race, with inactive/dead scope receipt. Corrected monitor before invocation 5.

- id: lead-sol:8
  kind: huddle-convened
  refs: [lead-sol:7]
  based_on: 484c923
  issue_key: rsync-upload-selection-args
  note: Invocation 5/7 passed default upload and all five integration cases after upload-only --ignore-times, but real caller --size-only still skipped equal-size bytes despite the later flag. Asked root on pane %91 (submitted receipt) to decide visible rejection versus override of the published arbitrary rsync_args surface. No contract change or full GREEN claimed.

- id: lead-sol:9
  kind: interpretation-filed
  refs: [lead-sol:8]
  based_on: 484c923
  issue_key: rsync-upload-selection-args
  note: Existing pass-through test includes --exclude=*.tmp; filters and size limits can also suppress a selected upload. Sent root an addendum on pane %91 (submitted receipt) and filed oq-rsync-upload-selection-args for the product decision. No policy change made.

- id: lead-sol:10
  kind: state-change
  refs: [lead-sol:9]
  based_on: 484c923
  state: STARTED
  note: Root reviewed the focused results and retained this exact lane for Ted's policy ruling and fix-delta review. Two remaining test invocations are held unused. Root identified the two decision choices: reviewed upload-safe allowlist with visible rejection, or arbitrary arguments with actual remote-content verification before CLEAN. Existing open question oq-rsync-upload-selection-args was updated rather than duplicated. No policy-dependent edit or runtime followed.

- id: lead-sol:11
  kind: state-change
  refs: [lead-sol:10]
  based_on: f294de9
  state: DELIVERED
  note: Prepared the coherent draft checkpoint: production-change commit f294de9 contains only upload-only --ignore-times; the report and status separate original-source integrated RED, focused default-path GREEN, caller --size-only RED, and the held product question. No full module or exact pushed-head GREEN is claimed. Remaining two scoped invocations are unused and all prior owned scopes are inactive/dead.

- id: lead-sol:12
  kind: state-change
  refs: [lead-sol:11]
  based_on: 34931d6b29c092d99e54589fd9e609673d1f6b0c
  state: STARTED
  note: Ted ruled behavioral startup validation of configured rsync options; root assigned this same PR179 lane the startup hook/config scope and six new focused invocations, replacing the prior two-run hold. Wrote assumptions, source boundary, argv confinement and failure behavior before probe implementation. Selected existing rsync_backend and allow_other test modules; no runtime or live member access yet.

- id: lead-sol:13
  kind: state-change
  refs: [lead-sol:12]
  based_on: 87cc102
  state: STARTED
  note: Root's design review found that _run_rsync can consult/reset ControlSockets even for local operands. Revised the plan before probe implementation: pure shared transfer-argument builder, separate local subprocess path with no SSH state access, immutable snapshot of caller rsync_args, and strict argv spelling grammar with negative spies. No new product boundary or runtime grant.

- id: lead-sol:14
  kind: state-change
  refs: [lead-sol:13]
  based_on: 87cc102
  state: VERIFIED
  note: Scoped startup invocation 1 failed behaviorally on unchanged mount hook. Draft validator uses actual local rsync with frozen caller options and never enters SSH/control-socket/member paths; invocation 2 passed new cases but exposed an older direct --size-only test expectation. After changing that expectation to startup rejection, invocation 3 passed all three affected modules. A controlled deletion of the byte comparison in invocation 4 produced seven test failures, and the check was restored. All four scopes inactive/dead; network bytes UNKNOWN. This is local synthetic verification, not pushed-head or release acceptance.

- id: lead-sol:15
  kind: state-change
  refs: [lead-sol:14]
  based_on: 5869948
  state: VERIFIED
  note: Pushed implementation and record. On pushed 5869948, invocation 5's two focused Bazel targets passed, but the wrapper returned 90 when its du scratch read raced Bazel deleting a sandbox and stopped the scope during cleanup. Scope is inactive/dead. Changed the external monitor to retry the transient read at most three times, retaining fail-closed stop on repeated failure. Final full affected-module run remains as invocation 6/6; no clean wrapper receipt is claimed for invocation 5.

- id: lead-sol:16
  kind: state-change
  refs: [lead-sol:15]
  based_on: 27c3ca96c66573be12c4c6eba5692a3c24231c7c
  state: STARTED
  note: Invocation 6/6 on exact pushed head passed three affected nonmount modules; wrapper exit 0 after 14 seconds, scope inactive/dead and empty control group. Root reviewed full diff and run receipts, with an independent native Sol source/test reviewer CLEAN, then retained this lane for formal council and release preparation. New bounded authority is four build/test invocations, 30 minutes combined, client 2 CPU/8 GiB/2 GiB swap/256 tasks, new owned scratch <=10 GiB, and bounded synthetic Docker fixtures. No council, full release gate, CLI build, merge, or deployment evidence yet.
