# Foreground lead events

## foreground-lead:1

- kind: state-change
- state: STARTED
- based_on: `66c640a`
- refs: none
- evidence: Clean assigned worktree and branch confirmed; ROLES, DOCTRINE, repository instructions, DESIGN and committed integration roster read before source change.
- intent: Foreground reads and browsing progress under bounded demand-scaled work; #167's sync-only scope does not fulfill Ted's ruling.
- authority: Implement and push reviewable work here; root retains acceptance and merge authority.
- next: trace live code boundaries with scratch-only synthetic evidence, then cut packages after a necessity critique.

## foreground-lead:2

- kind: state-change
- state: TRACER_VERIFIED
- based_on: `48b5ea0`
- refs: foreground-lead:1
- evidence: `nix develop --command bazel test //tests:storage_test_fs_threading --test_arg=-k --test_arg=cold_full_copy --test_output=errors` failed on the pre-fix tree: copy interactive=true, listing interactive=false. The same command passed after the fix. Full-file copy now has a nested bulk scope; readdir eagerly resolves under the FUSE interactive context.
- limit: This tracer proves classification and bytes on a local synthetic member. It does not prove channel fairness or native FUSE thread bounds.
- next: Two sequential packages cut at `org/work-packages/`; fresh necessity critique pending.

## foreground-lead:3

- kind: state-change
- state: READY for FR-foreground and FR-sync, sequentially
- based_on: `48b5ea0`
- refs: foreground-lead:2
- necessity: Fresh OpenAI Luna/Utility critique returned SIMPLIFY. Retain the two tracer fixes; require a red admission saturation test before its machinery. Clarified cap-3 reserve, progress conditional on active completion, queued cancellation versus active member I/O, and the one-channel SSH limit in spec/package. No additional scheduler or load policy justified.
- next: implement FR-foreground first; review its diff and publish the config interface before FR-sync.

## foreground-lead:4

- kind: state-change
- state: FR-foreground implemented, focused verification; not accepted
- based_on: `edc9736` plus uncommitted foreground diff
- refs: foreground-lead:3
- evidence: Admission synthetic test was red before implementation and green afterward. Four focused Bazel targets passed. Fake SSH pool held three foreground-initiated bulk transfers with `bulk_leased=3` and `sftp_open=3` while metadata and demand interactive acquisitions completed; its targeted Bazel test passed. No live member or mount used.
- limitation: Active Python callback cap only. Libfuse 2 native threads can still grow with waiting callbacks. This is documented in CLI, README and spec. Full gates and review pending.
- control: tmux message attempt to root `%87` returned `mixed-input-after-paste`, no new receipt; its pending payload is not replayed. Root delivery remains unknown and will be retried only from a verified empty composer.

## foreground-lead:5

- kind: state-change
- state: FR-foreground pushed, review pending
- based_on: `2a5490c`
- refs: foreground-lead:4
- evidence: Exact branch push succeeded; `git diff --check` clean. Focused Bazel commands cited in FR-foreground package ran on the pre-commit tree, not the final head; exact-head gates remain pending. Self-review found no changed byte/write path: admission wraps callbacks and the cold-copy scope only changes SSH lease classification.
- next: FR-sync reconciliation, then final gate and independent review.

## foreground-lead:6

- kind: state-change
- state: FR-sync reconciled, focused verification; not accepted
- based_on: `effadd7` plus uncommitted compatibility fix
- refs: foreground-lead:5
- evidence: Four #167 commits cherry-picked in order into this branch without touching its sibling. Conflicts in `fs.py`, `cli_mount.py`, and `mount_profiles.py` retained both cap interfaces and current-master display names. `UpdateHostKeys=no` remains in rsync and watcher. Five focused targets passed; full SSH module initially exposed the no-init probe subclass and passed after the optional gate lookup fix. No live member used.
- limitation: Original #167 all-gates/council evidence does not transfer to this union tree. Exact pushed-head gates and fresh review required.
- control: BUILD one-line source addition delivered to integration lead `%93` with submitted receipt. Root `%87` message still lacks a receipt after mixed input; do not count it delivered.

## foreground-lead:7

- kind: state-change
- state: PR #171 draft, council round 1 findings; no gate acceptance
- based_on: `1f96002449914088621964445880dcf16c889376`
- refs: foreground-lead:6
- evidence: Combined compatibility fix committed and pushed. Five focused Bazel targets and `//:storage_build_srcs_check` passed; exact-head `nix develop --command bazel build //:storage_cli` passed. Formal council opened on exact pushed head, with OpenAI one Important, Anthropic three Important plus nits, and Google empty due to usage limit. Outcome and findings posted to PR #171. No all-gates receipt.
- repairs: pre-lock sync validation; metadata browse admission for `opendir`/`releasedir`; warm `open` progress under cold copies; shutdown cancellation contract. Nits to resolve or file follow-up per council rules.
- authority: Root retains acceptance and merge. Fleet has not granted a runtime slot. Root `%87` received a later fresh submitted receipt for status and prep; integration `%93` remains consolidated-roster owner.

## foreground-lead:8

- kind: state-change
- state: fixture source prep committed, runtime denied pending Fleet grant
- based_on: `cf82398`
- refs: foreground-lead:7
- evidence: User correction withdrew private wrapper/client-3CPU proposal and authorized source-only fixture edit. Each of three `docker create` calls now carries literal 0.5 CPU, 512 MiB memory/no swap, 128 PID caps; flushed run, network and returned container IDs precede next operation. Existing exact-ID container teardown and exact-name network teardown retained. `git diff --check` passed. No Docker probe, image acquisition, container, FUSE or gate operation ran.
- next: push coherent record, inform root and Fleet of new head, compact, then repair council findings. Later gates require a named grant, fresh admission and cached image/tooling preflight; image absence stops.

## foreground-lead:9

- kind: state-change
- state: council repairs coded, source review pending
- based_on: `05401f8`
- refs: foreground-lead:8
- evidence: Direct sync limits validate before mount lock; directory handles classified metadata; cached opens classified demand, cold opens ordinary. Added synthetic saturation and direct-lock tests. Corrected shutdown contract to state libfuse joins/drains callbacks before `destroy` closes the admission gate; explicit gate close still cancels queued callbacks. Python AST parsing and `git diff --check` passed. No runtime tests, Docker probe or gates under absent Fleet grant.
- next: push new head, formal council convergence on the pushed delta, then exact-head CLI/all-gates under named Fleet grant and fresh admission. Root receives independent review package after council.

## foreground-lead:10

- kind: state-change
- state: formal convergence findings and second repair pushed; no gate acceptance
- based_on: `22379e2`
- refs: foreground-lead:9
- evidence: Formal round at `96f5c3d` had OpenAI one new Important, Anthropic abandoned because head moved, Google empty. Formal round at `e53cb50` had OpenAI CLEAN and Anthropic one new Important: admission transfer waited while holding path/namespace locks, allowing a lock-slot cycle; Google empty. Round records and findings posted to PR #171. Latest repair drops outer locks before transfer, retries open and carries revalidation pin outside shared state during the wait; added lock-release and pin test. Python AST and diff checks passed; runtime tests/gates remain unrun under absent Fleet grant.
- limit: at cap 3 a slow cached-open freshness stat may occupy the only protected demand slot until its member timeout; documented as a product tradeoff. Native libfuse thread count remains unbounded.
- next: new formal council convergence and exact-head tests when Fleet grants a local cached-image slot; changed head invalidates earlier CLI build for acceptance.

## foreground-lead:11

- kind: stop-and-adjudication
- state: DESIGN_SOURCE_ONLY; application repair and runtime stopped
- based_on: `0631a73bff98b7649b9d65c181b5fe04dcf72ff9`
- applied_at: this unique ledger checkpoint after root ruling `foreground-open-pin-ownership`
- refs: foreground-lead:10, PR #171 formal round at `0631a73`
- evidence: OpenAI and Anthropic formal seats each found Important freshness race(s): private changed-member pin during ordinary-admission wait permits another opener to fetch stale holder bytes without recheck; outer cleanup after path-lock release can erase a newer opener's pin. Third unresolved repair failure triggered stop. Round outcome and separate findings comment posted; Google EMPTY usage limit. Anthropic's possible non-open sentinel escape was checked against source: `_ensure_file_in_cache` has only `_open` as caller at this head.
- decision: Root retains warm-open demand guarantee, rejects all-open-ordinary downgrade, and authorizes only contract/huddle design plus deterministic two-member regression source. Durable path-scoped freshness obligation/generation must remain visible and owed until checked member bytes commit or explicit path invalidation; cancellation, failed fetch, unavailable member, rename and late stream must not erase newer debt. Root reviews boundary before application-code repair resumes.
- prerequisite: Sole-owner read began after explicit 16:27:02 UTC scope (exact second not logged) and ended before 16:30:04 UTC. Local Docker Unix socket verified; cached named image present, 95,680,856 bytes, ID `sha256:0f7e761730d313b2cdcd74d36c53b6bbb9f1dc0093890e26d8ec5c20cd0ed07b`; `/dev/fuse` and setuid helper available; host libfuse2 absent, flake/lock unchanged since earlier CLI pass. No retained process/fixture. Fleet %68 received cancellation with submitted receipt; quiet/release confirmation pending.
- next: checkpoint/push, then stop/compact for token tripwire; source-only design and tests await bounded resume. No runtime tests or gate scheduling.

## foreground-lead:12

- kind: state-change
- state: IDLE_AT_CHECKPOINT pending bounded resume
- based_on: `4887e70`
- refs: foreground-lead:11
- evidence: Root acknowledged prerequisite execution/quiet and Fleet cancellation; token tripwire 316>300 calls for checkpoint and compact/stop now. Formal third-round outcome includes finished Anthropic and OpenAI findings; separate PR comment records both races and triages Anthropic's non-open sentinel concern against the sole `_open` caller. No application repair, runtime test or gate followed root stop.
- open_question: cap 3 gives one demand slot; a slow cached-open freshness stat may hold it until member timeout. Root retains warm-open demand guarantee and requires this limitation to remain explicit; file an open-questions tracker issue on bounded resume if a further product choice is needed.
- next: await bounded resume for design/contract and deterministic two-member regression source only; root must lift application-code and runtime holds separately.

## foreground-lead:13

- kind: state-change
- state: DESIGN_REGRESSION_SOURCE_DELIVERED; root boundary review pending
- based_on: root adjudication `foreground-open-pin-ownership` at `0631a73bff98b7649b9d65c181b5fe04dcf72ff9` and clean resume checkpoint `a95f6d6c34413f042aa4cd02febc524132ba9b12`
- applied_at: source contract/regressions `47937490c085132f6fb51a56614329dd0745f452`
- refs: foreground-lead:11, foreground-lead:12, `contracts/path-freshness-obligation.md`, `org/huddles/foreground-open-pin-ownership.md`, PR #171
- evidence: Proposed shared path identity/generation, checked-member obligation, private staging before token-checked byte and baseline publication, fallback recheck debt, rename success/failure identity, late stream exclusion, and lock order. Added ten two-member Event-gated scratch regression sources in the existing threading test module, forcing ordinary selection toward stale member B. Python AST parse and `git diff --check` passed before commit; tests were UNRUN under the runtime stop. No `storage/` application code changed.
- open_question: filed and synced ditz `oq-foreground-freshness-stat-slot` for the cap-3 slow-stat demand-slot product choice. Warm-open demand admission stays as ruled.
- authority: Ted's bounded resume allows design/contracts/tests/status only. Root must review boundary and explicitly lift application-code repair separately. Runtime gates/probes and tedks/Fleet liaison remain stopped; no capacity request sent.
- next: push reviewable checkpoint, update draft PR evidence and deliver one exact-head boundary summary to Storage root `%91`; then remain idle pending root review.

## foreground-lead:14

- kind: review-feedback-disposition
- state: BOUNDARY_FEEDBACK_DELTA_DELIVERED; root re-review pending
- based_on: frozen `cd116cc7b590c142a172b450940b6c38b4581d2c` boundary review at `/home/tedks/.cache/storage-orgs-20261110/boundary-review-cd116cc.md`; verdict FINDINGS, no application or runtime clearance
- applied_at: source feedback commit `e91ccabe74118236c2f9d877cffd25e5c5360d9b`
- refs: foreground-lead:13, `contracts/path-freshness-obligation.md` v2, `docs/specs/foreground-responsiveness-20261110.md`, `tests/test_fs_threading.py`, PR #171
- F1 disposition: specify a short content-epoch/active-mutation leaf guard announced before `pwrite`, truncate or O_TRUNC, DIRTY publication before retirement, exact staged-inode ownership and no-replace install. Three Event-gated tests hold the real byte-to-DIRTY boundary for write/truncate/create and assert local bytes and DIRTY state. Path lock alone is explicitly insufficient; no writer wait while holding namespace/path locks.
- F2 disposition: choose versioned cache-file xattr as durable debt authority, sync it before invalidation or B fallback visibility, and force first-open all-holder reprobe when marker/cache is missing or invalid. Two restart sequences assert B cache retains A debt and missing cache refetches changed A despite stale-B ordinary selection. Ambiguous holder copies require content comparison or conservative failure, never silent B certification.
- F3 disposition: independent member-ID debt map and per-member generations; C completion clears only C while A remains owed. Added A-unavailable/B-fallback, changed-C completion, then A-return sequence asserting C bytes/baseline followed by A bytes/baseline and debt cleanup.
- evidence: Python AST parsing and `git diff --check` passed on the source delta. All tests/gates UNRUN as instructed. No `storage/` edit, daemon probe, live data or paused Fleet request. Feedback is contract/test source, not a runtime or implementation outcome.
- next: push frozen head, annotate draft PR and deliver one checkpoint to Storage root `%91`; await boundary re-review before any application repair.

## foreground-lead:15

- kind: review-feedback-disposition
- state: BOUNDARY_CONVERGENCE_DELTA_DELIVERED; root re-review pending
- based_on: frozen `baa66ed105212dd647eb46a13e91cce444407241` convergence review at `/home/tedks/.cache/storage-orgs-20261110/boundary-review-baa66ed.md`; verdict FINDINGS, no application or runtime clearance
- applied_at: source correction `794d0536df1555d5505a579289728eea0022163a`
- refs: foreground-lead:14, `contracts/path-freshness-obligation.md` v3, `docs/specs/foreground-responsiveness-20261110.md`, `tests/test_fs_threading.py`, PR #171
- C1 disposition: path-wide publication revision advances on any changed-member observation, byte/baseline materialization, local mutation start or namespace identity change. Stream/fetch tokens capture it before reads and validate at locked install and baseline publication. An A observation invalidates old B publication attempts while per-member A/C debts remain independent. Absent-final-path late-B test now asserts B generation unchanged, revision changed, and A debt retained after rejection.
- C2 disposition: every CLEAN cached inode carries a versioned local marker; locally verifiable restart opens stay demand. Only uncertain member recovery releases namespace/path locks and transfers to ordinary. New cap-3 Event-gated cold-copy test saturates ordinary while first restarted warm read completes without member stat. Missing-cache A/B divergence without source proof now fails EIO in the proposed contract/test, instead of picking A by ranking.
- C3 disposition: write test holds after `pwrite` and before DIRTY, observes late callback at `_record_fetched` publication entry, finishes its validation before releasing the writer, and asserts A debt and absence of B baseline along with local bytes/DIRTY. Truncate/create retain path-locked interleavings and assert post-callback baseline/debt/bytes. The absent-final-path late-partial test independently exercises token guard without no-replace shortcut.
- evidence: Python AST parse and `git diff --check` passed; 17 regression sources and all gates UNRUN under source-only authority. No `storage/` application edit, daemon probe, live-data work, deployment or paused Fleet request.
- next: push frozen head, annotate draft PR and deliver one exact-head checkpoint to Storage root `%91`; await its boundary re-review before any application or runtime phase.

## foreground-lead:16

- kind: targeted-review-disposition
- state: TARGETED_BOUNDARY_TEST_DELTA_DELIVERED; root re-review pending
- based_on: root source review of frozen `7b1f5e6ee4f64605959c8d8aceef79ec1f95a2b5`; C1 path-wide revision and C2 local demand recovery architecture clean, C3 test integration and remote freshness wording needed correction
- applied_at: source correction `09242aee96e256c668f9b4c43c3447d3448b2ea0`
- refs: foreground-lead:15, `contracts/path-freshness-obligation.md`, `docs/specs/foreground-responsiveness-20261110.md`, `tests/test_fs_threading.py`, PR #171
- C3 disposition: B token is captured after A debt and before local mutation, so the write itself invalidates it. Callback entry is observed while writer pauses after `pwrite` inside metadata-locked DIRTY marking; the test permits early rejection or metadata-lock blocking, releases writer before joining callback, then asserts local bytes, DIRTY, A debt and no B baseline. Removed the forced `_record_fetched` entry helper that deadlocked a correct callback.
- C2 disposition: valid local marker restores identity/debt only; it does not skip normal first-open freshness stat or forge `verified_at`. The ordinary-saturated restarted warm test now permits and observes A's unchanged stat while demand progresses. Added a changed-A-while-stopped case: demand observes changed A, releases path lock, waits for ordinary behind a cold copy, then returns A bytes/baseline. Missing-cache ambiguity remains conservative.
- evidence: Python AST parse and `git diff --check` passed. Eighteen scratch regression sources and all gates UNRUN under source-only authority. No application edit, runtime probe, live-data work, paused Fleet request or #171 deployment.
- boundary: Ted's splinter1 authorization is being exercised by Storage root only for accepted `81793ac` (#165/#166); separate restart-design Sol `%99` studies mount-preserving restart. Neither changes this branch's hold.
- next: push clean frozen head, annotate draft PR and deliver exact-head checkpoint to root `%91`; await re-review before application/runtime work.

## foreground-lead:17

- kind: tracer-prerequisite-blocker
- state: TRACER_RUNTIME_BLOCKED_CACHED_INPUTS; no application repair started
- based_on: root and warm Sol CLEAN boundary decision at `07134bc9029a0e135f0c7dbb792765c1e8f03543`, `/home/tedks/.cache/storage-orgs-20261110/boundary-review-07134bc.md`; authority is one smallest coherent tracer with meaningful RED/GREEN and cached-only local nonmount run
- applied_at: test/contract refinement `33dac384a62d6f9ca755786b48356f42a52733fa`
- C3 refinement: B is now eligible through A-unavailable fallback after A debt exists and before mutation; the callback enters while `pwrite` is past bytes but before DIRTY, and debt/baseline are inspected before writer release without waiting for callback completion. This removes source-preference and old-token masking of the mutation guard.
- capacity: framework0 recheck showed 106 GiB MemAvailable, full memory PSI avg10=0.00, vmstat zero swap in/out. Owned `storage171-red-20261111.scope` reported cgroup cpu.max `200000 100000`, memory.max `4294967296`, swap.max `0`; it ended inactive/dead. No peer holder was touched.
- attempted RED: `timeout 600s nix develop --offline --command bazel --output_base=/tmp/storage171-tracer-ybuymvxe/bazel --batch test //tests:storage_test_fs_threading --repository_disable_download --jobs=2 --local_cpu_resources=2 --local_ram_resources=3500 --test_arg=-k --test_arg=racing_opener_cannot_select_stale_b_while_a_is_owed --test_output=errors` in that scope. Exit 1 after 6.8s, before any test: rules_python pip extension requested uncached PyPI metadata for nh3, idna, rich and others; no acquisition occurred. Exact log `/tmp/storage171-tracer-ybuymvxe/red.log` (5609 bytes).
- evidence limit: no behavioral RED race, no GREEN, no application edit, no all-gates/council acceptance. Python AST and `git diff --check` passed for source refinement. This is a cached-input gate blocker, not a test result. Paused Fleet and live storage untouched.
- next: push clean checkpoint and report blocker to root `%91`; stop runtime until a separately authorized cached-input solution fits the named envelope, then rerun meaningful RED before repair.

## foreground-lead:18

- kind: diagnosis-checkpoint-and-context-stop
- state: DIAGNOSIS_CHECKPOINT_CONTEXT_STOP; native compaction needed before further diagnosis
- based_on: Storage root's bounded cached-input diagnosis assignment at pushed `beffec75735e77bc9b10adafc137086403f32fcf`, then `STORAGE171-CONTEXT-TRIPWIRE-20261111` requiring immediate checkpoint/push/stop
- evidence: Read-only inspection found project `MODULE.bazel` depends on rules_python 1.6.3 and parses project `requirements.lock`, which has no twine. The owned failed output base's `external/rules_python~/MODULE.bazel` defines a non-dev `rules_python_publish_deps` `pip.parse` using the PyPI simple index and platform publish requirements; the extension processes module parse calls during analysis. That explains twine and its metadata dependencies appearing for the focused test. Root `MODULE.bazel.lock` contains older rules_python 0.28.0 references. Computed default output base for this owned worktree is `/home/tedks/.cache/bazel/_bazel_tedks/02701c9aebad39d47c941364695f8529`, which exists, but its extension cache compatibility is unverified. No reproducible cached route or smallest acquisition is yet established.
- deviation: a broad read-only `ls` of `/home/tedks/.cache/bazel/_bazel_tedks/*` printed 1387 directory entries despite the no-broad-traversal limit. It did not mutate or use any peer base; the result is not treated as cache provenance. Subsequent read identified only the exact hash-derived worktree base. No repeat broad traversal.
- resources: owned RED systemd scope previously ended inactive/dead; no new runtime, Nix evaluation, test/build, download, acquisition, application edit, Fleet contact or peer-base mutation in diagnosis. Owned failed output base/log remain for evidence.
- next: push this unique record and stop for native `/compact`; then resume the same read-only cached-input diagnosis under its original 15-minute limit and constraints. Runtime retry remains prohibited.

## foreground-lead:19

- kind: bounded-prerequisite-diagnosis
- state: CACHED_INPUT_DIAGNOSIS_DELIVERED; application tracer and runtime remain blocked pending root review of prerequisite route
- based_on: root `STORAGE171-DIAG-RESUME-20261111` after native compaction; source head `cca4e5f97d5d373639f0cdda7b04789be78839e0`
- scope: read-only, exact owned failed base `/tmp/storage171-tracer-ybuymvxe/bazel`, exact hash-derived worktree default base `/home/tedks/.cache/bazel/_bazel_tedks/02701c9aebad39d47c941364695f8529`, and project module/lock/config files; no shared-root traversal or peer base inspection in this resumed phase
- provenance: MD5 of the canonical owned worktree path is `02701c9aebad39d47c941364695f8529`, and that default base's README names the same workspace. Its November 10 `command.log` records a successful `//:storage_cli` build. Both bases' rules_python `MODULE.bazel` bytes have SHA-256 `a7b80c42cb3de5ee2a5fa1abc119684593704fcd2fec83165ebe615dec76574f`, consistent with the 1.6.3 registry hash in `MODULE.bazel.lock`. The default base's server metadata exists, so exclusive ownership for a new batch test is not established by the path alone.
- cause: project `MODULE.bazel` has one `pip.parse` using `requirements.lock` (no twine), while rules_python's non-dev `rules_python_publish_deps` parse requests the PyPI Simple API for its own publishing requirements. Its extension iterates these parses in one evaluation. The failed analysis enumerates 30 missing Simple API pages. rules_python `simpleapi_download.bzl` says it intentionally avoids Bazel's persistent download cache for index pages and stores responses only within the extension evaluation; the current project `MODULE.bazel.lock` has no pip extension result. The default base has project `rules_python~~pip~pypi*` repos but no `rules_python_publish_deps` repo. The fresh scratch base has no publish hub either. The November 10 CLI result does not prove a new isolated batch test can evaluate offline.
- acquisition boundary: no reproducible cached-input command is supported by these exact artifacts. The first observed lower bound for an unmodified fresh evaluation is 30 PyPI Simple API page requests at `https://pypi.org/simple/<package>/`; package names are in `/tmp/storage171-tracer-ybuymvxe/red.log`. Potential subsequent wheels/repositories and aggregate bytes are unknown. An online retry would need to omit `--repository_disable_download`, but that alone is not a bounded acquisition plan. Root must review a network/resource envelope or an explicit source/dependency repair before any retry; do not silently reuse the default base, bypass `MODULE.bazel`, or edit external cache content.
- resources and verification: only read-only `stat`, exact-base `ls`, `sha256sum`, `rg`, `sed`, and Python parsing of existing files were used. No Bazel/Nix invocation, download, build/test, application edit, Fleet contact, or live storage access. Prior RED scope is inactive/dead; evidence scratch base/log retained without a running owned process. Tests remain UNRUN and no behavioral RED exists. Prior broad-root `ls` deviation in event 18 was not repeated.
- next: push this status/ledger checkpoint and report bounded blocker to root; stop diagnosis until a new scoped route is authorized.
