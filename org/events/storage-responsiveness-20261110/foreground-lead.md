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
