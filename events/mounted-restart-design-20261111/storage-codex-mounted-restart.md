# Mounted restart design lane events

## storage-codex-mounted-restart:1 · 2026-11-11T21:26:16Z · state-change
- actor: lead (OpenAI gpt-6-sol)
- based_on: 66c640aec005feccd0bd585f3355295c3bb772ba
- refs: mounted-restart-design-20261111
Work package `mounted-restart-design-20261111` —→CLAIMED. Scope: source and primary-document research, a docs-only design draft PR, and tactical follow-up issues. No application code, mounts, host probes, or rollout. Acceptance: concrete invariants, option comparison, smallest first phase, source citations, synthetic tests, and rollback/stop conditions.

## storage-codex-mounted-restart:2 · 2026-11-11T21:30:55Z · state-change
- actor: lead (OpenAI gpt-6-sol)
- based_on: 66c640aec005feccd0bd585f3355295c3bb772ba
- refs: storage-codex-mounted-restart:1; mounted-restart-design-20261111
Work package `mounted-restart-design-20261111` CLAIMED→IN_PROGRESS. Context manifest written at `status/mounted-restart-design-20261111-context.md`; assumptions and source snapshot are stated in the report. Product questions filed as `oq-mounted-sftp-maintenance-semantics` and `oq-mounted-daemon-handoff-scope`; tactical follow-ups are `mounted-sftp-pool-recycle` and `mounted-worker-reconfig-contract`.

## storage-codex-mounted-restart:3 · 2026-11-11T21:34:03Z · state-change
- actor: lead (OpenAI gpt-6-sol)
- based_on: 7f17f10472a1d1ed4a4b4f2d4d170da14695a813
- refs: storage-codex-mounted-restart:2; mounted-restart-design-20261111; PR 172
Work package `mounted-restart-design-20261111` IN_PROGRESS→REVIEW. The report and own records are pushed at `7f17f10472a1d1ed4a4b4f2d4d170da14695a813`; draft PR 172 targets master. Self-review checked the full docs diff and source anchors, corrected the SSH pool scope to include exec channels and nested leases, and distinguished Bazel's fusepy pin from the Nix-packaged CLI. `git diff HEAD^ HEAD --check` passed. Tests and live operations were excluded by mission authority; no council or root acceptance is claimed. Four follow-up/product-ruling issues were synchronized with `ditz sync`.

## storage-codex-mounted-restart:4 · 2026-11-11T21:42:40Z · lead-review
- actor: lead (OpenAI gpt-6-sol), recording the root review report
- based_on: d4a372cc290901f03d6da557761d6caabc41a419
- refs: storage-codex-mounted-restart:3; mounted-restart-design-20261111; PR 172
Root source review and a fresh native Sol review returned CLEAN for the bounded docs investigation at exact `d4a372cc290901f03d6da557761d6caabc41a419`, base `66c640a`. This records the review report, not a formal council round, implementation acceptance, product ruling, or merge clearance. The source-link fragment in the Debian fuse.py reference used web-extraction numbering and is removed in this record update; the design conclusion is unchanged. The PR stays draft and the lane is idle with no implementation or runtime mission. Ted retains mount launch.
