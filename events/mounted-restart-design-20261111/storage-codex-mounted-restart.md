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

## storage-codex-mounted-restart:5 · 2026-11-18T04:42:36Z · state-change
- actor: lead (Anthropic claude-opus-5-5), PR 172 owner under STORAGE-RESUME-172
- based_on: 0953369de47a7e9b46a0edca8facfee99d6bb490
- refs: storage-codex-mounted-restart:4; oq-mounted-sftp-maintenance-semantics; oq-mounted-daemon-handoff-scope; PR 172
Rebased on master `0953369`. Folded in Ted's 2026-11-17 product rulings, accepted by the CTO: same daemon, recycle one SSH member connection, bounded wait for new work, and on drain timeout keep the active transfer running and report maintenance incomplete. Unresolved wording is superseded; the original P0 question is kept for the record. Line-anchored source links are pinned to snapshot `66c640a` so the cited evidence survives master drift; one post-snapshot note cites master's bulk capacity deadline. Docs only: no runtime feature, production access, rollout or mount change.

## storage-codex-mounted-restart:6 · 2026-11-18T04:49:12Z · review-finding
- actor: lead (Anthropic claude-opus-5-5), PR 172 owner
- based_on: 0953369de47a7e9b46a0edca8facfee99d6bb490
- refs: storage-codex-mounted-restart:5; PR 172 council rounds 53f2d4152f72-20261118T044249Z-ef2e72 and bac65c285a16-20261118T044639Z-47467b
Council findings changed the outcome semantics; no ruling changed. Completed now requires the new transport seen connected (the lazy-reconnect path is dropped); failed means retired and the new connect failed or missed a P1-enforced reconnect allowance, because the member's connect timeout is per phase and does not bound name lookup; incomplete covers a missed drain deadline or maintenance cancellation; maintenance is refused without a connected transport or while a connection attempt is installing one. Dispositions are on the PR. Google seat EMPTY both rounds (usage limit).

## storage-codex-mounted-restart:7 · 2026-11-18T04:58:24Z · review-finding
- actor: lead (Anthropic claude-opus-5-5), PR 172 owner
- based_on: 0953369de47a7e9b46a0edca8facfee99d6bb490
- refs: storage-codex-mounted-restart:6; PR 172 council rounds 085841b3cfdc-20261118T044927Z-2c849b, 9919998e8691-20261118T045122Z-591a95, 075321928dea-20261118T045334Z-46efb2, e1d25dcb1ffe-20261118T045447Z-3d8dcc
Council rounds 3 to 6 refined the outcome model without changing a ruling: a late connect is serialized by the pool's single-flight CONNECTING state and epoch fence; a mid-drain transport loss or mark-down reports failed; whichever of deadline/cancellation or transport loss comes first decides; design decisions are labelled apart from ruling text. Round 6 at `e1d25dc` was CLEAN from the Anthropic and OpenAI seats; Google EMPTY (usage limit) in every round. Dispositions are on the PR.

## storage-codex-mounted-restart:8 · 2026-11-18T04:58:24Z · lead-review
- actor: lead (Anthropic claude-opus-5-5), PR 172 owner, recording a fresh-context lead review
- based_on: e1d25dcb1ffe81ef4da95f554269ed3f6a693234
- refs: storage-codex-mounted-restart:7; PR 172
Fresh-context lead review at `e1d25dc`: FINDINGS 0/1/5. Important: the PR body said "drain ... connection attempts" where the doc refuses. Nits: cancellation after retirement unspecified; contract 5's refused list omitted an in-flight connect; item 2 lacked a ruling/reading marker and carried P1 mechanism (moved to a P1 subsection); ledger lagged; one fs.py range unlinked. All fixed in the next commit; a council delta and a lead re-review of that delta follow.
