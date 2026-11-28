# Require sustained reads before retaining a download

Maintain this ExecPlan according to `.planning/PLANS.md`.

## Purpose / Big Picture

Opening a large file to inspect its first few megabytes can make Storage download the rest of it. The earlier default qualified a 2 GiB download after a contiguous 4 MiB read. With the new default, a 1 MiB read of the same file can trigger up to 4 MiB of speculative fetching beyond the read span, and a span of 644,245,095 bytes qualifies the file to finish downloading after close. Operators can tune both settings in a mount profile, on the mount command line, or on their running mount.

## Progress

- [x] (2026-11-19 00:20Z) Started in a dedicated worktree from the mount control branch and confirmed the existing qualification also bounded speculation.
- [x] (2026-11-19 00:32Z) Implemented independent settings, documented their usage, and added synthetic and mounted cases. Rebased on the cache eviction branch, which includes mount control.
- [x] (2026-11-19 00:34Z) Passed five focused Bazel suites, including 72 streaming unit tests.
- [x] Verified all four Bazel gates in the Nix development shell at 0f02210 (49 of 49 targets passed, as reported in the PR #188 body).
- [x] Published the branch as draft pull request #188 against the cache eviction branch.
- [x] (2026-11-28) Addressed council round 1 nits: an integer read-ahead window from `streaming.set` and from profiles, a case-insensitive `all:` prefix with an error that names the expected form, the sub-chunk read-ahead note in the README, and this record. Focused suites for the changed modules pass.
- [ ] Re-run all four Bazel gates on the commit that carries the round 1 fixes.

## Surprises & Discoveries

The previous worker rounded the speculative boundary up to the end of a chunk and could fetch behind the current read. Constraining the actual range passed to the backend gives a byte-accurate budget even after an unaligned seek or with a window smaller than the chunk size.

## Decision Log

On 2026-11-19 the implementation used `all:` to opt in to requiring every term, with percentages rounded up and the result capped at the file size. Explicit comma lists retain their original first-term-reached behavior, including rounding percentages down. The independent 4 MiB byte window preserves the old default distance on large files at the default chunk size. A live change reuses the observed spans of open unqualified sessions. Qualification stays in effect once earned, so increasing the threshold cannot interrupt downloads already authorized to finish.

On 2026-11-28, after council round 1, the read-ahead window accepts a non-negative integer of bytes as well as a size string, in `streaming.set` and in a profile, because `streaming.get` reports an integer and a JSON client should be able to send it back. The profile loader checks only the type and leaves the value to the parser the flag uses. The keep-after-close value stays string-only, since `get` reports its parsed structure, not a policy string. The `all:` prefix is matched case-insensitively and allows whitespace before the colon.

## Outcomes & Retrospective

The four Bazel gates passed at 0f02210 and the branch is published as draft PR #188. The round 1 fixes after that commit passed their focused suites and still need the four gates re-run on their head. The 2 GiB case uses a synthetic session with a declared size. The mounted cases use scratch files and the same control commands an operator runs.

## Context and Orientation

`storage/streaming.py` owns read sessions, which represent uncached files a client has opened. It records contiguous spans requested by readers separately from all ranges fetched into the partial cache file. The background pool starts once the requested span reaches one chunk or qualifies for retention. Qualification lets that download finish after its last reader closes. See `.planning/mount-control.md` for the control socket and session lifecycle.

## Plan of Work

Add a policy mode to `StreamingKeepAfterClose` in `storage/models.py`. Put parsing shared by startup and live control in `storage/streaming_settings.py` and register the module in `storage/BUILD`. Add the speculative window to profile resolution and forward it from `storage/cli_mount.py` through `storage/fs.py` to the manager. In the streaming manager, pick an unclaimed missing range between the end of the latest read span and the end of its window before qualification. Nothing in that range may extend beyond the window or the end of the file. Once qualified, use the existing full-file fill.

Extend `storage/cli_control.py` and the manager's control dispatch so a single request validates and applies the new settings alongside the scheduler limits. Reconsider pending work under the session lock so a parked open file can resume. Keep the legacy aggregate stats endpoint available.

## Concrete Steps

From the `retention-qualification` worktree, run the four Bazel quality gates using the shared runtime lock. The runner starts Bazel inside the Nix development shell and records a receipt for a clean commit after a pass.

    flock --close -x -w 1200 "$HOME/.cache/storage-orgs-20261110/storage175-mux-runtime-20261112.lock" systemd-run --user --scope -q -p CPUQuota=200% -p MemoryMax=8G -p MemorySwapMax=2G scripts/run-gates.sh

## Validation and Acceptance

`tests/test_fs_streaming.py` checks that a 1 MiB read of a synthetic 2 GiB file stops background work at 5 MiB until readers request a contiguous 644,245,095-byte span. It also checks the exact bound after a seek. `tests/test_mount_profiles.py` and `tests/test_cli_mount.py` cover legacy rules, larger-of rules, small files, and configuration precedence. `tests/test_streaming_system.py` checks both FUSE loops, including a 1 MiB read, a rejected mixed update, and the retention of an existing download after its qualification threshold is raised.

## Idempotence and Recovery

Repeat the gate command in a clean worktree. Live updates set explicit values and can be repeated. A mount restart restores the profile or flags. A background range already in flight can finish after a window change.

## Artifacts and Notes

Ditz tracks the implementation as `streaming-retention-qualification`. The default combined rule is `all:32M,30%` and the window is 4,194,304 bytes.

## Interfaces and Dependencies

The startup and live flags are `--streaming-keep-after-close` and `--streaming-read-ahead-window`. Their profile keys replace dashes with underscores. The mount control `streaming.set` request uses the same strings as the CLI and profile, and also accepts the window as a non-negative integer of bytes, so a JSON client can send back what `streaming.get` reported; the profile key accepts a TOML integer for the same reason. `streaming.get` reports an `any` or `all` rule with thresholds and a window size in bytes. The keep-after-close value stays string-only in `streaming.set`, because `get` reports its parsed structure rather than the policy string. The controls depend on the mount control branch and share its private cache-local socket.

2026-11-19. Recorded the implementation and the acceptance criteria for the combined branch.

2026-11-28. Recorded the gate pass at 0f02210, the draft PR, and the council round 1 fixes awaiting a gate re-run.
