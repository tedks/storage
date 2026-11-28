# Operate a running Storage mount from the command line

This ExecPlan follows `.planning/PLANS.md`. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.

## Purpose / Big Picture

A mount owner can inspect live download activity, change limits without restarting the mount, and abandon a particular background download. The control command uses the same cache-local Unix socket as `storage debug streaming-stats`.

## Progress

- [x] (2026-11-19 00:09Z) Created Codex lane worktree `mount-control` on `feat/mount-control` at `59e8d28` and agreed on the streaming manager API with the reader-downloads lane.
- [x] (2026-11-19 00:19Z) Implemented the control socket framing and owner checks, the CLI, and operator documentation.
- [x] (2026-11-19) Added the CLI and server contract tests and the scratch FUSE mount test, which runs the built CLI in both loop modes (`tests/test_streaming_system.py`). Before the rebase, Bazel built the CLI, passed the source manifest check, and passed 5 control and 37 debug tests.
- [x] (2026-11-19 00:22Z) Rebased on the integrated scheduler and member-attribute changes.
- [x] Opened draft PR #186, stacked on `feat/adaptive-reader-downloads`.
- [x] (2026-11-28) Fixed council round 1 findings: the control dispatcher's rejections are tested in the sandboxed fast gate, handler errors are logged and answered instead of dropped, the read timeout reply is a fixed message, the cache lock comment says it does not wait, and the server restarts after a stop whose join timed out.
- [ ] Run the full gate (`scripts/run-gates.sh`) on the final head. That result, and the revision it covers, is recorded on PR #186, not here.

## Surprises & Discoveries

The current control socket uses one `recv` on a stream, which can yield partial requests. A control request needs complete line framing independent of transport chunk boundaries. The legacy CLI writes `streaming-stats` plus a newline and reads through connection close.

## Decision Log

- Decision (2026-11-19, Codex): Identify a mount by its cache directory or a configured profile that names the cache directory. This is the identity already encoded in the socket path. Use explicit file listing to expose session IDs, and allow cancellation only by exact ID so a replacement at the same path is unaffected.
- Decision (2026-11-19, Codex): Keep the legacy request and response and add one JSON object per newline-terminated control request. Bound request reads by byte count and wall time. Keep the endpoint private from publication and verify Unix peer ownership.

## Outcomes & Retrospective

The operator command, socket transport, documentation, and scratch tests are implemented and in draft PR #186. Council round 1 found missing fast-gate coverage of the dispatcher and four smaller transport issues, all fixed on the branch. The full-gate result for the head that merges is recorded on the PR.

## Context and Orientation

`storage/streaming.py` owns download sessions. Its legacy `_streaming_stats_snapshot` produces an aggregate without member filenames. `storage/cli_debug.py` exposes that snapshot by sending a short command to `.storage.control.sock` in the target mount's cache. FUSE `init` starts the server after daemonization and FUSE `destroy` stops it. `storage/mount_profiles.py` already resolves profile names through an explicit TOML file or the standard mounts config paths.

Each running download gets a new session ID when its partial is created, so a path can look the same after a cancellation or completion even though its ID has changed. Closing the last reader may leave a background download if the configured rule keeps read-ahead after close.

## Plan of Work

Add `storage/control.py` for the private socket lifecycle and framing. Dispatch the new commands from `StreamingReadManager`, which already owns all affected state. Add `storage/cli_control.py`, register its Click group in `storage/cli.py`, reuse the profile resolver, and share the client transport with the legacy CLI. Add focused socket and CLI tests and add the end-to-end test to the existing streaming mounted suite.

## Concrete Steps

Run these commands from the `mount-control` worktree. Use the shared runtime lock for heavy tests.

    nix develop --command bazel build //:storage_cli
    nix develop --command bazel test //tests:storage_test_cli_control //tests:storage_test_cli_debug //tests:storage_tests_streaming_system //:storage_build_srcs_check --test_output=all

Run `scripts/run-gates.sh` from the final clean worktree to record full-gate evidence. A mounted run should report passes from both the default and threaded FUSE modes. The branch is stacked on `feat/adaptive-reader-downloads`; when that merges, rebase onto `master` and rerun the gates.

## Validation and Acceptance

Prove that `status` omits filenames while `downloads` shows the live session with its Unicode path. Change running limits and verify that an excess cold open follows the manager's fallback. Submit mixed valid and invalid settings and verify all values remain at their earlier settings. Refuse cancellation while a reader is open, then cancel after its last close. Reopen the same path and verify the old ID affects no new session. Query the same running mount with legacy debug stats. Exercise socket permissions, existing endpoints, short and oversized frames, and clean shutdown independently.

## Idempotence and Recovery

Live changes are in-memory and set explicit values, so repeating an update is safe. An already cancelled or vanished ID returns `not-found`. The tests use short-lived scratch caches and leave mounted directories intact on a failed teardown for recovery.

## Artifacts and Notes

The parent lane maintains the CTO roster. Its mission and these tests prohibit touching a field mount.

## Interfaces and Dependencies

Depend on `StreamingReadManager.download_limits`, `update_download_limits`, `list_download_sessions`, and `cancel_download_session` from the reader-downloads branch. The socket handler validates command and argument shape, and the streaming manager validates parameter values and applies updates in one operation. The initial live parameters are `max_streaming_sessions` (1 to 1024), `max_retained_streaming_sessions` (0 to 1024), and `max_download_workers` (1 to 32).

2026-11-19: Recorded the assigned worktree, agreed API, and mount-level test. Recorded the fast gate results and the rebase onto the integrated scheduler.

2026-11-28: Marked the contract tests and draft PR done, recorded the council round 1 fixes, and pointed the final gate result at the PR. Replaced the stale instruction to rebase onto the reader-downloads branch, which is done.
