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
- [x] (2026-11-28) Fixed council rounds 3 to 6 and the early lead review: fd-pinned publication, the cache-root rule, mount identity (mountpoint and cache identity) on every profile or cache request, Unicode-sized frames, the final `.`/`..` refusal, O_PATH pin-then-connect in the client, the legacy EOF frame, distinct cancel exit codes, peer-check tests with mutation checks, and a subprocess reader in the mounted test.
- [x] (2026-11-28) Rebased onto `master` after #185 merged as `77f2983`, keeping #185's behaviour; the PR now targets `master`.
- [x] (2026-11-28) Fixed the post-integration round (P1 to P5): replacement and withdrawal act only on an entry moved into a private stage and verified there, stages are proven by fd and an O_EXCL nonce and swept after a crash, profile matching follows a final mountpoint symlink, the mounted test reaps its reader and bounds its waits, and the response bounds are tested.
- [x] (2026-11-28) Applied the e819934 dispositions. Stale-socket takeover uses atomic exchange and holds the probed inode until the decision is final. Withdrawal requires atomic no-replace rename. Stages left by a crash are kept for manual recovery. Mountpoint parents are checked by a kernel directory lookup.
- [ ] Run the full gate (`scripts/run-gates.sh`) on the exact head before merge. That result, and the revision it covers, is recorded on PR #186, not here.

## Surprises & Discoveries

The current control socket uses one `recv` on a stream, which can yield partial requests. A control request needs complete line framing independent of transport chunk boundaries. The legacy CLI writes `streaming-stats` plus a newline and reads through connection close.

## Decision Log

- Decision (2026-11-19, Codex): Identify a mount by its cache directory or a configured profile that names the cache directory. This is the identity already encoded in the socket path. Use explicit file listing to expose session IDs, and allow cancellation only by exact ID so a replacement at the same path is unaffected.
- Decision (2026-11-19, Codex): Keep the legacy request and response and add one JSON object per newline-terminated control request. Bound request reads by byte count and wall time. Keep the endpoint private from publication and verify Unix peer ownership.
- Decision (2026-11-28): Every request names the cache directory by (st_dev, st_ino), taken from one O_PATH open of the cache that the client also connects through, and the mount refuses a mismatch; a socket moved into another cache cannot answer for it. The server pins its cache directory fd while listening so the identity cannot be reused.
- Decision (2026-11-28): With `--profile`, requests also name the canonical mountpoint. Validate the original parent with a kernel directory lookup and read the final component with readlink through that directory fd, so a hung mount does not block the lookup. A final `.` or `..` is refused. Such a mount still publishes its socket with no mountpoint, reachable by `--cache`.
- Decision (2026-11-28): The client opens the socket entry with O_PATH | O_NOFOLLOW, checks that fd (a socket owned by this user) and connects through /proc/self/fd, so the entry checked is the entry connected to.
- Decision (2026-11-28): An unterminated frame is accepted only when it is exactly `streaming-stats`, for older clients that ended the request with EOF.
- Decision (2026-11-28): `cancel` exits 3 for busy and 4 for not-found (1 for other errors, 2 for usage); a busy reply says why (`readers`, `finalizing`, `stopping`).
- Decision (2026-11-28): A group writer can replace names in the cache. Publication and withdrawal use a private stage opened with O_NOFOLLOW and checked for ownership, mode and emptiness. For takeover, pin the public inode with O_PATH and probe it for a refused connection. Exchange the staged and public entries with renameat2, then unlink only the probed inode or exchange the entries back if the check fails. Keep the pin through the decision. Shutdown moves the pinned socket to the stage and restores a swapped entry with no-replace rename. Hard-link fallback is limited to publishing at a free name. A stage name and nonce cannot establish provenance, so crash leftovers stay for manual recovery.

## Outcomes & Retrospective

The operator command, socket transport, documentation, and scratch tests are implemented and in draft PR #186. Council round 1 found missing fast-gate coverage of the dispatcher and four smaller transport issues, all fixed on the branch. The full-gate result for the head that merges is recorded on the PR. Later rounds hardened the socket against a group writer of the cache and against the wrong mount answering (shared caches, moved sockets, retargeted mountpoint links); each fix has a test, and the security tests were mutation-checked.

## Context and Orientation

`storage/streaming.py` owns download sessions. Its legacy `_streaming_stats_snapshot` produces an aggregate without member filenames. `storage/cli_debug.py` exposes that snapshot by sending a short command to `.storage.control.sock` in the target mount's cache. FUSE `init` starts the server after daemonization and FUSE `destroy` stops it. `storage/mount_profiles.py` already resolves profile names through an explicit TOML file or the standard mounts config paths.

Each running download gets a new session ID when its partial is created, so a path can look the same after a cancellation or completion even though its ID has changed. Closing the last reader may leave a background download if the configured rule keeps read-ahead after close.

## Plan of Work

Add `storage/control.py` for the private socket lifecycle and framing. Dispatch the new commands from `StreamingReadManager`, which already owns all affected state. Add `storage/cli_control.py`, register its Click group in `storage/cli.py`, reuse the profile resolver, and share the client transport with the legacy CLI. Add focused socket and CLI tests and add the end-to-end test to the existing streaming mounted suite.

## Concrete Steps

Run these commands from the `mount-control` worktree. Use the shared runtime lock for heavy tests.

    nix develop --command bazel build //:storage_cli
    nix develop --command bazel test //tests:storage_test_cli_control //tests:storage_test_cli_debug //tests:storage_tests_streaming_system //:storage_build_srcs_check --test_output=all

Run `scripts/run-gates.sh` from the final clean worktree to record full-gate evidence. A mounted run should report passes from both the default and threaded FUSE modes. The branch is on `master`; rerun the gates on the exact head before merge.

## Validation and Acceptance

Prove that `status` omits filenames while `downloads` shows the live session with its Unicode path. Change running limits and verify that an excess cold open follows the manager's fallback. Submit mixed valid and invalid settings and verify all values remain at their earlier settings. Refuse cancellation while a reader is open, then cancel after its last close. Reopen the same path and verify the old ID affects no new session. Query the same running mount with legacy debug stats. Exercise socket permissions, existing endpoints, short and oversized frames, and clean shutdown independently.

## Idempotence and Recovery

Live changes are in-memory and set explicit values, so repeating an update is safe. An already cancelled or vanished ID returns `not-found`. The tests use short-lived scratch caches and leave mounted directories intact on a failed teardown for recovery.

## Artifacts and Notes

The parent lane maintains the CTO roster. Its mission and these tests prohibit touching a field mount.

## Interfaces and Dependencies

Depend on `StreamingReadManager.download_limits`, `update_download_limits` (a dict of limits and/or keywords), `list_download_sessions`, and `cancel_download_session` (on `master` since #185), plus `cancel_download_session_with_reason`, which this PR adds (5acb47a) for the busy reasons. `storage/control.py` owns the socket: `ControlServer`, `request_control_payload`, `open_cache_directory` and `canonical_mountpoint`. The socket handler validates command and argument shape, and the streaming manager validates parameter values and applies updates in one operation. The initial live parameters are `max_streaming_sessions` (1 to 1024), `max_retained_streaming_sessions` (0 to 1024), and `max_download_workers` (1 to 32).

2026-11-19: Recorded the assigned worktree, agreed API, and mount-level test. Recorded the fast gate results and the rebase onto the integrated scheduler.

2026-11-28: Marked the contract tests and draft PR done, recorded the council round 1 fixes, and pointed the final gate result at the PR. Replaced the stale instruction to rebase onto the reader-downloads branch, which is done.

2026-11-28: Recorded rounds 3 to 6, the lead review, the rebase onto master and the post-integration fixes; added their decisions and the interfaces they rely on.
