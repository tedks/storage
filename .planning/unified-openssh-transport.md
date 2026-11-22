# Share one SSH connection across all remote operations

Maintain this ExecPlan in accordance with [.planning/PLANS.md](PLANS.md).
Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes &
Retrospective current. Work is tracked as `gh-29-unified-openssh`.

## Purpose / Big Picture

A mount with several remote roots on a compatible SSH host should be able
to list and read through SFTP, upload through rsync, and watch remote
changes over one authenticated connection. A background upload on one
root should leave an SSH session available for an interactive operation
on another root when the server has enough capacity. The same SSH login
policy should apply when that connection reconnects.

## Progress

- [x] (2026-11-22) Prove SFTP, watcher, and rsync can carry real traffic on
  one OpenSSH master with a three-session synthetic SSH server.
- [x] (2026-11-22) Implement and test the owned master, bounded lifecycle
  and typed attach-only session opens in draft PR #201.
- [x] (2026-11-22) Reuse the mount registry and integrate host admission for
  SFTP, exec, watcher, and rsync in draft PR #202.
- [x] (2026-11-22) Verify the explicit selector on mounted traffic at
  server session limits 1, 2, 3 and 10 in both FUSE loop modes.
- [ ] Compare mixed OpenSSH foreground latency against separate
  per-member connections under matched synthetic traffic. Verify a
  mount with two simultaneously active policy identities.

## Surprises & Discoveries

The focused Docker test ran against a real sshd with `MaxSessions=3`. Two
simultaneous SFTP clients listed different roots alongside the inotify
watcher. A fourth session was refused. After releasing the two SFTP sessions,
one reopened SFTP session and the watcher stayed live during an rsync upload.
The watcher observed the change and SFTP returned the uploaded bytes. sshd
reported one authenticated login and one established connection. After the
master exited, an attach-only request failed and the connection count
returned to zero.

Paramiko's SFTP implementation negotiates over a local socketpair handed to
`ssh -s sftp`. Its client requires the stream to provide `get_name()` for
logging in addition to ordinary socket operations and timeout methods.

OpenSSH's socket clients may try a fresh connection if the master is gone
or a session open is refused. The test sets the attach client's
`ProxyCommand=exit 255` before config is applied, so that fallback cannot
dial. The master itself uses its normal configured route.

## Decision Log

- 2026-11-22, unified transport lane. Use explicit OpenSSH selection for the
  production route. Paramiko and OpenSSH differ in password support, agent
  and key discovery, configuration for proxy routes, and storage of accepted
  host keys. Reuse requires an agreed policy.
- 2026-11-22, unified transport lane. Discover and account for host session
  capacity in one scheduler. A fixed minimum would disable existing usable
  small servers. The three-session test models the case where only one slot
  remains for transient bulk work after the watcher and interactive reserve.

- 2026-11-22, mixed mount. Follow the host contract in #197 and require
  configured trust for the opt-in. Let profile aliases use the configured
  OpenSSH port. Use a separate startup probe, scoped by compatible
  identity and closed before FUSE daemonizes.
- 2026-11-22, mixed mount. Prove simultaneous session opens up to the
  nominal Paramiko budget of six. Negotiate SFTP only on the first
  session and retain one warm idle SFTP client. Keep a watcher slot only
  when watching is enabled. At one or two sessions, report watching
  unsupported for the generation and retain pull listing revalidation.

## Outcomes & Retrospective

The focused Docker test passed in 4.2 seconds at code revision `69b128e`
in [draft PR #198](https://github.com/tedks/storage/pull/198). The tracer
uses the existing watcher and rsync command execution, including watcher
readiness and delivery. It prepares the upload directory outside rsync and
uses a test SFTP adapter outside the mount.

Draft PR #202 extends that tracer to a named profile and an ordinary FUSE
mount with an SSH root and an rsync root on one synthetic server. At
`3223fdb`, all ten focused Docker cases passed in 70.84 seconds. They
checked one serving authentication and one established TCP connection,
distinct from the closed pre-fork probe. Mounted reads completed during
uploads when at least two sessions were available. An active watcher
invalidated listings with at least three sessions. With watching disabled
at three sessions, two simultaneous uploads left the foreground slot
available. Unmount stopped inotifywait and closed all serving connections.
Owned-master generation recovery and strict trust are separately covered
by PR #201 at `a059864`.

## Context and Orientation

`storage/backends/ssh_pool.py` owns SFTP and short command leases.
`storage/backends/ssh.py` supplies Paramiko authentication and routes storage
operations through that pool. One SFTP session can stay open and idle for
reuse. Open sessions, including idle sessions, count toward sshd's
`MaxSessions`, the server's limit on concurrent sessions per connection.
`storage/backends/ssh_mux.py` supplies private control sockets to system SSH,
which `storage/backends/rsync.py` and `storage/watcher.py` use today. OpenSSH's
master holds the network connection and accepts session requests on the local
Unix socket. `storage/mount_profiles.py` builds the remote backends, and
`StorageFS._start_watchers` in `storage/fs.py` groups watcher roots.

## Plan of Work

First, build an OpenSSH connection owner in the backend package. Key the
mount-scoped registry by transport policy, SSH host token, effective user,
port, and key selection. Canonicalize only inputs with equivalent SSH config
matching. Keep alias, proxy, host-key, and auth inputs in the policy identity,
and keep the member root and per-root availability result outside it. Run
one foreground master per group. It should honor configured OpenSSH routes
and keys, retain batch mode, require a trusted host key, and keep `UpdateHostKeys=no`
in line with the existing command path. A master
generation owns a private socket path and all associated local processes.
Publish readiness only after the listener answers and fail attach-only
operations when the path is gone. Serialize replacement, reap the previous
generation before publishing the next, bound connect and teardown, and
invalidate old leases and waiters by generation. Keep the Paramiko route
available for current profiles and password-backed construction.

The attach interface needs bounded SFTP and command opens, distinct results
for a server session refusal and master loss, and a cancellable command
stream with exit status. Prove this before changing callers. Evaluate
OpenSSH's documented Unix-socket passenger protocol version 4 for typed
open and exit replies and passing stdin, stdout, and stderr descriptors.
A command-line passenger already works for data transfer. A production
adapter must resolve refusal separately from command exit status and bound
each operation. [OpenSSH passenger protocol](https://github.com/openssh/openssh-portable/blob/master/PROTOCOL.mux)

Next, put admittance for real server sessions under one condition per host.
Charge pending opens, open SFTP sessions, watcher, checksum command, and
rsync against the same generation. Serialize capacity probes so concurrent
pending opens never count as successful openings. Grow from successful
opens and learn a server ceiling from a confirmed session refusal while the
master remains live. Protect an actual interactive SFTP channel before
admitting transient bulk work against unverified capacity. Evict a healthy
idle bulk SFTP session when an exec or rsync request needs its slot. Keep
the existing operation and capacity timeouts distinct, preserve reentrant
SFTP operations and FUSE request cancellation, and wake blocked callers on
release, loss, or shutdown.

Give the watcher a revocable slot when capacity is scarce. At three
sessions it can coexist with one interactive and one bulk session. With
two sessions, suspend the watch when it would block the only bulk slot,
invalidate listing state on suspension, and resume watching and resync
when the slot is available. At one session the interactive and finite bulk
requests must take turns and logs must report the loss of a dedicated
reserve. Treat a refused watcher or command as capacity pressure only on
an identified live master. Availability of a root and health of the host
connection remain distinct.

Finally, wire the same owner and admission into `SSHBackend`,
`RsyncBackend`, and the watcher grouping. Rsync takes its session only
after the SFTP parent setup returns, avoiding a nested lease on a
one-session server. Keep rsync's existing arguments, binary streams, and
exit behavior. Watcher cancellation releases its slot after its local
process and session are cleaned up. Add the opt-in to the mount profile
only when all these entry points use the same owner, then document the
OpenSSH authentication and low-capacity behavior.

## Concrete Steps

From the worktree, reproduce the focused test under the shared remote-gate
lock used on the development host.

    flock --exclusive --nonblock --close "$HOME/.cache/storage-orgs-20261110/storage175-mux-runtime-20261112.lock" nix develop --command bazel test //tests:storage_tests_remote_system --test_arg=-k --test_arg=openssh_master_carries --test_output=errors

Expected result for the current tracer is one passing selected test.
Extend `tests/test_remote_system.py` for the mounted profile and register any
new unit modules in `tests/BUILD`. Run `scripts/run-gates.sh` from the clean
feature worktree before merge.

## Validation and Acceptance

In Docker, prepare two mounted roots on one key-authenticated host and
another root under a different compatible-policy group, with separate
credentials or an alias route that resolves to the same destination. Read
and list one root while an rsync upload to another holds a background
session. Verify the returned bytes, watcher invalidation and SSH connection
counts. Repeat at server limits of one, two, three and the default ten.
Saturate the background sessions with gated work before starting a
foreground read. Verify the reserve at two or more slots and eventual
progress for all transfer types at one slot. Test that temporarily shedding
the watcher allows foreground and bulk traffic and that its restart
discovers changes made while it was paused.

Exercise wrong keys, changed host keys, a configured proxy route, unavailable
private socket directories, and a master dropped mid-operation. Verify no
direct fallback or second login on the same generation, one replacement
after reconnect, no old-generation lease counted in the new one, and no
remaining watcher, master, mux path, or waiter after mount shutdown. Keep
the legacy default covered by the existing remote tests and verify the
explicit selector through ordinary mount configuration.

## Idempotence and Recovery

A failed attach releases its partial session and admission accounting. A
failed start releases its private socket and process tree before retry.
Keep paths and child records generation-scoped so cleanup cannot affect a
replacement connection. Use only the synthetic fixture to force disconnects.
A mount restarted with the default transport configuration uses its
existing Paramiko and system SSH paths.

## Artifacts and Notes

The current synthetic test lives in `tests/test_remote_system.py` under
`test_openssh_master_carries_sftp_watcher_and_rsync`. The uncached rerun
at `4a1aafe` reported the result below.

    //tests:storage_tests_remote_system  PASSED in 6.0s
    Executed 1 out of 1 test: 1 test passes.

## Interfaces and Dependencies

The new connection owner should expose `open_sftp(deadline)`,
`open_command(command, stdin_fd, stdout_fd, stderr_fd, deadline)`, and
`close()` on a particular authenticated generation. Command opens return
a session handle whose lifetime covers copying data and collecting a
remote exit status. Open refusal and loss are distinct errors, and each
failed open closes every partially acquired descriptor. Use the installed
OpenSSH executable for authenticated connections and Paramiko's SFTP client
for protocol operations. Add new backend sources to
`storage/backends/BUILD`.

The scheduler wraps this interface and owns physical opens and permits.
Backends obtain SFTP with the current operation timeout and request class,
while a checksum or rsync owns a finite command permit. A watcher obtains
its revocable permit through the same scheduler. Existing
`Backend.io_capacity_scope()` can expose only the shared scheduling identity
to the surrounding worker admission. Keep low-level session handles inside
the backend layer.

2026-11-22 revision. Recorded the uncached rerun and the dependency
boundary for starting the next package from the pushed tracer.
