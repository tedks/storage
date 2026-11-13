# PR179 frozen release run: failure and resource receipt

This records the no-args release run on pushed `00082c4ce4e3c821db6b161221434bd9bf529cfd` (tree `7aa1b78b69b3787befaaccbf88914ba7c3773fd7`) before any rebase. It is a failed run, not a gate receipt or field diagnosis. PR173 reached master during the run; no PR173 attribution follows from that timing.

## Separate observed outcomes

- The actual Bazel remote-test child inherited the owned Docker shim in invocation 1. Invocation 3 built `//:storage_cli` successfully on this head with wrapper exit 0. The four-invocation release envelope is exhausted (343 seconds total); network bytes are UNKNOWN. No peer output base was used.
- Invocation 4 ran `scripts/run-gates.sh` with no arguments in the owned batch Bazel base. `//tests:storage_tests_sync_system` completed with 24 passes and two failures. `TestMemberSymlinks.test_member_only_link_is_a_link_through_the_mount` got `ENOENT` on its first `os.lstat` of a link just made on a local member. `TestKernelCacheRefetchThreaded.test_shrunk_file_reads_short` got `ENOENT` on the initial read of a file just made on a local member. Both use `LocalBackend`, construct `StorageFS` directly and call `run_fuse`; neither invokes the changed `mount_storage` startup hook or `RsyncBackend`. They are unchanged tests. The traces do not establish a cause, including whether this is a pre-existing timing failure or interference from the external monitor.
- The remote module had collected 55 tests and printed 33 results when the wrapper stopped the scope. Its log had no failure before that stop; it has no pass receipt either. This abort is separate from the two completed `sync_system` failures.
- The wrapper stopped the scope at 312.43 seconds with `bound_error: scratch or free-space monitor read failed`, exit 90. Its last recorded scanner total was 1689 MiB and host free space was 3858265 MiB; observed pids peak 227 and active cgroup values were 2 CPU, 8 GiB memory, 2 GiB swap and 1024 tasks. Because the scanner may have entered FUSE mounts, its total is not reliable as owned scratch. The failure was an inability to measure, not an observed bound crossing.

## Monitor defect and observability gap

The external `run-bounded.py` calls `scratch_size_mb((STATE, OUTPUT, temp))` and `df -Pm /home/tedks/.cache/storage179-release-20261113` on each sample. `temp` was `/tmp/s179.T4d6oF`, whose test directories contained active FUSE mounts. The scanner recursively calls `lstat` and `scandir` without a mount boundary guard, so it could traverse a live test mount. That is a concrete monitor design defect; the frozen record does not prove that it caused either test failure, the monitor stop or the stranded request.

The wrapper logs one generic error when the scanner returns `None` (three-second deadline, unreadable `lstat` or `scandir`) **or** `df` fails/parsing fails. It recorded no branch, path, errno, command result or duration at the failed sample. Thus the exact failure mechanism is **UNKNOWN**. A later read-only scan of the same roots returned 1702 MiB in 2.63 seconds, close to the scanner deadline, but is only a clue. A prior CLI attempt also failed closed when `du` touched Bazel's zero-block `inaccessibleHelperDir`; that was a different identified failure and was corrected before invocation 4.

Before another run, the monitor must record branch-specific bounded diagnostics and exclude live FUSE trees without a check/use race. Prefer separate never-mounted measurement roots for the Bazel output and state, plus a hard-bounded test-temp filesystem or quota whose contents are never recursively scanned; account for every byte of real temp scratch under that bound. If that route is unavailable, descriptor-relative traversal with Linux `openat2(RESOLVE_NO_XDEV | RESOLVE_NO_SYMLINKS)` on each child could count `fstat` blocks on owned files while excluding mounted virtual data. The underlying mountpoint inode must be bounded conservatively, and all other unreadable entries must still fail closed. A mountinfo snapshot plus lexical pruning alone is insufficient because a mount can appear between check and use. Keep a finite scan deadline and record a distinct deadline result. The existing scan cannot safely attest the 10 GiB scratch invariant during mounted tests; simply omitting temp would leave its real files unaccounted.

## Owned connection cleanup

After the wrapper stop, the exact three synthetic containers (IDs beginning `65583b77`, `a1a922d1`, `323ec810`) were stopped and removed, and network `storage-test-2033064-709f5f` was removed; exact Docker inspect returned not found. One owned mount under `/tmp/s179.T4d6oF` was lazily detached. That did not release the process: PID 2033064 remained in `D/request_wait_answer`, with cgroup `storage179-all-gates-20261113.scope` nonempty. Its boot ID was `f7a567ad-dca3-4961-9bbb-d10beaf3fce4`, start ticks `26507231`, mount namespace `mnt:[4026531832]`, and FD 18 pointed to `/dev/fuse` with `fuse_connection: 97` in `fdinfo`. Its mountinfo no longer listed the owned mount. Root independently validated this identity and `waiting=1` on connection 97, then authorized exactly one connection-scoped abort.

The cleanup revalidated boot, PID/start, cgroup membership, namespace, wait channel, FD target and connection number; opened `/sys/fs/fuse/connections/97/abort` with `O_NOFOLLOW`, checked the opened inode against the path, revalidated the process, then wrote one byte. The first observation showed connection 97 gone and the PID waking; the next, within one second, showed the PID absent, `cgroup.procs` empty and connection 97 absent. `systemctl --user show` then reported inactive/dead and empty `ControlGroup`, `Result=success`. Root and PR174 owner received the exact release receipt. No scratch tree was deleted while the request was pending.

## Evidence and next verification

Owned logs are under `/home/tedks/.local/state/storage-179-bounded-20261113T2130Z/`: `storage179-all-gates-20261113.scope.log` (SHA256 `62bdb2233a9ef7569efbbe526065f0b7a573fecbd323c29a8d0a2892fe1f4396`), scope JSON (`f14271836129750397e57e1a393e45b0f41a5c20df2c12babca662f5b06aba8d`), and `abort97-once.log` (`e19bf9835e5836200169b6822de07dae9566c5cb9503339a5c5d2c66dc89d38f`). The wrapper and Docker shim SHA256 values were `cca3625eba3fb285be3aeace97906caee5c8dec7d6754a6c9193bb984819e27c` and `49dcd9357d6800b29deef739aa9c7112c7689e0f7722ad5f479529a67fd2b400`. The Bazel `sync_system` test log SHA256 is `d14835aedcf96cc88b4d31bef60110db90b0ca9b425eba02d7033d940d438bcb`; the interrupted remote log SHA256 is `e83389274c7c48b5b68449ba7dbf59ec5379497f4686fff9ae3fe07333978020`. Temporary `.bazelrc` had SHA256 `7fae22b956b287524208bebf1419094f35e359f5bdb770522275d5725d737a12`; its exact bytes before removal were:

```text
# Run-scoped Storage PR179 infrastructure config, not source or gate selection.
startup --batch
startup --host_jvm_args=-XX:ActiveProcessorCount=2
startup --host_jvm_args=-Xmx4g
startup --output_base=/home/tedks/.cache/storage179-release-20261113/bazel
build --jobs=2
build --loading_phase_threads=2
build --local_resources=cpu=2
build --local_resources=memory=8192
test --test_env=PATH
test --nocache_test_results
```

The smallest next package, if root grants a new envelope, is: review a race-safe external monitor correction; investigate the two unchanged mounted-test failures with a focused bounded run; then rebase only after the published master/PR174 sequence is fixed and renew formal council plus all four gates and CLI on that exact final head. There is no release acceptance, merge or deployment authority from the frozen run.
