# Agent Instructions: storage

Storage is a FUSE meta-filesystem that presents one abstract filesystem
split across multiple local and remote storage members, with a local LRU
cache, per-directory N-way replication configured through extended
attributes, and background sync. See [DESIGN.md](DESIGN.md) for the design
and its Goals and non-goals, and the Status section of [README.md](README.md)
for what is built.

## Scope of this file

Global workflow rules live in `~/.claude/CLAUDE.md` (installed from the
dotfiles repo) and apply to every project: landing the plane (session
completion and mandatory push), branch + draft-PR discipline, granular
commits, stacked PRs, the bare-repo/worktree layout, and Nix/Bazel
environment detection. **Do not restate them here.** This file covers only
what is specific to storage; if a rule belongs to every repo, it
belongs in the global instructions instead.

`AGENTS.md` is the canonical instruction file. Keep `CLAUDE.md` (and
`GEMINI.md`, `COPILOT.md` if present) as symlinks to it unless a specific
agent genuinely needs divergent instructions.

## Project structure

- `storage/` - the Python package: FUSE filesystem (`fs.py`), SQLite
  operational index (`state.py`), cache and cache-metadata xattrs
  (`cache.py`, `cache_metadata.py`), replication and sync workers, CLI
  (`cli*.py`), and storage member adapters in `backends/` (local, SSH,
  rsync)
- `storage_cli_main.py` - entry point for the `//:storage_cli` Bazel binary
- `tests/` - pytest suites; `pytest_main.py` is the entry point every
  pytest target runs (it collects the target's own test modules);
  `build_srcs_check.py` checks that BUILD lists every top-level
  `tests/test_*.py` and `storage/` module; `benchmarks/` holds the
  benchmark suite
- `BUILD`, `MODULE.bazel`, `WORKSPACE` - Bazel build; PyPI deps are pinned
  in `requirements.lock` (generated from `requirements.txt`)
- `third_party/` - forked-submodule vendoring workflow (see its README) and
  `update.sh`
- `flake.nix`, `.envrc`, `.devcontainer/` - development environment
- `scripts/` - manual mount/unmount helpers and `ensure-nix-shell.sh`
- `setup.py`, `setup-venv.sh` - non-Bazel pip/virtualenv install path
- `docs/` - refactor roadmap and feature plans
- `storage.example.toml` - example mount profile
- `.planning/PLANS.md` - ExecPlan format guide

## Environment

This repo uses a Nix flake (Bazel 7, JDK 11, Python 3.11). Run project
tooling through it:

```bash
nix develop --command <cmd>
```

If using direnv, `.envrc` is `use flake`.

The tests run only on Linux and need more than the flake. Every pytest
target needs libfuse 2; the ones that mount (run by the mounted and
resilience gates) also need `/dev/fuse` and a setuid `fusermount`:

- **libfuse 2.** `fusepy` loads `libfuse.so.2`. If the host already has
  one in its `ldconfig` cache for the host architecture, `fusepy` finds it
  itself. Otherwise the dev shell sets `FUSE_LIBRARY_PATH` to nixpkgs'
  build. A non-empty value already in the environment always wins, and the
  shell banner's `libfuse2:` line shows the choice. Every pytest `py_test`
  in `BUILD` passes the variable into the tests (`env_inherit`).
  nixpkgs' build is loaded into Bazel's Python, which uses the host glibc,
  so it fails to load on a host whose glibc is older than that build needs
  (at the current `flake.lock`, `` version `GLIBC_2.38' not found ``, as
  in this repo's Debian 12 `.devcontainer`). Fix that in `flake.nix`, not
  by installing a host libfuse 2: add an older nixpkgs input whose libfuse
  2 the host can load, and use its package in place of `pkgs.fuse2.out` in
  `libfuse2Hook`. The flake cannot see the host glibc, so this changes the
  fallback on every host without a libfuse 2; an older build still loads
  on newer glibc. For example, nixos-23.05's `fuse` (2.9.9; `fuse2` did not
  exist yet) needs only `GLIBC_2.34` and loads on Debian 12 and Ubuntu
  24.04. Update this paragraph and the flake comments to match, and enter
  a fresh shell (or `direnv reload`) afterwards, since a
  `FUSE_LIBRARY_PATH` already set by an older shell wins. Without any
  libfuse 2, every test module that imports `fuse` fails at collection
  with `OSError: Unable to find libfuse`.
- **`/dev/fuse` and a setuid `fusermount`.** libfuse runs its built-in
  `fusermount` path first (`/run/wrappers/bin` for nixpkgs' build, which
  exists only on NixOS; `/bin` in Debian and Ubuntu builds), then the
  first `fusermount` on `PATH`. Whichever it finds must be setuid. The dev
  shell keeps nixpkgs' non-setuid `fusermount` off `PATH`, so do not put
  one ahead of `/usr/bin` yourself (for example with
  `nix profile install nixpkgs#fuse`).
- **No Bazel sandbox for mounts.** Mounts fail inside `linux-sandbox` with
  `fusermount: mount failed: Operation not permitted`, so the `py_test`s
  for the modules that mount are tagged `local`: unsandboxed, and never
  remotely executed or cached. Every other pytest target runs in the
  sandbox. (`storage_build_srcs_check`, which imports no fuse, is also
  `local` because it reads the source tree.)

## Build and test

```bash
nix develop --command bazel build //:storage_cli
```

`BUILD` lists every source explicitly; there are no globs. Add a new test
module to the `srcs` of `storage_tests_sandboxed`, or, if it mounts FUSE,
give it its own `py_test` tagged `local` (copy `storage_tests_sync_system`)
and add that to the mounted gate. New `storage/` modules go in
`storage_lib`'s `srcs`. `//:storage_build_srcs_check`, part of the fast
gate, fails when a `tests/test_*.py` or `storage/` module on disk is not
listed.

The gates are `test_suite`s over those `py_test`s:

| Target | Runs |
|--------|------|
| `//:storage_tests_fast_gate` | `storage_tests_nonmounting` (`storage_tests_sandboxed`, `storage_tests_resilience_gradient`: every module that does not mount, sandboxed) and `storage_build_srcs_check` |
| `//:storage_tests_mounted_system_gate` | `storage_tests_conflicts_integration`, `storage_tests_streaming_system`, `storage_tests_sync_system`: the modules that mount FUSE, unsandboxed |
| `//:storage_tests_resilience_gate` | `storage_tests_resilience_gradient`, `storage_tests_streaming_system` |
| `//:storage_tests` | the fast and mounted gates: every module except the remote gate's |
| `//:storage_tests_remote_gate` | `storage_tests_remote_system`: the SSH backend, rsync over SSH and the remote watcher against sshd in Docker containers; needs Docker (see below) |

Extra pytest flags reach a target's modules through `--test_arg`, for
example `bazel test //:storage_tests_sandboxed --test_arg=-k
--test_arg=rename`. Aim it at a single `py_test`: every target gets the
same flags, and one where `-k` selects nothing fails (pytest exits 5).
Bazel's `--test_filter` is ignored.

The quality gate before pushing code changes is all three gates (about
four minutes on framework0 with a warm Bazel cache, most of it
`storage_tests_sandboxed`). Bazel runs each `py_test` once, so the
resilience gate's overlap costs nothing:

```bash
nix develop --command bazel test \
  //:storage_tests_fast_gate //:storage_tests_mounted_system_gate //:storage_tests_resilience_gate \
  --test_output=errors
```

The benchmarks in `tests/benchmarks/` are not part of any Bazel target.
A red run whose failures are libfuse loading errors
(`Unable to find libfuse`, `GLIBC_... not found`) or
`fusermount: mount failed: Operation not permitted` usually means an
environment problem (see Environment), not a code signal, and it does not
count as running the gate. Rule out a regression in the setup itself
first: check the shell banner's `libfuse2:` line, and check that every
pytest `py_test` in `BUILD` still has `env_inherit` and the mounting ones
are still tagged `local`.

If you cannot run them (for example, no FUSE on the host), say so
explicitly in the commit or PR rather than implying they passed.

### The remote gate

`//:storage_tests_remote_gate` runs `tests/test_remote_system.py` against
real sshd: two containers built from `tests/remote/`, the only coverage of
the SSH backend, rsync over SSH and the inotify watcher talking to an
actual remote. It is not in `//:storage_tests` or the three gates above,
so hosts without Docker are unaffected. Run it, in addition to them,
before pushing a change to `storage/backends/ssh.py`,
`storage/backends/rsync.py`, `storage/watcher.py` or the remote paths of
the filesystem:

```bash
nix develop --command bazel test //:storage_tests_remote_gate --test_output=errors
```

It needs a reachable Docker daemon (the user in the `docker` group) and
`ssh`, `ssh-keygen` and `docker` on `PATH`. Without Docker it fails with a
message naming Docker; it never skips. The first run builds the image
(Debian packages from the network); later runs reuse it until anything in
`tests/remote/` changes, since the tag is a hash of that directory. The
target inherits `DOCKER_HOST`, `DOCKER_CONTEXT` and `DOCKER_CONFIG`.
The members share a network named by the run id; when the host's Docker
address pools are exhausted (other projects' networks), the fixture warns
and uses the default bridge instead. Tests connect to the container IPs
on port 22; on a host that cannot route to Docker's bridge they fall back
to ports published on 127.0.0.1, and `STORAGE_TEST_REMOTE_PUBLISHED=1`
forces that fallback.

What the fixtures create, they name `storage-test-*` and remove by exact
name: containers `storage-test-<pid>-<hex>-remote0`/`-remote1`, a network
`storage-test-<pid>-<hex>`, and the keys under the test tmp dir. The image
`storage-test-sshd:<hash>` stays as a cache; the fixture removes older
tags of it when it builds a new one. After an interrupted run, check
`docker ps -a` and `docker network ls` for `storage-test-` leftovers and
remove those by name.

The fixture puts an `ssh` shim first on `PATH`. `RsyncBackend` and
`RemoteWatcher` run the system `ssh` with
`StrictHostKeyChecking=accept-new` and cannot be given a known-hosts file
([#107](https://github.com/tedks/storage/issues/107)). Run as they are,
they would add every throwaway container host key to
`~/.ssh/known_hosts` and fail when a later run reuses an IP. The shim
runs the real `ssh` with `-F` and a per-run config whose known_hosts
holds the generated host keys, and it logs each invocation so tests can
assert which commands went over ssh. Remove it when #107 lets those call
sites take a known-hosts file. `SSHBackend` uses paramiko and does not
read or write `~/.ssh`.

`test_watcher_stop_ends_remote_inotifywait` is a strict xfail for
[#106](https://github.com/tedks/storage/issues/106) (the remote
`inotifywait` outlives `RemoteWatcher.stop()`). Fixing #106 turns it
red; then remove the marker.

## Issue tracking (ditz)

Issues live as plain-text YAML on the `ditz-metadata` git branch, one
file per issue; the `ditz` CLI reads and writes them. Nothing appears in
the working tree, and no per-worktree setup is needed — any worktree of
this repo can run `ditz`. Install via Nix:
`nix run github:tedks/ditz -- <cmd>` (or install `github:tedks/ditz#ditz`).

```bash
ditz add "title"              # file a new issue (-t bugfix|feature|task, -c <component>)
ditz ready                    # find available work (unblocked, ranked)
ditz show <id>                # view issue details (add --json for machine output)
ditz start <id>               # claim work (marks in_progress)
ditz close <id> --reason "…"  # complete work (or --wontfix / --reorg)
ditz comment <id> "message"   # add a progress note
ditz sync                     # fetch/merge/push the ditz-metadata branch
```

Include `ditz sync` in the end-of-session push workflow:

```bash
git pull --rebase
ditz sync
git push
```

Use `ditz` commands rather than hand-editing the `ditz-metadata` branch.
Ditz has no priority / label / assignee fields: grouping is by component
(`-c <component>`), sequencing is by the dependency graph
(`ditz blocks <a> <b>`), and urgency is derived. Use a deterministic id
with `--id <name>` when you need an idempotent add — re-creating with the
same id is a no-op, which is what makes `ditz add` safe to call from
unattended jobs.

### GitHub Issues and ditz epics

This repo uses both trackers, at different altitudes:

- **GitHub Issues** hold the bigger things: the problem statement, design
  discussion, and progress notes a human reads.
- **ditz** holds the tactical breakdown. A GitHub issue `#N` that is being
  worked has a ditz epic with id `gh-N` whose description links back to
  the issue. The epic's member tasks (ids `gh-N-<slug>`) each block the epic
  (`ditz blocks gh-N-<slug> gh-N`), and ordering between members is
  expressed the same way. The epic stays out of `ditz ready` until every
  member closes.

Not every open GitHub issue has an epic yet. When you open an issue, or
pick up an existing one that has no epic, create the epic and at least the
first concrete member task. Check for the epic with
`ditz list --ids-only | grep -qx gh-N`, not `ditz show gh-N`: `show`
matches id prefixes, so it can display a different issue (or report an
ambiguous match) when `gh-N` itself does not exist. When the last member
closes, close the epic and the GitHub issue together. Small,
self-contained work that needs no design record can be a plain ditz issue
with no GitHub issue.

## Planning

ExecPlans for non-trivial work follow the format in
[.planning/PLANS.md](.planning/PLANS.md).

## Repo specifics

- Do not use emojis in commit messages or documentation.
- Metadata authority: file and directory policy lives in the
  `user.storage.config` xattr and cached files carry `user.storage.metadata`.
  The SQLite index is rebuildable acceleration state, so do not make it the
  only record of anything that cannot be recovered from xattrs or a member
  reprobe.
- A cached file whose live size or mtime differs from the stat recorded in
  its index row or its metadata xattr is dirty, whatever status the record
  says: the record predates a write. `CacheMetadataRecord.to_file_metadata`
  applies this for rebuild-index, `CacheManager.heal_stale_clean` for the
  open path and the post-crash sweep. The comparison is exact (both sides
  come from `stat`); what it cannot see is a same-size write in the same
  timestamp tick as the record, a few ms on kernels without fine-grained
  timestamps. Anything that changes a cached file's bytes or mtime goes
  through `CacheManager.mark_file_dirty`, which keeps the records in step;
  a change that bypasses it looks like a lost write and gets resynced.
- Sync status is owned by `files.dirty_generation`. Record a modification
  with `StateManager.mark_dirty` (one UPDATE that bumps the generation) and
  a sync outcome with `publish_sync_status` (conditioned on the generation
  the worker read). Never read a row, change `sync_status` and write it
  back: `set_file_metadata` is for creating rows and changing policy, and it
  refuses to move the content columns backwards past a newer generation.
- `storage debug rebuild-index` and `storage debug reprobe-locations`
  mutate state (SQLite index rows and cache-file xattrs: rebuild normalizes
  metadata xattrs, reprobe can write conflict xattrs).
  `storage debug verify-index` does not change an existing index or
  xattrs, but all three open the database through `StateManager`, which
  creates an empty one (and its parent directory) at a mistyped `-d` path.
  Do not point the mutating commands, or experimental mounts, at a live
  cache or at real mount profiles. One concrete reason: the metadata xattr
  does not carry `dirty_generation`, so a rebuild restarts every row at 0,
  and a sync worker still holding an older generation could then have a
  stale CLEAN accepted once the counter climbs back to it (ditz
  `gh-96-xattr-generation`). A profile name given without `--config`
  is looked up in `~/.config/storage/storage.toml`, then
  `/etc/storage/storage.toml`.
- README.md's Status section is the public summary of what is built and
  what is not; update it when a feature lands. Known problems live in the
  issue tracker, not in the README. The code is the authority when they
  disagree.
