# Agent Instructions: storage

Storage is a FUSE meta-filesystem that presents one abstract filesystem
split across multiple local and remote storage members, with a local LRU
cache, per-directory N-way replication configured through extended
attributes, and background sync. See [DESIGN.md](DESIGN.md) for the design
and [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) for what is built.

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
- `tests/` - pytest suites; `pytest_main*.py` are the entry points for the
  Bazel test targets; `benchmarks/` holds the benchmark suite
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
- `.claude/skills/` - agent skills vendored from dotfiles

## Environment

This repo uses a Nix flake (Bazel 7, JDK 11, Python 3.11). Run project
tooling through it:

```bash
nix develop --command <cmd>
```

If using direnv, `.envrc` is `use flake`.

The tests that mount FUSE (in all three gates) run only on Linux and need
more than the flake:

- **libfuse 2.** `fusepy` loads `libfuse.so.2`. If the host already has
  one in its `ldconfig` cache for the host architecture, `fusepy` finds it
  itself. Otherwise the dev shell sets `FUSE_LIBRARY_PATH` to nixpkgs'
  build. A non-empty value already in the environment always wins, and the
  shell banner's `libfuse2:` line shows the choice. The `storage_tests*`
  targets in `BUILD` pass the variable into the tests (`env_inherit`).
  nixpkgs' build is loaded into Bazel's Python, which uses the host glibc,
  so it fails to load on a host whose glibc is older than that build needs
  (at the current `flake.lock`, `` version `GLIBC_2.38' not found ``, as
  in this repo's Debian 12 `.devcontainer`). Fix that in `flake.nix`, not
  by installing a host package: add an older nixpkgs input whose libfuse 2
  the host can load, and use its package in `libfuse2Hook`. For example,
  nixos-23.05's `fuse` (2.9.9; `fuse2` did not exist yet) needs only
  `GLIBC_2.34` and loads on Debian 12. Without any libfuse 2, every test
  module that imports `fuse` fails at collection with
  `OSError: Unable to find libfuse`.
- **`/dev/fuse` and a setuid `fusermount`.** libfuse runs its built-in
  `fusermount` path first (`/run/wrappers/bin` for nixpkgs' build, which
  exists only on NixOS; `/bin` in Debian and Ubuntu builds), then the
  first `fusermount` on `PATH`. Whichever it finds must be setuid. The dev
  shell keeps nixpkgs' non-setuid `fusermount` off `PATH`, so do not put
  one ahead of `/usr/bin` yourself (for example with
  `nix profile install nixpkgs#fuse`).
- **No Bazel sandbox.** Mounts fail inside `linux-sandbox` with
  `fusermount: mount failed: Operation not permitted`, so the
  `storage_tests*` targets are tagged `local`: unsandboxed, and never
  remotely executed or cached. A new test target that mounts FUSE needs
  the same `env_inherit` and `tags`.

## Build and test

```bash
nix develop --command bazel build //:storage_cli
```

Test targets:

| Target | Runs |
|--------|------|
| `//:storage_tests_fast_gate` | `tests/test_*.py` except the mounted system tests |
| `//:storage_tests_mounted_system_gate` | `test_streaming_system.py`, `test_sync_system.py` (real FUSE mounts) |
| `//:storage_tests_resilience_gate` | `test_resilience_gradient.py`, `test_streaming_system.py` |
| `//:storage_tests` | all `tests/test_*.py` |

The quality gate before pushing code changes is all three gates (about
four minutes on framework0 with a warm Bazel cache, most of it the fast
gate):

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
first: check the shell banner's `libfuse2:` line, and check that the
targets in `BUILD` still carry `env_inherit` and the `local` tag.

If you cannot run them (for example, no FUSE on the host), say so
explicitly in the commit or PR rather than implying they passed.

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
- `storage debug rebuild-index` and `storage debug reprobe-locations`
  mutate state (SQLite index rows and cache-file xattrs: rebuild normalizes
  metadata xattrs, reprobe can write conflict xattrs).
  `storage debug verify-index` does not change an existing index or
  xattrs, but all three open the database through `StateManager`, which
  creates an empty one (and its parent directory) at a mistyped `-d` path.
  Do not point the mutating commands, or experimental mounts, at a live
  cache or at real mount profiles. A profile name given without `--config`
  is looked up in `~/.config/storage/storage.toml`, then
  `/etc/storage/storage.toml`.
- README.md lags the code (for example, it still lists remote backends
  and background sync as unimplemented); trust the code and
  IMPLEMENTATION_STATUS.md over it.
