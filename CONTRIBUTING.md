# Contributing

Storage is alpha software maintained by one person; see the Status section
of [README.md](README.md) before relying on it.

- Build and test through the Nix dev shell and Bazel as described in
  [AGENTS.md](AGENTS.md); run all the gates, the remote gate included, on
  the exact commit you propose with `scripts/run-gates.sh`, and say in the
  PR which host ran them. The remote gate needs Docker; if your host has
  none, run it on one that does (`STORAGE_REMOTE_GATE_HOST`, see AGENTS.md)
  or say so in the PR and the maintainer will run it before merging.
- Open a PR against `master`; non-trivial changes get a multi-provider
  review to fixpoint, with the record posted on the PR.
- Known problems live in the issue tracker, not in the README.
