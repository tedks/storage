# Contributing

Storage is alpha software maintained by one person; see the Status section
of [README.md](README.md) before relying on it.

- Build and test through the Nix dev shell and Bazel as described in
  [AGENTS.md](AGENTS.md); run all three test gates on the exact commit you
  propose, and say in the PR which host ran them.
- Open a PR against `master`; non-trivial changes get a multi-provider
  review to fixpoint, with the record posted on the PR.
- Known problems live in the issue tracker, not in the README.
