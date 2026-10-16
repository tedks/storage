# Work package: WP-REMOTE

- **State:** DRAFT (needs a CEO ruling on hosts before READY)
- **Owner (role):** Lead-tier lane, storage · **Model:** to be recorded
- **Base revision:** master @ 00048dc
- **Intent:** DESIGN.md's main use case is remote members over SSH, and
  nothing in the automated suite talks to one: every "remote" in the tests
  is a local directory flagged remote. The SSH backend, the inotify watcher
  and rsync over SSH are covered only by field use from early 2026, on code
  that has since been refactored. A real-remote gate is what makes the rest
  of the backlog (#27/#29, #23, #50, #24) reproducible and verifiable.
- **Instruction:** add a gate that exercises the SSH backend, the watcher
  end to end (a real `inotifywait` over a real `ssh`), and rsync over SSH
  against a real sshd. Preferred shape: an sshd started inside the Nix dev
  shell on localhost with a generated host key and an authorized key, so
  the gate runs on any Linux host without network; an opt-in second mode
  targets a named host from an environment variable. Follow the doctrine's
  verification waterfall: keep one representative real run in the gate.
- **Non-goals:** no benchmark work; no change to backend semantics beyond
  what the real run exposes (file those as issues).
- **Owned scope:** `flake.nix` (an sshd and inotify-tools in the dev
  shell; this is inside the package's scope by explicit grant), `BUILD`
  (a new `py_test` tagged `local` and a `storage_tests_remote_gate`
  suite), a new `tests/test_remote_system.py`, AGENTS.md's gate table.
- **Dependencies:** `storage/backends/ssh.py`, `storage/backends/rsync.py`,
  `storage/watcher.py` as published.
- **Acceptance criteria:** the gate passes on framework0 with no network;
  it fails (not skips) when sshd cannot start; it runs an SSH mount, a
  write that syncs over rsync, a remote-side change that invalidates the
  cache through the watcher, and a streaming read; AGENTS.md names the new
  gate and when it runs.
- **Boundary tests:** the existing three gates keep passing.
- **Budget:** two lane sessions.
- **Authority:** as WP-074, plus the explicit grant to edit `flake.nix` and
  `BUILD` within the stated scope.
- **Stop conditions:** sshd cannot be made to run unprivileged in the dev
  shell after three approaches (then `NEEDS RULING` on using drynwyn
  instead); any libfuse/glibc regression on tower0 or drynwyn (see AGENTS.md).
- **Escalation destination:** CTO; the host question to the CEO.
- **Deviation envelope:** default.
- **Expected artifact:** one PR, possibly stacked (dev-shell sshd first,
  then the gate).
- **Open before READY:** CEO ruling on whether drynwyn may be a gate target
  at all (`NEEDS RULING`, filed in ditz `open-questions`).
