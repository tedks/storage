# Context manifest: mounted restart design

- **Mission:** source-grounded design for mounted maintenance, reconfiguration, and restart; no deployment or application code.
- **Worktree / base:** `docs/mounted-restart-design-20261111` at `66c640aec005feccd0bd585f3355295c3bb772ba` when claimed.
- **Governing context:** assigned mission; repo `AGENTS.md` and `DESIGN.md`; org `DOCTRINE.md`, `ROLES.md`, `orgs-spec`, `orgs-decompose`, and `orgs-ledger` instructions.
- **Code read:** `storage/fs.py`, `sync.py`, `sync_executor.py`, `state.py`, `cache.py`, `streaming.py`, `namespace.py`, `watcher.py`, `backends/ssh.py`, `backends/ssh_pool.py`, `cli_mount.py`, plus dependency pins.
- **External primary evidence:** Linux kernel FUSE overview, fusepy 3.0.1 source, libfuse low-level API reference; URLs in the report.
- **Boundaries:** no live mounts, backing/cache inspection, SSH or host probes, FUSE activation, Docker, benchmark/test execution, application code, or sibling-worktree edits. PR 171 is proposal only; PR 170 gate incident remains unresolved.
- **Deliverable:** `docs/mounted_restart_design_2026-11-11.md`, own status and event shard, follow-up and product-ruling ditz issues, a draft PR for root review.
