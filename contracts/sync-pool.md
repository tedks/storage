# Contract: sync-pool v1

- **Owner:** responsiveness lead; **consumers:** FUSE release, sync queue and mount CLI.
- **Inputs / outputs:** minimum `workers`, optional `max_sync_workers`, queued sync tasks; unchanged sync outcomes and task identities.
- **Behavioral invariants:** distinct queued demand may start workers to the configured cap, never past it. Excess idle workers retire. Stop preserves real queued tasks and a restart does not overlap still-running workers from the prior generation.
- **Failure semantics:** partial start cleans up launched workers; a failed growth attempt leaves queued work for existing workers; stop has bounded grace and reports lingering work.
- **Intentionally unspecified:** exact worker count at an instant while tasks enter or retire and ordering among equal-priority tasks beyond the queue's existing rule.
- **Compatibility:** no `max_sync_workers` keeps the fixed pool. SSH/rsync configured ceiling is 8, local-only 32; existing `workers` below cap is preserved.
- **Boundary tests:** `tests/test_sync_manager.py`, `tests/test_cli_mount.py`, `tests/test_mount_profiles.py`.
- **Interpretation register:** this document and the mission event shard.
