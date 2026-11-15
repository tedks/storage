# Work package: FR-sync

- **State:** READY after SIMPLIFY necessity critique; execute after FR-foreground's config interface is stable.
- **Owner:** Sol lead acting as implementer; OpenAI Sol.
- **Base revision:** `48b5ea0` plus the accepted foreground package head.
- **Intent:** Demand can grow background sync without monopolizing foreground channels or losing queued writes.
- **Instruction:** Port the reviewed #167 sync-pool commits into this branch, resolve current master/config collisions deliberately, and keep `remote0` identity, display labels and `UpdateHostKeys=no` intact.
- **Non-goals:** treat background-only scaling as mission completion; redesign sync scheduling or conflict policy.
- **Owned scope:** `storage/sync.py`, `storage/fs.py`, CLI/profile surfaces, `tests/test_sync_manager.py` and existing config tests, README/example config.
- **Dependencies:** `sync-pool v1`, `member-io-class v1`, foreground package's published config behavior.
- **Acceptance criteria:** old fixed default; bounded growth up to SSH/local ceiling, no cap overshoot or lost task at dequeue/stop; excess worker retirement; partial startup cleanup; stop/restart with lingering worker protection; all prior #167 synthetic evidence reproduced against this branch.
- **Boundary tests:** `tests/test_sync_manager.py`, `tests/test_cli_mount.py`, `tests/test_mount_profiles.py`.
- **Budget:** reuse four #167 commits after a range diff, one targeted red/green reconciliation and focused gates; do not iterate indefinitely on a conflict.
- **Authority:** read-only inspection of #167, edits/cherry-picks only in this branch; branch push and draft PR; no sibling force push, merge or deploy.
- **Worker bound:** sequential with foreground package because shared files collide.
- **Evidence on combined pre-fix tree `effadd7` plus two compatibility lines:** `git range-diff` maps all four #167 commits to `afd5318`, `630b76d`, `cc70ba6`, `effadd7`. Merge conflicts resolved by retaining current-master display labels and the foreground cap alongside sync config. `//tests:storage_test_sync_manager`, `//tests:storage_test_cli_mount`, `//tests:storage_test_mount_profiles`, `//tests:storage_test_fs_threading`, and `//:storage_build_srcs_check` passed on the combined tree. Full `//tests:storage_test_ssh_backend` first exposed a subclass that bypasses `StorageFS.__init__`; making the optional gate lookup tolerant restored its full 114-test pass. Exact-head all-gates, CLI build, council and lead review remain pending. Original #167's prior receipt is not this branch's evidence.
- **Stop conditions:** changed product scope, interface collision that cannot be reconciled without a ruling, third unresolved failure.
- **Escalation destination:** root coordinator at `%87`.
- **Expected artifact:** this branch's commits and draft PR explicitly superseding #167 without silently closing it.
