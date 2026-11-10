# Work package: FR-foreground

- **State:** READY after SIMPLIFY necessity critique; red admission test required before code.
- **Owner:** Sol lead acting as implementer; OpenAI Sol.
- **Base revision:** `48b5ea0`.
- **Intent:** Keep DESIGN's browsing and demand-read path responsive while cold opens or other callbacks saturate the multithreaded FUSE loop.
- **Instruction:** Add opt-in bounded active callback admission with protected metadata and read capacity; expose the cap through CLI/profile; preserve callback results, exception propagation and shutdown.
- **Non-goals:** hard native libfuse thread cap, CPU-load policy, field benchmarking, manual mount activation.
- **Owned scope:** `storage/fs.py`, new admission module, `storage/cli_mount.py`, `storage/mount_profiles.py`, existing relevant test modules, README and example config.
- **Dependencies:** `foreground-admission v1`, `member-io-class v1`.
- **Acceptance criteria:** a pre-fix synthetic test shows cold opens delaying browsing/read admission; saturated ordinary callbacks still allow metadata and read callbacks through protected slots; cap 3 gives one slot to each class; queued callbacks in each class are FIFO and progress when their own active call exits; never exceed active cap; queued cancellation and close release no slots and do not interrupt active member I/O; callback bytes/errors unchanged; configuration validates before mount side effects; opt-out remains single-threaded. A member with one usable SSH channel is explicitly outside the reserve guarantee.
- **Boundary tests:** `tests/test_fs_threading.py`, `tests/test_cli_mount.py`, `tests/test_mount_profiles.py`.
- **Budget:** bounded source package, one focused red/green cycle and one local gate cycle; escalate repeated third failure.
- **Authority:** scratch synthetic tests and branch edits; push draft PR. Exact-head all-gates and council remain required; root accepts/merges.
- **Worker bound:** one implementer at a time because config and `fs.py` overlap the sync package.
- **Evidence:** pending, to map each item to tested SHA and command.
- **Stop conditions:** changed product scope, live-data need, boundary collision, third unresolved failure.
- **Escalation destination:** root coordinator at `%87`.
- **Expected artifact:** commits on this branch and draft PR succeeding #167.
