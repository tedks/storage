# Contract: foreground-admission v1

- **Owner:** responsiveness lead; **consumers:** FUSE callback and mount CLI.
- **Inputs / outputs:** operation name and callable; unchanged callback result or exception. Configured active cap 3..128.
- **Behavioral invariants:** at most cap active Python callbacks; one active slot per metadata (including directory handles), demand-I/O (including warm cached opens), and ordinary (including cold opens) class is protected from the other classes; queued callbacks in one class enter in arrival order. A callback owns exactly one slot until it exits. A warm open that discovers a cache miss after revalidation or eviction releases demand capacity before waiting for ordinary capacity and fetching.
- **Failure semantics:** explicit close wakes queued callbacks with a cancellation error and does not interrupt a running callback; slot release happens on every exception, including during class transfer. Libfuse joins callbacks before `destroy`, so normal unmount may drain queued callbacks before close. Rejected configuration has no mount side effect.
- **Intentionally unspecified:** ordering across classes, native libfuse thread count, and the latency of a member call that already holds a slot.
- **Compatibility:** absent opt-in setting retains the prior FUSE loop; existing operation results and bytes are unchanged. Further changes in class mapping or reserve require a contract review.
- **Boundary tests:** `tests/test_fs_threading.py`, `tests/test_cli_mount.py`, `tests/test_mount_profiles.py`.
- **Interpretation register:** this document and the mission event shard.

