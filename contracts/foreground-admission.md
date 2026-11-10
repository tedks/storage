# Contract: foreground-admission v1

- **Owner:** responsiveness lead; **consumers:** FUSE callback and mount CLI.
- **Inputs / outputs:** operation name and callable; unchanged callback result or exception. Configured active cap 3..128.
- **Behavioral invariants:** at most cap active Python callbacks; one active slot per metadata, demand-I/O and ordinary class is protected from the other classes; queued callbacks in one class enter in arrival order. A callback owns exactly one slot until it exits.
- **Failure semantics:** close wakes queued callbacks with a cancellation error and does not interrupt a running callback; slot release happens on every exception. Rejected configuration has no mount side effect.
- **Intentionally unspecified:** ordering across classes, native libfuse thread count, and the latency of a member call that already holds a slot.
- **Compatibility:** absent opt-in setting retains the prior FUSE loop; existing operation results and bytes are unchanged. A change in class mapping or reserve is a v2 review.
- **Boundary tests:** `tests/test_fs_threading.py`, `tests/test_cli_mount.py`, `tests/test_mount_profiles.py`.
- **Interpretation register:** this document and the mission event shard.

