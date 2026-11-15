# Contract: member-io-class v1

- **Owner:** responsiveness lead; **consumers:** cache fetch, namespace, streaming, SSH pool.
- **Inputs / outputs:** scoped context marking a member call interactive or bulk; no change to member method arguments, bytes or errors.
- **Behavioral invariants:** metadata browsing and demand range reads are interactive; full-file copies and background sync/conflict checks are bulk. Nested bulk scope restores the caller's classification on success and failure. An SSH member with at least two usable SFTP channels keeps its configured interactive reserve available to an interactive waiter.
- **Failure semantics:** a one-channel server has no reserve; request timeout/cancellation leaves no leased channel. Pool close wakes waiters.
- **Intentionally unspecified:** exact wait time while all channels of a class are held and server-side concurrency beyond the negotiated pool cap.
- **Compatibility:** stable member IDs, display labels and SSH host-key policy do not change.
- **Boundary tests:** `tests/test_fs_threading.py`, `tests/test_ssh_backend.py` and existing remote gate.
- **Interpretation register:** this document and the mission event shard.

