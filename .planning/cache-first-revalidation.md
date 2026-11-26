# Serve Storage reads from local observations while members refresh

This ExecPlan follows .planning/PLANS.md. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.

## Purpose / Big Picture

A user reopening a directory or file should be able to use already observed entries and complete cached bytes while a member is slow or disconnected. A cold directory should show the first available contribution. New complete file generations should reach later opens while earlier readers keep their bytes. An opted-in Nautilus window should converge as Storage commits new observations.

## Progress

- [x] (2026-11-26 19:39Z) Proved that an expired listing waited on a blocked member and that the first change serves its entries and child attributes locally.
- [x] (2026-11-26 19:47Z) Added bounded persisted member observations and partial refresh publication.
- [x] (2026-11-26 20:01Z) Implemented bounded read-only validation and verified buffered generations on a synthetic mount with same-size, grow and shrink replacements.
- [ ] Extend the staged desktop bridge, exercise a synthetic mount, review the draft, and record exact-head gates.

## Surprises & Discoveries

The staged listing code already has per-directory publication tickets, separate member executors, and a progressive listing mode. The initial barrier test timed out on the staged source and passed after removing the age cutoff for a warm hit. The existing Nautilus preload only emits CREATED events for additions during the same scan. The local host has Nautilus, Docker, and a FUSE device. Its default native build lacks GIO and JSON-GLib headers, and Xvfb is absent from the system path.

## Decision Log

- (2026-11-26 / Storage lead) Age controls when to check a directory. Positive attributes can survive expiry. A conclusive negative answer uses a fresh successful observation from each configured candidate. The existing zero-TTL setting still explicitly bypasses a warm listing.
- (2026-11-26 / Storage lead) A read-only hit can schedule content validation. Directory refresh will continue to use namespace metadata, without downloading a replacement for a changed file. Remote size and times are evidence for a generation only to the degree that the backend reports them.

## Outcomes & Retrospective

The first regression exercises Storage's normal readdir and getattr entry points with a held member. The staged FUSE mount reproduced mixed old mapping pages with identical-size and growing replacements. The default draft kept buffered mmap valid across overlapping opens and published after final unmap, including shrink. The test-only direct-I/O variant passed this host’s MAP_PRIVATE probe. Startup, directory refresh and desktop checks use synthetic paths.

## Context and Orientation

storage/namespace.py holds the union of member listings, cached local names, conflict views and the attributes returned by getattr. Its publication ticket rejects a scan that raced invalidation. storage/state.py holds the rebuildable index. storage/fs.py routes FUSE opens and protects local writes using per-path locks and indexed dirty generations. storage/cache.py lands downloads through a temporary inode. storage/streaming.py caches sparse ranges for cold reads. storage/desktop_additions.py serves the private mount control protocol used by desktop/nautilus_additions.c inside Nautilus.

## Plan of Work

First, keep successful observations usable while a background scan runs. Preserve an unsuccessful member's observed entries without using them to prove absence. Record bounded snapshots in the rebuildable index and combine them with current local names on restart. Exercise a real blocked member, a successful empty contribution, and a local change that races publication with the integration tests.

Next, arrange validation from a cached read-only open. The network work runs without holding a FUSE path lock. Install a staged complete copy only while the cache identity, dirty generation and repair state still authorize replacement. A reader keeps the descriptor it originally opened. A missing stream range needs evidence that its source is still serving the session's generation.

Finally, publish ordered observations to the opted-in desktop monitor. Reconcile the monitor's known names against a committed directory view after a delivery gap, using bounded polling and the existing same-user and mount checks. Extend the isolated Nautilus fixture so it exercises a late arrival, removal and attribute change.

## Concrete Steps

From the assigned feature worktree, run the focused barrier and the full namespace suite through Bazel.

    nix develop --command bazel test //tests:storage_test_fs_namespace_integration --test_output=errors
    nix develop --command bazel test //tests:storage_test_fs_revalidate //tests:storage_test_fs_streaming --test_output=errors
    scripts/run-gates.sh

The desktop build and acceptance commands are in desktop/acceptance/README.md. Use temporary synthetic members and the isolated desktop fixture.

## Validation and Acceptance

While a member is held, list an expired directory, stat its child, and read exact old bytes from the complete cache. Repeat after restarting. With the member released, wait for publication, and compare exact old and new bytes on separate descriptors. A stale or failed observation must preserve good data and a local change must win a racing scan. In Nautilus, an existing window should show the refreshed union after a delivery gap. Record what the local environment can verify.

## Idempotence and Recovery

The gates and mount fixtures are repeatable against synthetic data. An unsuccessful refresh leaves the previous usable observation in place. A failed installation must leave the previous cache file intact. The SQLite snapshot data is rebuildable by scanning members.

## Artifacts and Notes

The initial test is test_expired_listing_and_child_attrs_survive_blocked_refresh in tests/test_fs_namespace_integration.py. Its baseline failure was a TimeoutError while waiting for the first readdir under the blocked-member barrier.

## Interfaces and Dependencies

Use NamespaceService's listing ticket and publication path, and StateManager for durable listing acceleration. Keep the mount control socket's existing peer-UID and mount checks. Use the existing cache budget, dirty generation, descriptor registration, and repair fence when publishing a replacement file.

2026-11-26. Updated the plan with the persisted observations, deferred-publication decision and mounted evidence.
