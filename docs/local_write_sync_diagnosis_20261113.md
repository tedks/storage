# Local write to rsync member: bounded diagnosis

State: blocked before behavioral reproduction. Source base `66c640aec005feccd0bd585f3355295c3bb772ba`; no live cache, member, profile, mount, or log identifiers were inspected. The field cause remains unproven.

## Source trace and what is measured

1. A canonical `StorageFS.write` writes to the local cache and calls `CacheManager.mark_file_dirty` (`storage/fs.py`, `write`); `create`, `truncate`, and timestamp changes also record modifications. `CacheManager.mark_file_dirty` calls `StateManager.mark_dirty_from_cache_file`, which bumps `dirty_generation` and records DIRTY, size, and mtime in the index, then mirrors the row to the cache-file xattr (`storage/cache.py`, `mark_file_dirty`; `storage/state.py`, `mark_dirty`). Direct writes to the cache outside Storage do not take this supported path.
2. Canonical `release` flushes and queues DIRTY work (`storage/fs.py`, `_release`). `SyncExecutor._process_file_task` snapshots the generation, records SYNCING, hashes the cache file, checks for conflicts, calls the member's `copy_from_local`, calls `sync`, then reports the cache checksum to `member_written_cb` if the cache file identity stayed fixed (`storage/sync_executor.py`). An overlapping Storage write moves the generation and prevents the old attempt from publishing CLEAN.
3. `StorageFS._record_member_written` stats the member after the call and persists that member's basis with the checksum passed by the executor (`storage/fs.py`). **That checksum is inferred from successful upload and unchanged local identity; it is not a measured member hash.** A successful member call also records a location and may publish CLEAN. A transfer exception counts as a failed member and cannot make a sole-replica file CLEAN.
4. `RsyncBackend.copy_from_local` uses `_run_rsync`, which currently constructs `rsync -a` without `--ignore-times` or `--checksum` unless caller options supply one (`storage/backends/rsync.py`). The already-tracked quick-check hypothesis (`gh-112-rsync-quickcheck`) is that same-size, same-mtime destination bytes can be skipped while rsync exits successfully. This source inspection establishes the possible false-clean route, not a runtime occurrence.
5. Reverification stats, hashes, and stats the member again against its persisted basis (`storage/fs.py`, `_reverify_member_copy`). `_settle_reverify` logs a mismatching measured digest and replaces the persisted member digest; it does not itself trigger resync. Both a genuinely rewritten member and a skipped upload followed by a false inferred baseline could lead to that log. The supplied INFO lines cannot distinguish them.

## Draft synthetic regression

`tests/test_rsync_backend.py` calls the production `copy_from_local` and real `_run_rsync` subprocess with two local files whose distinct bytes have identical size and nanosecond mtime. Only SSH metadata and remote-spec routing are replaced with local paths.

`tests/test_fs_revalidate.py` additionally drives `StorageFS.create`/`write`/`release`/`utimens`, the real `SyncExecutor` settlement and `StorageFS._record_member_written` callback, using local rsync for transfer. It covers changed-size success, equal-size/equal-mtime selection, a measured member rewrite at an unchanged SFTP-like basis, and failure remaining non-CLEAN. The conflict decision and SFTP metadata transport are bounded by test doubles. Neither a FUSE mount nor SSH remote is exercised.

These tests are **unrun**. They do not yet prove the skip, the callback outcome, the proposed fix, or a mutation check. In particular, no behavioral red/green result or exact pushed-source test result exists.

## Test prerequisite and recommendation

Focused Bazel invocation 1/4 ran under `storage-local-write-sync-test1.scope` with 2 CPU, 4 GiB, no swap, one owned output base, `--repository_disable_download`, and the shared repository cache. It stopped during `rules_python` module-extension analysis, before test execution: PyPI simple metadata for packages including `nh3`, `idna`, `rich`, and `twine` was absent. This is the package's no-cached-input-route stop condition. No package acquisition was attempted.

Once a bounded cached route exists, run the draft regression on unchanged production source to observe the predicted behavioral red. If confirmed, the minimal proposed fix is to require `--ignore-times` on `copy_from_local` after caller-supplied rsync arguments, leaving read behavior, `-a`, and rsync delta transfer intact. Rerun the regression on the fix and again with the fix removed. Check arguments that can still skip a selected upload (for example `--ignore-existing` or `--update`) and obtain a product ruling before changing their contract. A successful local regression would still not attribute the field logs without file/profile evidence.

The CLI build, all four release gates, formal council, and root review have not run. This branch is not merge ready.
