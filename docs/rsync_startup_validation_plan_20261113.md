# Rsync upload contract at mount startup

State: design checkpoint, before probe implementation. Ted's ruling is recorded in `oq-rsync-upload-selection-args`: test configured rsync options at startup and reject a configuration that cannot meet Storage's selected-upload contract. This serves DESIGN.md's offline writes and no silent loss. It does not attribute the field re-verification logs.

## Assumptions and source boundary

- `StorageFS.write` records DIRTY, and `SyncExecutor` may publish CLEAN after `RsyncBackend.copy_from_local` returns. The callback records the local source hash as the member hash without measuring member bytes then. A successful rsync exit with a skipped file therefore violates the upload contract.
- The CLI's `create_storage_members` constructs `RsyncBackend` with no `rsync_args` today (`storage/mount_profiles.py`). Programmatic callers can pass arbitrary `rsync_args` through the documented constructor. Startup validation must cover both. The exact live profile and host options are unknown.
- `mount_storage` is the FUSE entry point. After checking mount options and permissions, it can call a concrete `RsyncBackend.validate_upload_contract()` **before** `_acquire_mount_lock_ex`, `StorageFS`, sync workers, or `FUSE` (`storage/fs.py`). An exception propagates to the CLI's existing mount error handler (`storage/cli_mount.py`); direct callers receive it. The validator uses no remote host, keys, member root, cache root, mountpoint, or state DB.
- Local rsync and remote-shell rsync share file-selection option semantics for the probe-safe subset below. The local probe does not establish remote availability, permissions, rsync-version compatibility, or safety for an arbitrary pathname filter. Runtime transport errors still leave the file non-CLEAN.

## Probe and argv confinement

The validator creates its own `TemporaryDirectory` and passes only files below that directory to the production `_run_rsync` command construction, with the upload's `--ignore-times` suffix. The fixed local file names and contents are not derived from the member configuration. It checks destination **bytes** after each successful subprocess, not just exit status or metadata:

1. Existing destination with different equal-length bytes and exactly matching mtime: catches default quick-check and `--size-only` skips.
2. Missing destination: catches `--existing`/`--ignore-non-existing` and dry runs.
3. Existing destination with a newer mtime and different bytes: catches `--update` and dry runs.
4. Existing destination with changed size or mtime: positive control for ordinary overwrite.

Before any subprocess, inspect each configured argv token using a **probe-safety grammar**. Permit only known options whose arguments cannot redirect reads/writes outside the temporary directory, change the source/destination list, delete data, or alter the file's bytes. Within that safe grammar, the behavioral cases decide whether an option is accepted; a syntactically permitted `--size-only`, `--update`, `--ignore-existing`, or `--dry-run` must fail on its observed bytes. A modest performance option such as `--bwlimit` can pass if the bounded probe finishes.

Reject unknown options and path-affecting/destructive options without executing them. Reject filename filters (`--exclude`, `--filter`, related forms) and size limits (`--max-size`, `--min-size`) as **unprovable from finite benign names and sizes**; a passing sample cannot certify a later production path or larger file. These rejections must name the unsupported option safely, without printing private paths or option values. A rejected configuration remains an explicit unsupported surface, not a claim that the option itself is always wrong. The [rsync manual](https://download.samba.org/pub/rsync/rsync.1) documents these option effects and that local transfers use whole-file mode by default; the probe verifies selection, while production SSH transfers retain rsync's delta algorithm because the fix adds no `--whole-file` flag.

Give each tiny-file probe a short timeout rather than `_run_rsync`'s normal minimum 300-second transfer timeout. A nonzero rsync exit, missing executable, timeout, or byte mismatch raises a clear member/case-specific startup error. Scratch is removed by its owning context manager. No probe will execute caller `--temp-dir`, `--backup-dir`, `--partial-dir`, remote-shell override, deletion, file-list, or unknown options. Without the safety grammar, an arbitrary configured argv could redirect or delete outside scratch; without the short timeout, a slow option could hold mount startup for minutes per case. Those are necessary guards, not general configuration machinery.

## Tests and acceptance limit

Edit `tests/test_rsync_backend.py` for actual local-rsync probes covering the four cases, safe benign options, skip/update/dry-run rejection, static filter/size-limit rejection before subprocess, and error propagation. Edit `tests/test_allow_other.py` for `mount_storage` validation before mount lock, state DB, and FUSE, with a passing default case. Keep the existing `tests/test_fs_revalidate.py` false-CLEAN and rewrite controls. These are the only existing test modules selected for this phase.

The first behavioral regression must fail on unchanged startup code, then pass with the validator; mutation-check the boundary test. Six focused nonmount Bazel invocations are authorized for this phase, one at a time under the existing flock, owned output base, 2 CPU/4 GiB/no swap, <=2 GiB total owned scratch, >=10 GiB host free; each <=10 minutes, combined <=30 minutes. Nix remains offline; unchanged locked public dependencies may resolve. Full gates, FUSE/Docker/SSH, deploy, merge, and field attribution remain outside this package.
