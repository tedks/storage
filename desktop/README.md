# Experimental Nautilus directory updates

On Linux, the bridge reconciles an opted-in Nautilus process's native
directory monitor with the committed Storage view. It reports added,
removed and changed children while Nautilus stays on the ordinary
`file://` URI. Each subscription belongs to one directory and one mount
generation.

## Run the experiment

1. Build against the host Nautilus's GIO and JSON-GLib development
   packages. On Ubuntu these are `libglib2.0-dev` and `libjson-glib-dev`.
   From the Storage worktree, run
   `nix develop --command bazel build //desktop:nautilus_bridge`.
   The library is `bazel-bin/desktop/libstorage-nautilus.so`.
2. Set `progressive_listings = true` and `negative_timeout = 0` in the
   Storage mount profile. Launch Storage with
   `STORAGE_EXPERIMENTAL_DESKTOP_ADDITIONS=1` and wait for its
   control socket. Run Storage and Nautilus as the same user.
3. Start a fresh Nautilus process. Set `STORAGE_NAUTILUS_MOUNT` to the
   canonical absolute mount path, `STORAGE_NAUTILUS_SOCKET` to the
   absolute path to `.storage.control.sock` in its cache, and
   `LD_PRELOAD` to the absolute library path for that invocation.
   Nautilus normally reuses a running instance for new windows. See
   [the isolated acceptance test](acceptance/README.md) to try this in a
   private desktop session. Child processes can inherit `LD_PRELOAD`.
   The loaded library activates only in a process whose executable
   basename is `nautilus`. Keep the variable scoped to the dedicated
   launch command.

The mount and virtual directory paths each support up to 768 native bytes.
The UID-checked socket reports snapshot revisions, and the bridge compares
a new committed view with the last one it received. Skipped revisions
coalesce into the latest view. Under temporary socket failure the monitor
keeps its last view and continues polling. A new mount generation requires
a new monitor. Linux path checks require `openat2` and `statx` mount IDs
and exclude symlinks and nested mounts.

The bridge polls about every 500 ms with one request per monitor and up to
16 monitors per process. Reconciliation accepts up to 8,192 entries
per directory. Above that limit the subscription reports degraded
delivery and can recover when the directory comes back within the bound.
Metadata changes in a directory snapshot do not fetch replacement file
bytes. A cached file's published size follows the local generation.
