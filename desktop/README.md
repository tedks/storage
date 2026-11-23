# Experimental Nautilus late additions

On Linux, this bridge can deliver names added by a later member of an
ongoing Storage directory listing to the ordinary native directory
monitor of an opted-in Nautilus process. A client stays on the
`file://` URI and uses normal Storage operations to resolve the new entry.
The bridge targets one mount and the exact monitor that registered for
the directory.

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

The mount and virtual directory paths each support up to 768 native
bytes. The UID-checked socket uses a mount generation and a bounded
journal of up to 128 additions. A monitor polls at roughly 500 ms
intervals, with at most one query outstanding and a maximum of 16
monitors in the process. Linux path checks require `openat2` and
`statx` mount IDs and exclude symlinks and nested mounts.

A delivery gap, invalidated or superseding snapshot, socket failure, or
mount identity change stops the affected subscription and logs a
warning. One later member adding more than 128 names can cause a gap.
Nautilus keeps the rows already on screen. Use Reload to enumerate the
current view, or navigate away and reopen it to attach a new monitor.
The tracer covers additions by late members in the
same progressive scan. Removal, replacement, metadata changes, refresh
of an already complete listing, and automatic reconciliation are
follow-up work. The worker checks the mount identity before handing off an
event, so a concurrent remount between that check and the GIO callback is
outside the tracer's qualification.
