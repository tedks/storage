"""How the mounted suites run FUSE, in either loop mode.

Every mounted test mounts through run_fuse, so the suites run under the
options the real mount passes (storage.fs.fuse_mount_options, as
mount_storage computes them) and in both FUSE loop modes:
FUSE_THREAD_MODES parametrizes them (False is the single-threaded loop,
still the default; True is fuse_threads).
"""

from pathlib import Path
from typing import Union

from storage.fs import StorageFS, fuse_mount_options

FUSE_THREAD_MODES = (False, True)


def fuse_mode_id(fuse_threads: bool) -> str:
    return "threads" if fuse_threads else "nothreads"


def run_fuse(fs: StorageFS, mountpoint: Union[str, Path], *, fuse_threads: bool,
             **kernel_options) -> None:
    """Serve fs at mountpoint in the foreground until it is unmounted.

    Call it from a thread. kernel_options (entry_timeout, negative_timeout)
    go to fuse_mount_options, with the listing window and watcher setting
    taken from fs as mount_storage takes them from the profile. With
    fuse_threads, libfuse joins its workers before calling destroy, so an
    unmount returns only after every request in flight has.
    """
    from fuse import FUSE

    FUSE(
        fs,
        str(mountpoint),
        nothreads=not fuse_threads,
        foreground=True,
        allow_other=False,
        **fuse_mount_options(
            fs.backends,
            enable_watchers=fs._enable_watchers,
            fresh_window=fs._revalidate_after,
            **kernel_options,
        ),
    )
