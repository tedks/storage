"""How the mounted suites run FUSE, in either loop mode.

Every mounted test mounts through run_fuse, so the suites run under the
options the real mount passes (storage.fs.fuse_mount_options, as
mount_storage computes them) and in both FUSE loop modes:
FUSE_THREAD_MODES parametrizes them (False is the single-threaded loop,
still the default; True is fuse_threads).
"""

import os
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import List, Union

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


# How long a plain unmount is retried while the mount is busy. A desktop
# session probes every new mount (GIO's trash monitor looks up /.Trash and
# /.Trash-<uid>), and over SSH members that lookup takes a member round
# trip; with fuse_threads it can still be in flight when the test ends, and
# the kernel refuses the unmount until it returns. Only a busy that outlasts
# this is a problem.
UNMOUNT_BUSY_RETRY = 10.0


def unmount_fuse(mount: Union[str, Path]) -> List[str]:
    """Unmount; return what went wrong, falling back to a lazy unmount."""
    problems: List[str] = []
    for flags in ("-u", "-uz"):
        for tool in ("fusermount", "fusermount3"):
            if shutil.which(tool) is None:
                continue
            deadline = time.monotonic() + (UNMOUNT_BUSY_RETRY if flags == "-u" else 0.0)
            while True:
                try:
                    result = subprocess.run([tool, flags, str(mount)], capture_output=True,
                                            text=True, timeout=30.0)
                except subprocess.TimeoutExpired:
                    problems.append(f"{tool} {flags} timed out")
                    break
                if result.returncode == 0:
                    return problems
                if "busy" in result.stderr and time.monotonic() < deadline:
                    time.sleep(0.2)
                    continue
                problems.append(f"{tool} {flags}: {result.stderr.strip()}")
                break
    problems.append(f"{mount} could not be unmounted")
    return problems


def stop_fuse(mount: Union[str, Path], thread: threading.Thread,
              join_timeout: float = 30.0) -> List[str]:
    """Unmount and wait for the FUSE thread; return what went wrong.

    The join matters with fuse_threads: libfuse joins its workers and runs
    destroy after the unmount returns, so a teardown that goes on at once
    can remove the cache or close the index under a request still running.
    """
    problems: List[str] = []
    if os.path.ismount(mount):
        problems += unmount_fuse(mount)
    thread.join(timeout=join_timeout)
    if thread.is_alive():
        problems.append("the FUSE thread did not finish after unmount")
    return problems
