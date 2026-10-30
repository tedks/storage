"""How the mounted suites run FUSE, in either loop mode.

Every mounted test mounts through run_fuse, so the suites run under the
options the real mount passes (storage.fs.fuse_mount_options, as
mount_storage computes them) and in both FUSE loop modes:
FUSE_THREAD_MODES parametrizes them (False is the single-threaded loop,
still the default; True is fuse_threads).
"""

import ctypes
import os
import shutil
import subprocess
import sys
import threading
import time
import traceback
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

    _record_fuse_pointer(fs)
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


def _record_fuse_pointer(fs: StorageFS) -> None:
    """Keep the libfuse handle a request sees on the FUSE thread, for
    release_orphaned_loop. fusepy's fuse_exit() only works inside a callback."""
    import fuse
    real_destroy = fs.destroy
    fs.destroy_entered = threading.Event()
    fs.fuse_handle = None  # a second mount of this fs must not reuse a freed handle

    def destroy(path):
        fs.destroy_entered.set()
        return real_destroy(path)

    fs.destroy = destroy

    real_getattr = fs.getattr

    def getattr_(path, fh=None):
        # A request carries the context; init's does not reliably.
        if fs.fuse_handle is None:
            handle = ctypes.c_void_p(fuse._libfuse.fuse_get_context().contents.fuse)
            if handle.value:  # off a FUSE thread libfuse hands back a zeroed context
                fs.fuse_handle = handle
        return real_getattr(path, fh)

    fs.getattr = getattr_


# tgkill(2) numbers; os.tgkill does not exist before Python 3.13.
_SYS_TGKILL = {"x86_64": 234, "aarch64": 131}
# The kernel's name for the machine says nothing about a 32-bit userland.
ORPHAN_RELEASE_SUPPORTED = (os.uname().machine in _SYS_TGKILL
                            and ctypes.sizeof(ctypes.c_void_p) == 8)


def _noop_signal(*_):
    """SIGUSR2 only has to interrupt a blocked read."""



def release_orphaned_loop(fs: StorageFS, thread: threading.Thread,
                          wait: float = 10.0) -> str:
    """End a FUSE loop that outlived its unmount; "" if the thread finished,
    otherwise why it did not (for the caller's failure message).

    The kernel aborts a connection, which wakes the loop's read of /dev/fuse,
    only when the last mount of the filesystem goes. A process in another
    mount namespace that copied this mount (a Bazel linux-sandbox started
    meanwhile, a container) keeps the filesystem alive after our umount
    succeeded, and the loop then sits in the read until that process exits,
    however long that is. This asks the loop to exit (fuse_exit) and
    interrupts its reads, so libfuse runs destroy as usual.

    It acts only on that situation: the mount is gone from this namespace,
    destroy has not been entered, and a thread of this process is parked in
    fuse_dev_do_read. A destroy that hangs is not touched; the caller reports it.
    """
    import signal
    handle = getattr(fs, "fuse_handle", None)
    tgkill = _SYS_TGKILL.get(os.uname().machine) if ORPHAN_RELEASE_SUPPORTED else None
    if handle is None:
        return "no libfuse handle recorded (no request seen)"
    if tgkill is None:
        return f"no tgkill number for {os.uname().machine}"
    if fs.destroy_entered.is_set():
        return "destroy was entered; it is hung, not orphaned"
    if not thread.is_alive():
        return ""
    if threading.current_thread() is not threading.main_thread():
        return "not on the main thread; cannot install a signal handler"
    readers = []
    for task in Path("/proc/self/task").iterdir():
        try:
            if (task / "wchan").read_text().strip() == "fuse_dev_do_read":
                readers.append(int(task.name))
        except OSError:
            continue
    if not readers:
        return "no thread is reading /dev/fuse"
    import fuse
    libc = ctypes.CDLL(None, use_errno=True)
    # Installed once and never restored: a signal still pending when this
    # returns (a reader that had it blocked, or a thread id reused) would meet
    # the default disposition, which ends the whole test process. Nothing else
    # here uses SIGUSR2.
    if signal.getsignal(signal.SIGUSR2) is not _noop_signal:
        signal.signal(signal.SIGUSR2, _noop_signal)
    # Known gap: the holder can exit between the check above and this call,
    # letting libfuse tear the session down first; fuse_exit then writes freed
    # memory. Only a process in another namespace exiting in that window
    # triggers it.
    fuse._libfuse.fuse_exit(handle)
    deadline = time.monotonic() + wait
    failed = 0
    first_errno = 0
    while thread.is_alive() and time.monotonic() < deadline:
        for tid in readers:
            if libc.syscall(ctypes.c_long(tgkill), ctypes.c_long(os.getpid()),
                            ctypes.c_long(tid), ctypes.c_long(int(signal.SIGUSR2))) != 0:
                failed += 1
                first_errno = first_errno or ctypes.get_errno()
        thread.join(timeout=0.2)
    if failed:
        return f"{failed} tgkill calls failed (first errno {first_errno})"
    return "" if not thread.is_alive() else f"readers {readers} did not leave after fuse_exit and SIGUSR2"


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


ORPHAN_GRACE = 5.0


def stop_fuse(mount: Union[str, Path], thread: threading.Thread,
              join_timeout: float = 30.0, fs: Union[StorageFS, None] = None) -> List[str]:
    """Unmount and wait for the FUSE thread; return what went wrong.

    The join matters with fuse_threads: libfuse joins its workers and runs
    destroy after the unmount returns, so a teardown that goes on at once
    can remove the cache or close the index under a request still running.
    """
    problems: List[str] = []
    if os.path.ismount(mount):
        problems += unmount_fuse(mount)
    deadline = time.monotonic() + join_timeout
    thread.join(timeout=min(join_timeout, ORPHAN_GRACE))
    why = ""
    if thread.is_alive() and fs is not None and not os.path.ismount(mount):
        why = release_orphaned_loop(fs, thread, wait=max(0.0, min(10.0, deadline - time.monotonic())))
    thread.join(timeout=max(0.0, deadline - time.monotonic()))
    if thread.is_alive():
        problems.append("the FUSE thread did not finish after unmount"
                        + (f" (orphan release: {why})" if why else ""))
        problems.append(_thread_stacks())
    return problems


def native_thread_state(thread: threading.Thread, mount: Union[str, Path]) -> str:
    """What the kernel says about a FUSE thread stuck inside libfuse's C loop."""
    tid = thread.native_id
    out = [f"still a mount point: {os.path.ismount(mount)}"]
    for name in ("wchan", "syscall", "stat"):
        try:
            out.append(f"{name}: " + Path(f"/proc/self/task/{tid}/{name}").read_text().strip())
        except OSError as exc:
            out.append(f"{name}: unreadable ({exc})")
    try:
        entries = [e for e in Path("/proc/self/fdinfo").iterdir()]
        out.append(f"open fds: {len(entries)}")
        mounted = [line for line in Path("/proc/self/mountinfo").read_text().splitlines()
                   if str(mount) in line]
        out.append(f"mountinfo lines for the mount: {len(mounted)}")
    except OSError as exc:
        out.append(f"fd/mountinfo unreadable ({exc})")
    out.append(_other_holders(mount))
    return "\n".join(out)


def _other_holders(mount: Union[str, Path]) -> str:
    """Processes whose mount namespace still lists the mount, or that hold a
    descriptor or working directory under it (command names and counts only)."""
    needle = str(mount)
    mine = os.readlink("/proc/self/ns/mnt")
    listing, holders = [], []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            comm = (proc / "comm").read_text().strip()
            ns = os.readlink(proc / "ns" / "mnt")
            if f" {needle} " in (proc / "mountinfo").read_text():
                listing.append(f"{comm}[{'same ns' if ns == mine else 'other ns'}]")
            for target in [os.readlink(proc / "cwd")] + [
                    os.readlink(f) for f in (proc / "fd").iterdir()]:
                if target == needle or target.startswith(needle + "/"):
                    holders.append(comm)
        except OSError:
            continue
    return f"mount listed by: {sorted(listing)}; fd/cwd under it: {sorted(holders)}"


def _thread_stacks() -> str:
    """Every live thread's stack, so a hung teardown says where it is stuck."""
    names = {t.ident: t.name for t in threading.enumerate()}
    parts = []
    for ident, frame in sys._current_frames().items():
        parts.append(f"--- thread {names.get(ident, ident)}\n" + "".join(traceback.format_stack(frame)))
    return "\n".join(parts)
