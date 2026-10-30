"""
What Python thread state survives between FUSE callbacks.

fusepy reaches Python through ctypes callbacks. With nothreads=True every
callback runs on the thread that called FUSE(). With threads, libfuse 2 runs
callbacks on worker threads it creates itself, foreign to Python: ctypes
gives each callback a Python thread state for its duration only. Per-thread
caches (threading.local, or a dict keyed by thread identity with a weakref to
the thread object) then behave differently from what they do on a Python
thread. The SSH channel pool must not depend on thread identity either way;
these tests record what libfuse actually does, for the threaded mount to
come.
"""

import errno
import os
import shutil
import signal
import stat
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path

from fuse import FUSE, FuseOSError, Operations


class _ProbeOperations(Operations):
    """Answers getattr for /probe-N and records the thread state of each call."""

    def __init__(self):
        self.local = threading.local()
        self.lock = threading.Lock()
        self.calls = []

    def getattr(self, path, fh=None):
        if path == "/":
            return {"st_mode": stat.S_IFDIR | 0o755, "st_nlink": 2}
        if not path.startswith("/probe-"):
            raise FuseOSError(errno.ENOENT)
        thread = threading.current_thread()
        seen_before = getattr(self.local, "marker", None)
        self.local.marker = path
        with self.lock:
            self.calls.append({
                "path": path,
                "ident": threading.get_ident(),
                "thread_id": id(thread),
                "dummy": isinstance(thread, threading._DummyThread),
                "local_survived": seen_before is not None,
            })
        # Long enough that concurrent stats overlap on the worker threads.
        time.sleep(0.02)
        return {"st_mode": stat.S_IFREG | 0o644, "st_nlink": 1, "st_size": 0}

    def readdir(self, path, fh):
        return [".", ".."]


class TestFuseThreadState(unittest.TestCase):

    def _mount(self, nothreads: bool):
        self.temp_dir = tempfile.mkdtemp()
        self.mount_dir = Path(self.temp_dir) / "mount"
        self.mount_dir.mkdir()
        self.ops = _ProbeOperations()

        def run():
            FUSE(self.ops, str(self.mount_dir), nothreads=nothreads,
                 foreground=True, allow_other=False)

        self.mount_thread = threading.Thread(target=run, daemon=True)
        self.mount_thread.start()
        deadline = time.monotonic() + 10
        while not os.path.ismount(self.mount_dir):
            if time.monotonic() > deadline:
                self.fail("the probe mount did not come up")
            time.sleep(0.05)

    def tearDown(self):
        subprocess.run(["fusermount", "-u", str(self.mount_dir)],
                       check=False, timeout=10)
        self.mount_thread.join(timeout=10)
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        # libfuse's teardown leaves SIGPIPE at its default (see below).
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)

    def _stat_sequentially(self, count: int):
        for index in range(count):
            os.stat(self.mount_dir / f"probe-{index}")

    def _stat_concurrently(self, count: int):
        threads = [
            threading.Thread(target=os.stat, args=(self.mount_dir / f"probe-c{index}",))
            for index in range(count)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

    def test_single_threaded_loop_keeps_thread_locals(self):
        self._mount(nothreads=True)
        self._stat_sequentially(20)
        calls = self.ops.calls
        # The kernel may look a path up more than once; each was served.
        self.assertEqual({c["path"] for c in calls}, {f"/probe-{i}" for i in range(20)})
        self.assertEqual(len({c["ident"] for c in calls}), 1)
        # The loop runs on the Python thread that called FUSE().
        self.assertFalse(any(c["dummy"] for c in calls))
        self.assertTrue(all(c["local_survived"] for c in calls[1:]))

    @staticmethod
    def _sigpipe_ignored() -> bool:
        """The kernel's view; signal.getsignal() only reports Python's cache."""
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("SigIgn:"):
                return bool(int(line.split()[1], 16) & (1 << (signal.SIGPIPE - 1)))
        raise AssertionError("no SigIgn line in /proc/self/status")

    def test_unmount_resets_sigpipe_to_its_default(self):
        # Python starts with SIGPIPE ignored. libfuse 2's teardown resets a
        # SIGPIPE disposition it finds ignored to the default, behind
        # Python's back, so after an in-process mount ends, a write to a dead
        # socket kills the process (exit 141) instead of raising
        # BrokenPipeError. Fixtures that mount and then use sockets restore
        # SIG_IGN after unmounting.
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)  # as Python starts
        self.assertTrue(self._sigpipe_ignored())
        self._mount(nothreads=True)
        self._stat_sequentially(1)
        subprocess.run(["fusermount", "-u", str(self.mount_dir)], check=True, timeout=10)
        self.mount_thread.join(timeout=10)
        try:
            self.assertFalse(self._sigpipe_ignored())
        finally:
            signal.signal(signal.SIGPIPE, signal.SIG_IGN)
        self.assertTrue(self._sigpipe_ignored())

    def test_threaded_loop_loses_thread_locals_between_callbacks(self):
        self._mount(nothreads=False)
        self._stat_sequentially(20)
        self._stat_concurrently(16)
        calls = self.ops.calls
        self.assertEqual(
            {c["path"] for c in calls},
            {f"/probe-{i}" for i in range(20)} | {f"/probe-c{i}" for i in range(16)})
        # Callbacks run on libfuse's own threads, which Python sees as dummy
        # threads, and concurrent requests use more than one of them.
        self.assertTrue(all(c["dummy"] for c in calls))
        self.assertGreater(len({c["ident"] for c in calls}), 1)
        # A thread-local set in one callback is gone in the next, even on
        # the same OS thread: each callback gets a fresh Python thread
        # state. A per-thread SSH client would reconnect on every request.
        reused = [c for c in calls
                  if sum(1 for o in calls if o["ident"] == c["ident"]) > 1]
        self.assertTrue(reused, "no worker thread served two callbacks")
        self.assertFalse(any(c["local_survived"] for c in calls))


if __name__ == "__main__":
    unittest.main()
