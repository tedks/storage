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
from unittest.mock import patch

from fuse import FUSE, FuseOSError, Operations
from storage.backends.local import LocalBackend
from storage.backends.ssh_pool import MemberConnection
from storage.fs import FUSE as StorageFUSE, StorageFS, fuse_mount_options


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

    def destroy(self, path):
        # libfuse removes its signal handlers before it calls this.
        self.sigpipe_in_destroy = (
            TestFuseThreadState._sigpipe_bit("SigIgn"), TestFuseThreadState._sigpipe_bit("SigCgt"))


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
    def _sigpipe_bit(field: str) -> bool:
        """The kernel's view; signal.getsignal() only reports Python's cache."""
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith(field + ":"):
                return bool(int(line.split()[1], 16) & (1 << (signal.SIGPIPE - 1)))
        raise AssertionError(f"no {field} line in /proc/self/status")

    @classmethod
    def _sigpipe_ignored(cls) -> bool:
        return cls._sigpipe_bit("SigIgn")

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

    def test_unmount_leaves_a_sigpipe_handler_alone(self):
        # What mount_storage relies on: a no-op handler installed before
        # FUSE() survives libfuse's teardown, including at destroy(), where
        # an ignored SIGPIPE is already back at its default.
        signal.signal(signal.SIGPIPE, lambda signum, frame: None)
        self._mount(nothreads=True)
        self._stat_sequentially(1)
        subprocess.run(["fusermount", "-u", str(self.mount_dir)], check=True, timeout=10)
        self.mount_thread.join(timeout=10)
        self.assertEqual(self.ops.sigpipe_in_destroy, (False, True))
        self.assertTrue(self._sigpipe_bit("SigCgt"))

    def test_unmount_reaches_destroy_with_sigpipe_at_its_default(self):
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)  # as Python starts
        self._mount(nothreads=True)
        self._stat_sequentially(1)
        subprocess.run(["fusermount", "-u", str(self.mount_dir)], check=True, timeout=10)
        self.mount_thread.join(timeout=10)
        self.assertEqual(self.ops.sigpipe_in_destroy, (False, False))

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


class _Channel:
    def __init__(self):
        self.timeout = None

    def invoke_subsystem(self, name):
        assert name == "sftp"

    def gettimeout(self):
        return self.timeout

    def settimeout(self, timeout):
        self.timeout = timeout

    def close(self):
        pass


class _SFTP:
    def __init__(self, channel):
        self.channel = channel

    def get_channel(self):
        return self.channel

    def close(self):
        self.channel.close()


class _Transport:
    def __init__(self):
        self.active = True

    def is_active(self):
        return self.active

    def open_session(self, timeout=None):
        return _Channel()

    def close(self):
        self.active = False


class _Client:
    def __init__(self):
        self.transport = _Transport()

    def get_transport(self):
        return self.transport

    def close(self):
        self.transport.close()


class _CapacityMember(LocalBackend):
    """Scratch local bytes, with the real SFTP pool around each full copy."""

    def __init__(self, root):
        super().__init__("member0", str(root))
        self.pool = MemberConnection(
            self.name, _Client, connect_timeout=0.2, retry_interval=1,
            sftp_client_factory=_SFTP,
        )

    def copy_to_local(self, virtual_path, local_path):
        with self.pool.sftp(1.0):
            return super().copy_to_local(virtual_path, local_path)

    def close(self):
        self.pool.close()


class TestMountedCapacityCancellation(unittest.TestCase):
    """A real libfuse2 worker must leave the capacity wait before destroy."""

    def _exercise_session_exit(self, disable_cancel):
        root = Path(tempfile.mkdtemp(prefix="storage-capacity-unmount-"))
        mount = root / "mount"
        member_root = root / "member"
        mount.mkdir()
        member_root.mkdir()
        (member_root / "cold").write_bytes(b"exact-cold-bytes")
        backend = _CapacityMember(member_root)
        fs = StorageFS(root / "cache", root / "state.db", {backend.name: backend},
                       1024 * 1024, start_sync=False)
        fs.destroy_entered = threading.Event()
        real_destroy = fs.destroy

        def record_destroy(path):
            fs.destroy_entered.set()
            return real_destroy(path)

        fs.destroy = record_destroy
        real_getattr = fs.getattr
        exit_requested = threading.Event()

        def getattr_with_exit(path, fh=None):
            if path == "/exit-trigger":
                from fuse import fuse_exit
                fuse_exit()  # from a real second libfuse callback
                exit_requested.set()
                return {"st_mode": stat.S_IFREG | 0o444, "st_nlink": 1,
                        "st_size": 0}
            return real_getattr(path, fh)

        fs.getattr = getattr_with_exit
        held_release = threading.Event()
        held_ready = [threading.Event() for _ in range(3)]
        holder_errors = []
        holders = []
        mount_errors = []
        mount_thread = None
        opener = None
        trigger = None
        open_done = threading.Event()
        denied = threading.Event()
        original_can_open = backend.pool._can_open_locked

        def observe(interactive):
            allowed = original_can_open(interactive)
            if not interactive and not allowed:
                denied.set()
            return allowed

        backend.pool._can_open_locked = observe

        def hold(ready):
            try:
                with backend.pool.sftp(1.0):
                    ready.set()
                    held_release.wait(90)
            except BaseException as exc:
                holder_errors.append(exc)
                ready.set()

        def serve():
            try:
                StorageFUSE(
                    fs, str(mount), nothreads=False, foreground=True, allow_other=False,
                    **fuse_mount_options(
                        fs.backends, enable_watchers=False,
                        fresh_window=fs._revalidate_after,
                    ),
                )
            except BaseException as exc:
                mount_errors.append(exc)

        def unmount_owned():
            errors = []
            for flag in ("-u", "-uz"):
                result = subprocess.run(
                    ["fusermount", flag, str(mount)], capture_output=True,
                    text=True, timeout=10,
                )
                if result.returncode == 0:
                    return ("normal" if flag == "-u" else "lazy"), errors
                errors.append(result.stderr.strip())
            return "failed", errors

        def open_file():
            try:
                (mount / "cold").read_bytes()
            except OSError:
                pass  # the kernel may detach before replying to the open
            finally:
                open_done.set()

        try:
            for ready in held_ready:
                holder = threading.Thread(target=hold, args=(ready,), daemon=True)
                holders.append(holder)
                holder.start()
            self.assertTrue(all(ready.wait(3) for ready in held_ready))
            self.assertEqual([], holder_errors)
            self.assertEqual(3, backend.pool.snapshot()["bulk_leased"])

            mount_thread = threading.Thread(target=serve, daemon=True)
            mount_thread.start()
            deadline = time.monotonic() + 10
            while not os.path.ismount(mount) and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(os.path.ismount(mount), mount_errors)

            opener = threading.Thread(target=open_file, daemon=True)
            opener.start()
            self.assertTrue(denied.wait(5), "mounted open missed the real pool cap")
            self.assertFalse(open_done.is_set())
            self.assertEqual(3, backend.pool.snapshot()["bulk_leased"])
            def request_exit():
                try:
                    os.stat(mount / "exit-trigger")
                except OSError:
                    pass  # session teardown may reject the triggering stat

            trigger = threading.Thread(target=request_exit, daemon=True)
            trigger.start()
            self.assertTrue(exit_requested.wait(5), "second callback did not exit session")
            trigger.join(5)
            if not disable_cancel:
                self.assertFalse(trigger.is_alive(), "exit-trigger stat did not finish")
            if disable_cancel:
                self.assertFalse(fs.destroy_entered.wait(0.5),
                                 "disabled cancellation let destroy pass the held waiter")
            else:
                self.assertTrue(fs.destroy_entered.wait(5),
                                "session exit did not release queued callback")
                mount_thread.join(5)
                self.assertFalse(mount_thread.is_alive(), "libfuse did not finish destroy")
                self.assertFalse(held_release.is_set())
                self.assertTrue(all(holder.is_alive() for holder in holders))
        finally:
            held_release.set()
            for holder in holders:
                holder.join(5)
            if os.path.ismount(mount):
                unmount_owned()
            if mount_thread is not None and mount_thread.is_alive():
                mount_thread.join(10)
            if opener is not None:
                opener.join(5)
            if trigger is not None:
                trigger.join(5)
            backend.close()
            if mount_thread is None or not mount_thread.is_alive():
                shutil.rmtree(root, ignore_errors=True)
            signal.signal(signal.SIGPIPE, signal.SIG_IGN)
            if mount_thread is not None and mount_thread.is_alive():
                self.fail("owned FUSE callback did not exit after holder release")
        self.assertTrue(open_done.is_set())
        self.assertEqual([], holder_errors)
        self.assertEqual([], mount_errors)
        self.assertFalse(any(t.is_alive() for t in holders))
        self.assertFalse(mount_thread.is_alive())
        self.assertFalse(trigger.is_alive())

    def test_session_exit_releases_capacity_wait_before_destroy(self):
        self._exercise_session_exit(disable_cancel=False)

    def test_without_native_cancel_probe_waiter_blocks_until_holder_release(self):
        with patch("storage.fs._native_capacity_cancel_reason", return_value=None):
            self._exercise_session_exit(disable_cancel=True)


if __name__ == "__main__":
    unittest.main()
