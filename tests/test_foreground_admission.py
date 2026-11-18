"""Foreground admission: the callback cap, its class reserves and shutdown.

The gate is driven directly and through StorageFS.__call__ (the entry
libfuse uses), from threads, without a mount. Blocking is forced with
Events, and every wait has a timeout, so a regression fails instead of
hanging. Native libfuse thread growth is outside these tests' claim.
"""

import errno
import os
import threading
import unittest
from unittest.mock import patch

from fuse import FuseOSError

from storage.foreground_admission import (
    AdmissionClosed, ForegroundAdmission, MAX_CAP, MIN_CAP, validate_cap,
)
from storage.fs import StorageFS
from storage.member_repairs import VIEW_WRITE, MemberRepairs
from storage.models import SyncStatus

from tests.test_fs_threading import BLOCKED, WAIT, ThreadingTestBase, Worker


class Holder:
    """Hold one admission slot of a class until released."""

    def __init__(self, gate, kind):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.worker = Worker(self._run, gate, kind)

    def _run(self, gate, kind):
        with gate.admit(kind):
            self.entered.set()
            assert self.release.wait(WAIT), "holder never released"

    def start(self):
        self.worker.start()
        return self

    def finish(self):
        self.release.set()
        return self.worker.finish()


class TestCap(unittest.TestCase):
    def test_bounds(self):
        self.assertEqual(4, MIN_CAP)
        self.assertEqual(128, MAX_CAP)
        for good in (4, 5, 32, 128):
            self.assertEqual(good, validate_cap(good))
            self.assertEqual(good - 4, ForegroundAdmission(good).shared)
        for bad in (3, 0, -1, 129, True, False, 4.0, "4", None):
            with self.subTest(bad=bad), self.assertRaises(ValueError) as caught:
                ForegroundAdmission(bad)
            self.assertIn("from 4 to 128", str(caught.exception))

    def test_storagefs_rejects_cap_three(self):
        # Constructed before anything else, so a rejected cap has no state.
        with patch("storage.fs.StateManager") as state:
            with self.assertRaises(ValueError):
                StorageFS(None, None, {}, 1, max_foreground_operations=3)
        state.assert_not_called()


class TestGate(unittest.TestCase):
    def setUp(self):
        self.holders = []

    def tearDown(self):
        for holder in self.holders:
            holder.release.set()
        for holder in self.holders:
            holder.worker.join(WAIT)

    def hold(self, gate, kind):
        holder = Holder(gate, kind).start()
        self.holders.append(holder)
        return holder

    def held(self, gate, kind):
        holder = self.hold(gate, kind)
        self.assertTrue(holder.entered.wait(WAIT), f"{kind} slot not granted")
        return holder

    def queued(self, gate, kind):
        before = gate.waiting_count(kind)
        holder = self.hold(gate, kind)
        self.assertTrue(gate.waiting_for(kind, before + 1, WAIT))
        self.assertFalse(holder.entered.is_set())
        return holder

    def test_cap_four_reserves_two_demand_one_metadata_one_ordinary(self):
        gate = ForegroundAdmission(4)
        ordinary = self.held(gate, "ordinary")
        self.queued(gate, "ordinary")             # no shared slot at 4
        self.held(gate, "metadata")
        self.queued(gate, "metadata")
        self.held(gate, "demand")
        self.held(gate, "demand")                  # the second reserved read slot
        third_demand = self.queued(gate, "demand")
        self.assertEqual(4, gate.active_count)
        self.assertEqual(4, gate.peak_active)
        # A freed ordinary slot goes to the queued ordinary, not to demand.
        ordinary.finish()
        self.assertTrue(self.holders[1].entered.wait(WAIT))
        self.assertFalse(third_demand.entered.is_set())
        self.assertEqual(4, gate.active_count)

    def test_shared_slots_never_consume_a_reserve(self):
        gate = ForegroundAdmission(6)              # two shared
        for _ in range(3):
            self.held(gate, "ordinary")             # its reserve plus both shared
        self.queued(gate, "ordinary")
        # Every other class still has its whole reserve.
        self.held(gate, "metadata")
        self.held(gate, "demand")
        self.held(gate, "demand")
        self.queued(gate, "demand")
        self.queued(gate, "metadata")
        self.assertEqual(6, gate.active_count)
        self.assertEqual(6, gate.peak_active)

    def test_fifo_within_a_class(self):
        gate = ForegroundAdmission(4)
        first = self.held(gate, "ordinary")
        order = []
        lock = threading.Lock()

        def enter(index):
            with gate.admit("ordinary"):
                with lock:
                    order.append(index)

        waiters = []
        for index in range(4):
            waiter = Worker(enter, index)
            waiter.start()
            self.assertTrue(gate.waiting_for("ordinary", index + 1, WAIT))
            waiters.append(waiter)
        first.finish()
        for waiter in waiters:
            waiter.finish()
        self.assertEqual([0, 1, 2, 3], order)
        self.assertEqual(0, gate.active_count)

    def test_close_cancels_queued_only_and_slots_return_on_error(self):
        gate = ForegroundAdmission(4)
        running = self.held(gate, "ordinary")
        ran = []

        def body():
            with gate.admit("ordinary"):
                ran.append(True)

        waiter = Worker(body)
        waiter.start()
        self.assertTrue(gate.waiting_for("ordinary", 1, WAIT))
        gate.close()
        waiter.join(WAIT)
        self.assertFalse(waiter.is_alive())
        self.assertIsInstance(waiter.error, AdmissionClosed)
        self.assertEqual([], ran)
        self.assertEqual(0, gate.waiting_count("ordinary"))
        # The running callback was not interrupted and releases normally.
        self.assertEqual(1, gate.active_count)
        running.finish()
        self.assertEqual(0, gate.active_count)
        with self.assertRaises(AdmissionClosed):
            with gate.admit("metadata"):
                ran.append(True)
        self.assertEqual([], ran)

        gate = ForegroundAdmission(4)
        with self.assertRaises(KeyError):
            with gate.admit("demand"):
                raise KeyError("callback error")
        self.assertEqual(0, gate.active_count)
        with self.assertRaises(ValueError):
            with gate.admit("bulk"):
                pass

    def test_concurrent_load_never_exceeds_cap_or_takes_a_reserve(self):
        gate = ForegroundAdmission(5)
        violations = []
        lock = threading.Lock()
        active = {kind: 0 for kind in gate.CLASSES}
        go = threading.Event()

        def body(kind):
            assert go.wait(WAIT)
            for _ in range(50):
                with gate.admit(kind):
                    with lock:
                        active[kind] += 1
                        total = sum(active.values())
                        over = sum(max(0, active[k] - gate.RESERVED[k]) for k in active)
                        if total > gate.cap or over > gate.shared:
                            violations.append(dict(active))
                    with lock:
                        active[kind] -= 1

        workers = [Worker(body, kind) for kind in gate.CLASSES for _ in range(6)]
        for worker in workers:
            worker.start()
        go.set()
        for worker in workers:
            worker.finish()
        self.assertEqual([], violations)
        self.assertLessEqual(gate.peak_active, 5)
        self.assertEqual(0, gate.active_count)


class CappedFSBase(ThreadingTestBase):
    cap = 4
    streaming_reads = False

    def setUp(self):
        super().setUp()
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        self.fs = StorageFS(
            cache_root=self.cache_dir,
            state_db=self.temp_dir / "state.db",
            backends={"member": self.member},
            cache_size=10 * 1024 * 1024,
            num_sync_workers=1,
            start_sync=False,
            revalidate_after=self.revalidate_after,
            max_foreground_operations=self.cap,
            streaming_reads=self.streaming_reads,
        )
        self.gate_ = self.fs._foreground_admission
        self.blockers = []
        self.addCleanup(self._release_blockers)

    def _release_blockers(self):
        for event, worker in self.blockers:
            event.set()
        for event, worker in self.blockers:
            worker.join(WAIT)

    def call(self, op, *args):
        return self.fs(op, *args)

    def wait_for_path_lock_refs(self, canonical_path, refs):
        """Wait until refs holders and waiters share the path's lock."""
        for _ in range(int(WAIT / 0.01)):
            with self.fs._path_locks._mutex:
                entry = self.fs._path_locks._entries.get(canonical_path)
                if entry is not None and entry.refs >= refs:
                    return
            threading.Event().wait(0.01)
        self.fail(f"fewer than {refs} on the lock of {canonical_path}")

    def saturate(self, kind, op, *args):
        """Fill every slot kind can use with callbacks blocked in op.

        Starts blocked callbacks until one queues, so a reserve plus any
        shared capacity it can take is in use. op is patched to block, so
        nothing real runs; the class comes from the op name.
        """
        release = threading.Event()
        entered = []
        lock = threading.Lock()

        def blocked(*_args):
            with lock:
                entered.append(True)
            assert release.wait(WAIT), "blocker never released"
            return 0

        patcher = patch.object(self.fs, op, side_effect=blocked, create=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        while True:
            with lock:
                entered_before = len(entered)
            queued_before = self.gate_.waiting_count(kind)
            worker = Worker(self.call, op, *args)
            worker.start()
            self.blockers.append((release, worker))
            # Either it entered (one more blocked call) or it queued.
            for _ in range(int(WAIT / 0.01)):
                with lock:
                    if len(entered) > entered_before:
                        break
                if self.gate_.waiting_count(kind) > queued_before:
                    return release
                threading.Event().wait(0.01)
            else:
                self.fail(f"{op} neither entered nor queued")


class TestFixedClassAndCompetingRead(CappedFSBase):
    # Every open of a cached file stats its member.
    revalidate_after = 0.0

    def test_unrelated_read_progresses_past_a_slow_warm_to_cold_open(self):
        self.cache("a.txt", b"old a")
        b_bytes = os.urandom(64 * 1024)
        self.cache("b.txt", b_bytes)
        fd_b = self.call("open", "/b.txt", os.O_RDONLY)
        # The member's a changes: the next open of the cached a refetches.
        new_a = os.urandom(128 * 1024)
        (self.member_dir / "a.txt").write_bytes(new_a)
        copy = self.gate(self.member, "copy_to_local", only=lambda path, *_: path == "/a.txt")

        opener = Worker(self.open_read_release_called, "/a.txt")
        opener.start()
        self.assertTrue(copy.started.wait(WAIT))
        # Classified at entry as a warm open; it keeps the demand slot and
        # its path lock (master's lock order) through the cold copy.
        self.assertEqual(1, self.gate_.active("demand"))
        self.assertEqual(0, self.gate_.active("ordinary"))
        with self.fs._path_locks._mutex:
            self.assertIn("/a.txt", self.fs._path_locks._entries)

        # Saturate metadata and ordinary (cap 4 has no shared slots).
        self.saturate("metadata", "statfs", "/")
        self.saturate("ordinary", "mkdir", "/d", 0o755)

        reader = Worker(self.call, "read", "/b.txt", len(b_bytes), 0, fd_b)
        reader.start()
        self.assertEqual(b_bytes, reader.finish())
        self.assertTrue(opener.is_alive())

        # Both demand slots in use: the next demand callback waits.
        hold_fsync = self.saturate("demand", "fsync", "/b.txt", 0, fd_b)
        self.assertEqual(self.cap, self.gate_.active_count)
        self.assertLessEqual(self.gate_.peak_active, self.cap)

        # The queued fsync is ahead of the opener's read in the demand FIFO.
        hold_fsync.set()
        copy.opened.set()
        self.assertEqual(new_a, opener.finish())
        self._release_blockers()
        self.assertEqual(0, self.call("release", "/b.txt", fd_b))
        self.assertEqual(0, self.gate_.active_count)
        self.assertEqual(1, len([c for c in copy.calls if c[0] == "/a.txt"]))

    def open_read_release_called(self, path):
        fd = self.call("open", path, os.O_RDONLY)
        try:
            return self.call("read", path, 1 << 20, 0, fd)
        finally:
            self.call("release", path, fd)


class TestStreamingReadClass(CappedFSBase):
    streaming_reads = True

    def test_streaming_reads_cannot_take_the_demand_reserve(self):
        # A warm file: written through the mount, so its bytes are local.
        fd_w = self.call("create", "/warm.bin", 0o644)
        warm = os.urandom(32 * 1024)
        self.assertEqual(len(warm), self.call("write", "/warm.bin", warm, 0, fd_w))
        self.call("release", "/warm.bin", fd_w)
        fd_warm = self.call("open", "/warm.bin", os.O_RDONLY)

        # Two uncached files stream; every member range read blocks.
        for name in ("s1.bin", "s2.bin"):
            (self.member_dir / name).write_bytes(os.urandom(4 << 20))
        go = threading.Event()
        real_read = self.member.read_file

        def blocked_read(path, offset, size):
            assert go.wait(WAIT), "range read never released"
            return real_read(path, offset, size)

        self.addCleanup(go.set)
        with patch.object(self.member, "read_file", side_effect=blocked_read):
            streams = {}
            for name in ("/s1.bin", "/s2.bin"):
                fd = self.call("open", name, os.O_RDONLY)
                with self.fs._fd_lock:
                    self.assertIsNotNone(self.fs._fd_map[fd].prefetch_session)
                streams[name] = fd
            readers = [Worker(self.call, "read", name, 4096, 2 << 20, fd)
                       for name, fd in streams.items()]
            for reader in readers:
                reader.start()
            # One holds the ordinary reserve (no shared at cap 4); one queues.
            self.assertTrue(self.gate_.waiting_for("ordinary", 1, WAIT))
            self.assertEqual(1, self.gate_.active("ordinary"))
            self.assertEqual(0, self.gate_.active("demand"))
            got = Worker(self.call, "read", "/warm.bin", len(warm), 0, fd_warm)
            got.start()
            self.assertEqual(warm, got.finish())
            go.set()
            for reader, name in zip(readers, streams):
                data = reader.finish()
                self.assertEqual(
                    (self.member_dir / name.lstrip("/")).read_bytes()[2 << 20:(2 << 20) + 4096],
                    data,
                )
        for name, fd in streams.items():
            self.call("release", name, fd)
        self.call("release", "/warm.bin", fd_warm)
        self.assertEqual(0, self.gate_.active_count)


class TestClasses(CappedFSBase):
    def test_entry_classes(self):
        self.cache("warm.txt", b"warm")
        (self.member_dir / "cold.txt").write_bytes(b"cold")
        seen = {}
        real_open = self.fs.open

        def observe(path, flags):
            seen[path] = {k: self.gate_.active(k) for k in self.gate_.CLASSES}
            return real_open(path, flags)

        with patch.object(self.fs, "open", side_effect=observe):
            for name in ("/warm.txt", "/cold.txt"):
                self.call("release", name, self.call("open", name, os.O_RDONLY))
        self.assertEqual({"metadata": 0, "demand": 1, "ordinary": 0}, seen["/warm.txt"])
        self.assertEqual({"metadata": 0, "demand": 0, "ordinary": 1}, seen["/cold.txt"])

        cached_fd = self.call("open", "/warm.txt", os.O_RDONLY)
        table = {
            ("getattr", ("/warm.txt", None)): "metadata",
            ("readdir", ("/", 0)): "metadata",
            ("opendir", ("/",)): "metadata",
            ("statfs", ("/",)): "metadata",
            ("access", ("/", 0)): "metadata",
            ("readlink", ("/l",)): "metadata",
            ("getxattr", ("/warm.txt", "user.x")): "metadata",
            ("listxattr", ("/warm.txt",)): "metadata",
            ("read", ("/warm.txt", 1, 0, cached_fd)): "demand",
            ("read", ("/warm.txt", 1, 0, 999999)): "demand",   # unknown fd
            ("write", ("/x", b"", 0, 0)): "demand",
            ("fsync", ("/x", 0, 0)): "demand",
            ("open", ("/warm.txt", os.O_RDWR | os.O_TRUNC)): "demand",
            ("open", ("/warm.txt.member", os.O_RDONLY)): "ordinary",
            ("open", ("/never-cached.txt", os.O_RDONLY)): "ordinary",
            ("create", ("/x", 0o644)): "ordinary",
            ("mkdir", ("/x", 0o755)): "ordinary",
            ("rename", ("/x", "/y")): "ordinary",
            ("unlink", ("/x",)): "ordinary",
            ("truncate", ("/x", 0)): "ordinary",
            ("chmod", ("/x", 0o644)): "ordinary",
            ("utimens", ("/x", None)): "ordinary",
            ("setxattr", ("/x", "user.x", b"", 0)): "ordinary",
            ("removexattr", ("/x", "user.x")): "ordinary",
        }
        for (op, args), expected in table.items():
            with self.subTest(op=op, args=args):
                self.assertEqual(expected, self.fs._foreground_class(op, args))
        self.call("release", "/warm.txt", cached_fd)
        for op in ("init", "destroy", "release", "releasedir"):
            self.assertIn(op, StorageFS._ADMISSION_EXEMPT)
        # flush does work (member repairs), so it is admitted as demand.
        self.assertNotIn("flush", StorageFS._ADMISSION_EXEMPT)
        self.assertEqual("demand", self.fs._foreground_class("flush", ("/x", 0)))
        # A failing preflight falls back to ordinary; the open reports its own error.
        with patch.object(self.fs.cache_manager, "is_cached", side_effect=RuntimeError):
            self.assertEqual("ordinary", self.fs._foreground_class("open", ("/warm.txt", 0)))

    def test_results_and_errors_unchanged(self):
        self.cache("f.txt", b"exact bytes")
        fd = self.call("open", "/f.txt", os.O_RDONLY)
        self.assertEqual(b"exact bytes", self.call("read", "/f.txt", 64, 0, fd))
        self.call("release", "/f.txt", fd)
        with self.assertRaises(FuseOSError) as caught:
            self.call("getattr", "/missing", None)
        self.assertEqual(errno.ENOENT, caught.exception.errno)
        with self.assertRaises(FuseOSError) as caught:
            self.call("open", "/missing", os.O_RDONLY)
        self.assertEqual(errno.ENOENT, caught.exception.errno)
        self.assertEqual(0, self.gate_.active_count)

    def test_no_gate_without_the_option(self):
        plain = StorageFS(
            cache_root=self.temp_dir / "plain-cache",
            state_db=self.temp_dir / "plain.db",
            backends={"member": self.member},
            cache_size=1024, num_sync_workers=0, start_sync=False,
        )
        try:
            self.assertIsNone(plain._foreground_admission)
        finally:
            plain.sync_manager.stop()
            plain.state_manager.close()


class TestCleanupThroughShutdown(CappedFSBase):
    """release and releasedir: never queued, never cancelled, run once."""

    def saturate_all(self):
        self.saturate("metadata", "statfs", "/")
        self.saturate("demand", "fsync", "/", 0, 0)
        self.saturate("ordinary", "mkdir", "/d", 0o755)
        self.assertEqual(self.cap, self.gate_.active_count)

    def queued_syncs(self, path):
        return [task for task in list(self.fs.sync_manager.task_queue.queue)
                if getattr(task, "virtual_path", None) == path]

    def test_canonical_close_runs_once_while_saturated_and_after_close(self):
        self.cache("w.txt", b"before")
        fd1 = self.call("open", "/w.txt", os.O_RDWR)
        self.assertEqual(5, self.call("write", "/w.txt", b"after", 0, fd1))
        fd2 = self.call("open", "/w.txt", os.O_RDONLY)
        self.saturate_all()
        queue_sync = patch.object(
            self.fs.sync_manager, "queue_sync", wraps=self.fs.sync_manager.queue_sync,
        )
        with queue_sync as spy:
            # Not admitted: it runs while every slot is held.
            self.assertEqual(0, Worker(self.call, "release", "/w.txt", fd1).finish_started())
            self.assertEqual(1, spy.call_count)
            self.assertEqual(0, Worker(self.call, "releasedir", "/", 0).finish_started())
            self.gate_.close()
            # Still runs after the gate closed; the queued blockers do not.
            self.assertEqual(0, self.call("release", "/w.txt", fd2))
            self.assertEqual(0, self.call("release", "/w.txt", fd1))  # gone: no-op
            self.assertEqual(2, spy.call_count)
        self.assertEqual(
            SyncStatus.DIRTY, self.fs.state_manager.get_file_metadata("/w.txt").sync_status,
        )
        self.assertEqual(b"aftere", (self.cache_dir / "w.txt").read_bytes())
        self.assertNotIn(fd1, self.fs._fd_map)
        self.assertNotIn(fd2, self.fs._fd_map)
        with self.assertRaises(FuseOSError) as caught:
            self.call("read", "/w.txt", 1, 0, 0)
        self.assertEqual(errno.EINTR, caught.exception.errno)

    def test_member_copy_close_uploads_once_across_gate_close(self):
        self.cache("f.txt", b"canonical")
        (self.member_dir / "f.txt").write_bytes(b"member copy")
        fd = self.call("open", "/f.txt.member", os.O_RDWR)
        self.call("write", "/f.txt.member", b"edited", 0, fd)
        upload = self.gate(self.member, "copy_from_local")
        self.saturate_all()
        releaser = Worker(self.call, "release", "/f.txt.member", fd)
        releaser.start()
        self.assertTrue(upload.started.wait(WAIT))
        # Shutdown's first step, while the close is in its member upload.
        self.gate_.close()
        upload.opened.set()
        self.assertEqual(0, releaser.finish())
        self.assertEqual(0, self.call("release", "/f.txt.member", fd))
        self.assertEqual(1, len(upload.calls))
        self.assertEqual(b"edited copy", (self.member_dir / "f.txt").read_bytes())

    def test_closed_gate_fails_queued_callbacks_unrun(self):
        self.saturate("metadata", "statfs", "/")
        ran = []
        with patch.object(self.fs, "getattr", side_effect=lambda *a: ran.append(a)):
            waiter = Worker(self.call, "getattr", "/", None)
            waiter.start()
            self.assertTrue(self.gate_.waiting_for("metadata", 2, WAIT))
            self.gate_.close()
            waiter.join(WAIT)
        self.assertFalse(waiter.is_alive())
        self.assertIsInstance(waiter.error, FuseOSError)
        self.assertEqual(errno.EINTR, waiter.error.errno)
        self.assertEqual([], ran)

    def test_destroy_calls_close_first(self):
        order = []
        with patch.object(self.gate_, "close", side_effect=lambda: order.append("close")), \
                patch.object(self.fs._streaming_manager, "destroy",
                             side_effect=lambda: order.append("streaming")):
            try:
                self.fs.destroy("/")
            except Exception:  # noqa: BLE001 - later teardown steps are not under test
                pass
        self.assertEqual(["close", "streaming"], order[:2])

    def test_close_runs_while_every_slot_waits_on_a_path_lock(self):
        # One member-copy close holds /f.txt's path lock in its upload. Every
        # slot is then held: both demand slots by cached opens of /f.txt
        # waiting for that lock, metadata and ordinary by blocked callbacks.
        # Another close still runs at once, and the first close finishing
        # lets every waiter through. (No lock-order cycle exists either way;
        # the gate takes no slot under a lock. This pins that closes do not
        # queue behind slot holders.)
        self.cache("f.txt", b"canonical")
        self.cache("g.txt", b"other")
        (self.member_dir / "f.txt").write_bytes(b"member copy")
        fd = self.call("open", "/f.txt.member", os.O_RDWR)
        self.call("write", "/f.txt.member", b"edited", 0, fd)
        fd_g = self.call("open", "/g.txt", os.O_RDONLY)
        upload = self.gate(self.member, "copy_from_local")
        releaser = Worker(self.call, "release", "/f.txt.member", fd)
        releaser.start()
        self.assertTrue(upload.started.wait(WAIT))
        openers = [Worker(self.call, "open", "/f.txt", os.O_RDONLY) for _ in range(3)]
        for opener in openers:
            opener.start()
        self.assertTrue(self.gate_.waiting_for("demand", 1, WAIT))
        self.assertEqual(2, self.gate_.active("demand"))
        # Both admitted opens wait on the lock the close holds.
        self.wait_for_path_lock_refs("/f.txt", 3)
        self.saturate("metadata", "statfs", "/")
        self.saturate("ordinary", "mkdir", "/d", 0o755)
        self.assertEqual(self.cap, self.gate_.active_count)
        self.assertEqual(0, Worker(self.call, "release", "/g.txt", fd_g).finish_started())
        upload.opened.set()
        self.assertEqual(0, releaser.finish())
        for opener in openers:
            self.call("release", "/f.txt", opener.finish())
        self._release_blockers()
        self.assertEqual(0, self.gate_.active_count)
        self.assertEqual(b"edited copy", (self.member_dir / "f.txt").read_bytes())

    def test_flush_is_admitted_as_demand_and_release_still_never_waits(self):
        # StorageFS.flush does work, so it is admitted (fs.py
        # _ADMISSION_EXEMPT, CTO STORAGE-QUEUE-0645): with every slot held
        # it waits, while a release runs at once; it returns 0 once a
        # demand slot frees.
        from fuse import Operations
        self.assertIsNot(Operations.flush, type(self.fs).flush)
        self.cache("g.txt", b"other")
        fd_g = self.call("open", "/g.txt", os.O_RDONLY)
        self.saturate_all()
        flusher = Worker(self.call, "flush", "/g.txt", fd_g)
        flusher.start()
        # saturate already left one fsync queued; the flush is the second.
        self.assertTrue(self.gate_.waiting_for("demand", 2, WAIT))
        flusher.join(BLOCKED)
        self.assertTrue(flusher.is_alive())
        self._release_blockers()
        self.assertEqual(0, flusher.finish())
        self.assertEqual(0, Worker(self.call, "release", "/g.txt", fd_g).finish_started())
        self.assertEqual(0, self.gate_.active_count)


class TestFencedViewCloseUnderTheCap(CappedFSBase):
    """A conflict view whose member gains a repair while it is open, with
    the cap on (member repairs with foreground admission, STORAGE-QUEUE-0645).
    Cap off is covered in test_sync_races."""

    saturate_all = TestCleanupThroughShutdown.saturate_all

    def test_fenced_view_close_waits_for_demand_then_reports_ebusy(self):
        self.cache("f.txt", b"member copy")
        self.cache("g.txt", b"other")
        fd = self.call("open", "/f.txt.member", os.O_RDWR)
        self.call("write", "/f.txt.member", b"edited", 0, fd)
        fd_g = self.call("open", "/g.txt", os.O_RDONLY)
        # A rename missed by the member, at the view's name.
        self.fs.cache_manager.member_repairs.record(
            "/f.txt", "/h.txt", ["member"], "missed rename (test)")
        self.saturate_all()
        flusher = Worker(self.call, "flush", "/f.txt.member", fd)
        flusher.start()
        self.assertTrue(self.gate_.waiting_for("demand", 2, WAIT))
        flusher.join(BLOCKED)
        self.assertTrue(flusher.is_alive())
        # Cleanup is not admitted: a release runs while the flush queues.
        self.assertEqual(0, Worker(self.call, "release", "/g.txt", fd_g).finish_started())
        self._release_blockers()
        with self.assertRaises(FuseOSError) as raised:
            flusher.finish()
        self.assertEqual(errno.EBUSY, raised.exception.errno)
        copies = []
        with patch.object(self.member, "copy_from_local",
                          side_effect=lambda *a: copies.append(a)):
            # Two releases racing for the one descriptor: it runs once.
            releasers = [Worker(self.call, "release", "/f.txt.member", fd) for _ in range(2)]
            for releaser in releasers:
                releaser.start()
            for releaser in releasers:
                self.assertEqual(0, releaser.finish())
        self.assertEqual([], copies)
        self.assertEqual(b"member copy", (self.member_dir / "f.txt").read_bytes())
        kept = [r for r in MemberRepairs(self.cache_dir).list() if r.kind == VIEW_WRITE]
        self.assertEqual(1, len(kept))
        self.assertEqual(b"edited copy",
                         MemberRepairs(self.cache_dir).carrier_path(kept[0].id).read_bytes())
        self.assertEqual(0, self.gate_.active_count)


class TestQueuedCallbacksIgnoreNativeCancellation(CappedFSBase):
    """A queued callback waits through an interrupt or a session exit.

    One libfuse request can make several callbacks, so cancelling a later
    one would fail a request whose earlier step already happened (CTO
    ruling STORAGE-180-ESC-I1-C). Only close() cancels a waiter.
    """

    @staticmethod
    def _native_cancelled(calls):
        def probe():
            calls.append(1)
            return "session"
        return probe

    def _queued(self, probe, op, *args):
        from storage.backends.base import capacity_wait_cancellation

        def call():
            with capacity_wait_cancellation(probe):
                return self.call(op, *args)
        worker = Worker(call)
        worker.start()
        return worker

    def test_a_queued_callback_waits_through_a_cancelled_native_request(self):
        self.saturate("metadata", "statfs", "/")
        ran, probed = [], []
        with patch.object(self.fs, "getattr", side_effect=lambda *a: ran.append(a) or {}):
            waiter = self._queued(self._native_cancelled(probed), "getattr", "/", None)
            self.assertTrue(self.gate_.waiting_for("metadata", 2, WAIT))
            waiter.join(BLOCKED)
            self.assertTrue(waiter.is_alive())
            self.assertEqual([], ran)
            self._release_blockers()
            self.assertEqual({}, waiter.finish())
        self.assertEqual([("/", None)], ran)
        self.assertEqual([], probed)       # admission never consults the probe
        self.assertEqual(0, self.gate_.active_count)
        self.assertEqual(0, self.gate_.waiting_count("metadata"))

    HIDDEN = "/d/.fuse_hidden0000000a00000001"

    def test_every_unlink_is_admitted_ordinary(self):
        for path in (self.HIDDEN, "/plain.txt", "/.fuse_hiddenXYZ"):
            self.assertEqual("ordinary", self.fs._foreground_class("unlink", (path,)))
        self.assertNotIn("unlink", StorageFS._ADMISSION_EXEMPT)

    def test_queued_unlinks_of_any_name_wait_and_run_once(self):
        # libfuse's cleanup, a user lookalike and a plain name are alike.
        hold = self.saturate("ordinary", "mkdir", "/d", 0o755)
        ran, probed = [], []
        paths = (self.HIDDEN, "/plain.txt", "/.fuse_hidden0000000a0000000")
        with patch.object(self.fs, "unlink", side_effect=lambda p: ran.append(p) or 0):
            waiters = []
            for count, path in enumerate(paths, start=2):
                waiters.append(self._queued(self._native_cancelled(probed), "unlink", path))
                self.assertTrue(self.gate_.waiting_for("ordinary", count, WAIT))
            for waiter in waiters:
                waiter.join(BLOCKED / len(paths))
                self.assertTrue(waiter.is_alive())
            self.assertEqual([], ran)
            hold.set()
            for waiter in waiters:
                self.assertEqual(0, waiter.finish())
        self.assertEqual(list(paths), ran)     # FIFO, each once
        self.assertEqual([], probed)

    def test_explicit_close_cancels_queued_callbacks_and_rejects_new_requests(self):
        self.saturate("ordinary", "mkdir", "/d", 0o755)
        ran = []
        with patch.object(self.fs, "unlink", side_effect=lambda p: ran.append(p) or 0):
            hidden = self._queued(lambda: None, "unlink", self.HIDDEN)
            self.assertTrue(self.gate_.waiting_for("ordinary", 2, WAIT))
            self.gate_.close()
            hidden.join(WAIT)
            self.assertIsInstance(hidden.error, FuseOSError)
            self.assertEqual(errno.EINTR, hidden.error.errno)
            for op, args in (("unlink", (self.HIDDEN,)), ("getattr", ("/", None)),
                             ("read", ("/x", 1, 0, 0))):
                with self.subTest(op=op), self.assertRaises(FuseOSError) as caught:
                    self.call(op, *args)
                self.assertEqual(errno.EINTR, caught.exception.errno)
        self.assertEqual([], ran)

    def test_gate_close_cancels_a_waiter_and_rejects_later_ones(self):
        gate = ForegroundAdmission(4)
        with gate.admit("ordinary"):
            waiter = Worker(lambda: gate.admit("ordinary").__enter__())
            waiter.start()
            self.assertTrue(gate.waiting_for("ordinary", 1, WAIT))
            gate.close()
            waiter.join(WAIT)
            self.assertIsInstance(waiter.error, AdmissionClosed)
        with self.assertRaises(AdmissionClosed):
            with gate.admit("demand"):
                pass
        self.assertEqual(0, gate.active_count)
        self.assertEqual(0, gate.waiting_count("ordinary"))

    def test_fs_runs_an_uncontended_callback_whose_session_exited(self):
        ran, probed = [], []
        with patch.object(self.fs, "getattr", side_effect=lambda *a: ran.append(a) or {}):
            from storage.backends.base import capacity_wait_cancellation
            with capacity_wait_cancellation(self._native_cancelled(probed)):
                self.assertEqual({}, self.call("getattr", "/", None))
        self.assertEqual([("/", None)], ran)
        self.assertEqual(0, self.gate_.active_count)


if __name__ == "__main__":
    unittest.main()
