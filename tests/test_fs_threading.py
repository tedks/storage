"""Races between FUSE handlers run from several threads (threading stage 2).

StorageFS is driven directly from threads, with Event gates patched into
member calls so each interleaving is forced rather than hoped for. Every
wait has a timeout, so a regression fails instead of hanging.
"""

import dataclasses
import errno
import os
import shutil
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fuse import FuseOSError

from storage.backends.local import LocalBackend
from storage.backends.base import is_interactive_request
from storage.fs import StorageFS, mount_storage
from storage.models import FileLocation, SyncStatus

WAIT = 5.0
# How long a thread that should be blocked is given to (wrongly) finish.
BLOCKED = 0.3


class Gate:
    """Pause the first call of a wrapped function until opened.

    Later calls (and every call when `only` rejects the arguments) run
    straight through. `started` is set when the paused call arrives.
    """

    def __init__(self, real, only=None):
        self.real = real
        self.only = only
        self.started = threading.Event()
        self.opened = threading.Event()
        self.calls = []
        self._lock = threading.Lock()

    def __call__(self, *args, **kwargs):
        with self._lock:
            self.calls.append(args)
            pause = not self.started.is_set() and (self.only is None or self.only(*args))
            if pause:
                self.started.set()
        if pause:
            assert self.opened.wait(WAIT), "gate never opened"
        return self.real(*args, **kwargs)


class Worker(threading.Thread):
    """Run fn in a thread; keep its result or exception."""

    def __init__(self, fn, *args):
        super().__init__(daemon=True)
        self.fn, self.args = fn, args
        self.result = self.error = None

    def run(self):
        try:
            self.result = self.fn(*self.args)
        except BaseException as exc:  # noqa: BLE001 - reported by the test
            self.error = exc

    def finish_started(self):
        self.start()
        return self.finish()

    def finish(self):
        self.join(WAIT)
        assert not self.is_alive(), "worker did not finish"
        if self.error is not None:
            raise self.error
        return self.result


class ThreadingTestBase(unittest.TestCase):
    revalidate_after = 60.0

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_dir = self.temp_dir / "cache"
        self.member_dir = self.temp_dir / "member"
        self.cache_dir.mkdir()
        self.member_dir.mkdir()
        self.member = LocalBackend("member", str(self.member_dir))
        self.fs = StorageFS(
            cache_root=self.cache_dir,
            state_db=self.temp_dir / "state.db",
            backends={"member": self.member},
            cache_size=10 * 1024 * 1024,
            num_sync_workers=1,
            start_sync=False,
            revalidate_after=self.revalidate_after,
        )
        self.gates = []

    def tearDown(self):
        for gate in self.gates:
            gate.opened.set()
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def gate(self, target, name, only=None):
        gate = Gate(getattr(target, name), only)
        self.gates.append(gate)
        patcher = patch.object(target, name, side_effect=gate)
        patcher.start()
        self.addCleanup(patcher.stop)
        return gate

    def wait_for_path_waiter(self, canonical_path):
        """Wait until a second thread holds or waits for the path's lock."""
        deadline = time.monotonic() + WAIT
        while time.monotonic() < deadline:
            with self.fs._path_locks._mutex:
                entry = self.fs._path_locks._entries.get(canonical_path)
                if entry is not None and entry.refs >= 2:
                    return
            time.sleep(0.01)
        self.fail(f"nothing waited for the lock of {canonical_path}")

    def wait_for_namespace_writer(self):
        deadline = time.monotonic() + WAIT
        while time.monotonic() < deadline:
            with self.fs._namespace_lock._cond:
                if self.fs._namespace_lock._writers_waiting:
                    return
            time.sleep(0.01)
        self.fail("no exclusive namespace waiter")

    def read_all(self, path, fd):
        return self.fs.read(path, 1 << 20, 0, fd)

    def open_read_release(self, path):
        fd = self.fs.open(path, os.O_RDONLY)
        try:
            return self.read_all(path, fd)
        finally:
            self.fs.release(path, fd)

    def cache(self, name, content):
        (self.member_dir / name).write_bytes(content)
        self.assertEqual(content, self.open_read_release("/" + name))
        self.assertEqual(
            SyncStatus.CLEAN, self.fs.state_manager.get_file_metadata("/" + name).sync_status,
        )


class TestForegroundTransferClass(ThreadingTestBase):
    def test_cold_full_copy_is_bulk_but_browsing_remains_interactive(self):
        (self.member_dir / "cold.bin").write_bytes(b"cold bytes")
        observed = []
        real_copy = self.member.copy_to_local
        real_list = self.member.listdir_attrs

        def copy(path, target):
            observed.append(("copy", is_interactive_request()))
            return real_copy(path, target)

        def listing(path):
            observed.append(("listing", is_interactive_request()))
            return real_list(path)

        with patch.object(self.member, "copy_to_local", side_effect=copy), \
                patch.object(self.member, "listdir_attrs", side_effect=listing):
            fd = self.fs.__call__("open", "/cold.bin", os.O_RDONLY)
            self.assertEqual(b"cold bytes", self.fs.__call__("read", "/cold.bin", 32, 0, fd))
            self.fs.__call__("release", "/cold.bin", fd)
            self.assertIn("cold.bin", self.fs.__call__("readdir", "/", 0))

        self.assertIn(("copy", False), observed)
        self.assertIn(("listing", True), observed)
        self.assertFalse(is_interactive_request())


class TestForegroundAdmission(ThreadingTestBase):
    def setUp(self):
        super().setUp()
        self.fs.destroy('/')
        self.fs = StorageFS(
            cache_root=self.cache_dir,
            state_db=self.temp_dir / "state.db",
            backends={"member": self.member},
            cache_size=10 * 1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
            max_foreground_operations=3,
        )

    def test_foreground_admission_keeps_browsing_and_reads_moving(self):
        entered = threading.Event()
        release = threading.Event()
        second_entered = threading.Event()

        def blocked_open(path, flags):
            if path == "/first":
                entered.set()
                self.assertTrue(release.wait(WAIT))
            else:
                second_entered.set()
            return path

        with patch.object(self.fs, "open", side_effect=blocked_open), \
                patch.object(self.fs, "getattr", return_value={"st_mode": 0}), \
                patch.object(self.fs, "read", return_value=b"demand bytes"):
            first = Worker(self.fs.__call__, "open", "/first", os.O_RDONLY)
            second = Worker(self.fs.__call__, "open", "/second", os.O_RDONLY)
            first.start()
            self.assertTrue(entered.wait(WAIT))
            second.start()
            try:
                self.assertTrue(self.fs._foreground_admission.waiting_for("ordinary", 1, WAIT))
                self.assertFalse(second_entered.is_set())
                self.assertEqual({"st_mode": 0}, self.fs.__call__("getattr", "/", None))
                self.assertEqual(b"demand bytes", self.fs.__call__("read", "/first", 12, 0, 0))
                self.assertLessEqual(self.fs._foreground_admission.peak_active, 3)
            finally:
                release.set()
                first.finish()
                second.finish()
            self.assertTrue(second_entered.is_set())
            self.assertEqual(0, self.fs._foreground_admission.active_count)

    def test_warm_open_and_directory_handles_progress_while_cold_open_waits(self):
        (self.cache_dir / "warm").write_bytes(b"cached")
        self.assertEqual("demand", self.fs._foreground_class("open", ("/warm", os.O_RDONLY)))
        cold_entered = threading.Event()
        cold_release = threading.Event()
        warm_entered = threading.Event()

        def opened(path, flags):
            if path == "/cold":
                cold_entered.set()
                self.assertTrue(cold_release.wait(WAIT))
            if path == "/warm":
                warm_entered.set()
            return path

        with patch.object(self.fs, "open", side_effect=opened), \
                patch.object(self.fs, "opendir", return_value=17, create=True), \
                patch.object(self.fs, "releasedir", return_value=0, create=True):
            cold = Worker(self.fs.__call__, "open", "/cold", os.O_RDONLY)
            cold.start()
            self.assertTrue(cold_entered.wait(WAIT))
            try:
                self.assertEqual(17, self.fs.__call__("opendir", "/", 0))
                self.assertEqual(0, self.fs.__call__("releasedir", "/", 17))
                self.assertEqual("/warm", self.fs.__call__("open", "/warm", os.O_RDONLY))
                self.assertTrue(warm_entered.is_set())
                self.assertLessEqual(self.fs._foreground_admission.peak_active, 3)
            finally:
                cold_release.set()
                cold.finish()

    def test_changed_cached_open_releases_demand_slot_before_full_copy(self):
        (self.member_dir / "changed").write_bytes(b"new contents")
        (self.cache_dir / "changed").write_bytes(b"old")
        self.assertEqual("demand", self.fs._foreground_class("open", ("/changed", os.O_RDONLY)))
        (self.member_dir / "ready").write_bytes(b"ready bytes")
        ready_fd = self.fs.__call__("open", "/ready", os.O_RDONLY)
        copy = self.gate(self.member, "copy_to_local",
                         only=lambda path, _dest: path == "/changed")

        def invalidate_changed(resolved):
            if resolved.requested_path == "/changed":
                (self.cache_dir / "changed").unlink()

        with patch.object(self.fs, "_revalidate_cached_file", side_effect=invalidate_changed), \
                patch.object(self.fs._foreground_admission, "reclassify_current",
                             wraps=self.fs._foreground_admission.reclassify_current) as moved:
            changed = Worker(self.fs.__call__, "open", "/changed", os.O_RDONLY)
            changed.start()
            self.assertTrue(copy.started.wait(WAIT))
            read = Worker(self.fs.__call__, "read", "/ready", 32, 0, ready_fd)
            read.start()
            try:
                read.join(BLOCKED)
                self.assertFalse(read.is_alive(), "a full copy held the demand slot")
                self.assertEqual(b"ready bytes", read.finish())
                self.assertLessEqual(self.fs._foreground_admission.peak_active, 3)
            finally:
                copy.opened.set()
                changed_fd = changed.finish()
                self.fs.__call__("release", "/changed", changed_fd)
                self.fs.__call__("release", "/ready", ready_fd)
                read.finish()
            self.assertEqual(1, moved.call_count)
        self.assertEqual(0, self.fs._foreground_admission.active_count)

    def test_changed_open_drops_path_lock_before_ordinary_admission(self):
        (self.member_dir / "changed").write_bytes(b"fresh bytes")
        (self.cache_dir / "changed").write_bytes(b"old")

        def invalidate_changed(resolved):
            if resolved.requested_path == "/changed":
                with self.fs._member_checks_lock:
                    self.fs._refetch_pins["/changed"] = self.member
                (self.cache_dir / "changed").unlink()

        with patch.object(self.fs, "_revalidate_cached_file", side_effect=invalidate_changed):
            with self.fs._foreground_admission.admit("ordinary"):
                changed = Worker(self.fs.__call__, "open", "/changed", os.O_RDONLY)
                changed.start()
                self.assertTrue(self.fs._foreground_admission.waiting_for("ordinary", 1, WAIT))
                with self.fs._path_locks._mutex:
                    self.assertNotIn("/changed", self.fs._path_locks._entries)
                with self.fs._member_checks_lock:
                    self.assertNotIn("/changed", self.fs._refetch_pins)
            fd = changed.finish()
            try:
                self.assertEqual(b"fresh bytes", self.fs.__call__("read", "/changed", 32, 0, fd))
            finally:
                self.fs.__call__("release", "/changed", fd)
        self.assertEqual(0, self.fs._foreground_admission.active_count)

    def test_invalid_direct_sync_limit_fails_before_mount_lock(self):
        for workers, maximum in ((-1, None), (2, 1), (1, 33)):
            with self.subTest(workers=workers, maximum=maximum), \
                    patch("storage.fs._acquire_mount_lock_ex") as acquire:
                with self.assertRaises(ValueError):
                    mount_storage(
                        str(self.temp_dir / "mount"), self.cache_dir,
                        self.temp_dir / "state.db", {"member": self.member},
                        1024, allow_other=False, num_sync_workers=workers,
                        max_sync_workers=maximum,
                    )
                acquire.assert_not_called()

    def test_reclassification_cancel_releases_both_slots(self):
        from storage.foreground_admission import AdmissionClosed, ForegroundAdmission

        gate = ForegroundAdmission(3)
        with gate.admit("ordinary"):
            moved = Worker(lambda: self._move_admission(gate))
            moved.start()
            self.assertTrue(gate.waiting_for("ordinary", 1, WAIT))
            self.assertEqual(1, gate.active_count)
            gate.close()
            moved.join(WAIT)
            self.assertFalse(moved.is_alive())
            self.assertIsInstance(moved.error, AdmissionClosed)
            self.assertEqual(0, gate.waiting_count("ordinary"))
            self.assertEqual(1, gate.active_count)
        self.assertEqual(0, gate.active_count)

    @staticmethod
    def _move_admission(gate):
        with gate.admit("demand"):
            gate.reclassify_current("ordinary")

    def test_class_reserves_fifo_and_queued_cancellation(self):
        from storage.foreground_admission import AdmissionClosed

        gate = self.fs._foreground_admission
        release = threading.Event()
        started = [threading.Event() for _ in range(3)]
        order = []

        def hold(kind, index):
            with gate.admit(kind):
                started[index].set()
                order.append(index)
                self.assertTrue(release.wait(WAIT))

        holders = [Worker(hold, kind, index) for index, kind in enumerate(
            ("ordinary", "metadata", "demand")
        )]
        for holder in holders:
            holder.start()
        try:
            self.assertTrue(all(event.wait(WAIT) for event in started))
            self.assertEqual(3, gate.active_count)
            queued_order = []

            def queued(index):
                with gate.admit("ordinary"):
                    queued_order.append(index)

            first = Worker(queued, 1)
            second = Worker(queued, 2)
            first.start()
            self.assertTrue(gate.waiting_for("ordinary", 1, WAIT))
            second.start()
            self.assertTrue(gate.waiting_for("ordinary", 2, WAIT))
            self.assertEqual([], queued_order)
        finally:
            release.set()
            for holder in holders:
                holder.finish()
        first.finish()
        second.finish()
        self.assertEqual([1, 2], queued_order)
        self.assertEqual(3, gate.peak_active)

        cancel = threading.Event()
        with gate.admit("ordinary"):
            waiter = Worker(lambda: self._admit_once(gate, cancel))
            waiter.start()
            self.assertTrue(gate.waiting_for("ordinary", 1, WAIT))
            cancel.set()
            waiter.join(WAIT)
            self.assertFalse(waiter.is_alive())
            self.assertIsInstance(waiter.error, AdmissionClosed)
            self.assertEqual(0, gate.waiting_count("ordinary"))

        with gate.admit("ordinary"):
            waiter = Worker(lambda: self._admit_once(gate))
            waiter.start()
            self.assertTrue(gate.waiting_for("ordinary", 1, WAIT))
            gate.close()
            waiter.join(WAIT)
            self.assertFalse(waiter.is_alive())
            self.assertIsInstance(waiter.error, AdmissionClosed)
            self.assertEqual(1, gate.active_count)  # close did not interrupt us
        self.assertEqual(0, gate.active_count)
        self.assertEqual(0, gate.waiting_count("ordinary"))

    @staticmethod
    def _admit_once(gate, cancel=None):
        with gate.admit("ordinary", cancel=cancel):
            pass


class TestSharedFreshnessObligation(ThreadingTestBase):
    """Two-member schedules for shared freshness and later contract slices.

    _freshness_snapshot and _freshness_token are internal diagnostic seams
    specified in contracts/path-freshness-obligation.md.
    """

    OLD = b"old on both"
    NEW = b"new bytes from changed A"
    PATH = "/f.txt"

    def setUp(self):
        super().setUp()
        self.fs.destroy("/")
        self.a = LocalBackend("changedA", str(self.member_dir))
        self.b_dir = self.temp_dir / "staleB"
        self.b_dir.mkdir()
        self.b = LocalBackend("staleB", str(self.b_dir))
        self.a.speed_priority = 0
        self.b.speed_priority = 50
        self.fs = StorageFS(
            cache_root=self.cache_dir, state_db=self.temp_dir / "state.db",
            backends={"changedA": self.a, "staleB": self.b},
            cache_size=10 * 1024 * 1024, num_sync_workers=0,
            start_sync=False, revalidate_after=60.0,
            max_foreground_operations=3,
        )
        # Force the member stat boundary on every aged open. A fresh parent
        # listing must not short-circuit the observation under test.
        listing = patch.object(self.fs, "_listing_confirms_baseline", return_value=None)
        listing.start()
        self.addCleanup(listing.stop)
        for directory in (self.member_dir, self.b_dir):
            (directory / "f.txt").write_bytes(self.OLD)
            os.utime(directory / "f.txt", (1_700_000_000, 1_700_000_000))
        self.assertEqual(self.OLD, self._read(self.PATH))
        for member in (self.a, self.b):
            self.fs.state_manager.add_file_location(FileLocation(
                virtual_path=self.PATH, backend_name=member.name, checksum="",
                last_synced=time.time(), is_cached=False,
            ))

    def _read(self, path, *, direct=False):
        call = getattr(self.fs, "__call__") if not direct else None
        fd = call("open", path, os.O_RDONLY) if call else self.fs.open(path, os.O_RDONLY)
        try:
            return (call("read", path, 4096, 0, fd) if call else
                    self.fs.read(path, 4096, 0, fd))
        finally:
            if call:
                call("release", path, fd)
            else:
                self.fs.release(path, fd)

    def _change_a(self):
        (self.member_dir / "f.txt").write_bytes(self.NEW)
        os.utime(self.member_dir / "f.txt", (1_700_000_100, 1_700_000_100))
        self._age_checks()

    def _age_checks(self):
        with self.fs._member_checks_lock:
            for path, check in list(self.fs._member_checks.items()):
                self.fs._member_checks[path] = dataclasses.replace(
                    check, verified_at=check.verified_at - 3600,
                )

    def _restart_fs(self, after_stop=None):
        """Close the mount instance, retaining only scratch cache/member state."""
        self.fs.destroy("/")
        if after_stop is not None:
            after_stop()
        self.fs = StorageFS(
            cache_root=self.cache_dir, state_db=self.temp_dir / "state.db",
            backends={"changedA": self.a, "staleB": self.b},
            cache_size=10 * 1024 * 1024, num_sync_workers=0,
            start_sync=False, revalidate_after=60.0,
            max_foreground_operations=3,
        )
        listing = patch.object(self.fs, "_listing_confirms_baseline", return_value=None)
        listing.start()
        self.addCleanup(listing.stop)

    def _late_b_session(self, token, partial=None):
        return SimpleNamespace(
            canonical_path=self.PATH,
            final_cache_path=self.cache_dir / "f.txt",
            partial_cache_path=partial,
            backend=self.b,
            member_stat=self.b.stat(self.PATH),
            freshness_token=token,
        )

    def _assert_local_dirty(self, expected):
        self.assertEqual(expected, (self.cache_dir / "f.txt").read_bytes())
        self.assertEqual(
            SyncStatus.DIRTY,
            self.fs.state_manager.get_file_metadata(self.PATH).sync_status,
        )

    def _assert_no_b_baseline(self):
        check = self.fs._member_checks.get(self.PATH)
        if check is not None:
            self.assertNotIn("staleB", check.members)

    def _fail_dirty_after_registration(self, error, keep_fd):
        captured = {"close_calls": 0}

        def fail(path, cache_path):
            with self.fs._fd_lock:
                attempts = [(vfd, entry) for vfd, entry in self.fs._fd_map.items()
                            if vfd != keep_fd]
                self.assertEqual(1, len(attempts))
                vfd, entry = attempts[0]
                captured.update(vfd=vfd, entry=entry, real_fd=entry.real_fd)
                os.fstat(entry.real_fd)
            raise error

        return fail, captured

    @staticmethod
    def _count_failed_attempt_closes(captured):
        real_close = os.close

        def close(fd):
            if fd == captured.get("real_fd"):
                captured["close_calls"] += 1
            return real_close(fd)

        return patch("storage.fs.os.close", side_effect=close)

    def _assert_failed_attempt_closed(self, captured, keep_fd, keep_real_fd):
        with self.fs._fd_lock:
            self.assertNotIn(captured["vfd"], self.fs._fd_map)
            self.assertEqual(keep_real_fd, self.fs._fd_map[keep_fd].real_fd)
        with self.assertRaises(OSError) as closed:
            os.fstat(captured["real_fd"])
        self.assertEqual(errno.EBADF, closed.exception.errno)
        self.assertEqual(1, captured["close_calls"])
        os.fstat(keep_real_fd)
        with self.fs._member_checks_lock:
            state = self.fs._freshness[self.PATH]
            self.assertTrue(state.uncertain_local)
            self.assertEqual(0, state.active_mutations)
        self.assertIn("changedA", self._snapshot()["debts"])
        self._assert_no_b_baseline()

    def _start_late_callback(self, session):
        """Observe callback entry before a path-locked mutation finishes."""
        entered = threading.Event()

        def call():
            entered.set()
            return self.fs._streaming_manager.on_finalized(session)

        worker = Worker(call)
        worker.start()
        self.assertTrue(entered.wait(WAIT))
        return worker

    def _snapshot(self, path=PATH):
        return self.fs._freshness_snapshot(path)

    def _assert_a_baseline(self, path=PATH):
        check = self.fs._member_checks[path]
        self.assertEqual((len(self.NEW), 1_700_000_100.0), check.members["changedA"])
        self.assertNotIn(path, self.fs._refetch_pins)
        self.assertNotIn(path, self.fs._pinned_stats)

    def _assert_slots_released(self):
        self.assertEqual(0, self.fs._foreground_admission.active_count)
        for kind in self.fs._foreground_admission.CLASSES:
            self.assertEqual(0, self.fs._foreground_admission.waiting_count(kind))

    def _hold_after_invalidation(self):
        dropped = threading.Event()
        resume = threading.Event()
        self.addCleanup(resume.set)
        real = self.fs._invalidate_file_cache

        def invalidate(path):
            result = real(path)
            dropped.set()
            if not resume.wait(WAIT):
                raise AssertionError("invalidation gate was not released")
            return result

        return dropped, resume, patch.object(self.fs, "_invalidate_file_cache", side_effect=invalidate)

    def test_racing_opener_cannot_select_stale_b_while_a_is_owed(self):
        self._change_a()
        # First open has observed changed A and invalidated the old cache,
        # then drops the path lock to transfer to ordinary admission. Hold it
        # at that boundary so a second opener can choose from the members.
        paused = threading.Event()
        resume = threading.Event()
        self.addCleanup(resume.set)
        gate = self.fs._foreground_admission
        real_reclassify = gate.reclassify_current
        first = None

        def hold_first_reclassify(kind):
            if threading.current_thread() is first:
                paused.set()
                if not resume.wait(WAIT):
                    raise AssertionError("first opener admission gate was not released")
            return real_reclassify(kind)

        # A fourth slot lets the second demand request enter while the first
        # still owns its demand slot at this deterministic handoff barrier.
        with patch.object(gate, "cap", 4), \
                patch.object(gate, "reclassify_current", side_effect=hold_first_reclassify), \
                patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b), \
                patch.object(self.a, "copy_to_local", wraps=self.a.copy_to_local) as a_copy, \
                patch.object(self.b, "copy_to_local", wraps=self.b.copy_to_local) as b_copy:
            first = Worker(self._read, self.PATH)
            first.start()
            self.assertTrue(paused.wait(WAIT))
            try:
                second_bytes = self._read(self.PATH)
            finally:
                resume.set()
                first.join(WAIT)
                self.assertFalse(first.is_alive())
            # The byte assertion comes before diagnostics: a missing proposed
            # snapshot method is not the red signal for this regression.
            self.assertEqual(self.NEW, second_bytes)
            self.assertEqual(self.NEW, first.finish())
            self.assertGreaterEqual(a_copy.call_count, 1)
            b_copy.assert_not_called()
        self._assert_a_baseline()
        self.assertIsNone(self._snapshot())
        self._assert_slots_released()

    def test_old_open_exit_cannot_erase_newer_observation(self):
        # First opener pauses after releasing the path lock but before its
        # former private-pin cleanup point. The second opener then publishes
        # A's newer observation; first's exit cannot clear the shared debt.
        released = threading.Event()
        first_resume = threading.Event()
        self.addCleanup(first_resume.set)
        dropped, second_resume, hold_invalidation = self._hold_after_invalidation()
        real_path_locked = self.fs._path_locked
        first = None

        @contextmanager
        def hold_after_path_lock(*paths):
            with real_path_locked(*paths):
                yield
            if threading.current_thread() is first:
                released.set()
                if not first_resume.wait(WAIT):
                    raise AssertionError("first opener exit gate was not released")

        with patch.object(self.fs, "_path_locked", side_effect=hold_after_path_lock), \
                patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b), \
                hold_invalidation:
            first = Worker(self.fs.open, self.PATH, os.O_RDONLY)
            first.start()
            self.assertTrue(released.wait(WAIT))
            self._change_a()
            second = Worker(self._read, self.PATH)
            second.start()
            self.assertTrue(dropped.wait(WAIT))
            first_fd = None
            try:
                first_resume.set()
                first_fd = first.finish()
                second_resume.set()
                self.assertEqual(self.NEW, second.finish())
                self.assertEqual(self.OLD, self.fs.read(self.PATH, 4096, 0, first_fd))
            finally:
                first_resume.set()
                second_resume.set()
                if first_fd is not None:
                    self.fs.release(self.PATH, first_fd)
        self._assert_a_baseline()
        self._assert_slots_released()

    def test_cancelled_admission_leaves_a_owed_for_next_opener(self):
        self._change_a()
        gate = self.fs._foreground_admission
        with patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b):
            with gate.admit("ordinary"):
                first = Worker(self._read, self.PATH)
                first.start()
                self.assertTrue(gate.waiting_for("ordinary", 1, WAIT))
                gate.close()
                first.join(WAIT)
                self.assertFalse(first.is_alive())
                self.assertIsInstance(first.error, FuseOSError)
                self.assertEqual(errno.EINTR, first.error.errno)
            self.assertEqual("changedA", self._snapshot()["owed_member"])
            # Direct StorageFS call bypasses the now-closed FUSE gate; the
            # shared freshness obligation still controls member selection.
            self.assertEqual(self.NEW, self._read(self.PATH, direct=True))
        self._assert_a_baseline()
        self._assert_slots_released()

    def test_failed_a_fetch_keeps_obligation_and_never_accepts_stale_b(self):
        self._change_a()
        with patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b):
            with patch.object(self.a, "copy_to_local", side_effect=OSError(errno.EIO, "A failed")):
                with self.assertRaises((OSError, FuseOSError)):
                    self._read(self.PATH)
            self.assertEqual("changedA", self._snapshot()["owed_member"])
            self.assertEqual(self.NEW, self._read(self.PATH, direct=True))
        self._assert_a_baseline()
        self._assert_slots_released()

    def test_unavailable_a_fallback_keeps_recheck_debt_until_a_returns(self):
        self._change_a()
        real_stat = self.a.stat

        def stat_then_drop(path):
            result = real_stat(path)
            self.a.set_available(False)
            return result

        try:
            with patch.object(self.a, "stat", side_effect=stat_then_drop), \
                    patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b):
                self.assertEqual(self.OLD, self._read(self.PATH))
            debt = self._snapshot()
            self.assertEqual("changedA", debt["owed_member"])
            self.assertIn("changedA", debt["recheck_members"])
        finally:
            self.a.set_available(True)
        self.assertEqual(self.NEW, self._read(self.PATH))
        self.assertIsNone(self._snapshot())
        self._assert_a_baseline()
        self._assert_slots_released()

    def _prime_owed_a_with_b_cached(self):
        """Observe changed A, then make its copy unavailable so B fills cache."""
        self._change_a()
        real_stat = self.a.stat

        def stat_then_drop(path):
            result = real_stat(path)
            self.a.set_available(False)
            return result

        self.addCleanup(self.a.set_available, True)
        with patch.object(self.a, "stat", side_effect=stat_then_drop), \
                patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b):
            self.assertEqual(self.OLD, self._read(self.PATH))
        self.a.set_available(True)
        self.assertEqual("changedA", self._snapshot()["owed_member"])

    def _expire_owed_probe(self):
        key = (self.PATH, "changedA")
        with self.fs._member_checks_lock:
            generation, _deadline, was_available = self.fs._freshness_probe_retry[key]
            self.fs._freshness_probe_retry[key] = (
                generation, time.monotonic() - 1.0, was_available,
            )

    def test_owed_available_stat_failure_is_throttled_until_expiry(self):
        self._prime_owed_a_with_b_cached()
        with patch.object(self.a, "stat", side_effect=OSError(errno.EIO, "slow member")) as probe:
            for _ in range(4):
                self.assertEqual(self.OLD, self._read(self.PATH))
            self.assertEqual(1, probe.call_count)
        self.assertEqual("changedA", self._snapshot()["owed_member"])
        # A fresh successful B baseline must not extend A's retry deadline.
        cache_path = self.cache_dir / "f.txt"
        identity = self.fs._cache_identity(os.lstat(cache_path))
        b_stat = self.b.stat(self.PATH)
        self.fs._store_member_check(self.PATH, cache_path, identity,
                                    {"staleB": (b_stat.st_size, b_stat.st_mtime)})
        self._expire_owed_probe()
        self.assertEqual(self.NEW, self._read(self.PATH))
        self._assert_a_baseline()
        self.assertIsNone(self._snapshot())
        self._assert_slots_released()

    def test_owed_missing_file_probe_is_throttled_while_member_reports_available(self):
        self._prime_owed_a_with_b_cached()
        with patch.object(self.a, "stat", side_effect=FileNotFoundError(self.PATH)) as probe:
            for _ in range(4):
                self.assertEqual(self.OLD, self._read(self.PATH))
            self.assertEqual(1, probe.call_count)
        self.assertTrue(self.a.is_available())
        self.assertEqual("changedA", self._snapshot()["owed_member"])
        self._expire_owed_probe()
        self.assertEqual(self.NEW, self._read(self.PATH))
        self._assert_a_baseline()
        self._assert_slots_released()

    def test_owed_failed_probe_retries_when_member_becomes_available(self):
        self._prime_owed_a_with_b_cached()

        def fail_and_drop(_path):
            self.a.set_available(False)
            raise OSError(errno.EIO, "member went away")

        with patch.object(self.a, "stat", side_effect=fail_and_drop) as probe:
            self.assertEqual(self.OLD, self._read(self.PATH))
            self.assertEqual(self.OLD, self._read(self.PATH))
            self.assertEqual(1, probe.call_count)
        self.assertEqual("changedA", self._snapshot()["owed_member"])
        self.a.set_available(True)
        self.assertEqual(self.NEW, self._read(self.PATH))
        self._assert_a_baseline()
        self._assert_slots_released()

    def test_owed_failed_probe_recovers_after_later_offline_observation(self):
        self._prime_owed_a_with_b_cached()
        with patch.object(self.a, "stat", side_effect=OSError(errno.EIO, "stat failed")) as probe:
            self.assertEqual(self.OLD, self._read(self.PATH))
            self.assertEqual(1, probe.call_count)
        self.assertEqual("changedA", self._snapshot()["owed_member"])

        # The failure happened while A reported available. A later open
        # actually observes it offline; its return before the retry deadline
        # must recheck immediately rather than serve B for the full delay.
        self.a.set_available(False)
        seen_available = []
        real_available = self.a.is_available

        def observe_available():
            value = real_available()
            seen_available.append(value)
            return value

        with patch.object(self.a, "is_available", side_effect=observe_available):
            self.assertEqual(self.OLD, self._read(self.PATH))
        self.assertIn(False, seen_available)
        self.assertEqual("changedA", self._snapshot()["owed_member"])

        self.a.set_available(True)
        self.assertEqual(self.NEW, self._read(self.PATH))
        self._assert_a_baseline()
        self.assertIsNone(self._snapshot())
        self._assert_slots_released()

    def test_rename_during_admission_wait_moves_or_preserves_debt(self):
        self._change_a()
        gate = self.fs._foreground_admission
        with patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b):
            with gate.admit("ordinary"):
                first = Worker(self._read, self.PATH)
                first.start()
                self.assertTrue(gate.waiting_for("ordinary", 1, WAIT))
                self.fs.rename(self.PATH, "/moved.txt")
                self.assertIsNone(self._snapshot(self.PATH))
                self.assertEqual("changedA", self._snapshot("/moved.txt")["owed_member"])
            first.join(WAIT)
            self.assertFalse(first.is_alive())
            self.assertIsNotNone(first.error)  # old name no longer resolves
            self.assertEqual(self.NEW, self._read("/moved.txt", direct=True))
        self._assert_a_baseline("/moved.txt")
        self._assert_slots_released()

    def test_failed_rename_during_wait_keeps_old_identity_and_debt(self):
        self._change_a()
        gate = self.fs._foreground_admission
        with patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b):
            with gate.admit("ordinary"):
                first = Worker(self._read, self.PATH)
                first.start()
                self.assertTrue(gate.waiting_for("ordinary", 1, WAIT))
                with patch.object(self.fs.namespace, "rename", side_effect=OSError(errno.EIO, "rename failed")):
                    with self.assertRaises(OSError):
                        self.fs.rename(self.PATH, "/moved.txt")
                self.assertEqual("changedA", self._snapshot()["owed_member"])
            self.assertEqual(self.NEW, first.finish())
        self._assert_a_baseline()
        self._assert_slots_released()

    def test_partial_failed_rename_during_wait_requires_reprobe(self):
        self._change_a()
        gate = self.fs._foreground_admission

        def move_members_then_fail(old, new, on_local_moved=None):
            for directory in (self.member_dir, self.b_dir):
                os.rename(directory / "f.txt", directory / "moved.txt")
            raise OSError(errno.EIO, "rename failed after member moves")

        with patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b):
            with gate.admit("ordinary"):
                first = Worker(self._read, self.PATH)
                first.start()
                self.assertTrue(gate.waiting_for("ordinary", 1, WAIT))
                with patch.object(self.fs.namespace, "rename", side_effect=move_members_then_fail):
                    with self.assertRaises(OSError):
                        self.fs.rename(self.PATH, "/moved.txt")
                self.assertEqual("reprobe", self._snapshot(self.PATH)["state"])
                self.assertEqual("reprobe", self._snapshot("/moved.txt")["state"])
            first.join(WAIT)
            self.assertFalse(first.is_alive())
            self.assertIsNotNone(first.error)
            self.assertEqual(self.NEW, self._read("/moved.txt", direct=True))
        self._assert_a_baseline("/moved.txt")
        self._assert_slots_released()

    def test_late_stream_callback_cannot_replace_newer_a_baseline(self):
        # A session started on stale B before A's changed generation existed.
        # Its callback arrives after A bytes and baseline have committed.
        old_token = self.fs._freshness_token(self.PATH, "staleB")
        old_session = SimpleNamespace(
            canonical_path=self.PATH,
            final_cache_path=self.cache_dir / "f.txt",
            backend=self.b,
            member_stat=self.b.stat(self.PATH),
            freshness_token=old_token,
        )
        self._change_a()
        with patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b):
            self.assertEqual(self.NEW, self._read(self.PATH))
        self._assert_a_baseline()
        late = Worker(self.fs._streaming_manager.on_finalized, old_session)
        late.finish_started()
        self.assertEqual(self.NEW, (self.cache_dir / "f.txt").read_bytes())
        self._assert_a_baseline()
        self.assertIsNone(self._snapshot())
        self._assert_slots_released()

    def test_late_stream_partial_cannot_install_stale_b_bytes(self):
        # Model a B stream that began before A's newer observation. The
        # actual no-replace install boundary must reject its old token even
        # while the final cache path is temporarily absent.
        old_token = self.fs._freshness_token(self.PATH, "staleB")
        self._change_a()
        gate = self.fs._foreground_admission
        with gate.admit("ordinary"):
            opener = Worker(self._read, self.PATH)
            opener.start()
            self.assertTrue(gate.waiting_for("ordinary", 1, WAIT))
            current_token = self.fs._freshness_token(self.PATH, "staleB")
            self.assertNotEqual(old_token[1], current_token[1])  # path revision
            self.assertEqual(old_token[3], current_token[3])  # B generation
            before = self._snapshot()
            self.assertIn("changedA", before["debts"])
            final = self.cache_dir / "f.txt"
            self.assertFalse(final.exists())
            partial = self.cache_dir / "f.txt.storage.partial"
            partial.write_bytes(self.OLD)
            old_session = SimpleNamespace(
                canonical_path=self.PATH,
                final_cache_path=final,
                partial_cache_path=partial,
                backend=self.b,
                member_stat=self.b.stat(self.PATH),
                freshness_token=old_token,
            )
            outcome = self.fs._streaming_manager._install_partial(old_session)
            self.assertNotEqual("installed", outcome)
            self.assertFalse(final.exists())
            after = self._snapshot()
            self.assertEqual(before["publication_revision"], after["publication_revision"])
            self.assertIn("changedA", after["debts"])
        self.assertEqual(self.NEW, opener.finish())
        self.assertEqual(self.NEW, final.read_bytes())
        self._assert_a_baseline()
        self._assert_slots_released()

    def _prepared_stream(self, backend):
        """Start a real session, then supply its complete scratch-owned bytes."""
        manager = self.fs._streaming_manager
        with patch.object(manager, "select_backend_for_read", return_value=backend):
            session = manager._create_or_attach_prefetch_session(self.PATH, self.PATH)
        self.assertIsNotNone(session)
        data = (self.member_dir if backend is self.a else self.b_dir) / "f.txt"
        payload = data.read_bytes()
        os.pwrite(session.io_fd, payload, 0)
        with session.lock:
            session.available_ranges = [(0, len(payload))]
        return session

    def test_current_a_stream_commits_baseline_and_its_debt(self):
        self._change_a()
        with self.fs._path_locked(self.PATH):
            self.fs._observe_changed_member(self.PATH, self.a, self.a.stat(self.PATH))
            self.fs._invalidate_file_cache(self.PATH)
        session = self._prepared_stream(self.a)
        self.fs._finalize_streaming_session(session)
        self.assertEqual(self.NEW, (self.cache_dir / "f.txt").read_bytes())
        self._assert_a_baseline()
        self.assertIsNone(self._snapshot())

    def test_old_b_stream_cannot_install_after_a_observation(self):
        final = self.cache_dir / "f.txt"
        final.unlink()
        session = self._prepared_stream(self.b)
        self._change_a()
        with self.fs._path_locked(self.PATH):
            self.fs._observe_changed_member(self.PATH, self.a, self.a.stat(self.PATH))
        before = self._snapshot()
        self.fs._finalize_streaming_session(session)
        self.assertFalse(final.exists())
        self.assertEqual(before["debts"], self._snapshot()["debts"])
        self.assertEqual(before["publication_revision"], self._snapshot()["publication_revision"])

    def test_old_b_stream_cannot_publish_over_occupied_final_path(self):
        final = self.cache_dir / "f.txt"
        final.unlink()
        session = self._prepared_stream(self.b)
        self._change_a()
        with self.fs._path_locked(self.PATH):
            self.fs._observe_changed_member(self.PATH, self.a, self.a.stat(self.PATH))
        final.write_bytes(self.NEW)
        before = self._snapshot()
        self.fs._finalize_streaming_session(session)
        self.assertEqual(self.NEW, final.read_bytes())
        self.assertEqual(before["debts"], self._snapshot()["debts"])
        self._assert_no_b_baseline()

    def test_failed_and_cancelled_a_streams_keep_debt(self):
        self._change_a()
        with self.fs._path_locked(self.PATH):
            self.fs._observe_changed_member(self.PATH, self.a, self.a.stat(self.PATH))
            self.fs._invalidate_file_cache(self.PATH)
        manager = self.fs._streaming_manager
        failed = self._prepared_stream(self.a)
        with failed.lock:
            failed.error = OSError(5, "failed member read")
            failed.available_ranges = []
            failed.condition.notify_all()
        manager._finalize_streaming_session(failed)
        manager._release_prefetch_session(failed)
        failed.worker.join(WAIT)
        self.assertFalse(failed.worker.is_alive())
        self.assertFalse((self.cache_dir / "f.txt").exists())
        self.assertIn("changedA", self._snapshot()["debts"])
        cancelled = self._prepared_stream(self.a)
        manager._release_prefetch_session(cancelled)
        cancelled.worker.join(WAIT)
        self.assertFalse(cancelled.worker.is_alive())
        self.assertFalse((self.cache_dir / "f.txt").exists())
        self.assertIn("changedA", self._snapshot()["debts"])
        self._assert_no_b_baseline()

    def test_stale_b_prefix_rejected_before_install(self):
        final = self.cache_dir / "f.txt"
        final.unlink()
        self.fs.state_manager.delete_file(self.PATH)
        session = self._prepared_stream(self.b)
        self._change_a()
        with self.fs._path_locked(self.PATH):
            self.fs._observe_changed_member(self.PATH, self.a, self.a.stat(self.PATH))
        before = self._snapshot()
        self.assertFalse(self.fs._promote_failed_session_cache_prefix(session))
        self.assertFalse(final.exists())
        self.assertEqual(before["debts"], self._snapshot()["debts"])
        self.fs._streaming_manager._cleanup_prefetch_session(session, "failed")

    def test_b_stream_commit_leaves_independent_a_debt(self):
        self._change_a()
        with self.fs._path_locked(self.PATH):
            self.fs._observe_changed_member(self.PATH, self.a, self.a.stat(self.PATH))
            self.fs._invalidate_file_cache(self.PATH)
        (self.b_dir / "f.txt").write_bytes(b"independent B change")
        os.utime(self.b_dir / "f.txt", (1_700_000_200, 1_700_000_200))
        with self.fs._path_locked(self.PATH):
            self.fs._observe_changed_member(self.PATH, self.b, self.b.stat(self.PATH))
        session = self._prepared_stream(self.b)
        self.fs._finalize_streaming_session(session)
        self.assertEqual(b"independent B change", (self.cache_dir / "f.txt").read_bytes())
        debt = self._snapshot()["debts"]
        self.assertIn("changedA", debt)
        self.assertNotIn("staleB", debt)
        self.assertIn("staleB", self.fs._member_checks[self.PATH].members)

    def test_b_stream_fallback_keeps_a_debt(self):
        self._change_a()
        with self.fs._path_locked(self.PATH):
            self.fs._observe_changed_member(self.PATH, self.a, self.a.stat(self.PATH))
            self.fs._invalidate_file_cache(self.PATH)
        self.a.set_available(False)
        session = self._prepared_stream(self.b)
        self.fs._finalize_streaming_session(session)
        self.assertEqual(self.OLD, (self.cache_dir / "f.txt").read_bytes())
        self.assertIn("changedA", self._snapshot()["debts"])
        check = self.fs._member_checks[self.PATH]
        self.assertIn("staleB", check.members)
        self.assertNotIn("changedA", check.members)

    def test_write_byte_to_dirty_interval_rejects_late_stream_commit(self):
        fd = self.fs.open(self.PATH, os.O_RDWR)
        self._change_a()
        self.assertEqual(self.OLD, self._read(self.PATH))
        self.assertIn("changedA", self._snapshot()["debts"])
        # Make B an eligible fallback before capturing its token. A remains
        # owed while unavailable; only the later local mutation invalidates
        # this B attempt, rather than A's already-published observation.
        self.a.set_available(False)
        self.assertIs(self.b, self.fs._select_backend_for_read(self.PATH))
        token = self.fs._freshness_token(self.PATH, "staleB")
        entered, resume = threading.Event(), threading.Event()
        self.addCleanup(resume.set)
        real_mark = self.fs._mark_cache_file_dirty

        def mark_after_bytes(path, cache_path):
            entered.set()
            self.assertTrue(resume.wait(WAIT))
            return real_mark(path, cache_path)

        partial = self.cache_dir / "write-race.storage.partial"
        partial.write_bytes(self.OLD)
        try:
            with patch.object(self.fs, "_mark_cache_file_dirty", side_effect=mark_after_bytes):
                writer = Worker(self.fs.write, self.PATH, b"local", 0, fd)
                writer.start()
                self.assertTrue(entered.wait(WAIT))
                self.assertEqual(b"local", (self.cache_dir / "f.txt").read_bytes()[:5])
                late = self._late_b_session(token, partial)
                # The final inode belongs to the writer. No-replace must win
                # even before its DIRTY row is published.
                self.assertNotEqual("installed", self.fs._streaming_manager._install_partial(late))
                callback = self._start_late_callback(late)
                # write() holds metadata_lock through mark_file_dirty. A
                # correct callback may reject the token now, or wait for that
                # lock; it must not publish while the writer is paused.
                callback.join(BLOCKED)
                self.assertTrue(writer.is_alive())
                self.assertIn("changedA", self._snapshot()["debts"])
                self._assert_no_b_baseline()
                resume.set()
                self.assertEqual(5, writer.finish())
                callback.finish()
            self._assert_local_dirty(b"local" + self.OLD[5:])
            self.assertIn("changedA", self._snapshot()["debts"])
            self._assert_no_b_baseline()
        finally:
            resume.set()
            self.a.set_available(True)
            self.fs.release(self.PATH, fd)
        self._assert_slots_released()

    def test_truncate_byte_to_dirty_interval_rejects_late_baseline(self):
        writer_fd = self.fs.open(self.PATH, os.O_RDWR)
        try:
            self._change_a()
            self.assertEqual(self.OLD, self._read(self.PATH))
            self.assertIn("changedA", self._snapshot()["debts"])
        finally:
            self.fs.release(self.PATH, writer_fd)
        token = self.fs._freshness_token(self.PATH, "staleB")
        entered, resume = threading.Event(), threading.Event()
        self.addCleanup(resume.set)
        real_mark = self.fs._mark_cache_file_dirty

        def mark_after_truncate(path, cache_path):
            entered.set()
            self.assertTrue(resume.wait(WAIT))
            return real_mark(path, cache_path)

        with patch.object(self.fs, "_mark_cache_file_dirty", side_effect=mark_after_truncate):
            truncater = Worker(self.fs.truncate, self.PATH, 3)
            truncater.start()
            self.assertTrue(entered.wait(WAIT))
            self.assertEqual(b"old", (self.cache_dir / "f.txt").read_bytes())
            late = self._start_late_callback(self._late_b_session(token))
            resume.set()
            truncater.finish()
            late.finish()
        self._assert_local_dirty(b"old")
        self.assertIn("changedA", self._snapshot()["debts"])
        self._assert_no_b_baseline()
        self._assert_slots_released()

    def test_create_replacement_rejects_old_stream_and_keeps_dirty_bytes(self):
        token = self.fs._freshness_token(self.PATH, "staleB")
        entered, resume = threading.Event(), threading.Event()
        self.addCleanup(resume.set)
        real_set = self.fs.state_manager.set_file_metadata

        def set_after_truncation(metadata):
            entered.set()
            self.assertTrue(resume.wait(WAIT))
            return real_set(metadata)

        with patch.object(self.fs.state_manager, "set_file_metadata", side_effect=set_after_truncation):
            creator = Worker(self.fs.create, self.PATH, 0o600)
            creator.start()
            self.assertTrue(entered.wait(WAIT))
            self.assertEqual(b"", (self.cache_dir / "f.txt").read_bytes())
            late = self._start_late_callback(self._late_b_session(token))
            resume.set()
            fd = creator.finish()
            late.finish()
        try:
            self.assertNotEqual(token[0], self.fs._freshness_token(self.PATH, "staleB")[0])
            self.assertEqual(5, self.fs.write(self.PATH, b"local", 0, fd))
            self._assert_local_dirty(b"local")
            self._assert_no_b_baseline()
        finally:
            self.fs.release(self.PATH, fd)
        self._assert_slots_released()

    def test_full_fetch_rejects_changed_epoch_with_final_path_absent(self):
        # A write through an old descriptor can race a private B copy even
        # when its cache name has been invalidated. No-replace alone cannot
        # reject B here: the final name is absent at the commit boundary.
        writer_fd = self.fs.open(self.PATH, os.O_RDWR)
        self._change_a()
        with self.fs._path_locked(self.PATH):
            self.fs._observe_changed_member(self.PATH, self.a, self.a.stat(self.PATH))
            self.fs._invalidate_file_cache(self.PATH)
        self.a.set_available(False)
        final = self.cache_dir / "f.txt"
        final.unlink(missing_ok=True)
        copied, resume = threading.Event(), threading.Event()
        self.addCleanup(resume.set)
        real_copy = self.b.copy_to_local

        def pause_after_copy(path, destination):
            real_copy(path, destination)
            copied.set()
            self.assertTrue(resume.wait(WAIT))

        opener = Worker(self.fs.open, self.PATH, os.O_RDONLY)
        try:
            with patch.object(self.b, "copy_to_local", side_effect=pause_after_copy):
                opener.start()
                self.assertTrue(copied.wait(WAIT))
                self.assertFalse(final.exists())
                with self.assertRaises(FuseOSError):
                    self.fs.write(self.PATH, b"local", 0, writer_fd)
                with self.fs._fd_lock:
                    local_fd = self.fs._fd_map[writer_fd].real_fd
                self.assertEqual(b"local", os.pread(local_fd, 5, 0))
                resume.set()
                opener.join(WAIT)
                self.assertFalse(opener.is_alive())
            if opener.error is None:
                fetched_fd = opener.result
                try:
                    fetched = self.fs.read(self.PATH, 4096, 0, fetched_fd)
                finally:
                    self.fs.release(self.PATH, fetched_fd)
            else:
                fetched = final.read_bytes() if final.exists() else None
            self.assertNotEqual(self.OLD, fetched)
            self.assertFalse(final.exists())
            self.assertIn("changedA", self._snapshot()["debts"])
            self._assert_no_b_baseline()
        finally:
            resume.set()
            self.a.set_available(True)
            self.fs.release(self.PATH, writer_fd)

    def test_full_fetch_loses_to_local_write_and_returns_its_inode(self):
        writer_fd = self.fs.open(self.PATH, os.O_RDWR)
        self._change_a()
        with self.fs._path_locked(self.PATH):
            self.fs._observe_changed_member(self.PATH, self.a, self.a.stat(self.PATH))
            self.fs._invalidate_file_cache(self.PATH)
        self.a.set_available(False)
        final = self.cache_dir / "f.txt"
        held_inode = self.cache_dir / "f-held-inode"
        os.link(final, held_inode)
        final.unlink()
        self.assertFalse(final.exists())
        copied, resume = threading.Event(), threading.Event()
        self.addCleanup(resume.set)
        real_copy = self.b.copy_to_local

        def pause_after_copy(path, destination):
            real_copy(path, destination)
            copied.set()
            self.assertTrue(resume.wait(WAIT))

        try:
            with patch.object(self.b, "copy_to_local", side_effect=pause_after_copy):
                opener = Worker(self.fs.open, self.PATH, os.O_RDONLY)
                opener.start()
                self.assertTrue(copied.wait(WAIT))
                # The exact local inode is linked back while the staged B
                # copy remains private; write() publishes its DIRTY row.
                os.link(held_inode, final)
                self.assertEqual(5, self.fs.write(self.PATH, b"local", 0, writer_fd))
                with self.fs._fd_lock:
                    local_inode = os.fstat(self.fs._fd_map[writer_fd].real_fd).st_ino
                resume.set()
                returned_fd = opener.finish()
            try:
                with self.fs._fd_lock:
                    returned_inode = os.fstat(self.fs._fd_map[returned_fd].real_fd).st_ino
                self.assertEqual(local_inode, returned_inode)
                self.assertEqual(b"local" + self.OLD[5:],
                                 self.fs.read(self.PATH, 4096, 0, returned_fd))
                self._assert_local_dirty(b"local" + self.OLD[5:])
                self.assertIn("changedA", self._snapshot()["debts"])
                self._assert_no_b_baseline()
            finally:
                self.fs.release(self.PATH, returned_fd)
        finally:
            resume.set()
            self.a.set_available(True)
            held_inode.unlink(missing_ok=True)
            self.fs.release(self.PATH, writer_fd)

    def test_o_trunc_announces_mutation_before_dirty_publication(self):
        writer_fd = self.fs.open(self.PATH, os.O_RDWR)
        self._change_a()
        self.assertEqual(self.OLD, self._read(self.PATH))
        self.assertIn("changedA", self._snapshot()["debts"])
        self.a.set_available(False)
        token = self.fs._freshness_token(self.PATH, "staleB")
        entered, resume = threading.Event(), threading.Event()
        self.addCleanup(resume.set)
        real_mark = self.fs._mark_cache_file_dirty

        def hold_after_truncation(path, cache_path):
            entered.set()
            self.assertTrue(resume.wait(WAIT))
            return real_mark(path, cache_path)

        try:
            with patch.object(self.fs, "_mark_cache_file_dirty", side_effect=hold_after_truncation):
                opener = Worker(self.fs.open, self.PATH, os.O_RDWR | os.O_TRUNC)
                opener.start()
                self.assertTrue(entered.wait(WAIT))
                self.assertEqual(b"", (self.cache_dir / "f.txt").read_bytes())
                self.assertNotEqual(token, self.fs._freshness_token(self.PATH, "staleB"))
                self.assertIn("changedA", self._snapshot()["debts"])
                self._assert_no_b_baseline()
                resume.set()
                fd = opener.finish()
            self._assert_local_dirty(b"")
            self.fs.release(self.PATH, fd)
        finally:
            resume.set()
            self.a.set_available(True)
            self.fs.release(self.PATH, writer_fd)

    def test_failed_dirty_publication_keeps_local_uncertainty(self):
        fd = self.fs.open(self.PATH, os.O_RDWR)
        self._change_a()
        self.assertEqual(self.OLD, self._read(self.PATH))
        self.a.set_available(False)
        token = self.fs._freshness_token(self.PATH, "staleB")
        try:
            with patch.object(self.fs, "_mark_cache_file_dirty",
                              side_effect=OSError(errno.EIO, "failed dirty row")):
                with self.assertRaises(FuseOSError):
                    self.fs.write(self.PATH, b"local", 0, fd)
            self.assertEqual(b"local" + self.OLD[5:],
                             (self.cache_dir / "f.txt").read_bytes())
            self.assertNotEqual(token, self.fs._freshness_token(self.PATH, "staleB"))
            with self.fs._member_checks_lock:
                state = self.fs._freshness[self.PATH]
                self.assertTrue(state.uncertain_local)
                self.assertEqual(0, state.active_mutations)
            self.assertIn("changedA", self._snapshot()["debts"])
            self._assert_no_b_baseline()
        finally:
            self.a.set_available(True)
            self.fs.release(self.PATH, fd)

    def test_successful_dirty_publication_recovers_failed_mutation_for_refetch(self):
        fd = self.fs.open(self.PATH, os.O_RDWR)
        self._change_a()
        self.assertEqual(self.OLD, self._read(self.PATH))
        self.a.set_available(False)
        cache_path = self.cache_dir / "f.txt"
        try:
            with patch.object(self.fs, "_mark_cache_file_dirty",
                              side_effect=OSError(errno.EIO, "failed dirty row")):
                with self.assertRaises(FuseOSError):
                    self.fs.write(self.PATH, b"first", 0, fd)
            self.assertEqual(b"first" + self.OLD[5:], cache_path.read_bytes())
            self.assertEqual(5, self.fs.write(self.PATH, b"local", 0, fd))
            self._assert_local_dirty(b"local" + self.OLD[5:])
        finally:
            self.fs.release(self.PATH, fd)

        # Model a completed, generation-checked upload to the available B
        # member without a live sync worker. The independently owed A debt
        # cannot be cleared by B's copy or by this CLEAN row.
        self.b.copy_from_local(cache_path, self.PATH)
        row = self.fs.state_manager.get_file_metadata(self.PATH)
        self.assertTrue(self.fs.state_manager.publish_sync_status(
            self.PATH, row.dirty_generation, SyncStatus.CLEAN))
        row = self.fs.state_manager.get_file_metadata(self.PATH)
        self.fs.cache_manager.sync_cache_metadata(self.PATH, row, cache_path=cache_path)
        self.fs.cache_manager._evict_if_still_clean(self.PATH, cache_path)
        self.assertFalse(cache_path.exists())

        try:
            # The old code keeps uncertain_local forever, rejects this
            # private B stage and cannot return the bytes it just synced.
            self.assertEqual(b"local" + self.OLD[5:], self._read(self.PATH))
            self.assertIn("changedA", self._snapshot()["debts"])
            self.assertIn("staleB", self.fs._member_checks[self.PATH].members)
            with self.fs._member_checks_lock:
                self.assertFalse(self.fs._freshness[self.PATH].uncertain_local)
        finally:
            self.a.set_available(True)

    def _overlapping_writes_then_refetch(self, first_to_finish):
        fd = self.fs.open(self.PATH, os.O_RDWR)
        self._change_a()
        self.assertEqual(self.OLD, self._read(self.PATH))
        self.a.set_available(False)
        cache_path = self.cache_dir / "f.txt"
        entered = [threading.Event(), threading.Event()]
        resume = [threading.Event(), threading.Event()]
        for gate in resume:
            self.addCleanup(gate.set)
        real_pwrite = os.pwrite
        workers = []

        def pause_before_bytes(real_fd, data, offset):
            for index, worker in enumerate(workers):
                if threading.current_thread() is worker:
                    entered[index].set()
                    self.assertTrue(resume[index].wait(WAIT))
                    break
            return real_pwrite(real_fd, data, offset)

        workers.extend((Worker(self.fs.write, self.PATH, b"first", 0, fd),
                        Worker(self.fs.write, self.PATH, b"later", 0, fd)))
        try:
            with patch("storage.fs.os.pwrite", side_effect=pause_before_bytes):
                workers[0].start()
                self.assertTrue(entered[0].wait(WAIT))
                workers[1].start()
                self.assertTrue(entered[1].wait(WAIT))
                resume[first_to_finish].set()
                self.assertEqual(5, workers[first_to_finish].finish())
                resume[1 - first_to_finish].set()
                self.assertEqual(5, workers[1 - first_to_finish].finish())
            expected = (b"first" if first_to_finish == 1 else b"later") + self.OLD[5:]
            self._assert_local_dirty(expected)
            self.assertIn("changedA", self._snapshot()["debts"])
            self._assert_no_b_baseline()
        finally:
            for gate in resume:
                gate.set()
            for worker in workers:
                if worker.ident is not None:
                    worker.join(WAIT)
            self.fs.release(self.PATH, fd)

        # This is the behavioral assertion: permanent uncertainty makes the
        # correctly uploaded bytes unreachable after clean eviction.
        self._sync_evict_and_refetch_local_bytes(expected)

    def _sync_evict_and_refetch_local_bytes(self, expected):
        cache_path = self.cache_dir / "f.txt"
        self.b.copy_from_local(cache_path, self.PATH)
        row = self.fs.state_manager.get_file_metadata(self.PATH)
        self.assertTrue(self.fs.state_manager.publish_sync_status(
            self.PATH, row.dirty_generation, SyncStatus.CLEAN))
        row = self.fs.state_manager.get_file_metadata(self.PATH)
        self.fs.cache_manager.sync_cache_metadata(self.PATH, row, cache_path=cache_path)
        self.fs.cache_manager._evict_if_still_clean(self.PATH, cache_path)
        self.assertFalse(cache_path.exists())
        try:
            self.assertEqual(expected, self._read(self.PATH))
            self.assertIn("changedA", self._snapshot()["debts"])
            self.assertIn("staleB", self.fs._member_checks[self.PATH].members)
            with self.fs._member_checks_lock:
                self.assertFalse(self.fs._freshness[self.PATH].uncertain_local)
        finally:
            self.a.set_available(True)

    def test_overlapping_successful_writes_older_finishes_last_refetches(self):
        self._overlapping_writes_then_refetch(first_to_finish=1)

    def test_overlapping_successful_writes_newer_finishes_last_refetches(self):
        self._overlapping_writes_then_refetch(first_to_finish=0)

    def test_later_failed_dirty_mark_survives_old_successful_finalizer(self):
        fd = self.fs.open(self.PATH, os.O_RDWR)
        self._change_a()
        self.assertEqual(self.OLD, self._read(self.PATH))
        self.a.set_available(False)
        marked, resume = threading.Event(), threading.Event()
        self.addCleanup(resume.set)
        real_mutation = self.fs._local_mutation
        real_mark = self.fs._mark_cache_file_dirty
        workers = []

        @contextmanager
        def pause_after_first_mark(path, cache_path, **kwargs):
            with real_mutation(path, cache_path, **kwargs) as published:
                yield published
                if threading.current_thread() is workers[0]:
                    marked.set()
                    self.assertTrue(resume.wait(WAIT))

        def fail_later_mark(path, cache_path):
            if threading.current_thread() is workers[1]:
                raise OSError(errno.EIO, "later DIRTY publication failed")
            return real_mark(path, cache_path)

        workers.extend((Worker(self.fs.write, self.PATH, b"first", 0, fd),
                        Worker(self.fs.write, self.PATH, b"later", 0, fd)))
        try:
            with patch.object(self.fs, "_local_mutation", side_effect=pause_after_first_mark), \
                    patch.object(self.fs, "_mark_cache_file_dirty", side_effect=fail_later_mark):
                workers[0].start()
                self.assertTrue(marked.wait(WAIT))
                workers[1].start()
                workers[1].join(WAIT)
                self.assertFalse(workers[1].is_alive())
                self.assertIsInstance(workers[1].error, FuseOSError)
                self.assertEqual(errno.EIO, workers[1].error.errno)
                resume.set()
                self.assertEqual(5, workers[0].finish())
            self._assert_local_dirty(b"later" + self.OLD[5:])
            with self.fs._member_checks_lock:
                state = self.fs._freshness[self.PATH]
                self.assertTrue(state.uncertain_local)
                self.assertEqual(0, state.active_mutations)
            self.assertIn("changedA", self._snapshot()["debts"])
            self._assert_no_b_baseline()
        finally:
            resume.set()
            for worker in workers:
                if worker.ident is not None:
                    worker.join(WAIT)
            self.fs.release(self.PATH, fd)

        token = self.fs._freshness_token(self.PATH, "staleB")
        self.assertFalse(self.fs._stream_freshness_valid(self._late_b_session(token)))
        recovery_fd = self.fs.open(self.PATH, os.O_RDWR)
        try:
            self.assertEqual(5, self.fs.write(self.PATH, b"valid", 0, recovery_fd))
            self._assert_local_dirty(b"valid" + self.OLD[5:])
        finally:
            self.fs.release(self.PATH, recovery_fd)
        self._sync_evict_and_refetch_local_bytes(b"valid" + self.OLD[5:])

    def test_o_trunc_dirty_failure_closes_only_attempt_descriptor(self):
        keep_fd = self.fs.open(self.PATH, os.O_RDWR)
        with self.fs._fd_lock:
            keep_real_fd = self.fs._fd_map[keep_fd].real_fd
        self._change_a()
        self.assertEqual(self.OLD, self._read(self.PATH))
        self.a.set_available(False)
        fail, captured = self._fail_dirty_after_registration(
            OSError(errno.EIO, "dirty publication failed"), keep_fd)
        try:
            with self._count_failed_attempt_closes(captured), \
                    patch.object(self.fs, "_mark_cache_file_dirty", side_effect=fail):
                with self.assertRaises(FuseOSError) as error:
                    self.fs.open(self.PATH, os.O_RDWR | os.O_TRUNC)
            self.assertEqual(errno.EIO, error.exception.errno)
            self.assertEqual(b"", (self.cache_dir / "f.txt").read_bytes())
            self._assert_failed_attempt_closed(captured, keep_fd, keep_real_fd)
        finally:
            self.a.set_available(True)
            self.fs.release(self.PATH, keep_fd)

    def test_create_dirty_failure_closes_only_attempt_descriptor(self):
        keep_fd = self.fs.open(self.PATH, os.O_RDWR)
        with self.fs._fd_lock:
            keep_real_fd = self.fs._fd_map[keep_fd].real_fd
        self._change_a()
        self.assertEqual(self.OLD, self._read(self.PATH))
        self.a.set_available(False)
        fail, captured = self._fail_dirty_after_registration(
            RuntimeError("dirty publication failed"), keep_fd)
        try:
            with self._count_failed_attempt_closes(captured), \
                    patch.object(self.fs, "_mark_cache_file_dirty", side_effect=fail):
                with self.assertRaisesRegex(RuntimeError, "dirty publication failed"):
                    self.fs.create(self.PATH, 0o600)
            self.assertEqual(b"", (self.cache_dir / "f.txt").read_bytes())
            self._assert_failed_attempt_closed(captured, keep_fd, keep_real_fd)
        finally:
            self.a.set_available(True)
            self.fs.release(self.PATH, keep_fd)

    def test_restart_of_b_fallback_restores_a_debt_before_freshness_shortcut(self):
        self._change_a()
        real_stat = self.a.stat

        def stat_then_drop(path):
            result = real_stat(path)
            self.a.set_available(False)
            return result

        try:
            with patch.object(self.a, "stat", side_effect=stat_then_drop), \
                    patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b):
                self.assertEqual(self.OLD, self._read(self.PATH))
            self.assertEqual(self.OLD, (self.cache_dir / "f.txt").read_bytes())
            self._restart_fs()
            self.assertEqual(self.OLD, self._read(self.PATH))
            self.assertIn("changedA", self._snapshot()["debts"])
        finally:
            self.a.set_available(True)
        with patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b):
            self.assertEqual(self.NEW, self._read(self.PATH))
        self._assert_a_baseline()
        self.assertIsNone(self._snapshot())
        self._assert_slots_released()

    def test_valid_restarted_warm_open_keeps_demand_slot_under_cold_copy(self):
        (self.member_dir / "cold.txt").write_bytes(b"slow cold bytes")
        self._restart_fs()
        gate = self.fs._foreground_admission
        self.assertEqual("demand", self.fs._foreground_class("open", (self.PATH, os.O_RDONLY)))
        copy = self.gate(self.a, "copy_to_local", only=lambda path, _dest: path == "/cold.txt")
        cold = Worker(self._read, "/cold.txt")
        cold.start()
        self.assertTrue(copy.started.wait(WAIT))
        real_a_stat = self.a.stat
        stat_paths = []

        def a_stat(path):
            stat_paths.append(path)
            return real_a_stat(path)

        try:
            with patch.object(self.a, "stat", side_effect=a_stat):
                warm = Worker(self._read, self.PATH)
                warm.start()
                self.assertEqual(self.OLD, warm.finish())
                # Local marker verification did not forge a fresh member
                # timestamp: normal first-open revalidation still stat-ed A.
                self.assertIn(self.PATH, stat_paths)
                self.assertEqual(0, gate.waiting_count("ordinary"))
                self.assertLessEqual(gate.peak_active, 3)
            self.assertTrue(cold.is_alive())
        finally:
            copy.opened.set()
            cold.finish()
        self._assert_slots_released()

    def test_changed_a_while_stopped_transfers_after_demand_revalidation(self):
        (self.member_dir / "cold.txt").write_bytes(b"slow cold bytes")

        def change_while_stopped():
            (self.member_dir / "f.txt").write_bytes(self.NEW)
            os.utime(self.member_dir / "f.txt", (1_700_000_100, 1_700_000_100))

        self._restart_fs(after_stop=change_while_stopped)
        gate = self.fs._foreground_admission
        self.assertEqual("demand", self.fs._foreground_class("open", (self.PATH, os.O_RDONLY)))
        copy = self.gate(self.a, "copy_to_local", only=lambda path, _dest: path == "/cold.txt")
        cold = Worker(self._read, "/cold.txt")
        cold.start()
        self.assertTrue(copy.started.wait(WAIT))
        warm = Worker(self._read, self.PATH)
        warm.start()
        try:
            # The local marker did not certify A. Its changed stat makes the
            # full fetch ordinary; that wait must not retain the path lock.
            self.assertTrue(gate.waiting_for("ordinary", 1, WAIT))
            with self.fs._path_locks._mutex:
                self.assertNotIn(self.PATH, self.fs._path_locks._entries)
        finally:
            copy.opened.set()
            cold.finish()
        self.assertEqual(self.NEW, warm.finish())
        self._assert_a_baseline()
        self._assert_slots_released()

    def test_restart_with_missing_cache_rejects_ambiguous_a_b_copies(self):
        self._change_a()
        gate = self.fs._foreground_admission
        with gate.admit("ordinary"):
            opener = Worker(self._read, self.PATH)
            opener.start()
            self.assertTrue(gate.waiting_for("ordinary", 1, WAIT))
            self.assertFalse((self.cache_dir / "f.txt").exists())
            gate.close()
            opener.join(WAIT)
            self.assertFalse(opener.is_alive())
            self.assertIsInstance(opener.error, FuseOSError)
        self._restart_fs()
        with patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b):
            with self.assertRaises(FuseOSError) as raised:
                self._read(self.PATH)
        self.assertEqual(errno.EIO, raised.exception.errno)
        self.assertFalse((self.cache_dir / "f.txt").exists())
        self.assertEqual("reprobe", self._snapshot()["state"])
        self._assert_slots_released()

    def test_c_completion_cannot_clear_independent_a_debt(self):
        self._change_a()
        real_stat = self.a.stat

        def stat_then_drop(path):
            result = real_stat(path)
            self.a.set_available(False)
            return result

        try:
            with patch.object(self.a, "stat", side_effect=stat_then_drop), \
                    patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b):
                self.assertEqual(self.OLD, self._read(self.PATH))
            self.assertIn("changedA", self._snapshot()["debts"])
            c_dir = self.temp_dir / "changedC"
            c_dir.mkdir()
            c_bytes = b"C has an independent change"
            (c_dir / "f.txt").write_bytes(c_bytes)
            os.utime(c_dir / "f.txt", (1_700_000_200, 1_700_000_200))
            c = LocalBackend("changedC", str(c_dir))
            c.speed_priority = -10
            self.fs.backends[c.name] = c
            self.fs.replication_manager.backends[c.name] = c
            self.fs.state_manager.add_file_location(FileLocation(
                virtual_path=self.PATH, backend_name=c.name, checksum="",
                last_synced=time.time(), is_cached=False,
            ))
            self._age_checks()
            self.assertEqual(c_bytes, self._read(self.PATH))
            after_c = self._snapshot()
            self.assertIn("changedA", after_c["debts"])
            self.assertNotIn("changedC", after_c["debts"])
            self.assertEqual((len(c_bytes), 1_700_000_200.0),
                             self.fs._member_checks[self.PATH].members["changedC"])
        finally:
            self.a.set_available(True)
        self.assertEqual(self.NEW, self._read(self.PATH))
        self._assert_a_baseline()
        self.assertIsNone(self._snapshot())
        self._assert_slots_released()


class TestOpenRaces(ThreadingTestBase):
    def test_two_opens_of_one_uncached_path_fetch_once(self):
        payload = os.urandom(256 * 1024)
        (self.member_dir / "f.bin").write_bytes(payload)
        copy = self.gate(self.member, "copy_to_local")

        first = Worker(self.open_read_release, "/f.bin")
        first.start()
        self.assertTrue(copy.started.wait(WAIT))
        second = Worker(self.open_read_release, "/f.bin")
        second.start()
        # The second open waits for the first's fetch instead of starting
        # its own (or seeing a half-fetched file).
        self.wait_for_path_waiter("/f.bin")
        self.assertTrue(second.is_alive())
        copy.opened.set()

        self.assertEqual(payload, first.finish())
        self.assertEqual(payload, second.finish())
        self.assertEqual(1, len(copy.calls))
        self.assertEqual(0, len(self.fs._path_locks))

    def test_listing_and_other_paths_proceed_while_a_fetch_is_gated(self):
        # The headline property: one slow member transfer holds only its
        # own path.
        (self.member_dir / "slow.bin").write_bytes(b"s" * 4096)
        self.cache("other.txt", b"other")
        (self.member_dir / "uncached.txt").write_bytes(b"uncached")
        copy = self.gate(self.member, "copy_to_local",
                         only=lambda path, local: path == "/slow.bin")

        slow = Worker(self.open_read_release, "/slow.bin")
        slow.start()
        self.assertTrue(copy.started.wait(WAIT))

        def browse():
            names = set(self.fs.readdir("/", None))
            attrs = self.fs.getattr("/other.txt")
            return (names, attrs["st_size"],
                    self.open_read_release("/other.txt"),
                    self.open_read_release("/uncached.txt"))

        browser = Worker(browse)
        browser.start()
        names, size, other, uncached = browser.finish()
        self.assertTrue(slow.is_alive())
        self.assertLessEqual({"slow.bin", "other.txt", "uncached.txt"}, names)
        self.assertEqual(5, size)
        self.assertEqual(b"other", other)
        self.assertEqual(b"uncached", uncached)

        copy.opened.set()
        self.assertEqual(b"s" * 4096, slow.finish())

    def test_pin_survives_a_concurrent_revalidate(self):
        self.cache("f.txt", b"original")
        (self.member_dir / "f.txt").write_bytes(b"changed and longer")
        with self.fs._member_checks_lock:
            for path, check in list(self.fs._member_checks.items()):
                self.fs._member_checks[path] = dataclasses.replace(
                    check, verified_at=check.verified_at - 3600,
                )
        # Pause the first opener after its check pinned the refetch and
        # before its fetch consumed the pin.
        invalidate = self.gate(self.fs, "_invalidate_file_cache")
        pins_at_select = []
        real_select = self.fs._select_backend_for_read

        def select(canonical_path):
            with self.fs._member_checks_lock:
                pins_at_select.append(self.fs._refetch_pins.get(canonical_path))
            return real_select(canonical_path)

        copies = self.gate(self.member, "copy_to_local", only=lambda *a: False)
        with patch.object(self.fs, "_select_backend_for_read", side_effect=select):
            first = Worker(self.open_read_release, "/f.txt")
            first.start()
            self.assertTrue(invalidate.started.wait(WAIT))
            second = Worker(self.open_read_release, "/f.txt")
            second.start()
            self.wait_for_path_waiter("/f.txt")
            self.assertTrue(second.is_alive())
            invalidate.opened.set()
            self.assertEqual(b"changed and longer", first.finish())
            self.assertEqual(b"changed and longer", second.finish())
        # The first opener's fetch found its pin (the second opener's
        # revalidate did not pop it), and the second did not fetch again.
        self.assertEqual([self.member], pins_at_select)
        self.assertEqual(1, len(copies.calls))
        self.assertEqual({}, self.fs._refetch_pins)

    def test_open_of_a_cached_file_is_not_undercut_by_eviction(self):
        self.cache("f.txt", b"cached")
        # Pause the open after it found the file cached, before os.open.
        heal = self.gate(self.fs.cache_manager, "heal_stale_clean")
        opener = Worker(self.open_read_release, "/f.txt")
        opener.start()
        self.assertTrue(heal.started.wait(WAIT))
        # Eviction wants everything, but skips the busy path.
        self.fs.cache_manager.evict_lru_files(10 * 1024 * 1024)
        self.assertTrue((self.cache_dir / "f.txt").exists())
        heal.opened.set()
        self.assertEqual(b"cached", opener.finish())
        # Once the open is done the path is evictable again.
        self.assertTrue(self.fs.cache_manager.evict_lru_files(1))
        self.assertFalse((self.cache_dir / "f.txt").exists())


    def test_unlink_waits_for_an_open_of_the_path(self):
        (self.member_dir / "f.txt").write_bytes(b"fetched")
        copy = self.gate(self.member, "copy_to_local")
        opener = Worker(self.open_read_release, "/f.txt")
        opener.start()
        self.assertTrue(copy.started.wait(WAIT))
        unlinker = Worker(self.fs.unlink, "/f.txt")
        unlinker.start()
        self.wait_for_path_waiter("/f.txt")
        copy.opened.set()
        self.assertEqual(b"fetched", opener.finish())
        unlinker.finish()
        self.assertFalse((self.cache_dir / "f.txt").exists())
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/f.txt"))

    def test_rmdir_waits_for_every_path_locked_handler(self):
        (self.member_dir / "empty").mkdir()
        (self.cache_dir / "empty").mkdir()
        (self.member_dir / "slow.txt").write_bytes(b"slow")
        copy = self.gate(self.member, "copy_to_local")
        opener = Worker(self.open_read_release, "/slow.txt")
        opener.start()
        self.assertTrue(copy.started.wait(WAIT))
        remover = Worker(self.fs.rmdir, "/empty")
        remover.start()
        self.wait_for_namespace_writer()
        self.assertTrue(remover.is_alive())
        copy.opened.set()
        self.assertEqual(b"slow", opener.finish())
        remover.finish()
        self.assertFalse((self.cache_dir / "empty").exists())

    def test_member_copy_upload_holds_the_path(self):
        self.cache("f.txt", b"canonical")
        (self.member_dir / "f.txt").write_bytes(b"member copy")
        fd = self.fs.open("/f.txt.member", os.O_RDWR)
        self.fs.write("/f.txt.member", b"edited", 0, fd)
        upload = self.gate(self.member, "copy_from_local")
        releaser = Worker(self.fs.release, "/f.txt.member", fd)
        releaser.start()
        self.assertTrue(upload.started.wait(WAIT))
        opener = Worker(self.fs.open, "/f.txt", os.O_RDONLY)
        opener.start()
        self.wait_for_path_waiter("/f.txt")
        self.assertTrue(opener.is_alive())
        upload.opened.set()
        releaser.finish()
        self.fs.release("/f.txt", opener.finish())
        self.assertEqual(b"edited copy", (self.member_dir / "f.txt").read_bytes())


class TestRenameRaces(ThreadingTestBase):
    def _rename_gated_at_member(self, old, new):
        member_rename = self.gate(self.member, "rename")
        renamer = Worker(self.fs.rename, old, new)
        renamer.start()
        self.assertTrue(member_rename.started.wait(WAIT))
        return member_rename, renamer

    def test_open_of_the_old_name_does_not_rebuild_a_stale_copy(self):
        self.cache("old.txt", b"content")
        gate, renamer = self._rename_gated_at_member("/old.txt", "/new.txt")
        # The cache has moved; the member still has the old name.
        self.assertTrue((self.member_dir / "old.txt").exists())
        opener = Worker(self.fs.open, "/old.txt", os.O_RDONLY)
        opener.start()
        self.wait_for_path_waiter("/old.txt")
        self.assertTrue(opener.is_alive())
        gate.opened.set()
        renamer.finish()
        with self.assertRaises(FuseOSError) as ctx:
            opener.finish()
        self.assertEqual(errno.ENOENT, ctx.exception.errno)
        self.assertFalse((self.cache_dir / "old.txt").exists())
        self.assertEqual(b"content", self.open_read_release("/new.txt"))

    def test_write_during_a_rename_is_recorded_at_the_new_path(self):
        self.cache("w.txt", b"before")
        fd = self.fs.open("/w.txt", os.O_RDWR)
        try:
            generation = self.fs.state_manager.get_file_metadata("/w.txt").dirty_generation
            gate, renamer = self._rename_gated_at_member("/w.txt", "/moved.txt")
            # write takes no path lock: it runs inside the rename.
            self.assertEqual(5, self.fs.write("/w.txt", b"after", 0, fd))
            gate.opened.set()
            renamer.finish()
        finally:
            self.fs.release("/moved.txt", fd)
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/w.txt"))
        moved = self.fs.state_manager.get_file_metadata("/moved.txt")
        self.assertEqual(SyncStatus.DIRTY, moved.sync_status)
        self.assertGreater(moved.dirty_generation, generation)
        self.assertEqual(b"after", (self.cache_dir / "moved.txt").read_bytes()[:5])

    def test_write_waits_for_the_local_move(self):
        self.cache("w.txt", b"before")
        fd = self.fs.open("/w.txt", os.O_RDWR)
        try:
            # Pause the rename inside its index-locked local move, after the
            # cache rename and before the descriptors are re-pointed.
            move = self.gate(self.fs.state_manager, "rename_path")
            renamer = Worker(self.fs.rename, "/w.txt", "/moved.txt")
            renamer.start()
            self.assertTrue(move.started.wait(WAIT))
            writer = Worker(self.fs.write, "/w.txt", b"after", 0, fd)
            writer.start()
            # The bytes land (the cache file has already moved), so the
            # writer is past pwrite and waiting for the index lock to
            # record them.
            deadline = time.monotonic() + WAIT
            while (self.cache_dir / "moved.txt").read_bytes()[:5] != b"after":
                self.assertLess(time.monotonic(), deadline, "the write never landed")
                time.sleep(0.01)
            writer.join(BLOCKED)
            self.assertTrue(writer.is_alive())
            move.opened.set()
            renamer.finish()
            self.assertEqual(5, writer.finish())
        finally:
            self.fs.release("/moved.txt", fd)
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/w.txt"))
        self.assertEqual(
            SyncStatus.DIRTY, self.fs.state_manager.get_file_metadata("/moved.txt").sync_status,
        )

    def test_rename_failing_after_the_local_move_repoints_descriptors(self):
        self.cache("w.txt", b"before")
        fd = self.fs.open("/w.txt", os.O_RDWR)
        try:
            with patch.object(self.fs.namespace, "_propagate_dir_operation",
                              side_effect=OSError(errno.EIO, "member rename failed")):
                with self.assertRaises(OSError):
                    self.fs.rename("/w.txt", "/moved.txt")
            # The cache copy moved, so the descriptor follows it.
            self.fs.write("/w.txt", b"after", 0, fd)
        finally:
            self.fs.release("/moved.txt", fd)
        self.assertEqual(
            SyncStatus.DIRTY, self.fs.state_manager.get_file_metadata("/moved.txt").sync_status,
        )

    def test_uncached_file_rename_does_not_wait_for_other_paths(self):
        (self.member_dir / "slow.txt").write_bytes(b"slow")
        (self.member_dir / "plain.txt").write_bytes(b"plain")
        copy = self.gate(self.member, "copy_to_local")
        opener = Worker(self.open_read_release, "/slow.txt")
        opener.start()
        self.assertTrue(copy.started.wait(WAIT))
        # Not cached and not a directory: path locks, not the namespace, so
        # it finishes while the other path's transfer is gated. (It fails:
        # renaming a file the cache does not hold is ENOENT today, ditz
        # rename-uncached-file-enoent.)
        with self.assertRaises(FuseOSError) as ctx:
            Worker(self.fs.rename, "/plain.txt", "/renamed.txt").finish_started()
        self.assertEqual(errno.ENOENT, ctx.exception.errno)
        self.assertTrue(opener.is_alive())
        copy.opened.set()
        self.assertEqual(b"slow", opener.finish())

    def test_opposite_renames_wait_on_the_sorted_first_path(self):
        # x -> y and y -> x name the same two locks in opposite argument
        # order; the second waits on /x.txt, the path that sorts first, so
        # neither can hold one lock while waiting for the other. (Onto an
        # existing file a rename fails in the index today, ditz
        # rename-onto-existing-unique, so y starts absent.)
        self.cache("x.txt", b"x")
        gate = self.gate(self.fs.state_manager, "rename_path")
        first = Worker(self.fs.rename, "/x.txt", "/y.txt")
        first.start()
        self.assertTrue(gate.started.wait(WAIT))
        second = Worker(self.fs.rename, "/y.txt", "/x.txt")
        second.start()
        self.wait_for_path_waiter("/x.txt")
        gate.opened.set()
        first.finish()
        second.finish()
        self.assertEqual(b"x", self.open_read_release("/x.txt"))
        self.assertFalse((self.cache_dir / "y.txt").exists())

    def test_directory_rename_waits_for_an_open_beneath_it(self):
        (self.member_dir / "d").mkdir()
        (self.member_dir / "d" / "f.txt").write_bytes(b"inside")
        copy = self.gate(self.member, "copy_to_local")
        opener = Worker(self.open_read_release, "/d/f.txt")
        opener.start()
        self.assertTrue(copy.started.wait(WAIT))
        renamer = Worker(self.fs.rename, "/d", "/e")
        renamer.start()
        self.wait_for_namespace_writer()
        self.assertTrue(renamer.is_alive())
        copy.opened.set()
        self.assertEqual(b"inside", opener.finish())
        renamer.finish()
        self.assertEqual(b"inside", self.open_read_release("/e/f.txt"))


class TestWatcherInvalidation(ThreadingTestBase):
    def test_remote_change_waits_for_an_open_of_the_path(self):
        self.cache("f.txt", b"cached")
        heal = self.gate(self.fs.cache_manager, "heal_stale_clean")
        opener = Worker(self.open_read_release, "/f.txt")
        opener.start()
        self.assertTrue(heal.started.wait(WAIT))
        watcher = Worker(self.fs._on_remote_change, "/f.txt")
        watcher.start()
        self.wait_for_path_waiter("/f.txt")
        self.assertTrue(watcher.is_alive())
        heal.opened.set()
        self.assertEqual(b"cached", opener.finish())
        watcher.finish()
        self.assertFalse((self.cache_dir / "f.txt").exists())


if __name__ == "__main__":
    unittest.main()
