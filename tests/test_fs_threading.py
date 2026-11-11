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
    """Red contract tests: two members expose opener-private freshness loss.

    These tests are source only until root authorizes runtime checks. The
    proposed _freshness_snapshot and _freshness_token are diagnostic seams
    specified in contracts/path-freshness-obligation.md, not present yet.
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

    def _restart_fs(self):
        """Close the mount instance, retaining only scratch cache/member state."""
        self.fs.destroy("/")
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

    def _late_callback_at_commit_entry(self, session):
        """Reach baseline publication while the caller still holds its gate.

        _record_fetched is the current stream-finalization publication entry.
        A repair may reject the stale token earlier; the proposed boundary
        keeps this observation seam so the rejected attempt is distinguishable
        from a callback that never ran.
        """
        entered, resume = threading.Event(), threading.Event()
        self.addCleanup(resume.set)
        real = self.fs._record_fetched

        def commit(*args, **kwargs):
            entered.set()
            self.assertTrue(resume.wait(WAIT))
            return real(*args, **kwargs)

        with patch.object(self.fs, "_record_fetched", side_effect=commit):
            callback = Worker(self.fs._streaming_manager.on_finalized, session)
            callback.start()
            self.assertTrue(entered.wait(WAIT), "late callback missed commit entry")
            resume.set()
            callback.finish()

    def _assert_no_b_baseline(self):
        check = self.fs._member_checks.get(self.PATH)
        if check is not None:
            self.assertNotIn("staleB", check.members)

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
        dropped, resume, hold = self._hold_after_invalidation()
        with hold, patch.object(self.fs, "_select_unpinned_backend_for_read", return_value=self.b), \
                patch.object(self.a, "copy_to_local", wraps=self.a.copy_to_local) as a_copy, \
                patch.object(self.b, "copy_to_local", wraps=self.b.copy_to_local) as b_copy:
            first = Worker(self._read, self.PATH)
            first.start()
            self.assertTrue(dropped.wait(WAIT))
            second = Worker(self._read, self.PATH)
            second.start()
            self.wait_for_path_waiter(self.PATH)
            try:
                resume.set()
                self.assertEqual(self.NEW, first.finish())
                self.assertEqual(self.NEW, second.finish())
            finally:
                resume.set()
            self.assertGreaterEqual(a_copy.call_count, 1)
            b_copy.assert_not_called()
        self._assert_a_baseline()
        self.assertIsNone(self._snapshot())
        self._assert_slots_released()

    def test_old_open_exit_cannot_erase_newer_observation(self):
        # First opener pauses after releasing the path lock but before its
        # cleanup. The second opener then publishes A's newer observation.
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

    def test_write_byte_to_dirty_interval_rejects_late_stream_commit(self):
        token = self.fs._freshness_token(self.PATH, "staleB")
        fd = self.fs.open(self.PATH, os.O_RDWR)
        self._change_a()
        self.assertEqual(self.OLD, self._read(self.PATH))
        self.assertIn("changedA", self._snapshot()["debts"])
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
                self._late_callback_at_commit_entry(late)
                self.assertIn("changedA", self._snapshot()["debts"])
                self._assert_no_b_baseline()
                resume.set()
                self.assertEqual(5, writer.finish())
            self._assert_local_dirty(b"local" + self.OLD[5:])
            self.assertIn("changedA", self._snapshot()["debts"])
        finally:
            resume.set()
            self.fs.release(self.PATH, fd)
        self._assert_slots_released()

    def test_truncate_byte_to_dirty_interval_rejects_late_baseline(self):
        token = self.fs._freshness_token(self.PATH, "staleB")
        writer_fd = self.fs.open(self.PATH, os.O_RDWR)
        try:
            self._change_a()
            self.assertEqual(self.OLD, self._read(self.PATH))
            self.assertIn("changedA", self._snapshot()["debts"])
        finally:
            self.fs.release(self.PATH, writer_fd)
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
        real_a_stat, real_b_stat = self.a.stat, self.b.stat

        def a_stat(path):
            if path == self.PATH:
                raise AssertionError("valid restarted cache did member I/O")
            return real_a_stat(path)

        def b_stat(path):
            if path == self.PATH:
                raise AssertionError("valid restarted cache did member I/O")
            return real_b_stat(path)

        try:
            with patch.object(self.a, "stat", side_effect=a_stat), \
                    patch.object(self.b, "stat", side_effect=b_stat):
                warm = Worker(self._read, self.PATH)
                warm.start()
                self.assertEqual(self.OLD, warm.finish())
                self.assertEqual(0, gate.waiting_count("ordinary"))
                self.assertLessEqual(gate.peak_active, 3)
            self.assertTrue(cold.is_alive())
        finally:
            copy.opened.set()
            cold.finish()
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
