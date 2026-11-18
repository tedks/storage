"""Unit tests for storage/member_repairs.py (docs/member-rename-repairs.md)."""

import errno
import fcntl
import json
import logging
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import storage.member_repairs as member_repairs_module
from storage.member_repairs import (
    VIEW_WRITE,
    CARRIER_DIR,
    MAX_RECORD_BYTES,
    RECORD_XATTR,
    CorruptRepairRecord,
    MemberRepairs,
    is_at_or_under,
    is_carrier_path,
    paths_overlap,
)


class MemberRepairsTestBase(unittest.TestCase):
    def setUp(self):
        self.cache = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.cache, ignore_errors=True)
        self.carriers = self.cache / CARRIER_DIR

    def write_raw(self, name, payload):
        self.carriers.mkdir(exist_ok=True)
        path = self.carriers / name
        path.touch()
        os.setxattr(str(path), RECORD_XATTR, payload)
        return path


class TestPaths(unittest.TestCase):
    def test_overlap_is_segment_bounded(self):
        self.assertTrue(paths_overlap("/d", "/d"))
        self.assertTrue(paths_overlap("/d", "/d/x"))
        self.assertTrue(paths_overlap("/d/x", "/d"))
        self.assertFalse(paths_overlap("/d", "/dx"))
        self.assertFalse(paths_overlap("/a/b", "/a/c"))
        self.assertTrue(is_at_or_under("/anything", "/"))

    def test_carrier_paths(self):
        self.assertTrue(is_carrier_path("/.storage.repairs"))
        self.assertTrue(is_carrier_path("/.storage.repairs/abc"))
        self.assertFalse(is_carrier_path("/.storage.repairsx"))
        self.assertFalse(is_carrier_path("/d/.storage.repairs"))


class TestRecords(MemberRepairsTestBase):
    def test_a_record_survives_a_new_instance(self):
        repairs = MemberRepairs(self.cache)
        rid = repairs.record("/a", "/b", ["m1", "m0"], "missed")
        [record] = MemberRepairs(self.cache).list()
        self.assertEqual((rid, "/a", "/b", ("m0", "m1"), "open"),
                         (record.id, record.old_path, record.new_path, record.members, record.state))

    def test_a_record_over_the_bound_is_refused_and_leaves_nothing(self):
        repairs = MemberRepairs(self.cache)
        with self.assertRaises(OSError) as raised:
            repairs.record("/a", "/" + "x" * MAX_RECORD_BYTES, ["m"], "missed")
        self.assertEqual(errno.ENAMETOOLONG, raised.exception.errno)
        self.assertEqual([], repairs.list())

    def test_a_failed_directory_fsync_leaves_nothing_behind(self):
        repairs = MemberRepairs(self.cache)
        real = os.fsync
        calls = []

        def failing_on_directory(fd):
            calls.append(fd)
            if os.path.isdir(f"/proc/self/fd/{fd}"):
                raise OSError(errno.EIO, "injected")
            return real(fd)

        repairs._ensure_dir()
        with patch("storage.member_repairs.os.fsync", side_effect=failing_on_directory):
            with self.assertRaises(OSError):
                repairs.record("/a", "/b", ["m"], "missed")
        self.assertEqual([], os.listdir(self.carriers))
        self.assertEqual([], MemberRepairs(self.cache).list())

    def test_invalid_records_refuse_the_open(self):
        good = {"v": 1, "id": "a" * 32, "old": "/a", "new": "/b", "members": ["m"],
                "state": "open", "reason": "r", "recorded": 1.0}
        bad = [
            b"{not json",
            json.dumps({**good, "v": 2}).encode(),
            json.dumps({**good, "id": "b" * 32}).encode(),
            json.dumps({**good, "state": "done"}).encode(),
            json.dumps({**good, "members": []}).encode(),
            json.dumps({**good, "members": "m"}).encode(),
            json.dumps({**good, "kind": "other"}).encode(),
            json.dumps({**good, "kind": "view-write"}).encode(),   # old != new
            json.dumps({**good, "kind": "view-write", "new": "/a",
                        "members": ["m", "n"]}).encode(),
            json.dumps({**good, "source": 3}).encode(),
            json.dumps({**good, "members": {"m": 1}}).encode(),
            json.dumps({**good, "old": "relative"}).encode(),
            json.dumps({k: v for k, v in good.items() if k != "new"}).encode(),
        ]
        for payload in bad:
            path = self.write_raw("a" * 32, payload)
            with self.assertRaises(CorruptRepairRecord, msg=payload):
                MemberRepairs(self.cache)
            path.unlink()
        self.write_raw("not-an-id", json.dumps(good).encode())
        with self.assertRaises(CorruptRepairRecord):
            MemberRepairs(self.cache)

    def test_only_the_owner_sweeps_unfinished_carriers(self):
        self.carriers.mkdir()
        unfinished = self.carriers / ("c" * 32 + ".new")
        unfinished.touch()
        MemberRepairs(self.cache)
        self.assertTrue(unfinished.exists())
        MemberRepairs(self.cache, sweep_new=True)
        self.assertFalse(unfinished.exists())

    def test_an_external_change_reaches_an_open_instance(self):
        mine = MemberRepairs(self.cache)
        rid = mine.record("/a", "/b", ["m"], "missed")
        other = MemberRepairs(self.cache)
        other_rid = other.record("/c", "/d", ["m"], "missed")
        self.assertEqual({rid, other_rid}, {r.id for r in mine.list()})
        self.assertTrue(other.resolve(rid))
        self.assertEqual({other_rid}, {r.id for r in mine.list()})

    def test_resolve_is_replay_safe_and_rejects_foreign_ids(self):
        repairs = MemberRepairs(self.cache)
        rid = repairs.record("/a", "/b", ["m"], "missed")
        for bad in ("", "1", "../x", "A" * 32, "0" * 32):
            self.assertFalse(repairs.resolve(bad), bad)
        self.assertTrue(repairs.resolve(rid))
        self.assertFalse(repairs.resolve(rid))

    def test_settle_without_misses_drops_the_intent(self):
        repairs = MemberRepairs(self.cache)
        rid = repairs.begin("/a", "/b", ["m0", "m1"])
        self.assertEqual({"m0", "m1"}, repairs.backends_for(["/b"]))
        repairs.settle(rid, "/a", "/b", {})
        self.assertEqual([], MemberRepairs(self.cache).list())

    def test_settle_keeps_exactly_the_members_that_missed(self):
        repairs = MemberRepairs(self.cache)
        rid = repairs.begin("/a", "/b", ["m0", "m1"])
        repairs.settle(rid, "/a", "/b", {"m1": "unreachable"})
        [record] = MemberRepairs(self.cache).list()
        # A new id: a late resolve of the intent id cannot drop the miss.
        self.assertNotEqual(rid, record.id)
        self.assertEqual((("m1",), "open", "/a", "/b"),
                         (record.members, record.state, record.old_path, record.new_path))
        self.assertIn("unreachable", record.reason)
        self.assertFalse(MemberRepairs(self.cache).resolve(rid))
        self.assertEqual([record.id], [r.id for r in MemberRepairs(self.cache).list()])

    def test_settle_uses_the_callers_paths_after_a_reload_dropped_the_intent(self):
        """Another process resolves the intent and a lookup reloads before
        settle runs: settle still records the miss from the paths given."""
        repairs = MemberRepairs(self.cache)
        rid = repairs.begin("/a", "/b", ["m"])
        # Only possible once the holder's lock is gone (as after its crash);
        # this keeps settle's fallback covered.
        os.close(repairs._held.pop(rid))
        self.assertTrue(MemberRepairs(self.cache).resolve(rid))
        self.assertEqual(set(), repairs.backends_for(["/b"]))  # the reload
        repairs.settle(rid, "/a", "/b", {"m": "unreachable"})
        [record] = MemberRepairs(self.cache).list()
        self.assertEqual((("m",), "open", "/a", "/b"),
                         (record.members, record.state, record.old_path, record.new_path))

    def test_a_failed_reload_in_begin_leaves_no_intent_or_lock(self):
        repairs = MemberRepairs(self.cache)
        with patch.object(repairs, "_reload_locked", side_effect=OSError(errno.EIO, "injected")):
            with self.assertRaises(OSError):
                repairs.begin("/a", "/b", ["m"])
        self.assertEqual({}, repairs._held)
        self.assertEqual([], os.listdir(self.carriers))

    def test_an_in_flight_intent_cannot_be_resolved(self):
        repairs = MemberRepairs(self.cache)
        rid = repairs.begin("/a", "/b", ["m"])
        with patch("storage.member_repairs.FLOCK_WAIT", 0.05):
            with self.assertRaises(OSError):
                MemberRepairs(self.cache).resolve(rid)
        repairs.settle(rid, "/a", "/b", {})
        self.assertEqual([], MemberRepairs(self.cache).list())

    def test_settle_leaves_a_busy_intent_in_place(self):
        """An intent begun by another instance whose flock is held elsewhere
        (a resolve stopped mid-way): settle gives up after its bound and
        leaves the intent, over-fencing, with the miss recorded beside it."""
        repairs = MemberRepairs(self.cache)
        rid = repairs.begin("/a", "/b", ["m0", "m1"])
        os.close(repairs._held.pop(rid))
        fd = os.open(str(self.carriers / rid), os.O_RDONLY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            with patch("storage.member_repairs.FLOCK_WAIT", 0.05):
                repairs.settle(rid, "/a", "/b", {"m1": "unreachable"})
        finally:
            os.close(fd)
        states = sorted((r.state, r.members) for r in MemberRepairs(self.cache).list())
        self.assertEqual([("intent", ("m0", "m1")), ("open", ("m1",))], states)

    def test_the_owner_finishes_a_resolution_only_when_it_is_free(self):
        repairs = MemberRepairs(self.cache)
        rid = repairs.record("/a", "/b", ["m"], "missed")
        carrier = self.carriers / rid
        record = json.loads(os.getxattr(str(carrier), RECORD_XATTR))
        os.setxattr(str(carrier), RECORD_XATTR, json.dumps({**record, "state": "resolved"}).encode())
        # Not the owner: still enforced and listed.
        self.assertEqual({"m"}, MemberRepairs(self.cache).backends_for(["/b"]))
        fd = os.open(str(carrier), os.O_RDONLY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            with patch("storage.member_repairs.FLOCK_WAIT", 0.05):
                busy = MemberRepairs(self.cache, sweep_new=True)
            self.assertEqual({"m"}, busy.backends_for(["/b"]))
        finally:
            os.close(fd)
        MemberRepairs(self.cache, sweep_new=True)
        self.assertFalse(carrier.exists())

    def test_a_view_write_keeps_exact_bytes_once_per_identity(self):
        view = self.cache / "view"
        view.write_bytes(b"edited")
        repairs = MemberRepairs(self.cache)
        first = repairs.preserve_view_write("/f", "m", view, "r")
        self.assertEqual(first, repairs.preserve_view_write("/f", "m", view, "r"))
        self.assertEqual(b"edited", repairs.carrier_path(first).read_bytes())
        os.utime(view, ns=(1, 1))
        view.write_bytes(b"edited again")
        second = repairs.preserve_view_write("/f", "m", view, "r")
        self.assertNotEqual(first, second)
        reopened = MemberRepairs(self.cache)
        kept = sorted((r.kind, r.new_path, r.old_path, r.members,
                       reopened.carrier_path(r.id).read_bytes()) for r in reopened.list())
        self.assertEqual([(VIEW_WRITE, "/f", "/f", ("m",), b"edited"),
                          (VIEW_WRITE, "/f", "/f", ("m",), b"edited again")], kept)
        self.assertEqual({"m"}, reopened.backends_for(["/f"]))
        self.assertTrue(reopened.resolve(first))
        self.assertFalse(reopened.carrier_path(first).exists())
        self.assertEqual([second], [r.id for r in MemberRepairs(self.cache).list()])

    def test_a_view_write_copied_while_it_changes_is_copied_again(self):
        view = self.cache / "view"
        view.write_bytes(b"v1")
        repairs = MemberRepairs(self.cache)
        real = member_repairs_module._copy_into
        calls = []

        def copy_then_change(source, fd):
            real(source, fd)
            calls.append(1)
            if len(calls) == 1:
                view.write_bytes(b"v2, longer")
        with patch("storage.member_repairs._copy_into", side_effect=copy_then_change):
            kept = repairs.preserve_view_write("/f", "m", view, "r")
        self.assertEqual(2, len(calls))
        self.assertEqual(b"v2, longer", repairs.carrier_path(kept).read_bytes())
        self.assertEqual([kept], os.listdir(self.carriers))

    def test_a_missing_view_keeps_nothing(self):
        repairs = MemberRepairs(self.cache)
        self.assertIsNone(repairs.preserve_view_write("/f", "m", self.cache / "gone", "r"))
        self.assertEqual([], repairs.list())

    def test_record_with_no_members_records_nothing(self):
        """A record for no member would refuse the next open."""
        repairs = MemberRepairs(self.cache)
        self.assertIsNone(repairs.record("/a", "/b", [], "r"))
        self.assertFalse(self.carriers.exists())
        MemberRepairs(self.cache)

    def test_begin_with_no_members_records_nothing(self):
        repairs = MemberRepairs(self.cache)
        self.assertIsNone(repairs.begin("/a", "/b", []))
        self.assertFalse(self.carriers.exists())

    def test_many_open_repairs_warn_on_open(self):
        repairs = MemberRepairs(self.cache)
        with patch("storage.member_repairs.GROWTH_WARNING", 2):
            for i in range(3):
                repairs.record(f"/a{i}", f"/b{i}", ["m"], "missed")
            with self.assertLogs("storage.member_repairs", logging.WARNING) as logs:
                MemberRepairs(self.cache)
        self.assertTrue(any("3 member repairs are open" in line for line in logs.output))


if __name__ == "__main__":
    unittest.main()
