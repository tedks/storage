"""Unit tests for storage/member_repairs.py (docs/member-rename-repairs.md)."""

import errno
import json
import logging
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from storage.member_repairs import (
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
        repairs.settle(rid, {})
        self.assertEqual([], MemberRepairs(self.cache).list())

    def test_settle_keeps_exactly_the_members_that_missed(self):
        repairs = MemberRepairs(self.cache)
        rid = repairs.begin("/a", "/b", ["m0", "m1"])
        repairs.settle(rid, {"m1": "unreachable"})
        [record] = MemberRepairs(self.cache).list()
        self.assertEqual((rid, ("m1",), "open"), (record.id, record.members, record.state))
        self.assertIn("unreachable", record.reason)

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
