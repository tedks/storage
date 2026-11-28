"""Sync uploads racing renames and unlinks through the mount (ditz sync-namespace-orphans).

A real sync worker uploads while a FUSE handler runs in another thread; an
Event gate in the member's copy_from_local holds the upload mid-transfer,
so each interleaving is forced. Every wait has a timeout.
"""

import errno
import json
import multiprocessing
import os
import queue
import shutil
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from fuse import FuseOSError

from storage.backends.local import LocalBackend
from storage.fs import StorageFS
import storage.namespace as storage_namespace
from storage.models import SyncStatus
from storage.index_audit import rebuild_index_from_cache
from storage.member_repairs import MAX_RECORD_BYTES, VIEW_WRITE, CorruptRepairRecord, MemberRepairs
from storage.state import StateManager
from storage.sync_executor import RetryScheduler, SyncExecutor
from storage.sync_tasks import DirOperation, DirSyncTask, SyncConfig, SyncTask
from tests.test_fs_threading import BLOCKED, WAIT, ThreadingTestBase, Worker


def _rename_then_die(temp_dir):
    """In a forked child: a fresh mount whose member misses /d -> /e, then
    an exit that skips every close and flush."""
    root = temp_dir / "crash"
    (root / "cache").mkdir(parents=True)
    (root / "member").mkdir()
    member = LocalBackend("member", str(root / "member"))
    fs = StorageFS(cache_root=root / "cache", state_db=root / "state.db",
                   backends={"member": member}, cache_size=10 * 1024 * 1024,
                   num_sync_workers=1, start_sync=False)
    fs.mkdir("/d", 0o755)

    def unreachable(*_args):
        raise OSError("member unreachable")

    member.rename = member.rename_noreplace = unreachable
    fs.rename("/d", "/e")
    os._exit(0)


class SyncRaceTestBase(ThreadingTestBase):
    def write_file(self, path, data=b"payload"):
        fh = self.fs.create(path, 0o644)
        self.fs.write(path, data, 0, fh)
        self.fs.release(path, fh)

    def start_sync(self):
        # The monitor sleeps this long between scans, and stop() joins it.
        self.fs.sync_manager.config.monitor_interval = 0.1
        self.fs.sync_manager.start()
        self.addCleanup(self.fs.sync_manager.stop)

    def wait_synced(self, path):
        deadline = time.monotonic() + WAIT
        while time.monotonic() < deadline:
            metadata = self.fs.state_manager.get_file_metadata(path)
            if metadata is not None and metadata.sync_status == SyncStatus.CLEAN:
                return
            time.sleep(0.02)
        self.fail(f"{path} never synced")

    def wait_idle(self):
        self.assertTrue(self.fs.sync_manager.wait_for_completion(timeout=WAIT))

    def gate_upload(self, path):
        return self.gate(self.member, "copy_from_local",
                         only=lambda _local, virtual: virtual == path)

    def member_names(self, rel=""):
        return sorted(os.listdir(self.member_dir / rel))

    def fail_member_once(self, name):
        """Fail the first call to the member's name with OSError; for
        "rename", either entry point (rename or rename_noreplace)."""
        names = [name, "rename_noreplace"] if name == "rename" else [name]
        calls = []

        def failing_once(real):
            def call(*args):
                calls.append(args)
                if len(calls) == 1:
                    raise OSError("member unreachable")
                return real(*args)
            return call

        for each in names:
            patcher = patch.object(self.member, each, side_effect=failing_once(getattr(self.member, each)))
            patcher.start()
            self.addCleanup(patcher.stop)
        return calls

    def repairs(self, cache_dir=None):
        """(member, old, new) per open repair, read from the carriers on
        disk as a fresh open of the cache would."""
        return sorted(
            (member, r.old_path, r.new_path)
            for r in MemberRepairs(cache_dir or self.cache_dir).list()
            for member in r.members
        )


class TestUploadRacingRename(SyncRaceTestBase):
    def test_rename_during_upload_leaves_no_old_name_on_the_member(self):
        gate = self.gate_upload("/a.txt")
        self.start_sync()
        self.write_file("/a.txt")
        self.assertTrue(gate.started.wait(WAIT))

        rename = Worker(self.fs.rename, "/a.txt", "/b.txt")
        rename.start()
        rename.join(BLOCKED)
        self.assertTrue(rename.is_alive(), "rename did not wait for the upload")

        gate.opened.set()
        rename.finish()
        self.wait_synced("/b.txt")
        self.wait_idle()
        self.assertEqual(["b.txt"], self.member_names())
        self.assertEqual(b"payload", (self.member_dir / "b.txt").read_bytes())
        self.assertEqual({"member"}, self.fs.state_manager.get_location_backends("/b.txt"))
        self.assertEqual([], self.fs.state_manager.get_file_locations("/a.txt"))
        self.assertEqual(["b.txt"], sorted(self.fs.readdir("/", None))[2:])

    def test_directory_rename_during_upload_leaves_no_old_directory(self):
        self.fs.mkdir("/d", 0o755)
        gate = self.gate_upload("/d/f.txt")
        self.start_sync()
        self.write_file("/d/f.txt")
        self.assertTrue(gate.started.wait(WAIT))

        rename = Worker(self.fs.rename, "/d", "/e")
        rename.start()
        rename.join(BLOCKED)
        self.assertTrue(rename.is_alive(), "directory rename did not wait for the upload")
        # It waits outside the namespace lock: other paths stay usable.
        other = Worker(self.fs.create, "/other.txt", 0o644)
        fh = other.finish_started()
        self.fs.release("/other.txt", fh)

        gate.opened.set()
        rename.finish()
        self.wait_idle()
        self.assertEqual(["e", "other.txt"], self.member_names())
        self.assertEqual(["f.txt"], self.member_names("e"))
        self.assertEqual({"member"}, self.fs.state_manager.get_location_backends("/e/f.txt"))
        self.assertEqual(SyncStatus.CLEAN, self.fs.state_manager.get_file_metadata("/e/f.txt").sync_status)

    def test_directory_rename_with_more_uploads_under_it(self):
        """Other uploads under the directory end under the new name, whether
        they ran before the drain, waited in it, or found their rows moved."""
        self.fs.mkdir("/d", 0o755)
        self.write_file("/d/f.txt")
        self.write_file("/d/g.txt")
        gate = self.gate_upload("/d/f.txt")
        self.fs.sync_manager.num_workers = 2
        self.start_sync()
        self.assertTrue(gate.started.wait(WAIT))
        self.wait_synced("/d/g.txt")

        self.write_file("/d/h.txt")  # queued; the free worker picks it up
        rename = Worker(self.fs.rename, "/d", "/e")
        rename.start()
        rename.join(BLOCKED)
        self.assertTrue(rename.is_alive())

        gate.opened.set()
        rename.finish()
        self.wait_idle()
        self.wait_synced("/e/h.txt")
        self.wait_idle()
        self.assertEqual(["e"], self.member_names())
        self.assertEqual(["f.txt", "g.txt", "h.txt"], self.member_names("e"))

    def test_open_does_not_wait_for_an_upload(self):
        gate = self.gate_upload("/a.txt")
        self.start_sync()
        self.write_file("/a.txt")
        self.assertTrue(gate.started.wait(WAIT))

        fh = Worker(self.fs.open, "/a.txt", os.O_RDONLY).finish_started()
        self.assertEqual(b"payload", self.read_all("/a.txt", fh))
        self.fs.release("/a.txt", fh)
        gate.opened.set()
        self.wait_synced("/a.txt")


class TestUploadRacingUnlink(SyncRaceTestBase):
    def test_unlink_during_upload_leaves_no_location_row(self):
        gate = self.gate_upload("/a.txt")
        self.start_sync()
        self.write_file("/a.txt")
        self.assertTrue(gate.started.wait(WAIT))

        unlink = Worker(self.fs.unlink, "/a.txt")
        unlink.start()
        unlink.join(BLOCKED)
        self.assertTrue(unlink.is_alive(), "unlink did not wait for the upload")

        gate.opened.set()
        unlink.finish()
        self.wait_idle()
        self.assertIsNone(self.fs.state_manager.get_file_metadata("/a.txt"))
        self.assertEqual([], self.fs.state_manager.get_file_locations("/a.txt"))
        # An unlink does not delete member copies yet (oq-unlink-member-copies).
        self.assertEqual(["a.txt"], self.member_names())


class TestFailedAttemptRacingRename(SyncRaceTestBase):
    def test_failed_attempt_is_settled_before_a_rename_can_move_its_row(self):
        """An attempt that raises after claiming the row (SYNCING) gives it
        back as DIRTY before releasing the guard: a rename between the
        failure and the retry moves a DIRTY row, which it queues again."""
        real_checksum = self.fs.state_manager.compute_checksum
        calls = []

        def failing_once(path):
            calls.append(path)
            if len(calls) == 1:
                raise OSError("checksum read failed")
            return real_checksum(path)

        patcher = patch.object(self.fs.state_manager, "compute_checksum", side_effect=failing_once)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.fs.sync_manager.config.retry_delay = 0
        self.start_sync()
        executor = self.fs.sync_manager.workers[0].executor
        gate = self.gate(executor, "handle_task_failure")
        self.write_file("/a.txt")
        self.assertTrue(gate.started.wait(WAIT))

        self.assertEqual(SyncStatus.DIRTY, self.fs.state_manager.get_file_metadata("/a.txt").sync_status)
        self.fs.rename("/a.txt", "/b.txt")
        gate.opened.set()
        self.wait_synced("/b.txt")
        self.wait_idle()
        self.assertEqual(["b.txt"], self.member_names())


class TestDirectoryRetries(SyncRaceTestBase):
    def queued_dir_ops(self):
        return sorted(
            (task.operation, task.virtual_path)
            for _priority, task in [(t.priority, t) for t in list(self.fs.sync_manager.task_queue.queue)]
            if hasattr(task, "operation")
        )

    def test_mkdir_retry_after_the_directory_was_renamed_is_dropped(self):
        self.fail_member_once("mkdir")
        self.fs.mkdir("/d", 0o755)
        self.assertEqual([(DirOperation.MKDIR, "/d")], self.queued_dir_ops())
        self.fs.rename("/d", "/e")

        self.start_sync()
        self.wait_idle()
        # Only the old name is checked: the rename found no /d on the member
        # to move, so /e is not there either (its mkdir was the retry).
        self.assertNotIn("d", self.member_names())

    def test_mkdir_retry_still_applies_while_the_directory_exists(self):
        self.fail_member_once("mkdir")
        self.fs.mkdir("/d", 0o755)
        self.start_sync()
        self.wait_idle()
        self.assertEqual(["d"], self.member_names())

    def test_rmdir_retry_after_the_directory_was_made_again_is_dropped(self):
        self.fs.mkdir("/d", 0o755)
        self.fail_member_once("rmdir")
        self.fs.rmdir("/d")
        self.assertEqual([(DirOperation.RMDIR, "/d")], self.queued_dir_ops())
        self.fs.mkdir("/d", 0o755)

        self.start_sync()
        self.wait_idle()
        self.assertEqual(["d"], self.member_names())

    def test_missed_rename_whose_destination_is_gone_leaves_the_member(self):
        self.fs.mkdir("/d", 0o755)
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        self.fs.rmdir("/e")

        self.start_sync()
        self.wait_idle()
        # The member keeps an empty /d: what a chain of missed operations
        # leaves is ditz sync-member-reconcile. It must not gain /e.
        self.assertNotIn("e", self.member_names())
        self.assertEqual([("member", "/d", "/e")], self.repairs())

    def test_file_under_an_open_repair_is_not_evicted_until_resolved(self):
        """With one member, the bytes of /b.txt are on it only under /a.txt
        once it missed the rename: the cached copy is the one the namespace
        can read, so it stays until the repair is resolved."""
        self.write_file("/a.txt")
        self.start_sync()
        self.wait_synced("/a.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.fail_member_once("rename")
        self.fs.rename("/a.txt", "/b.txt")
        cache_path = self.fs.cache_manager.get_cache_path("/b.txt")
        self.assertEqual(0, self.fs.cache_manager._evict_if_still_clean("/b.txt", cache_path))
        self.assertEqual(b"payload", cache_path.read_bytes())
        self.assertEqual(["a.txt"], self.member_names())

        [repair] = self.fs.cache_manager.member_repairs.list()
        self.assertTrue(self.fs.cache_manager.member_repairs.resolve(repair.id))
        self.assertGreater(self.fs.cache_manager._evict_if_still_clean("/b.txt", cache_path), 0)

    def test_missed_rename_never_overwrites_a_newly_occupied_target(self):
        """The member missed /a.txt -> /b.txt, then something else put a
        /b.txt there (another writer of the member; members change behind
        the mount). The missed move is not replayed over it: both objects
        stay, and the missed move is recorded for repair."""
        self.write_file("/a.txt", b"old bytes")
        self.start_sync()
        self.wait_synced("/a.txt")
        self.wait_idle()
        self.fail_member_once("rename")
        self.fs.rename("/a.txt", "/b.txt")
        (self.member_dir / "b.txt").write_bytes(b"unrelated writer")

        self.wait_idle()
        self.fs.sync_manager.stop()
        self.assertEqual(b"unrelated writer", (self.member_dir / "b.txt").read_bytes())
        self.assertEqual(b"old bytes", (self.member_dir / "a.txt").read_bytes())
        self.assertEqual([("member", "/a.txt", "/b.txt")], self.repairs())

    def test_missed_rename_never_moves_a_reused_source_name(self):
        """The member missed /d -> /e; /d was then made again and gained a
        file on the member. Replaying the move would carry the new /d's
        contents to /e and take /d away from the namespace that has it."""
        self.fs.mkdir("/d", 0o755)
        (self.member_dir / "d" / "kept.txt").write_text("k")
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        self.fs.mkdir("/d", 0o755)
        (self.member_dir / "d" / "new.txt").write_text("n")

        self.start_sync()
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.assertEqual(["kept.txt", "new.txt"], self.member_names("d"))
        self.assertNotIn("e", self.member_names())
        self.assertEqual([("member", "/d", "/e")], self.repairs())

    def test_missed_rename_repair_survives_reopen_and_retries(self):
        """The repair is a carrier in the cache, not an index row or the
        in-memory queue; a queued rename task (no record of what the member
        held) becomes a record of its own, with no member call."""
        self.fs.mkdir("/d", 0o755)
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        # A rename task from a direct caller, carrying only the two names
        # (the queue is in memory, so none survives from an older build).
        self.fs.sync_manager.queue_dir_sync(
            "/d", DirOperation.RENAME, new_path="/e", backend_hint="member", force=True,
        )
        renames = []
        patcher = self.patch_member_rename(side_effect=lambda *a: renames.append(a))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.start_sync()
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.assertEqual([], renames)
        self.assertEqual(["d"], self.member_names())

        self.assertEqual([("member", "/d", "/e")] * 2, self.repairs())

    def test_open_repair_fences_a_later_rename_on_that_member(self):
        """After the member missed /a.txt -> /b.txt and something else put a
        /b.txt there, renaming /b.txt back to /a.txt through the mount must
        not run on that member: it would replace its /a.txt, the only copy
        of the renamed bytes there, with the unrelated /b.txt."""
        self.write_file("/a.txt", b"old bytes")
        self.start_sync()
        self.wait_synced("/a.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.fail_member_once("rename")
        self.fs.rename("/a.txt", "/b.txt")
        (self.member_dir / "b.txt").write_bytes(b"unrelated writer")

        self.fs.rename("/b.txt", "/a.txt")
        self.assertEqual(b"old bytes", (self.member_dir / "a.txt").read_bytes())
        self.assertEqual(b"unrelated writer", (self.member_dir / "b.txt").read_bytes())
        self.assertEqual(
            [("member", "/a.txt", "/b.txt"), ("member", "/b.txt", "/a.txt")],
            sorted(self.repairs()),
        )

    def test_open_repair_fences_a_later_directory_rename_below_it(self):
        """A repair at /d -> /e fences a rename of /e/sub on that member (a
        path below the repair's new name), not only its exact names."""
        self.fs.mkdir("/d", 0o755)
        self.fs.mkdir("/d/sub", 0o755)
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        renames = []
        patcher = self.patch_member_rename(side_effect=lambda *a: renames.append(a))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.fs.rename("/e/sub", "/e/other")
        self.assertEqual([], renames)
        self.assertEqual(["sub"], self.member_names("d"))
        self.assertIn(("member", "/e/sub", "/e/other"), self.repairs())

    def test_open_repair_does_not_fence_a_sibling_name(self):
        """Fencing is by path segment: a repair at /d leaves /dx alone."""
        self.fs.mkdir("/d", 0o755)
        self.fs.mkdir("/dx", 0o755)
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        self.fs.rename("/dx", "/dy")
        self.assertEqual(["d", "dy"], self.member_names())
        self.assertEqual([("member", "/d", "/e")], self.repairs())

    def test_open_repair_fences_uploads_at_those_names(self):
        """/d/f.txt was written but never synced when the member missed
        /d -> /e, and something else then put an /e/f.txt there. The member
        has no location row for it, so the conflict check would let the
        upload replace it: the open repair keeps the upload off that member,
        and the file stays dirty, as for a member that is down."""
        self.fs.mkdir("/d", 0o755)
        self.write_file("/d/f.txt", b"new bytes")
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        (self.member_dir / "e").mkdir()
        (self.member_dir / "e" / "f.txt").write_bytes(b"unrelated writer")
        uploads = []
        real_copy = self.member.copy_from_local
        patcher = patch.object(self.member, "copy_from_local",
                               side_effect=lambda *a: (uploads.append(a[1]), real_copy(*a)))
        patcher.start()
        self.addCleanup(patcher.stop)

        self.write_file("/c.txt", b"elsewhere")
        self.start_sync()
        self.wait_synced("/c.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.assertEqual(["/c.txt"], uploads)
        self.assertEqual(b"unrelated writer", (self.member_dir / "e" / "f.txt").read_bytes())
        # DIRTY, not ERROR: the monitor syncs it once the repair is resolved.
        self.assertEqual(SyncStatus.DIRTY,
                         self.fs.state_manager.get_file_metadata("/e/f.txt").sync_status)

    def test_open_repair_fences_link_uploads_at_those_names(self):
        """A link made through the mount at /e/l, after the member missed
        /d -> /e, must not be made on that member: there /e is not the
        namespace's /e. The link stays DIRTY, as a fenced file does."""
        self.write_file("/t.txt", b"target")
        self.fs.mkdir("/d", 0o755)
        self.start_sync()
        self.wait_synced("/t.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        (self.member_dir / "e").mkdir()
        links = []
        patcher = patch.object(self.member, "symlink", side_effect=lambda *a: links.append(a))
        patcher.start()
        self.addCleanup(patcher.stop)

        self.fs.symlink("/e/l", "../t.txt")
        self.write_file("/c.txt", b"elsewhere")
        self.start_sync()
        self.wait_synced("/c.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.assertEqual([], links)
        self.assertEqual([], self.member_names("e"))
        self.assertEqual(SyncStatus.DIRTY,
                         self.fs.state_manager.get_file_metadata("/e/l").sync_status)

    def test_open_repair_refuses_a_conflict_view_unlink_on_that_member(self):
        """After the member missed /d -> /e, its /d/f.txt may hold the only
        copy of the renamed bytes. Removing the conflict view /d/f.txt.member
        would delete exactly that object: it is refused, before any effect."""
        self.fs.mkdir("/d", 0o755)
        self.write_file("/d/f.txt", b"sole bytes")
        self.start_sync()
        self.wait_synced("/d/f.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        view = self.fs.cache_manager.get_cache_path("/d/f.txt.member")
        view.parent.mkdir(parents=True, exist_ok=True)
        view.write_bytes(b"sole bytes")
        deletes = []
        patcher = patch.object(self.member, "delete_file", side_effect=lambda *a: deletes.append(a))
        patcher.start()
        self.addCleanup(patcher.stop)

        with self.assertRaises(FuseOSError) as raised:
            self.fs.unlink("/d/f.txt.member")
        self.assertEqual(errno.EBUSY, raised.exception.errno)
        self.assertEqual([], deletes)
        self.assertTrue(view.exists())
        self.assertEqual(b"sole bytes", (self.member_dir / "d" / "f.txt").read_bytes())

    def test_open_repair_refuses_a_conflict_view_write_on_that_member(self):
        """Closing a conflict view written through the mount copies it to
        that member at the canonical name; under an open repair that would
        replace the member's object, so opening or creating the view for
        writing is refused with EBUSY, before any write is accepted. Reading
        it still works."""
        self.fs.mkdir("/d", 0o755)
        self.write_file("/d/f.txt", b"sole bytes")
        self.start_sync()
        self.wait_synced("/d/f.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        self.fs.mkdir("/d", 0o755)
        copies = []
        patcher = patch.object(self.member, "copy_from_local",
                               side_effect=lambda *a: copies.append(a))
        patcher.start()
        self.addCleanup(patcher.stop)

        with self.assertRaises(FuseOSError) as raised:
            self.write_file("/d/f.txt.member", b"overwrite")
        self.assertEqual(errno.EBUSY, raised.exception.errno)
        view = self.fs.cache_manager.get_cache_path("/d/f.txt.member")
        view.parent.mkdir(parents=True, exist_ok=True)
        view.write_bytes(b"sole bytes")
        with self.assertRaises(FuseOSError) as raised:
            self.fs.open("/d/f.txt.member", os.O_WRONLY)
        self.assertEqual(errno.EBUSY, raised.exception.errno)
        fh = self.fs.open("/d/f.txt.member", os.O_RDONLY)
        self.fs.release("/d/f.txt.member", fh)
        self.assertEqual([], copies)
        self.assertEqual(b"sole bytes", (self.member_dir / "d" / "f.txt").read_bytes())

    def test_a_repair_recorded_while_a_conflict_view_is_open_fails_its_close(self):
        """A view opened for writing before the repair exists: the write
        lands in the view, but the close (flush) reports EBUSY and release
        sends nothing to the member."""
        self.fs.mkdir("/d", 0o755)
        self.write_file("/d/f.txt", b"sole bytes")
        self.start_sync()
        self.wait_synced("/d/f.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        view = self.fs.cache_manager.get_cache_path("/d/f.txt.member")
        view.write_bytes(b"sole bytes")
        fh = self.fs.open("/d/f.txt.member", os.O_WRONLY)
        self.fs.write("/d/f.txt.member", b"overwrite!", 0, fh)
        self.fs.flush("/d/f.txt.member", fh)  # no repair yet: fine

        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        copies = []
        patcher = patch.object(self.member, "copy_from_local",
                               side_effect=lambda *a: copies.append(a))
        patcher.start()
        self.addCleanup(patcher.stop)
        with self.assertRaises(FuseOSError) as raised:
            self.fs.flush("/d/f.txt.member", fh)
        self.assertEqual(errno.EBUSY, raised.exception.errno)
        self.fs.release("/d/f.txt.member", fh)
        self.assertEqual([], copies)
        self.assertEqual(b"sole bytes", (self.member_dir / "d" / "f.txt").read_bytes())

    def _synced_view(self):
        """/d/f.txt synced to the member, its conflict view cached; sync off."""
        self.fs.mkdir("/d", 0o755)
        self.write_file("/d/f.txt", b"sole bytes")
        self.start_sync()
        self.wait_synced("/d/f.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        view = self.fs.cache_manager.get_cache_path("/d/f.txt.member")
        view.write_bytes(b"sole bytes")
        return view

    def _open_repair(self):
        """A missed rename of /d records a repair that fences the member."""
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")

    def _record_copies(self):
        copies = []
        patcher = patch.object(self.member, "copy_from_local",
                               side_effect=lambda *a: copies.append(a))
        patcher.start()
        self.addCleanup(patcher.stop)
        return copies

    def _kept_view_writes(self):
        repairs = MemberRepairs(self.cache_dir)
        return [repairs.carrier_path(r.id).read_bytes()
                for r in repairs.list() if r.kind == VIEW_WRITE]

    def _reader_close_keeps_the_writers_refusal(self, change, expected):
        """Barrier/control: a writer and an unmodified reader share a changed
        view; a repair appears; the reader closes first. The reader's close
        succeeds, the writer's close (flush) reports EBUSY, the member gets
        nothing, and the changed bytes are kept as one view-write repair.
        (Pre-#184, 59e8d28, behaved so too: a read-only close sent nothing
        and only writable descriptors were refused.)"""
        self._synced_view()
        reader = self.fs.open("/d/f.txt.member", os.O_RDONLY)
        writer = change()
        self._open_repair()
        copies = self._record_copies()
        self.assertEqual(0, self.fs.flush("/d/f.txt.member", reader))
        self.fs.release("/d/f.txt.member", reader)
        with self.assertRaises(FuseOSError) as raised:
            self.fs.flush("/d/f.txt.member", writer)
        self.assertEqual(errno.EBUSY, raised.exception.errno)
        self.fs.release("/d/f.txt.member", writer)
        self.assertEqual([], copies)
        self.assertEqual(b"sole bytes", (self.member_dir / "d" / "f.txt").read_bytes())
        self.assertEqual([expected], self._kept_view_writes())

    def test_a_reader_close_does_not_take_the_writers_refusal(self):
        def write():
            fh = self.fs.open("/d/f.txt.member", os.O_WRONLY)
            self.fs.write("/d/f.txt.member", b"overwrite!", 0, fh)
            return fh
        self._reader_close_keeps_the_writers_refusal(write, b"overwrite!")

    def test_a_reader_close_does_not_take_an_ftruncates_refusal(self):
        def ftruncate():
            fh = self.fs.open("/d/f.txt.member", os.O_WRONLY)
            self.fs.truncate("/d/f.txt.member", 3, fh)
            return fh
        self._reader_close_keeps_the_writers_refusal(ftruncate, b"sol")

    def test_a_reader_close_does_not_take_an_o_trunc_opens_refusal(self):
        self._reader_close_keeps_the_writers_refusal(
            lambda: self.fs.open("/d/f.txt.member", os.O_WRONLY | os.O_TRUNC), b"")

    def test_a_reader_close_does_not_take_a_creates_refusal(self):
        self._reader_close_keeps_the_writers_refusal(
            lambda: self.fs.create("/d/f.txt.member", 0o644), b"")

    def test_a_change_already_sent_is_not_refused_for_a_later_one(self):
        """A's write is sent by a reader's close; B then writes, a repair
        appears and another reader's close keeps B's bytes: only B's close
        reports the refusal."""
        self._synced_view()
        first_reader = self.fs.open("/d/f.txt.member", os.O_RDONLY)
        second_reader = self.fs.open("/d/f.txt.member", os.O_RDONLY)
        a = self.fs.open("/d/f.txt.member", os.O_WRONLY)
        self.fs.write("/d/f.txt.member", b"A", 0, a)
        self.fs.release("/d/f.txt.member", first_reader)  # sends A's change
        self.assertEqual(b"Aole bytes", (self.member_dir / "d" / "f.txt").read_bytes())
        b = self.fs.open("/d/f.txt.member", os.O_WRONLY)
        self.fs.write("/d/f.txt.member", b"B", 1, b)
        self._open_repair()
        self._record_copies()
        self.fs.release("/d/f.txt.member", second_reader)  # keeps B's
        self.assertEqual(0, self.fs.flush("/d/f.txt.member", a))
        with self.assertRaises(FuseOSError) as raised:
            self.fs.flush("/d/f.txt.member", b)
        self.assertEqual(errno.EBUSY, raised.exception.errno)
        for fh in (a, b):
            self.fs.release("/d/f.txt.member", fh)
        self.assertEqual([b"ABle bytes"], self._kept_view_writes())

    def test_bytes_not_kept_mark_no_one_refused(self):
        self._synced_view()
        reader = self.fs.open("/d/f.txt.member", os.O_RDONLY)
        writer = self.fs.open("/d/f.txt.member", os.O_WRONLY)
        self.fs.write("/d/f.txt.member", b"overwrite!", 0, writer)
        self._open_repair()
        self._record_copies()
        with patch.object(self.fs.cache_manager.member_repairs, "preserve_view_write",
                          return_value=False), self.assertLogs("storage.fs", level="ERROR"):
            self.fs.release("/d/f.txt.member", reader)
        with self.fs._fd_lock:
            self.assertFalse(self.fs._fd_map[writer].view_refused)
        # Still pending, so the writer's own close reports the refusal.
        with self.assertRaises(FuseOSError):
            self.fs.flush("/d/f.txt.member", writer)
        self.fs.release("/d/f.txt.member", writer)

    def test_flush_and_a_keeping_release_do_not_interleave(self):
        """Barrier: a reader's release is inside the keep (holding the path
        lock) when the writer's flush starts. The flush must not read
        "not refused" before the keep and "nothing pending" after it."""
        self._synced_view()
        reader = self.fs.open("/d/f.txt.member", os.O_RDONLY)
        writer = self.fs.open("/d/f.txt.member", os.O_WRONLY)
        self.fs.write("/d/f.txt.member", b"overwrite!", 0, writer)
        self._open_repair()
        self._record_copies()
        repairs = self.fs.cache_manager.member_repairs
        in_keep, finish_keep = threading.Event(), threading.Event()
        flush_checking, finish_flush = threading.Event(), threading.Event()
        real_preserve, real_backends_for = repairs.preserve_view_write, repairs.backends_for
        flusher = {}

        def slow_preserve(*args, **kwargs):
            in_keep.set()
            finish_keep.wait(10)
            return real_preserve(*args, **kwargs)

        def observed_backends_for(paths):
            if threading.current_thread() is flusher.get("thread"):
                flush_checking.set()
                finish_flush.wait(10)
            return real_backends_for(paths)

        def flush():
            try:
                flusher["result"] = self.fs.flush("/d/f.txt.member", writer)
            except FuseOSError as exc:
                flusher["result"] = exc.errno

        with patch.object(repairs, "preserve_view_write", side_effect=slow_preserve), \
                patch.object(repairs, "backends_for", side_effect=observed_backends_for):
            releaser = threading.Thread(target=self.fs.release, args=("/d/f.txt.member", reader))
            releaser.start()
            self.assertTrue(in_keep.wait(10))
            thread = threading.Thread(target=flush)
            flusher["thread"] = thread
            thread.start()
            # With the lock the flush waits for the keep; without it, it would
            # check here, between "not refused" and "pending".
            flush_checking.wait(0.5)
            finish_keep.set()
            releaser.join(10)
            finish_flush.set()
            thread.join(10)
        self.assertEqual(errno.EBUSY, flusher["result"])
        self.fs.release("/d/f.txt.member", writer)
        self.assertEqual([b"overwrite!"], self._kept_view_writes())

    def _path_truncate_kept(self, flags):
        """A truncate by path (no descriptor of its own), then a repair, then
        a descriptor that changes nothing closes: its close succeeds, and the
        truncated bytes are kept once. (59e8d28: a read-only close kept
        nothing; fa274d4: that close got EBUSY.)"""
        self._synced_view()
        # Opened before the repair: a repair refuses a writable view open.
        closer = self.fs.open("/d/f.txt.member", flags)
        self.fs.truncate("/d/f.txt.member", 4)
        self._open_repair()
        copies = self._record_copies()
        self.assertEqual(0, self.fs.flush("/e/f.txt.member", closer))
        self.fs.release("/e/f.txt.member", closer)
        self.assertEqual([], copies)
        self.assertEqual([b"sole"], self._kept_view_writes())

    def test_a_path_truncate_kept_by_a_readers_close(self):
        self._path_truncate_kept(os.O_RDONLY)

    def test_a_path_truncate_kept_by_an_unchanged_writers_close(self):
        self._path_truncate_kept(os.O_RDWR)

    def test_read_only_flush_takes_no_path_lock(self):
        self._synced_view()
        reader = self.fs.open("/d/f.txt.member", os.O_RDONLY)
        with patch.object(self.fs, "_path_locked", side_effect=AssertionError("path lock")):
            self.assertEqual(0, self.fs.flush("/d/f.txt.member", reader))
        self.fs.release("/d/f.txt.member", reader)

    def test_a_member_known_down_during_a_rename_gets_a_repair(self):
        """A member that is unavailable when the rename runs has missed it
        like one whose call failed: it must not drop out of the record."""
        self.fs.mkdir("/d", 0o755)
        self.member.set_available(False)
        self.addCleanup(self.member.set_available, True)
        calls = self.fail_member_once("rename")  # as an offline member's call fails
        self.fs.rename("/d", "/e")
        self.assertEqual([("/d", "/e")], calls)
        self.assertEqual([("member", "/d", "/e")], self.repairs())
        self.assertEqual(["d"], self.member_names())

    def test_a_dirty_rename_onto_a_new_member_target_is_a_conflict(self):
        """Where the member does have a location row for the file, the
        conflict check already refuses to replace an unrelated copy that
        appeared at the new name; the open repair fences it as well."""
        self.write_file("/a.txt", b"old bytes")
        self.start_sync()
        self.wait_synced("/a.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.fail_member_once("rename")
        self.fs.rename("/a.txt", "/b.txt")
        (self.member_dir / "b.txt").write_bytes(b"unrelated writer")
        self.write_file("/b.txt", b"new bytes")
        self.start_sync()
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.assertEqual(b"unrelated writer", (self.member_dir / "b.txt").read_bytes())
        self.assertEqual(b"old bytes", (self.member_dir / "a.txt").read_bytes())

    def test_rename_fails_before_any_effect_when_its_intent_cannot_be_written(self):
        """The intent is written before the local move and any member call:
        if the cache refuses it (full, no user xattrs, a record too large),
        the rename returns that errno and nothing has changed anywhere."""
        self.write_file("/a.txt", b"bytes")
        self.start_sync()
        self.wait_synced("/a.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        renames = []
        self.patch_member_rename(side_effect=lambda *a: renames.append(a)).start()
        self.addCleanup(patch.stopall)
        real_setxattr = os.setxattr
        for err in (errno.ENOSPC, errno.ENOTSUP, errno.E2BIG):
            def refusing(target, name, *args, err=err, **kwargs):
                if name == "user.storage.member_repair":
                    raise OSError(err, os.strerror(err))
                return real_setxattr(target, name, *args, **kwargs)

            with patch("storage.member_repairs.os.setxattr", side_effect=refusing):
                with self.assertRaises(FuseOSError) as raised:
                    self.fs.rename("/a.txt", "/b.txt")
            self.assertEqual(err, raised.exception.errno)
            self.assertTrue(self.fs.cache_manager.get_cache_path("/a.txt").exists())
            self.assertFalse(os.path.lexists(self.fs.cache_manager.get_cache_path("/b.txt")))
            self.assertIsNotNone(self.fs.state_manager.get_file_metadata("/a.txt"))
            self.assertIsNone(self.fs.state_manager.get_file_metadata("/b.txt"))
            self.assertEqual([], renames)
            self.assertEqual(["a.txt"], self.member_names())
            self.assertEqual([], self.repairs())
            self.assertEqual([], os.listdir(self.cache_dir / ".storage.repairs"))

    def test_failed_settle_leaves_the_intent_for_every_member(self):
        """The member rename failed, and writing its open record fails too:
        the intent stays, naming every target member."""
        self.fs.mkdir("/d", 0o755)
        self.fail_member_once("rename")
        repairs = self.fs.cache_manager.member_repairs
        real_write = repairs._write_new
        writes = []

        def failing_after_the_intent(record, **kwargs):
            writes.append(record.state)
            if record.state != "intent":
                raise OSError(errno.EIO, "injected")
            return real_write(record, **kwargs)

        with patch.object(repairs, "_write_new", side_effect=failing_after_the_intent):
            self.fs.rename("/d", "/e")
        self.assertEqual(["intent", "open"], writes)
        [record] = MemberRepairs(self.cache_dir).list()
        self.assertEqual(("intent", ("member",), "/d", "/e"),
                         (record.state, record.members, record.old_path, record.new_path))

    def test_failed_unlink_after_every_member_renamed_keeps_the_record(self):
        """Every member took the move but the intent could not be removed:
        it stays (over-fencing, never under)."""
        self.fs.mkdir("/d", 0o755)
        carriers = self.cache_dir / ".storage.repairs"
        real_unlink = Path.unlink

        def refusing(path, *args, **kwargs):
            if path.parent == carriers:
                raise OSError(errno.EIO, "injected")
            return real_unlink(path, *args, **kwargs)

        with patch.object(Path, "unlink", refusing):
            self.fs.rename("/d", "/e")
        self.assertEqual(["e"], self.member_names())
        [record] = MemberRepairs(self.cache_dir).list()
        self.assertEqual(("intent", ("member",)), (record.state, record.members))

    def test_a_queued_rename_task_whose_record_fails_is_retried_until_written(self):
        """A rename task's only effect is its record: while the cache
        refuses it the task is retried past max_retries, with no member
        call, until it is written."""
        self.fs.mkdir("/d", 0o755)
        repairs = self.fs.cache_manager.member_repairs
        real_record = repairs.record
        attempts = []
        refusals = self.fs.sync_manager.config.max_retries + 3

        def failing(*args):
            attempts.append(args)
            if len(attempts) <= refusals:
                raise OSError(errno.ENOSPC, "injected")
            return real_record(*args)

        patch.object(repairs, "record", side_effect=failing).start()
        renames = []
        self.patch_member_rename(side_effect=lambda *a: renames.append(a)).start()
        self.addCleanup(patch.stopall)
        self.fs.sync_manager.queue_dir_sync("/d", DirOperation.RENAME, new_path="/e",
                                            backend_hint="member", force=True)
        self.fs.sync_manager.config.retry_delay = 0
        self.start_sync()
        deadline = time.monotonic() + WAIT
        while not self.repairs() and time.monotonic() < deadline:
            time.sleep(0.02)
        self.wait_idle()
        self.assertEqual([("member", "/d", "/e")], self.repairs())
        self.assertEqual(refusals + 1, len(attempts))
        self.assertEqual([], renames)
        self.assertEqual(["d"], self.member_names())

    def flaky_rename_record(self):
        """A queued /d -> /e rename for the member whose record the cache
        refuses until failing[0] is cleared; returns (failing, attempts,
        renames). Its retry backs off for an hour unless the test ends it."""
        self.fs.mkdir("/d", 0o755)
        self.fs.mkdir("/e", 0o755)
        repairs = self.fs.cache_manager.member_repairs
        real_record = repairs.record
        failing = [True]
        attempts = []

        def flaky(*args):
            attempts.append(args)
            if failing[0]:
                raise OSError(errno.ENOSPC, "injected")
            return real_record(*args)

        patch.object(repairs, "record", side_effect=flaky).start()
        renames = []
        self.patch_member_rename(side_effect=lambda *a: renames.append(a)).start()
        self.addCleanup(patch.stopall)
        self.fs.sync_manager.config.retry_delay = 3600.0
        self.fs.sync_manager.queue_dir_sync("/d", DirOperation.RENAME, new_path="/e",
                                            backend_hint="member", force=True)
        return failing, attempts, renames

    def scheduler(self):
        return RetryScheduler.for_queue(self.fs.sync_manager.task_queue)

    def wait_until(self, condition, what):
        deadline = time.monotonic() + WAIT
        while not condition():
            if time.monotonic() > deadline:
                self.fail(f"never {what}")
            time.sleep(0.01)

    def test_healthy_sync_runs_while_a_failing_rename_record_backs_off(self):
        """One worker. A queued rename's record keeps failing and its retry
        is backing off: an unrelated file still syncs meanwhile, with its
        bytes on the member, and an upload at the rename's new name is still
        kept off the member, as the record would keep it. Once the cache
        takes the record there is exactly one, and the member was never
        renamed (CTO RRS-CTO-BACKOFF). The backoff ends only when the test
        ends it, so nothing depends on timing."""
        failing, attempts, renames = self.flaky_rename_record()
        self.start_sync()
        self.assertEqual(1, len(self.fs.sync_manager.workers))
        self.wait_until(lambda: self.scheduler().pending() == 1, "deferred the retry")
        self.assertEqual(1, len(attempts))

        self.write_file("/c.txt", b"unrelated")
        self.write_file("/e/x.txt", b"under the new name")
        self.wait_synced("/c.txt")
        # Both files' tasks have run (the deferred retry is not queued).
        manager = self.fs.sync_manager
        self.wait_until(lambda: manager.task_queue.qsize() == 0 and all(
            w.get_active_task() is None for w in manager.workers), "run the queued tasks")
        self.assertEqual(b"unrelated", (self.member_dir / "c.txt").read_bytes())
        self.assertEqual(1, len(attempts))
        self.assertEqual(1, self.scheduler().pending())
        self.assertFalse((self.member_dir / "e" / "x.txt").exists())
        self.assertFalse(self.fs.sync_manager.wait_for_completion(timeout=0.2))

        failing[0] = False
        self.scheduler().deliver_all_now()
        self.wait_until(lambda: self.repairs(), "wrote the record")
        self.wait_idle()
        self.assertEqual(2, len(attempts))
        self.assertEqual([("member", "/d", "/e")], self.repairs())
        self.assertEqual([], renames)
        self.assertEqual(["c.txt", "d", "e"], self.member_names())
        self.assertFalse((self.member_dir / "e" / "x.txt").exists())
        self.assertEqual(SyncStatus.DIRTY,
                         self.fs.state_manager.get_file_metadata("/e/x.txt").sync_status)

    def test_a_deferred_rename_retry_survives_stop_and_restart(self):
        """A retry deferred when the workers stop stays counted as
        unfinished, and the restarted workers run it."""
        failing, attempts, renames = self.flaky_rename_record()
        self.start_sync()
        self.wait_until(lambda: self.scheduler().pending() == 1, "deferred the retry")
        self.fs.sync_manager.stop()
        self.assertFalse(self.fs.sync_manager.wait_for_completion(timeout=0.2))
        failing[0] = False
        self.start_sync()
        self.scheduler().deliver_all_now()
        self.wait_until(lambda: self.repairs(), "wrote the record")
        self.wait_idle()
        self.assertEqual([("member", "/d", "/e")], self.repairs())
        self.assertEqual([], renames)
        self.assertEqual(0, self.fs.sync_manager.task_queue.unfinished_tasks)


    def test_open_repair_fences_rmdir_of_a_reused_source(self):
        """The member missed an empty /d -> /e; /d was made again and is
        now removed through the mount. The member's /d is the one the
        missed rename left there, so it stays."""
        self.fs.mkdir("/d", 0o755)
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        self.fs.mkdir("/d", 0o755)
        self.fs.rmdir("/d")
        self.assertEqual(["d"], self.member_names())

    def test_open_repair_fences_rmdir_of_a_new_target_occupant(self):
        """Another writer made an empty /e on the member after it missed
        /d -> /e; removing /e through the mount leaves it there."""
        self.fs.mkdir("/d", 0o755)
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        (self.member_dir / "e").mkdir()
        self.fs.rmdir("/e")
        self.assertEqual(["d", "e"], self.member_names())

    def test_open_repair_fences_a_queued_rmdir_retry(self):
        """An rmdir retry that still applies (the cache has no /e) is not
        run on a member with an open repair at /e: its empty /e may be the
        object that repair is about."""
        (self.member_dir / "e").mkdir()
        self.fs.cache_manager.member_repairs.record("/d", "/e", ["member"], "missed")
        self.assertFalse(os.path.lexists(self.fs.cache_manager.get_cache_path("/e")))
        executor = SyncExecutor(
            worker_id=9, state_manager=self.fs.state_manager, cache_manager=self.fs.cache_manager,
            replication_manager=self.fs.replication_manager, config=self.fs.sync_manager.config,
        )
        self.assertTrue(executor.process_dir_task(
            DirSyncTask("/e", DirOperation.RMDIR, backend_hint="member")))
        self.assertEqual(["e"], self.member_names())

    def test_repair_is_on_disk_when_the_rename_returns(self):
        """A process that dies right after the rename returns, closing
        nothing, leaves the record in the database file."""
        ctx = multiprocessing.get_context("fork")
        child = ctx.Process(target=_rename_then_die, args=(self.temp_dir,))
        child.start()
        child.join(WAIT)
        if child.exitcode is None:
            child.kill()
            child.join()
            self.fail("the child neither renamed nor exited")
        self.assertEqual(0, child.exitcode)
        self.assertEqual([("member", "/d", "/e")], self.repairs(self.temp_dir / "crash" / "cache"))
        self.assertEqual(["d"], sorted(os.listdir(self.temp_dir / "crash" / "member")))

    def test_retry_for_a_member_known_down_is_not_tried_under_the_guard(self):
        self.fail_member_once("mkdir")
        self.fs.mkdir("/d", 0o755)
        self.member.set_available(False)
        guarded = []

        def recording_guard(*paths):
            guarded.append(paths)
            return self.fs._path_locked(*paths)

        task = [t for t in self.fs.sync_manager.task_queue.queue if hasattr(t, "operation")][0]
        executor = SyncExecutor(
            worker_id=9, state_manager=self.fs.state_manager, cache_manager=self.fs.cache_manager,
            replication_manager=self.fs.replication_manager, config=self.fs.sync_manager.config,
            path_guard=recording_guard,
        )
        with self.assertRaises(RuntimeError):
            executor.process_dir_task(task)
        self.assertEqual([], guarded)
        self.member.set_available(True)
        self.assertTrue(executor.process_dir_task(task))
        self.assertEqual(["d"], self.member_names())


class TestFencePublicationOrder(SyncRaceTestBase):
    """A queued rename's fence, published outside the namespace's path
    locks, is ordered against member mutations: one already running
    finishes before the fence takes effect, a later one skips the fenced
    member, and unrelated member work goes on (CTO-182-J-LENGTH-C3 (3))."""

    def executor(self):
        return SyncExecutor(
            worker_id=9, state_manager=self.fs.state_manager,
            cache_manager=self.fs.cache_manager,
            replication_manager=self.fs.replication_manager,
            config=self.fs.sync_manager.config, path_guard=self.fs._path_locked,
        )

    def publish(self, old, new):
        """A queued rename of old -> new for the member, recorded from a
        worker of its own; returns the running Worker."""
        task = DirSyncTask(old, DirOperation.RENAME, new_path=new, backend_hint="member")
        worker = Worker(self.executor().process_dir_task, task)
        worker.start()
        return worker

    def test_a_running_upload_below_the_names_finishes_before_the_fence(self):
        """Mutation first, descendant: an upload of /d/x.txt is in its
        member copy when /d -> /e is published. The publication waits, an
        unrelated mkdir reaches the member meanwhile, and the record is
        written once the upload is done."""
        self.fs.mkdir("/d", 0o755)
        gate = self.gate_upload("/d/x.txt")
        self.start_sync()
        self.write_file("/d/x.txt", b"before the fence")
        self.assertTrue(gate.started.wait(WAIT))
        publisher = self.publish("/d", "/e")
        publisher.join(BLOCKED)
        self.assertTrue(publisher.is_alive(), "the fence did not wait for the upload")
        self.assertEqual([], self.repairs())
        self.fs.mkdir("/z", 0o755)                       # disjoint: not held up
        self.assertIn("z", self.member_names())
        gate.opened.set()
        self.assertTrue(publisher.finish())
        self.wait_idle()
        self.assertEqual([("member", "/d", "/e")], self.repairs())
        self.assertEqual(b"before the fence", (self.member_dir / "d" / "x.txt").read_bytes())

    def test_a_running_rename_above_the_names_finishes_before_the_fence(self):
        """Mutation first, ancestor: a rename of /p is in its member call
        when /p/q -> /p/r is published; the publication waits for it."""
        self.fs.mkdir("/p", 0o755)
        self.fs.mkdir("/p/q", 0o755)
        gate = self.gate_member_rename()
        renamer = Worker(self.fs.rename, "/p", "/p2")
        renamer.start()
        self.assertTrue(gate.started.wait(WAIT))
        publisher = self.publish("/p/q", "/p/r")
        publisher.join(BLOCKED)
        self.assertTrue(publisher.is_alive(), "the fence did not wait for the rename")
        gate.opened.set()
        renamer.finish()
        self.assertTrue(publisher.finish())
        self.assertEqual(["p2"], self.member_names())
        self.assertEqual([("member", "/p/q", "/p/r")], self.repairs())

    def block_record(self):
        """Make the next queued rename's record wait, with its fence already
        published, until the returned event is set."""
        repairs = self.fs.cache_manager.member_repairs
        real = repairs.record
        entered, release = threading.Event(), threading.Event()

        def blocked(*args):
            entered.set()
            assert release.wait(WAIT), "record never released"
            return real(*args)

        patch.object(repairs, "record", side_effect=blocked).start()
        self.addCleanup(patch.stopall)
        return entered, release

    def test_a_mutation_below_a_published_fence_skips_the_member(self):
        """Fence first, descendant: while /d -> /e's record is still being
        written, a mkdir of /d/sub and an upload of /e/x.txt skip the
        member; an unrelated mkdir does not."""
        self.fs.mkdir("/d", 0o755)
        self.fs.mkdir("/e", 0o755)
        entered, release = self.block_record()
        publisher = self.publish("/d", "/e")
        self.assertTrue(entered.wait(WAIT))
        mkdirs = []
        real_mkdir = self.member.mkdir
        patch.object(self.member, "mkdir",
                     side_effect=lambda *a: (mkdirs.append(a), real_mkdir(*a))).start()
        self.fs.mkdir("/d/sub", 0o755)
        self.fs.mkdir("/z", 0o755)
        self.write_file("/e/x.txt", b"after the fence")
        self.assertTrue(self.executor().process_file_task(SyncTask("/e/x.txt")))
        self.assertEqual([("/z",)], mkdirs)
        self.assertFalse((self.member_dir / "e" / "x.txt").exists())
        release.set()
        self.assertTrue(publisher.finish())
        self.assertEqual([("member", "/d", "/e")], self.repairs())
        self.assertEqual(["d", "e", "z"], self.member_names())
        self.assertEqual([], self.member_names("d"))

    def test_a_rename_above_a_published_fence_skips_the_member(self):
        """Fence first, ancestor: a rename of /p while /p/q -> /p/r's record
        is being written is not sent to the member: its miss is recorded."""
        self.fs.mkdir("/p", 0o755)
        self.fs.mkdir("/p/q", 0o755)
        entered, release = self.block_record()
        publisher = self.publish("/p/q", "/p/r")
        self.assertTrue(entered.wait(WAIT))
        renames = []
        self.patch_member_rename(side_effect=lambda *a: renames.append(a)).start()
        self.fs.rename("/p", "/p2")
        self.assertEqual([], renames)
        release.set()
        self.assertTrue(publisher.finish())
        self.assertEqual(["p"], self.member_names())
        self.assertEqual(sorted([("member", "/p", "/p2"), ("member", "/p/q", "/p/r")]),
                         self.repairs())

    def test_a_fence_lifted_after_registration_keeps_the_member_registered(self):
        """Repair A fences the member at /d when a mkdir of /d/sub registers;
        A is resolved before the mkdir looks the fence up again; repair B
        (/d -> /e) is then published while the member call runs. B must
        wait for it: the mutation registered the member for its whole
        life, not just the members unfenced at registration (CTO-182-DELTA)."""
        self.fs.mkdir("/d", 0o755)
        repairs = self.fs.cache_manager.member_repairs
        repair_a = repairs.record("/d", "/x", ["member"], "repair A")
        real_lookup = repairs.backends_for
        registered, resolved = threading.Event(), threading.Event()

        def lookup_after_resolve(paths):
            if not registered.is_set():
                registered.set()
                assert resolved.wait(WAIT)
            return real_lookup(paths)

        gate = self.gate(self.member, "mkdir")
        with patch.object(repairs, "backends_for", side_effect=lookup_after_resolve):
            maker = Worker(self.fs.mkdir, "/d/sub", 0o755)
            maker.start()
            self.assertTrue(registered.wait(WAIT))
            self.assertTrue(repairs.resolve(repair_a))
            resolved.set()
            self.assertTrue(gate.started.wait(WAIT))     # in its member call
        publisher = self.publish("/d", "/e")
        publisher.join(BLOCKED)
        self.assertTrue(publisher.is_alive(), "B did not wait for the running member call")
        gate.opened.set()
        maker.finish()
        self.assertTrue(publisher.finish())
        self.assertEqual(["sub"], self.member_names("d"))
        self.assertEqual([("member", "/d", "/e")], self.repairs())

    def test_a_publication_stalled_by_member_work_frees_its_only_worker(self):
        """One worker. A queued rename's fence waits for an overlapping
        foreground file rename stuck in its member call; past the bound the
        task backs off with the fence still active, an unrelated file syncs
        on the same worker meanwhile, and the record is written once the
        call ends. (A stuck directory rename holds the namespace lock
        exclusive, which stalls every sync by design.)"""
        self.write_file("/f.txt", b"file")
        self.start_sync()
        self.wait_synced("/f.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        gate = self.gate_member_rename()
        renamer = Worker(self.fs.rename, "/f.txt", "/g.txt")
        renamer.start()
        self.assertTrue(gate.started.wait(WAIT))
        self.fs.sync_manager.config.retry_delay = 3600.0
        with patch("storage.member_repairs.PUBLISH_WAIT", 0.2):
            self.fs.sync_manager.queue_dir_sync("/g.txt", DirOperation.RENAME, new_path="/h.txt",
                                                backend_hint="member", force=True)
            self.start_sync()
            scheduler = RetryScheduler.for_queue(self.fs.sync_manager.task_queue)
            deadline = time.monotonic() + WAIT
            while scheduler.pending() != 1 and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(1, scheduler.pending())
            self.assertEqual({"member"},
                             self.fs.cache_manager.member_repairs.backends_for(["/h.txt"]))
            self.write_file("/c.txt", b"unrelated")
            self.wait_synced("/c.txt")
            self.assertEqual(b"unrelated", (self.member_dir / "c.txt").read_bytes())
            # Only the running rename's own intent: the queued record waits.
            self.assertNotIn(("member", "/g.txt", "/h.txt"), self.repairs())
            gate.opened.set()
            renamer.finish()
            scheduler.deliver_all_now()
            deadline = time.monotonic() + WAIT
            while not self.repairs() and time.monotonic() < deadline:
                time.sleep(0.02)
            self.wait_idle()
        self.assertEqual([("member", "/g.txt", "/h.txt")], self.repairs())
        self.assertEqual(["c.txt", "g.txt"], self.member_names())

    def test_a_snapshot_is_never_between_a_pending_fence_and_its_record(self):
        """The handoff from a pending fence to its durable record cannot
        land between the snapshot's two reads: a snapshot in progress holds
        it off, and then sees the fence."""
        repairs = self.fs.cache_manager.member_repairs
        key = repairs.fence_pending("/h", "/h2", ["member"])
        real_stamp = repairs._dir_stamp
        in_snapshot, go_on = threading.Event(), threading.Event()
        main = threading.get_ident()

        def pausing_stamp():
            # The stamp is read first, then the snapshot pauses: the
            # handoff's record lands on disk after the snapshot looked.
            stamp = real_stamp()
            if threading.get_ident() == main and not in_snapshot.is_set():
                in_snapshot.set()
                assert go_on.wait(WAIT)
            return stamp

        def handoff():
            repairs.record("/h", "/h2", ["member"], "handoff")
            repairs.release_pending(key)

        mover = Worker(handoff)
        held_off = []

        def start_mover_during_snapshot():
            assert in_snapshot.wait(WAIT)
            mover.start()
            carriers = self.cache_dir / ".storage.repairs"
            deadline = time.monotonic() + WAIT
            while not (carriers.exists() and os.listdir(carriers)):
                assert time.monotonic() < deadline, "the record was never written"
                time.sleep(0.01)
            mover.join(BLOCKED)
            held_off.append(mover.is_alive())   # waiting on the snapshot's lock
            go_on.set()

        starter = threading.Thread(target=start_mover_during_snapshot)
        starter.start()
        fenced_holder = {}
        with patch.object(repairs, "_dir_stamp", side_effect=pausing_stamp):
            fenced_holder["fenced"] = repairs.backends_for(["/h"])
        starter.join(WAIT)
        mover.finish()
        self.assertEqual([True], held_off)
        self.assertEqual({"member"}, fenced_holder["fenced"])
        self.assertEqual({"member"}, repairs.backends_for(["/h"]))
        self.assertEqual([("member", "/h", "/h2")], self.repairs())


class TestRetryScheduler(unittest.TestCase):
    """The rename retry backoff: bounded threads, nothing stranded."""

    def task(self, name):
        return DirSyncTask(name, DirOperation.RENAME, new_path=name + "-new")

    def scheduler_threads(self):
        return [t for t in threading.enumerate() if t.name == "sync-retry-scheduler"]

    def test_a_burst_of_retries_uses_one_thread_and_delivers_each_once(self):
        q = queue.PriorityQueue()
        scheduler = RetryScheduler.for_queue(q)
        for i in range(50):
            self.assertTrue(scheduler.defer(self.task(f"/t{i}"), 3600.0))
        self.assertLessEqual(len(self.scheduler_threads()), 1)
        self.assertEqual(50, q.unfinished_tasks)
        self.assertEqual(0, q.qsize())
        scheduler.deliver_all_now()
        deadline = time.monotonic() + WAIT
        while q.qsize() < 50 and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(50, q.qsize())
        self.assertEqual(50, q.unfinished_tasks)  # each counted once
        self.assertEqual(sorted(f"/t{i}" for i in range(50)),
                         sorted(q.get_nowait().virtual_path for _ in range(50)))
        deadline = time.monotonic() + WAIT
        while self.scheduler_threads() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual([], self.scheduler_threads())  # exits when idle

    def test_a_scheduler_that_cannot_start_holds_and_counts_nothing(self):
        q = queue.PriorityQueue()
        scheduler = RetryScheduler.for_queue(q)
        with patch("storage.sync_executor.threading.Thread.start",
                   side_effect=RuntimeError("can't start new thread")):
            self.assertFalse(scheduler.defer(self.task("/t"), 3600.0))
        self.assertEqual(0, q.unfinished_tasks)
        self.assertEqual(0, scheduler.pending())

    def test_a_rename_retry_that_cannot_be_deferred_is_requeued_inline(self):
        q = queue.PriorityQueue()
        executor = SyncExecutor(worker_id=0, state_manager=None, cache_manager=None,
                                replication_manager=None,
                                config=SyncConfig(retry_delay=0.0, max_retries=3))
        task = self.task("/t")
        with patch("storage.sync_executor.threading.Thread.start",
                   side_effect=RuntimeError("can't start new thread")):
            self.assertFalse(executor.handle_task_failure(task, "injected", q))
        self.assertIs(task, q.get_nowait())
        self.assertEqual(1, q.unfinished_tasks)


def _die_after_member_rename(temp_dir):
    """In a forked child: the member takes /d -> /e, then the process dies
    before the rename settles its intent."""
    root = temp_dir / "crash2"
    (root / "cache").mkdir(parents=True)
    (root / "member").mkdir()
    member = LocalBackend("member", str(root / "member"))
    fs = StorageFS(cache_root=root / "cache", state_db=root / "state.db",
                   backends={"member": member}, cache_size=10 * 1024 * 1024,
                   num_sync_workers=1, start_sync=False)
    fs.mkdir("/d", 0o755)
    real = member.rename_noreplace

    def rename_then_die(*args):
        real(*args)
        os._exit(0)

    member.rename = member.rename_noreplace = rename_then_die
    fs.rename("/d", "/e")
    os._exit(1)


class TestRepairsOutliveTheIndex(SyncRaceTestBase):
    """The repair records are carriers in the cache: a discarded or rebuilt
    index loses none of them, and the fences and pins are in force from the
    moment the cache opens (CTO RRS-CTO-20261118C)."""

    def reopen(self, rebuild=False):
        """Stop this StorageFS, discard its index, open a fresh one (after
        rebuild-index from the cache when asked) over the same cache."""
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.temp_dir / "state.db") + suffix)
            if path.exists():
                path.unlink()
        db = self.temp_dir / "state.db"
        if rebuild:
            state = StateManager(str(db))
            try:
                rebuild_index_from_cache(self.cache_dir, state)
            finally:
                state.close()
        self.fs = StorageFS(
            cache_root=self.cache_dir, state_db=db, backends={"member": self.member},
            cache_size=10 * 1024 * 1024, num_sync_workers=1, start_sync=False,
        )

    def open_edited_view(self):
        """/d/f.txt is synced, and its conflict view /d/f.txt.member is
        open for writing with edited bytes, flushed while no repair exists:
        close() would report success from here on."""
        self.fs.mkdir("/d", 0o755)
        self.write_file("/d/f.txt", b"sole bytes")
        self.start_sync()
        self.wait_synced("/d/f.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        view = self.fs.cache_manager.get_cache_path("/d/f.txt.member")
        view.write_bytes(b"sole bytes")
        fh = self.fs.open("/d/f.txt.member", os.O_RDWR)
        self.fs.write("/d/f.txt.member", b"EDITED", 0, fh)
        self.assertEqual(0, self.fs.flush("/d/f.txt.member", fh))
        self.assertEqual(0, self.fs.flush("/d/f.txt.member", fh))  # a dup'd close
        return view, fh

    def view_writes(self, cache_dir=None):
        """(member, path, bytes) per open view-write repair, from disk."""
        repairs = MemberRepairs(cache_dir or self.cache_dir)
        return sorted(
            (r.members, r.new_path, repairs.carrier_path(r.id).read_bytes())
            for r in repairs.list() if r.kind == VIEW_WRITE
        )

    def test_a_view_write_kept_off_its_member_after_close_is_kept_durably(self):
        """Barrier: the flush has succeeded, then a missed rename records a
        repair, then the view is released. The member keeps its object,
        and the exact edited bytes are kept as a repair that survives
        eviction of the view, a fresh index and a rebuilt one, and is
        listed (CTO RRS-CTO-FLUSH-RELEASE). The descriptor followed the
        rename, so the record names /e/f.txt."""
        view, fh = self.open_edited_view()
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        copies = []
        real_copy = self.member.copy_from_local
        patcher = patch.object(self.member, "copy_from_local",
                               side_effect=lambda *a: (copies.append(a), real_copy(*a)))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.fs.release("/d/f.txt.member", fh)

        self.assertEqual([], copies)
        self.assertEqual(b"sole bytes", (self.member_dir / "d" / "f.txt").read_bytes())
        expected = [(("member",), "/e/f.txt", b"EDITEDytes")]
        self.assertEqual(expected, self.view_writes())
        # The open view followed the rename, so its write is recorded at
        # the name it then had. Its inode moved into the carrier: the view's
        # cache name is gone, so neither eviction nor a refetch reaches it.
        self.assertFalse(os.path.lexists(self.fs.cache_manager.get_cache_path("/e/f.txt.member")))
        self.reopen()
        self.assertEqual(expected, self.view_writes())
        self.reopen(rebuild=True)
        self.assertEqual(expected, self.view_writes())
        self.assertEqual(b"sole bytes", (self.member_dir / "d" / "f.txt").read_bytes())
        self.assertIn("member", self.fs.cache_manager.member_repairs.backends_for(["/e/f.txt"]))

    def test_concurrent_releases_of_one_view_keep_its_bytes_once(self):
        """Two descriptors of the same edited view, released at once after
        the repair appeared: one record, not two."""
        view, fh = self.open_edited_view()
        fh2 = self.fs.open("/d/f.txt.member", os.O_WRONLY)
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        workers = [Worker(self.fs.release, "/d/f.txt.member", h) for h in (fh, fh2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.finish()
        self.assertEqual([(("member",), "/e/f.txt", b"EDITEDytes")], self.view_writes())
        self.assertEqual(b"sole bytes", (self.member_dir / "d" / "f.txt").read_bytes())

    def test_two_descriptors_with_equal_size_writes_keep_the_last_bytes(self):
        """Two descriptors of the view write different bytes of the same
        size, and the mtime is reset: the record holds the bytes the view
        last had, which both descriptors share, not an earlier version
        (Codex C2 at c75d1a7)."""
        view, fh = self.open_edited_view()
        fh2 = self.fs.open("/d/f.txt.member", os.O_WRONLY)
        self.fs.write("/d/f.txt.member", b"SECOND", 0, fh2)
        self.assertEqual(0, self.fs.flush("/d/f.txt.member", fh2))
        os.utime(view, ns=(1, 1))
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        self.fs.release("/e/f.txt.member", fh)
        self.fs.release("/e/f.txt.member", fh2)
        self.assertEqual([(("member",), "/e/f.txt", b"SECONDytes")], self.view_writes())

    def test_a_view_write_that_cannot_be_kept_leaves_no_partial_record(self):
        """An injected failure while copying the bytes leaves no carrier
        (nothing half-written is listed later) and the member untouched."""
        view, fh = self.open_edited_view()
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        with patch("storage.member_repairs.os.link",
                   side_effect=OSError(errno.ENOSPC, "injected")):
            self.fs.release("/d/f.txt.member", fh)
        self.assertEqual([], self.view_writes())
        self.assertEqual([("member", "/d", "/e")], self.repairs())
        self.assertEqual(b"EDITEDytes",
                         self.fs.cache_manager.get_cache_path("/e/f.txt.member").read_bytes())
        self.assertEqual(b"sole bytes", (self.member_dir / "d" / "f.txt").read_bytes())

    def test_a_legal_deep_multibyte_rename_is_recorded_beyond_the_xattr_bound(self):
        """A rename whose two names serialize far past MAX_RECORD_BYTES is
        legal (each under PATH_MAX, each part under NAME_MAX), so it must
        work: its miss is kept in a version-2 carrier, through a reopen, a
        rebuilt index and a resolve (CTO-182-J-LENGTH-C3)."""
        part = "\u97f3\u697d" * 40                       # 240 bytes
        deep = "/" + "/".join([part] * 8)               # 1928 bytes
        self.fs.mkdir(deep.rsplit("/", 7)[0], 0o755)
        parent = deep.rsplit("/", 7)[0]
        for depth in range(2, 9):
            self.fs.mkdir("/" + "/".join([part] * depth), 0o755)
        self.fs.mkdir(deep + "/a", 0o755)
        self.fail_member_once("rename")
        self.fs.rename(deep + "/a", deep + "/b")
        self.assertGreater(len(MemberRepairs(self.cache_dir).list()[0].to_json()),
                           MAX_RECORD_BYTES)
        expected = [("member", deep + "/a", deep + "/b")]
        self.assertEqual(expected, self.repairs())
        self.reopen()
        self.assertEqual(expected, self.repairs())
        self.reopen(rebuild=True)
        self.assertEqual(expected, self.repairs())
        self.assertEqual({"member"},
                         self.fs.cache_manager.member_repairs.backends_for([deep + "/b/x"]))
        [record] = self.fs.cache_manager.member_repairs.list()
        self.assertTrue(self.fs.cache_manager.member_repairs.resolve(record.id))
        self.assertEqual([], self.repairs())
        self.assertTrue(parent)

    def test_a_still_open_view_descriptor_cannot_write_into_kept_bytes(self):
        """After release kept the view's inode, a second descriptor of the
        same inode is refused (EBUSY): nothing rewrites the kept bytes."""
        view, fh = self.open_edited_view()
        fh2 = self.fs.open("/d/f.txt.member", os.O_WRONLY)
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        self.fs.release("/e/f.txt.member", fh)
        with self.assertRaises(FuseOSError) as raised:
            self.fs.write("/e/f.txt.member", b"LATER!", 0, fh2)
        self.assertEqual(errno.EBUSY, raised.exception.errno)
        self.fs.release("/e/f.txt.member", fh2)
        self.assertEqual([(("member",), "/e/f.txt", b"EDITEDytes")], self.view_writes())

    def test_a_view_write_the_cache_cannot_record_keeps_its_name_and_bytes(self):
        """Injected ENOSPC on the link, then on the record's xattr: the
        narrow durability limit. The member keeps its original bytes, the
        view's name keeps exactly the edited bytes, nothing partial is
        left, and the log names errno, member, path and where to recover
        the bytes (CTO-182-J-LENGTH-C3)."""
        for target in ("storage.member_repairs.os.link", "storage.member_repairs.os.setxattr"):
            with self.subTest(injected=target):
                self.tearDown_view_case()
                view, fh = self.open_edited_view()
                self.fail_member_once("rename")
                self.fs.rename("/d", "/e")
                with patch(target, side_effect=OSError(errno.ENOSPC, "injected")), \
                        self.assertLogs("storage", "ERROR") as logged:
                    self.fs.release("/e/f.txt.member", fh)
                kept_view = self.fs.cache_manager.get_cache_path("/e/f.txt.member")
                self.assertEqual(b"EDITEDytes", kept_view.read_bytes())
                self.assertEqual(b"sole bytes", (self.member_dir / "d" / "f.txt").read_bytes())
                self.assertEqual([], self.view_writes())
                self.assertEqual([], [n for n in os.listdir(self.cache_dir / ".storage.repairs")
                                      if n.endswith((".bytes", ".new"))])
                message = "\n".join(logged.output)
                for needle in ("ENOSPC", "on member member", "/e/f.txt", str(kept_view)):
                    self.assertIn(needle, message)

    def tearDown_view_case(self):
        """Reset the namespace between subtests of the same fixture."""
        for repair in self.fs.cache_manager.member_repairs.list():
            self.fs.cache_manager.member_repairs.resolve(repair.id)
        for name in ("d", "e"):
            for root in (self.member_dir, self.fs.cache_manager.get_cache_path("/")):
                target = Path(root) / name
                if target.exists():
                    shutil.rmtree(target)
        self.fs.state_manager.delete_file("/d/f.txt")
        self.fs.state_manager.delete_file("/e/f.txt")

    def test_a_surviving_alias_of_kept_bytes_cannot_change_them(self):
        """The view's cache name could not be unlinked after its bytes were
        kept, so it still links to them: a truncate through the view is
        refused, a read sees the kept bytes, a refetch does not touch them,
        and the next open of the cache removes the alias (CTO-182-DELTA)."""
        view, fh = self.open_edited_view()
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        alias = self.fs.cache_manager.get_cache_path("/e/f.txt.member")
        with patch("storage.member_repairs.os.unlink", side_effect=OSError(errno.EIO, "x")):
            self.fs.release("/e/f.txt.member", fh)
        [(members, path, kept)] = self.view_writes()
        self.assertEqual(b"EDITEDytes", kept)
        self.assertTrue(alias.exists())
        with self.assertRaises(FuseOSError) as raised:
            self.fs.truncate("/e/f.txt.member", 0)
        self.assertEqual(errno.EBUSY, raised.exception.errno)
        rh = self.fs.open("/e/f.txt.member", os.O_RDONLY)
        self.assertEqual(b"EDITEDytes", self.fs.read("/e/f.txt.member", 64, 0, rh))
        self.fs.release("/e/f.txt.member", rh)
        self.assertEqual([(("member",), "/e/f.txt", b"EDITEDytes")], self.view_writes())
        self.reopen()
        self.assertFalse(os.path.lexists(alias))
        self.assertEqual([(("member",), "/e/f.txt", b"EDITEDytes")], self.view_writes())
        self.assertEqual(b"sole bytes", (self.member_dir / "d" / "f.txt").read_bytes())

    def test_a_moved_alias_outside_every_fence_still_cannot_be_written(self):
        """A surviving alias is moved by a directory rename, and the rename
        repairs are resolved: no fence covers the alias's name now, but a
        write open of it is still refused, since its file is kept bytes."""
        view, fh = self.open_edited_view()
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        with patch("storage.member_repairs.os.unlink", side_effect=OSError(errno.EIO, "x")):
            self.fs.release("/e/f.txt.member", fh)
        self.fs.rename("/e", "/k")
        repairs = self.fs.cache_manager.member_repairs
        for record in repairs.list():
            if record.kind != VIEW_WRITE:
                self.assertTrue(repairs.resolve(record.id))
        self.assertNotIn("member", repairs.backends_for(["/k/f.txt"]))
        with self.assertRaises(FuseOSError) as raised:
            self.fs.open("/k/f.txt.member", os.O_WRONLY)
        self.assertEqual(errno.EBUSY, raised.exception.errno)
        with self.assertRaises(FuseOSError):
            self.fs.truncate("/k/f.txt.member", 0)
        self.assertEqual([(("member",), "/e/f.txt", b"EDITEDytes")], self.view_writes())

    def kept_with_surviving_alias(self):
        """A view-write kept while its view name could not be unlinked:
        returns (record id, alias path)."""
        view, fh = self.open_edited_view()
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        with patch("storage.member_repairs.os.unlink", side_effect=OSError(errno.EIO, "x")):
            self.fs.release("/e/f.txt.member", fh)
        [kept] = [r.id for r in self.fs.cache_manager.member_repairs.list() if r.kind == VIEW_WRITE]
        return kept, self.fs.cache_manager.get_cache_path("/e/f.txt.member")

    def test_an_alias_of_missing_kept_bytes_survives_reopen(self):
        """The payload is gone but the alias still has the bytes: reopening
        never unlinks it (it may be the only copy), and the record stays
        listed as missing, with its fence (CTO-182-REOPEN; OpenAI C1 at
        fddd4f1)."""
        kept, alias = self.kept_with_surviving_alias()
        (self.cache_dir / ".storage.repairs" / (kept + ".bytes")).unlink()
        self.reopen()
        self.assertEqual(b"EDITEDytes", alias.read_bytes())
        repairs = self.fs.cache_manager.member_repairs
        self.assertTrue(repairs.bytes_missing(kept))
        self.assertIn("member", repairs.backends_for(["/e/f.txt"]))
        self.assertEqual([kept], [r.id for r in repairs.list() if r.kind == VIEW_WRITE])

    def test_an_unrelated_file_with_a_reused_inode_number_survives_reopen(self):
        """The payload and alias are gone and an unrelated file now holds the
        view name with the inode number the record names: it is neither
        unlinked nor taken for the lost bytes (CTO-182-REOPEN)."""
        kept, alias = self.kept_with_surviving_alias()
        carriers = self.cache_dir / ".storage.repairs"
        (carriers / (kept + ".bytes")).unlink()
        alias.unlink()
        alias.write_bytes(b"unrelated replacement")
        st = os.lstat(alias)
        record = json.loads((carriers / kept).read_bytes())
        record["source"] = f"{st.st_dev}:{st.st_ino}"       # as if the number were reused
        (carriers / kept).write_bytes(json.dumps(record).encode())
        self.reopen()
        self.assertEqual(b"unrelated replacement", alias.read_bytes())
        repairs = self.fs.cache_manager.member_repairs
        self.assertEqual([kept], [r.id for r in repairs.list() if r.kind == VIEW_WRITE])
        self.assertTrue(repairs.bytes_missing(kept))

    def test_recovery_keeps_only_positive_views_of_the_fenced_member(self):
        """With members "b.a" and "a" configured in that order, /x.b.a is
        b.a's view, not a's; with "gone" no longer configured, /z/report.gone
        is an ordinary file. Neither is kept or unlinked for a repair that
        fences "a" or "gone"; a's own view is (CTO-182-REOPEN)."""
        repairs = self.fs.cache_manager.member_repairs
        repairs.record("/x", "/y", ["a"], "fences a")
        repairs.record("/z", "/w", ["gone"], "fences a removed member")
        cache = self.cache_dir
        (cache / "x.b.a").write_bytes(b"b.a's view")
        (cache / "x.a").write_bytes(b"a's view")
        (cache / "z").mkdir()
        (cache / "z" / "report.gone").write_bytes(b"an ordinary file")
        self.fs.cache_manager.recover_views(["b.a", "a"])
        self.assertEqual(b"b.a's view", (cache / "x.b.a").read_bytes())
        self.assertEqual(b"an ordinary file", (cache / "z" / "report.gone").read_bytes())
        self.assertFalse((cache / "x.a").exists())
        self.assertEqual([(("a",), "/x", b"a's view")], self.view_writes())

    def test_recovery_never_leaves_the_cache_through_a_symlinked_ancestor(self):
        """An old path whose cache ancestor is now a symlink out of the cache:
        nothing there is walked, kept or unlinked (CTO-182-REOPEN)."""
        outside = self.temp_dir / "outside" / "t"
        outside.mkdir(parents=True)
        (outside / "f.member").write_bytes(b"outside the cache")
        (outside.parent / "t.member").write_bytes(b"also outside")
        os.symlink(outside.parent, self.cache_dir / "s")
        self.fs.cache_manager.member_repairs.record("/s/t", "/u", ["member"], "fenced")
        self.fs.cache_manager.recover_views(["member"])
        self.assertEqual(b"outside the cache", (outside / "f.member").read_bytes())
        self.assertEqual(b"also outside", (outside.parent / "t.member").read_bytes())
        self.assertEqual([], self.view_writes())

    def test_a_descriptor_moved_out_of_every_fence_cannot_write_kept_bytes(self):
        """Two descriptors of a view; the first release keeps the inode, a
        directory rename re-points the second outside the view-write's
        fence, and the rename's own repair is resolved: the second still
        cannot write, since its file is kept bytes (fstat, not its name)."""
        view, fh = self.open_edited_view()
        fh2 = self.fs.open("/d/f.txt.member", os.O_WRONLY)
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        self.fs.release("/e/f.txt.member", fh)
        self.fail_member_once("rename")
        self.fs.rename("/e", "/k")
        repairs = self.fs.cache_manager.member_repairs
        for record in repairs.list():
            if record.kind != VIEW_WRITE:
                self.assertTrue(repairs.resolve(record.id))
        with self.assertRaises(FuseOSError) as raised:
            self.fs.write("/k/f.txt.member", b"LATER!", 0, fh2)
        self.assertEqual(errno.EBUSY, raised.exception.errno)
        self.fs.release("/k/f.txt.member", fh2)
        self.assertEqual([(("member",), "/e/f.txt", b"EDITEDytes")], self.view_writes())

    def test_an_alias_read_as_an_ordinary_file_cannot_change_kept_bytes(self):
        """A kept alias survives, and the mount restarts with that member no
        longer configured, so recovery leaves the alias in place and its name
        now reads as an ordinary file. A write open, a create and a truncate
        of it are refused; reading it, and writing an unrelated file, work
        (CTO-182-IDENTITY)."""
        kept, alias = self.kept_with_surviving_alias()
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        self.fs = StorageFS(
            cache_root=self.cache_dir, state_db=self.temp_dir / "state.db",
            backends={"other": self.member}, cache_size=10 * 1024 * 1024,
            num_sync_workers=1, start_sync=False,
        )
        self.assertTrue(alias.exists())               # left in place: not other's view
        for attempt in (lambda: self.fs.open("/e/f.txt.member", os.O_WRONLY),
                        lambda: self.fs.create("/e/f.txt.member", 0o644),
                        lambda: self.fs.truncate("/e/f.txt.member", 0)):
            with self.assertRaises(FuseOSError) as raised:
                attempt()
            self.assertEqual(errno.EBUSY, raised.exception.errno)
        rh = self.fs.open("/e/f.txt.member", os.O_RDONLY)
        self.assertEqual(b"EDITEDytes", self.fs.read("/e/f.txt.member", 64, 0, rh))
        self.fs.release("/e/f.txt.member", rh)
        self.write_file("/e/notes.txt", b"ordinary write")
        self.assertEqual(b"ordinary write",
                         self.fs.cache_manager.get_cache_path("/e/notes.txt").read_bytes())
        self.assertEqual(b"EDITEDytes",
                         MemberRepairs(self.cache_dir).carrier_path(kept).read_bytes())

    def test_a_crash_between_flush_and_release_still_keeps_the_view(self):
        """flush succeeded, a missed rename fenced the member, and the
        process died before the release: the next open of the cache keeps
        the view's bytes as a view-write repair, never sent to the member."""
        view, fh = self.open_edited_view()
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        self.reopen()          # no release: the descriptor dies with the process
        self.assertEqual([(("member",), "/e/f.txt", b"EDITEDytes")], self.view_writes())
        self.assertFalse(os.path.lexists(self.fs.cache_manager.get_cache_path("/e/f.txt.member")))
        self.assertEqual(b"sole bytes", (self.member_dir / "d" / "f.txt").read_bytes())
        self.reopen(rebuild=True)
        self.assertEqual([(("member",), "/e/f.txt", b"EDITEDytes")], self.view_writes())

    def offline_rename_of_an_uncached_directory(self):
        """/d/f.txt's only bytes are on the member, the file is not cached,
        and the member is offline when /d becomes /e."""
        self.fs.mkdir("/d", 0o755)
        self.write_file("/d/f.txt", b"sole bytes")
        self.start_sync()
        self.wait_synced("/d/f.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        cache_file = self.fs.cache_manager.get_cache_path("/d/f.txt")
        self.assertGreater(self.fs.cache_manager._evict_if_still_clean("/d/f.txt", cache_file), 0)
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        self.assertFalse(os.path.lexists(self.fs.cache_manager.get_cache_path("/e/f.txt")))
        self.assertEqual(b"sole bytes", (self.member_dir / "d" / "f.txt").read_bytes())

    def assert_both_endpoints_fenced(self):
        repairs = self.fs.cache_manager.member_repairs
        self.assertEqual([("member", "/d", "/e")], self.repairs())
        for path in ("/d", "/d/f.txt", "/e", "/e/f.txt", "/e/new.txt"):
            self.assertEqual({"member"}, repairs.backends_for([path]), path)
        self.assertEqual(set(), repairs.backends_for(["/dx", "/elsewhere"]))
        # Behaviorally: no member rename, mkdir or upload at those names.
        calls = []
        for name in ("rename", "mkdir", "rmdir", "copy_from_local"):
            patch.object(self.member, name,
                         side_effect=lambda *a, name=name: calls.append((name, a))).start()
        self.addCleanup(patch.stopall)
        self.fs.mkdir("/e/sub", 0o755)
        self.write_file("/e/new.txt", b"new")
        self.fs.rename("/e", "/d")
        self.start_sync()
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.assertEqual([], calls)
        self.assertEqual(["d"], self.member_names())
        self.assertEqual(b"sole bytes", (self.member_dir / "d" / "f.txt").read_bytes())

    def test_offline_member_sole_bytes_survive_a_discarded_index(self):
        self.offline_rename_of_an_uncached_directory()
        self.reopen()
        self.assert_both_endpoints_fenced()

    def test_offline_member_sole_bytes_survive_a_rebuilt_index(self):
        self.offline_rename_of_an_uncached_directory()
        self.reopen(rebuild=True)
        self.assert_both_endpoints_fenced()

    def test_cached_file_under_a_repair_stays_pinned_after_a_rebuilt_index(self):
        self.write_file("/a.txt", b"only cached copy at the new name")
        self.start_sync()
        self.wait_synced("/a.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.fail_member_once("rename")
        self.fs.rename("/a.txt", "/b.txt")
        self.reopen(rebuild=True)
        cache_path = self.fs.cache_manager.get_cache_path("/b.txt")
        self.assertEqual(SyncStatus.CLEAN, self.fs.state_manager.get_file_metadata("/b.txt").sync_status)
        self.assertFalse(self.fs.cache_manager.evict_lru_files(1 << 30))
        self.assertEqual(0, self.fs.cache_manager._evict_if_still_clean("/b.txt", cache_path))
        self.assertTrue(cache_path.exists())

    def test_resolution_is_by_carrier_id_and_replay_safe_across_a_rebuild(self):
        self.offline_rename_of_an_uncached_directory()
        [before] = self.fs.cache_manager.member_repairs.list()
        self.reopen(rebuild=True)
        repairs = self.fs.cache_manager.member_repairs
        self.assertEqual([before.id], [r.id for r in repairs.list()])
        self.assertFalse(repairs.resolve("0" * 32))
        self.assertTrue(repairs.resolve(before.id))
        self.assertFalse(repairs.resolve(before.id))
        self.assertEqual([], self.repairs())
        self.assertEqual(set(), repairs.backends_for(["/d", "/e"]))
        self.reopen()
        self.assertEqual([], self.fs.cache_manager.member_repairs.list())

    def test_a_resolution_by_another_process_reaches_the_open_cache(self):
        self.offline_rename_of_an_uncached_directory()
        [record] = self.fs.cache_manager.member_repairs.list()
        self.assertTrue(MemberRepairs(self.cache_dir).resolve(record.id))
        self.assertEqual(set(), self.fs.cache_manager.member_repairs.backends_for(["/d"]))

    def test_crash_between_member_call_and_settle_leaves_an_intent(self):
        ctx = multiprocessing.get_context("fork")
        child = ctx.Process(target=_die_after_member_rename, args=(self.temp_dir,))
        child.start()
        child.join(WAIT)
        if child.exitcode is None:
            child.kill()
            child.join()
            self.fail("the child neither renamed nor exited")
        self.assertEqual(0, child.exitcode)
        [record] = MemberRepairs(self.temp_dir / "crash2" / "cache").list()
        self.assertEqual(("intent", ("member",), "/d", "/e"),
                         (record.state, record.members, record.old_path, record.new_path))

    def test_a_corrupt_carrier_stops_the_cache_from_opening(self):
        self.fs.mkdir("/d", 0o755)
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        [record] = self.fs.cache_manager.member_repairs.list()
        os.setxattr(str(self.cache_dir / ".storage.repairs" / record.id),
                    "user.storage.member_repair", b"{not json")
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        with self.assertRaises(CorruptRepairRecord):
            StorageFS(cache_root=self.cache_dir, state_db=self.temp_dir / "state2.db",
                      backends={"member": self.member}, cache_size=10 * 1024 * 1024,
                      num_sync_workers=1, start_sync=False)
        self.fs.state_manager = StateManager(str(self.temp_dir / "state.db"))

    def test_an_unfinished_new_carrier_is_removed_when_the_cache_opens(self):
        carriers = self.cache_dir / ".storage.repairs"
        carriers.mkdir()
        (carriers / ("a" * 32 + ".new")).touch()
        self.reopen()
        self.assertEqual([], os.listdir(carriers))

    def test_the_carriers_are_not_part_of_the_namespace(self):
        self.fs.mkdir("/d", 0o755)
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        [record] = self.fs.cache_manager.member_repairs.list()
        self.assertNotIn(".storage.repairs", self.fs.readdir("/", None))
        for op, args in (
            ("getattr", ("/.storage.repairs",)),
            ("getattr", (f"/.storage.repairs/{record.id}",)),
            ("unlink", (f"/.storage.repairs/{record.id}",)),
            ("rmdir", ("/.storage.repairs",)),
            ("rename", ("/e", "/.storage.repairs/x")),
            ("setxattr", (f"/.storage.repairs/{record.id}", "user.storage.member_repair", b"{}", 0)),
            ("removexattr", (f"/.storage.repairs/{record.id}", "user.storage.member_repair")),
            ("chmod", (f"/.storage.repairs/{record.id}", 0o644)),
            ("chown", (f"/.storage.repairs/{record.id}", os.getuid(), os.getgid())),
        ):
            with self.assertRaises(FuseOSError, msg=op) as raised:
                getattr(self.fs, op)(*args)
            self.assertEqual(errno.ENOENT, raised.exception.errno, op)
        self.assertEqual([record.id], [r.id for r in MemberRepairs(self.cache_dir).list()])
        self.reopen(rebuild=True)
        self.assertIsNone(self.fs.state_manager.get_file_metadata(f"/.storage.repairs/{record.id}"))

    def test_a_resolution_during_a_rename_is_not_hidden_by_the_rename_s_own_write(self):
        """Another process resolves repair R while a rename is in its member
        call; the rename's own settle must not stamp over that removal."""
        self.fs.mkdir("/old", 0o755)
        self.fail_member_once("rename")
        self.fs.rename("/old", "/older")
        [earlier] = self.fs.cache_manager.member_repairs.list()
        self.fs.mkdir("/d", 0o755)
        real = self.member.rename

        def resolve_elsewhere_then_rename(*args):
            self.assertTrue(MemberRepairs(self.cache_dir).resolve(earlier.id))
            return real(*args)

        with self.patch_member_rename(side_effect=resolve_elsewhere_then_rename):
            self.fs.rename("/d", "/e")
        self.assertEqual([], self.fs.cache_manager.member_repairs.list())
        self.assertEqual(set(), self.fs.cache_manager.member_repairs.backends_for(["/old", "/older"]))

    def test_an_intent_cannot_be_resolved_while_its_rename_runs(self):
        """Someone tries to resolve the in-flight intent from another
        process during the member call: the rename holds the intent's flock,
        so the resolve fails as busy and changes nothing; the member then
        misses the move, and the miss is recorded."""
        self.fs.mkdir("/d", 0o755)
        attempts = []

        def try_resolve_then_fail(*_args):
            [intent] = MemberRepairs(self.cache_dir).list()
            self.assertEqual("intent", intent.state)
            with patch("storage.member_repairs.FLOCK_WAIT", 0.05):
                with self.assertRaises(OSError) as raised:
                    MemberRepairs(self.cache_dir).resolve(intent.id)
            attempts.append(raised.exception.errno)
            # A lookup reloads meanwhile; settle does not depend on it.
            self.assertEqual({"member"}, self.fs.cache_manager.member_repairs.backends_for(["/e"]))
            raise OSError("member unreachable")

        with self.patch_member_rename(side_effect=try_resolve_then_fail):
            self.fs.rename("/d", "/e")
        self.assertEqual([errno.EWOULDBLOCK], attempts)
        [record] = MemberRepairs(self.cache_dir).list()
        self.assertEqual(("open", ("member",), "/d", "/e"),
                         (record.state, record.members, record.old_path, record.new_path))
        self.assertEqual({"member"}, self.fs.cache_manager.member_repairs.backends_for(["/e"]))

    def test_an_intent_left_by_a_failed_move_is_released_and_resolvable(self):
        """The rename fails after its local move (here: the index rewrite):
        the intent stays for every target, and its flock is released, so it
        can be resolved."""
        self.fs.mkdir("/d", 0o755)
        with patch.object(self.fs.state_manager, "rename_path", side_effect=OSError(errno.EIO, "injected")):
            with self.assertRaises(FuseOSError):
                self.fs.rename("/d", "/e")
        [record] = MemberRepairs(self.cache_dir).list()
        self.assertEqual(("intent", ("member",)), (record.state, record.members))
        with patch("storage.member_repairs.FLOCK_WAIT", 0.05):
            self.assertTrue(MemberRepairs(self.cache_dir).resolve(record.id))

    def test_an_interrupted_resolution_still_fences_until_the_cache_finishes_it(self):
        """A resolution that wrote its durable resolved state but did not get
        to unlink the carrier: open caches still enforce the carrier, and
        the next open by the cache's owner finishes the resolution."""
        self.offline_rename_of_an_uncached_directory()
        [record] = self.fs.cache_manager.member_repairs.list()
        carriers = self.cache_dir / ".storage.repairs"
        real_unlink = Path.unlink

        def refusing(path, *args, **kwargs):
            if path.parent == carriers:
                raise OSError(errno.EIO, "injected")
            return real_unlink(path, *args, **kwargs)

        with patch.object(Path, "unlink", refusing):
            with self.assertRaises(OSError):
                MemberRepairs(self.cache_dir).resolve(record.id)
        [still] = MemberRepairs(self.cache_dir).list()
        self.assertEqual("resolved", still.state)
        self.assertEqual({"member"}, self.fs.cache_manager.member_repairs.backends_for(["/d"]))
        self.reopen()
        self.assertEqual([], os.listdir(carriers))
        self.assertEqual(set(), self.fs.cache_manager.member_repairs.backends_for(["/d"]))

    def test_an_interruption_right_after_the_cache_rename_keeps_the_intent(self):
        """An exception (here KeyboardInterrupt) after the cache rename took
        effect but before the rename noted it: the intent stays."""
        self.fs.mkdir("/d", 0o755)
        real_rename = Path.rename

        def rename_then_interrupt(path, target):
            result = real_rename(path, target)
            if Path(target) == self.cache_dir / "e":
                raise KeyboardInterrupt
            return result

        with patch.object(Path, "rename", rename_then_interrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.fs.rename("/d", "/e")
        self.assertTrue((self.cache_dir / "e").is_dir())
        [record] = MemberRepairs(self.cache_dir).list()
        self.assertEqual(("intent", ("member",)), (record.state, record.members))

    def test_a_refused_cache_rename_drops_its_intent_an_io_error_keeps_it(self):
        """rename(2) refusing (EACCES) has no effect: nothing moved, no
        member called, no intent left. An I/O error leaves the outcome
        unknown: the intent stays."""
        real_rename = Path.rename
        for err, kept in ((errno.EACCES, False), (errno.EIO, True)):
            self.fs.mkdir("/d", 0o755)

            def failing(path, target, err=err):
                if Path(target) == self.cache_dir / "e":
                    raise OSError(err, os.strerror(err))
                return real_rename(path, target)

            with patch.object(Path, "rename", failing):
                with self.assertRaises(FuseOSError):
                    self.fs.rename("/d", "/e")
            self.assertTrue((self.cache_dir / "d").is_dir())
            records = MemberRepairs(self.cache_dir).list()
            self.assertEqual(kept, [("intent", ("member",))] == [(r.state, r.members) for r in records],
                             err)
            for record in records:
                self.assertTrue(MemberRepairs(self.cache_dir).resolve(record.id))
            self.fs.rmdir("/d")

    def test_the_local_move_is_made_durable_before_any_member_call(self):
        """If the cache directories cannot be fsynced after the local move,
        no member is called and the intent stays."""
        self.fs.mkdir("/d", 0o755)
        renames = []
        self.patch_member_rename(side_effect=lambda *a: renames.append(a)).start()
        self.addCleanup(patch.stopall)
        with patch("storage.namespace.fsync_directories", side_effect=OSError(errno.EIO, "injected")):
            with self.assertRaises(FuseOSError):
                self.fs.rename("/d", "/e")
        self.assertEqual([], renames)
        [record] = MemberRepairs(self.cache_dir).list()
        self.assertEqual(("intent", ("member",)), (record.state, record.members))

    def test_directories_the_move_creates_are_made_durable_before_any_member_call(self):
        """Moving /d to /x/y/d makes /x and /x/y in the cache: each is
        fsynced in its own parent before the member is called."""
        self.fs.mkdir("/d", 0o755)
        self.fs.mkdir("/x", 0o755)
        self.fs.mkdir("/x/y", 0o755)
        import shutil as _shutil
        _shutil.rmtree(self.cache_dir / "x")  # as if never cached
        synced, calls = [], []
        real = storage_namespace.fsync_directories

        def recording(*paths):
            synced.extend(str(p) for p in paths)
            return real(*paths)

        real_rename = self.member.rename
        self.patch_member_rename(side_effect=lambda *a: (calls.append(list(synced)), real_rename(*a))).start()
        self.addCleanup(patch.stopall)
        with patch("storage.namespace.fsync_directories", side_effect=recording):
            self.fs.rename("/d", "/x/y/d")
        [before_member] = calls
        for directory in (self.cache_dir, self.cache_dir / "x", self.cache_dir / "x" / "y"):
            self.assertIn(str(directory), before_member)


class TestCacheAncestors(unittest.TestCase):
    def test_chain_from_the_root(self):
        root = Path("/c")
        self.assertEqual([root], storage_namespace._cache_ancestors(root, root))
        self.assertEqual([root, root / "x", root / "x" / "y"],
                         storage_namespace._cache_ancestors(root, root / "x" / "y"))

    def test_a_path_not_spelled_under_the_root_gets_every_ancestor(self):
        self.assertEqual([Path("/"), Path("/other"), Path("/other/x")],
                         storage_namespace._cache_ancestors(Path("/c"), Path("/other/x")))


class TestFirstMemberRenameTarget(SyncRaceTestBase):
    """The mount's own member rename onto a name the namespace did not hold
    (docs/first-member-rename-target.md, ditz gh-181-sync-rename-noreplace).

    The first two cases were the synthetic RED of the design: another
    writer puts an unrelated object at the target in the window between the
    namespace's decision and the member call, or before it, unlisted. They
    pass now that the member rename refuses rather than replace.
    """

    def occupy_target_first(self, rel, data):
        """Another writer puts an object at rel just before the member call,
        whichever rename the namespace makes."""
        real_rename, real_noreplace = self.member.rename, self.member.rename_noreplace

        def racing(real):
            def call(old, new):
                (self.member_dir / rel).write_bytes(data)
                return real(old, new)
            return call

        patcher = patch.multiple(self.member, rename=Mock(side_effect=racing(real_rename)),
                                 rename_noreplace=Mock(side_effect=racing(real_noreplace)))
        patcher.start()
        self.addCleanup(patcher.stop)

    def synced(self, path, data):
        self.write_file(path, data)
        self.start_sync()
        self.wait_synced(path)
        self.wait_idle()
        self.fs.sync_manager.stop()

    def test_known_absent_target_created_during_the_rename_is_kept(self):
        """The namespace had no /b.txt and the member's fresh listing had
        none; another writer creates one during the rename."""
        self.synced("/a.txt", b"renamed bytes")
        self.assertEqual(["a.txt"], sorted(self.fs.readdir("/", None))[2:])
        self.occupy_target_first("b.txt", b"unrelated writer")
        self.fs.rename("/a.txt", "/b.txt")
        self.assertEqual(b"unrelated writer", (self.member_dir / "b.txt").read_bytes())
        self.assertEqual(b"renamed bytes", (self.member_dir / "a.txt").read_bytes())

    def test_unknown_target_the_mount_never_listed_is_kept(self):
        """The member already had a /b.txt the mount never listed (no
        listing of / since it appeared); the namespace has none."""
        self.synced("/a.txt", b"renamed bytes")
        (self.member_dir / "b.txt").write_bytes(b"unrelated writer")
        self.fs.rename("/a.txt", "/b.txt")
        self.assertEqual(b"unrelated writer", (self.member_dir / "b.txt").read_bytes())
        self.assertEqual(b"renamed bytes", (self.member_dir / "a.txt").read_bytes())

    def test_occupied_targets_are_kept_and_recorded(self):
        """Both REDs above also record the miss: a repair naming both paths,
        and the namespace's own move stands."""
        self.synced("/a.txt", b"renamed bytes")
        (self.member_dir / "b.txt").write_bytes(b"unrelated writer")
        self.fs.rename("/a.txt", "/b.txt")
        self.assertEqual([("member", "/a.txt", "/b.txt")], self.repairs())
        self.assertTrue(self.fs.cache_manager.get_cache_path("/b.txt").exists())

    def test_a_directory_onto_an_occupied_member_directory_keeps_both(self):
        """The member holds an unrelated /e with a file in it; /d -> /e is
        refused there (renameat2 EEXIST) and both directories stay."""
        self.fs.mkdir("/d", 0o755)
        (self.member_dir / "d" / "mine.txt").write_bytes(b"mine")
        (self.member_dir / "e").mkdir()
        (self.member_dir / "e" / "theirs.txt").write_bytes(b"theirs")
        self.fs.rename("/d", "/e")
        self.assertEqual(["mine.txt"], self.member_names("d"))
        self.assertEqual(["theirs.txt"], self.member_names("e"))
        self.assertEqual([("member", "/d", "/e")], self.repairs())

    def test_a_free_target_moves_with_the_no_replace_rename(self):
        """A free name on a filesystem with RENAME_NOREPLACE: moved, no repair."""
        self.synced("/a.txt", b"renamed bytes")
        self.fs.mkdir("/d", 0o755)
        noreplace, replacing = [], []
        real_noreplace, real_rename = self.member.rename_noreplace, self.member.rename
        with patch.multiple(
                self.member,
                rename_noreplace=Mock(side_effect=lambda *a: (noreplace.append(a), real_noreplace(*a))),
                rename=Mock(side_effect=lambda *a: (replacing.append(a), real_rename(*a)))):
            self.fs.rename("/a.txt", "/b.txt")
            self.fs.rename("/d", "/e")
        self.assertEqual([("/a.txt", "/b.txt"), ("/d", "/e")], noreplace)
        self.assertEqual([], replacing)
        self.assertEqual(["b.txt", "e"], self.member_names())
        self.assertEqual([], self.repairs())

    def test_without_a_no_replace_rename_the_member_is_skipped(self):
        """A filesystem or libc without RENAME_NOREPLACE: no stat-then-rename
        or link-then-unlink fallback. The member keeps the source, gets no
        target, and the miss is recorded; the namespace move stands."""
        self.synced("/a.txt", b"renamed bytes")
        self.fs.mkdir("/d", 0o755)
        for unsupported in (None, errno.EINVAL):
            with self.subTest(unsupported=unsupported):
                if unsupported is None:
                    ctx = patch("storage.backends.local._RENAMEAT2", None)
                else:
                    ctx = patch("storage.backends.local.rename_noreplace",
                                side_effect=OSError(errno.EINVAL, "not here"))
                with ctx:
                    old, new = ("/a.txt", "/b.txt") if unsupported is None else ("/d", "/e")
                    self.fs.rename(old, new)
                self.assertTrue((self.member_dir / old.lstrip("/")).exists())
                self.assertFalse((self.member_dir / new.lstrip("/")).exists())
                self.assertTrue(self.fs.cache_manager.get_cache_path(new).exists())
        self.assertEqual([("member", "/a.txt", "/b.txt"), ("member", "/d", "/e")], self.repairs())
        [first, second] = sorted(MemberRepairs(self.cache_dir).list(), key=lambda r: r.old_path)
        self.assertIn("no-replace rename unsupported", first.reason)
        self.assertIn("no-replace rename unsupported", second.reason)

    def test_an_intended_replacement_whose_source_vanished_keeps_the_target(self):
        """/a.txt and /b.txt are both recorded on the member, but the member's
        /a.txt was removed behind the mount: renaming /a.txt over /b.txt moves
        nothing there, so it is a miss, never a success. The member's /b.txt
        keeps its bytes and an open repair remains (CTO-183-SOURCE-OUTCOME)."""
        self.synced("/a.txt", b"renamed bytes")
        self.synced("/b.txt", b"replaced bytes")
        (self.member_dir / "a.txt").unlink()
        self.fs.rename("/a.txt", "/b.txt")
        self.assertEqual(b"replaced bytes", (self.member_dir / "b.txt").read_bytes())
        [record] = MemberRepairs(self.cache_dir).list()
        self.assertEqual(("open", ("member",), "/a.txt", "/b.txt"),
                         (record.state, record.members, record.old_path, record.new_path))
        self.assertIn("no source", record.reason)

    def test_an_enoent_with_the_source_present_keeps_its_own_cause(self):
        """An ENOENT from the member's rename call is not proof the source
        vanished (a destination parent removed meanwhile, say): the miss is
        recorded with that error, not as a missing source."""
        self.synced("/a.txt", b"renamed bytes")
        self.synced("/b.txt", b"replaced bytes")
        with patch("storage.backends.local._replace",
                   side_effect=FileNotFoundError(errno.ENOENT, "parent went away")):
            self.fs.rename("/a.txt", "/b.txt")
        [record] = MemberRepairs(self.cache_dir).list()
        self.assertNotIn("no source", record.reason)
        self.assertIn("parent went away", record.reason)
        self.assertEqual(b"renamed bytes", (self.member_dir / "a.txt").read_bytes())
        self.assertEqual(b"replaced bytes", (self.member_dir / "b.txt").read_bytes())

    def test_a_source_that_cannot_be_inspected_is_not_called_missing(self):
        """The member's lstat of the source fails with EACCES or EIO: that is
        no evidence the source is gone. The rename is a miss carrying that
        error, not "no source", and both objects stay (CTO-183-ERRNO)."""
        self.synced("/a.txt", b"renamed bytes")
        self.synced("/b.txt", b"replaced bytes")
        self.synced("/c.txt", b"free target source")
        for err, old, new in ((errno.EACCES, "/a.txt", "/b.txt"), (errno.EIO, "/c.txt", "/d.txt")):
            with self.subTest(errno=err), patch(
                    "storage.backends.local._lstat",
                    side_effect=OSError(err, os.strerror(err))):
                self.fs.rename(old, new)
        reasons = {r.old_path: r.reason for r in MemberRepairs(self.cache_dir).list()}
        self.assertEqual({"/a.txt", "/c.txt"}, set(reasons))
        for old, text in (("/a.txt", os.strerror(errno.EACCES)), ("/c.txt", os.strerror(errno.EIO))):
            self.assertNotIn("no source", reasons[old])
            self.assertIn(text, reasons[old])
        self.assertEqual(b"renamed bytes", (self.member_dir / "a.txt").read_bytes())
        self.assertEqual(b"replaced bytes", (self.member_dir / "b.txt").read_bytes())
        self.assertEqual(b"free target source", (self.member_dir / "c.txt").read_bytes())
        self.assertFalse((self.member_dir / "d.txt").exists())

    def test_a_dangling_link_is_a_present_source(self):
        """A link whose target is gone is still a source: it is renamed."""
        self.synced("/a.txt", b"x")
        (self.member_dir / "a.txt").unlink()
        os.symlink("nowhere", self.member_dir / "a.txt")
        self.fs.rename("/a.txt", "/b.txt")
        self.assertTrue(os.path.islink(self.member_dir / "b.txt"))
        self.assertEqual([], self.repairs())

    def test_a_dangling_link_replaces_a_recorded_target(self):
        """The replacing path also counts a dangling link as a present source."""
        self.synced("/a.txt", b"x")
        self.synced("/b.txt", b"replaced bytes")
        (self.member_dir / "a.txt").unlink()
        os.symlink("nowhere", self.member_dir / "a.txt")
        self.fs.rename("/a.txt", "/b.txt")
        self.assertTrue(os.path.islink(self.member_dir / "b.txt"))
        self.assertEqual([], self.repairs())

    def test_a_noted_cause_reaches_the_repair_reason(self):
        """Notes on a member's error (a failed source check) are kept in the
        durable repair, not only on the exception."""
        self.synced("/a.txt", b"renamed bytes")
        self.synced("/b.txt", b"replaced bytes")
        failure = FileNotFoundError(errno.ENOENT, "parent gone")
        failure.add_note("source check failed: PermissionError(13, 'denied')")
        with patch("storage.backends.local._replace", side_effect=failure):
            self.fs.rename("/a.txt", "/b.txt")
        [record] = MemberRepairs(self.cache_dir).list()
        self.assertIn("source check failed", record.reason)
        self.assertNotIn("no source at the old name", record.reason)

    def test_known_intended_replacement_still_replaces(self):
        """Control: /b.txt is the namespace's own, synced to the member;
        renaming /a.txt over it replaces it there, as rename(2) does."""
        self.synced("/a.txt", b"renamed bytes")
        self.synced("/b.txt", b"replaced bytes")
        self.fs.rename("/a.txt", "/b.txt")
        self.assertEqual(b"renamed bytes", (self.member_dir / "b.txt").read_bytes())
        self.assertFalse((self.member_dir / "a.txt").exists())
