"""Sync uploads racing renames and unlinks through the mount (ditz sync-namespace-orphans).

A real sync worker uploads while a FUSE handler runs in another thread; an
Event gate in the member's copy_from_local holds the upload mid-transfer,
so each interleaving is forced. Every wait has a timeout.
"""

import errno
import multiprocessing
import os
import time
from pathlib import Path
from unittest.mock import patch

from fuse import FuseOSError

from storage.backends.local import LocalBackend
from storage.fs import StorageFS
import storage.namespace as storage_namespace
from storage.models import SyncStatus
from storage.index_audit import rebuild_index_from_cache
from storage.member_repairs import CorruptRepairRecord, MemberRepairs
from storage.state import StateManager
from storage.sync_executor import SyncExecutor
from storage.sync_tasks import DirOperation, DirSyncTask
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

    member.rename = unreachable
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
        real = getattr(self.member, name)
        calls = []

        def failing_once(*args):
            calls.append(args)
            if len(calls) == 1:
                raise OSError("member unreachable")
            return real(*args)

        patcher = patch.object(self.member, name, side_effect=failing_once)
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
        patcher = patch.object(self.member, "rename", side_effect=lambda *a: renames.append(a))
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
        patcher = patch.object(self.member, "rename", side_effect=lambda *a: renames.append(a))
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
        patch.object(self.member, "rename", side_effect=lambda *a: renames.append(a)).start()
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

    def test_rename_onto_a_name_too_long_to_record_fails_before_any_effect(self):
        """A record over the bound (two paths together over about 3.3 KiB)
        is refused before anything is written: ENAMETOOLONG, nothing moved."""
        self.fs.mkdir("/d", 0o755)
        long_target = "/e" + ("/" + "x" * 250) * 14  # under PATH_MAX
        with self.assertRaises(FuseOSError) as raised:
            self.fs.rename("/d", long_target)
        self.assertEqual(errno.ENAMETOOLONG, raised.exception.errno)
        self.assertTrue(self.fs.cache_manager.get_cache_path("/d").is_dir())
        self.assertEqual(["d"], self.member_names())
        self.assertEqual([], self.repairs())

    def test_failed_settle_leaves_the_intent_for_every_member(self):
        """The member rename failed, and writing its open record fails too:
        the intent stays, naming every target member."""
        self.fs.mkdir("/d", 0o755)
        self.fail_member_once("rename")
        repairs = self.fs.cache_manager.member_repairs
        real_write = repairs._write_new
        writes = []

        def failing_after_the_intent(record):
            writes.append(record.state)
            if record.state != "intent":
                raise OSError(errno.EIO, "injected")
            return real_write(record)

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
        patch.object(self.member, "rename", side_effect=lambda *a: renames.append(a)).start()
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
    real = member.rename

    def rename_then_die(*args):
        real(*args)
        os._exit(0)

    member.rename = rename_then_die
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

        with patch.object(self.member, "rename", side_effect=resolve_elsewhere_then_rename):
            self.fs.rename("/d", "/e")
        self.assertEqual([], self.fs.cache_manager.member_repairs.list())
        self.assertEqual(set(), self.fs.cache_manager.member_repairs.backends_for(["/old", "/older"]))

    def test_an_intent_resolved_while_its_rename_runs_still_records_the_miss(self):
        """Someone resolves the in-flight intent from another process, then
        the member misses the move: the miss gets a new open record."""
        self.fs.mkdir("/d", 0o755)

        def resolve_intent_then_fail(*_args):
            [intent] = MemberRepairs(self.cache_dir).list()
            self.assertEqual("intent", intent.state)
            self.assertTrue(MemberRepairs(self.cache_dir).resolve(intent.id))
            raise OSError("member unreachable")

        with patch.object(self.member, "rename", side_effect=resolve_intent_then_fail):
            self.fs.rename("/d", "/e")
        [record] = MemberRepairs(self.cache_dir).list()
        self.assertEqual({"member"}, self.fs.cache_manager.member_repairs.backends_for(["/e"]))

    def test_an_intent_resolved_while_its_rename_runs_survives_a_concurrent_reload(self):
        """As above, with a lookup (a sync worker, an eviction) reloading
        the registry between the resolve and the settle."""
        self.fs.mkdir("/d", 0o755)

        def resolve_intent_reload_then_fail(*_args):
            [intent] = MemberRepairs(self.cache_dir).list()
            self.assertTrue(MemberRepairs(self.cache_dir).resolve(intent.id))
            self.assertEqual([], self.fs.cache_manager.member_repairs.list())
            raise OSError("member unreachable")

        with patch.object(self.member, "rename", side_effect=resolve_intent_reload_then_fail):
            self.fs.rename("/d", "/e")
        [record] = MemberRepairs(self.cache_dir).list()
        self.assertEqual(("open", ("member",), "/d", "/e"),
                         (record.state, record.members, record.old_path, record.new_path))
        self.assertEqual({"member"}, self.fs.cache_manager.member_repairs.backends_for(["/e"]))

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

    def test_the_local_move_is_made_durable_before_any_member_call(self):
        """If the cache directories cannot be fsynced after the local move,
        no member is called and the intent stays."""
        self.fs.mkdir("/d", 0o755)
        renames = []
        patch.object(self.member, "rename", side_effect=lambda *a: renames.append(a)).start()
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
        patch.object(self.member, "rename",
                     side_effect=lambda *a: (calls.append(list(synced)), real_rename(*a))).start()
        self.addCleanup(patch.stopall)
        with patch("storage.namespace.fsync_directories", side_effect=recording):
            self.fs.rename("/d", "/x/y/d")
        [before_member] = calls
        for directory in (self.cache_dir, self.cache_dir / "x", self.cache_dir / "x" / "y"):
            self.assertIn(str(directory), before_member)
