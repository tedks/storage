"""Sync uploads racing renames and unlinks through the mount (ditz sync-namespace-orphans).

A real sync worker uploads while a FUSE handler runs in another thread; an
Event gate in the member's copy_from_local holds the upload mid-transfer,
so each interleaving is forced. Every wait has a timeout.
"""

import os
import time
from unittest.mock import patch

from storage.models import SyncStatus
from storage.state import StateManager
from storage.sync_executor import SyncExecutor
from storage.sync_tasks import DirOperation
from tests.test_fs_threading import BLOCKED, WAIT, ThreadingTestBase, Worker


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

    def test_missed_rename_of_an_evicted_file_is_left_for_repair(self):
        self.write_file("/a.txt")
        self.start_sync()
        self.wait_synced("/a.txt")
        self.wait_idle()
        self.fs.sync_manager.stop()
        self.fail_member_once("rename")
        self.fs.rename("/a.txt", "/b.txt")
        # Stands in for cache pressure evicting the renamed, CLEAN file.
        cache_path = self.fs.cache_manager.get_cache_path("/b.txt")
        self.assertGreater(self.fs.cache_manager._evict_if_still_clean("/b.txt", cache_path), 0)

        self.start_sync()
        self.wait_idle()
        self.assertEqual(["a.txt"], self.member_names())
        self.assertEqual([("member", "/a.txt", "/b.txt")], self.repairs())

    def repairs(self, state_manager=None):
        state_manager = state_manager or self.fs.state_manager
        return [
            (r.backend_name, r.old_path, r.new_path)
            for r in state_manager.list_member_rename_repairs()
        ]

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

    def test_missed_rename_repair_survives_restart_and_retries(self):
        """The repair record is in the index database, not the in-memory
        queue: a restart keeps it, and a legacy queued retry (no record of
        what the member held) is fenced to the same record, with no member
        call."""
        self.fs.mkdir("/d", 0o755)
        self.fail_member_once("rename")
        self.fs.rename("/d", "/e")
        # A retry queued by an older build, carrying only the two names.
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

        reopened = StateManager(str(self.temp_dir / "state.db"))
        self.addCleanup(reopened.close)
        self.assertEqual([("member", "/d", "/e")], self.repairs(reopened))

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
