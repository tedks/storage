"""Sync uploads racing renames and unlinks through the mount (ditz sync-namespace-orphans).

A real sync worker uploads while a FUSE handler runs in another thread; an
Event gate in the member's copy_from_local holds the upload mid-transfer,
so each interleaving is forced. Every wait has a timeout.
"""

import os
import time

from storage.models import SyncStatus
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
        """An upload starting under a directory whose rename is draining
        waits for the rename, then finds its row moved and skips it."""
        self.fs.mkdir("/d", 0o755)
        self.write_file("/d/f.txt")
        self.write_file("/d/g.txt")
        gate = self.gate_upload("/d/f.txt")
        self.fs.sync_manager.num_workers = 2
        self.start_sync()
        self.assertTrue(gate.started.wait(WAIT))
        deadline = time.monotonic() + WAIT
        while (self.fs.state_manager.get_file_metadata("/d/g.txt").sync_status != SyncStatus.CLEAN
               and time.monotonic() < deadline):
            time.sleep(0.02)

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
