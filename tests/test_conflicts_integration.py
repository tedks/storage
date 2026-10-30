"""
Integration tests for checksum-based conflict detection and resolution.

These tests mount the filesystem and verify that:
- differing files on different backends are surfaced as <name>.<backend>
- checksums are captured without downloading to cache
- the conflict-resolution CLI can pick a backend and converge contents
"""

import os
import subprocess
import tempfile
import time
import threading
import unittest
import sys
from pathlib import Path

import yaml

from storage.fs import StorageFS
from storage.backends.local import LocalBackend
from storage.checksums import fast_checksum_bytes
from tests.fuse_mount import ORPHAN_RELEASE_SUPPORTED, run_fuse, stop_fuse


class BaseConflictMountTest(unittest.TestCase):
    """Base class that mounts a StorageFS with two backends."""

    backend0_name = "local0"
    backend1_name = "local1"
    # Each suite also runs with the multithreaded loop (subclasses below).
    fuse_threads = False
    # Different sizes: split from the listing alone, nothing hashed.
    content0 = "alpha\n"
    content1 = "beta\n"

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.mount_dir = Path(self.temp_dir) / "mount"
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend0_dir = Path(self.temp_dir) / "backend0"
        self.backend1_dir = Path(self.temp_dir) / "backend1"
        self.db_path = Path(self.temp_dir) / "state.db"

        for path in (
            self.mount_dir,
            self.cache_dir,
            self.backend0_dir,
            self.backend1_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

        # Seed conflicting files directly on backends
        (self.backend0_dir / "report.txt").write_text(self.content0)
        (self.backend1_dir / "report.txt").write_text(self.content1)

        backends = {
            self.backend0_name: LocalBackend(self.backend0_name, str(self.backend0_dir)),
            self.backend1_name: LocalBackend(self.backend1_name, str(self.backend1_dir)),
        }

        self.fs = StorageFS(
            self.cache_dir,
            self.db_path,
            backends,
            cache_size=10 * 1024 * 1024,
            num_sync_workers=1,
            start_sync=False,  # keep the sync workers quiet during conflict setup
        )

        self.mount_thread = threading.Thread(
            target=run_fuse, args=(self.fs, self.mount_dir),
            kwargs={"fuse_threads": self.fuse_threads}, daemon=True
        )
        self.mount_thread.start()

        # Give FUSE a moment to come up
        time.sleep(1.0)

    def tearDown(self):
        problems = stop_fuse(self.mount_dir, self.mount_thread, fs=self.fs)
        if os.path.ismount(self.mount_dir) or self.mount_thread.is_alive():
            # Still serving (a lazy unmount leaves it running): removing the
            # cache or destroying under it would race its requests.
            self.fail(f"mount teardown, {self.temp_dir} left in place: {problems}")

        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)
        if problems:
            self.fail(f"mount teardown: {problems}")

    # Utility helpers -----------------------------------------------------

    def _listdir_until(self, predicate, timeout=5.0):
        """Poll os.listdir on the mount until predicate(entries) is True."""
        deadline = time.time() + timeout
        last = []
        while time.time() < deadline:
            try:
                last = os.listdir(self.mount_dir)
                if predicate(last):
                    return last
            except Exception:
                pass
            time.sleep(0.25)
        return last

    def _read_xattr_yaml(self, path: Path, name: str):
        raw = os.getxattr(str(path), name)
        return yaml.safe_load(raw.decode() if isinstance(raw, (bytes, bytearray)) else raw)


class TestLocalConflictMount(BaseConflictMountTest):
    """Conflicts on two local backends"""

    maxDiff = None

    def test_unmount_ends_the_loop_while_another_namespace_holds_the_mount(self):
        """A process in a copied mount namespace must not hang the teardown.

        A Bazel linux-sandbox or a container started while the mount is up
        copies it; the kernel then keeps the filesystem alive after our umount
        and never wakes the loop's read of /dev/fuse.
        """
        if not ORPHAN_RELEASE_SUPPORTED:
            self.skipTest("no tgkill number for this architecture")
        os.listdir(self.mount_dir)
        try:
            subprocess.run(["unshare", "-Urm", "true"], check=True, timeout=10,
                           capture_output=True)
        except (OSError, subprocess.SubprocessError) as exc:
            self.skipTest(f"cannot create a mount namespace here: {exc}")
        holder = subprocess.Popen(["unshare", "-Urm", "sleep", "120"])
        try:
            # Wait until the holder has really moved into its own mount
            # namespace and sees the mount; /proc/<pid> exists from fork on.
            mine = os.readlink("/proc/self/ns/mnt")
            needle = f" {os.path.realpath(self.mount_dir)} "
            deadline = time.monotonic() + 10
            while True:
                try:
                    ready = (os.readlink(f"/proc/{holder.pid}/ns/mnt") != mine
                             and needle in Path(f"/proc/{holder.pid}/mountinfo").read_text())
                except OSError:
                    ready = False
                if ready:
                    break
                self.assertLess(time.monotonic(), deadline, "the holder never copied the mount")
                time.sleep(0.05)
            started = time.monotonic()
            problems = stop_fuse(self.mount_dir, self.mount_thread, fs=self.fs)
            self.assertEqual(problems, [])
            self.assertIsNone(holder.poll(), "the holder must have outlived the unmount")
            self.assertTrue(self.fs.destroy_entered.is_set(), "libfuse should have run destroy")
            self.assertLess(time.monotonic() - started, 25.0)
        finally:
            holder.kill()
            holder.wait(timeout=10)

    def test_conflict_detection_and_take(self):
        # First read primes directory cache and kicks off checksum workers
        try:
            os.listdir(self.mount_dir)
        except Exception as exc:
            self.skipTest(f"Mount unavailable: {exc}")

        entries = self._listdir_until(
            lambda names: any(name.endswith(self.backend0_name) for name in names)
            and any(name.endswith(self.backend1_name) for name in names)
        )

        conflict0 = f"report.txt.{self.backend0_name}"
        conflict1 = f"report.txt.{self.backend1_name}"

        self.assertIn(conflict0, entries)
        self.assertIn(conflict1, entries)
        self.assertNotIn("report.txt", entries, "Conflicted files should be split by backend")

        # Checksums should be exposed via xattr on the mount without caching the file
        cache_conflict_path = self.cache_dir / conflict0.lstrip("/")
        self.assertFalse(
            cache_conflict_path.exists(),
            "Checksumming should not download or cache the file",
        )

        checksum_list = self._read_xattr_yaml(self.mount_dir / conflict0, "user.storage.checksums")
        values = {entry.split(":")[0]: entry.split(":")[1] for entry in checksum_list}
        self.assertSetEqual(
            set(values), {self.backend0_name, self.backend1_name}, "All backends should be recorded"
        )
        self.assertEqual(values, self.expected_checksum_values())

        # Resolve by taking backend0's version
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "storage.cli",
                "conflicts",
                str(self.mount_dir),
                "--file",
                conflict0,
                "--take",
                self.backend0_name,
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self.fail(f"conflicts CLI failed: {result.stderr}")

        # After resolution, listing should collapse to the base name
        final_entries = self._listdir_until(lambda names: "report.txt" in names)
        self.assertIn("report.txt", final_entries)
        self.assertNotIn(conflict0, final_entries)
        self.assertNotIn(conflict1, final_entries)

        # Both backend files should now match backend0 content
        self.assertEqual((self.backend0_dir / "report.txt").read_text(), self.content0)
        self.assertEqual((self.backend1_dir / "report.txt").read_text(), self.content0)

    def expected_checksum_values(self):
        # A size split hashes nothing: each copy carries its listed size.
        return {self.backend0_name: f"size={len(self.content0)}",
                self.backend1_name: f"size={len(self.content1)}"}


class TestLocalConflictMountEqualSize(TestLocalConflictMount):
    """Same size, different bytes: plain name until both hashes land."""

    content0 = "alpha\n"
    content1 = "gamma\n"

    def expected_checksum_values(self):
        return {self.backend0_name: fast_checksum_bytes(self.content0.encode()),
                self.backend1_name: fast_checksum_bytes(self.content1.encode())}


class TestConflictMerge(BaseConflictMountTest):
    """Conflicts resolved via external merge tool."""

    def test_merge_uses_external_tool(self):
        try:
            os.listdir(self.mount_dir)
        except Exception as exc:
            self.skipTest(f"Mount unavailable: {exc}")

        # Ensure both backend conflict files appear
        conflict0 = f"report.txt.{self.backend0_name}"
        conflict1 = f"report.txt.{self.backend1_name}"
        entries = self._listdir_until(
            lambda names: conflict0 in names and conflict1 in names
        )
        self.assertIn(conflict0, entries)
        self.assertIn(conflict1, entries)

        # Use merge; this relies on git merge-file or diff3 being installed in the test env
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "storage.cli",
                "conflicts",
                str(self.mount_dir),
                "--file",
                "report.txt",
                "--merge",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self.fail(f"conflicts merge failed: {result.stderr}")

        final_entries = self._listdir_until(lambda names: "report.txt" in names)
        self.assertIn("report.txt", final_entries)
        self.assertNotIn(conflict0, final_entries)
        self.assertNotIn(conflict1, final_entries)


class LoopbackRemoteBackend(LocalBackend):
    """A LocalBackend flagged as remote to exercise remote code paths."""

    def __init__(self, name: str, root_path: str):
        super().__init__(name, root_path, speed_priority=50)
        self.is_remote = True


class TestRemoteConflictMount(BaseConflictMountTest):
    """Conflicts with a backend marked as remote."""

    backend1_name = "remote1"

    def setUp(self):
        super().setUp()
        # Swap backend1 to remote-like backend after base setup
        self.fs.backends[self.backend1_name] = LoopbackRemoteBackend(
            self.backend1_name, str(self.backend1_dir)
        )

    def test_remote_conflict_detection_without_cache(self):
        try:
            os.listdir(self.mount_dir)
        except Exception as exc:
            self.skipTest(f"Mount unavailable: {exc}")

        entries = self._listdir_until(
            lambda names: any(name.endswith(self.backend0_name) for name in names)
            and any(name.endswith(self.backend1_name) for name in names)
        )

        conflict_remote = f"report.txt.{self.backend1_name}"
        self.assertIn(conflict_remote, entries)

        cache_path = self.cache_dir / conflict_remote.lstrip("/")
        self.assertFalse(
            cache_path.exists(),
            "Remote checksum pass must not populate cache when only listing",
        )

        checksum_list = self._read_xattr_yaml(self.mount_dir / conflict_remote, "user.storage.checksums")
        backend_tags = {entry.split(':')[0] for entry in checksum_list}
        self.assertIn(self.backend1_name, backend_tags)


class TestLocalConflictMountThreaded(TestLocalConflictMount):
    fuse_threads = True


class TestConflictMergeThreaded(TestConflictMerge):
    fuse_threads = True


class TestLocalConflictMountEqualSizeThreaded(TestLocalConflictMountEqualSize):
    fuse_threads = True


class TestRemoteConflictMountThreaded(TestRemoteConflictMount):
    fuse_threads = True


if __name__ == "__main__":
    unittest.main()
