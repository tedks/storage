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
from tests.fuse_mount import run_fuse


class BaseConflictMountTest(unittest.TestCase):
    """Base class that mounts a StorageFS with two backends."""

    backend0_name = "local0"
    backend1_name = "local1"
    # Each suite also runs with the multithreaded loop (subclasses below).
    fuse_threads = False

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
        (self.backend0_dir / "report.txt").write_text("alpha\n")
        (self.backend1_dir / "report.txt").write_text("beta\n")

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
        try:
            subprocess.run(
                ["fusermount", "-u", str(self.mount_dir)],
                check=False,
                timeout=5,
            )
        except Exception:
            pass

        time.sleep(0.5)

        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

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
        backend_tags = {entry.split(":")[0] for entry in checksum_list}
        self.assertSetEqual(
            backend_tags, {self.backend0_name, self.backend1_name}, "All backends should be recorded"
        )

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
        self.assertEqual((self.backend0_dir / "report.txt").read_text(), "alpha\n")
        self.assertEqual((self.backend1_dir / "report.txt").read_text(), "alpha\n")


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


class TestRemoteConflictMountThreaded(TestRemoteConflictMount):
    fuse_threads = True

if __name__ == "__main__":
    unittest.main()
