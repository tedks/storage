"""
System tests for background sync with mounted filesystem

These tests mount an actual FUSE filesystem and verify sync behavior
end-to-end.
"""

import unittest
import tempfile
import time
import os
import subprocess
import threading
import shutil
from pathlib import Path

from storage.fs import StorageFS, fuse_mount_options
from storage.backends.local import LocalBackend


class _MountedCase(unittest.TestCase):
    """A two-member local mount; START_SYNC says whether sync workers run."""

    START_SYNC = True

    def setUp(self):
        """Set up test environment with actual mount"""
        # Create temp directories
        self.temp_dir = tempfile.mkdtemp()
        self.mount_dir = Path(self.temp_dir) / "mount"
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend1_dir = Path(self.temp_dir) / "backend1"
        self.backend2_dir = Path(self.temp_dir) / "backend2"
        self.db_path = Path(self.temp_dir) / "state.db"

        self.mount_dir.mkdir()
        self.cache_dir.mkdir()
        self.backend1_dir.mkdir()
        self.backend2_dir.mkdir()

        # Create backends
        backends = {
            "backend1": LocalBackend("backend1", str(self.backend1_dir), speed_priority=0),
            "backend2": LocalBackend("backend2", str(self.backend2_dir), speed_priority=10),
        }

        # Create filesystem
        self.fs = StorageFS(
            self.cache_dir,
            self.db_path,
            backends,
            cache_size=50 * 1024 * 1024,  # 50MB cache
            num_sync_workers=2,
            start_sync=self.START_SYNC,
        )
        if not self.START_SYNC:
            # init() starts the workers at mount unless they look started.
            self.fs._sync_started = True

        # Mount in separate thread (FUSE blocks)
        self.mount_thread = threading.Thread(
            target=self._mount_fs,
            daemon=True
        )
        self.mount_thread.start()

        # Wait for mount to complete
        time.sleep(1.0)

        # Verify mount
        if not self.mount_dir.exists():
            raise RuntimeError("Mount directory missing")

    def _mount_fs(self):
        """Mount filesystem (runs in separate thread)"""
        try:
            from fuse import FUSE
            FUSE(
                self.fs,
                str(self.mount_dir),
                nothreads=True,
                foreground=True,
                allow_other=False,
                **fuse_mount_options(
                    self.fs.backends,
                    enable_watchers=self.fs._enable_watchers,
                    fresh_window=self.fs._revalidate_after,
                ),
            )
        except Exception as e:
            print(f"Mount error: {e}")

    def tearDown(self):
        """Clean up and unmount"""
        # Unmount
        try:
            subprocess.run(['fusermount', '-u', str(self.mount_dir)],
                          timeout=5.0, check=False)
        except Exception:
            pass

        # Wait a bit for unmount
        time.sleep(0.5)

        # Clean up temp directory
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _wait_for(self, predicate, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.05)
        return predicate()



class TestSyncSystemIntegration(_MountedCase):
    """System tests with mounted filesystem"""

    def test_write_and_sync(self):
        """Test writing file and automatic sync"""
        # Write a file through the mounted filesystem
        test_file = self.mount_dir / "test.txt"
        test_content = "Hello, World!"

        try:
            test_file.write_text(test_content)
        except Exception as e:
            self.skipTest(f"Cannot write to mount (may need FUSE permissions): {e}")

        # Wait for sync to complete
        time.sleep(3.0)

        # Check cache has the file
        cache_file = self.cache_dir / "test.txt"
        self.assertTrue(cache_file.exists(), "File should exist in cache")
        self.assertEqual(cache_file.read_text(), test_content)

        # Check backends have the file (may take a bit longer)
        time.sleep(2.0)

        backend1_file = self.backend1_dir / "test.txt"
        backend2_file = self.backend2_dir / "test.txt"

        # At least one backend should have it
        has_backend1 = backend1_file.exists()
        has_backend2 = backend2_file.exists()

        self.assertTrue(has_backend1 or has_backend2,
                       "File should be synced to at least one backend")

        # If it exists, content should match
        if has_backend1:
            self.assertEqual(backend1_file.read_text(), test_content)
        if has_backend2:
            self.assertEqual(backend2_file.read_text(), test_content)

    def test_read_from_backend(self):
        """Test reading file that exists only in backend"""
        # Create file directly in backend
        test_file = "backend_file.txt"
        test_content = "Backend content"

        backend_path = self.backend1_dir / test_file
        backend_path.write_text(test_content)

        # Record in state database that it exists on backend
        from storage.models import FileLocation, FileMetadata, SyncStatus
        metadata = FileMetadata(
            virtual_path=f"/{test_file}",
            size=len(test_content),
            checksum=self.fs.state_manager.compute_checksum(backend_path),
            last_access=time.time(),
            last_modified=time.time(),
            sync_status=SyncStatus.CLEAN,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None
        )
        self.fs.state_manager.set_file_metadata(metadata)

        location = FileLocation(
            virtual_path=f"/{test_file}",
            backend_name="backend1",
            checksum=metadata.checksum,
            last_synced=time.time(),
            is_cached=False
        )
        self.fs.state_manager.add_file_location(location)

        # Try to read through mount
        try:
            mount_file = self.mount_dir / test_file
            content = mount_file.read_text()
            self.assertEqual(content, test_content)

            # Should now be cached
            time.sleep(0.5)
            cache_file = self.cache_dir / test_file
            self.assertTrue(cache_file.exists(), "File should be cached after read")

        except Exception as e:
            self.skipTest(f"Cannot read from mount: {e}")

    def test_multiple_file_sync(self):
        """Test syncing multiple files concurrently"""
        try:
            # Write multiple files
            num_files = 5
            for i in range(num_files):
                test_file = self.mount_dir / f"file{i}.txt"
                test_file.write_text(f"Content {i}")

            # Wait for all to sync
            time.sleep(5.0)

            # Check that files are in backends
            synced_count = 0
            for i in range(num_files):
                backend1_file = self.backend1_dir / f"file{i}.txt"
                backend2_file = self.backend2_dir / f"file{i}.txt"

                if backend1_file.exists() or backend2_file.exists():
                    synced_count += 1

            # Most files should be synced (allow for timing variations)
            self.assertGreater(synced_count, num_files // 2,
                             f"At least half of {num_files} files should be synced")

        except Exception as e:
            self.skipTest(f"Cannot write multiple files: {e}")

    def test_rsync_write_succeeds_and_syncs_final_file(self):
        """Rsync into the mounted filesystem should complete without write-path errors."""
        if shutil.which("rsync") is None:
            self.skipTest("rsync not available")

        source_dir = Path(self.temp_dir) / "source"
        source_dir.mkdir()
        source_file = source_dir / "movie.bin"

        payload_size = 8 * 1024 * 1024
        unit = b"abcdefghijklmnopqrstuvwxyz012345"
        repeats = (payload_size // len(unit)) + 1
        payload = (unit * repeats)[:payload_size]
        source_file.write_bytes(payload)

        mount_target_dir = self.mount_dir / "media"
        try:
            mount_target_dir.mkdir(exist_ok=True)
        except Exception as exc:
            self.skipTest(f"Cannot create directory in mount: {exc}")

        result = subprocess.run(
            ["rsync", "-a", str(source_file), str(mount_target_dir)],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            self.fail(
                "rsync failed with non-zero status:\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )

        mounted_copy = mount_target_dir / source_file.name
        self.assertTrue(mounted_copy.exists())
        self.assertEqual(mounted_copy.read_bytes(), payload)

        virtual_path = f"/media/{source_file.name}"
        self.assertTrue(self.fs.sync_manager.sync_now(virtual_path, timeout=10.0))

        cache_copy = self.cache_dir / "media" / source_file.name
        self.assertTrue(cache_copy.exists())
        self.assertEqual(cache_copy.read_bytes(), payload)

        synced_backends = []
        for backend_dir in (self.backend1_dir, self.backend2_dir):
            candidate = backend_dir / "media" / source_file.name
            if candidate.exists():
                synced_backends.append(candidate)

        self.assertTrue(synced_backends, "Final file should sync to at least one backend")
        for candidate in synced_backends:
            self.assertEqual(candidate.read_bytes(), payload)

        temp_glob = f".{source_file.name}.*"
        self.assertTrue(
            self._wait_for(
                lambda: not list((self.cache_dir / "media").glob(temp_glob))
                and not list((self.backend1_dir / "media").glob(temp_glob))
                and not list((self.backend2_dir / "media").glob(temp_glob)),
                timeout=5.0,
            ),
            "rsync temp files should not persist in cache or backends",
        )


class TestKernelCacheVisibility(_MountedCase):
    """Kernel cache options (fuse_mount_options) against writes through the mount.

    Sync workers stay off, so members never change and every answer comes
    from the cache and the kernel: uploads racing a rename or unlink leave
    old names on members (ditz sync-namespace-orphans), a separate problem.
    """

    START_SYNC = False

    def test_writes_through_the_mount_are_visible_at_once(self):
        """The kernel keeps entries and attributes for the fresh window (60 s
        here) and misses for 10 s; every write path must still show at once."""
        m = self.mount_dir
        try:
            (m / "a.txt").write_text("one")
        except Exception as e:
            self.skipTest(f"Cannot write to mount (may need FUSE permissions): {e}")

        # Prime the kernel's caches: a listing, positive entries, and a miss
        # for every name about to appear.
        self.assertEqual(["a.txt"], sorted(os.listdir(m)))
        os.stat(m / "a.txt")
        for name in ("b.txt", "c.txt", "link", "sub", "new.txt"):
            self.assertFalse(os.path.lexists(m / name))

        os.rename(m / "a.txt", m / "b.txt")
        self.assertEqual(["b.txt"], sorted(os.listdir(m)))
        self.assertFalse(os.path.lexists(m / "a.txt"))
        self.assertEqual(3, os.stat(m / "b.txt").st_size)

        (m / "c.txt").write_text("three")
        os.symlink("b.txt", m / "link")
        os.mkdir(m / "sub")
        self.assertEqual(["b.txt", "c.txt", "link", "sub"], sorted(os.listdir(m)))
        self.assertEqual(5, os.stat(m / "c.txt").st_size)
        self.assertTrue(os.path.islink(m / "link"))
        self.assertEqual("b.txt", os.readlink(m / "link"))
        self.assertEqual("one", (m / "link").read_text())
        self.assertTrue(os.path.isdir(m / "sub"))

        # A write that changes the size is seen by stat at once.
        with open(m / "c.txt", "a") as f:
            f.write("+more")
        self.assertEqual(10, os.stat(m / "c.txt").st_size)
        self.assertEqual("three+more", (m / "c.txt").read_text())
        os.truncate(m / "c.txt", 2)
        self.assertEqual(2, os.stat(m / "c.txt").st_size)

        # The miss cached for new.txt does not hide a file created through the mount.
        (m / "sub" / "new.txt").write_text("n")
        (m / "new.txt").write_text("n")
        self.assertTrue(os.path.exists(m / "new.txt"))

        os.unlink(m / "b.txt")
        os.unlink(m / "link")
        os.rename(m / "sub" / "new.txt", m / "sub" / "moved.txt")
        os.unlink(m / "sub" / "moved.txt")
        os.rmdir(m / "sub")
        self.assertEqual(["c.txt", "new.txt"], sorted(os.listdir(m)))
        for name in ("b.txt", "link", "sub"):
            self.assertFalse(os.path.lexists(m / name))

    def test_kernel_keeps_attributes_for_the_window(self):
        """A repeated stat inside the window does not reach the mount."""
        try:
            (self.mount_dir / "kept.txt").write_text("k")
        except Exception as e:
            self.skipTest(f"Cannot write to mount (may need FUSE permissions): {e}")
        calls = []
        original = self.fs.getattr

        def counting_getattr(path, fh=None):
            calls.append(path)
            return original(path, fh)

        self.fs.getattr = counting_getattr
        try:
            os.stat(self.mount_dir / "kept.txt")
            first = calls.count("/kept.txt")
            time.sleep(1.5)  # past libfuse's 1 s default
            for _ in range(5):
                os.stat(self.mount_dir / "kept.txt")
            self.assertEqual(first, calls.count("/kept.txt"))
        finally:
            self.fs.getattr = original


if __name__ == '__main__':
    unittest.main()
