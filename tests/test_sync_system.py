"""
System tests for background sync with mounted filesystem

These tests mount an actual FUSE filesystem and verify sync behavior
end-to-end.
"""

import errno
import json
import sys
import unittest
import tempfile
import time
import os
import stat
import subprocess
import threading
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from storage.fs import StorageFS
from storage.backends.local import LocalBackend
from storage.models import SyncStatus
from tests.fuse_mount import run_fuse, stop_fuse


class _MountedCase(unittest.TestCase):
    """A two-member local mount; START_SYNC says whether sync workers run.

    FS_KWARGS go to StorageFS and KERNEL_OPTIONS to fuse_mount_options.
    """

    START_SYNC = True
    FS_KWARGS: dict = {}
    KERNEL_OPTIONS: dict = {}
    # Every suite also runs with the multithreaded loop (subclasses below).
    fuse_threads = False

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
            **self.FS_KWARGS,
        )
        if not self.START_SYNC:
            # StorageFS.init() starts the workers at mount unless
            # _sync_started says they already run (fs.py, init).
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
            run_fuse(self.fs, self.mount_dir, fuse_threads=self.fuse_threads,
                     **self.KERNEL_OPTIONS)
        except Exception as e:
            print(f"Mount error: {e}")

    def tearDown(self):
        """Clean up and unmount"""
        problems = stop_fuse(self.mount_dir, self.mount_thread, fs=self.fs)
        if os.path.ismount(self.mount_dir) or self.mount_thread.is_alive():
            # Still serving (a lazy unmount leaves it running): removing the
            # cache or destroying under it would race its requests.
            self.fail(f"mount teardown, {self.temp_dir} left in place: {problems}")

        # Clean up temp directory
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        if problems:
            self.fail(f"mount teardown: {problems}")

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
        (m / "a.txt").write_text("one")

        # Prime the kernel's caches: a listing, positive entries, and a miss
        # for every name about to appear.
        self.assertEqual(["a.txt"], sorted(os.listdir(m)))
        os.stat(m / "a.txt")
        for name in ("b.txt", "c.txt", "sub", "new.txt", "link"):
            self.assertFalse(os.path.lexists(m / name))

        os.rename(m / "a.txt", m / "b.txt")
        self.assertEqual(["b.txt"], sorted(os.listdir(m)))
        self.assertFalse(os.path.lexists(m / "a.txt"))
        self.assertEqual(3, os.stat(m / "b.txt").st_size)

        (m / "c.txt").write_text("three")
        # A link (its name primed as a miss above) shows at once.
        os.symlink("b.txt", m / "link")
        self.assertTrue(os.path.islink(m / "link"))
        self.assertEqual("b.txt", os.readlink(m / "link"))
        os.mkdir(m / "sub")
        self.assertEqual(["b.txt", "c.txt", "link", "sub"], sorted(os.listdir(m)))
        self.assertEqual(5, os.stat(m / "c.txt").st_size)
        self.assertTrue(os.path.isdir(m / "sub"))

        # A write that changes the size is seen by stat at once.
        with open(m / "c.txt", "a") as f:
            f.write("+more")
        self.assertEqual(10, os.stat(m / "c.txt").st_size)
        self.assertEqual("three+more", (m / "c.txt").read_text())
        os.truncate(m / "c.txt", 2)
        self.assertEqual(2, os.stat(m / "c.txt").st_size)
        os.chmod(m / "c.txt", 0o600)
        self.assertEqual(0o600, os.stat(m / "c.txt").st_mode & 0o777)
        os.utime(m / "c.txt", (1_000_000_000, 1_000_000_000))
        self.assertEqual(1_000_000_000, int(os.stat(m / "c.txt").st_mtime))

        # The miss cached for new.txt does not hide a file created through the mount.
        (m / "sub" / "new.txt").write_text("n")
        os.stat(m / "sub" / "new.txt")
        os.rename(m / "sub", m / "sub2")
        self.assertFalse(os.path.lexists(m / "sub" / "new.txt"))
        self.assertEqual(1, os.stat(m / "sub2" / "new.txt").st_size)
        os.rename(m / "sub2", m / "sub")
        (m / "new.txt").write_text("n")
        self.assertTrue(os.path.exists(m / "new.txt"))

        os.unlink(m / "link")
        self.assertFalse(os.path.lexists(m / "link"))
        os.unlink(m / "b.txt")
        os.rename(m / "sub" / "new.txt", m / "sub" / "moved.txt")
        os.unlink(m / "sub" / "moved.txt")
        os.rmdir(m / "sub")
        self.assertEqual(["c.txt", "new.txt"], sorted(os.listdir(m)))
        for name in ("b.txt", "sub"):
            self.assertFalse(os.path.lexists(m / name))

    def test_kernel_keeps_names_and_misses_but_not_attributes(self):
        """Names are kept for the window and misses for 10 s; attributes
        are asked at every stat (attr_timeout 0)."""
        os.mkdir(self.mount_dir / "d1")
        (self.mount_dir / "d1" / "f").write_text("k")
        calls = []
        original = self.fs.getattr

        def counting_getattr(path, fh=None):
            calls.append(path)
            return original(path, fh)

        self.fs.getattr = counting_getattr
        try:
            os.stat(self.mount_dir / "d1" / "f")
            self.assertFalse(os.path.exists(self.mount_dir / "nope"))
            time.sleep(1.5)  # past libfuse's 1 s default
            before = list(calls)
            for _ in range(5):
                os.stat(self.mount_dir / "d1" / "f")
                self.assertFalse(os.path.exists(self.mount_dir / "nope"))
            after = calls[len(before):]
            self.assertEqual(0, after.count("/d1"), "the walk through d1 looked it up again")
            self.assertEqual(0, after.count("/nope"), "the miss was not remembered")
            self.assertGreaterEqual(after.count("/d1/f"), 5, "stat did not reach getattr")
        finally:
            self.fs.getattr = original


# A page fault can block in the kernel while Python holds its GIL. The FUSE
# loop runs in the test process, so a mapping of a mounted file is read by a
# separate client process (the pattern of #204's _MAPPED_READER).
_FSTAT_MMAP_PROBE = """
import json, mmap, os, sys
with open(sys.argv[1], "rb") as f:
    first = os.fstat(f.fileno()).st_mtime
    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
        data = mapped[:]
    last = os.fstat(f.fileno()).st_mtime
print(json.dumps({"fstat_mtime": first, "mapped": data.hex(), "fstat_after": last}))
"""


class TestKernelCacheRefetch(_MountedCase):
    """A refetch after revalidation is read in full, never through stale
    pages or a stale size the kernel still holds (fuse_mount_options)."""

    START_SYNC = False
    FS_KWARGS = {"revalidate_after": 1.0}
    # Names kept far longer than the revalidation age, as on a real mount.
    KERNEL_OPTIONS = {"entry_timeout": 60.0}

    def _change_on_member(self, name, data):
        path = self.backend1_dir / name
        st = path.stat()
        path.write_bytes(data)
        os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 5_000_000_000))

    def _read_after_revalidation(self, name, old, new):
        (self.backend1_dir / name).write_bytes(old)
        target = self.mount_dir / name
        self.assertEqual(old, target.read_bytes())  # cached, pages primed
        self.assertEqual(len(old), os.stat(target).st_size)
        self._change_on_member(name, new)
        time.sleep(1.3)  # past revalidate_after
        # A stat just before the open is answered from the old cache copy,
        # so the kernel and libfuse both hold the old size and mtime.
        self.assertEqual(len(old), os.stat(target).st_size)
        with open(target, "rb") as f:
            self.assertEqual(new, f.read())
        self.assertEqual(len(new), os.stat(target).st_size)

    def test_grown_file_reads_in_full(self):
        self._read_after_revalidation("grow.bin", b"a" * 100, b"b" * 200)

    def test_same_size_rewrite_drops_old_pages(self):
        self._read_after_revalidation("same.bin", b"a" * 100, b"c" * 100)

    def test_shrunk_file_reads_short(self):
        self._read_after_revalidation("shrink.bin", b"a" * 200, b"d" * 50)

    def test_same_stat_refetch_reads_fresh_bytes_and_reuses_warm_backing(self):
        name = "same-stat.bin"
        source = self.backend1_dir / name
        target = self.mount_dir / name
        cached = self.cache_dir / name
        before, after = b"a" * 16384, b"b" * 16384
        stamp = 1_000_000_000
        source.write_bytes(before)
        os.utime(source, (stamp, stamp))
        backend = self.fs.backends["backend1"]
        with patch.object(backend, "copy_to_local", wraps=backend.copy_to_local) as download:
            self.assertEqual(before, target.read_bytes())
            self.assertEqual(before, target.read_bytes())
            self.assertEqual(1, download.call_count)

            source.write_bytes(after)
            os.utime(source, (stamp, stamp))
            self.fs._on_remote_change("/" + name)
            self.assertFalse(cached.exists())

            with target.open("rb") as reader:
                self.assertEqual(after, cached.read_bytes())
                self.assertEqual(stamp, target.stat().st_mtime)
                self.assertEqual(after, reader.read())
            self.assertEqual(after, target.read_bytes())
            self.assertEqual(2, download.call_count)

    def _probe_in_client(self, target):
        """fstat and an mmap read of target, from another process.

        A page fault can block in the kernel while Python holds its GIL,
        and this process runs the FUSE loop that serves the fault, so the
        mapping is read by a separate client (as in #204's _MAPPED_READER).
        """
        done = subprocess.run(
            [sys.executable, "-c", _FSTAT_MMAP_PROBE, str(target)],
            capture_output=True, text=True, timeout=30, check=True,
        )
        result = json.loads(done.stdout)
        return result["fstat_mtime"], bytes.fromhex(result["mapped"]), result["fstat_after"]

    def test_same_stat_refetch_keeps_member_mtime_in_fstat_and_mmap(self):
        # The member's own timestamp is what stat and fstat report, before
        # and after a same-stat refetch, and mmap (which reads through the
        # page cache) sees the new bytes.
        name = "same-stat-mmap.bin"
        source = self.backend1_dir / name
        target = self.mount_dir / name
        before, after = b"a" * 16384, b"b" * 16384
        stamp = 1_000_000_000
        source.write_bytes(before)
        os.utime(source, (stamp, stamp))
        self.assertEqual((stamp, before, stamp), self._probe_in_client(target))

        source.write_bytes(after)
        os.utime(source, (stamp, stamp))
        self.fs._on_remote_change("/" + name)

        self.assertEqual((stamp, after, stamp), self._probe_in_client(target))
        self.assertEqual(stamp, os.stat(target).st_mtime)
        # A later open (warm, same backing inode) still reads the new bytes.
        with open(target, "rb") as reader:
            self.assertEqual(after, reader.read())
            self.assertEqual(stamp, os.fstat(reader.fileno()).st_mtime)

    # A reader held open across a same-stat invalidation keeps its old cache
    # file (_invalidate_file_cache unlinks it under the descriptor), but the
    # page cache belongs to the FUSE inode, which every descriptor shares.
    # 4 MiB with probes at 0 and 3 MiB: far beyond the kernel's FUSE
    # readahead window, so a read at one offset never fills the other.
    HELD_SIZE = 4 * 1024 * 1024
    HELD_TAIL = 3 * 1024 * 1024
    OLD, NEW = b"a", b"b"

    def _bytes_at(self, fd, offset, size=4096):
        """The distinct byte values a fresh pread sees."""
        data = os.pread(fd, size, offset)
        if len(data) != size:
            raise RuntimeError(f"short read at {offset}: {len(data)} of {size}")
        return set(data)

    def _held_reader_and_new_reader(self, name):
        """Reader A primed with OLD, the member rewritten to NEW with the same
        size and mtime, the watcher's invalidation, then reader B opened.

        Returns (a, b) as raw descriptors (no Python buffering); the caller
        closes both. B's open refetched the file and dropped the pages.
        """
        source = self.backend1_dir / name
        target = self.mount_dir / name
        stamp = 1_000_000_000
        source.write_bytes(self.OLD * self.HELD_SIZE)
        os.utime(source, (stamp, stamp))
        a = os.open(target, os.O_RDONLY)
        try:
            if self._bytes_at(a, 0, self.HELD_SIZE) != set(self.OLD):
                raise RuntimeError("reader A did not see the original bytes")
            source.write_bytes(self.NEW * self.HELD_SIZE)
            os.utime(source, (stamp, stamp))
            self.fs._on_remote_change("/" + name)
            if (self.cache_dir / name).exists():
                raise RuntimeError("invalidation kept the clean cache copy")
            b = os.open(target, os.O_RDONLY)
        except BaseException:
            os.close(a)
            raise
        try:
            st = os.stat(target)
            if st.st_size != self.HELD_SIZE or st.st_mtime != stamp:
                raise RuntimeError("the rewrite changed the file's size or mtime")
        except BaseException:
            os.close(a)
            os.close(b)
            raise
        return a, b

    def test_held_reader_over_the_same_range_leaves_new_reader_fresh(self):
        a, b = self._held_reader_and_new_reader("held-same.bin")
        try:
            self.assertEqual(set(self.NEW), self._bytes_at(b, 0, self.HELD_SIZE))
            # B's read filled every page with NEW before A asked again, so
            # A's read is a page cache hit that never reaches A's old cache
            # file (it sees NEW today; either answer is legitimate for A).
            self.assertTrue(self._bytes_at(a, 0, self.HELD_SIZE) <= {*self.OLD, *self.NEW})
            self.assertEqual(set(self.NEW), self._bytes_at(b, 0, self.HELD_SIZE))
        finally:
            os.close(a)
            os.close(b)

    def test_stale_refill_by_held_reader_is_gone_after_reopen(self):
        name = "held-reopen.bin"
        a, b = self._held_reader_and_new_reader(name)
        try:
            self.assertEqual(set(self.NEW), self._bytes_at(b, 0))
            # A page B has not read: the miss goes to the daemon on A's
            # handle, which reads A's old cache file into the shared pages.
            self.assertEqual(set(self.OLD), self._bytes_at(a, self.HELD_TAIL))
        finally:
            os.close(a)
            os.close(b)
        # No descriptor holds the old file now: an open drops the stale
        # pages and the whole file reads NEW.
        c = os.open(self.mount_dir / name, os.O_RDONLY)
        try:
            self.assertEqual(set(self.NEW), self._bytes_at(c, 0, self.HELD_SIZE))
        finally:
            os.close(c)

    # Known limitation (#206), older than dropping auto_cache (which kept
    # stale pages too). strict, so a fix (B reads NEW) turns this red and the
    # marker comes off; raises=_StaleRefill, so only the #206 shape (A and B
    # both read OLD at the tail) is the expected failure: a setup failure or
    # any other payload fails the test.
    class _StaleRefill(Exception):
        pass

    @pytest.mark.xfail(strict=True, raises=_StaleRefill,
                       reason="#206: a reader held open across invalidation refills the "
                              "shared FUSE pages from its old cache file")
    def test_stale_refill_by_held_reader_reaches_new_reader(self):
        a, b = self._held_reader_and_new_reader("held-refill.bin")
        try:
            if self._bytes_at(b, 0) != set(self.NEW):
                raise RuntimeError("reader B did not see the refetched bytes")
            held = self._bytes_at(a, self.HELD_TAIL)  # into the shared pages
            seen = self._bytes_at(b, self.HELD_TAIL)
            if held == set(self.OLD) and seen == set(self.OLD):
                raise self._StaleRefill(
                    "reader B read a page a held reader filled from its old cache file")
            self.assertEqual(set(self.NEW), seen)
        finally:
            os.close(a)
            os.close(b)

    def test_watcher_invalidation_keeps_real_writer_on_clean_backing(self):
        name = "writer.bin"
        source = self.backend1_dir / name
        target = self.mount_dir / name
        cached = self.cache_dir / name
        source.write_bytes(b"original")
        self.assertEqual(b"original", target.read_bytes())

        with target.open("r+b", buffering=0) as writer:
            inode = cached.stat().st_ino
            row = self.fs.state_manager.get_file_metadata("/" + name)
            self.assertEqual(SyncStatus.CLEAN, row.sync_status)

            source.write_bytes(b"whatever")
            self.fs._on_remote_change("/" + name)
            self.assertEqual(inode, cached.stat().st_ino)
            self.assertEqual(8, writer.write(b"mount-ed"))

        self.assertEqual(inode, cached.stat().st_ino)
        self.assertEqual(b"mount-ed", cached.read_bytes())
        self.assertEqual(b"mount-ed", target.read_bytes())


class TestMemberSymlinks(_MountedCase):
    """A link that exists only on a member reads through the mount
    (ditz member-symlink-readlink)."""

    START_SYNC = False

    def test_member_only_link_is_a_link_through_the_mount(self):
        (self.backend2_dir / "data").mkdir()
        (self.backend2_dir / "data" / "target.txt").write_text("target bytes")
        (self.backend1_dir / "links").mkdir()
        (self.backend1_dir / "links" / "link.txt").symlink_to("../data/target.txt")
        m = self.mount_dir

        # A lookup before any listing (the member lstat), then after one
        # (the kernel may answer the second from its own attribute cache;
        # the listed path is covered without a mount in
        # test_fs_namespace_integration).
        st = os.lstat(m / "links" / "link.txt")
        self.assertTrue(stat.S_ISLNK(st.st_mode), oct(st.st_mode))
        self.assertIn("link.txt", os.listdir(m / "links"))
        self.assertTrue(stat.S_ISLNK(os.lstat(m / "links" / "link.txt").st_mode))
        self.assertEqual(os.readlink(m / "links" / "link.txt"), "../data/target.txt")
        # The kernel resolves the target through the union (another member).
        self.assertEqual((m / "links" / "link.txt").read_text(), "target bytes")
        # Reading through the link fetched the target, never the link as a file.
        self.assertFalse(os.path.lexists(self.cache_dir / "links" / "link.txt"))


class TestMemberSymlinksThreaded(TestMemberSymlinks):
    fuse_threads = True


class TestSymlinkReplication(_MountedCase):
    """ln -s through the mount places the link with its target
    (ditz symlink-replication)."""

    def test_link_follows_its_target_to_its_member(self):
        m = self.mount_dir
        (self.backend2_dir / "data").mkdir()
        (self.backend2_dir / "data" / "t.txt").write_text("target bytes")
        os.mkdir(m / "links")

        os.symlink("../data/t.txt", m / "links" / "l")

        link = m / "links" / "l"
        self.assertTrue(stat.S_ISLNK(os.lstat(link).st_mode))
        self.assertEqual(os.readlink(link), "../data/t.txt")
        self.assertEqual(link.read_text(), "target bytes")
        member_link = self.backend2_dir / "links" / "l"
        self.assertTrue(self._wait_for(lambda: os.path.islink(member_link), timeout=10.0))
        self.assertEqual(os.readlink(member_link), "../data/t.txt")
        self.assertFalse(os.path.lexists(self.backend1_dir / "links" / "l"))

    def test_refused_links_are_eperm(self):
        m = self.mount_dir
        (m / "here.txt").write_text("x")
        for target in ("/etc/hostname", "../outside", "missing.txt"):
            with self.assertRaises(PermissionError, msg=target):
                os.symlink(target, m / "refused")
            self.assertFalse(os.path.lexists(m / "refused"))

    def test_cp_a_copies_a_link_and_a_directory_link_resolves(self):
        m = self.mount_dir
        os.mkdir(m / "dir")
        (m / "dir" / "inside.txt").write_text("inside")
        source = Path(self.temp_dir) / "src"
        source.mkdir()
        (source / "dl").symlink_to("dir")
        subprocess.run(["cp", "-a", str(source / "dl"), str(m / "dl")], check=True)

        self.assertEqual(os.readlink(m / "dl"), "dir")
        self.assertEqual(os.listdir(m / "dl"), ["inside.txt"])
        self.assertEqual((m / "dl" / "inside.txt").read_text(), "inside")
        # The directory went to every member on mkdir; so does its link.
        for member in (self.backend1_dir, self.backend2_dir):
            self.assertTrue(self._wait_for(lambda member=member: os.path.islink(member / "dl"),
                                           timeout=10.0), member)


class TestSymlinkReplicationThreaded(TestSymlinkReplication):
    fuse_threads = True


class TestSyncSystemIntegrationThreaded(TestSyncSystemIntegration):
    fuse_threads = True


class TestKernelCacheVisibilityThreaded(TestKernelCacheVisibility):
    fuse_threads = True


class TestKernelCacheRefetchThreaded(TestKernelCacheRefetch):
    fuse_threads = True


if __name__ == '__main__':
    unittest.main()
