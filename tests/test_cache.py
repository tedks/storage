import errno
import json
import os
import shutil
import tempfile
import threading
import unittest
from contextlib import contextmanager
from dataclasses import replace
from unittest.mock import patch
from pathlib import Path

from storage.backends.local import LocalBackend
from storage.cache import PARTIAL_SUFFIX, CacheManager
from storage.cache_metadata import (
    MEMBER_ATTRS_XATTR, XATTR_CACHE_METADATA_NAME, CacheMetadataRecord, read_cache_metadata,
)
from storage.models import FileLocation, FileMetadata, SyncStatus
from storage.state import StateManager


class TestCacheLinks(unittest.TestCase):
    """Symlinks in the cache tree are not cached copies
    (ditz cache-scans-follow-symlinks)."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.cache_dir.mkdir()
        self.outside = Path(self.temp_dir) / "outside.bin"
        self.outside.write_bytes(b"x" * 100_000)
        (self.cache_dir / "small.txt").write_bytes(b"12345")
        (self.cache_dir / "big-link").symlink_to(self.outside)
        (self.cache_dir / "dangling").symlink_to(Path(self.temp_dir) / "nowhere")
        self.state_manager = StateManager(str(Path(self.temp_dir) / "state.db"))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 1024 * 1024)

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_size_scan_does_not_charge_a_link_target(self):
        self.assertEqual(self.cache_manager._scan_total_cache_size(), self.cache_manager.block_size)
        self.assertEqual(self.cache_manager.get_total_cache_size(), self.cache_manager.block_size)

    def test_a_link_is_not_cached(self):
        self.assertTrue(self.cache_manager.is_cached("/small.txt"))
        self.assertFalse(self.cache_manager.is_cached("/big-link"))
        self.assertEqual(self.cache_manager.get_cached_size("/big-link"), 0)

    def test_sync_cache_metadata_skips_a_link(self):
        metadata = FileMetadata(
            virtual_path="/big-link", size=1, checksum=None, last_access=1.0,
            last_modified=1.0, sync_status=SyncStatus.CLEAN, replica_count=1,
            recomputable=False, recompute_cmd=None, encrypted=False, encryption_key_id=None,
        )
        self.assertFalse(self.cache_manager.sync_cache_metadata("/big-link", metadata))
        self.assertEqual(os.listxattr(str(self.outside)), [])

    def _row(self, virtual_path, **changes):
        fields = dict(
            virtual_path=virtual_path, size=100_000, checksum=None, last_access=1.0,
            last_modified=1.0, sync_status=SyncStatus.CLEAN, replica_count=1,
            recomputable=False, recompute_cmd=None, encrypted=False, encryption_key_id=None,
        )
        fields.update(changes)
        self.state_manager.set_file_metadata(FileMetadata(**fields))

    def test_recomputable_eviction_never_truncates_through_a_link(self):
        self._row("/big-link", recomputable=True, recompute_cmd="true")
        self.cache_manager.evict_recomputable_files(1)
        self.assertEqual(self.outside.stat().st_size, 100_000)
        self.assertTrue((self.cache_dir / "big-link").is_symlink())

    def test_recomputable_eviction_refuses_a_link_swapped_in_after_the_check(self):
        # The link replaces the file between the lstat and the open.
        swapped = self.cache_dir / "swapped"
        swapped.write_bytes(b"y" * 10)
        self._row("/swapped", recomputable=True, recompute_cmd="true",
                  size=10, last_modified=swapped.stat().st_mtime)
        real_open = os.open

        def swap_then_open(path, flags, *args, **kwargs):
            if Path(path) == swapped and flags & os.O_WRONLY:
                swapped.unlink()
                swapped.symlink_to(self.outside)
            return real_open(path, flags, *args, **kwargs)

        with patch("storage.cache.os.open", side_effect=swap_then_open):
            self.cache_manager.evict_recomputable_files(1)
        self.assertTrue(swapped.is_symlink())
        self.assertEqual(self.outside.stat().st_size, 100_000)

    def test_recomputable_eviction_refuses_a_file_swapped_in_after_the_check(self):
        swapped = self.cache_dir / "swapped"
        swapped.write_bytes(b"y" * 10)
        self._row("/swapped", recomputable=True, recompute_cmd="true",
                  size=10, last_modified=swapped.stat().st_mtime)
        real_open = os.open

        def swap_then_open(path, flags, *args, **kwargs):
            if Path(path) == swapped and flags & os.O_WRONLY:
                replacement = self.cache_dir / "replacement"
                replacement.write_bytes(b"z" * 20)
                os.replace(replacement, swapped)
            return real_open(path, flags, *args, **kwargs)

        with patch("storage.cache.os.open", side_effect=swap_then_open):
            self.cache_manager.evict_recomputable_files(1)
        self.assertEqual(swapped.read_bytes(), b"z" * 20)

    def test_lru_eviction_skips_a_link(self):
        self._row("/big-link")
        self.cache_manager.evict_lru_files(1)
        self.assertTrue((self.cache_dir / "big-link").is_symlink())
        self.assertEqual(self.outside.stat().st_size, 100_000)

    def test_delete_removes_a_dangling_link_without_touching_the_size(self):
        self.cache_manager.delete_cached_file("/dangling")
        self.assertFalse(os.path.lexists(self.cache_dir / "dangling"))
        self.cache_manager.delete_cached_file("/big-link")
        self.assertFalse(os.path.lexists(self.cache_dir / "big-link"))
        self.assertTrue(self.outside.exists())
        self.assertEqual(self.cache_manager.get_total_cache_size(), self.cache_manager.block_size)


class TestCacheAccounting(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend_dir = Path(self.temp_dir) / "backend"
        self.db_path = Path(self.temp_dir) / "state.db"
        self.cache_dir.mkdir()
        self.backend_dir.mkdir()

        self.state_manager = StateManager(str(self.db_path))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 1024 * 1024)
        self.backend = LocalBackend("backend", str(self.backend_dir))

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_cache_file_and_delete_update_tracked_size(self):
        source_path = self.backend_dir / "f.txt"
        source_path.write_bytes(b"abcdefghij")

        self.assertEqual(self.cache_manager.get_total_cache_size(), 0)
        self.assertTrue(self.cache_manager.cache_file("/f.txt", self.backend))
        self.assertEqual(self.cache_manager.get_total_cache_size(), self.cache_manager.block_size)

        self.cache_manager.delete_cached_file("/f.txt")
        self.assertEqual(self.cache_manager.get_total_cache_size(), 0)

    def test_mark_cache_size_dirty_reconciles_external_changes(self):
        source_path = self.backend_dir / "x.bin"
        source_path.write_bytes(b"1234567890")
        self.assertTrue(self.cache_manager.cache_file("/x.bin", self.backend))
        block = self.cache_manager.block_size
        self.assertEqual(self.cache_manager.get_total_cache_size(), block)

        cache_path = self.cache_manager.get_cache_path("/x.bin")
        with open(cache_path, "ab") as f:
            f.write(b"1" * block)

        self.cache_manager.mark_cache_size_dirty()
        self.assertEqual(self.cache_manager.get_total_cache_size(), 2 * block)

    def test_clean_reads_do_not_rescan(self):
        source_path = self.backend_dir / "n.txt"
        source_path.write_bytes(b"1234")
        self.assertTrue(self.cache_manager.cache_file("/n.txt", self.backend))
        self.assertEqual(self.cache_manager.get_total_cache_size(), self.cache_manager.block_size)

        def _raise_if_scanned():
            raise AssertionError("unexpected full cache scan")

        self.cache_manager._scan_total_cache_size = _raise_if_scanned

        self.assertEqual(self.cache_manager.get_total_cache_size(), self.cache_manager.block_size)
        self.assertTrue(self.cache_manager.ensure_space(1))

    @contextmanager
    def _partial(self, name):
        path = self.cache_dir / (name + PARTIAL_SUFFIX)
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.ftruncate(fd, 1 << 30)
            yield path, fd
        finally:
            self.cache_manager.close_streaming_fd(fd)

    def test_sparse_budget_counts_new_blocks_once_and_survives_a_scan(self):
        manager = self.cache_manager
        block = manager.block_size
        manager.max_size_bytes = 2 * block
        with self._partial("sparse") as (path, fd):
            os.setxattr(path, "user.storage.stream", b"x" * 450)
            manager.mark_cache_size_dirty()
            self.assertEqual(manager.get_total_cache_size(), 0)
            far = (1 << 29) + block
            with patch.object(manager, "_scan_total_cache_size", side_effect=AssertionError("repeated scan")):
                self.assertTrue(manager.write_streaming_chunk(fd, far, b"ABC"))
                self.assertEqual(manager.get_total_cache_size(), block)
                self.assertTrue(manager.write_streaming_chunk(fd, far + 1, b"xyz"))
                self.assertEqual(manager.get_total_cache_size(), block)
                self.assertTrue(manager.write_streaming_chunk(fd, 0, b"near"))
                self.assertEqual(manager.get_total_cache_size(), 2 * block)
                self.assertFalse(manager.write_streaming_chunk(fd, block, b"overflow"))
                self.assertEqual(os.pread(fd, 4, far), b"Axyz")
                self.assertEqual(os.pread(fd, 8, block), b"\0" * 8)
            manager.mark_cache_size_dirty()
            self.assertEqual(manager.get_total_cache_size(), 2 * block)
            path.unlink()
            manager.mark_cache_size_dirty()
            self.assertEqual(manager.get_total_cache_size(), 2 * block)
        self.assertEqual(manager.get_total_cache_size(), 0)

    def test_concurrent_chunks_compete_for_one_allocation(self):
        manager = self.cache_manager
        manager.max_size_bytes = manager.block_size
        ready = threading.Barrier(3)
        results = []

        def write(fd):
            ready.wait(timeout=3)
            results.append(manager.write_streaming_chunk(fd, 0, b"x"))

        with self._partial("left") as (_, left), self._partial("right") as (_, right):
            workers = [threading.Thread(target=write, args=(fd,)) for fd in (left, right)]
            for worker in workers:
                worker.start()
            ready.wait(timeout=3)
            for worker in workers:
                worker.join(timeout=3)
                self.assertFalse(worker.is_alive())
            self.assertEqual(sorted(results), [False, True])
            self.assertEqual(manager.get_total_cache_size(), manager.block_size)

    def test_published_inode_and_restart_count_sparse_data_once(self):
        manager = self.cache_manager
        block = manager.block_size
        final = self.cache_dir / "published"
        with self._partial("sparse") as (temporary, fd):
            self.assertTrue(manager.write_streaming_chunk(fd, 1 << 29, b"far"))
            os.link(temporary, final)
            manager.mark_cache_size_dirty()
            self.assertEqual(manager.get_total_cache_size(), block)
            temporary.unlink()
            manager.publish_streaming_fd(fd)
            self.assertEqual(manager.get_total_cache_size(), block)

        restarted = CacheManager(self.cache_dir, self.state_manager, block)
        self.assertEqual(restarted.get_total_cache_size(), block)
        self.assertEqual(restarted.get_free_space_needed(block), block)

    def test_failed_partial_write_charges_only_the_blocks_it_wrote(self):
        manager = self.cache_manager
        block = manager.block_size
        manager.max_size_bytes = 2 * block
        real_pwrite = os.pwrite

        def short_then_fail(fd, data, offset):
            if offset == 0:
                return real_pwrite(fd, data[:block], offset)
            raise OSError(errno.ENOSPC, "short backing write")

        with self._partial("failed") as (path, fd):
            with patch("storage.cache.os.pwrite", side_effect=short_then_fail):
                with self.assertRaises(OSError):
                    manager.write_streaming_chunk(fd, 0, b"x" * (2 * block))
            self.assertEqual(manager.get_total_cache_size(), block)
            path.unlink()
        self.assertEqual(manager.get_total_cache_size(), 0)

    def test_direct_writer_uses_space_before_the_next_stream_grant(self):
        manager = self.cache_manager
        block = manager.block_size
        manager.max_size_bytes = block
        with self._partial("new") as (_, fd):
            with manager.cache_size_change():
                (self.cache_dir / "dirty").write_bytes(b"local content")
            self.assertFalse(manager.write_streaming_chunk(fd, 0, b"x"))
            self.assertEqual(manager.get_total_cache_size(), block)
            self.assertEqual((self.cache_dir / "dirty").read_bytes(), b"local content")


if __name__ == "__main__":
    unittest.main()


class _GatedBackend(LocalBackend):
    """A member whose copy_to_local writes half the file, then waits on a gate."""

    def __init__(self, name, root):
        super().__init__(name, root)
        self.started = threading.Event()
        self.gate = threading.Event()
        self.destinations = []
        self.fail = False

    def copy_to_local(self, virtual_path, local_path):
        self.destinations.append(Path(local_path))
        data = self._resolve_path(virtual_path).read_bytes()
        with open(local_path, "wb") as f:
            f.write(data[: len(data) // 2])
            f.flush()
            self.started.set()
            assert self.gate.wait(10), "gate never opened"
            if self.fail:
                raise OSError("member went away mid-transfer")
            f.write(data[len(data) // 2:])


class TestFetchThroughTempFile(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend_dir = Path(self.temp_dir) / "backend"
        self.cache_dir.mkdir()
        self.backend_dir.mkdir()
        self.state_manager = StateManager(str(Path(self.temp_dir) / "state.db"))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 1024 * 1024)
        self.backend = _GatedBackend("backend", str(self.backend_dir))
        self.payload = bytes(range(256)) * 64
        (self.backend_dir / "f.bin").write_bytes(self.payload)
        self.cache_path = self.cache_manager.get_cache_path("/f.bin")
        self.results = []

    def tearDown(self):
        self.backend.gate.set()
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _start_fetch(self):
        worker = threading.Thread(
            target=lambda: self.results.append(self.cache_manager.cache_file("/f.bin", self.backend)),
        )
        worker.start()
        self.assertTrue(self.backend.started.wait(10))
        return worker

    def _partials(self):
        return sorted(p.name for p in self.cache_dir.rglob(f"*{PARTIAL_SUFFIX}"))

    def test_half_fetched_file_is_never_seen_as_cached(self):
        worker = self._start_fetch()
        try:
            # Mid-transfer: the cache path does not exist, the bytes so far
            # are in a hidden sibling, and the cache lock is free.
            self.assertFalse(self.cache_manager.is_cached("/f.bin"))
            self.assertFalse(self.cache_path.exists())
            temp = self.backend.destinations[0]
            self.assertNotEqual(temp, self.cache_path)
            self.assertEqual(temp.parent, self.cache_path.parent)
            self.assertTrue(temp.name.endswith(PARTIAL_SUFFIX))
            self.assertTrue(self.cache_manager._lock.acquire(timeout=1))
            self.cache_manager._lock.release()
            # The transfer's size is promised to it.
            self.assertEqual(
                self.cache_manager.get_free_space_needed(1024 * 1024),
                len(self.payload),
            )
        finally:
            self.backend.gate.set()
            worker.join(10)
        self.assertEqual(self.results, [True])
        self.assertEqual(self.cache_path.read_bytes(), self.payload)
        self.assertEqual(self._partials(), [])
        self.assertEqual(self.cache_manager.get_total_cache_size(), len(self.payload))
        self.assertEqual(self.cache_manager.get_free_space_needed(1024 * 1024), len(self.payload))

    def test_failed_transfer_leaves_nothing_behind(self):
        self.backend.fail = True
        worker = self._start_fetch()
        self.backend.gate.set()
        worker.join(10)
        self.assertEqual(self.results, [False])
        self.assertFalse(self.cache_path.exists())
        self.assertEqual(self._partials(), [])
        self.assertEqual(self.cache_manager.get_total_cache_size(), 0)
        self.assertEqual(self.cache_manager.get_free_space_needed(1024 * 1024), 0)

    def test_late_fetch_keeps_a_file_installed_and_written_meanwhile(self):
        worker = self._start_fetch()
        try:
            # While this fetch is gated, another installs the path and a
            # writer changes it (and its xattrs) through an open descriptor.
            written = b"written through the mount"
            self.cache_path.write_bytes(written)
            os.setxattr(self.cache_path, MEMBER_ATTRS_XATTR, b"winning inode")
            os.utime(self.cache_path, (1_650_000_000, 1_650_000_000))
            original_mtime = self.cache_path.stat().st_mtime_ns
            self.cache_manager.mark_cache_size_dirty()
            writer = os.open(self.cache_path, os.O_RDWR)
        finally:
            self.backend.gate.set()
            worker.join(10)
        try:
            # The late fetch still reports the path cached, but did not
            # replace the inode the writer holds.
            self.assertEqual(self.results, [True])
            self.assertEqual(self.cache_path.stat().st_mtime_ns, original_mtime)
            os.pwrite(writer, b"W", 0)
        finally:
            os.close(writer)
        self.assertEqual(self.cache_path.read_bytes(), b"W" + written[1:])
        self.assertEqual(os.getxattr(self.cache_path, MEMBER_ATTRS_XATTR), b"winning inode")
        self.assertEqual(self._partials(), [])
        self.assertEqual(self.cache_manager.get_total_cache_size(), self.cache_manager.block_size)

    def test_fetch_onto_a_directory_fails(self):
        self.cache_path.mkdir()
        worker = self._start_fetch()
        self.backend.gate.set()
        worker.join(10)
        self.assertEqual(self.results, [False])
        self.assertEqual(self._partials(), [])

    def test_failed_transfer_after_rescan_is_not_counted(self):
        self.backend.fail = True
        worker = self._start_fetch()
        try:
            self.cache_manager.mark_cache_size_dirty()
            self.assertEqual(self.cache_manager.get_total_cache_size(), len(self.payload) // 2)
        finally:
            self.backend.gate.set()
            worker.join(10)
        self.assertEqual(self.results, [False])
        self.assertEqual(self.cache_manager.get_total_cache_size(), 0)

    def test_rescan_during_transfer_is_not_double_counted(self):
        worker = self._start_fetch()
        try:
            # A rescan mid-transfer counts the temp file's bytes so far.
            self.cache_manager.mark_cache_size_dirty()
            self.assertEqual(self.cache_manager.get_total_cache_size(), len(self.payload) // 2)
        finally:
            self.backend.gate.set()
            worker.join(10)
        self.assertEqual(self.results, [True])
        self.assertEqual(self.cache_manager.get_total_cache_size(), len(self.payload))

    def test_partial_cannot_spend_a_whole_copy_reservation(self):
        manager = self.cache_manager
        manager.max_size_bytes = len(self.payload)
        self.backend.fail = True
        worker = self._start_fetch()
        fd = os.open(self.cache_dir / ("range" + PARTIAL_SUFFIX), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            os.ftruncate(fd, 1 << 30)
            self.assertFalse(manager.write_streaming_chunk(fd, 0, b"x"))
        finally:
            self.backend.gate.set()
            worker.join(10)
        try:
            self.assertEqual(self.results, [False])
            self.assertTrue(manager.write_streaming_chunk(fd, 0, b"x"))
            self.assertEqual(manager.get_total_cache_size(), manager.block_size)
        finally:
            manager.close_streaming_fd(fd)


class TestStoreFileMetadata(unittest.TestCase):
    """The xattr mirror follows the row as stored, not the caller's snapshot."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.cache_dir.mkdir()
        self.state_manager = StateManager(str(Path(self.temp_dir) / "state.db"))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 1024 * 1024)

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_stale_clean_snapshot_mirrors_the_stored_dirty_row(self):
        cache_path = self.cache_dir / "f.txt"
        cache_path.write_bytes(b"v1")
        self.state_manager.set_file_metadata(FileMetadata(
            virtual_path="/f.txt", size=2, checksum=None, last_access=1.0,
            last_modified=1.0, sync_status=SyncStatus.CLEAN, replica_count=1,
            recomputable=False, recompute_cmd=None, encrypted=False,
            encryption_key_id=None,
        ))
        snapshot = self.state_manager.get_file_metadata("/f.txt")
        # A write lands after the snapshot was taken.
        cache_path.write_bytes(b"v2 longer")
        self.state_manager.mark_dirty_from_cache_file("/f.txt", cache_path)

        stored = self.cache_manager.store_file_metadata(snapshot, cache_path=cache_path)

        self.assertEqual(stored.sync_status, SyncStatus.DIRTY)
        self.assertEqual(stored.size, 9)
        record = read_cache_metadata(cache_path)
        self.assertIsNotNone(record)
        self.assertEqual(record.sync_status, SyncStatus.DIRTY)
        self.assertEqual(record.size, 9)


class TestEvictionSkipsStaleClean(unittest.TestCase):
    """LRU eviction must not unlink a file written after its CLEAN row."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.cache_dir.mkdir()
        self.state_manager = StateManager(str(Path(self.temp_dir) / "state.db"))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 1024 * 1024)

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _clean_cached(self, name, content, *, last_access=1.0, recomputable=False):
        cache_path = self.cache_dir / name
        cache_path.write_bytes(content)
        st = cache_path.lstat()
        self.state_manager.set_file_metadata(FileMetadata(
            virtual_path=f"/{name}", size=st.st_size, checksum="c", last_access=last_access,
            last_modified=st.st_mtime, sync_status=SyncStatus.CLEAN, replica_count=1,
            recomputable=recomputable, recompute_cmd="true" if recomputable else None,
            encrypted=False, encryption_key_id=None,
        ))
        return cache_path

    def test_stale_clean_file_is_marked_dirty_and_kept(self):
        stale = self._clean_cached("stale.txt", b"v1")
        fresh = self._clean_cached("fresh.txt", b"v1", last_access=2.0)
        stale.write_bytes(b"v2, the only copy")  # row update lost

        self.cache_manager.evict_lru_files(bytes_needed=1)

        self.assertTrue(stale.exists())
        self.assertEqual(self.state_manager.get_file_metadata("/stale.txt").sync_status, SyncStatus.DIRTY)
        self.assertFalse(fresh.exists())

    def _mirror(self, cache_path, **stat):
        """Write the metadata xattr from the row, at the given stat."""
        row = self.state_manager.get_file_metadata("/" + cache_path.name)
        record = CacheMetadataRecord.from_file_metadata(row, **stat)
        os.setxattr(cache_path, XATTR_CACHE_METADATA_NAME,
                    json.dumps(record.to_dict(), sort_keys=True).encode())

    def test_xattr_recording_another_stat_marks_dirty_and_keeps_the_copy(self):
        """The row matches the file but the xattr does not: either record
        disagreeing with the live stat makes the file dirty (AGENTS.md)."""
        written = self._clean_cached("written.txt", b"v1")
        fresh = self._clean_cached("fresh.txt", b"v1", last_access=2.0)
        st = written.stat()
        self._mirror(written, stat_size=st.st_size + 1, stat_mtime=st.st_mtime)
        self._mirror(fresh, stat_size=fresh.stat().st_size, stat_mtime=fresh.stat().st_mtime)

        self.assertTrue(self.cache_manager.evict_lru_files(bytes_needed=1))

        self.assertEqual(written.read_bytes(), b"v1")
        self.assertEqual(self.state_manager.get_file_metadata("/written.txt").sync_status,
                         SyncStatus.DIRTY)
        self.assertFalse(fresh.exists())

    def test_dry_run_does_not_count_or_mark_an_xattr_stale_copy(self):
        written = self._clean_cached("written.txt", b"v1")
        st = written.stat()
        self._mirror(written, stat_size=st.st_size, stat_mtime=st.st_mtime + 1)
        manager = self.cache_manager
        manager.mark_cache_size_dirty()
        manager.max_size_bytes = manager.get_total_cache_size()

        self.assertFalse(manager.ensure_space(manager.block_size))
        self.assertEqual(written.read_bytes(), b"v1")
        self.assertEqual(self.state_manager.get_file_metadata("/written.txt").sync_status,
                         SyncStatus.CLEAN)

    def test_unreadable_metadata_xattr_keeps_the_copy_unmarked(self):
        garbled = self._clean_cached("garbled.txt", b"v1")
        os.setxattr(garbled, XATTR_CACHE_METADATA_NAME, b"{not json")
        other = self._clean_cached("other.txt", b"v1", last_access=2.0)

        self.assertTrue(self.cache_manager.evict_lru_files(bytes_needed=1))

        self.assertEqual(garbled.read_bytes(), b"v1")
        self.assertEqual(self.state_manager.get_file_metadata("/garbled.txt").sync_status,
                         SyncStatus.CLEAN)
        self.assertFalse(other.exists())

    def test_copy_whose_xattr_matches_is_evicted(self):
        mirrored = self._clean_cached("mirrored.txt", b"v1")
        row = self.state_manager.get_file_metadata("/mirrored.txt")
        self.cache_manager.sync_cache_metadata("/mirrored.txt", row, cache_path=mirrored)
        self.assertIsNotNone(read_cache_metadata(mirrored))

        self.assertTrue(self.cache_manager.evict_lru_files(bytes_needed=1))
        self.assertFalse(mirrored.exists())

    def test_path_with_an_open_descriptor_is_kept(self):
        held = self._clean_cached("held.txt", b"v1")
        other = self._clean_cached("other.txt", b"v1")
        guard_calls = []

        @contextmanager
        def guard(path):
            guard_calls.append(path)
            yield path == "/held.txt"

        self.cache_manager.descriptor_guard = guard

        self.cache_manager.evict_lru_files(bytes_needed=1)

        self.assertTrue(held.exists())
        self.assertFalse(other.exists())
        self.assertEqual(sorted(guard_calls), ["/held.txt", "/other.txt"])

    def test_file_made_dirty_after_selection_is_kept(self):
        """The LRU list is a snapshot; the row is re-checked before the unlink."""
        turned = self._clean_cached("turned.txt", b"v1")
        snapshot = list(self.state_manager.iter_lru_paths())
        # A write lands after the snapshot was taken, through the real path.
        turned.write_bytes(b"v2")
        self.cache_manager.mark_file_dirty("/turned.txt", turned)

        with patch.object(self.state_manager, "iter_lru_paths", return_value=snapshot):
            self.cache_manager.evict_lru_files(bytes_needed=1)

        self.assertTrue(turned.exists())
        self.assertEqual(self.state_manager.get_file_metadata("/turned.txt").sync_status, SyncStatus.DIRTY)

    def test_missing_rows_cannot_hide_the_oldest_cached_copy_under_pressure(self):
        oldest = self._clean_cached("oldest.txt", b"old", last_access=2.0)
        newer = self._clean_cached("newer.txt", b"new", last_access=3.0)
        template = self.state_manager.get_file_metadata("/oldest.txt")
        for i in range(1001):
            # Previously evicted rows share one timestamp, spanning a page.
            self.state_manager.set_file_metadata(replace(
                template, virtual_path=f"/missing-{i:04d}", last_access=1.0,
            ))

        self.cache_manager.mark_cache_size_dirty()
        self.cache_manager.max_size_bytes = self.cache_manager.get_total_cache_size()
        self.assertTrue(self.cache_manager.ensure_space(1))
        self.assertFalse(oldest.exists())
        self.assertEqual(newer.read_bytes(), b"new")

    def test_recomputable_pressure_preserves_pinned_and_dirty_bytes(self):
        ordinary = self._clean_cached("ordinary.txt", b"one", last_access=20.0)
        oldest = self._clean_cached("older.txt", b"older", last_access=4.0, recomputable=True)
        newer = self._clean_cached("newer.txt", b"n", last_access=5.0, recomputable=True)
        dirty = self._clean_cached("dirty.txt", b"d", last_access=1.0, recomputable=True)
        held = self._clean_cached("held.txt", b"h", last_access=2.0, recomputable=True)
        pinned = self._clean_cached("pinned.txt", b"p", last_access=3.0, recomputable=True)
        self.cache_manager.mark_file_dirty("/dirty.txt", dirty)
        self.cache_manager.member_repairs.record("/original.txt", "/pinned.txt", ["member"], "offline")
        os.setxattr(oldest, "user.test-recompute", b"preserve")

        @contextmanager
        def guard(path):
            yield path == "/held.txt"

        self.cache_manager.descriptor_guard = guard
        self.cache_manager.mark_cache_size_dirty()
        self.cache_manager.max_size_bytes = self.cache_manager.get_total_cache_size()
        self.assertTrue(self.cache_manager.evict_lru_files(1))
        self.assertTrue(self.cache_manager.evict_recomputable_files(1))
        self.assertFalse(ordinary.exists())
        self.assertEqual(oldest.read_bytes(), b"")
        self.assertEqual(os.getxattr(oldest, "user.test-recompute"), b"preserve")
        row = self.state_manager.get_file_metadata("/older.txt")
        self.assertEqual((row.size, row.last_modified), (0, oldest.stat().st_mtime))
        self.assertEqual(row.sync_status, SyncStatus.CLEAN)
        self.assertEqual(newer.read_bytes(), b"n")
        self.assertEqual(dirty.read_bytes(), b"d")
        self.assertEqual(held.read_bytes(), b"h")
        self.assertEqual(pinned.read_bytes(), b"p")

    def test_cache_pressure_uses_global_recency_among_recoverable_copies(self):
        only_recipe = self._clean_cached("recipe.txt", b"unique", last_access=1.0, recomputable=True)
        recoverable = self._clean_cached("recoverable.txt", b"member", last_access=2.0, recomputable=True)
        ordinary = self._clean_cached("ordinary.txt", b"ordinary", last_access=3.0)
        self.state_manager.add_file_location(FileLocation("/recoverable.txt", "member", "c", 1.0, False))
        manager = self.cache_manager
        manager.mark_cache_size_dirty()
        manager.max_size_bytes = manager.get_total_cache_size()

        self.assertTrue(manager.ensure_space(manager.block_size))
        self.assertFalse(recoverable.exists())
        self.assertEqual(ordinary.read_bytes(), b"ordinary")
        self.assertEqual(only_recipe.read_bytes(), b"unique")
        row = self.state_manager.get_file_metadata("/recoverable.txt")
        self.assertEqual(row.recompute_cmd, "true")
        self.assertEqual([location.backend_name for location in self.state_manager.get_file_locations(
            "/recoverable.txt",
        )], ["member"])

    def test_insufficient_victims_keep_existing_clean_bytes(self):
        ordinary = self._clean_cached("ordinary.txt", b"keep", last_access=1.0)
        dirty = self._clean_cached("dirty.txt", b"unsynced", last_access=2.0)
        self.cache_manager.mark_file_dirty("/dirty.txt", dirty)
        self.cache_manager.mark_cache_size_dirty()
        self.cache_manager.max_size_bytes = self.cache_manager.get_total_cache_size()

        self.assertFalse(self.cache_manager.ensure_space(2 * self.cache_manager.block_size))
        self.assertEqual(ordinary.read_bytes(), b"keep")
        self.assertEqual(dirty.read_bytes(), b"unsynced")
