import os
import shutil
import tempfile
import threading
import unittest
from contextlib import contextmanager
from unittest.mock import patch
from pathlib import Path

from storage.backends.local import LocalBackend
from storage.cache import PARTIAL_SUFFIX, CacheManager
from storage.cache_metadata import read_cache_metadata, regular_file_stat
from storage.models import FileMetadata, SyncStatus
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
        self.assertEqual(self.cache_manager._scan_total_cache_size(), 5)
        self.assertEqual(self.cache_manager.get_total_cache_size(), 5)

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
        self._row("/swapped", recomputable=True, recompute_cmd="true")
        swapped = self.cache_dir / "swapped"
        swapped.write_bytes(b"y" * 10)
        real_check = regular_file_stat

        def check_then_swap(path):
            st = real_check(path)
            if Path(path) == swapped:
                swapped.unlink()
                swapped.symlink_to(self.outside)
            return st

        with patch("storage.cache.regular_file_stat", side_effect=check_then_swap):
            self.cache_manager.evict_recomputable_files(1)
        self.assertEqual(self.outside.stat().st_size, 100_000)

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
        self.assertEqual(self.cache_manager.get_total_cache_size(), 5)


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
        self.assertEqual(self.cache_manager.get_total_cache_size(), 10)

        self.cache_manager.delete_cached_file("/f.txt")
        self.assertEqual(self.cache_manager.get_total_cache_size(), 0)

    def test_mark_cache_size_dirty_reconciles_external_changes(self):
        source_path = self.backend_dir / "x.bin"
        source_path.write_bytes(b"1234567890")
        self.assertTrue(self.cache_manager.cache_file("/x.bin", self.backend))
        self.assertEqual(self.cache_manager.get_total_cache_size(), 10)

        cache_path = self.cache_manager.get_cache_path("/x.bin")
        with open(cache_path, "ab") as f:
            f.write(b"12345")

        self.cache_manager.mark_cache_size_dirty()
        self.assertEqual(self.cache_manager.get_total_cache_size(), 15)

    def test_clean_reads_do_not_rescan(self):
        source_path = self.backend_dir / "n.txt"
        source_path.write_bytes(b"1234")
        self.assertTrue(self.cache_manager.cache_file("/n.txt", self.backend))
        self.assertEqual(self.cache_manager.get_total_cache_size(), 4)

        def _raise_if_scanned():
            raise AssertionError("unexpected full cache scan")

        self.cache_manager._scan_total_cache_size = _raise_if_scanned

        self.assertEqual(self.cache_manager.get_total_cache_size(), 4)
        self.assertTrue(self.cache_manager.ensure_space(1))


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
            os.setxattr(self.cache_path, "user.storage.test", b"kept")
            self.cache_manager.mark_cache_size_dirty()
            writer = os.open(self.cache_path, os.O_RDWR)
        finally:
            self.backend.gate.set()
            worker.join(10)
        try:
            # The late fetch still reports the path cached, but did not
            # replace the inode the writer holds.
            self.assertEqual(self.results, [True])
            os.pwrite(writer, b"W", 0)
        finally:
            os.close(writer)
        self.assertEqual(self.cache_path.read_bytes(), b"W" + written[1:])
        self.assertEqual(os.getxattr(self.cache_path, "user.storage.test"), b"kept")
        self.assertEqual(self._partials(), [])
        self.assertEqual(self.cache_manager.get_total_cache_size(), len(written))

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

    def _clean_cached(self, name, content):
        cache_path = self.cache_dir / name
        cache_path.write_bytes(content)
        st = cache_path.lstat()
        self.state_manager.set_file_metadata(FileMetadata(
            virtual_path=f"/{name}", size=st.st_size, checksum="c", last_access=1.0,
            last_modified=st.st_mtime, sync_status=SyncStatus.CLEAN, replica_count=1,
            recomputable=False, recompute_cmd=None, encrypted=False,
            encryption_key_id=None,
        ))
        return cache_path

    def test_stale_clean_file_is_marked_dirty_and_kept(self):
        stale = self._clean_cached("stale.txt", b"v1")
        fresh = self._clean_cached("fresh.txt", b"v1")
        stale.write_bytes(b"v2, the only copy")  # row update lost

        self.cache_manager.evict_lru_files(bytes_needed=1)

        self.assertTrue(stale.exists())
        self.assertEqual(self.state_manager.get_file_metadata("/stale.txt").sync_status, SyncStatus.DIRTY)
        self.assertFalse(fresh.exists())

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
        snapshot = self.state_manager.get_lru_files(limit=10)
        # A write lands after the snapshot was taken, through the real path.
        turned.write_bytes(b"v2")
        self.cache_manager.mark_file_dirty("/turned.txt", turned)

        with patch.object(self.state_manager, "get_lru_files", return_value=snapshot):
            self.cache_manager.evict_lru_files(bytes_needed=1)

        self.assertTrue(turned.exists())
        self.assertEqual(self.state_manager.get_file_metadata("/turned.txt").sync_status, SyncStatus.DIRTY)
