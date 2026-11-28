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


class _ReportedBlocks:
    """Report chosen st_blocks for chosen inodes, as a compressing
    filesystem or an external xattr block would, while the extents
    (SEEK_DATA/SEEK_HOLE) stay the real ones. Patches os.fstat and
    os.lstat for the duration of the with block."""

    def __init__(self):
        self.sectors = {}
        self._real_fstat = os.fstat
        self._real_lstat = os.lstat

    def set(self, path, allocated_bytes):
        st = self._real_lstat(path)
        self.sectors[(st.st_dev, st.st_ino)] = allocated_bytes // 512

    def _wrap(self, st):
        sectors = self.sectors.get((st.st_dev, st.st_ino))
        return st if sectors is None else _StatWithBlocks(st, sectors)

    def __enter__(self):
        self._patches = [
            patch("os.fstat", lambda fd: self._wrap(self._real_fstat(fd))),
            patch("os.lstat", lambda path, *a, **k: self._wrap(self._real_lstat(path, *a, **k))),
        ]
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in reversed(self._patches):
            p.stop()


class _StatWithBlocks:
    def __init__(self, st, sectors):
        self._st = st
        self.st_blocks = sectors

    def __getattr__(self, name):
        return getattr(self._st, name)


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

    def _no_rescan(self):
        return patch.object(self.cache_manager, "_scan_total_cache_size",
                            side_effect=AssertionError("unexpected full cache scan"))

    def test_direct_writes_and_truncates_are_charged_without_a_rescan(self):
        manager = self.cache_manager
        block = manager.block_size
        path = self.cache_dir / "local.bin"
        with self._no_rescan():
            with manager.cache_size_change(path):
                path.touch()
            fd = os.open(path, os.O_RDWR)
            try:
                with manager.cache_size_change(fd=fd, span=(0, 3 * block)):
                    os.pwrite(fd, b"x" * (3 * block), 0)
                self.assertEqual(manager.get_total_cache_size(), 3 * block)
                # An overwrite allocates nothing; a write into a hole one block.
                with manager.cache_size_change(fd=fd, span=(10, 20)):
                    os.pwrite(fd, b"y" * 10, 10)
                with manager.cache_size_change(fd=fd, span=(9 * block + 1, 9 * block + 2)):
                    os.pwrite(fd, b"z", 9 * block + 1)
                self.assertEqual(manager.get_total_cache_size(), 4 * block)
                with manager.cache_size_change(path):
                    os.truncate(path, block)
                self.assertEqual(manager.get_total_cache_size(), block)
            finally:
                os.close(fd)
        manager.mark_cache_size_dirty()
        self.assertEqual(manager.get_total_cache_size(), block)

    def test_a_hard_linked_or_unknown_target_falls_back_to_a_rescan(self):
        manager = self.cache_manager
        block = manager.block_size
        path = self.cache_dir / "linked.bin"
        path.write_bytes(b"a")
        os.link(path, self.cache_dir / "alias.bin")
        manager.mark_cache_size_dirty()
        self.assertEqual(manager.get_total_cache_size(), block)
        with manager.cache_size_change(path):
            with open(path, "ab") as f:
                f.write(b"b" * block)
        self.assertTrue(manager._size_dirty)
        self.assertEqual(manager.get_total_cache_size(), 2 * block)
        with manager.cache_size_change():
            (self.cache_dir / "other.bin").write_bytes(b"c")
        self.assertTrue(manager._size_dirty)
        self.assertEqual(manager.get_total_cache_size(), 3 * block)

    def _fill_with_dirty_bytes(self):
        manager = self.cache_manager
        dirty = self.cache_dir / "dirty.bin"
        dirty.write_bytes(b"d")
        self.state_manager.set_file_metadata(FileMetadata(
            virtual_path="/dirty.bin", size=1, checksum=None, last_access=1.0,
            last_modified=dirty.stat().st_mtime, sync_status=SyncStatus.DIRTY, replica_count=1,
            recomputable=False, recompute_cmd=None, encrypted=False, encryption_key_id=None,
        ))
        manager.mark_cache_size_dirty()
        manager.max_size_bytes = manager.get_total_cache_size()

    def test_a_victim_shortfall_is_remembered_until_something_changes(self):
        manager = self.cache_manager
        block = manager.block_size
        self._fill_with_dirty_bytes()
        walks = []
        real = manager._evict_by_access

        def counting(*args, **kwargs):
            walks.append(kwargs.get("dry_run", False))
            return real(*args, **kwargs)

        with patch.object(manager, "_evict_by_access", side_effect=counting):
            self.assertFalse(manager.ensure_space(block))
            self.assertFalse(manager.ensure_space(block))
            self.assertEqual(walks, [True])
            # A descriptor closed: walk again.
            manager.note_evictable_change()
            self.assertFalse(manager.ensure_space(block))
            self.assertEqual(walks, [True, True])
            # An index row changed (a sync published CLEAN, say): walk again.
            self.state_manager.update_access_time("/dirty.bin", accessed_at=5.0)
            self.state_manager._flush_access_updates(force=True)
            self.assertFalse(manager.ensure_space(block))
            self.assertEqual(walks, [True, True, True])
            # And once the shortfall is old.
            with patch.object(manager, "SHORTFALL_TTL_SECONDS", 0):
                self.assertFalse(manager.ensure_space(block))
            self.assertEqual(walks, [True, True, True, True])

    def test_a_remembered_shortfall_still_lets_a_copy_turn_clean(self):
        manager = self.cache_manager
        block = manager.block_size
        self._fill_with_dirty_bytes()
        self.assertFalse(manager.ensure_space(block))
        row = self.state_manager.get_file_metadata("/dirty.bin")
        # A sync publishes CLEAN with the member it reached.
        self.state_manager.add_file_location(FileLocation("/dirty.bin", "member", "c", 1.0, False))
        self.assertTrue(self.state_manager.publish_sync_status(
            "/dirty.bin", row.dirty_generation, SyncStatus.CLEAN))
        self.assertTrue(manager.ensure_space(block))
        self.assertFalse((self.cache_dir / "dirty.bin").exists())

    def test_first_grant_refused_when_extents_exceed_allocation(self):
        """A filesystem that reports holes as data (the generic llseek)
        cannot be charged: the first grant refuses, logging once."""
        manager = self.cache_manager

        def holes_as_data(fd, start, end):
            return [(start, end)] if start < end else []

        with self._partial("llseek") as (_, fd), \
                patch.object(manager, "_extent_ranges", side_effect=holes_as_data), \
                self.assertLogs("storage.cache", "WARNING") as logged:
            self.assertFalse(manager.write_streaming_chunk(fd, 0, b"x"))
            self.assertFalse(manager.write_streaming_chunk(fd, 1 << 29, b"y"))
            self.assertNotIn(fd, manager._streaming_fds)
        self.assertEqual(manager.get_total_cache_size(), 0)
        self.assertEqual(len(logged.output), 1)
        self.assertIn("holes as data", logged.output[0])
        self.assertIn("Some streaming reads are not being cached", logged.output[0])

    def test_later_extent_misreports_cannot_hide_unpaid_blocks(self):
        """After the first grant the charge is what this descriptor wrote,
        so a filesystem that starts reporting holes as data (OpenZFS with
        dirty data) neither stops the budget nor is consulted."""
        manager = self.cache_manager
        block = manager.block_size
        manager.max_size_bytes = 2 * block
        with self._partial("zfs") as (_, fd):
            self.assertTrue(manager.write_streaming_chunk(fd, 0, b"x"))
            with patch.object(manager, "_extent_ranges",
                              side_effect=AssertionError("extent walk on a later grant")):
                self.assertTrue(manager.write_streaming_chunk(fd, 1 << 29, b"y"))
                self.assertTrue(manager.write_streaming_chunk(fd, 1, b"z"))  # same block
                self.assertFalse(manager.write_streaming_chunk(fd, 1 << 28, b"over"))
                self.assertEqual(manager.get_total_cache_size(), 2 * block)

    def test_a_fragmented_partial_grants_without_walking_its_extents(self):
        manager = self.cache_manager
        block = manager.block_size
        with self._partial("fragmented") as (_, fd):
            self.assertTrue(manager.write_streaming_chunk(fd, 0, b"0"))
            with patch.object(manager, "_extent_ranges",
                              side_effect=AssertionError("extent walk on a later grant")):
                for index in range(1, 200):
                    self.assertTrue(manager.write_streaming_chunk(fd, 2 * index * block, b"x"))
                # Bridging two written blocks adds one block, not three.
                self.assertTrue(manager.write_streaming_chunk(fd, block, b"b" * block))
            self.assertEqual(manager.get_total_cache_size(), 201 * block)
            manager.mark_cache_size_dirty()
            self.assertEqual(manager.get_total_cache_size(), 201 * block)

    def test_streaming_grant_refused_without_extent_seeks(self):
        manager = self.cache_manager
        with self._partial("noseek") as (_, fd), \
                patch.object(manager, "_extent_ranges", side_effect=OSError(errno.EINVAL, "no seek")), \
                self.assertLogs("storage.cache", "WARNING"):
            self.assertFalse(manager.write_streaming_chunk(fd, 0, b"x"))
        self.assertEqual(manager.get_total_cache_size(), 0)

    def test_a_failed_write_charges_nothing_and_keeps_its_error(self):
        manager = self.cache_manager
        with self._partial("failed-write") as (_, fd):
            with patch("storage.cache.os.pwrite", side_effect=OSError(errno.ENOSPC, "disk full")):
                with self.assertRaises(OSError) as raised:
                    manager.write_streaming_chunk(fd, 0, b"x")
            self.assertEqual(raised.exception.errno, errno.ENOSPC)
            self.assertEqual(manager._streaming_fds[fd].bytes, 0)
            self.assertEqual(manager.get_total_cache_size(), 0)

    def test_close_streaming_fd_closes_even_when_fstat_fails(self):
        manager = self.cache_manager
        path = self.cache_dir / ("closing" + PARTIAL_SUFFIX)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        self.assertTrue(manager.write_streaming_chunk(fd, 0, b"x"))
        with patch("storage.cache.os.fstat", side_effect=OSError(errno.EIO, "fstat failed")):
            manager.close_streaming_fd(fd)
        with self.assertRaises(OSError) as closed:
            os.fstat(fd)
        self.assertEqual(closed.exception.errno, errno.EBADF)
        self.assertNotIn(fd, manager._streaming_fds)
        self.assertEqual(manager.get_total_cache_size(), manager.block_size)

    def test_truncated_prefix_keeps_only_its_blocks_charged(self):
        manager = self.cache_manager
        block = manager.block_size
        with self._partial("prefix") as (path, fd):
            self.assertTrue(manager.write_streaming_chunk(fd, 0, b"p" * (2 * block)))
            self.assertTrue(manager.write_streaming_chunk(fd, 1 << 29, b"far"))
            self.assertEqual(manager.get_total_cache_size(), 3 * block)
            with self._no_rescan():
                manager.truncate_streaming_fd(fd, block + 1)
                self.assertEqual(manager.get_total_cache_size(), 2 * block)
                self.assertEqual(manager._streaming_fds[fd].bytes, 2 * block)
            manager.mark_cache_size_dirty()
            self.assertEqual(manager.get_total_cache_size(), 2 * block)
            # Promoted: the prefix is a named file the tree scan counts.
            final = self.cache_dir / "prefix"
            os.link(path, final)
            path.unlink()
            self.assertEqual(manager.get_total_cache_size(), 2 * block)
        self.assertEqual(manager.get_total_cache_size(), 2 * block)

    def test_prefix_truncate_cannot_fail_after_the_truncate(self):
        """Nothing is measured after the truncate, so an extent query that
        would fail cannot stop the caller's prefix marker."""
        manager = self.cache_manager
        block = manager.block_size
        with self._partial("prefix-measure") as (_, fd):
            self.assertTrue(manager.write_streaming_chunk(fd, 0, b"p" * (3 * block)))
            with patch.object(manager, "_extent_ranges",
                              side_effect=OSError(errno.EIO, "extent query failed")):
                manager.truncate_streaming_fd(fd, block)
            self.assertEqual(os.fstat(fd).st_size, block)
            self.assertEqual(manager.get_total_cache_size(), block)

    def test_superseded_partial_charge_is_released_at_its_session_close(self):
        """A reader still holding an unlinked partial keeps its bytes, but
        the charge belongs to the session descriptor (close_streaming_fd)."""
        manager = self.cache_manager
        block = manager.block_size
        path = self.cache_dir / ("superseded" + PARTIAL_SUFFIX)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        self.assertTrue(manager.write_streaming_chunk(fd, 0, b"s" * block))
        reader = os.open(path, os.O_RDONLY)
        try:
            path.unlink()
            self.assertEqual(manager.get_total_cache_size(), block)
            manager.close_streaming_fd(fd)
            self.assertEqual(manager.get_total_cache_size(), 0)
            self.assertEqual(os.pread(reader, block, 0), b"s" * block)
            manager.mark_cache_size_dirty()
            self.assertEqual(manager.get_total_cache_size(), 0)
        finally:
            os.close(reader)

    def test_publication_counts_a_sparse_inode_with_an_xattr_block_once(self):
        """One data block, one hole, and possibly an external xattr block:
        the tree scan counts what streaming charged, not st_blocks."""
        manager = self.cache_manager
        block = manager.block_size
        path = self.cache_dir / ("xattr" + PARTIAL_SUFFIX)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            os.ftruncate(fd, 2 * block)
            try:
                os.setxattr(path, "user.test-large", b"x" * (block // 2))
            except OSError:
                pass  # no large xattrs here: the data-only case still holds
            manager.mark_cache_size_dirty()
            self.assertEqual(manager.get_total_cache_size(), 0)
            self.assertTrue(manager.write_streaming_chunk(fd, 0, b"d"))
            self.assertEqual(manager.get_total_cache_size(), block)
            final = self.cache_dir / "xattr"
            os.link(path, final)
            path.unlink()
            self.assertEqual(manager.get_total_cache_size(), block)
        finally:
            manager.close_streaming_fd(fd)
        self.assertEqual(manager.get_total_cache_size(), block)

    def test_positional_write_keeps_the_total_a_scan_counts_when_allocation_changes(self):
        """A named file is charged min(data extents, allocated blocks). A
        pwrite that leaves the extents alone but changes the allocation
        (a rewrite that compresses differently) moves the charge, and the
        incremental total must stay what a fresh scan counts."""
        manager = self.cache_manager
        block = manager.block_size
        path = self.cache_dir / "capped"
        path.write_bytes(b"a" * (2 * block))
        fd = os.open(path, os.O_RDWR)
        try:
            with _ReportedBlocks() as reported:
                reported.set(path, block)  # two data blocks compressed into one
                manager.mark_cache_size_dirty()
                self.assertEqual(manager.get_total_cache_size(), block)
                with manager.cache_size_change(fd=fd, span=(0, block)):
                    os.pwrite(fd, b"b" * block, 0)
                    reported.set(path, 2 * block)
                grown = manager.get_total_cache_size()
                manager.mark_cache_size_dirty()
                self.assertEqual(grown, manager.get_total_cache_size())
                self.assertEqual(grown, 2 * block)
                with manager.cache_size_change(fd=fd, span=(block, 2 * block)):
                    os.pwrite(fd, b"c" * block, block)
                    reported.set(path, block)
                shrunk = manager.get_total_cache_size()
                manager.mark_cache_size_dirty()
                self.assertEqual(shrunk, manager.get_total_cache_size())
                self.assertEqual(shrunk, block)
                # Deleting refunds what was charged: nothing phantom stays.
                manager.delete_cached_file("/capped")
                self.assertEqual(manager.get_total_cache_size(), 0)
        finally:
            os.close(fd)

    def test_positional_write_past_the_cap_is_charged_from_one_measure(self):
        """The cap binds before the write but not after (the write fills a
        compressed file's tail with incompressible bytes and extends it)."""
        manager = self.cache_manager
        block = manager.block_size
        path = self.cache_dir / "extended"
        path.write_bytes(b"a" * (2 * block))
        fd = os.open(path, os.O_RDWR)
        try:
            with _ReportedBlocks() as reported:
                reported.set(path, block)
                manager.mark_cache_size_dirty()
                self.assertEqual(manager.get_total_cache_size(), block)
                with manager.cache_size_change(fd=fd, span=(2 * block, 3 * block)):
                    os.pwrite(fd, b"z" * block, 2 * block)
                    reported.set(path, 3 * block)
                incremental = manager.get_total_cache_size()
                manager.mark_cache_size_dirty()
                self.assertEqual(incremental, manager.get_total_cache_size())
                self.assertEqual(incremental, 3 * block)
        finally:
            os.close(fd)

    def _clean_row(self, name, *, last_access):
        cache_path = self.cache_dir / name
        st = cache_path.lstat()
        self.state_manager.set_file_metadata(FileMetadata(
            virtual_path=f"/{name}", size=st.st_size, checksum="c", last_access=last_access,
            last_modified=st.st_mtime, sync_status=SyncStatus.CLEAN, replica_count=1,
            recomputable=False, recompute_cmd=None, encrypted=False, encryption_key_id=None,
        ))
        self.state_manager.add_file_location(FileLocation(f"/{name}", "member", "c", 1.0, False))

    def test_an_installed_inode_is_not_evicted_while_its_session_descriptor_holds_it(self):
        """A readerless fill installs its inode, CLEAN with a member
        location, before its session descriptor closes. No FUSE descriptor
        pins it, but unlinking the name then would free nothing: eviction
        skips it, keeps reclaiming unrelated copies, and reclaims it once
        the descriptor has closed."""
        manager = self.cache_manager
        block = manager.block_size
        manager.max_size_bytes = 3 * block
        partial = self.cache_dir / ("installed" + PARTIAL_SUFFIX)
        fd = os.open(partial, os.O_RDWR | os.O_CREAT, 0o600)
        closed = False
        try:
            self.assertTrue(manager.write_streaming_chunk(fd, 0, b"i" * (2 * block)))
            final = self.cache_dir / "installed"
            os.link(partial, final)
            partial.unlink()
            self._clean_row("installed", last_access=1.0)  # least recently used
            other = self.cache_dir / "other"
            with manager.cache_size_change(other):
                other.write_bytes(b"o" * block)
            self._clean_row("other", last_access=2.0)
            self.assertEqual(manager.get_total_cache_size(), 3 * block)

            # Only the installed inode could cover two blocks: refused, and
            # nothing is unlinked or credited.
            self.assertFalse(manager.ensure_space(2 * block))
            self.assertTrue(final.exists())
            self.assertTrue(other.exists())
            self.assertEqual(manager.get_total_cache_size(), 3 * block)
            manager.mark_cache_size_dirty()
            self.assertEqual(manager.get_total_cache_size(), 3 * block)

            # An unrelated copy still makes progress.
            self.assertTrue(manager.ensure_space(block))
            self.assertFalse(other.exists())
            self.assertTrue(final.exists())
            self.assertEqual(manager.get_total_cache_size(), 2 * block)

            # A direct write to the held inode leaves the total to a rescan.
            writer = os.open(final, os.O_RDWR)
            try:
                with manager.cache_size_change(fd=writer, span=(0, 1)):
                    os.pwrite(writer, b"I", 0)
            finally:
                os.close(writer)
            self._clean_row("installed", last_access=1.0)  # the write was synced
            self.assertEqual(manager.get_total_cache_size(), 2 * block)

            manager.close_streaming_fd(fd)
            closed = True
            self.assertEqual(manager.get_total_cache_size(), 2 * block)
            manager.mark_cache_size_dirty()
            self.assertEqual(manager.get_total_cache_size(), 2 * block)
            # Released: the next allocation reclaims it at once (no
            # remembered shortfall stands in the way).
            self.assertTrue(manager.ensure_space(3 * block))
            self.assertFalse(final.exists())
            self.assertEqual(manager.get_total_cache_size(), 0)
            manager.mark_cache_size_dirty()
            self.assertEqual(manager.get_total_cache_size(), 0)
        finally:
            if not closed:
                manager.close_streaming_fd(fd)

    def test_a_write_to_an_unlinked_held_inode_moves_its_descriptor_charge(self):
        """R4: another descriptor on a held inode (here with no name left)
        grows it; the change is charged to the session descriptor, so a
        rescan agrees, a grant cannot spend the new blocks again, and the
        close releases all of them."""
        manager = self.cache_manager
        block = manager.block_size
        manager.max_size_bytes = 3 * block
        partial = self.cache_dir / ("held" + PARTIAL_SUFFIX)
        fd = os.open(partial, os.O_RDWR | os.O_CREAT, 0o600)
        writer = os.open(partial, os.O_RDWR)
        try:
            self.assertTrue(manager.write_streaming_chunk(fd, 0, b"h" * block))
            partial.unlink()
            with manager.cache_size_change(fd=writer, span=(block, 3 * block)):
                os.pwrite(writer, b"w" * (2 * block), block)
            self.assertEqual(manager.get_total_cache_size(), 3 * block)
            self.assertFalse(manager.ensure_space(block))
            manager.mark_cache_size_dirty()
            self.assertEqual(manager.get_total_cache_size(), 3 * block)
            # No more grants from ranges that no longer describe the inode.
            self.assertFalse(manager.write_streaming_chunk(fd, 0, b"x"))
        finally:
            os.close(writer)
        manager.close_streaming_fd(fd)
        self.assertEqual(manager.get_total_cache_size(), 0)
        manager.mark_cache_size_dirty()
        self.assertEqual(manager.get_total_cache_size(), 0)

    def _held_installed(self, name, blocks):
        """A session descriptor that wrote `blocks` blocks, its inode then
        installed at `name` (the partial name gone): held and named."""
        manager = self.cache_manager
        partial = self.cache_dir / (name + PARTIAL_SUFFIX)
        fd = os.open(partial, os.O_RDWR | os.O_CREAT, 0o600)
        self.assertTrue(manager.write_streaming_chunk(fd, 0, b"s" * (blocks * manager.block_size)))
        final = self.cache_dir / name
        os.link(partial, final)
        partial.unlink()
        return fd, final

    def _assert_total(self, expected):
        manager = self.cache_manager
        self.assertEqual(manager.get_total_cache_size(), expected)
        manager.mark_cache_size_dirty()
        self.assertEqual(manager.get_total_cache_size(), expected)

    def test_a_held_inode_grown_after_a_failed_pre_measurement_is_charged(self):
        """R5: the holder is found by a stat alone, so an extent query that
        fails before the write cannot hide a real grow from the charge."""
        manager = self.cache_manager
        block = manager.block_size
        manager.max_size_bytes = 3 * block
        fd, final = self._held_installed("grown", 1)
        writer = os.open(final, os.O_RDWR)
        real_extents = manager._extent_ranges
        failing = [True]

        def extents(*args, **kwargs):
            if failing[0]:
                raise OSError(errno.EIO, "extent query failed")
            return real_extents(*args, **kwargs)

        try:
            with patch.object(manager, "_extent_ranges", side_effect=extents):
                with manager.cache_size_change(fd=writer, span=(block, 3 * block)):
                    failing[0] = False
                    self.assertEqual(os.pwrite(writer, b"g" * (2 * block), block), 2 * block)
            self.assertEqual(manager.get_total_cache_size(), 3 * block)
            self.assertFalse(manager.ensure_space(block))
            self._assert_total(3 * block)
        finally:
            os.close(writer)
        manager.close_streaming_fd(fd)
        self._assert_total(3 * block)
        self.assertFalse(manager.ensure_space(block))

    def test_o_trunc_of_a_held_inode_under_its_stream_charge_leaves_no_phantom(self):
        """R5: the stream charged the blocks it wrote; the installed inode
        is allocated fewer (compression). O_TRUNC through the name while a
        reader is attached frees the whole charge, not the capped part."""
        manager = self.cache_manager
        block = manager.block_size
        manager.max_size_bytes = 2 * block
        fd, final = self._held_installed("compressed", 2)
        reader = os.open(final, os.O_RDONLY)
        try:
            with _ReportedBlocks() as reported:
                reported.set(final, block)  # two written blocks, one allocated
                self._assert_total(2 * block)
                with manager.cache_size_change(final):
                    os.close(os.open(final, os.O_WRONLY | os.O_TRUNC))
                self._assert_total(0)
                self.assertEqual(manager._streaming_fds[fd].bytes, 0)
                self.assertTrue(manager.ensure_space(2 * block))
        finally:
            os.close(reader)
        manager.close_streaming_fd(fd)
        self._assert_total(0)

    def test_a_held_inode_changed_through_a_hard_link_alias_is_charged_once(self):
        manager = self.cache_manager
        block = manager.block_size
        fd, final = self._held_installed("aliased", 2)
        alias = self.cache_dir / "alias"
        os.link(final, alias)
        self._assert_total(2 * block)
        with manager.cache_size_change(alias):
            os.truncate(alias, block)
        self._assert_total(block)
        manager.close_streaming_fd(fd)
        self._assert_total(block)

    def test_allocations_are_withheld_while_a_held_charge_cannot_be_measured(self):
        manager = self.cache_manager
        block = manager.block_size
        fd, final = self._held_installed("unmeasured", 1)
        writer = os.open(final, os.O_RDWR)
        real_fstat = os.fstat

        def fstat(target):
            if target == fd:
                raise OSError(errno.EIO, "fstat failed")
            return real_fstat(target)

        try:
            with patch("os.fstat", side_effect=fstat):
                with manager.cache_size_change(fd=writer, span=(block, 2 * block)):
                    os.pwrite(writer, b"g" * block, block)
                self.assertTrue(manager._streaming_fds[fd].unmeasured)
                self.assertFalse(manager.ensure_space(block))
            # Measurable again: the next allocation recharges and proceeds.
            self.assertTrue(manager.ensure_space(block))
            self.assertFalse(manager._streaming_fds[fd].unmeasured)
            self._assert_total(2 * block)
        finally:
            os.close(writer)
        manager.close_streaming_fd(fd)
        self._assert_total(2 * block)

    def test_a_failed_write_to_a_held_inode_keeps_its_error(self):
        manager = self.cache_manager
        block = manager.block_size
        fd, final = self._held_installed("failing", 1)
        reader = os.open(final, os.O_RDONLY)
        try:
            with self.assertRaises(OSError) as raised:
                with manager.cache_size_change(fd=reader, span=(0, block)):
                    os.pwrite(reader, b"x", 0)
            self.assertEqual(raised.exception.errno, errno.EBADF)
            self._assert_total(block)
        finally:
            os.close(reader)
        manager.close_streaming_fd(fd)
        self._assert_total(block)

    def test_deleting_a_held_installed_inode_refunds_its_charge_once(self):
        """Nit 4: a promoted prefix is named while its session descriptor
        stays open; deleting the name, then closing, refunds the blocks
        once."""
        manager = self.cache_manager
        block = manager.block_size
        partial = self.cache_dir / ("promoted" + PARTIAL_SUFFIX)
        fd = os.open(partial, os.O_RDWR | os.O_CREAT, 0o600)
        self.assertTrue(manager.write_streaming_chunk(fd, 0, b"p" * (2 * block)))
        other = self.cache_dir / "other"
        with manager.cache_size_change(other):
            other.write_bytes(b"o" * block)
        os.link(partial, self.cache_dir / "promoted")
        partial.unlink()
        self.assertEqual(manager.get_total_cache_size(), 3 * block)
        manager.delete_cached_file("/promoted")
        self.assertEqual(manager.get_total_cache_size(), 3 * block)  # still held
        manager.close_streaming_fd(fd)
        self.assertEqual(manager.get_total_cache_size(), block)
        manager.mark_cache_size_dirty()
        self.assertEqual(manager.get_total_cache_size(), block)

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

    def test_local_write_during_a_reserved_copy_does_not_undo_it(self):
        """The reservation owns its space: a dirty write that takes the
        cache over budget mid-copy does not discard the finished copy."""
        manager = self.cache_manager
        manager.max_size_bytes = len(self.payload)
        dirty = self.cache_dir / "dirty.bin"
        worker = self._start_fetch()
        try:
            with manager.cache_size_change(dirty):
                dirty.write_bytes(b"local write")
            self.assertGreater(manager.get_free_space_needed(0), 0)
        finally:
            self.backend.gate.set()
            worker.join(10)
        self.assertEqual(self.results, [True])
        self.assertEqual(self.cache_path.read_bytes(), self.payload)
        self.assertEqual(dirty.read_bytes(), b"local write")
        self.assertEqual(self._partials(), [])
        self.assertEqual(manager.get_total_cache_size(), len(self.payload) + manager.block_size)

    def test_growth_beyond_the_reservation_must_fit(self):
        manager = self.cache_manager
        manager.max_size_bytes = len(self.payload)
        (self.backend_dir / "f.bin").write_bytes(self.payload)
        grown = self.payload + b"g" * manager.block_size
        real_copy = LocalBackend.copy_to_local

        def grow_then_copy(backend, virtual_path, local_path):
            (self.backend_dir / "f.bin").write_bytes(grown)
            return real_copy(backend, virtual_path, local_path)

        with patch.object(_GatedBackend, "copy_to_local", grow_then_copy):
            with self.assertRaises(OSError) as raised:
                manager.cache_file_result("/f.bin", self.backend)
        self.assertEqual(raised.exception.errno, errno.ENOSPC)
        self.assertFalse(self.cache_path.exists())
        self.assertEqual(self._partials(), [])
        self.assertEqual(manager.get_total_cache_size(), 0)
        self.assertEqual(manager.get_free_space_needed(0), 0)

    def test_grown_copy_losing_to_an_installed_file_spends_nothing(self):
        manager = self.cache_manager
        block = manager.block_size
        manager.max_size_bytes = len(self.payload) + block
        grown = self.payload + b"g" * (2 * block)
        real_copy = LocalBackend.copy_to_local

        def winner_then_grow(backend, virtual_path, local_path):
            self.cache_path.write_bytes(b"winner")
            manager.mark_cache_size_dirty()
            (self.backend_dir / "f.bin").write_bytes(grown)
            return real_copy(backend, virtual_path, local_path)

        with patch.object(_GatedBackend, "copy_to_local", winner_then_grow), \
                patch.object(manager, "_fits_beyond_reservation",
                             side_effect=AssertionError("spent space for a losing copy")):
            self.assertEqual(manager.cache_file_result("/f.bin", self.backend).name, "ALREADY_CACHED")
        self.assertEqual(self.cache_path.read_bytes(), b"winner")
        self.assertEqual(self._partials(), [])
        self.assertEqual(manager.get_total_cache_size(), block)

    def test_a_grown_copy_that_exactly_fits_is_installed(self):
        """The growth check counts the finished copy by its measured data
        blocks, the measure the total then charges, not by its allocation
        (which here includes an external xattr block)."""
        manager = self.cache_manager
        block = manager.block_size
        manager.max_size_bytes = 2 * block
        reported = _ReportedBlocks()

        class SparseBackend(LocalBackend):
            def copy_to_local(self, virtual_path, local_path):
                with open(local_path, "wb") as f:
                    f.write(b"d" * block)
                    f.seek(3 * block)
                    f.write(b"e" * block)
                # Two data blocks, three allocated (one for an xattr).
                reported.set(local_path, 3 * block)

        backend = SparseBackend("backend", str(self.backend_dir))
        with reported:
            self.assertTrue(manager.fetch_into_cache(
                backend, "/f.bin", self.cache_path, expected_size=block))
            self.assertEqual(manager.get_total_cache_size(), 2 * block)
            manager.mark_cache_size_dirty()
            self.assertEqual(manager.get_total_cache_size(), 2 * block)
        self.assertEqual(self._partials(), [])
        self.assertEqual(os.path.getsize(self.cache_path), 4 * block)

    def test_no_room_for_a_copy_raises_enospc_before_downloading(self):
        manager = self.cache_manager
        manager.max_size_bytes = len(self.payload) - 1
        with patch.object(self.backend, "copy_to_local") as copy:
            with self.assertRaises(OSError) as raised:
                manager.cache_file("/f.bin", self.backend)
        self.assertEqual(raised.exception.errno, errno.ENOSPC)
        copy.assert_not_called()
        self.assertEqual(self._partials(), [])

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

    def test_dry_run_marks_an_xattr_stale_copy_dirty_without_reclaiming_it(self):
        written = self._clean_cached("written.txt", b"v1")
        self.state_manager.add_file_location(FileLocation("/written.txt", "member", "c", 1.0, False))
        st = written.stat()
        self._mirror(written, stat_size=st.st_size, stat_mtime=st.st_mtime + 1)
        manager = self.cache_manager
        manager.mark_cache_size_dirty()
        manager.max_size_bytes = manager.get_total_cache_size()

        self.assertFalse(manager.ensure_space(manager.block_size))
        self.assertEqual(written.read_bytes(), b"v1")
        self.assertEqual(self.state_manager.get_file_metadata("/written.txt").sync_status,
                         SyncStatus.DIRTY)

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
        for name in ("/oldest.txt", "/newer.txt"):
            self.state_manager.add_file_location(FileLocation(name, "member", "c", 1.0, False))
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

    def test_pressure_keeps_a_clean_copy_no_member_is_recorded_to_hold(self):
        """A CLEAN row is not evidence of a member copy: without a recorded
        member location the cached file may be the only copy."""
        unrecorded = self._clean_cached("unrecorded.txt", b"sole", last_access=1.0)
        recorded = self._clean_cached("recorded.txt", b"held", last_access=2.0)
        self.state_manager.add_file_location(FileLocation("/recorded.txt", "member", "c", 1.0, False))
        self.state_manager.add_file_location(FileLocation("/unrecorded.txt", "cache", "c", 1.0, True))
        manager = self.cache_manager
        manager.mark_cache_size_dirty()
        manager.max_size_bytes = manager.get_total_cache_size()

        self.assertTrue(manager.ensure_space(manager.block_size))
        self.assertEqual(unrecorded.read_bytes(), b"sole")
        self.assertFalse(recorded.exists())
        self.assertFalse(manager.ensure_space(2 * manager.block_size))
        self.assertEqual(unrecorded.read_bytes(), b"sole")

    def test_insufficient_victims_keep_existing_clean_bytes(self):
        ordinary = self._clean_cached("ordinary.txt", b"keep", last_access=1.0)
        dirty = self._clean_cached("dirty.txt", b"unsynced", last_access=2.0)
        self.cache_manager.mark_file_dirty("/dirty.txt", dirty)
        self.cache_manager.mark_cache_size_dirty()
        self.cache_manager.max_size_bytes = self.cache_manager.get_total_cache_size()

        self.assertFalse(self.cache_manager.ensure_space(2 * self.cache_manager.block_size))
        self.assertEqual(ordinary.read_bytes(), b"keep")
        self.assertEqual(dirty.read_bytes(), b"unsynced")
