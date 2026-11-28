import errno
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
from storage.cache_metadata import MEMBER_ATTRS_XATTR, read_cache_metadata, regular_file_stat
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

    def test_recomputable_eviction_refuses_a_file_swapped_in_after_the_check(self):
        self._row("/swapped", recomputable=True, recompute_cmd="true")
        swapped = self.cache_dir / "swapped"
        swapped.write_bytes(b"y" * 10)
        real_check = regular_file_stat

        def check_then_swap(path):
            st = real_check(path)
            if Path(path) == swapped:
                replacement = self.cache_dir / "replacement"
                replacement.write_bytes(b"z" * 20)
                os.replace(replacement, swapped)
            return st

        with patch("storage.cache.regular_file_stat", side_effect=check_then_swap):
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

    def _fill_with_unindexed_file(self, size):
        # Like a streaming partial: counted by the scan, unknown to eviction.
        held = self.cache_dir / "held.storage.partial"
        held.write_bytes(b"\0" * size)
        self.cache_manager.mark_cache_size_dirty()
        return held

    def test_reclaim_hook_runs_without_the_cache_lock_and_allows_one_retry(self):
        self.cache_manager.max_size_bytes = 1000
        held = self._fill_with_unindexed_file(800)
        (self.backend_dir / "w.bin").write_bytes(b"w" * 500)
        calls = []

        def reclaim(size):
            calls.append((size, self.cache_manager._lock._is_owned()))
            held.unlink()
            self.cache_manager.mark_cache_size_dirty()
            return True

        self.cache_manager.reclaim_space = reclaim
        self.assertTrue(self.cache_manager.cache_file("/w.bin", self.backend))
        self.assertEqual(calls, [(500, False)])
        self.assertEqual(self.cache_manager.get_total_cache_size(), 500)

    def test_reclaim_hook_that_frees_nothing_fails_the_fill_once(self):
        self.cache_manager.max_size_bytes = 1000
        self._fill_with_unindexed_file(800)
        (self.backend_dir / "w.bin").write_bytes(b"w" * 500)
        for result in (False, True):
            calls = []
            self.cache_manager.reclaim_space = lambda size: calls.append(size) or result
            self.assertFalse(self.cache_manager.cache_file("/w.bin", self.backend))
            self.assertEqual(calls, [500])
        self.assertFalse(self.cache_manager.is_cached("/w.bin"))


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


class TestBoundedCopy(unittest.TestCase):
    """CTO-208-BOUNDED: a capped copy_to_local reads one open of the source
    and stops at the cap, however fast a writer appends (here after every
    chunk read, with the producer capped at a guard)."""

    CAP, GUARD = 40, 10_000

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.temp_dir, True)

    def appending_reader(self, read, append):
        """read() that appends to the source after every chunk it returns."""
        appended = []

        def read_and_grow(n):
            data = read(n)
            if sum(appended) < self.GUARD:
                appended.append(append())
            return data
        return read_and_grow

    def check(self, bound, dst):
        self.assertTrue(bound.grew)
        self.assertLessEqual(bound.written, self.CAP)
        self.assertLessEqual(bound.read, self.CAP + 1)
        self.assertEqual(dst.stat().st_size, self.CAP)

    def test_local_member_copy_stops_at_the_cap(self):
        from storage.backends import base, local
        root = self.temp_dir / "member"
        root.mkdir()
        src = root / "f.log"
        src.write_bytes(b"x" * self.CAP)
        backend = LocalBackend("m", str(root))
        real_stream = local.copy_bounded_stream

        def stream(read, write, bound):
            def append():
                with open(src, "ab") as f:
                    f.write(b"+" * 7)
                return 7
            return real_stream(self.appending_reader(read, append), write, bound)

        dst = self.temp_dir / "copy"
        with patch.object(base, "COPY_CHUNK", 8), patch.object(local, "copy_bounded_stream", stream), \
                base.bounded_copy(self.CAP) as bound:
            backend.copy_to_local("/f.log", dst)
        self.check(bound, dst)
        self.assertEqual(dst.read_bytes(), b"x" * self.CAP)

    def test_ssh_member_copy_stops_at_the_cap(self):
        from types import SimpleNamespace
        from storage.backends import base
        from storage.backends.ssh import SSHBackend
        data = bytearray(b"y" * self.CAP)
        offset = {"at": 0}

        class GrowingRemote:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def read(self, n):
                chunk = bytes(data[offset["at"]:offset["at"] + n])
                offset["at"] += len(chunk)
                return chunk

        remote = GrowingRemote()
        remote.read = self.appending_reader(remote.read, lambda: data.extend(b"+" * 7) or 7)
        sftp = SimpleNamespace(open=lambda path, mode: remote,
                               get=lambda *a: self.fail("an unbounded get"))
        backend = SSHBackend.__new__(SSHBackend)
        backend._resolve_path = lambda path: "/remote" + path
        backend.sftp_operation_timeout = 1.0
        backend._with_sftp = lambda what, path, fn, **kw: fn(sftp)
        dst = self.temp_dir / "copy"
        with patch.object(base, "COPY_CHUNK", 8), base.bounded_copy(self.CAP) as bound:
            backend.copy_to_local("/f.log", dst)
        self.check(bound, dst)

    def test_a_source_of_exactly_the_cap_copies_whole(self):
        from storage.backends import base
        root = self.temp_dir / "member"
        root.mkdir()
        (root / "f.log").write_bytes(b"z" * self.CAP)
        dst = self.temp_dir / "copy"
        with base.bounded_copy(self.CAP) as bound:
            LocalBackend("m", str(root)).copy_to_local("/f.log", dst)
        self.assertFalse(bound.grew)
        self.assertEqual(dst.read_bytes(), b"z" * self.CAP)

    def test_a_member_that_cannot_bound_its_copy_gives_eagain(self):
        state = StateManager(str(self.temp_dir / "state.db"))
        self.addCleanup(state.close)
        cache = CacheManager(self.temp_dir / "cache", state, 10_000)
        backend = LocalBackend("m", str(self.temp_dir))
        (self.temp_dir / "f.log").write_bytes(b"abc")
        with patch.object(LocalBackend, "supports_bounded_copy", False), \
                patch.object(backend, "copy_to_local",
                             side_effect=AssertionError("an unbounded copy")):
            self.assertFalse(cache.cache_file("/f.log", backend))
        self.assertEqual(cache.take_fill_errno(), errno.EAGAIN)


class TestConfirmedLargerRetryBudget(unittest.TestCase):
    """A full fill whose member is confirmed to have grown is fetched again
    only within the cache budget (pre-#191: reserve the difference first)."""

    def make(self, max_size):
        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir, True)
        cache_dir = Path(self.temp_dir) / "cache"
        backend_dir = Path(self.temp_dir) / "backend"
        cache_dir.mkdir()
        backend_dir.mkdir()
        state = StateManager(str(Path(self.temp_dir) / "state.db"))
        self.addCleanup(state.close)
        self.cache = CacheManager(cache_dir, state, max_size)
        self.backend = LocalBackend("backend", str(backend_dir))
        self.source = backend_dir / "f.bin"
        self.source.write_bytes(b"x" * 100)
        self.cache_dir = cache_dir
        copy = self.backend.copy_to_local
        self.reserved_during = []

        def grows_once(path, temp):
            self.reserved_during.append(self.cache._reserved_bytes)
            if len(self.reserved_during) == 1:
                self.source.write_bytes(b"y" * 400)  # after the probe: larger
            copy(path, temp)

        return patch.object(self.backend, "copy_to_local", side_effect=grows_once)

    def test_confirmed_larger_retry_reserves_the_difference(self):
        with self.make(max_size=10_000):
            self.assertTrue(self.cache.cache_file("/f.bin", self.backend))
        self.assertEqual(self.reserved_during, [100, 400])
        self.assertEqual(self.cache._reserved_bytes, 0)
        self.assertEqual(self.cache.get_cache_path("/f.bin").read_bytes(), b"y" * 400)

    def probe_then(self, after):
        """Member stat: a probe of the original 100 bytes, then after(path)."""
        from types import SimpleNamespace
        real, seen = self.backend.stat, []

        def stats(path):
            seen.append(path)
            if len(seen) == 1:
                st = real(path)
                return SimpleNamespace(st_size=100, st_mtime=st.st_mtime, st_mode=st.st_mode)
            return after(path)
        return patch.object(self.backend, "stat", side_effect=stats)

    def test_confirmed_larger_retry_reserves_the_newer_stats_size(self):
        # The member grows after the first copy: the retry is reserved for
        # the newer stat's size, not for what the first transfer was.
        with self.make(max_size=10_000):
            real_copy = LocalBackend.copy_to_local.__get__(self.backend)
            calls = []

            def grows_after(path, temp):
                calls.append(self.cache._reserved_bytes)
                real_copy(path, temp)
                if len(calls) == 1:
                    self.source.write_bytes(b"y" * 400)

            with patch.object(self.backend, "copy_to_local", side_effect=grows_after):
                self.assertTrue(self.cache.cache_file("/f.bin", self.backend))
        self.assertEqual(calls, [100, 400])
        self.assertEqual(self.cache._reserved_bytes, 0)

    def test_confirmed_larger_retry_needs_a_newer_token(self):
        # CHANGED (the transfer is another size) and the stat after it has no
        # token (it failed, or reports a size but no mtime): nothing to retry
        # against, so no retry and nothing installed.
        from types import SimpleNamespace

        def fails(path):
            raise OSError(5, "stat failed")
        for after in (fails, lambda path: SimpleNamespace(st_size=400)):
            with self.subTest(after=after):
                with self.make(max_size=10_000), self.probe_then(after):
                    self.assertFalse(self.cache.cache_file("/f.bin", self.backend))
                self.assertEqual(self.reserved_during, [100])
                self.assertEqual(self.cache._reserved_bytes, 0)
                self.assertFalse(self.cache.get_cache_path("/f.bin").exists())

    def test_confirmed_larger_retry_that_changes_again_fails(self):
        # The retry's probe is the newer stat (400 bytes, reserved); its
        # transfer then finds another size again: never installed.
        with self.make(max_size=10_000):
            copy = self.backend.copy_to_local.side_effect

            def grows_twice(path, temp):
                if len(self.reserved_during) == 1:
                    self.source.write_bytes(b"z" * 500)
                copy(path, temp)

            with patch.object(self.backend, "copy_to_local", side_effect=grows_twice):
                self.assertFalse(self.cache.cache_file("/f.bin", self.backend))
        self.assertEqual(self.reserved_during, [100, 400])
        self.assertEqual(self.cache._reserved_bytes, 0)
        self.assertFalse(self.cache.get_cache_path("/f.bin").exists())

    def test_confirmed_larger_retry_without_a_probe_never_installs_another_size(self):
        # No probe, and the stat after the copy reports another size.
        from types import SimpleNamespace
        with self.make(max_size=10_000):
            with patch.object(self.backend, "stat", side_effect=lambda path: SimpleNamespace(
                    st_size=999, st_mtime=1.0)):
                with self.assertRaises(OSError):
                    self.cache.fetch_into_cache(self.backend, "/f.bin",
                                                self.cache.get_cache_path("/f.bin"), None)
        self.assertEqual(len(self.reserved_during), 1)  # one transfer, no retry
        self.assertFalse(self.cache.get_cache_path("/f.bin").exists())

    def test_confirmed_larger_retry_fails_when_it_cannot_reserve(self):
        with self.make(max_size=300):
            self.assertFalse(self.cache.cache_file("/f.bin", self.backend))
        self.assertEqual(self.reserved_during, [100])  # no second transfer
        self.assertEqual(self.cache._reserved_bytes, 0)
        self.assertFalse(self.cache.get_cache_path("/f.bin").exists())
        self.assertEqual(sorted(p.name for p in self.cache_dir.rglob(f"*{PARTIAL_SUFFIX}")), [])


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


class TestCacheSizeMark(unittest.TestCase):
    """mark_cache_size_dirty takes no lock and is never lost to a rescan."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.cache_dir.mkdir()
        self.state_manager = StateManager(str(Path(self.temp_dir) / "state.db"))
        self.cache_manager = CacheManager(self.cache_dir, self.state_manager, 1024 * 1024)

    def tearDown(self):
        self.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_mark_does_not_wait_for_the_cache_lock(self):
        held = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)

        def hold():
            with self.cache_manager._lock:
                held.set()
                release.wait(timeout=5)

        holder = threading.Thread(target=hold, daemon=True)
        holder.start()
        self.assertTrue(held.wait(timeout=3))
        (self.cache_dir / "new.bin").write_bytes(b"x" * 10)
        marker = threading.Thread(target=self.cache_manager.mark_cache_size_dirty, daemon=True)
        marker.start()
        marker.join(timeout=1)
        alive = marker.is_alive()
        release.set()
        holder.join(timeout=3)
        self.assertFalse(alive)
        self.assertEqual(self.cache_manager.get_total_cache_size(), 10)

    def test_mark_during_a_rescan_is_not_lost(self):
        (self.cache_dir / "first.bin").write_bytes(b"x" * 10)
        self.cache_manager.mark_cache_size_dirty()
        real_scan = self.cache_manager._scan_total_cache_size

        def scan():
            total = real_scan()
            # A file lands after the scan counted, and is marked meanwhile.
            (self.cache_dir / "second.bin").write_bytes(b"y" * 20)
            self.cache_manager.mark_cache_size_dirty()
            return total

        with patch.object(self.cache_manager, "_scan_total_cache_size", side_effect=scan):
            self.assertEqual(self.cache_manager.get_total_cache_size(), 10)
        self.assertEqual(self.cache_manager.get_total_cache_size(), 30)
