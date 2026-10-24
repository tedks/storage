import errno
import json
import os
import shutil
import stat
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fuse import FuseOSError

from storage.backends.local import LocalBackend
from storage.fs import StorageFS
from storage.models import FileMetadata, SyncStatus
from storage.sync import DirOperation


class TestFSNamespaceIntegration(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_dir = self.temp_dir / "cache"
        self.cache_dir.mkdir()
        self.backend1_dir = self.temp_dir / "backend1"
        self.backend2_dir = self.temp_dir / "backend2"
        self.backend1_dir.mkdir()
        self.backend2_dir.mkdir()
        self.db_path = self.temp_dir / "state.db"

        self.backends = {
            "backend1": LocalBackend("backend1", str(self.backend1_dir)),
            "backend2": LocalBackend("backend2", str(self.backend2_dir)),
        }

        self.fs = StorageFS(
            cache_root=self.cache_dir,
            state_db=self.db_path,
            backends=self.backends,
            cache_size=10 * 1024 * 1024,
            num_sync_workers=1,
        )

    def tearDown(self):
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _wait_for(self, predicate, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.05)
        return predicate()

    def test_readdir_merges_backends_and_cache(self):
        # Files on different backends
        (self.backend1_dir / "foo.txt").write_text("one")
        (self.backend2_dir / "bar.txt").write_text("two")

        # Cache entry only
        cache_file = self.cache_dir / "baz.txt"
        cache_file.write_text("three")

        entries = set(self.fs.readdir('/', None))
        self.assertIn('foo.txt', entries)
        self.assertIn('bar.txt', entries)
        self.assertIn('baz.txt', entries)

    def test_readdir_falls_back_to_last_known_on_error(self):
        # Prime cache with successful listing
        (self.backend1_dir / "primed.txt").write_text("data")
        list(self.fs.readdir('/', None))

        # Simulate backend failure on subsequent calls
        original = self.backends['backend1'].listdir
        def _failing(_path):
            raise RuntimeError("backend offline")
        self.backends['backend1'].listdir = _failing

        entries = set(self.fs.readdir('/', None))
        self.assertIn('primed.txt', entries)

        # Restore for cleanliness
        self.backends['backend1'].listdir = original

    def test_readdir_uses_recent_snapshot_for_bursty_calls(self):
        (self.backend1_dir / "snap.txt").write_text("data")
        entries = set(self.fs.readdir('/', None))
        self.assertIn('snap.txt', entries)

        with patch.object(self.backends['backend1'], 'listdir_attrs', side_effect=RuntimeError('should not be called')):
            snap_entries = set(self.fs.readdir('/', None))
        self.assertIn('snap.txt', snap_entries)


    def test_getattr_remote_only_file(self):
        content = "hello"
        (self.backend1_dir / "remote.txt").write_text(content)

        attrs = self.fs.getattr('/remote.txt')
        self.assertTrue(stat.S_ISREG(attrs['st_mode']))
        self.assertEqual(attrs['st_size'], len(content))
        self.assertNotEqual(attrs['st_mtime'], 0)
        self.assertNotEqual(attrs['st_uid'], 0)

    def test_getattr_remote_only_directory(self):
        (self.backend2_dir / "subdir").mkdir()

        attrs = self.fs.getattr('/subdir')
        self.assertTrue(stat.S_ISDIR(attrs['st_mode']))

    def test_getattr_skips_unavailable_backend(self):
        content = "from-backend2"
        (self.backend2_dir / "fast.txt").write_text(content)

        self.backends["backend1"].set_available(False)

        with patch.object(self.backends["backend1"], "stat", side_effect=AssertionError("unavailable backend should not be probed")):
            attrs = self.fs.getattr('/fast.txt')

        self.assertEqual(attrs['st_size'], len(content))

    def test_getattr_targeted_unavailable_backend_returns_hostunreach(self):
        self.backends["backend1"].set_available(False)

        with self.assertRaises(FuseOSError) as ctx:
            self.fs.getattr('/missing.backend1')

        self.assertEqual(ctx.exception.errno, errno.EHOSTUNREACH)

    def test_readdir_probes_backend_even_if_marked_unavailable(self):
        self.backends["backend1"].set_available(False)
        (self.backend1_dir / "recover.txt").write_text("data")

        entries = set(self.fs.readdir('/', None))

        self.assertIn("recover.txt", entries)

    def test_readdir_thread_safety(self):
        (self.backend1_dir / "threaded.txt").write_text("data")

        def worker():
            list(self.fs.readdir('/', None))

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def test_readdir_prefers_listdir_attrs_when_available(self):
        (self.backend1_dir / "attrs.txt").write_text("data")

        with patch.object(self.backends["backend1"], "listdir", side_effect=RuntimeError("listdir should not be used")):
            entries = set(self.fs.readdir('/', None))

        self.assertIn("attrs.txt", entries)

    def test_readdir_schedules_checksums_only_for_collisions(self):
        (self.backend1_dir / "left.txt").write_text("left")
        (self.backend2_dir / "right.txt").write_text("right")

        with patch.object(self.fs, "_schedule_checksum_task") as schedule_task:
            list(self.fs.readdir('/', None))
        schedule_task.assert_not_called()

        (self.backend2_dir / "left.txt").write_text("same-name")
        self.fs._invalidate_dir_cache_entry('/')
        with patch.object(self.fs, "_schedule_checksum_task") as schedule_task:
            list(self.fs.readdir('/', None))
        self.assertEqual(schedule_task.call_count, 2)

    def test_getattr_after_readdir_uses_listing_attrs(self):
        (self.backend1_dir / "listed.txt").write_text("listed")
        (self.backend2_dir / "listeddir").mkdir()
        list(self.fs.readdir('/', None))

        with patch.object(self.backends["backend1"], "stat", side_effect=AssertionError("listed entry was probed")), \
                patch.object(self.backends["backend2"], "stat", side_effect=AssertionError("listed entry was probed")):
            file_attrs = self.fs.getattr('/listed.txt')
            dir_attrs = self.fs.getattr('/listeddir')

        self.assertTrue(stat.S_ISREG(file_attrs['st_mode']))
        self.assertEqual(file_attrs['st_size'], len("listed"))
        self.assertEqual(file_attrs['st_mtime'], (self.backend1_dir / "listed.txt").stat().st_mtime)
        self.assertTrue(stat.S_ISDIR(dir_attrs['st_mode']))

    def test_getattr_listing_attrs_follow_member_order(self):
        (self.backend1_dir / "both.txt").write_text("one")
        (self.backend2_dir / "both.txt").write_text("second member")
        list(self.fs.readdir('/', None))

        self.assertEqual(self.fs.getattr('/both.txt')['st_size'], len("one"))
        self.assertEqual(self.fs.getattr('/both.txt.backend2')['st_size'], len("second member"))

        self.backends["backend1"].set_available(False)
        self.assertEqual(self.fs.getattr('/both.txt')['st_size'], len("second member"))

    def test_getattr_probes_members_after_listing_expires(self):
        self.fs.namespace.readdir_snapshot_ttl = 0.05
        (self.backend1_dir / "aging.txt").write_text("old")
        list(self.fs.readdir('/', None))
        (self.backend1_dir / "aging.txt").write_text("newer bytes")
        time.sleep(0.1)

        self.assertEqual(self.fs.getattr('/aging.txt')['st_size'], len("newer bytes"))

    def test_getattr_probes_members_after_invalidation(self):
        (self.backend1_dir / "sub").mkdir()
        (self.backend1_dir / "sub" / "f.txt").write_text("old")
        list(self.fs.readdir('/sub', None))
        (self.backend1_dir / "sub" / "f.txt").write_text("newer bytes")

        self.fs._invalidate_dir_cache_entry('/sub/f.txt')
        self.assertEqual(self.fs.getattr('/sub/f.txt')['st_size'], len("newer bytes"))

    def test_getattr_probes_unlisted_name(self):
        list(self.fs.readdir('/', None))
        (self.backend2_dir / "late.txt").write_text("late")

        self.assertEqual(self.fs.getattr('/late.txt')['st_size'], len("late"))

    def test_getattr_leaves_member_symlink_to_member_stat(self):
        # Listings are lstat; whether a symlink is followed is the member's
        # stat's call (SSH follows it), so getattr still asks the member.
        (self.backend1_dir / "target.txt").write_text("target")
        (self.backend1_dir / "link.txt").symlink_to("target.txt")
        list(self.fs.readdir('/', None))

        member = self.backends["backend1"]
        with patch.object(member, "stat", wraps=member.stat) as member_stat:
            self.fs.getattr('/link.txt')
        member_stat.assert_called_once_with('/link.txt')

    def test_readdir_checksum_scheduling_skips_directories_without_isdir(self):
        (self.backend1_dir / "shared").mkdir()
        (self.backend2_dir / "shared").mkdir()
        (self.backend1_dir / "dup.txt").write_text("a")
        (self.backend2_dir / "dup.txt").write_text("b")

        with patch.object(self.fs, "_schedule_checksum_task") as schedule_task, \
                patch.object(self.backends["backend1"], "isdir", side_effect=AssertionError("isdir probed")), \
                patch.object(self.backends["backend2"], "isdir", side_effect=AssertionError("isdir probed")):
            list(self.fs.readdir('/', None))

        scheduled = sorted((call.args[0].name, call.args[1]) for call in schedule_task.call_args_list)
        self.assertEqual(scheduled, [("backend1", "/dup.txt"), ("backend2", "/dup.txt")])

    def test_getattr_probes_first_member_listed_without_attrs(self):
        # The first member decides: a listing without attributes for it means
        # its stat, not the next member's listing, answers.
        (self.backend1_dir / "both.txt").write_text("first")
        (self.backend2_dir / "both.txt").write_text("second member")
        with patch.object(self.backends["backend1"], "listdir_attrs", return_value=None):
            list(self.fs.readdir('/', None))

        self.assertEqual(self.fs.getattr('/both.txt')['st_size'], len("first"))

    def test_getattr_probes_listing_with_missing_fields(self):
        (self.backend1_dir / "partial.txt").write_text("partial")
        listing = [("partial.txt", {"st_mode": 0o100644, "st_size": None, "st_mtime": None})]
        with patch.object(self.backends["backend1"], "listdir_attrs", return_value=listing):
            list(self.fs.readdir('/', None))

        attrs = self.fs.getattr('/partial.txt')
        self.assertEqual(attrs['st_size'], len("partial"))
        self.assertIsNotNone(attrs['st_mtime'])

    def test_readdir_does_not_publish_listing_that_raced_invalidation(self):
        # Members are listed under dir_cache_lock; an invalidation (a watcher
        # callback) can land between that and publication, while checksums
        # are scheduled. The listing it raced must not be published.
        (self.backend1_dir / "raced.txt").write_text("old")
        namespace = self.fs.namespace
        real = namespace._schedule_directory_checksums

        def schedule_then_change(*args, **kwargs):
            real(*args, **kwargs)
            (self.backend1_dir / "raced.txt").write_text("newer bytes")
            self.fs._invalidate_dir_cache_entry('/')

        with patch.object(namespace, "_schedule_directory_checksums", side_effect=schedule_then_change):
            list(self.fs.readdir('/', None))

        self.assertNotIn('/', namespace.entry_attrs)
        self.assertNotIn('/', self.fs._readdir_snapshot)
        self.assertEqual(self.fs.getattr('/raced.txt')['st_size'], len("newer bytes"))

    def test_readdir_publishes_listing_despite_unrelated_invalidation(self):
        (self.backend1_dir / "kept.txt").write_text("kept")
        (self.backend1_dir / "elsewhere").mkdir()
        namespace = self.fs.namespace
        real = namespace._schedule_directory_checksums

        def schedule_then_unrelated_change(*args, **kwargs):
            real(*args, **kwargs)
            self.fs._invalidate_dir_cache_entry('/elsewhere/other.txt')

        with patch.object(namespace, "_schedule_directory_checksums", side_effect=schedule_then_unrelated_change):
            list(self.fs.readdir('/', None))

        self.assertIn('/', namespace.entry_attrs)
        self.assertIn('/', self.fs._readdir_snapshot)
        self.assertEqual(namespace._listings_in_flight, {})
        self.assertEqual(namespace._listing_generation, {})

    def test_readdir_failure_clears_in_flight_bookkeeping(self):
        # The in-flight maps are cleaned in a `finally`; a listing that
        # raises must not leave its directory marked in flight forever.
        namespace = self.fs.namespace

        with patch.object(namespace, "_list_members", side_effect=RuntimeError("listing failed")):
            with self.assertRaisesRegex(RuntimeError, "listing failed"):
                namespace.readdir('/')

        self.assertEqual(namespace._listings_in_flight, {})
        self.assertEqual(namespace._listing_generation, {})

    def test_conflict_view_invalidation_drops_canonical_listing(self):
        (self.backend1_dir / "dup.txt").write_text("one")
        (self.backend2_dir / "dup.txt").write_text("two")
        list(self.fs.readdir('/', None))
        (self.backend2_dir / "dup.txt").write_text("changed on two")

        self.fs._invalidate_dir_cache_entry('/dup.txt.backend2')

        self.assertEqual(self.fs.getattr('/dup.txt.backend2')['st_size'], len("changed on two"))

    def test_child_invalidation_keeps_sibling_attrs(self):
        (self.backend1_dir / "changed.txt").write_text("old")
        (self.backend1_dir / "sibling.txt").write_text("sibling")
        list(self.fs.readdir('/', None))

        self.fs._invalidate_dir_cache_entry('/changed.txt')
        (self.backend1_dir / "changed.txt").write_text("newer bytes")

        with patch.object(self.backends["backend1"], "stat", wraps=self.backends["backend1"].stat) as member_stat:
            self.assertEqual(self.fs.getattr('/sibling.txt')['st_size'], len("sibling"))
            self.assertEqual(self.fs.getattr('/changed.txt')['st_size'], len("newer bytes"))
        member_stat.assert_called_once_with('/changed.txt')

    def test_listing_attrs_age_is_capped_with_unbounded_ttl(self):
        self.fs.namespace.readdir_snapshot_ttl = float('inf')
        (self.backend1_dir / "capped.txt").write_text("old")
        with patch.object(type(self.fs.namespace), "LISTING_ATTRS_MAX_AGE", 0.05):
            list(self.fs.readdir('/', None))
            (self.backend1_dir / "capped.txt").write_text("newer bytes")
            (self.backend1_dir / "appeared.txt").write_text("new")
            time.sleep(0.1)

            self.assertEqual(self.fs.getattr('/capped.txt')['st_size'], len("newer bytes"))
            # The snapshot is not reused past the cap either: readdir relists.
            self.assertIn("appeared.txt", set(self.fs.readdir('/', None)))

    def test_readdir_prunes_expired_listing_attrs(self):
        (self.backend1_dir / "a").mkdir()
        (self.backend1_dir / "b").mkdir()
        self.fs.namespace.readdir_snapshot_ttl = 0.05
        list(self.fs.readdir('/a', None))
        time.sleep(0.1)
        list(self.fs.readdir('/b', None))

        self.assertEqual(set(self.fs.namespace.entry_attrs), {'/b'})

    def test_readdir_checksum_scheduling_follows_symlinked_directories(self):
        (self.backend1_dir / "real").mkdir()
        (self.backend2_dir / "real").mkdir()
        (self.backend1_dir / "linked").symlink_to("real")
        (self.backend2_dir / "linked").symlink_to("real")

        with patch.object(self.fs, "_schedule_checksum_task") as schedule_task:
            list(self.fs.readdir('/', None))

        schedule_task.assert_not_called()

    OLD = b"old"
    NEW = b"newer bytes"

    def _cache_clean(self, name, content):
        """A cached copy of /name with a CLEAN row describing it."""
        cache_path = self.cache_dir / name
        cache_path.write_bytes(content)
        st = cache_path.lstat()
        self.fs.state_manager.set_file_metadata(FileMetadata(
            virtual_path=f"/{name}", size=st.st_size, checksum="c", last_access=1.0,
            last_modified=st.st_mtime, sync_status=SyncStatus.CLEAN, replica_count=1,
            recomputable=False, recompute_cmd=None, encrypted=False,
            encryption_key_id=None,
        ))
        return cache_path

    def _listed_then_synced(self, name):
        """List /name at OLD, then a local write of NEW that has synced.

        The watcher's echo of the sync is lost (nothing invalidates the
        listing), and the readdir TTL is unbounded, as with watchers on.
        """
        self.fs.sync_manager.stop()
        self.fs.namespace.readdir_snapshot_ttl = float('inf')
        (self.backend1_dir / name).write_bytes(self.OLD)
        list(self.fs.readdir('/', None))
        cache_path = self._cache_clean(name, self.NEW)
        (self.backend1_dir / name).write_bytes(self.NEW)
        return cache_path

    def test_eviction_forgets_listing_attrs(self):
        cache_path = self._listed_then_synced("evicted.txt")

        self.assertTrue(self.fs.cache_manager.evict_lru_files(bytes_needed=1))

        self.assertFalse(cache_path.exists())
        self.assertEqual(self.fs.getattr('/evicted.txt')['st_size'], len(self.NEW))

    def test_eviction_keeps_a_racing_listing_unpublished(self):
        # A listing that read the member before the sync and publishes after
        # the eviction must not put the old attributes back.
        self.fs.sync_manager.stop()
        self.fs.namespace.readdir_snapshot_ttl = float('inf')
        (self.backend1_dir / "raced.txt").write_bytes(self.OLD)
        cache_path = self._cache_clean("raced.txt", self.NEW)
        namespace = self.fs.namespace
        real = namespace._schedule_directory_checksums

        def schedule_then_sync_and_evict(*args, **kwargs):
            real(*args, **kwargs)
            (self.backend1_dir / "raced.txt").write_bytes(self.NEW)
            self.assertTrue(self.fs.cache_manager.evict_lru_files(bytes_needed=1))

        with patch.object(namespace, "_schedule_directory_checksums", side_effect=schedule_then_sync_and_evict):
            list(self.fs.readdir('/', None))

        self.assertFalse(cache_path.exists())
        self.assertNotIn('/', namespace.entry_attrs)
        self.assertEqual(self.fs.getattr('/raced.txt')['st_size'], len(self.NEW))

    def test_invalidation_forgets_listing_attrs(self):
        # _invalidate_file_cache alone, without the watcher's directory
        # invalidation that _on_remote_change also makes.
        cache_path = self._listed_then_synced("invalidated.txt")

        self.fs._invalidate_file_cache('/invalidated.txt')

        self.assertFalse(cache_path.exists())
        self.assertEqual(self.fs.getattr('/invalidated.txt')['st_size'], len(self.NEW))

    def test_conflict_copy_invalidation_forgets_canonical_listing(self):
        (self.backend1_dir / "dup.txt").write_bytes(b"one")
        (self.backend2_dir / "dup.txt").write_bytes(self.OLD)
        self.fs.sync_manager.stop()
        self.fs.namespace.readdir_snapshot_ttl = float('inf')
        list(self.fs.readdir('/', None))
        cache_path = self.cache_dir / "dup.txt.backend2"
        cache_path.write_bytes(self.NEW)
        (self.backend2_dir / "dup.txt").write_bytes(self.NEW)

        self.fs._invalidate_file_cache('/dup.txt.backend2')

        self.assertFalse(cache_path.exists())
        self.assertEqual(self.fs.getattr('/dup.txt.backend2')['st_size'], len(self.NEW))

    def test_local_write_forgets_listing_attrs(self):
        self.fs.sync_manager.stop()
        (self.backend1_dir / "written.txt").write_bytes(self.OLD)
        (self.backend2_dir / "written.txt").write_bytes(self.OLD)
        list(self.fs.readdir('/', None))
        fd = self.fs.open('/written.txt', os.O_RDWR)
        listing = self.fs.namespace.entry_attrs['/'][1]
        self.assertIn("written.txt", listing["backend1"])

        self.fs.write('/written.txt', self.NEW, 0, fd)
        self.fs.release('/written.txt', fd)

        self.assertNotIn("written.txt", listing["backend1"])
        self.assertNotIn("written.txt", listing["backend2"])
        # The copy is the only record of the write; getattr reads it.
        self.assertEqual(self.fs.getattr('/written.txt')['st_size'], len(self.NEW))

    def test_forgetting_listing_attrs_does_not_wait_for_a_listing(self):
        # A listing holds dir_cache_lock across its member round trips;
        # eviction and invalidation run under the index and descriptor
        # locks and must not wait for it.
        evicted = self._listed_then_synced("evicted.txt")
        invalidated = self._cache_clean("invalidated.txt", self.NEW)
        written = self._cache_clean("written.txt", self.NEW)
        finished = threading.Event()

        def evict_and_invalidate():
            written.write_bytes(self.NEW + self.NEW)
            self.fs.cache_manager.mark_file_dirty('/written.txt', written)
            self.fs._invalidate_file_cache('/invalidated.txt')
            self.fs.cache_manager.evict_lru_files(bytes_needed=1)
            finished.set()

        worker = threading.Thread(target=evict_and_invalidate, daemon=True)
        with self.fs._dir_cache_lock:
            worker.start()
            self.assertTrue(finished.wait(5.0))
        worker.join()
        self.assertFalse(evicted.exists())
        self.assertFalse(invalidated.exists())
        self.assertTrue(written.exists())

    def test_dir_cache_scoped_per_instance(self):
        # Populate cache in first instance
        (self.backend1_dir / "onlyfirst.txt").write_text("one")
        list(self.fs.readdir('/', None))

        # New instance with same cache path but different DB should not share dir_cache
        other_db = self.temp_dir / "other.db"
        other_fs = StorageFS(
            cache_root=self.cache_dir,
            state_db=other_db,
            backends=self.backends,
            cache_size=10 * 1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
        )
        self.assertEqual({}, other_fs._dir_cache)
        other_fs.state_manager.close()

    def test_mkdir_propagates_to_backends(self):
        self.fs.mkdir('/newdir', 0o755)

        self.assertTrue((self.cache_dir / 'newdir').is_dir())
        self.assertTrue(self._wait_for(lambda: self.backends['backend1'].isdir('/newdir')))
        self.assertTrue(self._wait_for(lambda: self.backends['backend2'].isdir('/newdir')))

    def test_rmdir_propagates_to_backends(self):
        path = '/todelete'
        self.fs.mkdir(path, 0o755)
        self.assertTrue((self.cache_dir / 'todelete').is_dir())

        self.fs.rmdir(path)

        self.assertFalse((self.cache_dir / 'todelete').exists())
        self.assertTrue(self._wait_for(lambda: not self.backends['backend1'].isdir(path)))
        self.assertTrue(self._wait_for(lambda: not self.backends['backend2'].isdir(path)))

    def test_rename_directory_syncs_backends_and_state(self):
        self.fs.mkdir('/dir', 0o755)
        vfd = self.fs.create('/dir/file.txt', 0o644)
        data = b'data'
        self.fs.write('/dir/file.txt', data, 0, vfd)
        self.fs.release('/dir/file.txt', vfd)

        self.assertTrue(self.fs.sync_manager.sync_now('/dir/file.txt', timeout=5.0))

        self.fs.rename('/dir', '/renamed')

        self.assertTrue((self.cache_dir / 'renamed').is_dir())
        self.assertFalse((self.cache_dir / 'dir').exists())

        self.assertTrue(self._wait_for(lambda: not self.backend1_dir.joinpath('dir').exists()))
        self.assertTrue(self._wait_for(lambda: self.backend1_dir.joinpath('renamed').is_dir()))
        self.assertTrue(self._wait_for(lambda: self.backend1_dir.joinpath('renamed', 'file.txt').exists()))
        self.assertTrue(self._wait_for(lambda: not self.backend2_dir.joinpath('dir').exists()))
        self.assertTrue(self._wait_for(lambda: self.backend2_dir.joinpath('renamed').is_dir()))

        old_meta = self.fs.state_manager.get_file_metadata('/dir/file.txt')
        new_meta = self.fs.state_manager.get_file_metadata('/renamed/file.txt')
        self.assertIsNone(old_meta)
        self.assertIsNotNone(new_meta)

        entries = set(self.fs.readdir('/', None))
        self.assertIn('renamed', entries)
        self.assertNotIn('dir', entries)

    def test_rename_file_targets_only_backends_with_existing_location(self):
        vfd = self.fs.create('/staged.tmp', 0o644)
        data = b'staged-data'
        self.fs.write('/staged.tmp', data, 0, vfd)
        self.fs.release('/staged.tmp', vfd)

        self.assertTrue(self.fs.sync_manager.sync_now('/staged.tmp', timeout=5.0))

        remote_locations = [
            loc.backend_name
            for loc in self.fs.state_manager.get_file_locations('/staged.tmp')
            if not loc.is_cached
        ]
        self.assertEqual(1, len(remote_locations))
        remote_backend = remote_locations[0]
        untouched_backend = 'backend2' if remote_backend == 'backend1' else 'backend1'

        original_remote_rename = self.backends[remote_backend].rename

        with patch.object(
            self.backends[remote_backend],
            'rename',
            wraps=original_remote_rename,
        ) as rename_remote, patch.object(
            self.backends[untouched_backend],
            'rename',
            side_effect=AssertionError("rename should not fan out to backends without the file"),
        ):
            self.fs.rename('/staged.tmp', '/final.txt')

        rename_remote.assert_called_once_with('/staged.tmp', '/final.txt')
        self.assertTrue(self.backends[remote_backend].exists('/final.txt'))
        self.assertFalse(self.backends[remote_backend].exists('/staged.tmp'))

        new_locations = [
            loc.backend_name
            for loc in self.fs.state_manager.get_file_locations('/final.txt')
            if not loc.is_cached
        ]
        self.assertEqual([remote_backend], new_locations)

    def test_mkdir_retry_targets_failed_backend(self):
        path = '/needretry'
        original = self.backends['backend2'].mkdir

        def failing_mkdir(_path):
            raise RuntimeError("backend2 offline")

        self.backends['backend2'].mkdir = failing_mkdir
        try:
            with patch.object(self.fs.sync_manager, 'queue_dir_sync') as mock_queue:
                self.fs.mkdir(path, 0o755)
            mock_queue.assert_called_with(
                path,
                DirOperation.MKDIR,
                mode=0o755,
                backend_hint='backend2',
                priority=80,
                force=True,
            )
        finally:
            self.backends['backend2'].mkdir = original

    def test_rmdir_retry_targets_failed_backend(self):
        path = '/deleteme'
        self.fs.mkdir(path, 0o755)
        original = self.backends['backend1'].rmdir

        def failing_rmdir(_path):
            raise RuntimeError("backend1 busy")

        self.backends['backend1'].rmdir = failing_rmdir
        try:
            with patch.object(self.fs.sync_manager, 'queue_dir_sync') as mock_queue:
                self.fs.rmdir(path)
            mock_queue.assert_called_with(
                path,
                DirOperation.RMDIR,
                backend_hint='backend1',
                priority=80,
                force=True,
            )
        finally:
            self.backends['backend1'].rmdir = original

    def test_rename_retry_targets_failed_backend(self):
        self.fs.mkdir('/orig', 0o755)
        target = '/renamed'
        original = self.backends['backend2'].rename

        def failing_rename(old, new):
            raise RuntimeError("backend2 down")

        self.backends['backend2'].rename = failing_rename
        try:
            with patch.object(self.fs.sync_manager, 'queue_dir_sync') as mock_queue:
                self.fs.rename('/orig', target)
            mock_queue.assert_called_with(
                '/orig',
                DirOperation.RENAME,
                new_path=target,
                backend_hint='backend2',
                priority=90,
                force=True,
            )
        finally:
            self.backends['backend2'].rename = original

    def test_mkdir_retry_eventually_replicates(self):
        path = '/asyncdir'
        backend2 = self.backends['backend2']
        call_count = {'n': 0}
        original = backend2.mkdir

        def flaky_mkdir(target):
            call_count['n'] += 1
            if call_count['n'] == 1:
                raise RuntimeError("transient failure")
            return original(target)

        backend2.mkdir = flaky_mkdir
        try:
            self.fs.mkdir(path, 0o755)
            # Should not exist on backend2 immediately
            self.assertFalse(backend2.isdir(path))

            def replicated():
                return backend2.isdir(path)

            self.assertTrue(self._wait_for(replicated, timeout=5.0))
        finally:
            backend2.mkdir = original



if __name__ == '__main__':
    unittest.main()
