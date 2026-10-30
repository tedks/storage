import errno
import json
import os
import shutil
import sqlite3
import stat
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml
from fuse import FuseOSError

from storage.backends.base import ChecksumUnavailable
from storage.backends.local import LocalBackend
from storage.checksums import ChecksumRecord, fast_checksum_bytes
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
        self.fs.namespace.shutdown()
        self.fs._checksum_queues.shutdown()
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

        # A different size is a conflict without hashing.
        (self.backend2_dir / "left.txt").write_text("same-name")
        self.fs._invalidate_dir_cache_entry('/')
        with patch.object(self.fs, "_schedule_checksum_task") as schedule_task:
            self.assertIn("left.txt.backend2", set(self.fs.readdir('/', None)))
        schedule_task.assert_not_called()

        # The same size proves nothing: both copies are hashed.
        (self.backend2_dir / "left.txt").write_text("lift")
        self.fs._invalidate_dir_cache_entry('/')
        with patch.object(self.fs, "_schedule_checksum_task") as schedule_task:
            self.assertIn("left.txt", set(self.fs.readdir('/', None)))
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
        self.fs.namespace.listing_max_stale = 0.05
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

    def test_getattr_answers_unlisted_name_from_fresh_listing(self):
        # Ruling oq-listing-negative-lookup A: a name no available member
        # listed in a fresh listing does not exist; no member is asked. A
        # file added on a member behind the mount stays invisible until the
        # listing is no longer fresh.
        list(self.fs.readdir('/', None))
        (self.backend2_dir / "late.txt").write_text("late")

        with patch.object(self.backends["backend1"], "stat", side_effect=AssertionError("probed")), \
                patch.object(self.backends["backend2"], "stat", side_effect=AssertionError("probed")):
            with self.assertRaises(FuseOSError) as raised:
                self.fs.getattr('/late.txt')
        self.assertEqual(raised.exception.errno, errno.ENOENT)

    def test_getattr_probes_unlisted_name_once_listing_is_stale(self):
        self.fs.namespace.readdir_snapshot_ttl = 0.05
        list(self.fs.readdir('/', None))
        (self.backend2_dir / "late.txt").write_text("late")
        time.sleep(0.1)

        self.assertEqual(self.fs.getattr('/late.txt')['st_size'], len("late"))

    def test_getattr_probes_unlisted_name_when_a_member_listing_failed(self):
        (self.backend1_dir / "primed.txt").write_text("p")
        list(self.fs.readdir('/', None))
        self.fs._readdir_snapshot.clear()
        (self.backend2_dir / "late.txt").write_text("late")
        with patch.object(self.backends["backend2"], "listdir_attrs", side_effect=RuntimeError("down")):
            list(self.fs.readdir('/', None))

        self.assertEqual(self.fs.getattr('/late.txt')['st_size'], len("late"))

    def test_getattr_probes_unlisted_name_when_a_member_listed_without_attrs(self):
        (self.backend2_dir / "late.txt").write_text("late")
        with patch.object(self.backends["backend2"], "listdir_attrs", return_value=None), \
                patch.object(self.backends["backend2"], "listdir", return_value=[]):
            list(self.fs.readdir('/', None))

        self.assertEqual(self.fs.getattr('/late.txt')['st_size'], len("late"))

    def test_getattr_unlisted_name_skips_unavailable_member(self):
        list(self.fs.readdir('/', None))
        self.backends["backend2"].set_available(False)
        with patch.object(self.backends["backend1"], "stat", side_effect=AssertionError("probed")):
            with self.assertRaises(FuseOSError) as raised:
                self.fs.getattr('/late.txt')
        self.assertEqual(raised.exception.errno, errno.ENOENT)

    def test_getattr_probes_name_dropped_from_listing(self):
        # A dropped name is unknown, not absent: a file synced after the
        # listing and then evicted is on a member the listing never saw.
        list(self.fs.readdir('/', None))
        (self.backend1_dir / "synced.txt").write_text("synced")
        self.fs.namespace.forget_listed_attrs('/synced.txt')

        self.assertEqual(self.fs.getattr('/synced.txt')['st_size'], len("synced"))

    def test_symlink_is_refused_with_eperm(self):
        list(self.fs.readdir('/', None))
        with self.assertRaises(FuseOSError) as raised:
            self.fs.symlink('/link', 'target.txt')
        self.assertEqual(raised.exception.errno, errno.EPERM)
        self.assertFalse(os.path.lexists(self.cache_dir / "link"))
        self.assertFalse(os.path.lexists(self.cache_dir / "target.txt"))
        self.assertIn('/', self.fs.namespace.entry_attrs)

    def test_enabled_symlink_creates_the_link_path_and_drops_the_listing(self):
        # Kept for when links replicate: fusepy passes the link path first.
        list(self.fs.readdir('/', None))
        with patch.object(type(self.fs), "_SYMLINKS_ENABLED", True):
            self.fs.symlink('/link', 'target.txt')
        self.assertEqual(os.readlink(self.cache_dir / "link"), "target.txt")
        self.assertFalse(os.path.lexists(self.cache_dir / "target.txt"))
        self.assertNotIn('/', self.fs.namespace.entry_attrs)

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

    def _wrap_listdir_attrs(self, backend_name, before=None, after=None):
        backend = self.backends[backend_name]
        real = backend.listdir_attrs

        def wrapped(path):
            if before:
                before(path)
            result = real(path)
            if after:
                after(path)
            return result

        return patch.object(backend, "listdir_attrs", side_effect=wrapped)

    def test_readdir_lists_members_concurrently(self):
        # Each member's listing waits for the other's to start; listed one
        # after the other, the barrier breaks and both fall back to nothing.
        (self.backend1_dir / "one.txt").write_text("1")
        (self.backend2_dir / "two.txt").write_text("2")
        barrier = threading.Barrier(2, timeout=2)
        wait = lambda _path: barrier.wait()

        with self._wrap_listdir_attrs("backend1", before=wait), \
                self._wrap_listdir_attrs("backend2", before=wait):
            entries = set(self.fs.readdir('/', None))

        self.assertIn("one.txt", entries)
        self.assertIn("two.txt", entries)
        self.assertIn("/", self.fs.namespace.entry_attrs)
        self.assertEqual(set(self.fs.namespace.entry_attrs["/"][1]), {"backend1", "backend2"})

    def test_readdir_lists_members_outside_dir_cache_lock(self):
        (self.backend1_dir / "one.txt").write_text("1")
        held = []

        def probe_lock(_path):
            # Give the caller time to start waiting for this listing; a
            # caller holding the lock meanwhile never releases it in time.
            time.sleep(0.2)
            free = self.fs._dir_cache_lock.acquire(timeout=1)
            if free:
                self.fs._dir_cache_lock.release()
            held.append(not free)

        with self._wrap_listdir_attrs("backend1", before=probe_lock), \
                self._wrap_listdir_attrs("backend2", before=probe_lock):
            list(self.fs.readdir('/', None))

        self.assertEqual(held, [False, False])

    def test_readdir_concurrent_member_failure_falls_back_to_last_known(self):
        (self.backend1_dir / "primed.txt").write_text("1")
        (self.backend2_dir / "other.txt").write_text("2")
        list(self.fs.readdir('/', None))
        # Expire the snapshot and attributes, keeping dir_cache's last names.
        self.fs._readdir_snapshot.clear()
        self.fs.namespace.entry_attrs.clear()
        (self.backend2_dir / "newer.txt").write_text("3")

        with patch.object(self.backends["backend1"], "listdir_attrs", side_effect=RuntimeError("offline")):
            entries = set(self.fs.readdir('/', None))

        self.assertIn("primed.txt", entries)
        self.assertIn("newer.txt", entries)
        # The failed member keeps its last names and contributes no attributes.
        self.assertEqual(self.fs._dir_cache["/"]["backend1"], {"primed.txt"})
        self.assertNotIn("backend1", self.fs.namespace.entry_attrs["/"][1])
        self.assertIn("backend2", self.fs.namespace.entry_attrs["/"][1])

    def test_readdir_does_not_publish_listing_raced_during_member_io(self):
        # Members are listed outside dir_cache_lock, so an invalidation can
        # land while one is being listed. Neither the listing nor its names
        # in dir_cache may outlive it.
        (self.backend1_dir / "raced.txt").write_text("old")

        def change(_path):
            (self.backend1_dir / "raced.txt").write_text("newer bytes")
            self.fs._invalidate_dir_cache_entry('/')

        with self._wrap_listdir_attrs("backend1", after=change):
            entries = set(self.fs.readdir('/', None))

        self.assertIn("raced.txt", entries)
        namespace = self.fs.namespace
        self.assertNotIn('/', namespace.entry_attrs)
        self.assertNotIn('/', self.fs._readdir_snapshot)
        self.assertNotIn('/', self.fs._dir_cache)
        self.assertEqual(namespace._listings_in_flight, {})
        self.assertEqual(self.fs.getattr('/raced.txt')['st_size'], len("newer bytes"))

    def test_readdir_does_not_store_listing_raced_by_child_invalidation(self):
        # A watcher event for a child (_on_remote_change) during member I/O
        # bumps the parent's generation without popping the parent's names.
        (self.backend1_dir / "kept.txt").write_text("1")
        list(self.fs.readdir('/', None))
        previous = self.fs._dir_cache['/']
        self.fs._readdir_snapshot.clear()
        self.fs.namespace.entry_attrs.clear()
        (self.backend1_dir / "child.txt").write_text("new")

        def change(_path):
            self.fs._invalidate_dir_cache_entry('/child.txt')

        with self._wrap_listdir_attrs("backend1", after=change):
            entries = set(self.fs.readdir('/', None))

        self.assertIn("child.txt", entries)
        namespace = self.fs.namespace
        self.assertNotIn('/', namespace.entry_attrs)
        self.assertNotIn('/', self.fs._readdir_snapshot)
        self.assertIs(self.fs._dir_cache['/'], previous)
        self.assertEqual(namespace._listings_in_flight, {})

    def test_readdir_waits_for_member_listings_when_cache_listing_fails(self):
        finished = threading.Event()

        def slow(_path):
            time.sleep(0.3)

        with self._wrap_listdir_attrs("backend1", after=lambda _p: (slow(_p), finished.set())), \
                patch.object(self.fs.namespace.cache_manager, "get_cache_path",
                             side_effect=OSError(errno.EIO, "cache unreadable")):
            with self.assertRaises(OSError):
                self.fs.namespace.readdir('/')
            self.assertTrue(finished.is_set())

        self.assertEqual(self.fs.namespace._listings_in_flight, {})

    def test_readdir_does_not_store_listing_raced_by_child_eviction(self):
        # Evicting or invalidating a cached child (forget_listed_attrs with
        # removing=True) during member I/O keeps the listing from being
        # stored; a write (removing=False) must not, or an active writer
        # would starve listings of its directory.
        for removing, stored in ((True, False), (False, True)):
            with self.subTest(removing=removing):
                (self.backend1_dir / "kept.txt").write_text("1")
                self.fs._readdir_snapshot.clear()
                self.fs.namespace.entry_attrs.clear()
                list(self.fs.readdir('/', None))
                previous = self.fs._dir_cache['/']
                self.fs._readdir_snapshot.clear()
                self.fs.namespace.entry_attrs.clear()

                def evict(_path):
                    self.fs.namespace.forget_listed_attrs('/kept.txt', removing=removing)

                with self._wrap_listdir_attrs("backend1", after=evict):
                    list(self.fs.readdir('/', None))

                namespace = self.fs.namespace
                self.assertEqual('/' in namespace.entry_attrs, stored)
                self.assertEqual('/' in self.fs._readdir_snapshot, stored)
                self.assertEqual(self.fs._dir_cache['/'] is previous, not stored)

    def test_readdir_reuses_one_thread_per_member(self):
        threads = {"backend1": set(), "backend2": set()}

        def record(name):
            return lambda _path: threads[name].add(threading.current_thread())

        with self._wrap_listdir_attrs("backend1", before=record("backend1")), \
                self._wrap_listdir_attrs("backend2", before=record("backend2")):
            for _ in range(3):
                self.fs._readdir_snapshot.clear()
                list(self.fs.readdir('/', None))

        self.assertEqual(len(threads["backend1"]), 1)
        self.assertEqual(len(threads["backend2"]), 1)
        self.assertNotEqual(threads["backend1"], threads["backend2"])
        self.assertNotIn(threading.current_thread(), threads["backend1"] | threads["backend2"])

    def test_readdir_lists_inline_after_shutdown(self):
        (self.backend1_dir / "one.txt").write_text("1")
        list(self.fs.readdir('/', None))
        listing_threads = [
            t for t in threading.enumerate() if t.name.startswith("readdir-backend")
        ]
        self.assertTrue(listing_threads)

        self.fs.namespace.shutdown()
        for thread in listing_threads:
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())

        self.fs._readdir_snapshot.clear()
        seen = []
        with self._wrap_listdir_attrs("backend1", before=lambda _p: seen.append(threading.current_thread())):
            entries = set(self.fs.readdir('/', None))
        self.assertIn("one.txt", entries)
        self.assertEqual(seen, [threading.current_thread()])
        self.assertEqual(self.fs.namespace._listing_executors, {})

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

    def test_listing_never_expires_with_unbounded_ttl(self):
        # Watchers on every member: the listing stays until invalidated, with
        # no cap on its attributes' age, and nothing refreshes it.
        namespace = self.fs.namespace
        namespace.readdir_snapshot_ttl = float('inf')
        (self.backend1_dir / "kept.txt").write_text("old")
        list(self.fs.readdir('/', None))
        with namespace.listing_lock:  # a very old listing
            at, snapshot = namespace.readdir_snapshot['/']
            _, attrs, unknown = namespace.entry_attrs['/']
            namespace.readdir_snapshot['/'] = (at - 10 ** 7, snapshot)
            namespace.entry_attrs['/'] = (at - 10 ** 7, attrs, unknown)
        (self.backend1_dir / "kept.txt").write_text("newer bytes")
        (self.backend1_dir / "appeared.txt").write_text("new")

        self.assertEqual(self.fs.getattr('/kept.txt')['st_size'], len("old"))
        self.assertNotIn("appeared.txt", set(self.fs.readdir('/', None)))
        self.assertEqual(namespace._background_refreshes, set())

        self.fs._invalidate_dir_cache_entry('/')
        self.assertIn("appeared.txt", set(self.fs.readdir('/', None)))

    def test_readdir_prunes_expired_listing_attrs(self):
        (self.backend1_dir / "a").mkdir()
        (self.backend1_dir / "b").mkdir()
        self.fs.namespace.readdir_snapshot_ttl = 0.05
        self.fs.namespace.listing_max_stale = 0.05
        list(self.fs.readdir('/a', None))
        time.sleep(0.1)
        list(self.fs.readdir('/b', None))

        self.assertEqual(set(self.fs.namespace.entry_attrs), {'/b'})

    def test_readdir_keeps_stale_listing_attrs_until_max_stale(self):
        (self.backend1_dir / "a").mkdir()
        (self.backend1_dir / "b").mkdir()
        self.fs.namespace.readdir_snapshot_ttl = 0.05
        list(self.fs.readdir('/a', None))
        time.sleep(0.1)
        list(self.fs.readdir('/b', None))

        self.assertEqual(set(self.fs.namespace.entry_attrs), {'/a', '/b'})

    # Stale-while-revalidate (docs/browsing_latency_plan.md D1).

    def _make_stale(self, path='/'):
        """List path, then age its listing past the fresh window."""
        namespace = self.fs.namespace
        namespace.readdir_snapshot_ttl = 60.0
        list(self.fs.readdir(path, None))
        with namespace.listing_lock:
            at, snapshot = namespace.readdir_snapshot[path]
            _, attrs, unknown = namespace.entry_attrs[path]
            aged = at - 61.0
            namespace.readdir_snapshot[path] = (aged, snapshot)
            namespace.entry_attrs[path] = (aged, attrs, unknown)

    def _wait_refreshed(self, path='/'):
        namespace = self.fs.namespace
        self.assertTrue(self._wait_for(lambda: path not in namespace._background_refreshes))

    def test_stale_readdir_serves_old_listing_and_refreshes_in_background(self):
        (self.backend1_dir / "old.txt").write_text("o")
        self._make_stale()
        (self.backend2_dir / "new.txt").write_text("n")

        first = set(self.fs.readdir('/', None))
        self.assertIn("old.txt", first)
        self.assertNotIn("new.txt", first)
        self._wait_refreshed()

        self.assertIn("new.txt", set(self.fs.readdir('/', None)))
        self.assertEqual(self.fs.namespace._listings_in_flight, {})
        self.assertEqual(self.fs.namespace._listing_generation, {})

    def test_stale_getattr_answers_from_listing_and_refreshes(self):
        (self.backend1_dir / "f.txt").write_text("old")
        self._make_stale()
        (self.backend1_dir / "f.txt").write_text("newer bytes")

        with patch.object(self.backends["backend1"], "stat", side_effect=AssertionError("probed")), \
                patch.object(self.backends["backend2"], "stat", side_effect=AssertionError("probed")):
            self.assertEqual(self.fs.getattr('/f.txt')['st_size'], len("old"))
        self._wait_refreshed()
        self.assertEqual(self.fs.getattr('/f.txt')['st_size'], len("newer bytes"))

    def test_stale_listing_does_not_answer_enoent(self):
        list(self.fs.readdir('/', None))
        self._make_stale()
        (self.backend2_dir / "late.txt").write_text("late")

        self.assertEqual(self.fs.getattr('/late.txt')['st_size'], len("late"))
        self._wait_refreshed()

    def test_one_background_refresh_per_directory(self):
        (self.backend1_dir / "f.txt").write_text("f")
        self._make_stale()
        started, release = threading.Event(), threading.Event()
        calls = []

        def before(path):
            calls.append(path)
            started.set()
            release.wait(5)

        with self._wrap_listdir_attrs("backend1", before=before):
            began = time.monotonic()
            for _ in range(5):
                list(self.fs.readdir('/', None))
                self.fs.getattr('/f.txt')
            # Served while the refresh is still blocked in its member listing.
            self.assertLess(time.monotonic() - began, 2.0)
            self.assertFalse(release.is_set())
            self.assertTrue(started.wait(5))
            for _ in range(5):
                list(self.fs.readdir('/', None))
            release.set()
            self._wait_refreshed()
        self.assertEqual(calls, ['/'])

    def test_background_refresh_runs_off_the_calling_thread(self):
        self._make_stale()
        namespace = self.fs.namespace
        seen = []
        real = namespace._list_members

        def record(*args, **kwargs):
            seen.append(threading.current_thread().name)
            return real(*args, **kwargs)

        with patch.object(namespace, "_list_members", side_effect=record):
            list(self.fs.readdir('/', None))
            self._wait_refreshed()
        self.assertEqual(len(seen), 1)
        self.assertTrue(seen[0].startswith("readdir-refresh"), seen)

    def test_background_refresh_raced_by_invalidation_is_not_published(self):
        (self.backend1_dir / "f.txt").write_text("old")
        self._make_stale()
        namespace = self.fs.namespace
        started, release = threading.Event(), threading.Event()

        def before(_path):
            started.set()
            release.wait(5)

        with self._wrap_listdir_attrs("backend1", before=before):
            list(self.fs.readdir('/', None))
            self.assertTrue(started.wait(5))
            self.fs._invalidate_dir_cache_entry('/')
            release.set()
            self._wait_refreshed()

        self.assertNotIn('/', namespace.entry_attrs)
        self.assertNotIn('/', self.fs._readdir_snapshot)
        self.assertEqual(namespace._listings_in_flight, {})
        self.assertEqual(namespace._listing_generation, {})

    def test_background_refresh_raced_by_child_invalidation_is_not_published(self):
        (self.backend1_dir / "f.txt").write_text("old")
        self._make_stale()
        namespace = self.fs.namespace
        stale_at = namespace.entry_attrs['/'][0]
        started, release = threading.Event(), threading.Event()

        def before(_path):
            started.set()
            release.wait(5)

        with self._wrap_listdir_attrs("backend1", before=before):
            list(self.fs.readdir('/', None))
            self.assertTrue(started.wait(5))
            self.fs._invalidate_dir_cache_entry('/f.txt')
            release.set()
            self._wait_refreshed()

        # The stale listing stays (minus the child, now unknown); the raced
        # refresh did not replace it.
        self.assertEqual(namespace.entry_attrs['/'][0], stale_at)
        self.assertIn("f.txt", namespace.entry_attrs['/'][2])

    def test_older_background_refresh_does_not_replace_newer_listing(self):
        # A background refresh reads the old directory, then stalls; a
        # foreground listing (the stale one expired meanwhile) reads the new
        # file and publishes. The refresh finishing last must not put the
        # older read back, or the new file would be ENOENT for a window.
        (self.backend1_dir / "old.txt").write_text("o")
        namespace = self.fs.namespace
        namespace.listing_max_stale = 120.0
        self._make_stale()
        reads, release, stalled = [], threading.Event(), threading.Event()
        real_cache_path = namespace.cache_manager.get_cache_path

        def stall_in_refresh(path):
            if threading.current_thread().name.startswith("readdir-refresh"):
                stalled.set()
                release.wait(5)
            return real_cache_path(path)

        with self._wrap_listdir_attrs("backend1", after=reads.append), \
                self._wrap_listdir_attrs("backend2", after=reads.append), \
                patch.object(namespace.cache_manager, "get_cache_path", side_effect=stall_in_refresh):
            list(self.fs.readdir('/', None))  # stale: schedules the refresh
            self.assertTrue(stalled.wait(5))
            self.assertTrue(self._wait_for(lambda: len(reads) >= 2))  # both members read

            (self.backend2_dir / "new.txt").write_text("n")
            with namespace.listing_lock:  # expire the stale listing
                at, snapshot = namespace.readdir_snapshot['/']
                _, attrs, unknown = namespace.entry_attrs['/']
                namespace.readdir_snapshot['/'] = (at - 200.0, snapshot)
                namespace.entry_attrs['/'] = (at - 200.0, attrs, unknown)
            self.assertIn("new.txt", set(self.fs.readdir('/', None)))  # foreground
            release.set()
            self._wait_refreshed()

        self.assertIn("new.txt", set(self.fs.readdir('/', None)))
        self.assertEqual(self.fs.getattr('/new.txt')['st_size'], 1)
        with self.fs._dir_cache_lock:
            self.assertIn("new.txt", self.fs._dir_cache['/']['backend2'])

    def test_superseded_foreground_listing_answers_with_the_newer_one(self):
        # A foreground listing that read before a later listing published
        # returns the published names, not its own older read.
        (self.backend1_dir / "old.txt").write_text("o")
        namespace = self.fs.namespace
        real = namespace._schedule_directory_checksums
        state = {"done": False}

        def publish_newer_first(*args, **kwargs):
            real(*args, **kwargs)
            if not state["done"]:
                state["done"] = True
                (self.backend2_dir / "new.txt").write_text("n")
                generation = namespace._begin_listing('/')
                try:
                    namespace._list_members('/', generation)
                finally:
                    namespace._end_listing('/')

        with patch.object(namespace, "_schedule_directory_checksums", side_effect=publish_newer_first):
            entries = set(self.fs.readdir('/', None))
        self.assertIn("new.txt", entries)

    def test_listings_publish_whole_while_another_is_between_steps(self):
        # A later listing (on another thread) pauses after its member reads
        # while an earlier one finishes. Publication is one step, so the
        # earlier one never sees half of the later one: it publishes its own
        # read, and the later one replaces it when it resumes.
        (self.backend1_dir / "old.txt").write_text("o")
        namespace = self.fs.namespace
        real = namespace._schedule_directory_checksums
        inner_paused, inner_go = threading.Event(), threading.Event()
        inner_done = threading.Event()
        state = {"started": False}

        def inner():
            generation = namespace._begin_listing('/')
            try:
                namespace._list_members('/', generation)
            finally:
                namespace._end_listing('/')
                inner_done.set()

        def hook(*args, **kwargs):
            real(*args, **kwargs)
            if threading.current_thread().name == "inner-listing":
                inner_paused.set()
                inner_go.wait(5)
            elif not state["started"]:
                state["started"] = True
                (self.backend2_dir / "new.txt").write_text("n")
                threading.Thread(target=inner, name="inner-listing", daemon=True).start()
                self.assertTrue(inner_paused.wait(5))

        with patch.object(namespace, "_schedule_directory_checksums", side_effect=hook):
            outer = set(self.fs.readdir('/', None))
            self.assertNotIn("new.txt", outer)  # its own read; nothing newer published yet
            with namespace.listing_lock:
                self.assertEqual(namespace._listing_as_of['/'], namespace.readdir_snapshot['/'][0])
            inner_go.set()
            self.assertTrue(inner_done.wait(5))

        with namespace.listing_lock:
            self.assertEqual(namespace._listing_as_of['/'], namespace.readdir_snapshot['/'][0])
        self.assertIn("new.txt", set(self.fs.readdir('/', None)))

    def test_background_refresh_lists_no_member_inline_after_shutdown(self):
        namespace = self.fs.namespace
        self._make_stale()
        namespace.shutdown()
        with patch.object(self.backends["backend1"], "listdir_attrs", side_effect=AssertionError("listed")), \
                patch.object(self.backends["backend2"], "listdir_attrs", side_effect=AssertionError("listed")):
            namespace._list_members('/', namespace._begin_listing('/'), background=True)
        namespace._end_listing('/')

    def test_zero_window_has_no_stale_phase(self):
        namespace = self.fs.namespace
        namespace.readdir_snapshot_ttl = 0
        namespace.listing_max_stale = 3600.0
        list(self.fs.readdir('/', None))
        (self.backend2_dir / "new.txt").write_text("n")
        self.assertIn("new.txt", set(self.fs.readdir('/', None)))
        self.assertEqual(namespace._background_refreshes, set())

    def test_negative_answers_are_bounded_with_unbounded_window(self):
        # Watchers on every member: listings never expire, but a missing
        # name is stat'd once the listing is older than negative_window, so
        # a dropped watcher event does not hide a file for good.
        namespace = self.fs.namespace
        namespace.readdir_snapshot_ttl = float('inf')
        namespace.negative_window = 60.0
        list(self.fs.readdir('/', None))
        (self.backend2_dir / "late.txt").write_text("late")
        with self.assertRaises(FuseOSError):
            self.fs.getattr('/late.txt')
        with namespace.listing_lock:
            at, attrs, unknown = namespace.entry_attrs['/']
            namespace.entry_attrs['/'] = (at - 61.0, attrs, unknown)
        self.assertEqual(self.fs.getattr('/late.txt')['st_size'], len("late"))

    def test_stale_listing_reached_through_unknown_name_refreshes(self):
        (self.backend1_dir / "f.txt").write_text("f")
        self._make_stale()
        self.fs.namespace.forget_listed_attrs('/f.txt')
        self.fs.getattr('/f.txt')
        self.assertTrue(self._wait_for(lambda: '/' in self.fs.namespace.entry_attrs
                                       and 'f.txt' not in self.fs.namespace.entry_attrs['/'][2]))

    def test_served_snapshot_shows_conflict_split_after_hashing(self):
        (self.backend1_dir / "dup.txt").write_text("one")
        (self.backend2_dir / "dup.txt").write_text("two")
        self.fs.namespace.readdir_snapshot_ttl = 60.0
        with patch.object(self.fs, "_schedule_checksum_task"):  # no hashing yet
            self.assertIn("dup.txt", set(self.fs.readdir('/', None)))
        from storage.checksums import ChecksumRecord
        with self.fs._checksum_lock:
            for member, digest in (("backend1", "aa"), ("backend2", "bb")):
                self.fs._checksum_records[("/dup.txt", member)] = ChecksumRecord(
                    checksum=digest, timestamp=time.time())
        with patch.object(self.backends["backend1"], "listdir_attrs", side_effect=AssertionError("relisted")):
            entries = set(self.fs.readdir('/', None))
        self.assertIn("dup.txt.backend1", entries)
        self.assertIn("dup.txt.backend2", entries)
        self.assertNotIn("dup.txt", entries)

    def test_directory_rename_drops_subdirectory_listings(self):
        (self.backend1_dir / "current" / "sub").mkdir(parents=True)
        (self.backend1_dir / "current" / "sub" / "old.txt").write_text("o")
        (self.backend1_dir / "next" / "sub").mkdir(parents=True)
        (self.backend1_dir / "next" / "sub" / "new.txt").write_text("n")
        (self.cache_dir / "current").mkdir()
        (self.cache_dir / "next").mkdir()
        list(self.fs.readdir('/current/sub', None))
        list(self.fs.readdir('/next/sub', None))

        self.fs.rename('/current', '/current.old')
        self.fs.rename('/next', '/current')

        for gone in ('/current/sub', '/next/sub'):
            self.assertNotIn(gone, self.fs.namespace.entry_attrs)
        self.assertEqual(self.fs.getattr('/current/sub/new.txt')['st_size'], 1)
        self.assertIn("new.txt", set(self.fs.readdir('/current/sub', None)))

    def test_rmdir_drops_its_listings(self):
        (self.cache_dir / "gone").mkdir()
        (self.backend1_dir / "gone").mkdir()
        list(self.fs.readdir('/gone', None))
        self.fs.rmdir('/gone')
        self.assertNotIn('/gone', self.fs.namespace.entry_attrs)
        self.assertNotIn('/gone', self.fs.namespace._view_inputs)

    def test_background_refresh_failure_keeps_stale_listing(self):
        (self.backend1_dir / "f.txt").write_text("f")
        self._make_stale()
        namespace = self.fs.namespace
        with patch.object(namespace, "_list_members", side_effect=RuntimeError("boom")):
            self.assertIn("f.txt", set(self.fs.readdir('/', None)))
            self._wait_refreshed()
        self.assertIn('/', namespace.entry_attrs)
        self.assertEqual(namespace._listings_in_flight, {})
        self.assertEqual(namespace._listing_generation, {})

    def test_expired_listing_is_relisted_in_the_foreground(self):
        (self.backend1_dir / "old.txt").write_text("o")
        namespace = self.fs.namespace
        namespace.listing_max_stale = 120.0
        self._make_stale()
        with namespace.listing_lock:
            at, snapshot = namespace.readdir_snapshot['/']
            _, attrs, unknown = namespace.entry_attrs['/']
            namespace.readdir_snapshot['/'] = (at - 200.0, snapshot)
            namespace.entry_attrs['/'] = (at - 200.0, attrs, unknown)
        (self.backend2_dir / "new.txt").write_text("n")

        self.assertIn("new.txt", set(self.fs.readdir('/', None)))
        self.assertEqual(namespace._background_refreshes, set())

    def test_shutdown_tolerates_a_background_refresh_in_flight(self):
        (self.backend1_dir / "f.txt").write_text("f")
        (self.backend1_dir / "sub").mkdir()
        self._make_stale('/')
        self._make_stale('/sub')
        namespace = self.fs.namespace
        started, release = threading.Event(), threading.Event()

        def before(_path):
            started.set()
            release.wait(5)

        with self._wrap_listdir_attrs("backend1", before=before):
            list(self.fs.readdir('/', None))     # running
            self.assertTrue(started.wait(5))
            list(self.fs.readdir('/sub', None))  # queued behind it
            namespace.shutdown()
            release.set()
            self.assertTrue(self._wait_for(lambda: not namespace._background_refreshes))

        self.assertEqual(namespace._listings_in_flight, {})
        self.assertEqual(namespace._listing_generation, {})
        # After shutdown a stale listing schedules nothing.
        self._make_stale('/sub')
        list(self.fs.readdir('/sub', None))
        self.assertEqual(namespace._background_refreshes, set())

    def test_readdir_checksum_scheduling_follows_symlinked_directories(self):
        (self.backend1_dir / "real").mkdir()
        (self.backend2_dir / "real").mkdir()
        (self.backend1_dir / "linked").symlink_to("real")
        (self.backend2_dir / "linked").symlink_to("real")
        listing_thread = threading.get_ident()
        isdir_threads = []
        for backend in self.backends.values():
            real = backend.isdir

            def isdir(path, _real=real):
                isdir_threads.append(threading.get_ident())
                return _real(path)

            patcher = patch.object(backend, "isdir", side_effect=isdir)
            patcher.start()
            self.addCleanup(patcher.stop)
        hashes = self._hashes()

        # The link is followed on the hash worker, not the listing thread,
        # and a directory behind it is neither hashed nor recorded.
        self.assertIn("linked", self._list_and_settle())
        self.assertEqual(len(isdir_threads), 2)
        self.assertNotIn(listing_thread, isdir_threads)
        self.assertEqual([h for h in hashes if h[1] == "/linked"], [])
        self.assertNotIn(("/linked", "backend1"), self.fs._checksum_records)

    def test_view_hashes_never_download(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        member = self.backends["backend2"]
        with patch.object(member, "get_fast_checksum",
                          side_effect=ChecksumUnavailable(errno.EIO, "no remote hash")) as hashed:
            self.assertIn("dup.txt", self._list_and_settle())
            self.assertEqual(hashed.call_args.kwargs, {"allow_download": False})
            # Unverified: plain name, marked in the xattr, and not retried
            # while the listing reports the same basis.
            self.assertIn("dup.txt", self._list_and_settle())
            self.assertEqual(hashed.call_count, 1)
        record = self.fs._checksum_records[("/dup.txt", "backend2")]
        self.assertIsNone(record.checksum)
        entries = yaml.safe_load(self.fs.getxattr('/dup.txt', StorageFS.CHECKSUM_XATTR))
        self.assertIn(f"backend2:unverified:{record.timestamp}", entries)

    def test_hash_that_could_not_run_records_nothing_and_is_retried(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        member = self.backends["backend2"]
        with patch.object(member, "get_fast_checksum",
                          side_effect=ConnectionResetError("dropped")) as hashed:
            self.assertIn("dup.txt", self._list_and_settle())
            self.assertNotIn(("/dup.txt", "backend2"), self.fs._checksum_records)
            self._list_and_settle()
            self.assertEqual(hashed.call_count, 2)
        self._list_and_settle()
        self.assertIsNotNone(self.fs._checksum_records[("/dup.txt", "backend2")].checksum)

    def test_failed_replacement_hash_drops_the_old_record(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        self._list_and_settle()
        member = self.backends["backend2"]
        self.assertIn(("/dup.txt", "backend2"), self.fs._checksum_records)
        with patch.object(member, "get_fast_checksum", side_effect=ConnectionResetError("dropped")):
            self.fs._schedule_checksum_task(member, "/dup.txt", replace=True)
            self.assertTrue(self._wait_for(
                lambda: ("/dup.txt", "backend2") not in self.fs._checksum_records))

    def test_local_write_to_size_split_name_keeps_the_listing(self):
        (self.backend1_dir / "dup.txt").write_text("one")
        (self.backend2_dir / "dup.txt").write_text("three")
        self.fs.namespace.readdir_snapshot_ttl = float('inf')
        self.assertIn("dup.txt.backend2", set(self.fs.readdir('/', None)))
        # What mark_file_dirty does on every write: no member changed yet.
        self.fs.namespace.forget_listed_attrs('/dup.txt.backend2', removing=False)
        self.assertIn('/', self.fs.namespace.readdir_snapshot)

    def test_change_to_size_split_name_relists_its_directory(self):
        (self.backend1_dir / "dup.txt").write_text("one")
        (self.backend2_dir / "dup.txt").write_text("three")
        self.fs.namespace.readdir_snapshot_ttl = float('inf')  # as with watchers
        self.assertIn("dup.txt.backend2", set(self.fs.readdir('/', None)))
        (self.backend2_dir / "dup.txt").write_text("one")
        # A watcher event for the name (or a write to a copy) forgets it.
        self.fs._on_remote_change('/dup.txt')
        self.assertNotIn('/', self.fs.namespace.readdir_snapshot)
        self.assertIn("dup.txt", set(self.fs.readdir('/', None)))

    def test_failed_member_listing_keeps_size_split_and_hashes_nothing_there(self):
        (self.backend1_dir / "dup.txt").write_text("one")
        (self.backend2_dir / "dup.txt").write_text("three")
        (self.backend1_dir / "eq.txt").write_text("same")
        (self.backend2_dir / "eq.txt").write_text("same")
        self.assertIn("dup.txt.backend2", set(self.fs.readdir('/', None)))
        self.fs._forget_checksums({"/eq.txt"})
        with patch.object(self.fs, "_schedule_checksum_task") as schedule_task, \
                patch.object(self.backends["backend2"], "listdir_attrs",
                             side_effect=ConnectionResetError("dropped")):
            namespace = self.fs.namespace
            entries = set(namespace._list_members('/', namespace._begin_listing('/')))
            namespace._end_listing('/')
        self.assertIn("dup.txt.backend1", entries)
        self.assertIn("dup.txt.backend2", entries)
        self.assertNotIn("dup.txt", entries)
        # eq.txt is listed only by backend1 this round: nothing to compare.
        schedule_task.assert_not_called()

    def test_hash_dropped_from_full_queue_forgets_the_old_record(self):
        (self.backend2_dir / "a.txt").write_text("a")
        (self.backend2_dir / "b.txt").write_text("b")
        member = self.backends["backend2"]
        with self.fs._checksum_lock:
            for name in ("/a.txt", "/b.txt"):
                self.fs._checksum_records[(name, "backend2")] = ChecksumRecord(
                    checksum="old", timestamp=time.time())
        self.fs._checksum_queues.limit = 1
        started, release = threading.Event(), threading.Event()

        def hold():
            started.set()
            release.wait(5)

        blocker = self.fs._checksum_queues.submit("backend2", hold)
        self.assertTrue(started.wait(5))
        # A new basis (a listing's hash); a replacing hash is never dropped.
        self.fs._schedule_checksum_task(member, "/a.txt", (1, 1.0, None))
        self.fs._schedule_checksum_task(member, "/b.txt", (1, 1.0, None))  # drops /a.txt's
        self.assertNotIn(("/a.txt", "backend2"), self.fs._checksum_records)
        self.assertNotIn(("/a.txt", "backend2"), self.fs._checksum_tasks)
        release.set()
        blocker.result(timeout=5)
        self.assertTrue(self._wait_for(
            lambda: self.fs._checksum_records[("/b.txt", "backend2")].checksum != "old"))

    def test_size_split_xattr_carries_listed_sizes(self):
        (self.backend1_dir / "dup.txt").write_text("one")
        (self.backend2_dir / "dup.txt").write_text("three")
        # A listing that stays published (_list_and_settle's expires at once).
        self.assertIn("dup.txt.backend1", set(self.fs.readdir('/', None)))
        entries = yaml.safe_load(self.fs.getxattr('/dup.txt.backend1', StorageFS.CHECKSUM_XATTR))
        self.assertEqual(sorted(e.rsplit(":", 1)[0] for e in entries),
                         ["backend1:size=3", "backend2:size=5"])
        self.assertIn(StorageFS.CHECKSUM_XATTR, self.fs.listxattr('/dup.txt.backend2'))

    def test_one_hash_at_a_time_per_member(self):
        for i in range(6):
            for member_dir in (self.backend1_dir, self.backend2_dir):
                (member_dir / f"dup{i}.bin").write_bytes(b"x" * 64)
        lock = threading.Lock()
        running = {name: 0 for name in self.backends}
        peak = {name: 0 for name in self.backends}
        for name, backend in self.backends.items():
            real = backend.get_fast_checksum

            def tracked(path, _name=name, _real=real, **kwargs):
                with lock:
                    running[_name] += 1
                    peak[_name] = max(peak[_name], running[_name])
                time.sleep(0.02)
                try:
                    return _real(path, **kwargs)
                finally:
                    with lock:
                        running[_name] -= 1

            patcher = patch.object(backend, "get_fast_checksum", side_effect=tracked)
            patcher.start()
            self.addCleanup(patcher.stop)

        self._list_and_settle()
        self.assertEqual(peak, {name: 1 for name in self.backends})
        self.assertEqual(len(self.fs._checksum_records), 12)

    def test_release_of_conflict_copy_hashes_off_the_calling_thread(self):
        (self.backend1_dir / "dup.txt").write_text("one")
        (self.backend2_dir / "dup.txt").write_text("three")
        self.assertIn("dup.txt.backend2", self._list_and_settle())
        member = self.backends["backend2"]
        real = member.get_fast_checksum
        hash_threads = []

        def recorded(path, **kwargs):
            hash_threads.append(threading.get_ident())
            return real(path, **kwargs)

        with patch.object(member, "get_fast_checksum", side_effect=recorded):
            fh = self.fs.open('/dup.txt.backend2', os.O_WRONLY | os.O_TRUNC)
            self.fs.write('/dup.txt.backend2', b"fresh", 0, fh)
            self.fs.release('/dup.txt.backend2', fh)
            self.assertEqual((self.backend2_dir / "dup.txt").read_bytes(), b"fresh")
            # The split came from sizes the upload changed: relisted next.
            self.assertNotIn('/', self.fs.namespace.readdir_snapshot)
            self.assertTrue(self._wait_for(
                lambda: ("/dup.txt", "backend2") in self.fs._checksum_records))
        self.assertEqual(len(hash_threads), 1)
        self.assertNotEqual(hash_threads[0], threading.get_ident())
        self.assertEqual(self.fs._checksum_records[("/dup.txt", "backend2")].checksum,
                         fast_checksum_bytes(b"fresh"))

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

    def test_write_during_a_listing_does_not_discard_it(self):
        # Writes come at write rate; a listing must not be thrown away for
        # each one, or a directory with an active writer is never listed
        # for long enough to answer getattr. The written copy exists, so the
        # listing is not consulted for it.
        self.fs.sync_manager.stop()
        (self.backend1_dir / "appended.txt").write_bytes(self.OLD)
        fd = self.fs.open('/appended.txt', os.O_RDWR)
        namespace = self.fs.namespace
        real = namespace._schedule_directory_checksums

        def schedule_then_write(*args, **kwargs):
            real(*args, **kwargs)
            self.fs.write('/appended.txt', self.NEW, 0, fd)

        with patch.object(namespace, "_schedule_directory_checksums", side_effect=schedule_then_write):
            list(self.fs.readdir('/', None))
        self.fs.release('/appended.txt', fd)

        self.assertIn('/', namespace.entry_attrs)
        self.assertEqual(self.fs.getattr('/appended.txt')['st_size'], len(self.NEW))

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

    def _hashes(self):
        """Wrap every member's get_fast_checksum; return the list of
        (member, path) it is called with."""
        calls = []
        for name, backend in self.backends.items():
            real = backend.get_fast_checksum

            def counted(path, *args, _name=name, _real=real, **kwargs):
                calls.append((_name, path))
                return _real(path, *args, **kwargs)

            patcher = patch.object(backend, "get_fast_checksum", side_effect=counted)
            patcher.start()
            self.addCleanup(patcher.stop)
        return calls

    def _list_and_settle(self, path='/'):
        """List path past the snapshot and wait for its checksum tasks."""
        self.fs.namespace.readdir_snapshot_ttl = 0
        self.fs.namespace.listing_max_stale = 0  # relist at once, not in the background
        entries = set(self.fs.readdir(path, None))

        def settled():
            # A done callback drops its task and records its result under
            # the lock; reading outside it could land in between.
            with self.fs._checksum_lock:
                return not self.fs._checksum_tasks

        self.assertTrue(self._wait_for(settled))
        return entries

    def test_repeated_readdir_hashes_recorded_checksum_once(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        hashes = self._hashes()

        for _ in range(3):
            self.assertIn("dup.txt", self._list_and_settle())

        self.assertEqual(sorted(hashes), [("backend1", "/dup.txt"), ("backend2", "/dup.txt")])
        st = (self.backend2_dir / "dup.txt").stat()
        self.assertEqual(
            self.fs._checksum_records[("/dup.txt", "backend2")].basis,
            (4, st.st_mtime, st.st_ctime),
        )

    def _remount(self):
        """A fresh StorageFS on the same index, as after an unmount."""
        self.fs.namespace.shutdown()
        self.fs._checksum_queues.shutdown()
        self.fs.sync_manager.stop()
        self.fs.state_manager.close()
        self.fs = StorageFS(
            cache_root=self.cache_dir,
            state_db=self.db_path,
            backends=self.backends,
            cache_size=10 * 1024 * 1024,
            num_sync_workers=1,
        )

    def test_persisted_hashes_split_the_first_listing_after_a_remount(self):
        (self.backend1_dir / "dup.txt").write_text("aaaa")
        (self.backend2_dir / "dup.txt").write_text("bbbb")
        self._list_and_settle()
        self.assertIn("dup.txt.backend2", self._list_and_settle())
        self._remount()
        hashes = self._hashes()
        self.assertIn("dup.txt.backend2", self._list_and_settle())
        self.assertEqual(hashes, [])
        record = self.fs._checksum_records[("/dup.txt", "backend2")]
        self.assertEqual(record.checksum, fast_checksum_bytes(b"bbbb"))

    def test_persisted_hash_from_another_basis_is_not_used(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        self._list_and_settle()
        self._remount()
        target = self.backend2_dir / "dup.txt"
        before = target.stat()
        time.sleep(0.02)
        target.write_text("diff")
        os.utime(target, ns=(before.st_atime_ns, before.st_mtime_ns))  # only ctime moved
        hashes = self._hashes()
        self._list_and_settle()
        self.assertEqual(hashes, [("backend2", "/dup.txt")])
        self.assertIn("dup.txt.backend2", self._list_and_settle())
        row = self.fs.state_manager.get_member_checksum("/dup.txt", "backend2")
        self.assertEqual(row.checksum, fast_checksum_bytes(b"diff"))

    def test_forgotten_hashes_leave_the_index_too(self):
        (self.backend1_dir / "d").mkdir()
        (self.backend2_dir / "d").mkdir()
        for name in ("d/dup.txt", "dup.txt"):
            (self.backend1_dir / name).write_text("same")
            (self.backend2_dir / name).write_text("same")
        self._list_and_settle('/d')
        self._list_and_settle('/')
        state = self.fs.state_manager
        self.assertIsNotNone(state.get_member_checksum("/dup.txt", "backend1"))
        # A watcher event: the member changed, even if its stat did not.
        self.fs._on_remote_change('/dup.txt')
        self.assertIsNone(state.get_member_checksum("/dup.txt", "backend1"))
        self.assertIsNone(state.get_member_checksum("/dup.txt", "backend2"))
        self.fs._forget_checksums({"/d"}, subtree=True)
        self.assertIsNone(state.get_member_checksum("/d/dup.txt", "backend2"))

    def test_unverified_and_failed_hashes_are_not_persisted(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        self._list_and_settle()
        state = self.fs.state_manager
        self.assertIsNotNone(state.get_member_checksum("/dup.txt", "backend2"))
        member = self.backends["backend2"]
        with patch.object(member, "get_fast_checksum",
                          side_effect=ChecksumUnavailable(errno.EIO, "no remote hash")):
            self.fs._schedule_checksum_task(member, "/dup.txt", replace=True)
            self.assertTrue(self._wait_for(
                lambda: self.fs._checksum_records[("/dup.txt", "backend2")].checksum is None))
        self.assertIsNone(state.get_member_checksum("/dup.txt", "backend2"))
        # Without a basis a digest is kept in memory only.
        self.fs._schedule_checksum_task(member, "/dup.txt", replace=True)
        self.assertTrue(self._wait_for(
            lambda: self.fs._checksum_records[("/dup.txt", "backend2")].checksum is not None))
        self.assertIsNone(state.get_member_checksum("/dup.txt", "backend2"))
        st = (self.backend2_dir / "dup.txt").stat()
        basis = (st.st_size, st.st_mtime, st.st_ctime)
        self.fs._schedule_checksum_task(member, "/dup.txt", basis, replace=True)
        self.assertTrue(self._wait_for(
            lambda: state.get_member_checksum("/dup.txt", "backend2") is not None))
        with patch.object(member, "get_fast_checksum", side_effect=ConnectionResetError("dropped")):
            self.fs._schedule_checksum_task(member, "/dup.txt", basis, replace=True)
            self.assertTrue(self._wait_for(
                lambda: state.get_member_checksum("/dup.txt", "backend2") is None))

    def _upload(self, name, content):
        """What a sync of name does to backend2: copy, then the callback."""
        cache_path = self.temp_dir / "upload-src"
        cache_path.write_bytes(content)
        member = self.backends["backend2"]
        member.copy_from_local(cache_path, name)
        self.fs._record_member_written(name, member, cache_path, fast_checksum_bytes(content))

    def test_synced_upload_is_recorded_with_the_members_basis(self):
        (self.backend1_dir / "dup.txt").write_text("aaaa")
        self._upload("/dup.txt", b"bbbb")
        st = (self.backend2_dir / "dup.txt").stat()
        row = self.fs.state_manager.get_member_checksum("/dup.txt", "backend2")
        self.assertEqual(row.checksum, fast_checksum_bytes(b"bbbb"))
        self.assertEqual(row.basis, (4, st.st_mtime, st.st_ctime))
        # Not tracked by the view yet: persisted, not held in memory.
        self.assertNotIn(("/dup.txt", "backend2"), self.fs._checksum_records)
        hashes = self._hashes()
        self._list_and_settle()  # backend1's hash lands after this listing
        self.assertIn("dup.txt.backend2", self._list_and_settle())
        self.assertEqual(hashes, [("backend1", "/dup.txt")])

    def test_synced_upload_replaces_a_tracked_record_and_its_hash(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        self.assertIn("dup.txt", self._list_and_settle())
        target = self.backend2_dir / "dup.txt"
        before = target.stat()
        # A same-size rewrite that keeps the mtime: on a member without
        # ctime, no listing could tell. The upload's own record does.
        self._upload("/dup.txt", b"diff")
        os.utime(target, ns=(before.st_atime_ns, before.st_mtime_ns))
        st = target.stat()
        self.fs._record_member_written(
            "/dup.txt", self.backends["backend2"], self.temp_dir / "upload-src",
            fast_checksum_bytes(b"diff"))
        record = self.fs._checksum_records[("/dup.txt", "backend2")]
        self.assertEqual(record.checksum, fast_checksum_bytes(b"diff"))
        self.assertEqual(record.basis, (4, st.st_mtime, st.st_ctime))
        hashes = self._hashes()
        self.assertIn("dup.txt.backend2", self._list_and_settle())
        self.assertEqual(hashes, [])

    def test_upload_of_unknown_content_drops_the_record(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        self._list_and_settle()
        cache_path = self.temp_dir / "upload-src"
        cache_path.write_bytes(b"same")
        self.fs._record_member_written("/dup.txt", self.backends["backend2"], cache_path, None)
        self.assertNotIn(("/dup.txt", "backend2"), self.fs._checksum_records)
        self.assertIsNone(self.fs.state_manager.get_member_checksum("/dup.txt", "backend2"))
        # A member stat that fails leaves nothing either.
        self._upload("/dup.txt", b"same")
        self.assertIsNotNone(self.fs.state_manager.get_member_checksum("/dup.txt", "backend2"))
        with patch.object(self.backends["backend2"], "stat", side_effect=OSError("gone")):
            self.fs._record_member_written(
                "/dup.txt", self.backends["backend2"], cache_path, fast_checksum_bytes(b"same"))
        self.assertIsNone(self.fs.state_manager.get_member_checksum("/dup.txt", "backend2"))

    def _age_rows(self, seconds):
        """Make every persisted row look checked `seconds` ago."""
        state = self.fs.state_manager
        with state._checksum_db_lock:
            state._checksum_conn.execute(
                "UPDATE member_checksums SET verified_at = verified_at - ?", (seconds,))
            state._checksum_conn.commit()

    def _reverify_and_settle(self):
        self.fs._reverify_tick()

        def settled():
            with self.fs._checksum_lock:
                return not self.fs._checksum_tasks
        self.assertTrue(self._wait_for(settled))

    def test_reverify_finds_a_rewrite_the_basis_hides(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        self.assertIn("dup.txt", self._list_and_settle())
        self._age_rows(2 * StorageFS.CHECKSUM_REVERIFY_MIN_AGE)
        old_basis = self.fs._checksum_records[("/dup.txt", "backend2")].basis
        (self.backend2_dir / "dup.txt").write_text("diff")
        # What a member without ctime reports after a same-second rewrite:
        # the basis the record was made from.
        real_stat = self.backends["backend2"].stat

        def same_basis(path):
            st = real_stat(path)
            return os.stat_result((st.st_mode, st.st_ino, st.st_dev, st.st_nlink, st.st_uid,
                                   st.st_gid, old_basis[0], st.st_atime, old_basis[1],
                                   old_basis[2]))
        hashes = self._hashes()
        with patch.object(self.backends["backend2"], "stat", side_effect=same_basis):
            self._reverify_and_settle()
        # One file per member per pass: backend1's row and backend2's.
        self.assertEqual(sorted(hashes), [("backend1", "/dup.txt"), ("backend2", "/dup.txt")])
        record = self.fs._checksum_records[("/dup.txt", "backend2")]
        self.assertEqual(record.checksum, fast_checksum_bytes(b"diff"))
        self.assertEqual(record.basis, old_basis)
        self.assertEqual(self.fs.state_manager.get_member_checksum("/dup.txt", "backend2").checksum,
                         fast_checksum_bytes(b"diff"))
        self.fs.namespace.readdir_snapshot_ttl = float('inf')
        self.assertIn("dup.txt.backend2", set(self.fs.readdir('/', None)))

    def test_reverify_takes_only_old_rows_and_moves_them_back(self):
        for name in ("a.txt", "b.txt"):
            (self.backend1_dir / name).write_text("same")
            (self.backend2_dir / name).write_text("same")
        self._list_and_settle()
        hashes = self._hashes()
        self._reverify_and_settle()
        self.assertEqual(hashes, [])  # checked within the hour
        self._age_rows(2 * StorageFS.CHECKSUM_REVERIFY_MIN_AGE)
        self._reverify_and_settle()
        self._reverify_and_settle()
        # Each pass took the member's oldest row; the second, the other one.
        self.assertEqual(sorted(p for m, p in hashes if m == "backend2"), ["/a.txt", "/b.txt"])
        self._reverify_and_settle()
        self.assertEqual(len(hashes), 4)  # both rows checked again: nothing old is left

    def test_reverify_skips_a_busy_member_and_drops_vanished_files(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        self._list_and_settle()
        self._age_rows(2 * StorageFS.CHECKSUM_REVERIFY_MIN_AGE)
        hashes = self._hashes()
        with patch.object(self.fs._checksum_queues, "idle", return_value=False):
            self._reverify_and_settle()
        self.assertEqual(hashes, [])
        (self.backend2_dir / "dup.txt").unlink()
        self._reverify_and_settle()
        self.assertIsNone(self.fs.state_manager.get_member_checksum("/dup.txt", "backend2"))
        self.assertNotIn(("/dup.txt", "backend2"), self.fs._checksum_records)
        self.assertIsNotNone(self.fs.state_manager.get_member_checksum("/dup.txt", "backend1"))

    def test_reverify_that_cannot_settle_skips_the_row_unchanged(self):
        for name in ("dup.txt", "next.txt"):
            (self.backend1_dir / name).write_text("same")
            (self.backend2_dir / name).write_text("same")
        self._list_and_settle()
        self._age_rows(2 * StorageFS.CHECKSUM_REVERIFY_MIN_AGE)
        state = self.fs.state_manager
        member = self.backends["backend2"]
        first = state.oldest_member_checksum("backend2", time.time())
        hashes = []
        real = member.get_fast_checksum

        def no_digest(path, *args, **kwargs):
            hashes.append(path)
            raise ChecksumUnavailable(errno.EIO, "no remote hash")
        with patch.object(member, "get_fast_checksum", side_effect=no_digest):
            self._reverify_and_settle()
        # Not marked checked: no digest verifies nothing.
        self.assertEqual(state.get_member_checksum(first[0], "backend2"), first[1])
        self.assertEqual(state.oldest_member_checksum("backend2", time.time())[2], first[2])
        # But skipped: the next pass takes the other name.
        with patch.object(member, "get_fast_checksum", side_effect=real) as hashed:
            self._reverify_and_settle()
        self.assertEqual(hashed.call_args.args[0], [p for p in ("/dup.txt", "/next.txt")
                                                   if p != first[0]][0])
        self.assertEqual(hashes, [first[0]])

    def test_reverify_of_a_file_changing_while_hashed_skips_it(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        self._list_and_settle()
        self._age_rows(2 * StorageFS.CHECKSUM_REVERIFY_MIN_AGE)
        member = self.backends["backend2"]
        real = member.get_fast_checksum
        target = self.backend2_dir / "dup.txt"

        def grow(path, *args, **kwargs):
            result = real(path, *args, **kwargs)
            with open(target, "a") as fh:
                fh.write("more")
            return result
        with patch.object(member, "get_fast_checksum", side_effect=grow):
            self._reverify_and_settle()
        row = self.fs.state_manager.get_member_checksum("/dup.txt", "backend2")
        self.assertEqual(row.checksum, fast_checksum_bytes(b"same"))
        hashes = self._hashes()
        self._reverify_and_settle()
        self.assertNotIn(("backend2", "/dup.txt"), hashes)

    def test_reverify_of_a_row_from_another_basis_drops_it_unhashed(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        self._list_and_settle()
        self._age_rows(2 * StorageFS.CHECKSUM_REVERIFY_MIN_AGE)
        time.sleep(0.02)
        (self.backend2_dir / "dup.txt").write_text("other")
        hashes = self._hashes()
        self._reverify_and_settle()
        self.assertNotIn(("backend2", "/dup.txt"), hashes)
        self.assertIsNone(self.fs.state_manager.get_member_checksum("/dup.txt", "backend2"))
        self.assertNotIn(("/dup.txt", "backend2"), self.fs._checksum_records)

    def test_row_a_failed_delete_left_is_never_loaded(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        self._list_and_settle()
        state = self.fs.state_manager
        with patch.object(state, "delete_member_checksums", side_effect=sqlite3.OperationalError("locked")):
            self.fs._on_remote_change('/dup.txt')
        self.assertIsNotNone(state.get_member_checksum("/dup.txt", "backend2"))
        hashes = self._hashes()
        self._list_and_settle()
        self.assertEqual(sorted(hashes), [("backend1", "/dup.txt"), ("backend2", "/dup.txt")])

    def test_failed_persist_keeps_the_record_and_refuses_the_old_row(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        self._list_and_settle()
        state = self.fs.state_manager
        with patch.object(state, "put_member_checksum", side_effect=sqlite3.ProgrammingError("closed")):
            self._upload("/dup.txt", b"diff")
        self.assertEqual(self.fs._checksum_records[("/dup.txt", "backend2")].checksum,
                         fast_checksum_bytes(b"diff"))
        with self.fs._checksum_lock:
            self.assertTrue(self.fs._persisted_row_refused(("/dup.txt", "backend2")))
            self.assertFalse(self.fs._persisted_row_refused(("/dup.txt", "backend1")))
        # A later successful write lifts it.
        self._upload("/dup.txt", b"diff")
        with self.fs._checksum_lock:
            self.assertFalse(self.fs._persisted_row_refused(("/dup.txt", "backend2")))

    def test_refused_rows_are_deleted_once_the_index_is_back(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        (self.backend1_dir / "other.txt").write_text("same")
        (self.backend2_dir / "other.txt").write_text("same")
        self._list_and_settle()
        state = self.fs.state_manager
        with patch.object(state, "delete_member_checksums", side_effect=sqlite3.OperationalError("locked")):
            self.fs._on_remote_change('/dup.txt')
        self.assertIsNotNone(state.get_member_checksum("/dup.txt", "backend2"))
        # The next successful write retries the delete, so the next mount
        # does not load the row either.
        self._upload("/other.txt", b"same")
        self.assertIsNone(state.get_member_checksum("/dup.txt", "backend2"))
        self.assertEqual(self.fs._persisted_refused, set())

    def test_reverify_of_a_row_replaced_since_it_was_read_does_nothing(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        self._list_and_settle()
        member = self.backends["backend2"]
        stale = self.fs.state_manager.get_member_checksum("/dup.txt", "backend2")
        self._upload("/dup.txt", b"diff")
        hashes = self._hashes()
        self.fs._schedule_reverify(member, "/dup.txt", stale)
        self.assertNotIn(("/dup.txt", "backend2"), self.fs._checksum_tasks)
        self.assertEqual(hashes, [])
        self.assertEqual(self.fs.state_manager.get_member_checksum("/dup.txt", "backend2").checksum,
                         fast_checksum_bytes(b"diff"))

    def test_verify_results_keep_pending_directories(self):
        self.fs.VERIFY_RESULTS_KEPT = 2
        for d in ("a", "b", "c"):
            (self.backend1_dir / d).mkdir()
            (self.backend2_dir / d).mkdir()
            (self.backend1_dir / d / "dup.txt").write_text("same")
            (self.backend2_dir / d / "dup.txt").write_text("same")
        member = self.backends["backend1"]
        started, release = threading.Event(), threading.Event()
        real = member.get_fast_checksum

        def hold_a(path, *args, **kwargs):
            if path == "/a/dup.txt":
                started.set()
                release.wait(5)
            return real(path, *args, **kwargs)
        with patch.object(member, "get_fast_checksum", side_effect=hold_a):
            self.fs.setxattr('/a', StorageFS.VERIFY_XATTR, b"1", 0)
            self.assertTrue(started.wait(5))
            self.fs.setxattr('/b', StorageFS.VERIFY_XATTR, b"1", 0)
            self.fs.setxattr('/c', StorageFS.VERIFY_XATTR, b"1", 0)
            # /a is still pending: /b, settled or not, would go first.
            self.assertEqual(json.loads(self.fs.getxattr('/a', StorageFS.VERIFY_XATTR))["known"], 1)
            release.set()
            self.assertTrue(self._wait_for(lambda: not self.fs._checksum_tasks))
        self.assertEqual(json.loads(self.fs.getxattr('/a', StorageFS.VERIFY_XATTR)),
                         {"known": 1, "pending": 0, "verified": 1, "unverified": 0})

    def test_refused_retry_runs_before_a_write_under_it(self):
        (self.backend1_dir / "d").mkdir()
        (self.backend2_dir / "d").mkdir()
        (self.backend1_dir / "d" / "dup.txt").write_text("same")
        (self.backend2_dir / "d" / "dup.txt").write_text("same")
        self._list_and_settle('/d')
        state = self.fs.state_manager
        with patch.object(state, "delete_member_checksums", side_effect=sqlite3.OperationalError("locked")):
            self.fs._forget_checksums({"/d"}, subtree=True)
        self._upload("/d/dup.txt", b"same")
        self.assertIsNotNone(state.get_member_checksum("/d/dup.txt", "backend2"))
        self.assertEqual(self.fs._persisted_refused, set())

    def test_write_under_a_refusal_the_retry_cannot_lift_is_skipped(self):
        (self.backend1_dir / "d").mkdir()
        (self.backend2_dir / "d").mkdir()
        (self.backend1_dir / "d" / "dup.txt").write_text("same")
        (self.backend2_dir / "d" / "dup.txt").write_text("same")
        self._list_and_settle('/d')
        state = self.fs.state_manager
        with patch.object(state, "delete_member_checksums", side_effect=sqlite3.OperationalError("locked")):
            self.fs._forget_checksums({"/d"}, subtree=True)
            with patch.object(state, "put_member_checksum") as put:
                self._upload("/d/dup.txt", b"diff")
            put.assert_not_called()
        # In memory the upload's record stands; the old row stays refused.
        self.assertEqual(self.fs._checksum_records.get(("/d/dup.txt", "backend2"), None) is None
                         or self.fs._checksum_records[("/d/dup.txt", "backend2")].checksum
                         == fast_checksum_bytes(b"diff"), True)
        # Once deletes work again, a successful delete clears the backlog.
        self.fs._forget_checksums({"/elsewhere"})
        self.assertEqual(self.fs._persisted_refused, set())
        self.assertIsNone(state.get_member_checksum("/d/dup.txt", "backend2"))

    def test_settled_verify_results_are_dropped_first(self):
        self.fs.VERIFY_RESULTS_KEPT = 2
        for d in ("a", "b", "c"):
            (self.backend1_dir / d).mkdir()
            (self.backend2_dir / d).mkdir()
            (self.backend1_dir / d / "dup.txt").write_text("same")
            (self.backend2_dir / d / "dup.txt").write_text("same")
        for d in ("/a", "/b", "/c"):
            self.fs.setxattr(d, StorageFS.VERIFY_XATTR, b"1", 0)
            self.assertTrue(self._wait_for(lambda: not self.fs._checksum_tasks))
        known = {d: json.loads(self.fs.getxattr(d, StorageFS.VERIFY_XATTR))["known"]
                 for d in ("/a", "/b", "/c")}
        self.assertEqual(known, {"/a": 0, "/b": 1, "/c": 1})

    def test_reverify_thread_runs_from_init_and_stops_at_destroy(self):
        self.fs._checksum_reverify_interval = 0.05
        ticks = threading.Event()
        with patch.object(self.fs, "_reverify_tick", side_effect=lambda: ticks.set()):
            self.fs.init('/')
            self.assertTrue(ticks.wait(5))
            thread = self.fs._reverify_thread
            self.fs.destroy('/')
        self.assertFalse(thread.is_alive())

    def test_verify_of_a_file_is_refused(self):
        (self.backend1_dir / "f.txt").write_text("x")
        with self.assertRaises(FuseOSError) as ctx:
            self.fs.setxattr('/f.txt', StorageFS.VERIFY_XATTR, b"1", 0)
        self.assertEqual(ctx.exception.errno, errno.ENOTDIR)

    def test_reverify_superseded_by_an_upload_records_nothing(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        self._list_and_settle()
        self._age_rows(2 * StorageFS.CHECKSUM_REVERIFY_MIN_AGE)
        member = self.backends["backend2"]
        started, release = threading.Event(), threading.Event()

        def slow(path, *args, **kwargs):
            started.set()
            release.wait(5)
            return "0" * 32

        with patch.object(member, "get_fast_checksum", side_effect=slow):
            self.fs._reverify_tick()
            self.assertTrue(started.wait(5))
            self._upload("/dup.txt", b"newer")
            release.set()
            self.assertTrue(self._wait_for(lambda: not self.fs._checksum_tasks))
        row = self.fs.state_manager.get_member_checksum("/dup.txt", "backend2")
        self.assertEqual(row.checksum, fast_checksum_bytes(b"newer"))

    def test_forced_verify_rehashes_and_counts(self):
        zero = {"known": 0, "pending": 0, "verified": 0, "unverified": 0}
        self.assertEqual(json.loads(self.fs.getxattr('/', StorageFS.VERIFY_XATTR)), zero)
        (self.backend1_dir / "ok.txt").write_text("same")
        (self.backend2_dir / "ok.txt").write_text("same")
        (self.backend1_dir / "nohash.txt").write_text("same")
        (self.backend2_dir / "nohash.txt").write_text("same")
        (self.backend1_dir / "split.txt").write_text("one")
        (self.backend2_dir / "split.txt").write_text("three")
        (self.backend1_dir / "only1.txt").write_text("x")
        member = self.backends["backend2"]
        real = member.get_fast_checksum

        def no_hash_for_one(path, *args, **kwargs):
            if path == "/nohash.txt":
                raise ChecksumUnavailable(errno.EIO, "no remote hash")
            return real(path, *args, **kwargs)
        with patch.object(member, "get_fast_checksum", side_effect=no_hash_for_one):
            self._list_and_settle()
            hashes = self._hashes()
            # The listing expires at once here (ttl 0): verify lists itself.
            self.fs.setxattr('/', StorageFS.VERIFY_XATTR, b"1", 0)
            self.assertTrue(self._wait_for(lambda: not self.fs._checksum_tasks))
        # Both names on both members hashed again; the size split was not.
        self.assertEqual(sorted(hashes), [
            ("backend1", "/nohash.txt"), ("backend1", "/ok.txt"),
            ("backend2", "/nohash.txt"), ("backend2", "/ok.txt"),
        ])
        self.assertEqual(json.loads(self.fs.getxattr('/', StorageFS.VERIFY_XATTR)),
                         {"known": 1, "pending": 0, "verified": 1, "unverified": 1})
        self.assertNotIn(StorageFS.VERIFY_XATTR, self.fs.listxattr('/'))

    def test_readdir_rehashes_member_rewritten_with_same_size_and_mtime(self):
        # A tagger that preserves mtime: only ctime shows the change, and a
        # local member's listing carries it.
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        hashes = self._hashes()
        self._list_and_settle()
        hashes.clear()

        target = self.backend2_dir / "dup.txt"
        before = target.stat()
        time.sleep(0.02)
        target.write_text("diff")
        os.utime(target, ns=(before.st_atime_ns, before.st_mtime_ns))
        self.assertNotEqual(target.stat().st_ctime, before.st_ctime)
        self._list_and_settle()

        self.assertEqual(hashes, [("backend2", "/dup.txt")])
        self.assertIn("dup.txt.backend2", self._list_and_settle())

    def test_hashing_writes_nothing_the_watcher_would_see(self):
        # Watched members report xattr writes as ATTRIB, and the watcher's
        # invalidation drops the checksum records. A hash that wrote to the
        # member would undo itself, and every listing would hash again.
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        hashes = self._hashes()
        for name, backend in self.backends.items():
            for method in ("set_xattr", "write_file", "copy_from_local", "truncate_file"):
                if hasattr(backend, method):
                    patcher = patch.object(
                        backend, method,
                        side_effect=lambda path, *a, **kw: self.fs._on_remote_change(path),
                    )
                    patcher.start()
                    self.addCleanup(patcher.stop)

        self._list_and_settle()
        self._list_and_settle()

        self.assertEqual(sorted(hashes), [("backend1", "/dup.txt"), ("backend2", "/dup.txt")])

    def test_member_whose_size_changed_splits_without_a_hash(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        hashes = self._hashes()
        self._list_and_settle()
        hashes.clear()

        (self.backend2_dir / "dup.txt").write_text("different")
        # The listing that sees the new size splits the name itself.
        self.assertIn("dup.txt.backend2", self._list_and_settle())
        self.assertIn("dup.txt.backend2", self._list_and_settle())
        self.assertEqual(hashes, [])

    def test_readdir_rehashes_member_whose_mtime_changed(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        hashes = self._hashes()
        self._list_and_settle()
        hashes.clear()

        # Same size, different bytes: only the mtime shows the change.
        target = self.backend2_dir / "dup.txt"
        before = target.stat().st_mtime
        target.write_text("diff")
        os.utime(target, (before + 10, before + 10))
        self._list_and_settle()

        self.assertEqual(hashes, [("backend2", "/dup.txt")])
        entries = self._list_and_settle()
        self.assertIn("dup.txt.backend1", entries)
        self.assertIn("dup.txt.backend2", entries)

    def test_watcher_invalidation_of_uncached_file_rehashes_it(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        hashes = self._hashes()
        self._list_and_settle()
        hashes.clear()

        self.assertFalse((self.cache_dir / "dup.txt").exists())
        self.fs._on_remote_change('/dup.txt')
        self.assertNotIn(("/dup.txt", "backend1"), self.fs._checksum_records)
        self._list_and_settle()

        self.assertEqual(sorted(hashes), [("backend1", "/dup.txt"), ("backend2", "/dup.txt")])

    def test_invalidation_while_hashing_drops_the_result(self):
        (self.backend1_dir / "dup.txt").write_text("same")
        (self.backend2_dir / "dup.txt").write_text("same")
        started, release = threading.Event(), threading.Event()
        member = self.backends["backend2"]
        real = member.get_fast_checksum

        def slow(path, **kwargs):
            started.set()
            release.wait(5)
            return real(path, **kwargs)

        self.fs.namespace.readdir_snapshot_ttl = 0

        self.fs.namespace.listing_max_stale = 0  # relist at once, not in the background
        with patch.object(member, "get_fast_checksum", side_effect=slow):
            list(self.fs.readdir('/', None))
            self.assertTrue(started.wait(5))
            running = self.fs._checksum_tasks[("/dup.txt", "backend2")]
            self.fs._invalidate_file_cache('/dup.txt')
            # Callbacks run in the order added, so this one runs after the
            # fs's own has decided whether to record.
            callbacks_done = threading.Event()
            running.add_done_callback(lambda _: callbacks_done.set())
            release.set()
            self.assertTrue(callbacks_done.wait(5))
            self.assertIsNotNone(running.result())

        self.assertNotIn(("/dup.txt", "backend2"), self.fs._checksum_records)

    def test_rename_and_unlink_forget_recorded_checksums(self):
        (self.backend1_dir / "sub").mkdir()
        (self.backend2_dir / "sub").mkdir()
        for member_dir in (self.backend1_dir, self.backend2_dir):
            (member_dir / "sub" / "dup.txt").write_text("same")
            (member_dir / "gone.txt").write_text("same")
        (self.cache_dir / "sub").mkdir()
        self._list_and_settle('/sub')
        self._list_and_settle('/')
        self.assertIn(("/sub/dup.txt", "backend1"), self.fs._checksum_records)
        self.assertIn(("/gone.txt", "backend1"), self.fs._checksum_records)

        self.fs.rename('/sub', '/moved')
        (self.cache_dir / "gone.txt").write_text("same")
        self.fs.unlink('/gone.txt')

        self.assertEqual(
            {k for k in self.fs._checksum_records if k[0] in ("/sub/dup.txt", "/gone.txt")},
            set(),
        )

    def test_readdir_rehashes_symlinks_every_listing(self):
        # A symlink's listed attributes are the link's, which do not change
        # when its target does, so they cannot vouch for a recorded checksum.
        for member_dir in (self.backend1_dir, self.backend2_dir):
            (member_dir / "target.txt").write_text("same")
            (member_dir / "link.txt").symlink_to("target.txt")
        hashes = self._hashes()

        self._list_and_settle()
        self._list_and_settle()

        self.assertEqual(sum(1 for h in hashes if h[1] == "/link.txt"), 4)
        self.assertEqual(sum(1 for h in hashes if h[1] == "/target.txt"), 2)

    def test_conflict_view_survives_skipped_rehash_and_clears_on_change(self):
        (self.backend1_dir / "dup.txt").write_text("one")
        (self.backend2_dir / "dup.txt").write_text("two!")
        hashes = self._hashes()

        self._list_and_settle()
        for _ in range(2):
            entries = self._list_and_settle()
            self.assertIn("dup.txt.backend1", entries)
            self.assertIn("dup.txt.backend2", entries)
            self.assertNotIn("dup.txt", entries)
        # Different sizes: split from the listing, never hashed.
        self.assertEqual(hashes, [])

        (self.backend2_dir / "dup.txt").write_text("one")
        self._list_and_settle()
        entries = self._list_and_settle()
        self.assertIn("dup.txt", entries)
        self.assertNotIn("dup.txt.backend2", entries)
        # Now the same size: both copies hashed once, and they match.
        self.assertEqual(sorted(hashes), [("backend1", "/dup.txt"), ("backend2", "/dup.txt")])

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
