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
