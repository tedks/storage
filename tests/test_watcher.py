"""
Tests for the remote filesystem watcher (inotify-based cache invalidation).
"""

import os
import tempfile
import threading
import time
from pathlib import Path
from unittest import TestCase, mock

from storage.watcher import RemoteWatcher


class TestRemoteWatcherParsing(TestCase):
    """Test inotifywait output parsing without actually launching SSH."""

    def _make_watcher(self, on_change=None):
        return RemoteWatcher(
            backend_name="test-backend",
            ssh_host="testhost",
            ssh_port=22,
            remote_path="/data/storage",
            on_change=on_change or (lambda p: None),
            lock_dir=Path(tempfile.mkdtemp()),
        )

    def test_parse_create_event(self):
        w = self._make_watcher()
        vpath = w._parse_event_line("CREATE /data/storage/subdir/file.txt")
        self.assertEqual(vpath, "/subdir/file.txt")

    def test_parse_root_event(self):
        w = self._make_watcher()
        vpath = w._parse_event_line("MODIFY /data/storage")
        self.assertEqual(vpath, "/")

    def test_parse_nested_path(self):
        w = self._make_watcher()
        vpath = w._parse_event_line("CLOSE_WRITE /data/storage/a/b/c/d.bin")
        self.assertEqual(vpath, "/a/b/c/d.bin")

    def test_parse_unrelated_path_returns_none(self):
        w = self._make_watcher()
        vpath = w._parse_event_line("CREATE /other/path/file.txt")
        self.assertIsNone(vpath)

    def test_parse_empty_line(self):
        w = self._make_watcher()
        self.assertIsNone(w._parse_event_line(""))

    def test_event_to_dir(self):
        w = self._make_watcher()
        self.assertEqual(w._event_to_dir("/subdir/file.txt"), "/subdir")
        self.assertEqual(w._event_to_dir("/file.txt"), "/")
        self.assertEqual(w._event_to_dir("/"), "/")


class TestRemoteWatcherLocking(TestCase):
    """Test PID-file deduplication."""

    def test_first_lock_succeeds(self):
        lock_dir = Path(tempfile.mkdtemp())
        w = RemoteWatcher(
            backend_name="lock-test",
            ssh_host="h", ssh_port=22, remote_path="/data",
            on_change=lambda p: None, lock_dir=lock_dir,
        )
        self.assertTrue(w._try_acquire_lock())
        # Lock file should exist with our PID.
        lock_path = w._lock_path()
        self.assertTrue(lock_path.exists())
        with open(lock_path) as f:
            self.assertEqual(f.read().strip(), str(os.getpid()))
        w._release_lock()

    def test_second_lock_fails(self):
        lock_dir = Path(tempfile.mkdtemp())

        w1 = RemoteWatcher(
            backend_name="lock-test2",
            ssh_host="h", ssh_port=22, remote_path="/data",
            on_change=lambda p: None, lock_dir=lock_dir,
        )
        w2 = RemoteWatcher(
            backend_name="lock-test2",
            ssh_host="h", ssh_port=22, remote_path="/data",
            on_change=lambda p: None, lock_dir=lock_dir,
        )
        self.assertTrue(w1._try_acquire_lock())
        self.assertFalse(w2._try_acquire_lock())
        w1._release_lock()

    def test_lock_released_on_release(self):
        lock_dir = Path(tempfile.mkdtemp())

        w1 = RemoteWatcher(
            backend_name="lock-test3",
            ssh_host="h", ssh_port=22, remote_path="/data",
            on_change=lambda p: None, lock_dir=lock_dir,
        )
        w2 = RemoteWatcher(
            backend_name="lock-test3",
            ssh_host="h", ssh_port=22, remote_path="/data",
            on_change=lambda p: None, lock_dir=lock_dir,
        )
        self.assertTrue(w1._try_acquire_lock())
        w1._release_lock()
        # Now w2 can acquire.
        self.assertTrue(w2._try_acquire_lock())
        w2._release_lock()


class TestRemoteWatcherSSHCommand(TestCase):
    """Test SSH command building."""

    def test_basic_command(self):
        w = RemoteWatcher(
            backend_name="cmd-test",
            ssh_host="example.com",
            ssh_port=22,
            remote_path="/srv/storage",
            on_change=lambda p: None,
            username="user",
        )
        cmd = w._build_ssh_command()
        self.assertIn("ssh", cmd[0])
        self.assertIn("-p", cmd)
        self.assertIn("22", cmd)
        self.assertIn("user@example.com", cmd)
        # inotifywait command should be the last arg
        self.assertIn("inotifywait", cmd[-1])
        self.assertIn("/srv/storage", cmd[-1])

    def test_custom_port_and_key(self):
        w = RemoteWatcher(
            backend_name="cmd-test2",
            ssh_host="host",
            ssh_port=2222,
            remote_path="/data",
            on_change=lambda p: None,
            key_filename="/home/user/.ssh/id_rsa",
        )
        cmd = w._build_ssh_command()
        self.assertIn("2222", cmd)
        self.assertIn("-i", cmd)
        self.assertIn("/home/user/.ssh/id_rsa", cmd)


class TestConfigurableTTL(TestCase):
    """Test that readdir TTL is configurable."""

    def test_default_ttl(self):
        from storage.fs import StorageFS
        with tempfile.TemporaryDirectory() as tmp:
            cache = Path(tmp) / "cache"
            cache.mkdir()
            db = Path(tmp) / "state.db"
            fs = StorageFS(
                cache_root=cache, state_db=db, backends={},
                cache_size=1024 * 1024, num_sync_workers=0, start_sync=False,
            )
            self.assertEqual(fs._readdir_snapshot_ttl, 1.0)
            fs.state_manager.close()

    def test_custom_ttl(self):
        from storage.fs import StorageFS
        with tempfile.TemporaryDirectory() as tmp:
            cache = Path(tmp) / "cache"
            cache.mkdir()
            db = Path(tmp) / "state.db"
            fs = StorageFS(
                cache_root=cache, state_db=db, backends={},
                cache_size=1024 * 1024, num_sync_workers=0, start_sync=False,
                readdir_ttl=30.0,
            )
            self.assertEqual(fs._readdir_snapshot_ttl, 30.0)
            fs.state_manager.close()

    def test_zero_ttl(self):
        from storage.fs import StorageFS
        with tempfile.TemporaryDirectory() as tmp:
            cache = Path(tmp) / "cache"
            cache.mkdir()
            db = Path(tmp) / "state.db"
            fs = StorageFS(
                cache_root=cache, state_db=db, backends={},
                cache_size=1024 * 1024, num_sync_workers=0, start_sync=False,
                readdir_ttl=0,
            )
            self.assertEqual(fs._readdir_snapshot_ttl, 0.0)
            fs.state_manager.close()
