"""
Tests for the remote filesystem watcher (inotify-based cache invalidation).
"""

import tempfile
from pathlib import Path
from unittest import TestCase

from storage.watcher import RemoteWatcher


class TestRemoteWatcherParsing(TestCase):
    """Test inotifywait output parsing without actually launching SSH."""

    def _make_watcher(self, on_change=None, remote_path="/data/storage"):
        return RemoteWatcher(
            backend_name="test-backend",
            ssh_host="testhost",
            ssh_port=22,
            remote_path=remote_path,
            on_change=on_change or (lambda p: None),
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

    def test_root_remote_path_normalizes(self):
        """A backend rooted at / should normalize correctly."""
        w = self._make_watcher(remote_path="/")
        self.assertEqual(w.remote_path, "/")
        vpath = w._parse_event_line("CREATE /etc/hosts")
        self.assertEqual(vpath, "/etc/hosts")

    def test_trailing_slash_stripped(self):
        w = self._make_watcher(remote_path="/data/storage/")
        self.assertEqual(w.remote_path, "/data/storage")

    def test_prefix_boundary_no_false_match(self):
        """remote_path=/data must NOT match /datafile.txt."""
        w = self._make_watcher(remote_path="/data")
        self.assertIsNone(w._parse_event_line("CREATE /datafile.txt"))
        self.assertIsNone(w._parse_event_line("MODIFY /data2/foo.txt"))
        # But /data/file.txt should match.
        self.assertEqual(
            w._parse_event_line("CREATE /data/file.txt"),
            "/file.txt",
        )


class TestRemoteWatcherMultiMount(TestCase):
    """Test that each mount gets its own watcher (no deduplication)."""

    def test_two_watchers_same_backend_both_start(self):
        """Two watchers for the same backend should both be startable."""
        changes_1 = []
        changes_2 = []
        w1 = RemoteWatcher(
            backend_name="shared-backend",
            ssh_host="h", ssh_port=22, remote_path="/data",
            on_change=changes_1.append,
        )
        w2 = RemoteWatcher(
            backend_name="shared-backend",
            ssh_host="h", ssh_port=22, remote_path="/data",
            on_change=changes_2.append,
        )
        # Both should be independently functional (no lock contention).
        # We can't actually start them (no SSH), but verify they have
        # independent state.
        self.assertIsNot(w1._stop_event, w2._stop_event)
        self.assertIsNot(w1.on_change, w2.on_change)

    def test_event_dispatched_to_own_callback(self):
        """Each watcher dispatches to its own on_change, not a shared one."""
        results = {"w1": [], "w2": []}
        w1 = RemoteWatcher(
            backend_name="test", ssh_host="h", ssh_port=22,
            remote_path="/data", on_change=lambda p: results["w1"].append(p),
        )
        w2 = RemoteWatcher(
            backend_name="test", ssh_host="h", ssh_port=22,
            remote_path="/data", on_change=lambda p: results["w2"].append(p),
        )

        # Simulate an event on w1 only.
        vpath = w1._parse_event_line("CREATE /data/subdir/file.txt")
        if vpath:
            w1.on_change(vpath)

        self.assertEqual(results["w1"], ["/subdir/file.txt"])
        self.assertEqual(results["w2"], [])  # w2 not notified


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

    def test_path_with_spaces_is_quoted(self):
        """Paths with spaces must be shell-quoted in the inotifywait command."""
        w = RemoteWatcher(
            backend_name="space-test",
            ssh_host="h", ssh_port=22,
            remote_path="/data/my storage",
            on_change=lambda p: None,
        )
        cmd = w._build_ssh_command()
        inotify_part = cmd[-1]
        # shlex.quote wraps in single quotes
        self.assertIn("'/data/my storage'", inotify_part)


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

    def test_watchers_override_ttl(self):
        """When watchers are enabled, TTL should be infinite (push-only)."""
        import math
        from storage.fs import StorageFS
        with tempfile.TemporaryDirectory() as tmp:
            cache = Path(tmp) / "cache"
            cache.mkdir()
            db = Path(tmp) / "state.db"
            fs = StorageFS(
                cache_root=cache, state_db=db, backends={},
                cache_size=1024 * 1024, num_sync_workers=0, start_sync=False,
                readdir_ttl=5.0,  # should be ignored
                enable_watchers=True,
            )
            self.assertTrue(math.isinf(fs._readdir_snapshot_ttl))
            fs.state_manager.close()
