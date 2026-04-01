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


class TestMultiPathWatcher(TestCase):
    """Test coalesced multi-path watching."""

    def test_multi_path_command_includes_all_paths(self):
        w = RemoteWatcher(
            backend_name="remote0,remote1",
            ssh_host="host",
            ssh_port=22,
            remote_paths=["/srv/raid/warez", "/srv/chunk0/warez"],
            on_change=lambda p: None,
            username="user",
        )
        cmd = w._build_ssh_command()
        inotify_part = cmd[-1]
        self.assertIn("/srv/raid/warez", inotify_part)
        self.assertIn("/srv/chunk0/warez", inotify_part)

    def test_multi_path_parse_matches_correct_root(self):
        w = RemoteWatcher(
            backend_name="multi",
            ssh_host="host",
            ssh_port=22,
            remote_paths=["/srv/raid/warez", "/srv/chunk0/warez"],
            on_change=lambda p: None,
        )
        # Event under first path
        self.assertEqual(
            w._parse_event_line("CREATE /srv/raid/warez/movies/test.mkv"),
            "/movies/test.mkv",
        )
        # Event under second path
        self.assertEqual(
            w._parse_event_line("MODIFY /srv/chunk0/warez/music/song.flac"),
            "/music/song.flac",
        )
        # Event under neither path
        self.assertIsNone(
            w._parse_event_line("CREATE /srv/other/file.txt"),
        )

    def test_multi_path_root_event(self):
        w = RemoteWatcher(
            backend_name="multi",
            ssh_host="host",
            ssh_port=22,
            remote_paths=["/srv/raid/warez", "/srv/chunk0/warez"],
            on_change=lambda p: None,
        )
        self.assertEqual(
            w._parse_event_line("ATTRIB /srv/chunk0/warez"),
            "/",
        )

    def test_legacy_single_path_still_works(self):
        w = RemoteWatcher(
            backend_name="legacy",
            ssh_host="host",
            ssh_port=22,
            remote_path="/data/storage",
            on_change=lambda p: None,
        )
        self.assertEqual(w._remote_paths, ["/data/storage"])
        self.assertEqual(
            w._parse_event_line("CREATE /data/storage/file.txt"),
            "/file.txt",
        )

    def test_overlapping_roots_longest_match_wins(self):
        w = RemoteWatcher(
            backend_name="overlap",
            ssh_host="host",
            ssh_port=22,
            remote_paths=["/data", "/data/sub"],
            on_change=lambda p: None,
        )
        # /data/sub/file.txt should match /data/sub (more specific), not /data
        self.assertEqual(
            w._parse_event_line("CREATE /data/sub/file.txt"),
            "/file.txt",
        )
        # /data/other.txt should match /data
        self.assertEqual(
            w._parse_event_line("CREATE /data/other.txt"),
            "/other.txt",
        )
        # /data/sub itself is the root of the second path
        self.assertEqual(
            w._parse_event_line("ATTRIB /data/sub"),
            "/",
        )

    def test_positional_constructor_backwards_compat(self):
        changes = []
        w = RemoteWatcher("name", "host", 22, "/data", changes.append)
        self.assertEqual(w._remote_paths, ["/data"])
        # Verify on_change works by calling it
        w.on_change("/test")
        self.assertEqual(changes, ["/test"])
        vpath = w._parse_event_line("CREATE /data/file.txt")
        self.assertEqual(vpath, "/file.txt")

    def test_three_paths_single_command(self):
        w = RemoteWatcher(
            backend_name="r0,r1,r2",
            ssh_host="host",
            ssh_port=22,
            remote_paths=["/srv/raid/warez", "/srv/chunk0/warez", "/srv/chunk1/warez"],
            on_change=lambda p: None,
            username="tedks",
        )
        cmd = w._build_ssh_command()
        inotify_part = cmd[-1]
        # All three paths in one inotifywait command
        self.assertIn("/srv/raid/warez", inotify_part)
        self.assertIn("/srv/chunk0/warez", inotify_part)
        self.assertIn("/srv/chunk1/warez", inotify_part)
        # Only one ssh command (verified by cmd structure)
        self.assertEqual(cmd[0], "ssh")


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

    def test_watchers_override_ttl_all_push_capable_members(self):
        """When watchers are enabled and all members are watch-capable, TTL is infinite."""
        import math
        from storage.fs import StorageFS
        from storage.backends.base import WatcherSpec
        from storage.backends.local import LocalBackend

        class WatchableLoopbackMember(LocalBackend):
            def push_invalidation_spec(self):
                return WatcherSpec(
                    group_key=("watch-host", 22, "user", None),
                    ssh_host="watch-host",
                    ssh_port=22,
                    username="user",
                    key_filename=None,
                    remote_path="/srv/data",
                )

        with tempfile.TemporaryDirectory() as tmp:
            cache = Path(tmp) / "cache"
            cache.mkdir()
            db = Path(tmp) / "state.db"
            be_root = Path(tmp) / "backend"
            be_root.mkdir()
            fs = StorageFS(
                cache_root=cache, state_db=db,
                backends={"member0": WatchableLoopbackMember("member0", str(be_root))},
                cache_size=1024 * 1024, num_sync_workers=0, start_sync=False,
                readdir_ttl=5.0,
                enable_watchers=True,
            )
            self.assertTrue(math.isinf(fs._readdir_snapshot_ttl))
            fs.state_manager.close()

    def test_watchers_keep_ttl_mixed_backends(self):
        """When watchers are enabled but some members are not watch-capable, keep TTL."""
        import math
        from storage.fs import StorageFS
        from storage.backends.local import LocalBackend
        with tempfile.TemporaryDirectory() as tmp:
            cache = Path(tmp) / "cache"
            cache.mkdir()
            db = Path(tmp) / "state.db"
            be_root = Path(tmp) / "backend"
            be_root.mkdir()
            fs = StorageFS(
                cache_root=cache, state_db=db,
                backends={"local0": LocalBackend("local0", str(be_root))},
                cache_size=1024 * 1024, num_sync_workers=0, start_sync=False,
                readdir_ttl=5.0,
                enable_watchers=True,
            )
            self.assertFalse(math.isinf(fs._readdir_snapshot_ttl))
            self.assertEqual(fs._readdir_snapshot_ttl, 5.0)
            fs.state_manager.close()

    def test_start_watchers_uses_member_watcher_specs(self):
        from unittest import mock

        from storage.backends.base import WatcherSpec
        from storage.backends.local import LocalBackend
        from storage.fs import StorageFS

        class WatchableLoopbackMember(LocalBackend):
            def push_invalidation_spec(self):
                return WatcherSpec(
                    group_key=("watch-host", 22, "user", None),
                    ssh_host="watch-host",
                    ssh_port=22,
                    username="user",
                    key_filename=None,
                    remote_path="/srv/data",
                )

        with tempfile.TemporaryDirectory() as tmp:
            cache = Path(tmp) / "cache"
            cache.mkdir()
            db = Path(tmp) / "state.db"
            be_root = Path(tmp) / "backend"
            be_root.mkdir()
            fs = StorageFS(
                cache_root=cache,
                state_db=db,
                backends={"member0": WatchableLoopbackMember("member0", str(be_root))},
                cache_size=1024 * 1024,
                num_sync_workers=0,
                start_sync=False,
                enable_watchers=True,
            )
            try:
                with mock.patch("storage.watcher.RemoteWatcher") as watcher_cls:
                    watcher = watcher_cls.return_value
                    fs._start_watchers()
                    watcher.start.assert_called_once()
                    _, kwargs = watcher_cls.call_args
                    self.assertEqual("member0", kwargs["backend_name"])
                    self.assertEqual("watch-host", kwargs["ssh_host"])
                    self.assertEqual(["/srv/data"], kwargs["remote_paths"])
            finally:
                fs.state_manager.close()


class TestWatcherInvalidation(TestCase):
    """Test the watcher-triggered cache invalidation path in StorageFS."""

    def _make_fs(self, tmp, backend_roots=None):
        """Create a StorageFS with optional LocalBackend(s) for testing.

        backend_roots is a dict of {name: Path} for LocalBackend roots.
        If None, a single 'local0' backend is created.
        """
        from storage.fs import StorageFS
        from storage.backends.local import LocalBackend

        cache = Path(tmp) / "cache"
        cache.mkdir(exist_ok=True)
        db = Path(tmp) / "state.db"

        if backend_roots is None:
            be_root = Path(tmp) / "backend"
            be_root.mkdir(exist_ok=True)
            backends = {"local0": LocalBackend("local0", str(be_root))}
        else:
            backends = {
                name: LocalBackend(name, str(root))
                for name, root in backend_roots.items()
            }

        fs = StorageFS(
            cache_root=cache, state_db=db, backends=backends,
            cache_size=1024 * 1024, num_sync_workers=0, start_sync=False,
        )
        return fs

    def _cache_file(self, fs, virtual_path, content=b"hello"):
        """Write content into the cache at virtual_path and set CLEAN metadata."""
        from storage.models import FileMetadata, SyncStatus

        cache_path = fs.cache_manager.get_cache_path(virtual_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(content)

        meta = FileMetadata(
            virtual_path=virtual_path,
            size=len(content),
            checksum="abc123",
            last_access=0.0,
            last_modified=1.0,
            sync_status=SyncStatus.CLEAN,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        fs.state_manager.set_file_metadata(meta)
        return cache_path

    def test_invalidate_removes_cached_file(self):
        """_invalidate_file_cache removes the cached file for a CLEAN entry."""
        with tempfile.TemporaryDirectory() as tmp:
            fs = self._make_fs(tmp)
            try:
                cache_path = self._cache_file(fs, "/test.txt")
                self.assertTrue(cache_path.exists())

                fs._invalidate_file_cache("/test.txt")

                self.assertFalse(cache_path.exists())
            finally:
                fs.state_manager.close()

    def test_invalidate_skips_uncached_file(self):
        """_invalidate_file_cache on a non-existent cache path raises no error."""
        with tempfile.TemporaryDirectory() as tmp:
            fs = self._make_fs(tmp)
            try:
                # Should not raise.
                fs._invalidate_file_cache("/does-not-exist.txt")
            finally:
                fs.state_manager.close()

    def test_invalidate_skips_dirty_file(self):
        """_invalidate_file_cache must not remove a DIRTY cached file."""
        from storage.models import SyncStatus

        with tempfile.TemporaryDirectory() as tmp:
            fs = self._make_fs(tmp)
            try:
                cache_path = self._cache_file(fs, "/dirty.txt")

                # Mark the file as DIRTY in state.
                meta = fs.state_manager.get_file_metadata("/dirty.txt")
                meta.sync_status = SyncStatus.DIRTY
                fs.state_manager.set_file_metadata(meta)

                fs._invalidate_file_cache("/dirty.txt")

                # The cache file must still be present.
                self.assertTrue(cache_path.exists())
            finally:
                fs.state_manager.close()

    def test_on_remote_change_invalidates_canonical_and_suffixed(self):
        """_on_remote_change clears the canonical cache and dir cache entry."""
        with tempfile.TemporaryDirectory() as tmp:
            be0 = Path(tmp) / "be0"
            be1 = Path(tmp) / "be1"
            be0.mkdir()
            be1.mkdir()
            fs = self._make_fs(tmp, backend_roots={
                "remote0": be0,
                "remote1": be1,
            })
            try:
                # Cache the canonical path.
                cache_path = self._cache_file(fs, "/shared.txt")
                self.assertTrue(cache_path.exists())

                fs._on_remote_change("/shared.txt")

                # Canonical cache should be gone.
                self.assertFalse(cache_path.exists())
            finally:
                fs.state_manager.close()

    def test_invalidate_clears_checksum_records(self):
        """_invalidate_file_cache removes matching checksum records."""
        with tempfile.TemporaryDirectory() as tmp:
            fs = self._make_fs(tmp)
            try:
                self._cache_file(fs, "/checksummed.txt")

                from storage.checksums import ChecksumRecord
                key = ("/checksummed.txt", "local0")
                fs._checksum_records[key] = ChecksumRecord(
                    checksum="deadbeef", timestamp=1.0,
                )
                self.assertIn(key, fs._checksum_records)

                fs._invalidate_file_cache("/checksummed.txt")

                self.assertNotIn(key, fs._checksum_records)
            finally:
                fs.state_manager.close()

    def test_invalidate_suffixed_path_clears_canonical_checksums(self):
        """Invalidating /foo.remote0 also clears checksums keyed under canonical /foo."""
        with tempfile.TemporaryDirectory() as tmp:
            fs = self._make_fs(tmp)
            try:
                # Cache the suffixed conflict path.
                self._cache_file(fs, "/shared.txt.local0")

                from storage.checksums import ChecksumRecord
                # Record is keyed by canonical path, not the suffixed path.
                canonical_key = ("/shared.txt", "local0")
                fs._checksum_records[canonical_key] = ChecksumRecord(
                    checksum="cafebabe", timestamp=1.0,
                )
                self.assertIn(canonical_key, fs._checksum_records)

                fs._invalidate_file_cache("/shared.txt.local0")

                self.assertNotIn(canonical_key, fs._checksum_records)
            finally:
                fs.state_manager.close()

    def test_invalidate_uses_resolve_path_not_old_method(self):
        """_invalidate_file_cache works via _resolve_path (no _resolve_backend_target)."""
        with tempfile.TemporaryDirectory() as tmp:
            fs = self._make_fs(tmp)
            try:
                # Verify the old method does not exist.
                self.assertFalse(
                    hasattr(fs, "_resolve_backend_target"),
                    "_resolve_backend_target should not exist; "
                    "_resolve_path is the replacement.",
                )

                # _invalidate_file_cache should work for a cached file,
                # which implicitly exercises _resolve_path internally.
                cache_path = self._cache_file(fs, "/resolved.txt")
                self.assertTrue(cache_path.exists())

                fs._invalidate_file_cache("/resolved.txt")

                self.assertFalse(cache_path.exists())
            finally:
                fs.state_manager.close()
