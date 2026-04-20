import json
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path

from click.testing import CliRunner

from storage.cache_metadata import XATTR_CACHE_METADATA_NAME, write_cache_metadata
from storage.backends.local import LocalBackend
from storage.cli import main
from storage.fs import StorageFS
from storage.models import FileLocation, FileMetadata, SyncStatus
from storage.state import StateManager


class TestDebugStreamingStatsCLI(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.runner = CliRunner()
        self.fs = None

    def tearDown(self):
        if self.fs is not None:
            self.fs.destroy("/")
            self.fs = None
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _wait_for_socket(self, socket_path: Path, timeout: float = 2.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if socket_path.exists():
                return
            time.sleep(0.02)
        self.fail(f"Control socket was not created at {socket_path}")

    def test_streaming_stats_fails_without_running_mount(self):
        cache_dir = self.temp_dir / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)

        result = self.runner.invoke(
            main,
            ["debug", "streaming-stats", "--cache", str(cache_dir)],
        )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("No running mount control socket", result.output)

    def test_streaming_stats_returns_json_from_running_mount(self):
        cache_dir = self.temp_dir / "cache"
        backend_dir = self.temp_dir / "backend"
        cache_dir.mkdir(parents=True, exist_ok=True)
        backend_dir.mkdir(parents=True, exist_ok=True)

        self.fs = StorageFS(
            cache_root=cache_dir,
            state_db=self.temp_dir / "state.db",
            backends={"local0": LocalBackend("local0", str(backend_dir))},
            cache_size=8 * 1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
            streaming_reads=True,
        )
        socket_path = cache_dir / ".storage.control.sock"
        self.assertFalse(socket_path.exists())
        self.fs.init("/")
        socket_path = cache_dir / ".storage.control.sock"
        self._wait_for_socket(socket_path)

        result = self.runner.invoke(
            main,
            ["debug", "streaming-stats", "--cache", str(cache_dir), "--json-output"],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        payload = json.loads(result.output)
        self.assertEqual(payload["pid"], self.fs._streaming_stats_snapshot()["pid"])
        self.assertTrue(payload["streaming_enabled"])
        self.assertEqual(payload["active_streaming_sessions"], 0)
        self.assertEqual(payload["streaming_sessions_started"], 0)
        self.assertEqual(payload["streaming_fallback_opens"], 0)
        self.assertEqual(payload["foreground_fetches"], 0)
        self.assertEqual(payload["background_fetches"], 0)


class TestDebugIndexCLI(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.runner = CliRunner()
        self.cache_dir = self.temp_dir / "cache"
        self.cache_dir.mkdir()
        self.db_path = self.temp_dir / "state.db"

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _metadata(self, virtual_path: str, cache_path: Path, *, sync_status=SyncStatus.CLEAN):
        st = cache_path.stat()
        return FileMetadata(
            virtual_path=virtual_path,
            size=st.st_size,
            checksum="checksum-1",
            last_access=st.st_atime,
            last_modified=st.st_mtime,
            sync_status=sync_status,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )

    def test_rebuild_index_backfills_missing_xattr(self):
        cache_path = self.cache_dir / "movie.mkv"
        cache_path.write_bytes(b"payload")
        seed_metadata = self._metadata("/movie.mkv", cache_path, sync_status=SyncStatus.CLEAN)

        state_manager = StateManager(str(self.db_path))
        try:
            state_manager.set_file_metadata(seed_metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie.mkv",
                    backend_name="cache",
                    checksum=seed_metadata.checksum or "",
                    last_synced=seed_metadata.last_modified,
                    is_cached=True,
                )
            )
        finally:
            state_manager.close()

        result = self.runner.invoke(
            main,
            [
                "debug",
                "rebuild-index",
                "--cache",
                str(self.cache_dir),
                "--database",
                str(self.db_path),
                "--json-output",
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        payload = json.loads(result.output)
        self.assertEqual(payload["rebuilt_files"], 1)
        self.assertEqual(payload["inferred_metadata_files"], 1)
        self.assertEqual(payload["normalized_xattrs"], 1)
        self.assertEqual(payload["skipped_files"], 0)

        state_manager = StateManager(str(self.db_path))
        try:
            metadata = state_manager.get_file_metadata("/movie.mkv")
            self.assertIsNotNone(metadata)
            self.assertEqual(metadata.size, len(b"payload"))
            self.assertEqual(metadata.sync_status, SyncStatus.CLEAN)
            locations = state_manager.get_file_locations("/movie.mkv")
            self.assertEqual(["cache"], [location.backend_name for location in locations])
        finally:
            state_manager.close()

        self.assertIn(XATTR_CACHE_METADATA_NAME, os.listxattr(str(cache_path)))

    def test_verify_index_reports_metadata_drift(self):
        cache_path = self.cache_dir / "song.flac"
        cache_path.write_bytes(b"sound")
        metadata = self._metadata("/song.flac", cache_path, sync_status=SyncStatus.CLEAN)
        write_cache_metadata(cache_path, metadata)

        state_manager = StateManager(str(self.db_path))
        try:
            dirty_metadata = FileMetadata(
                virtual_path=metadata.virtual_path,
                size=metadata.size,
                checksum=metadata.checksum,
                last_access=metadata.last_access,
                last_modified=metadata.last_modified,
                sync_status=SyncStatus.DIRTY,
                replica_count=metadata.replica_count,
                recomputable=metadata.recomputable,
                recompute_cmd=metadata.recompute_cmd,
                encrypted=metadata.encrypted,
                encryption_key_id=metadata.encryption_key_id,
            )
            state_manager.set_file_metadata(dirty_metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/song.flac",
                    backend_name="cache",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )
        finally:
            state_manager.close()

        result = self.runner.invoke(
            main,
            [
                "debug",
                "verify-index",
                "--cache",
                str(self.cache_dir),
                "--database",
                str(self.db_path),
                "--json-output",
            ],
        )

        self.assertEqual(result.exit_code, 1, msg=result.output)
        payload = json.loads(result.output)
        self.assertGreater(payload["issue_count"], 0)
        self.assertTrue(
            any(
                issue["kind"] == "metadata_mismatch" and "sync_status" in issue["detail"]
                for issue in payload["issues"]
            )
        )
