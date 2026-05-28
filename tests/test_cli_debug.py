import json
import os
import shutil
import sqlite3
import tempfile
import time
import unittest
from unittest import mock
from pathlib import Path

from click.testing import CliRunner

from storage.checksums import fast_checksum_file
from storage.cache_metadata import XATTR_CACHE_METADATA_NAME, read_cache_metadata, write_cache_metadata
from storage.backends.base import Backend, BackendType, StorageMemberCapabilities
from storage.backends.local import LocalBackend
from storage.cli import main
from storage.fs import StorageFS
from storage.index_audit import (
    rebuild_index_from_cache,
    reprobe_member_locations,
    verify_index_against_cache,
)
from storage.models import FileLocation, FileMetadata, SyncStatus
from storage.state import StateManager


class FailingProbeBackend(Backend):
    def __init__(self, name: str):
        super().__init__(
            name=name,
            backend_type=BackendType.LOCAL,
            is_remote=False,
            speed_priority=0,
            capabilities=StorageMemberCapabilities(),
        )

    def exists(self, virtual_path: str) -> bool:
        return False

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        raise NotImplementedError

    def write_file(self, virtual_path: str, data: bytes, offset: int):
        raise NotImplementedError

    def create_file(self, virtual_path: str):
        raise NotImplementedError

    def delete_file(self, virtual_path: str):
        raise NotImplementedError

    def truncate_file(self, virtual_path: str):
        raise NotImplementedError

    def get_size(self, virtual_path: str) -> int:
        raise NotImplementedError

    def mkdir(self, virtual_path: str):
        raise NotImplementedError

    def rmdir(self, virtual_path: str):
        raise NotImplementedError

    def listdir(self, virtual_path: str):
        raise NotImplementedError

    def isdir(self, virtual_path: str) -> bool:
        return False

    def copy_to_local(self, virtual_path: str, local_path: Path):
        raise NotImplementedError

    def copy_from_local(self, local_path: Path, virtual_path: str):
        raise NotImplementedError

    def rename(self, old_virtual_path: str, new_virtual_path: str):
        raise NotImplementedError

    def get_free_space(self):
        return None

    def stat(self, virtual_path: str):
        self.set_available(False)
        raise ConnectionError(f"{self.name} unavailable")

    def sync(self):
        pass


class NoChecksumProbeBackend(Backend):
    def __init__(self, name: str, root: Path):
        super().__init__(
            name=name,
            backend_type=BackendType.LOCAL,
            is_remote=False,
            speed_priority=0,
            capabilities=StorageMemberCapabilities(),
        )
        self.root = root

    def _resolve(self, virtual_path: str) -> Path:
        return self.root / virtual_path.lstrip("/")

    def exists(self, virtual_path: str) -> bool:
        return self._resolve(virtual_path).exists()

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        raise NotImplementedError

    def write_file(self, virtual_path: str, data: bytes, offset: int):
        raise NotImplementedError

    def create_file(self, virtual_path: str):
        raise NotImplementedError

    def delete_file(self, virtual_path: str):
        raise NotImplementedError

    def truncate_file(self, virtual_path: str):
        raise NotImplementedError

    def get_size(self, virtual_path: str) -> int:
        raise NotImplementedError

    def mkdir(self, virtual_path: str):
        raise NotImplementedError

    def rmdir(self, virtual_path: str):
        raise NotImplementedError

    def listdir(self, virtual_path: str):
        raise NotImplementedError

    def isdir(self, virtual_path: str) -> bool:
        return False

    def copy_to_local(self, virtual_path: str, local_path: Path):
        raise NotImplementedError

    def copy_from_local(self, local_path: Path, virtual_path: str):
        raise NotImplementedError

    def rename(self, old_virtual_path: str, new_virtual_path: str):
        raise NotImplementedError

    def get_free_space(self):
        return None

    def stat(self, virtual_path: str):
        return os.lstat(self._resolve(virtual_path))

    def sync(self):
        pass


class MismatchThenFailProbeBackend(NoChecksumProbeBackend):
    def __init__(self, name: str, root: Path, *, fail_path: str, mismatch_path: str):
        super().__init__(name, root)
        self.fail_path = fail_path
        self.mismatch_path = mismatch_path

    def stat(self, virtual_path: str):
        if virtual_path == self.fail_path:
            self.set_available(False)
            raise ConnectionError(f"{self.name} unavailable mid-probe")
        return super().stat(virtual_path)

    def get_fast_checksum(self, virtual_path: str) -> str:
        if virtual_path == self.mismatch_path:
            return "remote-mismatch"
        raise AssertionError("get_fast_checksum should not be called for the failing path")


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
            checksum=fast_checksum_file(cache_path),
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

    def test_rebuild_index_rejects_missing_cache_dir_without_mutating_database(self):
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
            missing_cache = self.temp_dir / "missing-cache"
            with self.assertRaises(FileNotFoundError):
                rebuild_index_from_cache(missing_cache, state_manager)

            metadata = state_manager.get_file_metadata("/movie.mkv")
            self.assertIsNotNone(metadata)
            locations = state_manager.get_file_locations("/movie.mkv")
            self.assertEqual(["cache"], [location.backend_name for location in locations])
        finally:
            state_manager.close()

    def test_verify_index_rejects_missing_cache_dir(self):
        missing_cache = self.temp_dir / "missing-cache"
        state_manager = StateManager(str(self.db_path))
        try:
            with self.assertRaises(FileNotFoundError):
                verify_index_against_cache(missing_cache, state_manager)
        finally:
            state_manager.close()

    def test_reprobe_locations_rebuilds_noncache_member_rows(self):
        cache_path = self.cache_dir / "movie.mkv"
        cache_path.write_bytes(b"payload")
        metadata = self._metadata("/movie.mkv", cache_path, sync_status=SyncStatus.CLEAN)
        write_cache_metadata(cache_path, metadata)

        state_manager = StateManager(str(self.db_path))
        try:
            state_manager.set_file_metadata(metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie.mkv",
                    backend_name="cache",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )
        finally:
            state_manager.close()

        remote_dir = self.temp_dir / "remote"
        remote_dir.mkdir()
        (remote_dir / "movie.mkv").write_bytes(b"payload")

        result = self.runner.invoke(
            main,
            [
                "debug",
                "reprobe-locations",
                "--cache",
                str(self.cache_dir),
                "--database",
                str(self.db_path),
                "--member",
                f"local0={remote_dir}",
                "--json-output",
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        payload = json.loads(result.output)
        self.assertEqual(payload["files_considered"], 1)
        self.assertEqual(payload["members_requested"], 1)
        self.assertEqual(payload["members_reprobed"], 1)
        self.assertEqual(payload["members_unavailable"], 0)
        self.assertEqual(payload["remote_locations_found"], 1)

        state_manager = StateManager(str(self.db_path))
        try:
            locations = state_manager.get_file_locations("/movie.mkv")
            backend_names = sorted(location.backend_name for location in locations)
            self.assertEqual(["cache", "local0"], backend_names)
        finally:
            state_manager.close()

    def test_reprobe_locations_preserves_existing_rows_for_failed_member(self):
        cache_path = self.cache_dir / "movie.mkv"
        cache_path.write_bytes(b"payload")
        metadata = self._metadata("/movie.mkv", cache_path, sync_status=SyncStatus.CLEAN)

        state_manager = StateManager(str(self.db_path))
        try:
            state_manager.set_file_metadata(metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie.mkv",
                    backend_name="cache",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie.mkv",
                    backend_name="remote0",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=False,
                )
            )

            report = reprobe_member_locations(
                state_manager,
                {"remote0": FailingProbeBackend("remote0")},
                cache_root=self.cache_dir,
            )

            self.assertEqual(report.members_requested, 1)
            self.assertEqual(report.members_reprobed, 0)
            self.assertEqual(report.members_unavailable, 1)

            locations = state_manager.get_file_locations("/movie.mkv")
            backend_names = sorted(location.backend_name for location in locations)
            self.assertEqual(["cache", "remote0"], backend_names)
        finally:
            state_manager.close()

    def test_reprobe_locations_removes_existing_row_when_member_file_is_absent(self):
        cache_path = self.cache_dir / "movie-absent.mkv"
        cache_path.write_bytes(b"payload")
        metadata = self._metadata("/movie-absent.mkv", cache_path, sync_status=SyncStatus.CLEAN)
        write_cache_metadata(cache_path, metadata)

        state_manager = StateManager(str(self.db_path))
        try:
            state_manager.set_file_metadata(metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie-absent.mkv",
                    backend_name="cache",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie-absent.mkv",
                    backend_name="local0",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=False,
                )
            )

            remote_dir = self.temp_dir / "remote-absent"
            remote_dir.mkdir()

            report = reprobe_member_locations(
                state_manager,
                {"local0": LocalBackend("local0", str(remote_dir))},
                cache_root=self.cache_dir,
            )

            self.assertEqual(report.members_requested, 1)
            self.assertEqual(report.members_reprobed, 1)
            self.assertEqual(report.remote_locations_found, 0)
            self.assertEqual(
                ["cache"],
                sorted(location.backend_name for location in state_manager.get_file_locations("/movie-absent.mkv")),
            )
        finally:
            state_manager.close()

    def test_reprobe_locations_defers_conflict_promotion_until_member_probe_succeeds(self):
        first_cache_path = self.cache_dir / "movie-a.mkv"
        second_cache_path = self.cache_dir / "movie-b.mkv"
        first_cache_path.write_bytes(b"payload-a")
        second_cache_path.write_bytes(b"payload-b")
        first_metadata = self._metadata("/movie-a.mkv", first_cache_path, sync_status=SyncStatus.CLEAN)
        second_metadata = self._metadata("/movie-b.mkv", second_cache_path, sync_status=SyncStatus.CLEAN)
        write_cache_metadata(first_cache_path, first_metadata)
        write_cache_metadata(second_cache_path, second_metadata)

        state_manager = StateManager(str(self.db_path))
        try:
            for metadata in (first_metadata, second_metadata):
                state_manager.set_file_metadata(metadata)
                state_manager.add_file_location(
                    FileLocation(
                        virtual_path=metadata.virtual_path,
                        backend_name="cache",
                        checksum=metadata.checksum or "",
                        last_synced=metadata.last_modified,
                        is_cached=True,
                    )
                )
                state_manager.add_file_location(
                    FileLocation(
                        virtual_path=metadata.virtual_path,
                        backend_name="remote0",
                        checksum=metadata.checksum or "",
                        last_synced=metadata.last_modified,
                        is_cached=False,
                    )
                )

            remote_dir = self.temp_dir / "remote-partial-failure"
            remote_dir.mkdir()
            (remote_dir / "movie-a.mkv").write_bytes(b"payload-a")

            report = reprobe_member_locations(
                state_manager,
                {
                    "remote0": MismatchThenFailProbeBackend(
                        "remote0",
                        remote_dir,
                        fail_path="/movie-b.mkv",
                        mismatch_path="/movie-a.mkv",
                    ),
                },
                cache_root=self.cache_dir,
            )

            self.assertEqual(report.members_requested, 1)
            self.assertEqual(report.members_reprobed, 0)
            self.assertEqual(report.members_unavailable, 1)
            self.assertTrue(any(issue.kind == "member_checksum_mismatch" for issue in report.issues))

            refreshed_first = state_manager.get_file_metadata("/movie-a.mkv")
            refreshed_second = state_manager.get_file_metadata("/movie-b.mkv")
            self.assertEqual(SyncStatus.CLEAN, refreshed_first.sync_status)
            self.assertEqual(SyncStatus.CLEAN, refreshed_second.sync_status)
            self.assertEqual(
                ["cache", "remote0"],
                sorted(location.backend_name for location in state_manager.get_file_locations("/movie-a.mkv")),
            )
        finally:
            state_manager.close()

        self.assertEqual(SyncStatus.CLEAN, read_cache_metadata(first_cache_path).sync_status)
        self.assertEqual(SyncStatus.CLEAN, read_cache_metadata(second_cache_path).sync_status)

    def test_reprobe_locations_leaves_state_untouched_when_conflict_xattr_write_fails(self):
        cache_path = self.cache_dir / "movie-fail.mkv"
        cache_path.write_bytes(b"payload")
        metadata = self._metadata("/movie-fail.mkv", cache_path, sync_status=SyncStatus.DIRTY)
        write_cache_metadata(cache_path, metadata)

        state_manager = StateManager(str(self.db_path))
        try:
            state_manager.set_file_metadata(metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie-fail.mkv",
                    backend_name="cache",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie-fail.mkv",
                    backend_name="local0",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=False,
                )
            )

            remote_dir = self.temp_dir / "remote-xattr-fail"
            remote_dir.mkdir()
            (remote_dir / "movie-fail.mkv").write_bytes(b"other-payload")

            with mock.patch("storage.index_audit.write_cache_metadata", side_effect=OSError("xattr failed")):
                report = reprobe_member_locations(
                    state_manager,
                    {"local0": LocalBackend("local0", str(remote_dir))},
                    cache_root=self.cache_dir,
                )

            self.assertEqual(report.members_requested, 1)
            self.assertEqual(report.members_reprobed, 0)
            self.assertTrue(any(issue.kind == "cache_metadata_sync_failed" for issue in report.issues))

            refreshed = state_manager.get_file_metadata("/movie-fail.mkv")
            self.assertEqual(SyncStatus.DIRTY, refreshed.sync_status)
            self.assertEqual(
                ["cache", "local0"],
                sorted(location.backend_name for location in state_manager.get_file_locations("/movie-fail.mkv")),
            )
        finally:
            state_manager.close()

        self.assertEqual(SyncStatus.DIRTY, read_cache_metadata(cache_path).sync_status)

    def test_reprobe_locations_rolls_back_earlier_conflict_xattrs_when_later_write_fails(self):
        first_cache_path = self.cache_dir / "movie-a.mkv"
        second_cache_path = self.cache_dir / "movie-b.mkv"
        first_cache_path.write_bytes(b"payload-a")
        second_cache_path.write_bytes(b"payload-b")
        first_metadata = self._metadata("/movie-a.mkv", first_cache_path, sync_status=SyncStatus.CLEAN)
        second_metadata = self._metadata("/movie-b.mkv", second_cache_path, sync_status=SyncStatus.CLEAN)
        write_cache_metadata(first_cache_path, first_metadata)
        write_cache_metadata(second_cache_path, second_metadata)

        state_manager = StateManager(str(self.db_path))
        try:
            for metadata in (first_metadata, second_metadata):
                state_manager.set_file_metadata(metadata)
                state_manager.add_file_location(
                    FileLocation(
                        virtual_path=metadata.virtual_path,
                        backend_name="cache",
                        checksum=metadata.checksum or "",
                        last_synced=metadata.last_modified,
                        is_cached=True,
                    )
                )
                state_manager.add_file_location(
                    FileLocation(
                        virtual_path=metadata.virtual_path,
                        backend_name="local0",
                        checksum=metadata.checksum or "",
                        last_synced=metadata.last_modified,
                        is_cached=False,
                    )
                )
        finally:
            state_manager.close()

        remote_dir = self.temp_dir / "remote-rollback"
        remote_dir.mkdir()
        (remote_dir / "movie-a.mkv").write_bytes(b"other-a")
        (remote_dir / "movie-b.mkv").write_bytes(b"other-b")

        original_write_cache_metadata = write_cache_metadata
        write_calls = {"count": 0}

        def flaky_write_cache_metadata(cache_path, metadata, *, virtual_path=None):
            if metadata.sync_status == SyncStatus.CONFLICT:
                write_calls["count"] += 1
                if write_calls["count"] == 2:
                    raise OSError("second conflict xattr failed")
            return original_write_cache_metadata(cache_path, metadata, virtual_path=virtual_path)

        state_manager = StateManager(str(self.db_path))
        try:
            with mock.patch("storage.index_audit.write_cache_metadata", side_effect=flaky_write_cache_metadata):
                report = reprobe_member_locations(
                    state_manager,
                    {"local0": LocalBackend("local0", str(remote_dir))},
                    cache_root=self.cache_dir,
                )

            self.assertEqual(report.members_requested, 1)
            self.assertEqual(report.members_reprobed, 0)
            self.assertTrue(any(issue.kind == "cache_metadata_sync_failed" for issue in report.issues))

            refreshed_first = state_manager.get_file_metadata("/movie-a.mkv")
            refreshed_second = state_manager.get_file_metadata("/movie-b.mkv")
            self.assertEqual(SyncStatus.CLEAN, refreshed_first.sync_status)
            self.assertEqual(SyncStatus.CLEAN, refreshed_second.sync_status)
            self.assertEqual(
                ["cache", "local0"],
                sorted(location.backend_name for location in state_manager.get_file_locations("/movie-a.mkv")),
            )
            self.assertEqual(
                ["cache", "local0"],
                sorted(location.backend_name for location in state_manager.get_file_locations("/movie-b.mkv")),
            )
        finally:
            state_manager.close()

        self.assertEqual(SyncStatus.CLEAN, read_cache_metadata(first_cache_path).sync_status)
        self.assertEqual(SyncStatus.CLEAN, read_cache_metadata(second_cache_path).sync_status)

    def test_reprobe_locations_reconciles_database_when_conflict_xattr_rollback_fails(self):
        first_cache_path = self.cache_dir / "movie-a.mkv"
        second_cache_path = self.cache_dir / "movie-b.mkv"
        first_cache_path.write_bytes(b"payload-a")
        second_cache_path.write_bytes(b"payload-b")
        first_metadata = self._metadata("/movie-a.mkv", first_cache_path, sync_status=SyncStatus.CLEAN)
        second_metadata = self._metadata("/movie-b.mkv", second_cache_path, sync_status=SyncStatus.CLEAN)
        write_cache_metadata(first_cache_path, first_metadata)
        write_cache_metadata(second_cache_path, second_metadata)

        state_manager = StateManager(str(self.db_path))
        try:
            for metadata in (first_metadata, second_metadata):
                state_manager.set_file_metadata(metadata)
                state_manager.add_file_location(
                    FileLocation(
                        virtual_path=metadata.virtual_path,
                        backend_name="cache",
                        checksum=metadata.checksum or "",
                        last_synced=metadata.last_modified,
                        is_cached=True,
                    )
                )
                state_manager.add_file_location(
                    FileLocation(
                        virtual_path=metadata.virtual_path,
                        backend_name="local0",
                        checksum=metadata.checksum or "",
                        last_synced=metadata.last_modified,
                        is_cached=False,
                    )
                )

            remote_dir = self.temp_dir / "remote-rollback-reconcile"
            remote_dir.mkdir()
            (remote_dir / "movie-a.mkv").write_bytes(b"other-a")
            (remote_dir / "movie-b.mkv").write_bytes(b"other-b")

            original_write_cache_metadata = write_cache_metadata
            conflict_writes = {"count": 0}

            def flaky_write_cache_metadata(cache_path, metadata, *, virtual_path=None):
                if metadata.sync_status == SyncStatus.CONFLICT:
                    conflict_writes["count"] += 1
                    if conflict_writes["count"] == 2:
                        raise OSError("second conflict xattr failed")
                elif virtual_path == "/movie-a.mkv":
                    raise OSError("rollback failed")
                return original_write_cache_metadata(cache_path, metadata, virtual_path=virtual_path)

            with mock.patch("storage.index_audit.write_cache_metadata", side_effect=flaky_write_cache_metadata):
                report = reprobe_member_locations(
                    state_manager,
                    {"local0": LocalBackend("local0", str(remote_dir))},
                    cache_root=self.cache_dir,
                )

            self.assertEqual(report.members_reprobed, 0)
            self.assertTrue(any(issue.kind == "cache_metadata_sync_failed" for issue in report.issues))
            self.assertTrue(any(issue.kind == "cache_metadata_rollback_failed" for issue in report.issues))

            refreshed_first = state_manager.get_file_metadata("/movie-a.mkv")
            refreshed_second = state_manager.get_file_metadata("/movie-b.mkv")
            self.assertEqual(SyncStatus.CONFLICT, refreshed_first.sync_status)
            self.assertEqual(SyncStatus.CLEAN, refreshed_second.sync_status)
        finally:
            state_manager.close()

        self.assertEqual(SyncStatus.CONFLICT, read_cache_metadata(first_cache_path).sync_status)
        self.assertEqual(SyncStatus.CLEAN, read_cache_metadata(second_cache_path).sync_status)

    def test_reprobe_locations_promotes_clean_mismatch_to_conflict_and_drops_replica_row(self):
        cache_path = self.cache_dir / "movie.mkv"
        cache_path.write_bytes(b"payload")
        metadata = self._metadata("/movie.mkv", cache_path, sync_status=SyncStatus.CLEAN)
        write_cache_metadata(cache_path, metadata)

        state_manager = StateManager(str(self.db_path))
        try:
            state_manager.set_file_metadata(metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie.mkv",
                    backend_name="cache",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )
        finally:
            state_manager.close()

        remote_dir = self.temp_dir / "remote-mismatch"
        remote_dir.mkdir()
        (remote_dir / "movie.mkv").write_bytes(b"other-payload")

        result = self.runner.invoke(
            main,
            [
                "debug",
                "reprobe-locations",
                "--cache",
                str(self.cache_dir),
                "--database",
                str(self.db_path),
                "--member",
                f"local0={remote_dir}",
                "--json-output",
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        payload = json.loads(result.output)
        self.assertTrue(
            any(
                issue["kind"] == "member_checksum_mismatch"
                for issue in payload["issues"]
            )
        )

        state_manager = StateManager(str(self.db_path))
        try:
            refreshed = state_manager.get_file_metadata("/movie.mkv")
            self.assertIsNotNone(refreshed)
            self.assertEqual(SyncStatus.CONFLICT, refreshed.sync_status)
            locations = state_manager.get_file_locations("/movie.mkv")
            self.assertEqual(["cache"], [location.backend_name for location in locations])
        finally:
            state_manager.close()

        record = read_cache_metadata(cache_path)
        self.assertIsNotNone(record)
        self.assertEqual(SyncStatus.CONFLICT, record.sync_status)

    def test_reprobe_locations_records_unverified_checksum_when_backend_lacks_fast_checksum(self):
        cache_path = self.cache_dir / "movie-unverified.mkv"
        cache_path.write_bytes(b"payload")
        metadata = self._metadata("/movie-unverified.mkv", cache_path, sync_status=SyncStatus.CLEAN)
        write_cache_metadata(cache_path, metadata)

        state_manager = StateManager(str(self.db_path))
        try:
            state_manager.set_file_metadata(metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie-unverified.mkv",
                    backend_name="cache",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )
        finally:
            state_manager.close()

        remote_dir = self.temp_dir / "remote-unverified"
        remote_dir.mkdir()
        (remote_dir / "movie-unverified.mkv").write_bytes(b"payload")

        state_manager = StateManager(str(self.db_path))
        try:
            report = reprobe_member_locations(
                state_manager,
                {"local0": NoChecksumProbeBackend("local0", remote_dir)},
                cache_root=self.cache_dir,
            )

            self.assertTrue(any(issue.kind == "member_checksum_unverified" for issue in report.issues))
            self.assertEqual(0, report.remote_locations_found)
            self.assertEqual(
                ["cache"],
                sorted(location.backend_name for location in state_manager.get_file_locations("/movie-unverified.mkv")),
            )
        finally:
            state_manager.close()

    def test_reprobe_locations_preserves_existing_rows_when_checksum_is_unverified(self):
        cache_path = self.cache_dir / "movie-unverified-existing.mkv"
        cache_path.write_bytes(b"payload")
        metadata = self._metadata("/movie-unverified-existing.mkv", cache_path, sync_status=SyncStatus.CLEAN)
        write_cache_metadata(cache_path, metadata)

        state_manager = StateManager(str(self.db_path))
        try:
            state_manager.set_file_metadata(metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie-unverified-existing.mkv",
                    backend_name="cache",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie-unverified-existing.mkv",
                    backend_name="local0",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=False,
                )
            )

            remote_dir = self.temp_dir / "remote-unverified-existing"
            remote_dir.mkdir()
            (remote_dir / "movie-unverified-existing.mkv").write_bytes(b"payload")

            report = reprobe_member_locations(
                state_manager,
                {"local0": NoChecksumProbeBackend("local0", remote_dir)},
                cache_root=self.cache_dir,
            )

            self.assertTrue(any(issue.kind == "member_checksum_unverified" for issue in report.issues))
            self.assertEqual(
                ["cache", "local0"],
                sorted(location.backend_name for location in state_manager.get_file_locations("/movie-unverified-existing.mkv")),
            )
        finally:
            state_manager.close()

    def test_reprobe_locations_rolls_back_conflict_xattrs_when_database_update_fails(self):
        cache_path = self.cache_dir / "movie-db-fail.mkv"
        cache_path.write_bytes(b"payload")
        metadata = self._metadata("/movie-db-fail.mkv", cache_path, sync_status=SyncStatus.CLEAN)
        write_cache_metadata(cache_path, metadata)

        state_manager = StateManager(str(self.db_path))
        try:
            state_manager.set_file_metadata(metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie-db-fail.mkv",
                    backend_name="cache",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie-db-fail.mkv",
                    backend_name="local0",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=False,
                )
            )

            remote_dir = self.temp_dir / "remote-db-fail"
            remote_dir.mkdir()
            (remote_dir / "movie-db-fail.mkv").write_bytes(b"other-payload")

            with mock.patch.object(
                state_manager,
                "_commit_locked",
                side_effect=sqlite3.OperationalError("db write failed"),
            ):
                report = reprobe_member_locations(
                    state_manager,
                    {"local0": LocalBackend("local0", str(remote_dir))},
                    cache_root=self.cache_dir,
                )

            self.assertTrue(any(issue.kind == "member_db_update_failed" for issue in report.issues))
            other_cache_path = self.cache_dir / "other-after-db-fail.mkv"
            other_cache_path.write_bytes(b"payload")
            other_metadata = self._metadata("/other-after-db-fail.mkv", other_cache_path, sync_status=SyncStatus.CLEAN)
            state_manager.set_file_metadata(other_metadata)

            refreshed = state_manager.get_file_metadata("/movie-db-fail.mkv")
            self.assertEqual(SyncStatus.CLEAN, refreshed.sync_status)
            self.assertEqual(
                ["cache", "local0"],
                sorted(location.backend_name for location in state_manager.get_file_locations("/movie-db-fail.mkv")),
            )
        finally:
            state_manager.close()

        self.assertEqual(SyncStatus.CLEAN, read_cache_metadata(cache_path).sync_status)

    def test_reprobe_locations_does_not_create_remote_row_without_checksum_baseline(self):
        cache_path = self.cache_dir / "movie-no-checksum.mkv"
        cache_path.write_bytes(b"payload")
        st = cache_path.stat()
        metadata = FileMetadata(
            virtual_path="/movie-no-checksum.mkv",
            size=st.st_size,
            checksum=None,
            last_access=st.st_atime,
            last_modified=st.st_mtime,
            sync_status=SyncStatus.CLEAN,
            replica_count=1,
            recomputable=False,
            recompute_cmd=None,
            encrypted=False,
            encryption_key_id=None,
        )
        write_cache_metadata(cache_path, metadata)

        state_manager = StateManager(str(self.db_path))
        try:
            state_manager.set_file_metadata(metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie-no-checksum.mkv",
                    backend_name="cache",
                    checksum="",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )
        finally:
            state_manager.close()

        remote_dir = self.temp_dir / "remote-no-checksum"
        remote_dir.mkdir()
        (remote_dir / "movie-no-checksum.mkv").write_bytes(b"payload")

        state_manager = StateManager(str(self.db_path))
        try:
            report = reprobe_member_locations(
                state_manager,
                {"local0": LocalBackend("local0", str(remote_dir))},
                cache_root=self.cache_dir,
            )

            self.assertTrue(any(issue.kind == "member_checksum_baseline_missing" for issue in report.issues))
            self.assertEqual(0, report.remote_locations_found)
            self.assertEqual(
                ["cache"],
                sorted(location.backend_name for location in state_manager.get_file_locations("/movie-no-checksum.mkv")),
            )
        finally:
            state_manager.close()

    def test_index_audit_paths_avoid_full_table_loads(self):
        cache_path = self.cache_dir / "streamed.mkv"
        cache_path.write_bytes(b"payload")
        metadata = self._metadata("/streamed.mkv", cache_path, sync_status=SyncStatus.CLEAN)
        write_cache_metadata(cache_path, metadata)

        remote_dir = self.temp_dir / "remote-streamed"
        remote_dir.mkdir()
        (remote_dir / "streamed.mkv").write_bytes(b"payload")

        state_manager = StateManager(str(self.db_path))
        try:
            state_manager.set_file_metadata(metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/streamed.mkv",
                    backend_name="cache",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )

            with mock.patch.object(
                state_manager,
                "get_all_file_metadata",
                side_effect=AssertionError("full metadata load"),
            ), mock.patch.object(
                state_manager,
                "get_all_file_locations",
                side_effect=AssertionError("full location load"),
            ):
                rebuild_report = rebuild_index_from_cache(self.cache_dir, state_manager)
                verify_report = verify_index_against_cache(self.cache_dir, state_manager)
                reprobe_report = reprobe_member_locations(
                    state_manager,
                    {"local0": LocalBackend("local0", str(remote_dir))},
                    cache_root=self.cache_dir,
                )

            self.assertEqual(1, rebuild_report.rebuilt_files)
            self.assertEqual(0, verify_report.issue_count)
            self.assertEqual(1, reprobe_report.remote_locations_found)
        finally:
            state_manager.close()

    def test_reprobe_locations_overwrites_invalid_cache_xattr_from_database_state(self):
        cache_path = self.cache_dir / "movie-invalid-xattr.mkv"
        cache_path.write_bytes(b"payload")
        metadata = self._metadata("/movie-invalid-xattr.mkv", cache_path, sync_status=SyncStatus.DIRTY)
        os.setxattr(str(cache_path), XATTR_CACHE_METADATA_NAME, b"{not-json")

        state_manager = StateManager(str(self.db_path))
        try:
            state_manager.set_file_metadata(metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie-invalid-xattr.mkv",
                    backend_name="cache",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )
        finally:
            state_manager.close()

        remote_dir = self.temp_dir / "remote-invalid-xattr"
        remote_dir.mkdir()
        (remote_dir / "movie-invalid-xattr.mkv").write_bytes(b"other-payload")

        result = self.runner.invoke(
            main,
            [
                "debug",
                "reprobe-locations",
                "--cache",
                str(self.cache_dir),
                "--database",
                str(self.db_path),
                "--member",
                f"local0={remote_dir}",
                "--json-output",
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        payload = json.loads(result.output)
        self.assertTrue(any(issue["kind"] == "member_checksum_mismatch" for issue in payload["issues"]))

        state_manager = StateManager(str(self.db_path))
        try:
            refreshed = state_manager.get_file_metadata("/movie-invalid-xattr.mkv")
            self.assertIsNotNone(refreshed)
            self.assertEqual(SyncStatus.CONFLICT, refreshed.sync_status)
        finally:
            state_manager.close()

        record = read_cache_metadata(cache_path)
        self.assertIsNotNone(record)
        self.assertEqual(SyncStatus.CONFLICT, record.sync_status)

    def test_reprobe_locations_syncs_conflict_xattr_with_database_outside_cache_root(self):
        db_path = self.temp_dir / "state.db"
        cache_path = self.cache_dir / "movie.mkv"
        cache_path.write_bytes(b"payload")
        metadata = self._metadata("/movie.mkv", cache_path, sync_status=SyncStatus.DIRTY)
        write_cache_metadata(cache_path, metadata)

        state_manager = StateManager(str(db_path))
        try:
            state_manager.set_file_metadata(metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie.mkv",
                    backend_name="cache",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )
        finally:
            state_manager.close()

        remote_dir = self.temp_dir / "remote-conflict"
        remote_dir.mkdir()
        (remote_dir / "movie.mkv").write_bytes(b"other-payload")

        result = self.runner.invoke(
            main,
            [
                "debug",
                "reprobe-locations",
                "--cache",
                str(self.cache_dir),
                "--database",
                str(db_path),
                "--member",
                f"local0={remote_dir}",
                "--json-output",
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        payload = json.loads(result.output)
        self.assertTrue(
            any(
                issue["kind"] == "member_checksum_mismatch"
                for issue in payload["issues"]
            )
        )

        state_manager = StateManager(str(db_path))
        try:
            refreshed = state_manager.get_file_metadata("/movie.mkv")
            self.assertIsNotNone(refreshed)
            self.assertEqual(SyncStatus.CONFLICT, refreshed.sync_status)
        finally:
            state_manager.close()

        record = read_cache_metadata(cache_path)
        self.assertIsNotNone(record)
        self.assertEqual(SyncStatus.CONFLICT, record.sync_status)

    def test_reprobe_locations_rejects_missing_database_path(self):
        remote_dir = self.temp_dir / "remote-missing-db"
        remote_dir.mkdir()
        missing_db = self.temp_dir / "missing.db"

        result = self.runner.invoke(
            main,
            [
                "debug",
                "reprobe-locations",
                "--cache",
                str(self.cache_dir),
                "--database",
                str(missing_db),
                "--member",
                f"local0={remote_dir}",
            ],
        )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("does not exist", result.output)

    def test_reprobe_locations_rejects_noncanonical_member_name_without_mutating_db(self):
        cache_path = self.cache_dir / "movie.mkv"
        cache_path.write_bytes(b"payload")
        metadata = self._metadata("/movie.mkv", cache_path, sync_status=SyncStatus.CLEAN)
        write_cache_metadata(cache_path, metadata)

        state_manager = StateManager(str(self.db_path))
        try:
            state_manager.set_file_metadata(metadata)
            state_manager.add_file_location(
                FileLocation(
                    virtual_path="/movie.mkv",
                    backend_name="cache",
                    checksum=metadata.checksum or "",
                    last_synced=metadata.last_modified,
                    is_cached=True,
                )
            )
        finally:
            state_manager.close()

        remote_dir = self.temp_dir / "remote-phantom-name"
        remote_dir.mkdir()
        (remote_dir / "movie.mkv").write_bytes(b"payload")

        result = self.runner.invoke(
            main,
            [
                "debug",
                "reprobe-locations",
                "--cache",
                str(self.cache_dir),
                "--database",
                str(self.db_path),
                "--member",
                f"local1={remote_dir}",
            ],
        )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("canonical mounted identity set", result.output)

        state_manager = StateManager(str(self.db_path))
        try:
            locations = state_manager.get_file_locations("/movie.mkv")
            self.assertEqual(["cache"], [location.backend_name for location in locations])
        finally:
            state_manager.close()
