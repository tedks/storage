"""
System regression tests for streaming reads on a mounted filesystem.

These tests exercise the FUSE mount path (open/read/seek/reopen) to ensure
streaming stays healthy under sustained reads and does not regress to the
"first chunk then failure" behavior.
"""

import errno
from contextlib import ExitStack
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from storage.backends.local import LocalBackend
from storage.cli import main
from storage.fs import StorageFS
from tests.fuse_mount import run_fuse, stop_fuse


class SlowRemoteLoopbackBackend(LocalBackend):
    """Local backend test double flagged as remote with controllable read delay."""

    def __init__(self, name: str, root_path: str, range_delay: float = 0.003):
        super().__init__(name, root_path, speed_priority=50)
        self.is_remote = True
        self.range_delay = range_delay
        self.hold_past = None

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        if self.hold_past is not None:
            name, boundary, entered, resume = self.hold_past
            if virtual_path.lstrip("/") == name and offset >= boundary:
                entered.set()
                if not resume.wait(20):
                    raise TimeoutError("test did not release the held range read")
        if self.range_delay:
            time.sleep(self.range_delay)
        return super().read_file(virtual_path, offset, size)


class TestStreamingSystemRegression(unittest.TestCase):
    """Mounted-FUSE streaming regression checks."""

    # Also run with the multithreaded loop (subclass below).
    fuse_threads = False

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.mount_dir = Path(self.temp_dir) / "mount"
        self.cache_dir = Path(self.temp_dir) / "cache 演奏"
        self.backend_dir = Path(self.temp_dir) / "backend"
        self.db_path = Path(self.temp_dir) / "state.db"

        for path in (self.mount_dir, self.cache_dir, self.backend_dir):
            path.mkdir(parents=True, exist_ok=True)

        block = os.statvfs(self.cache_dir).f_frsize
        (self.backend_dir / "warm.bin").write_bytes(b"w" * block)
        (self.backend_dir / "recipe.bin").write_bytes(b"r" * block)

        payload_size = 12 * 1024 * 1024
        unit = b"abcdefghijklmnopqrstuvwxyz012345"
        repeats = (payload_size // len(unit)) + 1
        self.payload = (unit * repeats)[:payload_size]
        (self.backend_dir / "movie.bin").write_bytes(self.payload)
        self.contended_payloads = {
            name: os.urandom(3 * 1024 * 1024)
            for name in ("older.bin", "reopened.bin", "fresh.bin")
        }
        for name, payload in self.contended_payloads.items():
            (self.backend_dir / name).write_bytes(payload)
        (self.backend_dir / "recording 演奏.bin").write_bytes(self.payload)
        (self.backend_dir / "second file.bin").write_bytes(self.payload)

        self.reader_payload = os.urandom(768 * 1024)
        for index in range(10):
            (self.backend_dir / f"reader-{index}.bin").write_bytes(self.reader_payload)

        profile = Path(self.temp_dir) / "legacy.toml"
        profile.write_text(
            '[mounts.legacy]\n'
            f'mountpoint = {json.dumps(str(self.mount_dir), ensure_ascii=False)}\n'
            f'cache = {json.dumps(str(self.cache_dir), ensure_ascii=False)}\n'
            f'backends = [{json.dumps(str(self.backend_dir), ensure_ascii=False)}]\n'
            'max_streaming_sessions = 8\n'
            'max_retained_streaming_sessions = 64\n',
        )
        with patch("storage.cli_mount.mount_storage") as mount:
            result = CliRunner().invoke(main, ["mount", "--config", str(profile), "legacy"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("max_streaming_sessions is obsolete and ignored", result.output)
        self.assertIn("max_retained_streaming_sessions is obsolete and ignored", result.output)
        self.assertNotIn("max_streaming_sessions", mount.call_args.kwargs)
        self.assertIsNone(mount.call_args.kwargs["max_download_workers"])

        self.backend = SlowRemoteLoopbackBackend("remote0", str(self.backend_dir), range_delay=0.003)
        self.fs = StorageFS(
            cache_root=self.cache_dir,
            state_db=self.db_path,
            backends={"remote0": self.backend},
            cache_size=128 * 1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
            streaming_reads=True,
            streaming_chunk_size=64 * 1024,
            max_download_workers=mount.call_args.kwargs["max_download_workers"],
        )

        self.mount_thread = threading.Thread(
            target=run_fuse, args=(self.fs, self.mount_dir),
            kwargs={"fuse_threads": self.fuse_threads}, daemon=True,
        )
        self.mount_thread.start()
        time.sleep(1.0)

        try:
            os.listdir(self.mount_dir)
        except Exception as exc:
            self.skipTest(f"Mount unavailable in this environment: {exc}")

    def tearDown(self):
        # After the FUSE thread has finished, so this destroy never runs
        # beside libfuse's own (or a request still in flight).
        problems = stop_fuse(self.mount_dir, self.mount_thread, fs=self.fs)
        if os.path.ismount(self.mount_dir) or self.mount_thread.is_alive():
            # Still serving (a lazy unmount leaves it running): removing the
            # cache or destroying under it would race its requests.
            self.fail(f"mount teardown, {self.temp_dir} left in place: {problems}")
        try:
            self.fs.destroy("/")
        except Exception:
            pass

        shutil.rmtree(self.temp_dir, ignore_errors=True)
        if problems:
            self.fail(f"mount teardown: {problems}")

    def _wait_for(self, predicate, timeout: float = 8.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.05)
        return predicate()

    def _read_stream_state(self, cache_path: Path, partial_path: Path):
        probe = partial_path if partial_path.exists() else cache_path
        if not probe.exists():
            return None

        try:
            raw = os.getxattr(str(probe), self.fs._STREAM_STATE_XATTR)
        except OSError as exc:
            if exc.errno in (
                getattr(errno, "EOPNOTSUPP", -1),
                getattr(errno, "ENOTSUP", -1),
                getattr(errno, "ENODATA", -1),
            ):
                return None
            raise

        return json.loads(raw.decode("utf-8"))

    def test_cache_pressure_waits_for_data_and_evicts_the_least_recent_copy(self):
        manager = self.fs.cache_manager
        block = os.statvfs(self.cache_dir).f_frsize
        manager.max_size_bytes = 3 * block
        warm = self.cache_dir / "warm.bin"
        recipe = self.cache_dir / "recipe.bin"
        ordinary_bytes, recipe_bytes = b"w" * block, b"r" * block
        for name, content in ((warm.name, ordinary_bytes), (recipe.name, recipe_bytes)):
            with open(self.mount_dir / name, "rb", buffering=0) as source:
                self.assertEqual(source.read(), content)
            self.assertTrue(self._wait_for(lambda: self.fs.state_manager.get_file_metadata("/" + name)))
        row = self.fs.state_manager.get_file_metadata("/recipe.bin")
        row.recomputable = True
        row.recompute_cmd = "true"
        manager.store_file_metadata(row)
        # The second file was older after this actual open/read, despite
        # having been cached later and being a different eviction type.
        with open(self.mount_dir / warm.name, "rb", buffering=0) as source:
            self.assertEqual(source.read(), ordinary_bytes)

        reader = os.open(self.mount_dir / "movie.bin", os.O_RDONLY)
        try:
            os.posix_fadvise(reader, 0, 0, os.POSIX_FADV_RANDOM)
            self.assertEqual(manager.get_total_cache_size(), 2 * block)
            self.assertTrue(warm.exists() and recipe.exists())
            far = 256 * block
            self.assertEqual(os.pread(reader, block, far), self.payload[far:far + block])
            self.assertEqual(manager.get_total_cache_size(), 3 * block)
            self.assertTrue(warm.exists() and recipe.exists())
            self.assertEqual(os.pread(reader, block, 0), self.payload[:block])
            self.assertEqual(manager.get_total_cache_size(), 3 * block)
            self.assertTrue(warm.exists())
            self.assertFalse(recipe.exists())
        finally:
            os.close(reader)

        with open(self.mount_dir / recipe.name, "rb", buffering=0) as source:
            self.assertEqual(source.read(), recipe_bytes)
        self.assertEqual(self.fs.state_manager.get_file_metadata("/recipe.bin").recompute_cmd, "true")

    def test_full_cache_with_only_open_dirty_bytes_still_serves_cold_reads(self):
        manager = self.fs.cache_manager
        block = os.statvfs(self.cache_dir).f_frsize
        manager.max_size_bytes = block
        dirty = os.open(self.mount_dir / "local-only.bin", os.O_CREAT | os.O_WRONLY, 0o644)
        try:
            self.assertEqual(os.write(dirty, b"d" * block), block)
            reader = os.open(self.mount_dir / "movie.bin", os.O_RDONLY)
            try:
                os.posix_fadvise(reader, 0, 0, os.POSIX_FADV_RANDOM)
                offset = 256 * block
                self.assertEqual(os.pread(reader, block, offset), self.payload[offset:offset + block])
                self.assertEqual(os.pread(reader, block, 0), self.payload[:block])
                self.assertEqual(manager.get_total_cache_size(), block)
                self.assertFalse((self.cache_dir / "movie.bin").exists())
                self.assertEqual((self.cache_dir / "local-only.bin").read_bytes(), b"d" * block)
            finally:
                os.close(reader)
        finally:
            os.close(dirty)

    def test_streaming_seek_and_reopen_during_prefetch(self):
        """
        Reproduce mpv-like access:
        - initial header read
        - forward seek read
        - sustained sequential reads
        - reopen while prefetch is active
        """
        mount_file = self.mount_dir / "movie.bin"
        self.fs._streaming_manager.update_download_limits(streaming_keep_after_close="256K,50%")

        fd = os.open(str(mount_file), os.O_RDONLY)
        try:
            head = os.pread(fd, 256 * 1024, 0)
            self.assertEqual(head, self.payload[: len(head)])

            seek_offset = 4 * 1024 * 1024
            jumped = os.pread(fd, 256 * 1024, seek_offset)
            self.assertEqual(jumped, self.payload[seek_offset : seek_offset + len(jumped)])

            # Allow background prefetch to run while reader is open.
            time.sleep(0.4)

            cursor = seek_offset + len(jumped)
            for _ in range(64):
                chunk = os.pread(fd, 128 * 1024, cursor)
                if not chunk:
                    break
                self.assertEqual(chunk, self.payload[cursor : cursor + len(chunk)])
                cursor += len(chunk)
        finally:
            os.close(fd)

        stats = self.fs._streaming_stats_snapshot()
        self.assertGreater(stats["foreground_fetches"], 0)
        self.assertGreater(stats["background_fetches"], 0)
        self.assertEqual(stats["streaming_fallback_opens"], 0)

        pre_reopen_stats = self.fs._streaming_stats_snapshot()
        fd2 = os.open(str(mount_file), os.O_RDONLY)
        try:
            reopen_head = os.pread(fd2, 128 * 1024, 0)
        finally:
            os.close(fd2)
        post_reopen_stats = self.fs._streaming_stats_snapshot()

        self.assertEqual(reopen_head, self.payload[: len(reopen_head)])
        self.assertEqual(post_reopen_stats["foreground_fetches"], pre_reopen_stats["foreground_fetches"])
        self.assertEqual(post_reopen_stats["foreground_remote_bytes"], pre_reopen_stats["foreground_remote_bytes"])

        cache_path = self.cache_dir / "movie.bin"
        partial_path = self.cache_dir / "movie.bin.storage.partial"
        self.assertTrue(self._wait_for(lambda: cache_path.exists() or partial_path.exists(), timeout=8.0))

        stream_state = self._read_stream_state(cache_path, partial_path)
        if stream_state is not None:
            self.assertNotIn(stream_state.get("status"), {"error", "failed"})

    def test_legacy_eight_slots_do_not_limit_open_readers_or_preempt_a_transfer(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_download_workers=1)
        blocked = threading.Event()
        resume = threading.Event()
        self.backend.hold_past = ("reader-0.bin", 256 * 1024, blocked, resume)
        with patch.object(self.backend, "background_range_capacity", return_value=1), \
                patch.object(self.backend, "copy_to_local") as copies, ExitStack() as handles:
            try:
                first = handles.enter_context(open(self.mount_dir / "reader-0.bin", "rb", buffering=0))
                self.assertEqual(first.read(128 * 1024), self.reader_payload[:128 * 1024])
                self.assertTrue(blocked.wait(timeout=5), "first speculative transfer never reached barrier")
                for index in range(1, 10):
                    other = handles.enter_context(open(self.mount_dir / f"reader-{index}.bin", "rb", buffering=0))
                    self.assertEqual(os.pread(other.fileno(), 4096, 512 * 1024),
                                     self.reader_payload[512 * 1024:516 * 1024])
                stats = self.fs._streaming_stats_snapshot()
                self.assertEqual(stats["active_reader_sessions"], 10)
                self.assertEqual(stats["download_workers_running"], 1)
                self.assertEqual(stats["streaming_fallback_opens"], 0)
                copies.assert_not_called()
            finally:
                resume.set()

    def test_staging_enospc_fails_open_and_leaves_the_next_attempt_clean(self):
        path = self.mount_dir / "reader-0.bin"
        real_truncate = os.ftruncate

        def refuse_staging(fd, size):
            if Path(os.readlink(f"/proc/self/fd/{fd}")).name.startswith(".storage-staging-"):
                raise OSError(errno.ENOSPC, "synthetic disk allocation failure")
            return real_truncate(fd, size)

        with patch("storage.streaming.os.ftruncate", side_effect=refuse_staging), \
                patch.object(self.backend, "copy_to_local") as copies:
            with self.assertRaises(OSError) as error:
                path.open("rb")
            self.assertEqual(error.exception.errno, errno.ENOSPC)
            copies.assert_not_called()
        self.assertEqual(list(self.cache_dir.glob("*.storage.partial")), [])
        self.assertEqual(self.fs._streaming_stats_snapshot()["streaming_open_error_cache_space"], 1)
        with path.open("rb", buffering=0) as handle:
            self.assertEqual(handle.read(4096), self.reader_payload[:4096])

    def test_reader_opens_and_reopened_fill_overtakes_closed_downloads(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(
            max_download_workers=1, streaming_keep_after_close="256K,50%",
        )
        payloads = self.contended_payloads

        older_blocked = threading.Event()
        let_older_continue = threading.Event()
        turns_after_older = threading.Event()
        next_files = []
        real_read = self.backend.read_file
        real_copy = self.backend.copy_to_local

        def read(path, offset, size):
            if threading.current_thread().name == "storage-prefetch-worker":
                if path == "/older.bin" and offset >= 512 * 1024 and not older_blocked.is_set():
                    older_blocked.set()
                    if not let_older_continue.wait(timeout=15):
                        raise TimeoutError("test never released the older fill")
                elif older_blocked.is_set() and not turns_after_older.is_set():
                    next_files.append(path)
                    if len(next_files) == 5:
                        turns_after_older.set()
            return real_read(path, offset, size)

        def copy(path, target):
            if path == "/fresh.bin":
                raise AssertionError("fresh reader took the synchronous full-copy fallback")
            return real_copy(path, target)

        with patch.object(self.backend, "read_file", side_effect=read), \
                patch.object(self.backend, "copy_to_local", side_effect=copy):
            try:
                for name in ("older.bin", "reopened.bin"):
                    with open(self.mount_dir / name, "rb", buffering=0) as handle:
                        self.assertEqual(handle.read(256 * 1024), payloads[name][:256 * 1024])
                    self.assertTrue(self._wait_for(lambda: any(
                        session["path"] == "/" + name and session["readers"] == 0
                        for session in manager.list_download_sessions()
                    )))
                self.assertTrue(older_blocked.wait(timeout=5))
                retained = {session["path"]: session["id"] for session in manager.list_download_sessions()}
                self.assertEqual(set(retained), {"/older.bin", "/reopened.bin"})
                before = self.fs._streaming_stats_snapshot()
                self.assertEqual(before["download_workers_running"], 1)
                self.assertEqual(before["retained_streaming_sessions"], 2)

                with open(self.mount_dir / "fresh.bin", "rb", buffering=0) as handle:
                    self.assertEqual(handle.read(4096), payloads["fresh.bin"][:4096])
                    self.assertFalse(let_older_continue.is_set())
                self.assertEqual(self.fs._streaming_stats_snapshot()["streaming_fallback_opens"], 0)

                with open(self.mount_dir / "reopened.bin", "rb", buffering=0) as handle:
                    reopened = next(session for session in manager.list_download_sessions()
                                    if session["path"] == "/reopened.bin")
                    self.assertEqual(reopened["id"], retained["/reopened.bin"])
                    self.assertEqual(reopened["readers"], 1)
                    let_older_continue.set()
                    self.assertTrue(turns_after_older.wait(timeout=5))
                    self.assertEqual(next_files, ["/reopened.bin"] * 4 + ["/older.bin"])
                    self.assertEqual(os.pread(handle.fileno(), 4096, 512 * 1024),
                                     payloads["reopened.bin"][512 * 1024:516 * 1024])
            finally:
                let_older_continue.set()

        for name in ("older.bin", "reopened.bin"):
            cached = self.cache_dir / name
            self.assertTrue(self._wait_for(cached.exists))
            self.assertEqual(cached.read_bytes(), payloads[name])

    def _run_control_cli(self, *args, expected=0):
        cli = Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"] / "storage_cli"
        result = subprocess.run([str(cli), *args], check=False, capture_output=True,
                                text=True, timeout=8)
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        return result

    def test_live_mount_control(self):
        self.assertTrue(os.path.ismount(self.mount_dir), "requires a live scratch FUSE mount")
        name = "recording 演奏.bin"
        second = "second file.bin"
        partial = self.cache_dir / f"{name}.storage.partial"
        entered = threading.Event()
        resume = threading.Event()
        self.backend.hold_past = (name, 1024 * 1024, entered, resume)

        def command(*args, expected=0, json_output=True):
            options = ["control", "--cache", str(self.cache_dir)]
            if json_output:
                options += ["--json-output"]
            result = self._run_control_cli(*options, *args, expected=expected)
            return json.loads(result.stdout) if json_output else result

        command("set", "--streaming-keep-after-close", "256K,50%")
        try:
            with (self.mount_dir / name).open("rb") as handle:
                self.assertEqual(handle.read(512 * 1024), self.payload[:512 * 1024])
                self.assertTrue(entered.wait(4), "the background range reader must be held")

                status = command()
                self.assertEqual(status["stats"]["active_reader_sessions"], 1)
                self.assertNotIn(name, json.dumps(status, ensure_ascii=False))
                self.assertTrue(status["restart_only"]["streaming_reads"])
                row, = command("downloads")["downloads"]
                self.assertEqual(row["path"].lstrip("/"), name)
                self.assertEqual(row["readers"], 1)
                original = row["id"]

                outcome = command("cancel", original, expected=1)
                self.assertEqual(outcome, {"id": original, "result": "busy"})
                human = command("cancel", original, expected=1, json_output=False)
                self.assertIn("Close its readers or wait for finalization", human.stderr)
                self.assertEqual(handle.read(64), self.payload[512 * 1024:512 * 1024 + 64])

                limits = command("set", "--max-download-workers", "1")["limits"]
                self.assertEqual(limits, {
                    "max_download_workers": 1,
                    "streaming_keep_after_close": {"mode": "any", "bytes": [256 * 1024], "percent": ["50"]},
                    "streaming_read_ahead_window": 4 * 1024 ** 2,
                })
                rejected = command("set", "--max-download-workers", "0", expected=2, json_output=False)
                self.assertIn("--max-download-workers", rejected.stderr)
                self.assertEqual(command("get")["limits"], limits)

                with (self.mount_dir / second).open("rb") as reader:
                    self.assertEqual(reader.read(16), self.payload[:16])
                self.assertEqual(command("status")["stats"]["streaming_fallback_opens"], 0)
                self.assertIsNone(command("set", "--max-download-workers", "auto")["limits"]["max_download_workers"])

                legacy = json.loads(self._run_control_cli(
                    "debug", "streaming-stats", "--cache", str(self.cache_dir), "--json-output",
                ).stdout)
                self.assertEqual(legacy["pid"], status["stats"]["pid"])
                self.assertEqual(legacy["active_streaming_sessions"], 1)
            self.assertTrue(self._wait_for(
                lambda: command("status")["stats"]["retained_streaming_sessions"] == 1,
            ))
            self.assertTrue(partial.exists())
            self.assertEqual(command("cancel", original), {"id": original, "result": "cancelled"})
            self.assertEqual(command("downloads")["downloads"], [])
            self.assertFalse(partial.exists())

            with (self.mount_dir / name).open("rb") as replacement:
                self.assertEqual(replacement.read(512 * 1024), self.payload[:512 * 1024])
                row, = command("downloads")["downloads"]
                self.assertNotEqual(row["id"], original)
                self.assertEqual(command("cancel", original, expected=1)["result"], "not-found")
                self.assertEqual(command("downloads")["downloads"][0]["id"], row["id"])
            self.assertTrue(self._wait_for(lambda: command("status")["stats"]["retained_streaming_sessions"] == 1))
            self.assertEqual(command("cancel", row["id"])["result"], "cancelled")
            self.assertEqual(command("downloads")["downloads"], [])
            self.assertFalse(partial.exists())
        finally:
            resume.set()

    def test_live_qualification_and_window_on_an_open_file(self):
        self.assertTrue(os.path.ismount(self.mount_dir), "requires a live scratch FUSE mount")
        mib = 1024 ** 2
        name = "movie.bin"
        entered, resume = threading.Event(), threading.Event()
        self.backend.hold_past = (name, 6 * mib, entered, resume)

        def command(*args):
            return json.loads(self._run_control_cli(
                "control", "--cache", str(self.cache_dir), "--json-output", *args,
            ).stdout)

        defaults = command("get")["limits"]
        self.assertEqual(defaults["streaming_keep_after_close"],
                         {"mode": "all", "bytes": [32 * mib], "percent": ["30"]})
        self.assertEqual(defaults["streaming_read_ahead_window"], 4 * mib)

        try:
            with (self.mount_dir / name).open("rb", buffering=0) as handle:
                self.assertEqual(handle.read(mib), self.payload[:mib])

                def initial_window_filled():
                    row, = command("downloads")["downloads"]
                    return row["downloaded_bytes"] >= 5 * mib

                self.assertTrue(self._wait_for(initial_window_filled))
                self.assertTrue(self._wait_for(
                    lambda: command("status")["stats"]["download_workers_running"] == 0,
                ))
                self.assertFalse(entered.is_set(), "default read-ahead reached 6 MiB after a 1 MiB read")

                rejected = self._run_control_cli(
                    "control", "--cache", str(self.cache_dir), "set",
                    "--streaming-keep-after-close", "all:1M,5%",
                    "--streaming-read-ahead-window", "-1M", expected=1,
                )
                self.assertIn("streaming_read_ahead_window", rejected.stderr)
                self.assertEqual(command("get")["limits"], defaults)

                updated = command("set", "--streaming-keep-after-close", "all:1M,5%",
                                  "--streaming-read-ahead-window", "0")["limits"]
                self.assertEqual(updated["streaming_keep_after_close"],
                                 {"mode": "all", "bytes": [mib], "percent": ["5"]})
                self.assertEqual(updated["streaming_read_ahead_window"], 0)
                self.assertTrue(entered.wait(5), "existing read span should now qualify")
                command("set", "--streaming-keep-after-close", "100%")

            self.assertTrue(self._wait_for(
                lambda: command("status")["stats"]["retained_streaming_sessions"] == 1,
            ))
        finally:
            resume.set()
        cache_path = self.cache_dir / name
        self.assertTrue(self._wait_for(cache_path.exists))
        self.assertEqual(cache_path.read_bytes(), self.payload)


class TestStreamingSystemRegressionThreaded(TestStreamingSystemRegression):
    fuse_threads = True


if __name__ == "__main__":
    unittest.main()
