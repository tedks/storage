"""
System regression tests for streaming reads on a mounted filesystem.

These tests exercise the FUSE mount path (open/read/seek/reopen) to ensure
streaming stays healthy under sustained reads and does not regress to the
"first chunk then failure" behavior.
"""

import errno
import hashlib
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

from storage.backends.local import LocalBackend
from storage.control import canonical_mountpoint
from storage.fs import StorageFS
from tests.fuse_mount import run_fuse, stop_fuse


# Only a backstop for a test that never releases a held read: the test and
# tearDown release it explicitly, so a slow host never sees the hold expire.
HELD_READ_SAFETY_TIMEOUT = 600.0


class SlowRemoteLoopbackBackend(LocalBackend):
    """Local backend test double flagged as remote with controllable read delay."""

    def __init__(self, name: str, root_path: str, range_delay: float = 0.003):
        super().__init__(name, root_path, speed_priority=50)
        self.is_remote = True
        self.range_delay = range_delay
        self.hold_past = None

    def release_held_reads(self):
        if self.hold_past is not None:
            self.hold_past[3].set()

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        if self.hold_past is not None:
            name, boundary, entered, resume = self.hold_past
            if virtual_path.lstrip("/") == name and offset >= boundary:
                entered.set()
                if not resume.wait(HELD_READ_SAFETY_TIMEOUT):
                    raise TimeoutError("test did not release the held range read")
        if self.range_delay:
            time.sleep(self.range_delay)
        return super().read_file(virtual_path, offset, size)


# A client process for reads through the mount: this process runs the FUSE
# loop, so it never reads mounted data itself (a blocked fault or read here
# could wait on a callback that needs this interpreter). One JSON line per
# command on stdin, one JSON line back.
_STREAM_CLIENT = """
import hashlib, json, os, sys
path = sys.argv[1]
fd = None
for line in sys.stdin:
    cmd = line.strip()
    if cmd == "open_read":
        fd = os.open(path, os.O_RDONLY)
        data = b""
        while True:
            chunk = os.read(fd, 1 << 16)
            if not chunk:
                break
            data += chunk
        out = {"sha": hashlib.sha256(data).hexdigest(), "size": len(data)}
    elif cmd == "stats":
        out = {"fstat": os.fstat(fd).st_mtime, "stat": os.stat(path).st_mtime}
    elif cmd == "reopen":
        os.close(fd)
        fd = os.open(path, os.O_RDONLY)
        data = b""
        while True:
            chunk = os.read(fd, 1 << 16)
            if not chunk:
                break
            data += chunk
        out = {"sha": hashlib.sha256(data).hexdigest()}
    else:
        break
    print(json.dumps(out), flush=True)
"""


# The same rule, for reads of byte ranges: one JSON command per line.
# {"op": "read", "path", "offset", "size"} opens, reads and closes;
# "open" holds a descriptor on path, "pread" reads through it, "close"
# closes it. Reads answer with the bytes' sha256 and length.
_RANGE_CLIENT = """
import hashlib, json, os, sys
held = None
def answer(data):
    return {"sha": hashlib.sha256(data).hexdigest(), "size": len(data)}
for line in sys.stdin:
    cmd = json.loads(line)
    op = cmd["op"]
    if op == "read":
        fd = os.open(cmd["path"], os.O_RDONLY)
        try:
            out = answer(os.pread(fd, cmd["size"], cmd["offset"]))
        finally:
            os.close(fd)
    elif op == "open":
        held = os.open(cmd["path"], os.O_RDONLY)
        out = {}
    elif op == "pread":
        out = answer(os.pread(held, cmd["size"], cmd["offset"]))
    elif op == "close":
        os.close(held)
        held = None
        out = {}
    else:
        break
    print(json.dumps(out), flush=True)
"""


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
            max_streaming_sessions=4,
            mountpoint=canonical_mountpoint(str(self.mount_dir)),
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
        # A held member read would otherwise keep its download, and so the
        # unmount, waiting for the safety timeout.
        self.backend.release_held_reads()
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

    def test_streaming_seek_and_reopen_during_prefetch(self):
        """
        Reproduce mpv-like access:
        - initial header read
        - forward seek read
        - sustained sequential reads
        - reopen while prefetch is active
        """
        mount_file = self.mount_dir / "movie.bin"

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

    def test_reader_opens_and_reopened_fill_overtakes_closed_downloads(self):
        manager = self.fs._streaming_manager
        manager.update_download_limits(max_streaming_sessions=2, max_download_workers=1)
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

        client = self._range_client()

        def call(op, **args):
            return self._client_call(client, json.dumps({"op": op, **args}), timeout=30.0)

        def digest(data):
            return {"sha": hashlib.sha256(data).hexdigest(), "size": len(data)}

        def mounted(name):
            return str(self.mount_dir / name)

        with patch.object(self.backend, "read_file", side_effect=read), \
                patch.object(self.backend, "copy_to_local", side_effect=copy):
            try:
                for name in ("older.bin", "reopened.bin"):
                    self.assertEqual(call("read", path=mounted(name), offset=0, size=256 * 1024),
                                     digest(payloads[name][:256 * 1024]))
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

                self.assertEqual(call("read", path=mounted("fresh.bin"), offset=0, size=4096),
                                 digest(payloads["fresh.bin"][:4096]))
                self.assertFalse(let_older_continue.is_set())
                self.assertEqual(self.fs._streaming_stats_snapshot()["streaming_fallback_opens"], 0)

                call("open", path=mounted("reopened.bin"))
                try:
                    reopened = next(session for session in manager.list_download_sessions()
                                    if session["path"] == "/reopened.bin")
                    self.assertEqual(reopened["id"], retained["/reopened.bin"])
                    self.assertEqual(reopened["readers"], 1)
                    let_older_continue.set()
                    self.assertTrue(turns_after_older.wait(timeout=5))
                    self.assertEqual(next_files, ["/reopened.bin"] * 4 + ["/older.bin"])
                    self.assertEqual(call("pread", offset=512 * 1024, size=4096),
                                     digest(payloads["reopened.bin"][512 * 1024:516 * 1024]))
                finally:
                    # Not after a failed call (_client_call killed the
                    # client): a BrokenPipeError would hide that failure.
                    if client.poll() is None:
                        call("close")
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

        try:
            with (self.mount_dir / name).open("rb") as handle:
                self.assertEqual(handle.read(512 * 1024), self.payload[:512 * 1024])
                self.assertTrue(entered.wait(4), "the background range reader must be held")

                status = command()
                self.assertEqual(status["stats"]["active_reader_sessions"], 1)
                self.assertNotIn(name, json.dumps(status, ensure_ascii=False))
                self.assertTrue(status["restart_only"]["streaming_reads"])
                self.assertEqual(status["mount"]["mountpoint"], canonical_mountpoint(str(self.mount_dir)))
                cache_entry = os.stat(self.cache_dir)
                self.assertEqual(status["mount"]["cache"], [cache_entry.st_dev, cache_entry.st_ino])
                row, = command("downloads")["downloads"]
                self.assertEqual(row["path"].lstrip("/"), name)
                self.assertEqual(row["readers"], 1)
                original = row["id"]

                outcome = command("cancel", original, expected=1)
                self.assertEqual(outcome, {"id": original, "result": "busy"})
                human = command("cancel", original, expected=1, json_output=False)
                self.assertIn("Close its readers or wait for finalization", human.stderr)
                self.assertEqual(handle.read(64), self.payload[512 * 1024:512 * 1024 + 64])

                limits = command("set", "--max-streaming-sessions", "1",
                                 "--max-retained-streaming-sessions", "3",
                                 "--max-download-workers", "1")["limits"]
                self.assertEqual(limits, {
                    "max_streaming_sessions": 1,
                    "max_retained_streaming_sessions": 3,
                    "max_download_workers": 1,
                })
                rejected = command("set", "--max-streaming-sessions", "2",
                                   "--max-download-workers", "33", expected=1, json_output=False)
                self.assertIn("max_download_workers", rejected.stderr)
                self.assertEqual(command("get")["limits"], limits)

                # A profile reaches this cache's socket only for its own mountpoint.
                config = Path(self.temp_dir) / "storage.toml"
                config.write_text("".join(f"""
[mounts.{profile}]
mountpoint = {json.dumps(str(mountpoint))}
cache = {json.dumps(str(self.cache_dir))}
backends = [{json.dumps(str(self.backend_dir))}]
""" for profile, mountpoint in (("this", self.mount_dir),
                                ("other", Path(self.temp_dir) / "elsewhere"))))
                by_profile = ["control", "--config", str(config), "--profile"]
                refused = self._run_control_cli(*by_profile, "other", "set",
                                                "--max-download-workers", "2", expected=1)
                self.assertIn("belongs to the mount at", refused.stderr)
                self.assertEqual(command("get")["limits"], limits)
                mine = json.loads(self._run_control_cli(
                    *by_profile, "this", "--json-output", "set", "--max-download-workers", "1",
                ).stdout)
                self.assertEqual(mine["limits"], limits)

                with (self.mount_dir / second).open("rb") as fallback:
                    self.assertEqual(fallback.read(16), self.payload[:16])
                self.assertEqual(command("status")["stats"]["streaming_fallback_opens"], 1)

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
                command("set", "--max-retained-streaming-sessions", "0")
            self.assertTrue(self._wait_for(lambda: not command("downloads")["downloads"]))
            self.assertFalse(partial.exists())
        finally:
            resume.set()


    def _range_client(self):
        """A _RANGE_CLIENT process, stopped at cleanup."""
        import subprocess
        import sys

        client = subprocess.Popen(
            [sys.executable, "-c", _RANGE_CLIENT],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        def stop():
            try:
                if client.poll() is None:
                    try:
                        client.stdin.close()
                    except OSError:
                        pass  # BrokenPipeError: it already exited
                    client.wait(timeout=30)
            except subprocess.TimeoutExpired:
                client.kill()
                client.wait(timeout=30)
            finally:
                for stream in (client.stdin, client.stdout, client.stderr):
                    try:
                        stream.close()
                    except OSError:
                        pass

        self.addCleanup(stop)
        return client

    def _client_call(self, client, command, timeout=60.0):
        client.stdin.write(command + "\n")
        client.stdin.flush()
        ready = threading.Event()
        result = {}

        def read_line():
            result["line"] = client.stdout.readline()
            ready.set()

        threading.Thread(target=read_line, daemon=True).start()
        if not ready.wait(timeout):
            client.kill()
            self.fail(f"mount client did not answer {command!r} in {timeout} s")
        if not result["line"]:
            client.wait(timeout=30)
            self.fail(f"mount client exited on {command!r}: {client.stderr.read()}")
        return json.loads(result["line"])

    def test_member_replaced_before_finalize_reports_only_member_mtimes(self):
        """A stream whose member is replaced before it finishes is not kept:
        a client holding the descriptor sees only genuine member mtimes, and
        a reopen reads the replacement with its own."""
        import hashlib
        import subprocess
        import sys
        from unittest.mock import patch

        old, new = b"o" * (256 * 1024), b"n" * (256 * 1024)
        old_mtime, new_mtime = 1_600_000_000, 1_650_000_000
        member = self.backend_dir / "swap.bin"
        member.write_bytes(old)
        os.utime(member, (old_mtime, old_mtime))
        # Written behind the mount after setUp listed the root: drop that
        # listing as the member's watcher would.
        self.fs._on_remote_change("/swap.bin")
        backend = self.fs.backends["remote0"]
        real_stat, calls = backend.stat, []

        def stat(path):
            if path == "/swap.bin":
                calls.append(path)
                if len(calls) == 2:  # the stream's confirmation: replaced first
                    member.write_bytes(new)
                    os.utime(member, (new_mtime, new_mtime))
            return real_stat(path)

        client = subprocess.Popen(
            [sys.executable, "-c", _STREAM_CLIENT, str(self.mount_dir / "swap.bin")],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            with patch.object(backend, "stat", side_effect=stat):
                first = self._client_call(client, "open_read")
                self.assertEqual(first["sha"], hashlib.sha256(old).hexdigest())
                self.assertTrue(self._wait_for(
                    lambda: self.fs._streaming_stats_snapshot().get(
                        "streaming_member_changed_discards") == 1, timeout=30.0))
            self.assertFalse((self.cache_dir / "swap.bin").exists())
            held = self._client_call(client, "stats")
            for name, mtime in held.items():
                self.assertIn(mtime, (old_mtime, new_mtime), name)
            again = self._client_call(client, "reopen")
            self.assertEqual(again["sha"], hashlib.sha256(new).hexdigest())
            # Once the replacement's stream is installed, the cached file
            # answers with the replacement's own mtime.
            self.assertTrue(self._wait_for(
                lambda: (self.cache_dir / "swap.bin").exists(), timeout=30.0))
            installed = self._client_call(client, "stats")
            self.assertEqual((installed["fstat"], installed["stat"]), (new_mtime, new_mtime))
        finally:
            try:
                client.stdin.close()
                client.wait(timeout=30)
            except Exception:
                client.kill()
                client.wait(timeout=30)
            finally:
                client.stdout.close()
                client.stderr.close()


class TestStreamingSystemRegressionThreaded(TestStreamingSystemRegression):
    fuse_threads = True


if __name__ == "__main__":
    unittest.main()
