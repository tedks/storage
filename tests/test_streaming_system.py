"""
System regression tests for streaming reads on a mounted filesystem.

These tests exercise the FUSE mount path (open/read/seek/reopen) to ensure
streaming stays healthy under sustained reads and does not regress to the
"first chunk then failure" behavior.
"""

import errno
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from pathlib import Path

from storage.backends.local import LocalBackend
from storage.fs import StorageFS
from tests.fuse_mount import run_fuse, stop_fuse


class SlowRemoteLoopbackBackend(LocalBackend):
    """Local backend test double flagged as remote with controllable read delay."""

    def __init__(self, name: str, root_path: str, range_delay: float = 0.003):
        super().__init__(name, root_path, speed_priority=50)
        self.is_remote = True
        self.range_delay = range_delay

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
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


class TestStreamingSystemRegression(unittest.TestCase):
    """Mounted-FUSE streaming regression checks."""

    # Also run with the multithreaded loop (subclass below).
    fuse_threads = False

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.mount_dir = Path(self.temp_dir) / "mount"
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.backend_dir = Path(self.temp_dir) / "backend"
        self.db_path = Path(self.temp_dir) / "state.db"

        for path in (self.mount_dir, self.cache_dir, self.backend_dir):
            path.mkdir(parents=True, exist_ok=True)

        payload_size = 12 * 1024 * 1024
        unit = b"abcdefghijklmnopqrstuvwxyz012345"
        repeats = (payload_size // len(unit)) + 1
        self.payload = (unit * repeats)[:payload_size]
        (self.backend_dir / "movie.bin").write_bytes(self.payload)

        backend = SlowRemoteLoopbackBackend("remote0", str(self.backend_dir), range_delay=0.003)
        self.fs = StorageFS(
            cache_root=self.cache_dir,
            state_db=self.db_path,
            backends={"remote0": backend},
            cache_size=128 * 1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
            streaming_reads=True,
            streaming_chunk_size=64 * 1024,
            max_streaming_sessions=4,
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
