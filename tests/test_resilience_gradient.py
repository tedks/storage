import errno
import json
import os
import random
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fuse import FuseOSError

from storage.backends.local import LocalBackend
from storage.fs import StorageFS


class FaultInjectRemoteBackend(LocalBackend):
    """Local backend test double that behaves like a remote backend with faults."""

    def __init__(self, name: str, root_path: str):
        super().__init__(name, root_path, speed_priority=50)
        self.is_remote = True
        self.fail_after_offset = None
        self.read_delay = 0.0
        self.stat_delay = 0.0

    def _raise_if_unavailable(self):
        if not self.is_available():
            raise ConnectionError(f"backend {self.name} unavailable")

    def exists(self, virtual_path: str) -> bool:
        self._raise_if_unavailable()
        return super().exists(virtual_path)

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        self._raise_if_unavailable()
        if self.fail_after_offset is not None and offset >= self.fail_after_offset:
            raise OSError(errno.EIO, "simulated range-read failure")
        if self.read_delay > 0:
            time.sleep(self.read_delay)
        return super().read_file(virtual_path, offset, size)

    def get_size(self, virtual_path: str) -> int:
        self._raise_if_unavailable()
        return super().get_size(virtual_path)

    def stat(self, virtual_path: str):
        self._raise_if_unavailable()
        if self.stat_delay > 0:
            time.sleep(self.stat_delay)
        return super().stat(virtual_path)

    def listdir(self, virtual_path: str):
        self._raise_if_unavailable()
        return super().listdir(virtual_path)

    def listdir_attrs(self, virtual_path: str):
        self._raise_if_unavailable()
        return super().listdir_attrs(virtual_path)

    def copy_to_local(self, virtual_path: str, local_path: Path):
        self._raise_if_unavailable()
        return super().copy_to_local(virtual_path, local_path)


class TestResilienceGradient(unittest.TestCase):
    @staticmethod
    def _payload(size: int) -> bytes:
        unit = b"abcdefghijklmnopqrstuvwxyz012345"
        repeats = (size // len(unit)) + 1
        return (unit * repeats)[:size]

    @staticmethod
    def _wait_for(predicate, timeout=4.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.02)
        return predicate()

    def _build_runtime(self):
        temp_dir = Path(tempfile.mkdtemp())
        cache_dir = temp_dir / "cache"
        backend_dir = temp_dir / "backend"
        db_path = temp_dir / "state.db"

        cache_dir.mkdir(parents=True, exist_ok=True)
        backend_dir.mkdir(parents=True, exist_ok=True)

        backend = FaultInjectRemoteBackend("remote0", str(backend_dir))
        fs = StorageFS(
            cache_root=cache_dir,
            state_db=db_path,
            backends={"remote0": backend},
            cache_size=64 * 1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
            streaming_reads=True,
            streaming_chunk_size=32 * 1024,
            max_streaming_sessions=4,
        )

        return SimpleNamespace(
            temp_dir=temp_dir,
            cache_dir=cache_dir,
            backend_dir=backend_dir,
            db_path=db_path,
            backend=backend,
            fs=fs,
        )

    @staticmethod
    def _destroy_runtime(runtime):
        try:
            runtime.fs.destroy("/")
        except Exception:
            pass
        shutil.rmtree(runtime.temp_dir, ignore_errors=True)

    def setUp(self):
        self.rt = self._build_runtime()

    def tearDown(self):
        self._destroy_runtime(self.rt)

    def _write_remote_file(self, path: str, size: int, runtime=None) -> bytes:
        runtime = runtime or self.rt
        payload = self._payload(size)
        target = runtime.backend_dir / path.lstrip("/")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        return payload

    def _cached_file_prefix(self, path: str, runtime=None) -> bytes:
        runtime = runtime or self.rt
        cache_path = runtime.cache_dir / path.lstrip("/")
        if not cache_path.exists():
            return b""
        return cache_path.read_bytes()

    def _prepare_state(self, runtime, state: str, path: str):
        if state == "uncached":
            return

        if state == "full":
            self.assertTrue(runtime.fs._ensure_file_in_cache(runtime.fs._resolve_path(path)))
            return

        if state == "partial":
            runtime.backend.fail_after_offset = 64 * 1024
            fd = runtime.fs.open(path, os.O_RDONLY)
            _ = runtime.fs.read(path, 4096, 0, fd)
            with self.assertRaises(FuseOSError):
                _ = runtime.fs.read(path, 4096, 96 * 1024, fd)
            runtime.fs.release(path, fd)
            runtime.backend.fail_after_offset = None

            cache_path = runtime.cache_dir / path.lstrip("/")
            self.assertTrue(self._wait_for(lambda: cache_path.exists(), timeout=5.0))
            return

        raise ValueError(f"unknown state {state}")

    def test_invariant_delivered_bytes_survive_disconnect(self):
        payload = self._write_remote_file("/survive.bin", 192 * 1024)
        self.rt.backend.fail_after_offset = 64 * 1024

        fd = self.rt.fs.open("/survive.bin", os.O_RDONLY)
        head = self.rt.fs.read("/survive.bin", 4096, 0, fd)
        self.assertEqual(head, payload[:4096])
        with self.assertRaises(FuseOSError):
            self.rt.fs.read("/survive.bin", 4096, 96 * 1024, fd)
        self.rt.fs.release("/survive.bin", fd)
        # Release does not wait for the worker; its tail promotes the prefix.
        self.assertTrue(self._wait_for(lambda: len(self._cached_file_prefix("/survive.bin")) > 0))

        self.rt.backend.set_available(False)
        retry_fd = self.rt.fs.open("/survive.bin", os.O_RDONLY)
        retry_head = self.rt.fs.read("/survive.bin", 4096, 0, retry_fd)
        self.rt.fs.release("/survive.bin", retry_fd)

        self.assertEqual(retry_head, payload[:4096])
        self.assertGreater(len(self._cached_file_prefix("/survive.bin")), 0)

    def test_invariant_cached_open_never_requires_remote(self):
        payload = self._write_remote_file("/cached.bin", 64 * 1024)
        self.assertTrue(self.rt.fs._ensure_file_in_cache(self.rt.fs._resolve_path("/cached.bin")))

        self.rt.backend.set_available(False)
        with patch.object(self.rt.backend, "exists", side_effect=AssertionError("remote exists() must not be called")):
            with patch.object(self.rt.backend, "copy_to_local", side_effect=AssertionError("remote copy must not be called")):
                fd = self.rt.fs.open("/cached.bin", os.O_RDONLY)
                data = self.rt.fs.read("/cached.bin", 4096, 0, fd)
                self.rt.fs.release("/cached.bin", fd)

        self.assertEqual(data, payload[:4096])

    def test_invariant_disconnect_does_not_delete_cache_file(self):
        payload = self._write_remote_file("/retain.bin", 160 * 1024)
        self.rt.backend.fail_after_offset = 64 * 1024

        fd = self.rt.fs.open("/retain.bin", os.O_RDONLY)
        self.assertEqual(self.rt.fs.read("/retain.bin", 4096, 0, fd), payload[:4096])
        with self.assertRaises(FuseOSError):
            self.rt.fs.read("/retain.bin", 4096, 96 * 1024, fd)
        self.rt.fs.release("/retain.bin", fd)

        cache_path = self.rt.cache_dir / "retain.bin"
        partial_path = self.rt.cache_dir / "retain.bin.storage.partial"
        self.assertTrue(self._wait_for(lambda: cache_path.exists(), timeout=5.0))
        self.assertFalse(partial_path.exists())

    def test_invariant_cache_state_xattr_matches_filesystem(self):
        _ = self._write_remote_file("/xattr.bin", 128 * 1024)
        self.rt.backend.fail_after_offset = 64 * 1024

        fd = self.rt.fs.open("/xattr.bin", os.O_RDONLY)
        _ = self.rt.fs.read("/xattr.bin", 4096, 0, fd)
        with self.assertRaises(FuseOSError):
            self.rt.fs.read("/xattr.bin", 4096, 96 * 1024, fd)
        self.rt.fs.release("/xattr.bin", fd)

        raw = self.rt.fs.getxattr("/xattr.bin", StorageFS.CACHE_STATE_XATTR)
        state = json.loads(raw.decode("utf-8"))

        cache_path = self.rt.cache_dir / "xattr.bin"
        partial_path = self.rt.cache_dir / "xattr.bin.storage.partial"
        self.assertEqual(state["cache_present"], cache_path.exists())
        self.assertEqual(state["partial_present"], partial_path.exists())
        self.assertEqual(state["cache_size"], cache_path.stat().st_size if cache_path.exists() else 0)

    def test_transition_matrix_disconnect_paths(self):
        path = "/matrix.bin"
        scenarios = [
            ("uncached", "offline_before_open"),
            ("uncached", "offline_after_open_before_read"),
            ("uncached", "offline_mid_read"),
            ("full", "offline_before_open"),
            ("full", "offline_after_open_before_read"),
            ("full", "offline_mid_read"),
            ("partial", "offline_before_open"),
            ("partial", "offline_after_open_before_read"),
            ("partial", "offline_mid_read"),
        ]

        for state, event in scenarios:
            with self.subTest(state=state, event=event):
                runtime = self._build_runtime()
                try:
                    payload = self._write_remote_file(path, 192 * 1024, runtime=runtime)
                    self._prepare_state(runtime, state, path)

                    if event == "offline_before_open":
                        runtime.backend.set_available(False)

                    fd = None
                    try:
                        if event == "offline_before_open":
                            if state == "uncached":
                                with self.assertRaises(FuseOSError) as ctx:
                                    runtime.fs.open(path, os.O_RDONLY)
                                self.assertEqual(ctx.exception.errno, errno.EHOSTUNREACH)
                                continue

                            fd = runtime.fs.open(path, os.O_RDONLY)
                            got = runtime.fs.read(path, 4096, 0, fd)
                            self.assertEqual(got, payload[:4096])
                            continue

                        fd = runtime.fs.open(path, os.O_RDONLY)

                        if event == "offline_after_open_before_read":
                            runtime.backend.set_available(False)
                            if state == "uncached":
                                with self.assertRaises(FuseOSError):
                                    _ = runtime.fs.read(path, 4096, 0, fd)
                            else:
                                got = runtime.fs.read(path, 4096, 0, fd)
                                self.assertEqual(got, payload[:4096])
                            continue

                        if event == "offline_mid_read":
                            got = runtime.fs.read(path, 4096, 0, fd)
                            self.assertEqual(got, payload[:4096])
                            runtime.backend.set_available(False)
                            if state == "uncached":
                                try:
                                    got2 = runtime.fs.read(path, 4096, 96 * 1024, fd)
                                except FuseOSError as exc:
                                    self.assertIn(exc.errno, {errno.EIO, errno.EHOSTUNREACH})
                                else:
                                    # Streaming prefetch may already have filled
                                    # this later range before the backend drops.
                                    self.assertEqual(
                                        got2,
                                        payload[96 * 1024:96 * 1024 + 4096],
                                    )
                            else:
                                got2 = runtime.fs.read(path, 4096, 0, fd)
                                self.assertEqual(got2, payload[:4096])
                    finally:
                        if fd is not None:
                            try:
                                runtime.fs.release(path, fd)
                            except Exception:
                                pass

                    runtime.backend.set_available(False)
                    cache_path = runtime.cache_dir / path.lstrip("/")
                    if cache_path.exists() and cache_path.stat().st_size > 0:
                        retry_fd = runtime.fs.open(path, os.O_RDONLY)
                        try:
                            retry = runtime.fs.read(path, 4096, 0, retry_fd)
                            self.assertEqual(retry, payload[:4096])
                        finally:
                            runtime.fs.release(path, retry_fd)
                finally:
                    self._destroy_runtime(runtime)

    def test_seeded_stateful_fault_sequence_preserves_invariants(self):
        seed = 20260311
        rng = random.Random(seed)
        path = "/stateful.bin"
        payload = self._write_remote_file(path, 512 * 1024)

        seen_prefix = 0

        for _ in range(80):
            op = rng.choice(["read_head", "seek_read", "disconnect", "reconnect", "set_fault"])

            if op == "disconnect":
                self.rt.backend.set_available(False)
            elif op == "reconnect":
                self.rt.backend.set_available(True)
                self.rt.backend.fail_after_offset = None
            elif op == "set_fault":
                self.rt.backend.fail_after_offset = rng.choice([None, 64 * 1024, 96 * 1024, 128 * 1024])
            else:
                fd = None
                try:
                    fd = self.rt.fs.open(path, os.O_RDONLY)
                    if op == "read_head":
                        size = rng.choice([1024, 4096, 8192])
                        data = self.rt.fs.read(path, size, 0, fd)
                        if data:
                            self.assertEqual(data, payload[: len(data)])
                            seen_prefix = max(seen_prefix, len(data))
                    else:
                        offset = rng.randrange(0, len(payload), 4096)
                        size = rng.choice([1024, 4096, 8192])
                        data = self.rt.fs.read(path, size, offset, fd)
                        if data:
                            self.assertEqual(data, payload[offset:offset + len(data)])
                except FuseOSError:
                    pass
                finally:
                    if fd is not None:
                        try:
                            self.rt.fs.release(path, fd)
                        except Exception:
                            pass

            cache_path = self.rt.cache_dir / path.lstrip("/")
            if not self.rt.backend.is_available() and seen_prefix > 0 and cache_path.exists():
                retry_fd = self.rt.fs.open(path, os.O_RDONLY)
                try:
                    retry = self.rt.fs.read(path, min(seen_prefix, 4096), 0, retry_fd)
                finally:
                    self.rt.fs.release(path, retry_fd)
                self.assertEqual(retry, payload[: len(retry)], msg=f"seed={seed}")

    def test_offline_cached_open_read_is_fast(self):
        payload = self._write_remote_file("/fast.bin", 64 * 1024)
        self.assertTrue(self.rt.fs._ensure_file_in_cache(self.rt.fs._resolve_path("/fast.bin")))

        self.rt.backend.set_available(False)
        started = time.perf_counter()
        for _ in range(40):
            fd = self.rt.fs.open("/fast.bin", os.O_RDONLY)
            data = self.rt.fs.read("/fast.bin", 1024, 0, fd)
            self.rt.fs.release("/fast.bin", fd)
            self.assertEqual(data, payload[:1024])
        elapsed = time.perf_counter() - started

        self.assertLess(elapsed, 2.0)

    def test_offline_getattr_fails_fast_under_fanout(self):
        self.rt.backend.stat_delay = 0.05
        self.rt.backend.set_available(False)

        started = time.perf_counter()
        for idx in range(120):
            with self.assertRaises(FuseOSError) as ctx:
                self.rt.fs.getattr(f"/missing-{idx}.bin")
            self.assertEqual(ctx.exception.errno, errno.EHOSTUNREACH)
        elapsed = time.perf_counter() - started

        # If unavailable backend stat() were called this would be many seconds.
        self.assertLess(elapsed, 2.0)


if __name__ == "__main__":
    unittest.main()
