import io
import socket
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from storage.backends.ssh import SSHBackend


class _FakeSocket:
    def __init__(self):
        self.timeout = None

    def settimeout(self, timeout):
        self.timeout = timeout


class _FakeTransport:
    def __init__(self):
        self.sock = _FakeSocket()
        self._active = True

    def is_active(self):
        return self._active


class _FakeChannel:
    def __init__(self):
        self.timeout = None

    def settimeout(self, timeout):
        self.timeout = timeout


class _FakeRemoteFile:
    def __init__(self, data: bytes):
        self._data = data
        self._offset = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def seek(self, offset: int):
        self._offset = offset

    def read(self, size: int):
        start = self._offset
        end = start + size
        chunk = self._data[start:end]
        self._offset += len(chunk)
        return chunk

    def write(self, data: bytes):
        return len(data)

    def truncate(self, _size: int):
        pass


class _FakeSFTPClient:
    def __init__(self, payload: bytes, fail_open_count: int = 0,
                 fail_stat_count: int = 0, fail_stat_match: str = None):
        self.payload = payload
        self.fail_open_count = fail_open_count
        self.fail_stat_count = fail_stat_count
        self.fail_stat_match = fail_stat_match
        self.open_calls = []
        self.closed = False
        self._channel = _FakeChannel()

    def get_channel(self):
        return self._channel

    def stat(self, path: str):
        if self.fail_stat_count > 0:
            if self.fail_stat_match is None or self.fail_stat_match in path:
                self.fail_stat_count -= 1
                raise socket.timeout("simulated stat timeout")
        return SimpleNamespace(st_size=len(self.payload), st_mode=0o100644)

    def open(self, path: str, mode: str):
        self.open_calls.append((path, mode))
        if self.closed:
            raise OSError("simulated closed sftp client")
        if self.fail_open_count > 0:
            self.fail_open_count -= 1
            raise socket.timeout("simulated timeout")
        return _FakeRemoteFile(self.payload)

    def close(self):
        self.closed = True

    def remove(self, _path: str):
        pass

    def mkdir(self, _path: str):
        pass

    def rmdir(self, _path: str):
        pass

    def listdir(self, _path: str):
        return []

    def listdir_attr(self, _path: str):
        return []

    def get(self, _remote_path: str, _local_path: str):
        pass

    def put(self, _local_path: str, _remote_path: str):
        pass

    def rename(self, _old_path: str, _new_path: str):
        pass


class _FakeSSHClient:
    def __init__(self, sftp_client: _FakeSFTPClient):
        self.sftp_client = sftp_client
        self.transport = _FakeTransport()
        self.closed = False

    def set_missing_host_key_policy(self, _policy):
        pass

    def connect(self, **_kwargs):
        pass

    def open_sftp(self):
        return self.sftp_client

    def get_transport(self):
        return self.transport

    def exec_command(self, _cmd: str, timeout=None):
        _ = timeout
        return None, io.BytesIO(b"deadbeef  file\n"), io.BytesIO(b"")

    def close(self):
        self.closed = True
        self.transport._active = False


class TestSSHBackendThreadedClients(unittest.TestCase):
    def _new_backend(self):
        return SSHBackend(
            name="ssh0",
            host="example.local",
            remote_path="/data",
            username="test",
            key_filename="/tmp/id_test",
        )

    @patch("storage.backends.ssh.paramiko.AutoAddPolicy")
    @patch("storage.backends.ssh.paramiko.SSHClient")
    def test_read_retry_does_not_quarantine_backend(self, mock_ssh_client_cls, mock_auto_add_policy):
        _ = mock_auto_add_policy
        payload = b"abcdefghijklmnopqrstuvwxyz"

        first = _FakeSSHClient(_FakeSFTPClient(payload=payload, fail_open_count=1))
        second = _FakeSSHClient(_FakeSFTPClient(payload=payload))
        mock_ssh_client_cls.side_effect = [first, second]

        backend = self._new_backend()
        data = backend.read_file("/movie.bin", 0, 8)

        self.assertEqual(data, payload[:8])
        self.assertTrue(backend.is_available())
        self.assertEqual(mock_ssh_client_cls.call_count, 2)
        self.assertTrue(first.closed)
        self.assertFalse(second.closed)

        backend.close()

    @patch("storage.backends.ssh.paramiko.AutoAddPolicy")
    @patch("storage.backends.ssh.paramiko.SSHClient")
    def test_exists_disconnect_sets_unavailable_and_respects_probe_interval(self, mock_ssh_client_cls, mock_auto_add_policy):
        _ = mock_auto_add_policy

        failing = _FakeSSHClient(_FakeSFTPClient(payload=b""))

        def _fail_connect(**_kwargs):
            raise socket.timeout("simulated connect timeout")

        failing.connect = _fail_connect
        mock_ssh_client_cls.return_value = failing

        backend = self._new_backend()

        self.assertFalse(backend.exists("/movie.bin"))
        self.assertFalse(backend.is_available())

        # Immediate retry should short-circuit through cooldown, avoiding another client build.
        self.assertFalse(backend.exists("/movie.bin"))
        self.assertEqual(mock_ssh_client_cls.call_count, 1)

        backend.close()

    @patch("storage.backends.ssh.paramiko.AutoAddPolicy")
    @patch("storage.backends.ssh.paramiko.SSHClient")
    def test_exists_reconnects_on_next_probe_window(self, mock_ssh_client_cls, mock_auto_add_policy):
        _ = mock_auto_add_policy

        failing = _FakeSSHClient(_FakeSFTPClient(payload=b""))
        recovered = _FakeSSHClient(_FakeSFTPClient(payload=b"ok"))

        def _fail_connect(**_kwargs):
            raise socket.timeout("simulated connect timeout")

        failing.connect = _fail_connect
        mock_ssh_client_cls.side_effect = [failing, recovered]

        backend = self._new_backend()

        self.assertFalse(backend.exists("/movie.bin"))
        self.assertFalse(backend.is_available())

        # Simulate probe interval elapsed and allow one reconnect probe.
        backend._next_connect_retry_at = 0.0
        self.assertTrue(backend.exists("/movie.bin"))
        self.assertTrue(backend.is_available())
        self.assertEqual(mock_ssh_client_cls.call_count, 2)

        backend.close()

    @patch("storage.backends.ssh.paramiko.AutoAddPolicy")
    @patch("storage.backends.ssh.paramiko.SSHClient")
    def test_stat_missing_file_does_not_mark_backend_unavailable(self, mock_ssh_client_cls, mock_auto_add_policy):
        _ = mock_auto_add_policy

        fake = _FakeSSHClient(_FakeSFTPClient(payload=b""))

        def _missing(_path):
            raise FileNotFoundError("missing")

        fake.sftp_client.stat = _missing
        mock_ssh_client_cls.return_value = fake

        backend = self._new_backend()

        with self.assertRaises(FileNotFoundError):
            backend.stat("/missing.bin")

        self.assertTrue(backend.is_available())

        backend.close()

    @patch("storage.backends.ssh.paramiko.AutoAddPolicy")
    @patch("storage.backends.ssh.paramiko.SSHClient")
    def test_reset_closes_only_calling_thread_client(self, mock_ssh_client_cls, mock_auto_add_policy):
        _ = mock_auto_add_policy
        payload = b"x" * 1024
        created = []

        def _factory():
            client = _FakeSSHClient(_FakeSFTPClient(payload=payload))
            created.append(client)
            return client

        mock_ssh_client_cls.side_effect = _factory
        backend = self._new_backend()

        barrier = threading.Barrier(2)
        seen_clients = {}
        still_open = {}
        errors = []

        def _worker(name: str, do_reset: bool):
            try:
                self.assertEqual(backend.get_size("/movie.bin"), len(payload))
                ssh_client, _ = backend._get_thread_clients()
                seen_clients[name] = ssh_client
                barrier.wait(timeout=3)
                if do_reset:
                    backend._reset_connection()
                barrier.wait(timeout=3)
                still_open[name] = not ssh_client.closed
            except Exception as exc:  # pragma: no cover - explicit test failure path
                errors.append(exc)

        t1 = threading.Thread(target=_worker, args=("a", True))
        t2 = threading.Thread(target=_worker, args=("b", False))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        if errors:
            raise errors[0]

        self.assertEqual(len(created), 2)
        self.assertIsNotNone(seen_clients.get("a"))
        self.assertIsNotNone(seen_clients.get("b"))
        self.assertIsNot(seen_clients["a"], seen_clients["b"])
        self.assertFalse(still_open["a"])
        self.assertTrue(still_open["b"])

        backend.close()
        self.assertTrue(seen_clients["b"].closed)

    @patch("storage.backends.ssh.paramiko.AutoAddPolicy")
    @patch("storage.backends.ssh.paramiko.SSHClient")
    def test_prune_stale_thread_clients_closes_dead_thread_connections(self, mock_ssh_client_cls, mock_auto_add_policy):
        _ = mock_auto_add_policy
        payload = b"y" * 1024
        created = []

        def _factory():
            client = _FakeSSHClient(_FakeSFTPClient(payload=payload))
            created.append(client)
            return client

        mock_ssh_client_cls.side_effect = _factory
        backend = self._new_backend()

        worker = threading.Thread(target=lambda: backend.get_size("/movie.bin"))
        worker.start()
        worker.join(timeout=5)
        self.assertFalse(worker.is_alive())

        with backend._thread_clients_lock:
            self.assertEqual(len(backend._thread_clients), 1)

        backend._prune_stale_thread_clients()

        with backend._thread_clients_lock:
            self.assertEqual(len(backend._thread_clients), 0)

        self.assertEqual(len(created), 1)
        self.assertTrue(created[0].closed)
        backend.close()

    @patch("storage.backends.ssh.paramiko.AutoAddPolicy")
    @patch("storage.backends.ssh.paramiko.SSHClient")
    def test_write_file_reacquires_client_after_exists_reconnect(self, mock_ssh_client_cls, mock_auto_add_policy):
        _ = mock_auto_add_policy

        first = _FakeSSHClient(
            _FakeSFTPClient(payload=b"", fail_stat_count=1, fail_stat_match="movie.bin")
        )
        second = _FakeSSHClient(_FakeSFTPClient(payload=b""))
        mock_ssh_client_cls.side_effect = [first, second]

        backend = self._new_backend()
        backend.write_file("/movie.bin", b"abc", 0)

        self.assertTrue(first.closed)
        self.assertFalse(second.closed)
        self.assertEqual(len(first.sftp_client.open_calls), 0)
        self.assertEqual(len(second.sftp_client.open_calls), 1)
        backend.close()

    def _blocking_first_connect(self, mock_ssh_client_cls):
        """Make the first client's connect block until the returned event is set."""
        release = threading.Event()
        connecting = threading.Event()
        created = []

        def _factory():
            client = _FakeSSHClient(_FakeSFTPClient(payload=b""))
            if not created:
                def _slow_connect(**_kwargs):
                    connecting.set()
                    release.wait(timeout=5)
                client.connect = _slow_connect
            created.append(client)
            return client

        mock_ssh_client_cls.side_effect = _factory
        return release, connecting, created

    @staticmethod
    def _run_in_thread(fn):
        outcome = {}

        def target():
            try:
                outcome["result"] = fn()
            except Exception as exc:  # pragma: no cover - asserted by callers
                outcome["error"] = exc

        thread = threading.Thread(target=target)
        thread.start()
        return thread, outcome

    @patch("storage.backends.ssh.paramiko.AutoAddPolicy")
    @patch("storage.backends.ssh.paramiko.SSHClient")
    def test_connects_in_parallel_while_available(self, mock_ssh_client_cls, mock_auto_add_policy):
        # Two threads connecting an available member at once (a readdir
        # listing thread and a sync worker) both get a client, and the second
        # does not wait for the first: its connect completes while the first
        # is still blocked.
        _ = mock_auto_add_policy
        release, connecting, created = self._blocking_first_connect(mock_ssh_client_cls)
        backend = self._new_backend()

        first, first_outcome = self._run_in_thread(backend._current_clients)
        try:
            self.assertTrue(connecting.wait(timeout=5))
            ssh_client, sftp_client = backend._current_clients()
            self.assertTrue(first.is_alive())  # still connecting
        finally:
            release.set()
            first.join(timeout=5)

        self.assertFalse(first.is_alive())
        self.assertNotIn("error", first_outcome)
        self.assertIsNot(first_outcome["result"][0], ssh_client)
        self.assertIsNotNone(sftp_client)
        self.assertEqual(len(created), 2)
        self.assertTrue(backend.is_available())
        backend.close()

    @patch("storage.backends.ssh.paramiko.AutoAddPolicy")
    @patch("storage.backends.ssh.paramiko.SSHClient")
    def test_connect_failure_while_available_fails_fast_in_retry_window(self, mock_ssh_client_cls, mock_auto_add_policy):
        _ = mock_auto_add_policy

        def _factory():
            client = _FakeSSHClient(_FakeSFTPClient(payload=b""))

            def _fail_connect(**_kwargs):
                raise socket.timeout("simulated connect timeout")

            client.connect = _fail_connect
            return client

        mock_ssh_client_cls.side_effect = _factory
        backend = self._new_backend()

        with self.assertRaises(socket.timeout):
            backend._current_clients()
        self.assertFalse(backend.is_available())

        # Inside the retry window: fail fast, no new connection attempt.
        with self.assertRaises(ConnectionError):
            backend._current_clients()
        self.assertEqual(mock_ssh_client_cls.call_count, 1)
        backend.close()

    @patch("storage.backends.ssh.paramiko.AutoAddPolicy")
    @patch("storage.backends.ssh.paramiko.SSHClient")
    def test_connect_fails_fast_during_probe_after_disconnect(self, mock_ssh_client_cls, mock_auto_add_policy):
        _ = mock_auto_add_policy
        release, connecting, created = self._blocking_first_connect(mock_ssh_client_cls)
        backend = SSHBackend(
            name="ssh0",
            host="example.local",
            remote_path="/data",
            username="test",
            key_filename="/tmp/id_test",
            connect_timeout=30.0,
        )
        backend.set_available(False)  # retry window already open

        first = threading.Thread(target=backend._current_clients)
        first.start()
        try:
            self.assertTrue(connecting.wait(timeout=5))
            started = time.monotonic()
            with self.assertRaises(ConnectionError):
                backend._current_clients()
            self.assertLess(time.monotonic() - started, 5.0)
            self.assertEqual(len(created), 1)
        finally:
            release.set()
            first.join(timeout=5)
            backend.close()


if __name__ == "__main__":
    unittest.main()
