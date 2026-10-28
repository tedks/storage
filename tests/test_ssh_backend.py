import io
import socket
import subprocess
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from storage.backends import ssh as ssh_module
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


_GETENT_AHOSTS = (
    "192.0.2.7       STREAM nas.local\n"
    "192.0.2.7       DGRAM  \n"
    "192.0.2.7       RAW    \n"
    "2001:db8::7     STREAM \n"
    "2001:db8::7     DGRAM  \n"
)


def _getent_result(stdout="", returncode=0):
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr="")


class TestHostResolverFallback(unittest.TestCase):
    """Lookups through the host's getent when this Python's glibc cannot resolve."""

    def setUp(self):
        # Only /usr/bin/getent "exists", so the tests do not depend on the host.
        access = patch("storage.backends.ssh.os.access",
                       side_effect=lambda path, _mode: path == "/usr/bin/getent")
        which = patch("storage.backends.ssh.shutil.which", return_value=None)
        access.start()
        which.start()
        self.addCleanup(access.stop)
        self.addCleanup(which.stop)

    @patch("storage.backends.ssh.subprocess.run")
    def test_addresses_are_stream_lines_in_order_without_duplicates(self, mock_run):
        mock_run.return_value = _getent_result(_GETENT_AHOSTS)
        self.assertEqual(
            ssh_module._host_resolver_addresses("nas.local", 5.0),
            ["192.0.2.7", "2001:db8::7"],
        )
        args, kwargs = mock_run.call_args
        self.assertEqual(args[0], ["/usr/bin/getent", "ahosts", "--", "nas.local"])
        self.assertEqual(kwargs["timeout"], 5.0)

    @patch("storage.backends.ssh.subprocess.run")
    def test_unparseable_lines_are_skipped(self, mock_run):
        mock_run.return_value = _getent_result("not-an-ip STREAM x\n\n192.0.2.8 STREAM\n")
        self.assertEqual(ssh_module._host_resolver_addresses("nas.local", 5.0), ["192.0.2.8"])

    @patch("storage.backends.ssh.subprocess.run")
    def test_no_addresses_when_getent_fails_or_times_out(self, mock_run):
        for outcome in (
            _getent_result("", returncode=2),
            subprocess.TimeoutExpired(cmd="getent", timeout=5.0),
            FileNotFoundError("getent"),
        ):
            with self.subTest(outcome=outcome):
                if isinstance(outcome, BaseException):
                    mock_run.side_effect = outcome
                else:
                    mock_run.side_effect = None
                    mock_run.return_value = outcome
                self.assertEqual(ssh_module._host_resolver_addresses("nas.local", 5.0), [])

    @patch("storage.backends.ssh.subprocess.run")
    def test_no_getent_means_no_addresses(self, mock_run):
        with patch("storage.backends.ssh.os.access", return_value=False):
            self.assertEqual(ssh_module._host_resolver_addresses("nas.local", 5.0), [])
        mock_run.assert_not_called()

    @patch("storage.backends.ssh.socket.create_connection")
    @patch("storage.backends.ssh.subprocess.run")
    def test_connect_tries_each_address_until_one_answers(self, mock_run, mock_create):
        mock_run.return_value = _getent_result(_GETENT_AHOSTS)
        sock = object()
        mock_create.side_effect = [OSError("unreachable"), sock]
        lookup_error = socket.gaierror(-3, "Temporary failure in name resolution")
        self.assertIs(ssh_module._connect_via_host_resolver("nas.local", 2222, 4.0, lookup_error), sock)
        self.assertEqual(
            [c.args[0] for c in mock_create.call_args_list],
            [("192.0.2.7", 2222), ("2001:db8::7", 2222)],
        )
        self.assertEqual(mock_create.call_args.kwargs["timeout"], 4.0)

    @patch("storage.backends.ssh.socket.create_connection")
    @patch("storage.backends.ssh.subprocess.run")
    def test_connect_raises_the_last_connect_error_when_none_answers(self, mock_run, mock_create):
        mock_run.return_value = _getent_result(_GETENT_AHOSTS)
        mock_create.side_effect = [OSError("first"), ConnectionRefusedError("second")]
        with self.assertRaises(ConnectionRefusedError):
            ssh_module._connect_via_host_resolver(
                "nas.local", 22, 4.0, socket.gaierror(-3, "lookup"))

    @patch("storage.backends.ssh.socket.create_connection")
    @patch("storage.backends.ssh.subprocess.run")
    def test_connect_keeps_the_lookup_error_when_getent_finds_nothing(self, mock_run, mock_create):
        mock_run.return_value = _getent_result("", returncode=2)
        lookup_error = socket.gaierror(-2, "Name or service not known")
        with self.assertRaises(socket.gaierror) as caught:
            ssh_module._connect_via_host_resolver("typo.local", 22, 4.0, lookup_error)
        self.assertIs(caught.exception, lookup_error)
        mock_create.assert_not_called()


class _FakeTCPSocket:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class TestSSHBackendHostResolverConnect(unittest.TestCase):
    def _new_backend(self):
        return SSHBackend(
            name="ssh0",
            host="nas.local",
            remote_path="/data",
            username="test",
            key_filename="/tmp/id_test",
            port=2222,
            connect_timeout=3.0,
        )

    @staticmethod
    def _client_failing_lookup_without_sock(calls, fail_with_sock=None):
        client = _FakeSSHClient(_FakeSFTPClient(payload=b""))

        def _connect(**kwargs):
            calls.append(kwargs)
            if "sock" not in kwargs:
                raise socket.gaierror(-3, "Temporary failure in name resolution")
            if fail_with_sock is not None:
                raise fail_with_sock

        client.connect = _connect
        return client

    @patch("storage.backends.ssh._connect_via_host_resolver")
    @patch("storage.backends.ssh.paramiko.AutoAddPolicy")
    @patch("storage.backends.ssh.paramiko.SSHClient")
    def test_lookup_failure_connects_over_host_resolver_socket(self, mock_ssh_client_cls, _policy, mock_fallback):
        calls = []
        mock_ssh_client_cls.side_effect = lambda: self._client_failing_lookup_without_sock(calls)
        sock = _FakeTCPSocket()
        mock_fallback.return_value = sock
        backend = self._new_backend()

        backend._current_clients()

        self.assertTrue(backend.is_available())
        self.assertEqual(mock_ssh_client_cls.call_count, 1)
        host, port, timeout, lookup_error = mock_fallback.call_args.args
        self.assertEqual((host, port, timeout), ("nas.local", 2222, 3.0))
        self.assertIsInstance(lookup_error, socket.gaierror)
        self.assertEqual(len(calls), 2)
        # Same hostname (it names the host key) and settings, plus the socket.
        self.assertIs(calls[1].pop("sock"), sock)
        self.assertEqual(calls[1], calls[0])
        self.assertEqual(calls[1]["hostname"], "nas.local")
        self.assertFalse(sock.closed)
        backend.close()

    @patch("storage.backends.ssh._connect_via_host_resolver")
    @patch("storage.backends.ssh.paramiko.AutoAddPolicy")
    @patch("storage.backends.ssh.paramiko.SSHClient")
    def test_handshake_failure_over_fallback_socket_closes_it(self, mock_ssh_client_cls, _policy, mock_fallback):
        calls = []
        handshake_error = ssh_module.paramiko.SSHException("banner")
        mock_ssh_client_cls.side_effect = lambda: self._client_failing_lookup_without_sock(
            calls, fail_with_sock=handshake_error)
        sock = _FakeTCPSocket()
        mock_fallback.return_value = sock
        backend = self._new_backend()

        with self.assertRaises(ssh_module.paramiko.SSHException):
            backend._current_clients()
        self.assertTrue(sock.closed)
        self.assertFalse(backend.is_available())
        backend.close()

    @patch("storage.backends.ssh._connect_via_host_resolver")
    @patch("storage.backends.ssh.paramiko.AutoAddPolicy")
    @patch("storage.backends.ssh.paramiko.SSHClient")
    def test_unresolvable_host_keeps_lookup_error_and_marks_unavailable(self, mock_ssh_client_cls, _policy, mock_fallback):
        calls = []
        mock_ssh_client_cls.side_effect = lambda: self._client_failing_lookup_without_sock(calls)
        def _no_address(_host, _port, _timeout, lookup_error):
            raise lookup_error

        mock_fallback.side_effect = _no_address
        backend = self._new_backend()

        with self.assertRaises(socket.gaierror):
            backend._current_clients()
        self.assertFalse(backend.is_available())
        self.assertEqual(len(calls), 1)
        backend.close()


if __name__ == "__main__":
    unittest.main()
