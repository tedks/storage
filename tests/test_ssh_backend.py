import concurrent.futures
import errno
import contextvars
import socket
import subprocess
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from storage.backends import ssh as ssh_module
from storage.backends import ssh_pool
from storage.backends.base import interactive_request, is_interactive_request
from storage.backends.ssh import SSHBackend
from storage.backends.ssh_pool import MemberConnection, MemberUnavailable, is_connection_error


# --------------------------------------------------------------------------- #
# Fakes: a paramiko Transport with session channels, and an SFTP client that
# fails loudly if two threads use it at once (paramiko's is not thread-safe).
# --------------------------------------------------------------------------- #

class _FakeChannel:
    def __init__(self, transport):
        self.transport = transport
        self.timeout = None
        self.closed = False
        self.subsystem = None
        self.command = None
        self._out = b""
        self.status_event = threading.Event()
        self.exit_status = -1

    def settimeout(self, timeout):
        self.timeout = timeout

    def gettimeout(self):
        return self.timeout

    def _check(self):
        if self.closed or not self.transport.active:
            raise EOFError("channel closed")

    def invoke_subsystem(self, name):
        self._check()
        if self.transport.subsystem_hook is not None:
            self.transport.subsystem_hook(self)
        self.subsystem = name

    def exec_command(self, command):
        self._check()
        self.command = command
        self.transport.commands.append(command)
        rc, out = self.transport.exec_handler(command)
        self._out = out
        self.exit_status = rc
        self.status_event.set()

    def recv(self, size):
        self._check()
        chunk, self._out = self._out[:size], self._out[size:]
        return chunk

    def close(self):
        if not self.closed:
            self.closed = True
            self.transport._channel_closed(self)


class _FakeTransport:
    def __init__(self, max_sessions=10):
        self.active = True
        self.max_sessions = max_sessions
        self.lock = threading.Lock()
        self.open_channels = []
        self.max_open_seen = 0
        self.sessions_opened = 0
        self.refusals = 0
        self.commands = []
        self.exec_handler = lambda _cmd: (0, b"")
        self.open_hook = None
        self.subsystem_hook = None
        self.sock = SimpleNamespace(settimeout_calls=[])
        self.sock.settimeout = self.sock.settimeout_calls.append

    def is_active(self):
        return self.active

    def open_session(self, timeout=None):
        if timeout is None:
            raise AssertionError("open_session without a timeout")
        if self.open_hook is not None:
            self.open_hook(timeout)
        if not self.active:
            raise ssh_module.paramiko.SSHException("SSH session not active")
        with self.lock:
            if len(self.open_channels) >= self.max_sessions:
                self.refusals += 1
                raise ssh_module.paramiko.ChannelException(2, "open failed")
            chan = _FakeChannel(self)
            self.open_channels.append(chan)
            self.sessions_opened += 1
            self.max_open_seen = max(self.max_open_seen, len(self.open_channels))
        return chan

    def _channel_closed(self, chan):
        with self.lock:
            if chan in self.open_channels:
                self.open_channels.remove(chan)

    def close(self):
        self.active = False
        for chan in list(self.open_channels):
            chan.close()


class _FakeRemoteFile:
    def __init__(self, sftp, data: bytes, mode: str = "rb"):
        self._sftp = sftp
        self._data = data
        self._offset = 0
        # paramiko's file is writable only with w, a or + in the mode.
        self._writable = any(flag in mode for flag in "wa+")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def seek(self, offset: int):
        self._offset = offset

    def read(self, size: int):
        chunk = self._data[self._offset:self._offset + size]
        self._offset += len(chunk)
        return chunk

    def write(self, data: bytes):
        if not self._writable:
            raise OSError("File not open for writing")
        return len(data)

    def truncate(self, _size: int):
        pass


class _FakeSFTPClient:
    """Built on a channel, as paramiko.SFTPClient(chan) is."""

    # Set by tests: payload served by open(), files that exist, and hooks
    # called at the start of every request.
    def __init__(self, chan, server):
        self.chan = chan
        self.server = server
        self.closed = False
        self._busy = threading.Lock()
        server.clients.append(self)

    def get_channel(self):
        return self.chan

    def close(self):
        self.closed = True
        self.chan.close()

    def _request(self, name, path):
        if not self._busy.acquire(blocking=False):
            self.server.concurrent_use.append(name)
            self._busy.acquire()
        try:
            self.chan._check()
            self.server.requests.append((name, path, self.chan.gettimeout(),
                                         is_interactive_request()))
            hook = self.server.hooks.get(name)
            if hook is not None:
                hook(self, path)
            if self.server.delay:
                time.sleep(self.server.delay)
            self.chan._check()
        finally:
            self._busy.release()

    def stat(self, path):
        self._request("stat", path)
        if path in self.server.missing:
            # paramiko: IOError(errno.ENOENT, text), without the path.
            raise FileNotFoundError(errno.ENOENT, "No such file")
        return SimpleNamespace(st_size=len(self.server.payload), st_mode=0o100644,
                               st_mtime=1.0)

    def open(self, path, mode):
        self._request("open", path)
        self.server.opens.append((path, mode))
        exists = path not in self.server.missing
        if "r" in mode and not exists:
            raise FileNotFoundError(errno.ENOENT, "No such file")
        if "x" in mode:
            if exists:
                raise OSError("Failure")  # SFTP_FAILURE carries no errno
            self.server.missing.discard(path)
        return _FakeRemoteFile(self, self.server.payload, mode)

    def remove(self, path):
        self._request("remove", path)

    def mkdir(self, path):
        self._request("mkdir", path)

    def rmdir(self, path):
        self._request("rmdir", path)

    def listdir(self, path):
        self._request("listdir", path)
        return ["a", "b"]

    def listdir_attr(self, path):
        self._request("listdir_attr", path)
        return [SimpleNamespace(filename="a", st_mode=0o100644, st_size=1,
                                st_mtime=1.0, st_atime=1.0, st_uid=0, st_gid=0,
                                st_nlink=1)]

    def get(self, remote_path, local_path):
        self._request("get", remote_path)
        with open(local_path, "wb") as f:
            f.write(self.server.payload)

    def put(self, local_path, remote_path):
        self._request("put", remote_path)

    def rename(self, old_path, new_path):
        self._request("rename", old_path)


class _FakeServer:
    """One fake sshd: the transports it accepted and the SFTP clients on them."""

    def __init__(self, max_sessions=10, payload=b"0123456789abcdef"):
        self.max_sessions = max_sessions
        self.payload = payload
        self.missing = set()
        self.hooks = {}
        self.delay = 0.0
        self.clients = []
        self.requests = []
        self.concurrent_use = []
        self.opens = []
        self.transports = []
        self.ssh_clients = []
        self.connect_calls = 0
        self.connect_error = None
        self.connect_gate = None  # an Event a connect waits on, if set
        self.exec_handler = lambda _cmd: (0, b"")
        self.lock = threading.Lock()

    def sftp_factory(self, chan):
        return _FakeSFTPClient(chan, self)

    def new_ssh_client(self):
        client = _FakeSSHClient(self)
        with self.lock:
            self.ssh_clients.append(client)
        return client

    def connect(self):
        """A MemberConnection connect callable."""
        client = self.new_ssh_client()
        client.connect()
        return client

    @property
    def transport(self):
        return self.transports[-1]


class _FakeSSHClient:
    def __init__(self, server):
        self.server = server
        self.transport = None
        self.closed = False
        self.connect_kwargs = []

    def set_missing_host_key_policy(self, _policy):
        pass

    def connect(self, **kwargs):
        self.connect_kwargs.append(kwargs)
        with self.server.lock:
            self.server.connect_calls += 1
        if self.server.connect_gate is not None:
            self.server.connect_gate.wait(timeout=10)
        error = self.server.connect_error
        if callable(error):
            error = error(kwargs)
        if error is not None:
            raise error
        self.transport = _FakeTransport(self.server.max_sessions)
        self.transport.exec_handler = self.server.exec_handler
        with self.server.lock:
            self.server.transports.append(self.transport)

    def get_transport(self):
        return self.transport

    def close(self):
        self.closed = True
        if self.transport is not None:
            self.transport.close()


def _pool(server, **kwargs):
    kwargs.setdefault("connect_timeout", 2.0)
    kwargs.setdefault("retry_interval", 5.0)
    return MemberConnection("ssh0", server.connect,
                            sftp_client_factory=server.sftp_factory, **kwargs)


def _run_threads(count, target):
    errors = []

    def wrapped(index):
        try:
            target(index)
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [threading.Thread(target=wrapped, args=(i,), daemon=True) for i in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)
    stuck = [thread for thread in threads if thread.is_alive()]
    if stuck:
        raise AssertionError(f"{len(stuck)} of {count} worker threads never finished")
    return errors


class _InThread:
    """Run fn in a thread; result or exception is kept for the test."""

    def __init__(self, fn, interactive=False):
        self.result = None
        self.error = None
        self.done = threading.Event()

        def target():
            try:
                if interactive:
                    with interactive_request():
                        self.result = fn()
                else:
                    self.result = fn()
            except BaseException as exc:
                self.error = exc
            finally:
                self.done.set()

        self.thread = threading.Thread(target=target, daemon=True)
        self.thread.start()

    def join(self, timeout=10):
        self.thread.join(timeout)
        return not self.thread.is_alive()


class _Holder:
    """A thread that holds one SFTP lease until release() (a lease belongs to
    the thread that took it)."""

    def __init__(self, pool, interactive=False):
        self.pool = pool
        self._release = threading.Event()
        self._released = threading.Event()
        self._acquired = threading.Event()
        self.lease = None
        self.error = None

        def target():
            try:
                if interactive:
                    with interactive_request():
                        self.lease = pool.acquire_sftp(1.0)
                else:
                    self.lease = pool.acquire_sftp(1.0)
            except BaseException as exc:
                self.error = exc
                self._acquired.set()
                return
            self._acquired.set()
            self._release.wait(timeout=20)
            pool.release(self.lease)
            self._released.set()

        threading.Thread(target=target, daemon=True).start()
        if not self._acquired.wait(timeout=10):
            raise AssertionError("holder did not acquire")
        if self.error is not None:
            raise self.error

    def release(self):
        self._release.set()
        if not self._released.wait(timeout=10):
            raise AssertionError("holder did not release")


# --------------------------------------------------------------------------- #
# The pool on its own
# --------------------------------------------------------------------------- #

class TestMemberConnectionPool(unittest.TestCase):

    def test_many_threads_share_one_connect_and_at_most_pool_size_channels(self):
        server = _FakeServer()
        server.delay = 0.002
        pool = _pool(server)

        def work(_index):
            for _ in range(25):
                with pool.sftp(1.0) as sftp:
                    sftp.stat("/data/x")

        self.assertEqual(_run_threads(16, work), [])
        self.assertEqual(server.connect_calls, 1)
        self.assertEqual(len(server.transports), 1)
        self.assertLessEqual(server.transport.max_open_seen, 4)
        self.assertEqual(server.concurrent_use, [])
        self.assertEqual(len(server.requests), 16 * 25)
        pool.close()

    def test_sftp_channel_opened_with_timeout_and_subsystem(self):
        server = _FakeServer()
        pool = _pool(server, connect_timeout=3.0)
        with pool.sftp(7.0) as sftp:
            self.assertEqual(sftp.chan.subsystem, "sftp")
            self.assertEqual(sftp.chan.gettimeout(), 7.0)
        # Back to the metadata timeout once the lease is returned.
        self.assertEqual(sftp.chan.gettimeout(), 3.0)
        pool.close()

    def test_saturated_background_pool_still_serves_an_interactive_request(self):
        server = _FakeServer()
        pool = _pool(server, connect_timeout=0.3)
        held = [_Holder(pool) for _ in range(3)]  # the bulk limit
        try:
            started = time.monotonic()
            bulk = _InThread(lambda: pool.acquire_sftp(1.0))
            self.assertTrue(bulk.join())
            self.assertIsInstance(bulk.error, ConnectionError)
            self.assertLess(time.monotonic() - started, 2.0)
            interactive = _InThread(lambda: pool.acquire_sftp(1.0), interactive=True)
            self.assertTrue(interactive.join())
            self.assertIsNone(interactive.error)
            pool.release(interactive.result)
            self.assertEqual(server.transport.max_open_seen, 4)
        finally:
            for holder in held:
                holder.release()
        self.assertTrue(pool.available())
        pool.close()

    def test_lease_wait_times_out_without_marking_the_member_down(self):
        server = _FakeServer()
        pool = _pool(server, connect_timeout=0.2, sftp_channels=1)
        holder = _Holder(pool)
        waiter = _InThread(lambda: pool.acquire_sftp(1.0))
        self.assertTrue(waiter.join())
        self.assertIsInstance(waiter.error, ConnectionError)
        self.assertTrue(pool.available())
        holder.release()
        with pool.sftp(1.0) as sftp:
            sftp.stat("/data/x")
        self.assertEqual(server.connect_calls, 1)
        pool.close()

    def test_lease_is_reentrant_within_a_thread(self):
        server = _FakeServer()
        pool = _pool(server, sftp_channels=1)
        with pool.sftp(5.0) as outer:
            with pool.sftp(1.0) as inner:
                self.assertIs(inner, outer)
                self.assertEqual(inner.chan.gettimeout(), 1.0)
            self.assertEqual(outer.chan.gettimeout(), 5.0)
        self.assertEqual(pool.snapshot()["idle"], 1)
        pool.close()

    def test_refused_channel_shrinks_the_pool_and_leaves_the_member_available(self):
        server = _FakeServer(max_sessions=2)
        pool = _pool(server)
        def work(_index):
            with pool.sftp(1.0) as sftp:
                sftp.stat("/data/x")
                time.sleep(0.05)

        self.assertEqual(_run_threads(4, work), [])
        self.assertGreaterEqual(server.transport.refusals, 1)
        self.assertLessEqual(server.transport.max_open_seen, 2)
        snapshot = pool.snapshot()
        self.assertEqual(snapshot["sftp_cap"], 1)
        self.assertEqual(snapshot["exec_cap"], 1)
        self.assertTrue(pool.available())
        self.assertEqual(server.connect_calls, 1)
        self.assertEqual(server.concurrent_use, [])
        pool.close()

    def test_a_server_that_refuses_every_channel_fails_the_request_only(self):
        server = _FakeServer(max_sessions=0)
        pool = _pool(server)
        with self.assertRaises(ConnectionError):
            with pool.sftp(1.0):
                pass
        self.assertTrue(pool.available())
        pool.close()

    def test_transport_death_mid_operation_reconnects_once_with_a_new_generation(self):
        server = _FakeServer()
        pool = _pool(server)
        with pool.sftp(1.0) as sftp:
            sftp.stat("/data/x")
        first_generation = pool.snapshot()["generation"]
        first_transport = server.transport

        def die(_sftp, _path):
            server.hooks.pop("stat")
            first_transport.close()
            raise ssh_module.paramiko.SSHException("Server connection dropped: ")

        server.hooks["stat"] = die
        with self.assertRaises(ssh_module.paramiko.SSHException):
            with pool.sftp(1.0) as sftp:
                sftp.stat("/data/x")
        with pool.sftp(1.0) as sftp:
            sftp.stat("/data/x")
        self.assertEqual(server.connect_calls, 2)
        self.assertGreater(pool.snapshot()["generation"], first_generation)
        self.assertTrue(server.ssh_clients[0].closed)
        self.assertTrue(pool.available())
        pool.close()

    def test_stale_generation_channels_are_discarded_on_return(self):
        server = _FakeServer()
        pool = _pool(server)
        old = _Holder(pool)
        other = pool.acquire_sftp(1.0)
        # Another operation finds the transport gone.
        server.transport.close()
        pool.release(other, EOFError("gone"))
        with pool.sftp(1.0) as sftp:
            sftp.stat("/data/x")
        old.release()
        self.assertTrue(old.lease.client.closed)
        snapshot = pool.snapshot()
        self.assertEqual(snapshot["idle"], 1)
        self.assertEqual(snapshot["sftp_open"], 1)
        pool.close()

    def test_timeout_with_a_live_transport_drops_only_the_channel(self):
        server = _FakeServer()
        pool = _pool(server)
        lease = pool.acquire_sftp(1.0)
        pool.release(lease, socket.timeout("timed out"))
        self.assertTrue(lease.client.closed)
        with pool.sftp(1.0) as sftp:
            sftp.stat("/data/x")
        self.assertEqual(server.connect_calls, 1)
        pool.close()

    def test_timeout_with_a_dead_transport_closes_it(self):
        server = _FakeServer()
        pool = _pool(server)
        lease = pool.acquire_sftp(1.0)

        def no_reply(_timeout):
            raise ssh_module.paramiko.SSHException("Timeout opening channel.")

        server.transport.open_hook = no_reply
        pool.release(lease, socket.timeout("timed out"))
        self.assertTrue(server.ssh_clients[0].closed)
        with pool.sftp(1.0) as sftp:
            sftp.stat("/data/x")
        self.assertEqual(server.connect_calls, 2)
        pool.close()

    def test_check_keeps_a_live_transport_and_drops_a_dead_one(self):
        server = _FakeServer()
        pool = _pool(server)
        with pool.sftp(1.0):
            pass
        pool.check(RuntimeError("outside failure"))
        self.assertFalse(server.ssh_clients[0].closed)

        def no_reply(_timeout):
            raise ssh_module.paramiko.SSHException("Timeout opening channel.")

        server.transport.open_hook = no_reply
        pool.check(RuntimeError("outside failure"))
        self.assertTrue(server.ssh_clients[0].closed)
        self.assertTrue(pool.available())
        pool.close()

    def test_requests_that_stall_on_a_live_transport_mark_the_member_down(self):
        server = _FakeServer()
        pool = _pool(server, stall_limit=2)

        def hang(_sftp, _path):
            raise socket.timeout("timed out")

        server.hooks["stat"] = hang
        for attempt in range(2):
            self.assertTrue(pool.available())
            with self.assertRaises(socket.timeout):
                with pool.sftp(1.0) as sftp:
                    sftp.stat("/data/x")
        self.assertFalse(pool.available())
        with self.assertRaises(MemberUnavailable):
            pool.acquire_sftp(1.0)

    def test_a_successful_request_resets_the_stall_count(self):
        server = _FakeServer()
        pool = _pool(server, stall_limit=2)
        for _ in range(3):
            lease = pool.acquire_sftp(1.0)
            pool.release(lease, socket.timeout("timed out"))
            with pool.sftp(1.0) as sftp:
                sftp.stat("/data/x")
        self.assertTrue(pool.available())
        self.assertEqual(pool.snapshot()["stalls"], 0)
        pool.close()

    def test_mark_down_wins_over_a_connect_in_flight(self):
        server = _FakeServer()
        gate = threading.Event()
        server.connect_gate = gate
        pool = _pool(server, connect_timeout=10.0)
        connecting = _InThread(lambda: pool.acquire_sftp(1.0))
        while server.connect_calls == 0:
            time.sleep(0.01)
        pool.mark_down()
        gate.set()
        self.assertTrue(connecting.join())
        self.assertIsInstance(connecting.error, MemberUnavailable)
        self.assertTrue(server.ssh_clients[0].closed)
        self.assertFalse(pool.available())

    def test_close_is_terminal(self):
        server = _FakeServer()
        pool = _pool(server, retry_interval=0.0)
        with pool.sftp(1.0):
            pass
        pool.close()
        pool.clear_down()
        self.assertFalse(pool.available())
        with self.assertRaises(MemberUnavailable):
            pool.acquire_sftp(1.0)
        self.assertEqual(server.connect_calls, 1)

    def test_pool_caps_are_relearned_on_a_new_transport(self):
        server = _FakeServer(max_sessions=2)
        pool = _pool(server)
        holders = [_Holder(pool) for _ in range(2)]
        third = _InThread(lambda: pool.acquire_sftp(1.0))
        time.sleep(0.05)
        # Shrunk to one channel: the third waits for both to come back.
        holders[0].release()
        holders[1].release()
        self.assertTrue(third.join())
        self.assertIsNone(third.error)
        self.assertEqual(pool.snapshot()["sftp_cap"], 1)
        pool.release(third.result)
        server.max_sessions = 10
        server.transport.close()
        with pool.sftp(1.0):
            pass
        self.assertEqual(pool.snapshot()["sftp_cap"], 4)
        pool.close()

    def test_nested_connection_error_is_acted_on_even_if_the_outer_call_swallows_it(self):
        server = _FakeServer()
        pool = _pool(server)
        with pool.sftp(1.0):
            dead = server.transport
            try:
                with pool.sftp(1.0):
                    dead.close()
                    raise ssh_module.paramiko.SSHException("Server connection dropped: ")
            except ssh_module.paramiko.SSHException:
                pass  # the outer caller carries on
        self.assertTrue(server.ssh_clients[0].closed)
        with pool.sftp(1.0):
            pass
        self.assertEqual(server.connect_calls, 2)
        pool.close()

    def test_background_requests_wait_longer_for_a_channel(self):
        server = _FakeServer()
        pool = _pool(server, connect_timeout=0.1, bulk_wait=1.0, sftp_channels=1)
        holder = _Holder(pool)
        waiter = _InThread(lambda: pool.acquire_sftp(1.0))
        time.sleep(0.3)  # past connect_timeout
        self.assertFalse(waiter.done.is_set())
        holder.release()
        self.assertTrue(waiter.join())
        self.assertIsNone(waiter.error)
        pool.release(waiter.result)
        pool.close()

    def test_pool_errors_count_as_connection_errors_but_spare_the_channel(self):
        self.assertTrue(is_connection_error(MemberUnavailable("busy")))
        server = _FakeServer()
        pool = _pool(server)
        lease = pool.acquire_sftp(1.0)
        pool.release(lease, MemberUnavailable("nested exec refused"))
        self.assertFalse(lease.client.closed)
        pool.close()

    def test_errno_errors_are_not_read_as_connection_loss_by_their_message(self):
        local = FileNotFoundError(errno.ENOENT, "No such file", "/cache/videofiles/x")
        self.assertFalse(is_connection_error(local))
        self.assertTrue(is_connection_error(OSError("Socket is closed")))
        self.assertTrue(is_connection_error(ConnectionResetError(errno.ECONNRESET, "reset")))

    def test_run_command_bounds_total_time_and_output(self):
        server = _FakeServer()
        server.exec_handler = lambda _cmd: (0, b"x" * (ssh_pool.MAX_COMMAND_OUTPUT + 1))
        pool = _pool(server)
        with self.assertRaises(RuntimeError):
            pool.run_command("yes", 5.0)
        self.assertEqual(server.transport.open_channels, [])

        server.exec_handler = lambda _cmd: (0, b"")
        original_recv = _FakeChannel.recv

        def slow_recv(chan, size):
            time.sleep(0.3)
            return original_recv(chan, size) or b"."

        with patch.object(_FakeChannel, "recv", slow_recv):
            started = time.monotonic()
            with self.assertRaises(socket.timeout):
                pool.run_command("trickle", 0.5)
            self.assertLess(time.monotonic() - started, 2.0)
        pool.close()

    def test_path_errors_keep_the_channel(self):
        server = _FakeServer()
        pool = _pool(server)
        for exc in (FileNotFoundError("x"), PermissionError("x"), OSError(5, "I/O")):
            lease = pool.acquire_sftp(1.0)
            pool.release(lease, exc)
            self.assertFalse(lease.client.closed)
        self.assertEqual(len(server.clients), 1)
        self.assertEqual(server.connect_calls, 1)
        pool.close()

    def test_connect_failure_marks_down_once_and_fails_fast(self):
        server = _FakeServer()
        server.connect_error = socket.timeout("connect timed out")
        gate = threading.Event()
        server.connect_gate = gate
        pool = _pool(server, connect_timeout=5.0)
        availability = []
        pool._on_availability = availability.append

        first = _InThread(lambda: pool.acquire_sftp(1.0))
        while server.connect_calls == 0:
            time.sleep(0.01)
        waiters = [_InThread(lambda: pool.acquire_sftp(1.0)) for _ in range(4)]
        time.sleep(0.05)
        gate.set()
        for thread in [first, *waiters]:
            self.assertTrue(thread.join())
        self.assertIsInstance(first.error, socket.timeout)
        for waiter in waiters:
            self.assertIsInstance(waiter.error, ConnectionError)
        self.assertEqual(server.connect_calls, 1)
        self.assertEqual(availability, [False])
        self.assertFalse(pool.available())

        # Inside the retry window: no new connect.
        with self.assertRaises(ConnectionError):
            pool.acquire_sftp(1.0)
        self.assertEqual(server.connect_calls, 1)
        pool.close()

    def test_reconnect_probe_after_the_retry_window(self):
        server = _FakeServer()
        server.connect_error = socket.timeout("connect timed out")
        pool = _pool(server, retry_interval=0.1)
        with self.assertRaises(socket.timeout):
            pool.acquire_sftp(1.0)
        self.assertFalse(pool.available())
        time.sleep(0.15)
        self.assertTrue(pool.available())
        server.connect_error = None
        with pool.sftp(1.0) as sftp:
            sftp.stat("/data/x")
        self.assertTrue(pool.available())
        self.assertEqual(server.connect_calls, 2)
        pool.close()

    def test_reconnect_probe_in_progress_fails_other_callers_fast(self):
        server = _FakeServer()
        pool = _pool(server, connect_timeout=30.0)
        pool.mark_down()
        pool.clear_down()
        pool.mark_down()
        pool._next_retry_at = 0.0  # the retry window has passed
        gate = threading.Event()
        server.connect_gate = gate
        prober = _InThread(lambda: pool.acquire_sftp(1.0))
        try:
            while server.connect_calls == 0:
                time.sleep(0.01)
            self.assertFalse(pool.available())
            started = time.monotonic()
            with self.assertRaises(ConnectionError):
                pool.acquire_sftp(1.0)
            self.assertLess(time.monotonic() - started, 1.0)
        finally:
            gate.set()
            self.assertTrue(prober.join())
        self.assertIsNone(prober.error)
        pool.release(prober.result)
        self.assertEqual(server.connect_calls, 1)
        pool.close()

    def test_callers_wait_for_the_connect_of_an_available_member(self):
        # A member believed up connects once; a second caller waits for that
        # connect instead of failing or opening its own (#117).
        server = _FakeServer()
        gate = threading.Event()
        server.connect_gate = gate
        pool = _pool(server, connect_timeout=5.0)
        first = _InThread(lambda: pool.acquire_sftp(1.0))
        while server.connect_calls == 0:
            time.sleep(0.01)
        self.assertTrue(pool.available())
        second = _InThread(lambda: pool.acquire_sftp(1.0))
        time.sleep(0.05)
        self.assertFalse(second.done.is_set())
        gate.set()
        self.assertTrue(first.join())
        self.assertTrue(second.join())
        self.assertIsNone(first.error)
        self.assertIsNone(second.error)
        self.assertEqual(server.connect_calls, 1)
        pool.release(first.result)
        pool.release(second.result)
        pool.close()

    def test_close_wakes_lease_waiters_and_connect_waiters(self):
        server = _FakeServer()
        pool = _pool(server, connect_timeout=30.0, sftp_channels=1)
        holder = _Holder(pool)
        waiter = _InThread(lambda: pool.acquire_sftp(1.0))
        time.sleep(0.05)
        started = time.monotonic()
        pool.close()
        self.assertTrue(waiter.join(timeout=5))
        self.assertLess(time.monotonic() - started, 5.0)
        self.assertIsInstance(waiter.error, ConnectionError)
        self.assertTrue(holder.lease.client.chan.closed)
        holder.release()
        self.assertFalse(pool.available())

        # A connect that close() raced is discarded.
        server2 = _FakeServer()
        gate = threading.Event()
        server2.connect_gate = gate
        pool2 = _pool(server2, connect_timeout=30.0)
        connecting = _InThread(lambda: pool2.acquire_sftp(1.0))
        while server2.connect_calls == 0:
            time.sleep(0.01)
        connect_waiter = _InThread(lambda: pool2.acquire_sftp(1.0))
        time.sleep(0.05)
        pool2.close()
        self.assertTrue(connect_waiter.join(timeout=5))
        self.assertIsInstance(connect_waiter.error, ConnectionError)
        gate.set()
        self.assertTrue(connecting.join())
        self.assertIsInstance(connecting.error, ConnectionError)
        self.assertTrue(server2.ssh_clients[0].closed)

    def test_exec_channels_are_bounded_and_always_closed(self):
        server = _FakeServer()
        release = threading.Event()
        in_flight = []

        def slow(cmd):
            in_flight.append(cmd)
            release.wait(timeout=5)
            return 0, b"ok\n"

        server.exec_handler = slow
        pool = _pool(server, connect_timeout=5.0)
        runners = [_InThread(lambda i=i: pool.run_command(f"cmd{i}", 5.0)) for i in range(3)]
        deadline = time.monotonic() + 5
        while len(in_flight) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.05)
        self.assertEqual(len(in_flight), 2)
        release.set()
        for runner in runners:
            self.assertTrue(runner.join())
            self.assertIsNone(runner.error)
            self.assertEqual(runner.result, (0, b"ok\n"))
        self.assertEqual(server.transport.open_channels, [])
        self.assertEqual(pool.snapshot()["exec_open"], 0)
        pool.close()

    def test_refused_exec_channel_raises_connection_error_and_member_stays_up(self):
        server = _FakeServer(max_sessions=1)
        pool = _pool(server)
        with pool.sftp(1.0):
            with self.assertRaises(ConnectionError):
                pool.run_command("true", 1.0)
        self.assertTrue(pool.available())
        self.assertEqual(pool.snapshot()["exec_open"], 0)
        pool.close()

    def test_stalled_subsystem_start_probes_and_keeps_the_transport(self):
        server = _FakeServer()
        pool = _pool(server, connect_timeout=0.2)
        with pool.sftp(1.0):
            pass

        def hang(chan):
            # paramiko's wait for a reply ends when the channel is closed.
            while not chan.closed:
                time.sleep(0.01)
            raise ssh_module.paramiko.SSHException("Channel closed.")

        server.transport.subsystem_hook = hang
        held = _Holder(pool)  # the idle channel
        started = time.monotonic()
        # The channel opened, so the transport answered: the stalled
        # subsystem start fails this request, a probe keeps the transport.
        with self.assertRaises(socket.timeout):
            pool.acquire_sftp(1.0)
        self.assertLess(time.monotonic() - started, 3.0)
        self.assertFalse(server.ssh_clients[0].closed)
        self.assertEqual(server.connect_calls, 1)
        self.assertEqual(server.transport.open_channels, [held.lease.client.chan])
        held.release()
        pool.close()

    def test_unanswered_channel_open_drops_the_transport_and_reconnects(self):
        server = _FakeServer()
        pool = _pool(server)
        held = _Holder(pool)

        def no_reply(_timeout):
            raise ssh_module.paramiko.SSHException("Timeout opening channel.")

        server.transport.open_hook = no_reply
        lease = pool.acquire_sftp(1.0)
        self.assertTrue(server.ssh_clients[0].closed)
        self.assertEqual(server.connect_calls, 2)
        pool.release(lease)
        held.release()
        pool.close()

    def test_one_members_stall_does_not_block_another(self):
        slow_server, fast_server = _FakeServer(), _FakeServer()
        gate = threading.Event()
        slow_server.connect_gate = gate
        slow_pool, fast_pool = _pool(slow_server, connect_timeout=10.0), _pool(fast_server)
        stalled = _InThread(lambda: slow_pool.acquire_sftp(1.0))
        try:
            while slow_server.connect_calls == 0:
                time.sleep(0.01)
            started = time.monotonic()
            with fast_pool.sftp(1.0) as sftp:
                sftp.stat("/data/x")
            self.assertLess(time.monotonic() - started, 1.0)
        finally:
            gate.set()
            stalled.join()
        slow_pool.release(stalled.result)
        slow_pool.close()
        fast_pool.close()

    def test_interactive_marker_follows_a_copied_context_into_an_executor(self):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            with interactive_request():
                inside = executor.submit(
                    contextvars.copy_context().run, is_interactive_request).result()
            outside = executor.submit(is_interactive_request).result()
        finally:
            executor.shutdown()
        self.assertTrue(inside)
        self.assertFalse(outside)
        self.assertFalse(is_interactive_request())


# --------------------------------------------------------------------------- #
# SSHBackend on the pool
# --------------------------------------------------------------------------- #

class _BackendTestCase(unittest.TestCase):
    """SSHBackend with paramiko's SSHClient and SFTPClient replaced by fakes."""

    def setUp(self):
        self.server = _FakeServer()
        for target, replacement in (
            ("storage.backends.ssh.paramiko.SSHClient", self.server.new_ssh_client),
            ("storage.backends.ssh.paramiko.AutoAddPolicy", lambda: None),
            ("storage.backends.ssh_pool.paramiko.SFTPClient", self.server.sftp_factory),
        ):
            patcher = patch(target, replacement)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _new_backend(self, **kwargs):
        kwargs.setdefault("connect_timeout", 2.0)
        backend = SSHBackend(
            name="ssh0",
            host="example.local",
            remote_path="/data",
            username="test",
            key_filename="/tmp/id_test",
            **kwargs,
        )
        self.addCleanup(backend.close)
        return backend


class TestSSHBackendOnThePool(_BackendTestCase):

    def test_parallel_operations_share_one_connection(self):
        self.server.delay = 0.002
        backend = self._new_backend()

        def work(index):
            for _ in range(10):
                backend.stat(f"/f{index}")
                backend.read_file("/movie.bin", 0, 4)
                backend.listdir("/")

        self.assertEqual(_run_threads(12, work), [])
        self.assertEqual(self.server.connect_calls, 1)
        self.assertLessEqual(self.server.transport.max_open_seen, 4)
        self.assertEqual(self.server.concurrent_use, [])

    def test_no_socket_timeout_override(self):
        backend = self._new_backend()
        backend.stat("/x")
        self.assertEqual(self.server.transport.sock.settimeout_calls, [])

    def test_metadata_uses_connect_timeout_and_reads_the_operation_timeout(self):
        backend = self._new_backend(connect_timeout=3.0)
        backend.stat("/x")
        backend.read_file("/x", 0, 4)
        timeouts = {name: timeout for name, _path, timeout, _i in self.server.requests}
        self.assertEqual(timeouts["stat"], 3.0)
        self.assertEqual(timeouts["open"], backend.sftp_operation_timeout)

    def test_read_retry_after_transport_loss_does_not_quarantine(self):
        backend = self._new_backend()

        def die(_sftp, _path):
            self.server.hooks.pop("open")
            self.server.transport.close()
            raise ssh_module.paramiko.SSHException("Server connection dropped: ")

        backend.stat("/x")
        self.server.hooks["open"] = die
        self.assertEqual(backend.read_file("/movie.bin", 0, 8), self.server.payload[:8])
        self.assertTrue(backend.is_available())
        self.assertEqual(self.server.connect_calls, 2)

    def test_stat_missing_file_does_not_mark_backend_unavailable(self):
        backend = self._new_backend()
        self.server.missing.add("/data/missing.bin")
        with self.assertRaises(FileNotFoundError):
            backend.stat("/missing.bin")
        self.assertFalse(backend.exists("/missing.bin"))
        self.assertTrue(backend.is_available())
        self.assertEqual(self.server.connect_calls, 1)
        self.assertEqual(len(self.server.clients), 1)

    def test_exists_connect_failure_marks_unavailable_and_respects_retry_window(self):
        self.server.connect_error = socket.timeout("simulated connect timeout")
        backend = self._new_backend()
        self.assertFalse(backend.exists("/movie.bin"))
        self.assertFalse(backend.is_available())
        self.assertFalse(backend.exists("/movie.bin"))
        self.assertEqual(self.server.connect_calls, 1)

        # Retry window over: one probe connects and the member is back.
        self.server.connect_error = None
        backend._pool._next_retry_at = 0.0
        self.assertTrue(backend.exists("/movie.bin"))
        self.assertTrue(backend.is_available())
        self.assertEqual(self.server.connect_calls, 2)

    def test_exists_or_raise_raises_when_the_member_cannot_be_asked(self):
        backend = self._new_backend()
        self.server.missing.add("/data/missing.bin")
        self.assertTrue(backend.exists_or_raise("/movie.bin"))
        self.assertFalse(backend.exists_or_raise("/missing.bin"))
        backend.set_available(False)
        self.assertFalse(backend.exists("/movie.bin"))
        with self.assertRaises(ConnectionError):
            backend.exists_or_raise("/movie.bin")

    def test_conflict_check_does_not_read_an_unreachable_member_as_absent(self):
        from storage.conflicts import ConflictDetector

        backend = self._new_backend()
        backend.set_available(False)
        detector = ConflictDetector.__new__(ConflictDetector)
        with self.assertRaises(ConnectionError):
            detector.check_conflict("/movie.bin", backend, "0" * 32)

    def test_a_timeout_on_a_live_connection_is_not_retried(self):
        backend = self._new_backend()

        def hang(_sftp, _path):
            raise socket.timeout("timed out")

        self.server.hooks["stat"] = hang
        with self.assertRaises(socket.timeout):
            backend.stat("/x")
        self.assertEqual(sum(1 for name, *_ in self.server.requests if name == "stat"), 1)
        self.assertEqual(self.server.connect_calls, 1)

    def test_set_available_maps_onto_the_pool(self):
        backend = self._new_backend()
        backend.set_available(False)
        self.assertFalse(backend.is_available())
        self.assertFalse(backend.get_info().available)
        with self.assertRaises(ConnectionError):
            backend.stat("/x")
        backend.set_available(True)
        self.assertTrue(backend.is_available())
        backend.stat("/x")

    def test_write_file_updates_in_place_and_creates_only_exclusively(self):
        backend = self._new_backend()
        backend._pool._sftp_channels = backend._pool._sftp_cap = 1
        backend.write_file("/movie.bin", b"abc", 0)
        self.server.missing.add("/data/new.bin")
        backend.write_file("/new.bin", b"abc", 0)
        self.assertEqual(self.server.opens, [
            ("/data/movie.bin", "r+b"),
            ("/data/new.bin", "r+b"), ("/data/new.bin", "x+b"),
        ])
        self.assertEqual(len(self.server.clients), 1)

    def test_write_file_opens_a_file_created_in_between_without_truncating(self):
        backend = self._new_backend()
        self.server.missing.add("/data/raced.bin")

        def created_by_someone_else(_sftp, path):
            # The exclusive create finds the file another writer just made.
            if ("/data/raced.bin", "r+b") in self.server.opens:
                self.server.missing.discard(path)

        self.server.hooks["open"] = created_by_someone_else
        backend.write_file("/raced.bin", b"abc", 0)
        modes = [mode for path, mode in self.server.opens if path == "/data/raced.bin"]
        self.assertEqual(modes, ["r+b", "x+b", "r+b"])
        self.assertNotIn("wb", modes)

    def test_write_file_never_truncates_after_a_connection_error(self):
        backend = self._new_backend()

        def fail(_sftp, _path):
            raise socket.timeout("timed out")

        self.server.hooks["open"] = fail
        with self.assertRaises(socket.timeout):
            backend.write_file("/movie.bin", b"abc", 0)
        # One open was attempted (for update) and nothing after it.
        self.assertEqual([name for name, *_ in self.server.requests].count("open"), 1)

    def test_pool_of_one_survives_the_checksum_download_fallback(self):
        self.server.exec_handler = lambda _cmd: (127, b"")
        backend = self._new_backend()
        backend._pool._sftp_channels = backend._pool._sftp_cap = 1
        digest = backend.get_fast_checksum("/movie.bin")
        self.assertEqual(len(digest), 32)
        self.assertEqual(self.server.transport.open_channels[0].subsystem, "sftp")
        self.assertEqual(len(self.server.transport.open_channels), 1)

        # And a nested call on a thread that already holds the one channel.
        with backend._pool.sftp(1.0):
            self.assertEqual(len(backend.get_fast_checksum("/movie.bin")), 32)

    def test_remote_checksum_and_xattrs_close_their_exec_channels(self):
        digest = "0123456789abcdef0123456789abcdef"

        def handler(cmd):
            if cmd.startswith("xxh128sum"):
                return 0, f"{digest}  /data/movie.bin\n".encode()
            if cmd.startswith("getfattr"):
                return 0, b"value"
            return 0, b""

        self.server.exec_handler = handler
        backend = self._new_backend()
        self.assertEqual(backend.get_fast_checksum("/movie.bin"), digest)
        backend.set_xattr("/movie.bin", "user.x", b"v")
        self.assertEqual(backend.get_xattr("/movie.bin", "user.x"), b"value")
        self.assertEqual(self.server.transport.open_channels, [])
        self.assertEqual(len(self.server.transport.commands), 3)
        self.assertTrue(all(cmd.endswith("2>/dev/null") for cmd in self.server.transport.commands))

    def test_get_xattr_failure_is_none(self):
        self.server.exec_handler = lambda _cmd: (1, b"")
        backend = self._new_backend()
        self.assertIsNone(backend.get_xattr("/movie.bin", "user.x"))

    def test_close_wakes_waiters_and_marks_unavailable(self):
        backend = self._new_backend(connect_timeout=30.0)
        backend._pool._sftp_channels = backend._pool._sftp_cap = 1
        holder = _Holder(backend._pool)
        waiter = _InThread(lambda: backend.stat("/x"))
        time.sleep(0.05)
        backend.close()
        self.assertTrue(waiter.join(timeout=5))
        self.assertIsInstance(waiter.error, ConnectionError)
        holder.release()
        self.assertFalse(backend.is_available())

    def test_fuse_dispatch_marks_requests_interactive(self):
        from storage.fs import StorageFS

        class _Probe(StorageFS):
            def __init__(self):  # no filesystem state needed
                pass

            def getattr(self, path, fh=None):
                return is_interactive_request()

        self.assertTrue(_Probe()("getattr", "/"))
        self.assertFalse(is_interactive_request())

    def test_rsync_timeout_probes_the_transport_instead_of_dropping_it(self):
        from storage.backends.rsync import RsyncBackend

        backend = RsyncBackend(name="r0", host="example.local", remote_path="/data",
                               username="test", key_filename="/tmp/id_test",
                               connect_timeout=2.0)
        self.addCleanup(backend.close)
        timeout = subprocess.TimeoutExpired(cmd="rsync", timeout=300)
        with patch.object(backend, "_run_rsync", side_effect=timeout):
            with self.assertRaises(subprocess.TimeoutExpired):
                backend.copy_from_local(SimpleNamespace(), "/f.bin")
        self.assertTrue(backend.is_available())
        self.assertFalse(self.server.ssh_clients[0].closed)
        backend.stat("/f.bin")
        self.assertEqual(self.server.connect_calls, 1)

    def test_rsync_mkdir_goes_through_the_pool(self):
        from storage.backends.rsync import RsyncBackend

        backend = RsyncBackend(name="r0", host="example.local", remote_path="/data",
                               username="test", key_filename="/tmp/id_test",
                               connect_timeout=2.0)
        self.addCleanup(backend.close)
        self.server.missing.add("/data/dir")
        with patch.object(backend, "_run_rsync") as run_rsync:
            backend.copy_from_local(SimpleNamespace(), "/dir/f.bin")
        run_rsync.assert_called_once()
        self.assertIn(("mkdir", "/data/dir"),
                      [(name, path) for name, path, *_ in self.server.requests])
        self.assertEqual(self.server.connect_calls, 1)


_GETENT_AHOSTS = (
    "192.0.2.7       STREAM nas.local\n"
    "192.0.2.7       DGRAM  \n"
    "192.0.2.7       RAW    \n"
    "2001:db8::7     STREAM \n"
    "2001:db8::7     DGRAM  \n"
    "fe80::7%eth0    STREAM \n"
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
            ["192.0.2.7", "2001:db8::7", "fe80::7%eth0"],
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
        # The remainder of the shared 4s deadline.
        self.assertTrue(0 < mock_create.call_args.kwargs["timeout"] <= 4.0)

    @patch("storage.backends.ssh.socket.create_connection")
    @patch("storage.backends.ssh.subprocess.run")
    def test_connect_raises_the_last_connect_error_when_none_answers(self, mock_run, mock_create):
        mock_run.return_value = _getent_result(_GETENT_AHOSTS)
        mock_create.side_effect = [
            OSError("first"), OSError("second"), ConnectionRefusedError("third")]
        with self.assertRaises(ConnectionRefusedError):
            ssh_module._connect_via_host_resolver(
                "nas.local", 22, 4.0, socket.gaierror(-3, "lookup"))

    @patch("storage.backends.ssh.socket.create_connection")
    @patch("storage.backends.ssh.subprocess.run")
    def test_getent_and_connects_share_one_deadline(self, mock_run, mock_create):
        mock_run.return_value = _getent_result(_GETENT_AHOSTS)
        clock = SimpleNamespace(now=100.0)
        fake_time = SimpleNamespace(monotonic=lambda: clock.now)

        def _getent(*_args, **_kwargs):
            clock.now += 1.0
            return _getent_result(_GETENT_AHOSTS)

        def _timed_out(_address, timeout):
            clock.now += timeout
            raise socket.timeout("timed out")

        mock_run.side_effect = _getent
        mock_create.side_effect = _timed_out
        with patch.object(ssh_module, "time", fake_time):
            with self.assertRaises(socket.timeout):
                ssh_module._connect_via_host_resolver(
                    "nas.local", 22, 4.0, socket.gaierror(-3, "lookup"))
        # getent took 1s of 4; the first address timed out with the other 3;
        # nothing was left for the rest.
        self.assertEqual([c.kwargs["timeout"] for c in mock_create.call_args_list], [3.0])
        self.assertEqual(clock.now, 104.0)

    @patch("storage.backends.ssh.socket.create_connection")
    @patch("storage.backends.ssh.subprocess.run")
    def test_deadline_with_addresses_untried_raises_timeout_not_the_last_error(self, mock_run, mock_create):
        mock_run.return_value = _getent_result(_GETENT_AHOSTS)
        clock = SimpleNamespace(now=0.0)

        def _refused_at_deadline(_address, timeout):
            clock.now += timeout
            raise ConnectionRefusedError("refused")

        mock_create.side_effect = _refused_at_deadline
        with patch.object(ssh_module, "time", SimpleNamespace(monotonic=lambda: clock.now)):
            with self.assertRaises(socket.timeout) as caught:
                ssh_module._connect_via_host_resolver(
                    "nas.local", 22, 4.0, socket.gaierror(-3, "lookup"))
        # A refusal from one address must not read as "every address refused".
        self.assertIsInstance(caught.exception.__cause__, ConnectionRefusedError)
        self.assertEqual(mock_create.call_count, 1)

    @patch("storage.backends.ssh.socket.create_connection")
    @patch("storage.backends.ssh.subprocess.run")
    def test_getent_using_the_whole_deadline_raises_timeout(self, mock_run, mock_create):
        clock = SimpleNamespace(now=0.0)

        def _slow_getent(*_args, **_kwargs):
            clock.now += 4.0
            return _getent_result(_GETENT_AHOSTS)

        mock_run.side_effect = _slow_getent
        lookup_error = socket.gaierror(-3, "lookup")
        with patch.object(ssh_module, "time", SimpleNamespace(monotonic=lambda: clock.now)):
            with self.assertRaises(socket.timeout) as caught:
                ssh_module._connect_via_host_resolver("nas.local", 22, 4.0, lookup_error)
        mock_create.assert_not_called()
        self.assertIs(caught.exception.__cause__, lookup_error)

    @patch("storage.backends.ssh.subprocess.run")
    def test_undecodable_getent_output_still_parses_addresses(self, mock_run):
        # subprocess decodes with errors="replace"; a replaced name is harmless.
        mock_run.return_value = _getent_result("192.0.2.9 STREAM caf\ufffd.local\n")
        self.assertEqual(ssh_module._host_resolver_addresses("nas.local", 5.0), ["192.0.2.9"])
        self.assertEqual(mock_run.call_args.kwargs["errors"], "replace")

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


class TestSSHBackendHostResolverConnect(_BackendTestCase):

    def _make(self):
        backend = SSHBackend(
            name="ssh0",
            host="nas.local",
            remote_path="/data",
            username="test",
            key_filename="/tmp/id_test",
            port=2222,
            connect_timeout=3.0,
        )
        self.addCleanup(backend.close)
        return backend

    def _fail_lookup_without_sock(self, fail_with_sock=None):
        def error(kwargs):
            if "sock" not in kwargs:
                return socket.gaierror(-3, "Temporary failure in name resolution")
            return fail_with_sock

        self.server.connect_error = error

    @patch("storage.backends.ssh._connect_via_host_resolver")
    def test_lookup_failure_connects_over_host_resolver_socket(self, mock_fallback):
        self._fail_lookup_without_sock()
        sock = _FakeTCPSocket()
        mock_fallback.return_value = sock
        backend = self._make()

        backend.stat("/x")

        self.assertTrue(backend.is_available())
        self.assertEqual(len(self.server.ssh_clients), 1)
        host, port, timeout, lookup_error = mock_fallback.call_args.args
        self.assertEqual((host, port, timeout), ("nas.local", 2222, 3.0))
        self.assertIsInstance(lookup_error, socket.gaierror)
        calls = self.server.ssh_clients[0].connect_kwargs
        self.assertEqual(len(calls), 2)
        # Same hostname (it names the host key) and settings, plus the socket.
        self.assertIs(calls[1].pop("sock"), sock)
        self.assertEqual(calls[1], calls[0])
        self.assertEqual(calls[1]["hostname"], "nas.local")
        self.assertFalse(sock.closed)

    @patch("storage.backends.ssh._connect_via_host_resolver")
    def test_getent_runs_once_per_connect_not_per_thread(self, mock_fallback):
        self._fail_lookup_without_sock()
        mock_fallback.side_effect = lambda *_args: _FakeTCPSocket()
        backend = self._make()

        def work(_index):
            for _ in range(5):
                backend.stat("/x")

        self.assertEqual(_run_threads(8, work), [])
        self.assertEqual(mock_fallback.call_count, 1)

        # A reconnect resolves again: addresses are not cached.
        self.server.transport.close()
        backend.stat("/x")
        self.assertEqual(mock_fallback.call_count, 2)

    @patch("storage.backends.ssh._connect_via_host_resolver")
    def test_handshake_failure_over_fallback_socket_closes_it(self, mock_fallback):
        handshake_error = ssh_module.paramiko.SSHException("banner")
        self._fail_lookup_without_sock(fail_with_sock=handshake_error)
        sock = _FakeTCPSocket()
        mock_fallback.return_value = sock
        backend = self._make()

        with self.assertRaises(ssh_module.paramiko.SSHException):
            backend.stat("/x")
        self.assertTrue(sock.closed)
        self.assertTrue(self.server.ssh_clients[0].closed)
        self.assertFalse(backend.is_available())

    @patch("storage.backends.ssh._connect_via_host_resolver")
    def test_client_close_error_does_not_hide_handshake_error_or_leak_socket(self, mock_fallback):
        handshake_error = ssh_module.paramiko.SSHException("banner")
        self._fail_lookup_without_sock(fail_with_sock=handshake_error)
        original = self.server.new_ssh_client

        def client_with_failing_close():
            client = original()

            def _close_fails():
                raise RuntimeError("close failed")

            client.close = _close_fails
            return client

        sock = _FakeTCPSocket()
        mock_fallback.return_value = sock
        backend = self._make()

        with patch("storage.backends.ssh.paramiko.SSHClient", client_with_failing_close):
            with self.assertRaises(ssh_module.paramiko.SSHException) as caught:
                backend.stat("/x")
        self.assertIs(caught.exception, handshake_error)
        self.assertTrue(sock.closed)

    @patch("storage.backends.ssh._connect_via_host_resolver")
    def test_socket_close_error_does_not_hide_handshake_error(self, mock_fallback):
        handshake_error = ssh_module.paramiko.SSHException("banner")
        self._fail_lookup_without_sock(fail_with_sock=handshake_error)
        sock = _FakeTCPSocket()

        def _close_fails():
            raise OSError("close failed")

        sock.close = _close_fails
        mock_fallback.return_value = sock
        backend = self._make()

        with self.assertRaises(ssh_module.paramiko.SSHException) as caught:
            backend.stat("/x")
        self.assertIs(caught.exception, handshake_error)
        self.assertTrue(self.server.ssh_clients[0].closed)

    @patch("storage.backends.ssh._connect_via_host_resolver")
    def test_unresolvable_host_keeps_lookup_error_and_marks_unavailable(self, mock_fallback):
        self._fail_lookup_without_sock()

        def _no_address(_host, _port, _timeout, lookup_error):
            raise lookup_error

        mock_fallback.side_effect = _no_address
        backend = self._make()

        with self.assertRaises(socket.gaierror):
            backend.stat("/x")
        self.assertFalse(backend.is_available())
        self.assertEqual(len(self.server.ssh_clients[0].connect_kwargs), 1)


if __name__ == "__main__":
    unittest.main()
