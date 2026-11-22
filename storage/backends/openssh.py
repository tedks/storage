"""Owned OpenSSH master generations and attach-only session streams."""

import array
import errno
import os
import select
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Optional

import paramiko

from ._openssh_supervisor import open_pidfd, signal_pidfd
from .ssh_mux import _MAX_CONTROL_PATH, _private_base, default_base_dirs

# OpenSSH PROTOCOL.mux version 4. Passenger clients pass three local file
# descriptors and keep their control socket open for the session's lifetime.
_HELLO = 0x00000001
_NEW_SESSION = 0x10000002
_ALIVE_CHECK = 0x10000004
_PERMISSION_DENIED = 0x80000002
_FAILURE = 0x80000003
_EXIT = 0x80000004
_ALIVE = 0x80000005
_SESSION_OPENED = 0x80000006
_PROTOCOL_VERSION = 4
_REQUEST = 1
_MAX_REPLY = 64 * 1024
_SHUTDOWN_TIMEOUT = 2.0
_SFTP_CLOSE_TIMEOUT = 0.5


class MasterLost(ConnectionError):
    """The generation's control socket no longer answers."""


class MasterStartError(ConnectionError):
    """OpenSSH did not establish a usable authenticated master."""


class SessionRefused(Exception):
    """The server refused to open the requested session."""


class SessionClosed(ConnectionError):
    """The session ended without a remote status or confirmed master loss."""


class MuxProtocolError(ConnectionError):
    """The local OpenSSH process returned an unsupported response."""


def _pack(*values: int) -> bytes:
    return struct.pack(f"!{len(values)}I", *values)


def _string(value: bytes) -> bytes:
    return _pack(len(value)) + value


def _pid_exited(pidfd: int, timeout: float) -> bool:
    poll = select.poll()
    poll.register(pidfd, select.POLLIN)
    return bool(poll.poll(max(0, int(timeout * 1000))))


class _MuxConnection:
    def __init__(self, path: Path, deadline: float):
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._buffer = bytearray()
        try:
            self.socket.settimeout(max(0.0, deadline - time.monotonic()))
            self.socket.connect(os.fspath(path))
            self.send(_pack(_HELLO, _PROTOCOL_VERSION), deadline)
            reply = self.receive(deadline)
            if reply is None:
                raise MasterLost("OpenSSH master closed during mux negotiation")
            if reply[:8] != _pack(_HELLO, _PROTOCOL_VERSION):
                raise MuxProtocolError("OpenSSH mux protocol version 4 is required")
        except BaseException:
            self.close()
            raise

    def send(self, payload: bytes, deadline: float) -> None:
        self.socket.settimeout(max(0.0, deadline - time.monotonic()))
        self.socket.sendall(_string(payload))

    def send_fd(self, fd: int, deadline: float) -> None:
        self.socket.settimeout(max(0.0, deadline - time.monotonic()))
        handles = array.array("i", [fd])
        self.socket.sendmsg([b"\0"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, handles)])

    def receive(self, deadline: float) -> Optional[bytes]:
        while True:
            if len(self._buffer) >= 4:
                size = struct.unpack_from("!I", self._buffer)[0]
                if not 4 <= size <= _MAX_REPLY:
                    raise MuxProtocolError("invalid OpenSSH mux packet length")
                if len(self._buffer) >= 4 + size:
                    payload = bytes(self._buffer[4:4 + size])
                    del self._buffer[:4 + size]
                    return payload
            self.socket.settimeout(max(0.0, deadline - time.monotonic()))
            try:
                data = self.socket.recv(4096)
            except BlockingIOError as exc:
                raise TimeoutError("OpenSSH mux response timed out") from exc
            if not data:
                if self._buffer:
                    raise MasterLost("truncated OpenSSH mux response")
                return None
            self._buffer.extend(data)

    def close(self) -> None:
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.socket.close()


class MuxSession:
    """A command on one generation. Closing cancels its local mux session."""

    def __init__(self, connection: _MuxConnection, session_id: int,
                 generation: "OpenSSHGeneration"):
        self._connection = connection
        self._session_id = session_id
        self._generation = generation
        self._exit: Optional[int] = None
        self._finished = False
        self._wait_lock = threading.Lock()

    def wait(self, timeout: float) -> int:
        deadline = time.monotonic() + timeout
        if not self._wait_lock.acquire(timeout=max(0.0, timeout)):
            raise TimeoutError("OpenSSH session wait timed out")
        try:
            while not self._finished:
                try:
                    reply = self._connection.receive(deadline)
                except TimeoutError:
                    raise
                except OSError as exc:
                    raise self._closed_error() from exc
                if reply is None:
                    if self._exit is None:
                        raise self._closed_error()
                    self._finished = True
                    break
                if len(reply) != 12:
                    raise MuxProtocolError("invalid OpenSSH session response")
                kind, session_id, status = struct.unpack("!III", reply)
                if kind != _EXIT or session_id != self._session_id:
                    raise MuxProtocolError("unexpected OpenSSH session response")
                self._exit = status
            assert self._exit is not None
            return self._exit
        finally:
            self._wait_lock.release()

    def poll(self) -> Optional[int]:
        try:
            return self.wait(0.0)
        except TimeoutError:
            return None

    def _closed_error(self) -> ConnectionError:
        if not self._generation.is_active():
            return MasterLost("OpenSSH master exited during the session")
        return SessionClosed("OpenSSH session ended without an exit status")

    def close(self) -> None:
        self._connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class _SFTPStream(socket.socket):
    def __init__(self, fd: int, session: MuxSession):
        super().__init__(fileno=fd)
        self._session = session
        self._close_lock = threading.Lock()

    def get_name(self) -> str:
        return "OpenSSH SFTP session"

    def close(self) -> None:
        with self._close_lock:
            if self.fileno() < 0:
                return
            try:
                # Let sftp-server exit on EOF and release the sshd session
                # before the caller counts its slot as available again.
                self.shutdown(socket.SHUT_WR)
                self._session.wait(_SFTP_CLOSE_TIMEOUT)
            except OSError:
                # Stalled or already lost. Close the control stream to ask
                # the master to cancel the channel asynchronously.
                pass
            finally:
                self._session.close()
                try:
                    self.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                super().close()


class OpenSSHGeneration:
    """One authenticated master process and its generation-specific socket."""

    def __init__(self, directory, process, supervisor_fd, handoff):
        self._state_lock = threading.Lock()
        self._ready = False
        self._supervisor_fd = supervisor_fd
        self._handoff = handoff
        self._ssh_fd = None
        self._ssh_pid = None
        self._directory = directory
        self._path = Path(directory.name) / "master"
        self._process = process

    def is_active(self) -> bool:
        with self._state_lock:
            return (self._ready and not _pid_exited(self._supervisor_fd, 0)
                    and not _pid_exited(self._ssh_fd, 0))

    def _authorize(self, deadline: float) -> None:
        self._handoff.settimeout(max(0, deadline - time.monotonic()))
        fds = array.array("i")
        payload, ancillary, flags, _ = self._handoff.recvmsg(
            4, socket.CMSG_SPACE(fds.itemsize), socket.MSG_CMSG_CLOEXEC)
        for level, message_type, data in ancillary:
            if level == socket.SOL_SOCKET and message_type == socket.SCM_RIGHTS:
                fds.frombytes(data[:len(data) - len(data) % fds.itemsize])
        if flags & (socket.MSG_CTRUNC | socket.MSG_TRUNC) or len(fds) != 1 or len(payload) != 4:
            for fd in fds:
                os.close(fd)
            raise MasterStartError("OpenSSH supervisor exited without authorizing its child")
        self._ssh_fd = fds[0]
        self._ssh_pid = struct.unpack("!I", payload)[0]
        self._handoff.sendall(b"1")

    def attach_options(self) -> list[str]:
        # OpenSSH's command-line passenger may fall back on refusal or loss.
        # Its local proxy command exits immediately without opening a route.
        return ["-F", os.devnull, "-o", "ControlMaster=no",
                "-o", "ControlPath=" + str(self._path).replace("%", "%%"),
                "-o", "ProxyCommand=exit 255"]

    def _check_ready(self, deadline: float) -> bool:
        connection = _MuxConnection(self._path, deadline)
        try:
            connection.send(_pack(_ALIVE_CHECK, _REQUEST), deadline)
            reply = connection.receive(deadline)
            if reply is None or len(reply) != 12 or reply[:8] != _pack(_ALIVE, _REQUEST):
                return False
            pid = struct.unpack_from("!I", reply, 8)[0]
            return pid == self._ssh_pid
        finally:
            connection.close()

    def _open(self, command: str, fds: tuple[int, int, int],
              subsystem: bool, deadline: float) -> MuxSession:
        connection = None
        try:
            connection = _MuxConnection(self._path, deadline)
            connection.send(
                _pack(_NEW_SESSION, _REQUEST) + _string(b"")
                + _pack(0, 0, 0, int(subsystem), 0xFFFFFFFF)
                + _string(b"") + _string(os.fsencode(command)),
                deadline,
            )
            for fd in fds:
                connection.send_fd(fd, deadline)
            reply = connection.receive(deadline)
            if reply is None:
                raise MasterLost("OpenSSH master closed while opening a session")
            if reply[:8] == _pack(_SESSION_OPENED, _REQUEST) and len(reply) == 12:
                session_id = struct.unpack_from("!I", reply, 8)[0]
                session = MuxSession(connection, session_id, self)
                connection = None
                return session
            if reply[:8] == _pack(_FAILURE, _REQUEST):
                # The reply kind alone covers multiple local failures. This
                # reason is emitted by OpenSSH's session-open confirmation.
                if reply[8:] == _string(b"Session open refused by peer"):
                    raise SessionRefused("the SSH server refused a session")
                raise MuxProtocolError("the OpenSSH master rejected the session request")
            if reply[:8] == _pack(_PERMISSION_DENIED, _REQUEST):
                raise PermissionError("the OpenSSH master denied the session request")
            raise MuxProtocolError("unexpected OpenSSH mux open response")
        except (MuxProtocolError, MasterLost, TimeoutError, PermissionError):
            raise
        except (FileNotFoundError, BrokenPipeError, ConnectionError) as exc:
            raise MasterLost("OpenSSH master is unavailable") from exc
        finally:
            if connection is not None:
                connection.close()

    def open_command(self, command: str, stdin_fd: int, stdout_fd: int,
                     stderr_fd: int, timeout: float) -> MuxSession:
        return self._open(command, (stdin_fd, stdout_fd, stderr_fd),
                          False, time.monotonic() + timeout)

    def _sftp_stream(self, deadline: float) -> socket.socket:
        parent, child = socket.socketpair()
        with parent, child, open(os.devnull, "wb") as stderr:
            session = self._open("sftp", (child.fileno(), child.fileno(), stderr.fileno()),
                                 True, deadline)
            child.close()
            stream = _SFTPStream(parent.detach(), session)
            stream.settimeout(max(0.0, deadline - time.monotonic()))
            return stream

    def open_sftp_stream(self, timeout: float) -> socket.socket:
        """Open the server session before negotiating the SFTP protocol."""
        return self._sftp_stream(time.monotonic() + timeout)

    def open_sftp(self, timeout: float) -> paramiko.SFTPClient:
        stream = self.open_sftp_stream(timeout)
        try:
            return paramiko.SFTPClient(stream)
        except BaseException:
            stream.close()
            raise

    def close(self) -> None:
        with self._state_lock:
            self._ready = False
        if self._supervisor_fd is None:
            return
        # An unacknowledged SSH child cannot dial. Wake it if startup
        # timed out before the descriptor handoff completed.
        try:
            self._handoff.shutdown(socket.SHUT_WR)
        except OSError as exc:
            if exc.errno != errno.ENOTCONN:
                raise
        try:
            signal_pidfd(self._supervisor_fd, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            self._process.wait(timeout=_SHUTDOWN_TIMEOUT)
        except subprocess.TimeoutExpired:
            try:
                signal_pidfd(self._supervisor_fd, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self._process.wait(timeout=_SHUTDOWN_TIMEOUT)
        if self._ssh_fd is not None and not _pid_exited(self._ssh_fd, _SHUTDOWN_TIMEOUT):
            raise MasterLost("the previous OpenSSH child has not exited")
        # The guard keeps the other endpoint until it has either cleaned
        # up the group after supervisor loss, or been killed by the leader.
        self._handoff.settimeout(_SHUTDOWN_TIMEOUT)
        while self._handoff.recv(16):
            pass
        self._handoff.close()
        if self._ssh_fd is not None:
            os.close(self._ssh_fd)
        os.close(self._supervisor_fd)
        self._supervisor_fd = None
        self._directory.cleanup()


class OpenSSHMaster:
    """Lazily start an owned master; serialize replacement and shutdown."""

    def __init__(self, username: Optional[str], host: str, port: Optional[int],
                 key_filename: Optional[str], *, connect_timeout: float = 10.0):
        self._target = f"{username}@{host}" if username else host
        self._port = port
        self._key_filename = key_filename
        self._connect_timeout = connect_timeout
        self._lock = threading.Lock()
        self._generation: Optional[OpenSSHGeneration] = None
        self._closed = False

    def _start(self) -> OpenSSHGeneration:
        directory = None
        for base in default_base_dirs():
            if not _private_base(base):
                continue
            try:
                candidate = tempfile.TemporaryDirectory(prefix="p", dir=base)
            except OSError:
                continue
            if len(os.fsencode(Path(candidate.name) / "master")) <= _MAX_CONTROL_PATH:
                directory = candidate
                break
            candidate.cleanup()
        if directory is None:
            raise MasterStartError("no usable OpenSSH control socket directory")

        path = Path(directory.name) / "master"
        argv = [
            "ssh", "-NnT",
            "-o", "ControlMaster=yes", "-o", "ControlPersist=no",
            "-o", "ControlPath=" + str(path).replace("%", "%%"),
            "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
            "-o", "UpdateHostKeys=no", "-o", "ClearAllForwardings=yes",
            "-o", "ForwardAgent=no", "-o", "ForwardX11=no",
            "-o", "ForkAfterAuthentication=no", "-o", "PermitLocalCommand=no",
            "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=2",
        ]
        if self._port is not None:
            argv.extend(["-p", str(self._port)])
        if self._key_filename:
            argv.extend(["-o", "IdentitiesOnly=yes", "-i", self._key_filename])
        argv.extend(["--", self._target])
        parent = process = None
        try:
            parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
            with child:
                owner_fd = open_pidfd(os.getpid())
                try:
                    supervisor = Path(__file__).with_name("_openssh_supervisor.py")
                    process = subprocess.Popen(
                        [sys.executable, str(supervisor), str(owner_fd), str(child.fileno()),
                         directory.name, *argv],
                        pass_fds=(owner_fd, child.fileno()), stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        start_new_session=True)
                    supervisor_fd = open_pidfd(process.pid)
                finally:
                    os.close(owner_fd)
        except BaseException as exc:
            if parent is not None:
                parent.close()  # No acknowledgment means the child cannot dial.
            if process is not None:
                try:
                    process.wait(timeout=_SHUTDOWN_TIMEOUT)
                except subprocess.TimeoutExpired:
                    pass
            directory.cleanup()
            if isinstance(exc, (AttributeError, OSError)):
                raise MasterStartError("could not start the OpenSSH supervisor") from exc
            raise
        generation = OpenSSHGeneration(directory, process, supervisor_fd, parent)
        self._generation = generation
        deadline = time.monotonic() + self._connect_timeout
        try:
            generation._authorize(deadline)
            while not _pid_exited(supervisor_fd, 0) and time.monotonic() < deadline:
                try:
                    if generation._check_ready(deadline):
                        generation._ready = True
                        return generation
                    raise MasterStartError("OpenSSH master sent an unexpected readiness reply")
                except (FileNotFoundError, ConnectionRefusedError):
                    time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))
                except TimeoutError as exc:
                    raise MasterStartError("OpenSSH master startup timed out") from exc
            raise MasterStartError("OpenSSH master failed to become ready")
        except BaseException:
            generation.close()
            self._generation = None
            raise

    def generation(self) -> OpenSSHGeneration:
        with self._lock:
            if self._closed:
                raise MasterLost("OpenSSH master owner is closed")
            if self._generation is not None and not self._generation.is_active():
                self._generation.close()
                self._generation = None
            if self._generation is None:
                self._generation = self._start()
            return self._generation

    def reset(self, generation: OpenSSHGeneration) -> None:
        with self._lock:
            if generation is self._generation:
                generation.close()
                self._generation = None

    def close(self) -> None:
        with self._lock:
            self._closed = True
            if self._generation is not None:
                self._generation.close()
                self._generation = None
