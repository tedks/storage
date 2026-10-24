"""
SSH/SFTP backend implementation for remote filesystems
"""

import errno
import os
import shlex
import socket
import stat as stat_module
import logging
import threading
import time
import weakref
import paramiko
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Tuple

from .base import Backend, BackendType, StorageMemberCapabilities, WatcherSpec


class SSHBackend(Backend):
    """Backend for SSH/SFTP remote filesystems"""

    def __init__(self, name: str, host: str, remote_path: str,
                 username: str, password: Optional[str] = None,
                 key_filename: Optional[str] = None,
                 port: int = 22, speed_priority: int = 50,
                 connect_timeout: float = 10.0):
        """
        Initialize SSH backend

        Args:
            name: Backend identifier
            host: Remote host address
            remote_path: Root directory on remote host
            username: SSH username
            password: SSH password (optional if using key)
            key_filename: Path to private key file
            port: SSH port (default 22)
            speed_priority: Speed priority (default 50 for remote)
            connect_timeout: Timeout in seconds for SSH connection/auth handshakes
        """
        super().__init__(
            name,
            BackendType.SSH,
            is_remote=True,
            speed_priority=speed_priority,
            capabilities=StorageMemberCapabilities(
                range_reads=True,
                transient_connectivity=True,
            ),
        )

        self.host = host
        self.port = port
        self.remote_path = PurePosixPath(remote_path)
        self.username = username
        self.password = password
        self.key_filename = key_filename
        self.connect_timeout = connect_timeout
        # Keep I/O timeout higher than connect timeout to avoid aborting large/slow reads.
        self.sftp_operation_timeout = max(60.0, float(connect_timeout) * 6.0)
        self.logger = logging.getLogger(__name__)

        # Per-thread SSH/SFTP connections (lazy connected). This avoids
        # cross-thread contention and prevents one thread from closing a
        # live channel owned by another thread.
        self._thread_local = threading.local()
        self._thread_clients: Dict[int, Tuple[paramiko.SSHClient, paramiko.SFTPClient, weakref.ReferenceType[threading.Thread]]] = {}
        self._thread_clients_lock = threading.Lock()
        self._availability_lock = threading.Lock()
        self._probe_lock = threading.Lock()
        self._reconnect_probe_interval = max(1.0, min(5.0, float(connect_timeout) / 2.0))
        self._next_connect_retry_at = 0.0

    @staticmethod
    def _close_client_pair(ssh_client: Optional[paramiko.SSHClient], sftp_client: Optional[paramiko.SFTPClient]):
        try:
            if sftp_client:
                sftp_client.close()
        except Exception:
            pass
        try:
            if ssh_client:
                ssh_client.close()
        except Exception:
            pass

    def _prune_stale_thread_clients(self, exclude_thread_id: Optional[int] = None):
        stale_clients: List[Tuple[paramiko.SSHClient, paramiko.SFTPClient]] = []

        with self._thread_clients_lock:
            for thread_id, entry in list(self._thread_clients.items()):
                if exclude_thread_id is not None and thread_id == exclude_thread_id:
                    continue

                ssh_client, sftp_client, thread_ref = entry
                thread_obj = thread_ref()
                if thread_obj is None or not thread_obj.is_alive():
                    self._thread_clients.pop(thread_id, None)
                    stale_clients.append((ssh_client, sftp_client))

        for ssh_client, sftp_client in stale_clients:
            self._close_client_pair(ssh_client, sftp_client)

    def _set_thread_clients(self, ssh_client: paramiko.SSHClient, sftp_client: paramiko.SFTPClient):
        thread_id = threading.get_ident()
        self._prune_stale_thread_clients(exclude_thread_id=thread_id)

        old_pair: Optional[Tuple[paramiko.SSHClient, paramiko.SFTPClient]] = None
        thread_ref = weakref.ref(threading.current_thread())
        with self._thread_clients_lock:
            previous = self._thread_clients.get(thread_id)
            if previous is not None:
                old_pair = (previous[0], previous[1])
            self._thread_clients[thread_id] = (ssh_client, sftp_client, thread_ref)

        self._thread_local.ssh_client = ssh_client
        self._thread_local.sftp_client = sftp_client

        if old_pair is not None and old_pair != (ssh_client, sftp_client):
            self._close_client_pair(old_pair[0], old_pair[1])

    def _get_thread_clients(self) -> Tuple[Optional[paramiko.SSHClient], Optional[paramiko.SFTPClient]]:
        ssh_client = getattr(self._thread_local, "ssh_client", None)
        sftp_client = getattr(self._thread_local, "sftp_client", None)
        return ssh_client, sftp_client

    @staticmethod
    def _transport_active(ssh_client: Optional[paramiko.SSHClient]) -> bool:
        if ssh_client is None:
            return False
        try:
            transport = ssh_client.get_transport()
            return bool(transport and transport.is_active())
        except Exception:
            return False

    def _mark_available(self):
        with self._availability_lock:
            self._next_connect_retry_at = 0.0
        self.set_available(True)

    def _mark_unavailable(self):
        with self._availability_lock:
            self._next_connect_retry_at = max(
                self._next_connect_retry_at,
                time.monotonic() + self._reconnect_probe_interval,
            )
        self.set_available(False)

    @staticmethod
    def _is_transient_oserror(exc: OSError) -> bool:
        """Return True when an OSError likely indicates connection loss."""
        transient_errnos = {
            errno.ECONNABORTED,
            errno.ECONNRESET,
            errno.ENETRESET,
            errno.ESHUTDOWN,
            errno.EHOSTDOWN,
            errno.EHOSTUNREACH,
            errno.ENETDOWN,
            errno.ENETUNREACH,
            errno.ETIMEDOUT,
            errno.EPIPE,
        }
        if exc.errno in transient_errnos:
            return True

        message = str(exc).lower()
        markers = (
            "socket is closed",
            "connection reset",
            "connection timed out",
            "broken pipe",
            "connection aborted",
            "eof",
        )
        return any(marker in message for marker in markers)

    def _connect(self):
        """Establish per-thread SSH/SFTP connection."""
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_kwargs = {
                'hostname': self.host,
                'port': self.port,
                'username': self.username,
                'timeout': self.connect_timeout,
                'banner_timeout': self.connect_timeout,
                'auth_timeout': self.connect_timeout,
            }

            if self.password:
                connect_kwargs['password'] = self.password
            if self.key_filename:
                connect_kwargs['key_filename'] = self.key_filename
            else:
                # Prevent agent/key probing delays when no explicit key provided
                connect_kwargs['allow_agent'] = False
                connect_kwargs['look_for_keys'] = False

            ssh_client.connect(**connect_kwargs)
            sftp_client = ssh_client.open_sftp()

            # Keep SFTP socket/channel timeouts above connect timeout to tolerate
            # large files and slower links without spurious range-read aborts.
            transport = ssh_client.get_transport()
            if transport and transport.sock:
                try:
                    transport.sock.settimeout(self.sftp_operation_timeout)
                except Exception:
                    # Best-effort; SFTP may still function without socket-level timeout
                    pass

            try:
                chan = sftp_client.get_channel()
                if chan:
                    chan.settimeout(self.sftp_operation_timeout)
            except Exception:
                pass

            self._set_thread_clients(ssh_client, sftp_client)
            self._mark_available()

        except paramiko.AuthenticationException as e:
            self.logger.error("SSH authentication failed for %s@%s:%s: %s",
                              self.username, self.host, self.port, e)
            self._mark_unavailable()
            raise
        except paramiko.SSHException as e:
            self.logger.error("SSH handshake failed for %s@%s:%s: %s",
                              self.username, self.host, self.port, e)
            self._mark_unavailable()
            raise
        except Exception as e:
            self.logger.error("Failed to connect to %s@%s:%s: %s",
                              self.username, self.host, self.port, e)
            self._mark_unavailable()
            raise

    def _resolve_path(self, virtual_path: str) -> PurePosixPath:
        """Convert virtual path to remote path"""
        relative_path = virtual_path.lstrip('/')
        return self.remote_path / relative_path

    def push_invalidation_spec(self) -> Optional[WatcherSpec]:
        return WatcherSpec(
            group_key=(self.host, self.port, self.username, self.key_filename),
            ssh_host=self.host,
            ssh_port=self.port,
            username=self.username,
            key_filename=self.key_filename,
            remote_path=str(self.remote_path),
        )

    def _ensure_connected(self):
        """Ensure current thread has an active SSH/SFTP connection."""
        ssh_client, sftp_client = self._get_thread_clients()
        if sftp_client is not None and self._transport_active(ssh_client):
            return

        with self._availability_lock:
            is_available = self.is_available()
            next_retry_at = self._next_connect_retry_at

        if not is_available and time.monotonic() < next_retry_at:
            raise ConnectionError(
                f"SSH backend {self.name} is temporarily unavailable after a disconnect"
            )

        # Single-probe reconnect gate: one thread probes; others fail fast.
        if not self._probe_lock.acquire(blocking=False):
            raise ConnectionError(
                f"SSH backend {self.name} reconnect probe already in progress"
            )
        try:
            # Recheck after acquiring probe lock to avoid duplicate probes.
            with self._availability_lock:
                is_available = self.is_available()
                next_retry_at = self._next_connect_retry_at

            if not is_available and time.monotonic() < next_retry_at:
                raise ConnectionError(
                    f"SSH backend {self.name} is temporarily unavailable after a disconnect"
                )

            self._reset_connection()
            self._connect()
        finally:
            self._probe_lock.release()

    def _current_clients(self) -> Tuple[paramiko.SSHClient, paramiko.SFTPClient]:
        self._prune_stale_thread_clients(exclude_thread_id=threading.get_ident())
        self._ensure_connected()
        ssh_client, sftp_client = self._get_thread_clients()
        if ssh_client is None or sftp_client is None:
            raise RuntimeError("SSH backend has no active thread client")
        return ssh_client, sftp_client

    def _reset_connection(self):
        """Drop current thread's SSH/SFTP clients so the next operation reconnects."""
        thread_id = threading.get_ident()
        ssh_client, sftp_client = self._get_thread_clients()

        with self._thread_clients_lock:
            pair = self._thread_clients.pop(thread_id, None)
            if pair:
                ssh_client = pair[0]
                sftp_client = pair[1]

        self._close_client_pair(ssh_client, sftp_client)

        self._thread_local.ssh_client = None
        self._thread_local.sftp_client = None

    def exists(self, virtual_path: str) -> bool:
        """Check if a file exists on remote"""
        remote_path = str(self._resolve_path(virtual_path))

        for attempt in range(2):
            try:
                _, sftp_client = self._current_clients()
                sftp_client.stat(remote_path)
                return True
            except FileNotFoundError:
                return False
            except (socket.timeout, EOFError, paramiko.SSHException) as exc:
                self.logger.warning(
                    "SFTP exists() failed on %s attempt=%d/2: %r",
                    virtual_path,
                    attempt + 1,
                    exc,
                )
                self._reset_connection()
                if attempt == 1:
                    self._mark_unavailable()
                    return False
            except OSError as exc:
                if not self._is_transient_oserror(exc):
                    self.logger.warning("Error checking existence of %s: %r", virtual_path, exc)
                    return False
                self.logger.warning(
                    "SFTP exists() failed on %s attempt=%d/2: %r",
                    virtual_path,
                    attempt + 1,
                    exc,
                )
                self._reset_connection()
                if attempt == 1:
                    self._mark_unavailable()
                    return False
            except Exception as exc:
                self.logger.warning("Error checking existence of %s: %r", virtual_path, exc)
                return False

        return False

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        """Read data from remote file with one reconnect/retry on transport errors."""
        remote_path = str(self._resolve_path(virtual_path))

        last_exc = None
        for attempt in range(2):
            try:
                _, sftp_client = self._current_clients()
                with sftp_client.open(remote_path, 'rb') as f:
                    f.seek(offset)
                    return f.read(size)
            except FileNotFoundError:
                raise
            except (socket.timeout, EOFError, paramiko.SSHException) as exc:
                last_exc = exc
                self.logger.warning(
                    "SFTP read failed on %s (offset=%d size=%d attempt=%d/2): %r",
                    virtual_path,
                    offset,
                    size,
                    attempt + 1,
                    exc,
                )
                self._reset_connection()
                if attempt == 1:
                    self._mark_unavailable()
                    raise
            except OSError as exc:
                if not self._is_transient_oserror(exc):
                    raise
                last_exc = exc
                self.logger.warning(
                    "SFTP read failed on %s (offset=%d size=%d attempt=%d/2): %r",
                    virtual_path,
                    offset,
                    size,
                    attempt + 1,
                    exc,
                )
                self._reset_connection()
                if attempt == 1:
                    self._mark_unavailable()
                    raise

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"read_file failed unexpectedly for {virtual_path}")

    def write_file(self, virtual_path: str, data: bytes, offset: int):
        """Write data to remote file"""
        _, sftp_client = self._current_clients()
        remote_path = str(self._resolve_path(virtual_path))

        # Ensure parent directory exists
        parent = str(PurePosixPath(remote_path).parent)
        try:
            sftp_client.stat(parent)
        except FileNotFoundError:
            self._mkdir_recursive(parent, sftp_client=sftp_client)

        # Open or create file. exists() may reconnect, so reacquire client afterward.
        mode = 'r+b' if self.exists(virtual_path) else 'wb'
        _, sftp_client = self._current_clients()

        with sftp_client.open(remote_path, mode) as f:
            f.seek(offset)
            f.write(data)

    def create_file(self, virtual_path: str):
        """Create a new empty file on remote"""
        _, sftp_client = self._current_clients()
        remote_path = str(self._resolve_path(virtual_path))

        # Ensure parent directory exists
        parent = str(PurePosixPath(remote_path).parent)
        try:
            sftp_client.stat(parent)
        except FileNotFoundError:
            self._mkdir_recursive(parent, sftp_client=sftp_client)

        # Create empty file
        with sftp_client.open(remote_path, 'wb'):
            pass

    def delete_file(self, virtual_path: str):
        """Delete a file from remote"""
        _, sftp_client = self._current_clients()
        remote_path = str(self._resolve_path(virtual_path))

        try:
            sftp_client.remove(remote_path)
        except FileNotFoundError:
            pass

    def truncate_file(self, virtual_path: str):
        """Truncate a file to zero length"""
        _, sftp_client = self._current_clients()
        remote_path = str(self._resolve_path(virtual_path))

        with sftp_client.open(remote_path, 'wb') as f:
            f.truncate(0)

    def get_size(self, virtual_path: str) -> int:
        """Get file size"""
        remote_path = str(self._resolve_path(virtual_path))
        try:
            _, sftp_client = self._current_clients()
            stat_result = sftp_client.stat(remote_path)
            return stat_result.st_size
        except FileNotFoundError:
            raise
        except (socket.timeout, EOFError, paramiko.SSHException) as exc:
            self.logger.warning("SFTP get_size failed on %s: %r", virtual_path, exc)
            self._mark_unavailable()
            self._reset_connection()
            raise
        except OSError as exc:
            if self._is_transient_oserror(exc):
                self.logger.warning("SFTP get_size failed on %s: %r", virtual_path, exc)
                self._mark_unavailable()
                self._reset_connection()
            raise

    def mkdir(self, virtual_path: str):
        """Create a directory on remote"""
        _, sftp_client = self._current_clients()
        remote_path = str(self._resolve_path(virtual_path))
        self._mkdir_recursive(remote_path, sftp_client=sftp_client)

    def _mkdir_recursive(self, remote_path: str, sftp_client: Optional[paramiko.SFTPClient] = None):
        """Recursively create directories on remote"""
        if sftp_client is None:
            _, sftp_client = self._current_clients()

        try:
            sftp_client.stat(remote_path)
            return  # Already exists
        except FileNotFoundError:
            pass

        # Try to create parent first
        parent = str(PurePosixPath(remote_path).parent)
        if parent != remote_path:  # Not root
            self._mkdir_recursive(parent, sftp_client=sftp_client)

        # Create this directory
        try:
            sftp_client.mkdir(remote_path)
        except OSError:
            pass  # May already exist due to race condition

    def rmdir(self, virtual_path: str):
        """Remove a directory from remote"""
        _, sftp_client = self._current_clients()
        remote_path = str(self._resolve_path(virtual_path))

        try:
            sftp_client.rmdir(remote_path)
        except FileNotFoundError:
            pass

    def listdir(self, virtual_path: str) -> List[str]:
        """List directory contents on remote"""
        remote_path = str(self._resolve_path(virtual_path))

        try:
            _, sftp_client = self._current_clients()
            return sftp_client.listdir(remote_path)
        except FileNotFoundError:
            return []
        except (socket.timeout, EOFError, paramiko.SSHException) as exc:
            self.logger.warning("SFTP listdir failed on %s: %r", virtual_path, exc)
            self._mark_unavailable()
            self._reset_connection()
            raise
        except OSError as exc:
            if self._is_transient_oserror(exc):
                self.logger.warning("SFTP listdir failed on %s: %r", virtual_path, exc)
                self._mark_unavailable()
                self._reset_connection()
            raise

    def listdir_attrs(self, virtual_path: str):
        """List directory contents with attributes using SFTP listdir_attr."""
        remote_path = str(self._resolve_path(virtual_path))
        try:
            _, sftp_client = self._current_clients()
            attrs = sftp_client.listdir_attr(remote_path)
            result = []
            for attr in attrs:
                # attr.filename may be bytes on some servers
                name = attr.filename
                if isinstance(name, bytes):
                    name = name.decode()
                result.append((name, {
                    'st_mode': getattr(attr, 'st_mode', None),
                    'st_size': getattr(attr, 'st_size', 0),
                    'st_mtime': getattr(attr, 'st_mtime', time.time()),
                    'st_atime': getattr(attr, 'st_atime', None) or getattr(attr, 'st_mtime', time.time()),
                    'st_gid': getattr(attr, 'st_gid', os.getgid()),
                    'st_uid': getattr(attr, 'st_uid', os.getuid()),
                    'st_nlink': getattr(attr, 'st_nlink', 1),
                }))
            return result
        except (socket.timeout, EOFError, paramiko.SSHException) as exc:
            self.logger.warning("SFTP listdir_attrs failed on %s: %r", virtual_path, exc)
            self._mark_unavailable()
            self._reset_connection()
            return None
        except OSError as exc:
            if self._is_transient_oserror(exc):
                self.logger.warning("SFTP listdir_attrs failed on %s: %r", virtual_path, exc)
                self._mark_unavailable()
                self._reset_connection()
            return None
        except Exception:
            return None

    def isdir(self, virtual_path: str) -> bool:
        """Check if path is a directory on remote"""
        try:
            _, sftp_client = self._current_clients()
            remote_path = str(self._resolve_path(virtual_path))
            stat_result = sftp_client.stat(remote_path)
            return stat_module.S_ISDIR(stat_result.st_mode)
        except (socket.timeout, EOFError, paramiko.SSHException) as exc:
            self.logger.warning("SFTP isdir failed on %s: %r", virtual_path, exc)
            self._mark_unavailable()
            self._reset_connection()
            return False
        except OSError as exc:
            if self._is_transient_oserror(exc):
                self.logger.warning("SFTP isdir failed on %s: %r", virtual_path, exc)
                self._mark_unavailable()
                self._reset_connection()
            return False
        except Exception:
            return False

    def copy_to_local(self, virtual_path: str, local_path: Path):
        """Copy a file from remote to local path"""
        remote_path = str(self._resolve_path(virtual_path))

        # Ensure local parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            _, sftp_client = self._current_clients()
            # Download file
            sftp_client.get(remote_path, str(local_path))
        except FileNotFoundError:
            raise
        except (socket.timeout, EOFError, paramiko.SSHException) as exc:
            self.logger.warning("SFTP copy_to_local failed on %s: %r", virtual_path, exc)
            self._mark_unavailable()
            self._reset_connection()
            raise
        except OSError as exc:
            if self._is_transient_oserror(exc):
                self.logger.warning("SFTP copy_to_local failed on %s: %r", virtual_path, exc)
                self._mark_unavailable()
                self._reset_connection()
            raise

    def copy_from_local(self, local_path: Path, virtual_path: str):
        """Copy a local file to remote"""
        _, sftp_client = self._current_clients()
        remote_path = str(self._resolve_path(virtual_path))

        # Ensure remote parent directory exists
        parent = str(PurePosixPath(remote_path).parent)
        try:
            sftp_client.stat(parent)
        except FileNotFoundError:
            self._mkdir_recursive(parent, sftp_client=sftp_client)

        # Upload file
        sftp_client.put(str(local_path), remote_path)

    def rename(self, old_virtual_path: str, new_virtual_path: str):
        """Rename a file or directory on the remote backend."""
        _, sftp_client = self._current_clients()

        old_remote = str(self._resolve_path(old_virtual_path))
        new_remote = str(self._resolve_path(new_virtual_path))
        parent = str(PurePosixPath(new_remote).parent)

        try:
            sftp_client.stat(parent)
        except FileNotFoundError:
            self._mkdir_recursive(parent, sftp_client=sftp_client)

        try:
            sftp_client.rename(old_remote, new_remote)
        except FileNotFoundError:
            # Nothing to rename; best-effort
            pass

    def get_free_space(self) -> Optional[int]:
        """Get free space on remote (if supported)"""
        # SFTP doesn't have a standard way to get free space
        # Would need to execute df command via SSH
        return None

    def sync(self):
        """Sync any buffered writes"""
        # SFTP writes are synchronous
        pass

    def stat(self, virtual_path: str):
        remote_path = str(self._resolve_path(virtual_path))
        try:
            _, sftp_client = self._current_clients()
            return sftp_client.stat(remote_path)
        except FileNotFoundError:
            raise
        except (socket.timeout, EOFError, paramiko.SSHException) as exc:
            self.logger.warning("SFTP stat failed on %s: %r", virtual_path, exc)
            self._mark_unavailable()
            self._reset_connection()
            raise
        except OSError as exc:
            if self._is_transient_oserror(exc):
                self.logger.warning("SFTP stat failed on %s: %r", virtual_path, exc)
                self._mark_unavailable()
                self._reset_connection()
            raise

    def close(self):
        """Close SSH/SFTP connections"""
        with self._thread_clients_lock:
            clients = [(entry[0], entry[1]) for entry in self._thread_clients.values()]
            self._thread_clients.clear()

        for ssh_client, sftp_client in clients:
            self._close_client_pair(ssh_client, sftp_client)

        self._thread_local.ssh_client = None
        self._thread_local.sftp_client = None
        self._mark_unavailable()

    def __del__(self):
        """Cleanup on deletion"""
        self.close()

    def __repr__(self):
        return f"SSHBackend(name={self.name}, host={self.host}, remote_path={self.remote_path})"

    # ------------------------------------------------------------------ #
    # Optional capabilities used by conflict detection
    # ------------------------------------------------------------------ #
    def _shell_quote(self, value: str) -> str:
        return shlex.quote(value)

    def get_fast_checksum(self, virtual_path: str) -> str:
        """
        Compute checksum without downloading the full file.

        Tries xxh128sum on the remote (matches local xxh3_128).
        Falls back to downloading a temp copy and checksumming locally
        so the result is always consistent with local checksums.
        """
        ssh_client, _ = self._current_clients()
        remote_path = str(self._resolve_path(virtual_path))

        # Try remote xxh128sum first (fast path, avoids download).
        # Use sftp_operation_timeout (not connect_timeout) since hashing
        # large files can take much longer than a connection handshake.
        # Wrap in try/except so SSH channel errors (timeout, disconnect)
        # fall through to the download-and-hash fallback.
        try:
            cmd = f"xxh128sum {self._shell_quote(remote_path)} 2>/dev/null"
            _, stdout, stderr = ssh_client.exec_command(
                cmd, timeout=self.sftp_operation_timeout
            )
            rc = stdout.channel.recv_exit_status()
            output = stdout.read().decode().strip()
            if rc == 0 and output:
                token = output.split()[0]
                # xxh128sum outputs a 32-char hex digest; validate format.
                if len(token) == 32 and all(c in "0123456789abcdef" for c in token):
                    return token
        except Exception:
            # exec_command itself failed (timeout, channel error, etc.)
            # Fall through to download-and-hash.
            pass

        # Fallback: download to temp file and compute locally.
        import tempfile
        from ..checksums import fast_checksum_file
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            self.copy_to_local(virtual_path, tmp_path)
            return fast_checksum_file(tmp_path)
        finally:
            try:
                tmp_path.unlink()
            except OSError:
                pass

    def set_xattr(self, virtual_path: str, name: str, value: bytes):
        """Best-effort remote setfattr."""
        try:
            ssh_client, _ = self._current_clients()
            remote_path = str(self._resolve_path(virtual_path))
            value_str = value.decode() if isinstance(value, (bytes, bytearray)) else str(value)
            cmd = (
                f"setfattr -n {self._shell_quote(name)} "
                f"-v {self._shell_quote(value_str)} {self._shell_quote(remote_path)}"
            )
            ssh_client.exec_command(cmd, timeout=self.connect_timeout)
        except Exception:
            # Remote might not support xattrs; ignore
            pass

    def get_xattr(self, virtual_path: str, name: str):
        try:
            ssh_client, _ = self._current_clients()
            remote_path = str(self._resolve_path(virtual_path))
            cmd = (
                f"getfattr --absolute-names --only-values "
                f"-n {self._shell_quote(name)} {self._shell_quote(remote_path)}"
            )
            _, stdout, _ = ssh_client.exec_command(
                cmd, timeout=self.connect_timeout
            )
            data = stdout.read()
            return data if data else None
        except Exception:
            return None
