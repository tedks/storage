"""
SSH/SFTP backend implementation for remote filesystems
"""

import errno
import os
import shlex
import socket
import stat as stat_module
import logging
import time
import paramiko
from pathlib import Path, PurePosixPath
from typing import List, Optional, Tuple

from .base import Backend, BackendType
from .ssh_transport import SSHTransportManager


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
        super().__init__(name, BackendType.SSH, is_remote=True, speed_priority=speed_priority)

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
        self._transport = SSHTransportManager(
            backend_name=self.name,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            key_filename=self.key_filename,
            connect_timeout=self.connect_timeout,
            sftp_operation_timeout=self.sftp_operation_timeout,
            is_available=self.is_available,
            set_available=self.set_available,
            logger=self.logger,
            paramiko_module=paramiko,
        )
        self._thread_clients = self._transport.thread_clients
        self._thread_clients_lock = self._transport.thread_clients_lock

    @property
    def _next_connect_retry_at(self) -> float:
        return self._transport.next_connect_retry_at

    @_next_connect_retry_at.setter
    def _next_connect_retry_at(self, value: float):
        self._transport.next_connect_retry_at = value

    @staticmethod
    def _close_client_pair(
        ssh_client: Optional[paramiko.SSHClient],
        sftp_client: Optional[paramiko.SFTPClient],
    ):
        SSHTransportManager.close_client_pair(ssh_client, sftp_client)

    def _prune_stale_thread_clients(self, exclude_thread_id: Optional[int] = None):
        self._transport.prune_stale_thread_clients(exclude_thread_id=exclude_thread_id)

    def _get_thread_clients(
        self,
    ) -> Tuple[Optional[paramiko.SSHClient], Optional[paramiko.SFTPClient]]:
        return self._transport.get_thread_clients()

    @staticmethod
    def _transport_active(ssh_client: Optional[paramiko.SSHClient]) -> bool:
        return SSHTransportManager.transport_active(ssh_client)

    def _mark_available(self):
        self._transport.mark_available()

    def _mark_unavailable(self):
        self._transport.mark_unavailable()

    @staticmethod
    def _is_transient_oserror(exc: OSError) -> bool:
        return SSHTransportManager.is_transient_oserror(exc)

    def _connect(self):
        self._transport.connect()

    def _resolve_path(self, virtual_path: str) -> PurePosixPath:
        """Convert virtual path to remote path"""
        relative_path = virtual_path.lstrip('/')
        return self.remote_path / relative_path

    def _ensure_connected(self):
        """Ensure current thread has an active SSH/SFTP connection."""
        self._transport.ensure_connected()

    def _current_clients(self) -> Tuple[paramiko.SSHClient, paramiko.SFTPClient]:
        return self._transport.current_clients()

    def _reset_connection(self):
        """Drop current thread's SSH/SFTP clients so the next operation reconnects."""
        self._transport.reset_connection()

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

    def supports_range_reads(self) -> bool:
        """SSH backend supports offset/size reads via SFTP seek/read."""
        return True

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
        self._transport.close()

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
