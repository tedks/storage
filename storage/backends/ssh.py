"""
SSH/SFTP backend implementation for remote filesystems
"""

import os
import shlex
import logging
import paramiko
from pathlib import Path, PurePosixPath
from typing import List, Optional

from .base import Backend, BackendType


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
            connect_timeout: Timeout in seconds for SSH banner/connect and SFTP socket ops
        """
        super().__init__(name, BackendType.SSH, is_remote=True, speed_priority=speed_priority)

        self.host = host
        self.port = port
        self.remote_path = PurePosixPath(remote_path)
        self.username = username
        self.password = password
        self.key_filename = key_filename
        self.connect_timeout = connect_timeout
        self.logger = logging.getLogger(__name__)

        # SSH/SFTP connections (lazy connected)
        self._ssh_client: Optional[paramiko.SSHClient] = None
        self._sftp_client: Optional[paramiko.SFTPClient] = None

    def _connect(self):
        """Establish SSH/SFTP connection"""
        try:
            self._ssh_client = paramiko.SSHClient()
            self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

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
                connect_kwargs['allow_agent'] = False
                connect_kwargs['look_for_keys'] = False

            self._ssh_client.connect(**connect_kwargs)
            self._sftp_client = self._ssh_client.open_sftp()

            # Ensure the underlying socket respects the timeout for SFTP ops
            transport = self._ssh_client.get_transport()
            if transport and transport.sock:
                try:
                    transport.sock.settimeout(self.connect_timeout)
                except Exception:
                    # Best-effort; SFTP may still function without socket-level timeout
                    pass
            # Also set channel-level timeout so listdir/stat calls don't hang forever
            try:
                chan = self._sftp_client.get_channel()
                if chan:
                    chan.settimeout(self.connect_timeout)
            except Exception:
                pass

            self.set_available(True)

        except paramiko.AuthenticationException as e:
            self.logger.error("SSH authentication failed for %s@%s:%s: %s", self.username, self.host, self.port, e)
            self.set_available(False)
            raise
        except paramiko.SSHException as e:
            self.logger.error("SSH handshake failed for %s@%s:%s: %s", self.username, self.host, self.port, e)
            self.set_available(False)
            raise
        except Exception as e:
            self.logger.error("Failed to connect to %s@%s:%s: %s", self.username, self.host, self.port, e)
            self.set_available(False)
            raise

    def _resolve_path(self, virtual_path: str) -> PurePosixPath:
        """Convert virtual path to remote path"""
        relative_path = virtual_path.lstrip('/')
        return self.remote_path / relative_path

    def _ensure_connected(self):
        """Ensure connection is active, reconnect if needed"""
        if not self._sftp_client or not self._ssh_client:
            self._connect()

    def exists(self, virtual_path: str) -> bool:
        """Check if a file exists on remote"""
        if not self.is_available():
            return False

        try:
            self._ensure_connected()
            remote_path = str(self._resolve_path(virtual_path))
            self._sftp_client.stat(remote_path)
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            print(f"Error checking existence of {virtual_path}: {e}")
            return False

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        """Read data from remote file"""
        self._ensure_connected()

        remote_path = str(self._resolve_path(virtual_path))

        with self._sftp_client.open(remote_path, 'rb') as f:
            f.seek(offset)
            return f.read(size)

    def write_file(self, virtual_path: str, data: bytes, offset: int):
        """Write data to remote file"""
        self._ensure_connected()

        remote_path = str(self._resolve_path(virtual_path))

        # Ensure parent directory exists
        parent = str(PurePosixPath(remote_path).parent)
        try:
            self._sftp_client.stat(parent)
        except FileNotFoundError:
            self._mkdir_recursive(parent)

        # Open or create file
        mode = 'r+b' if self.exists(virtual_path) else 'wb'

        with self._sftp_client.open(remote_path, mode) as f:
            f.seek(offset)
            f.write(data)

    def create_file(self, virtual_path: str):
        """Create a new empty file on remote"""
        self._ensure_connected()

        remote_path = str(self._resolve_path(virtual_path))

        # Ensure parent directory exists
        parent = str(PurePosixPath(remote_path).parent)
        try:
            self._sftp_client.stat(parent)
        except FileNotFoundError:
            self._mkdir_recursive(parent)

        # Create empty file
        with self._sftp_client.open(remote_path, 'wb') as f:
            pass

    def delete_file(self, virtual_path: str):
        """Delete a file from remote"""
        self._ensure_connected()

        remote_path = str(self._resolve_path(virtual_path))

        try:
            self._sftp_client.remove(remote_path)
        except FileNotFoundError:
            pass

    def truncate_file(self, virtual_path: str):
        """Truncate a file to zero length"""
        self._ensure_connected()

        remote_path = str(self._resolve_path(virtual_path))

        with self._sftp_client.open(remote_path, 'wb') as f:
            f.truncate(0)

    def get_size(self, virtual_path: str) -> int:
        """Get file size"""
        self._ensure_connected()

        remote_path = str(self._resolve_path(virtual_path))
        stat = self._sftp_client.stat(remote_path)
        return stat.st_size

    def mkdir(self, virtual_path: str):
        """Create a directory on remote"""
        self._ensure_connected()

        remote_path = str(self._resolve_path(virtual_path))
        self._mkdir_recursive(remote_path)

    def _mkdir_recursive(self, remote_path: str):
        """Recursively create directories on remote"""
        try:
            self._sftp_client.stat(remote_path)
            return  # Already exists
        except FileNotFoundError:
            pass

        # Try to create parent first
        parent = str(PurePosixPath(remote_path).parent)
        if parent != remote_path:  # Not root
            self._mkdir_recursive(parent)

        # Create this directory
        try:
            self._sftp_client.mkdir(remote_path)
        except OSError:
            pass  # May already exist due to race condition

    def rmdir(self, virtual_path: str):
        """Remove a directory from remote"""
        self._ensure_connected()

        remote_path = str(self._resolve_path(virtual_path))

        try:
            self._sftp_client.rmdir(remote_path)
        except FileNotFoundError:
            pass

    def listdir(self, virtual_path: str) -> List[str]:
        """List directory contents on remote"""
        self._ensure_connected()

        remote_path = str(self._resolve_path(virtual_path))

        try:
            return self._sftp_client.listdir(remote_path)
        except FileNotFoundError:
            return []

    def listdir_attrs(self, virtual_path: str):
        """List directory contents with attributes to avoid per-entry stat."""
        self._ensure_connected()
        remote_path = str(self._resolve_path(virtual_path))
        try:
            attrs = self._sftp_client.listdir_attr(remote_path)
            result = []
            for attr in attrs:
                # attr.filename is bytes on some servers; ensure str
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
        except Exception:
            return None

    def isdir(self, virtual_path: str) -> bool:
        """Check if path is a directory on remote"""
        if not self.is_available():
            return False

        try:
            self._ensure_connected()
            remote_path = str(self._resolve_path(virtual_path))
            stat = self._sftp_client.stat(remote_path)
            import stat as stat_module
            return stat_module.S_ISDIR(stat.st_mode)
        except:
            return False

    def copy_to_local(self, virtual_path: str, local_path: Path):
        """Copy a file from remote to local path"""
        self._ensure_connected()

        remote_path = str(self._resolve_path(virtual_path))

        # Ensure local parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Download file
        self._sftp_client.get(remote_path, str(local_path))

    def copy_from_local(self, local_path: Path, virtual_path: str):
        """Copy a local file to remote"""
        self._ensure_connected()

        remote_path = str(self._resolve_path(virtual_path))

        # Ensure remote parent directory exists
        parent = str(PurePosixPath(remote_path).parent)
        try:
            self._sftp_client.stat(parent)
        except FileNotFoundError:
            self._mkdir_recursive(parent)

        # Upload file
        self._sftp_client.put(str(local_path), remote_path)

    def rename(self, old_virtual_path: str, new_virtual_path: str):
        """Rename a file or directory on the remote backend."""
        self._ensure_connected()

        old_remote = str(self._resolve_path(old_virtual_path))
        new_remote = str(self._resolve_path(new_virtual_path))
        parent = str(PurePosixPath(new_remote).parent)

        try:
            self._sftp_client.stat(parent)
        except FileNotFoundError:
            self._mkdir_recursive(parent)

        try:
            self._sftp_client.rename(old_remote, new_remote)
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
        self._ensure_connected()
        remote_path = str(self._resolve_path(virtual_path))
        return self._sftp_client.stat(remote_path)

    def close(self):
        """Close SSH/SFTP connections"""
        if self._sftp_client:
            self._sftp_client.close()
            self._sftp_client = None

        if self._ssh_client:
            self._ssh_client.close()
            self._ssh_client = None

        self.set_available(False)

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
        Compute checksum on the remote side without downloading the full file.

        Falls back to SHA256 via ssh exec. If that fails, raises an exception.
        """
        self._ensure_connected()
        remote_path = str(self._resolve_path(virtual_path))
        cmd = f"sha256sum {self._shell_quote(remote_path)}"
        _, stdout, stderr = self._ssh_client.exec_command(
            cmd, timeout=self.connect_timeout
        )
        output = stdout.read().decode().strip()
        if not output:
            raise RuntimeError(f"checksum command failed: {stderr.read().decode()}")
        return output.split()[0]

    def set_xattr(self, virtual_path: str, name: str, value: bytes):
        """Best-effort remote setfattr."""
        try:
            self._ensure_connected()
            remote_path = str(self._resolve_path(virtual_path))
            value_str = value.decode() if isinstance(value, (bytes, bytearray)) else str(value)
            cmd = (
                f"setfattr -n {self._shell_quote(name)} "
                f"-v {self._shell_quote(value_str)} {self._shell_quote(remote_path)}"
            )
            self._ssh_client.exec_command(cmd, timeout=self.connect_timeout)
        except Exception:
            # Remote might not support xattrs; ignore
            pass

    def get_xattr(self, virtual_path: str, name: str):
        try:
            self._ensure_connected()
            remote_path = str(self._resolve_path(virtual_path))
            cmd = (
                f"getfattr --absolute-names --only-values "
                f"-n {self._shell_quote(name)} {self._shell_quote(remote_path)}"
            )
            _, stdout, _ = self._ssh_client.exec_command(
                cmd, timeout=self.connect_timeout
            )
            data = stdout.read()
            return data if data else None
        except Exception:
            return None
