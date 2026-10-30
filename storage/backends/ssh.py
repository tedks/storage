"""
SSH/SFTP backend implementation for remote filesystems
"""

import errno
import ipaddress
import os
import shlex
import shutil
import socket
import subprocess
import stat as stat_module
import logging
import time
import paramiko
from pathlib import Path, PurePosixPath
from typing import Callable, List, Optional, TypeVar

from .base import Backend, BackendType, ChecksumUnavailable, StorageMemberCapabilities, WatcherSpec
from .ssh_pool import (
    CommandTimeout, MemberConnection, SFTPLease, is_connection_error, is_transient_oserror,
)

_T = TypeVar("_T")


# The flake-packaged storage runs on nixpkgs' glibc, which on a non-NixOS host
# cannot load the host's NSS modules (nss-mdns for .local names, or any other
# the host's nsswitch.conf names) and has no nscd to ask, so getaddrinfo fails
# for names the host resolves. getent is the host's own NSS front end, so SSH
# connects fall back to it on a lookup failure. /usr/bin first: a nixpkgs
# getent earlier on PATH has the same blind spot. A member holds one
# transport, so this runs once per connect or retry window, not per thread;
# the result is not cached, because the connect that follows an address
# change is the one that must resolve afresh.
_GETENT_CANDIDATES = ("/usr/bin/getent", "/bin/getent")


def _host_resolver_addresses(host: str, timeout: float) -> List[str]:
    """Return the host's NSS addresses for host, in getent's order.

    Runs the first executable getent among _GETENT_CANDIDATES and PATH (one
    process: a second getent would consult the same nsswitch.conf). Returns
    [] when there is no getent, it fails or times out, or it prints no
    address; the caller then keeps its original lookup error.
    """
    getent = next(
        (candidate for candidate in _GETENT_CANDIDATES
         if os.access(candidate, os.X_OK)),
        None,
    ) or shutil.which("getent")
    if getent is None:
        return []
    try:
        # "--" so a host starting with "-" is not read as an option.
        result = subprocess.run(
            [getent, "ahosts", "--", host],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            # Only the address field is parsed; a stray byte in a name from
            # /etc/hosts must not replace the caller's lookup error.
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if result.returncode != 0:
        return []
    # Lines are "ADDRESS SOCKTYPE [NAME]", one per address and socket type.
    addresses: List[str] = []
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) < 2 or fields[1] != "STREAM":
            continue
        try:
            address = str(ipaddress.ip_address(fields[0]))
        except ValueError:
            continue
        if address not in addresses:
            addresses.append(address)
    return addresses


def _connect_via_host_resolver(host: str, port: int, timeout: float,
                               lookup_error: socket.gaierror) -> socket.socket:
    """Open a TCP connection to host:port through the host's NSS.

    getent and the connects share one deadline of timeout seconds; each
    getent address is tried in order with what remains, as getaddrinfo
    callers do. Raises lookup_error when getent finds no address (including
    when getent itself times out), the last connect error when every address
    was tried, and socket.timeout when the deadline passes with addresses
    left untried.
    """
    deadline = time.monotonic() + timeout
    addresses = _host_resolver_addresses(host, timeout)
    if not addresses:
        raise lookup_error
    last_error: Optional[OSError] = None
    for address in addresses:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout(
                f"timed out connecting to {host}:{port} after resolving it "
                f"with getent") from (last_error or lookup_error)
        try:
            return socket.create_connection((address, port), timeout=remaining)
        except OSError as exc:
            last_error = exc
    if last_error is None:  # unreachable: addresses is non-empty
        raise lookup_error
    raise last_error


class SSHBackend(Backend):
    """Backend for SSH/SFTP remote filesystems.

    One SSH transport per member, shared by every thread through a leased
    pool of SFTP channels (see ssh_pool.MemberConnection).
    """

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
            connect_timeout: Timeout in seconds for SSH connection/auth
                handshakes, and for metadata requests
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
        # Reads and transfers get a longer timeout than metadata requests,
        # so a large or slow read is not aborted as a dead link.
        self.sftp_operation_timeout = max(60.0, float(connect_timeout) * 6.0)
        self.logger = logging.getLogger(__name__)
        self._reconnect_probe_interval = max(1.0, min(5.0, float(connect_timeout) / 2.0))
        # Log the first getent fallback at info, later ones at debug.
        self._host_resolver_logged = False
        self._pool = MemberConnection(
            name,
            self._open_client,
            connect_timeout=connect_timeout,
            retry_interval=self._reconnect_probe_interval,
            # Background work waits as long as a transfer may hold a channel.
            bulk_wait=self.sftp_operation_timeout,
            on_availability=lambda available: Backend.set_available(self, available),
            logger=self.logger,
        )

    # ------------------------------------------------------------------ #
    # Availability
    # ------------------------------------------------------------------ #
    def is_available(self) -> bool:
        """Transport connected, or the retry window after a failed connect has passed."""
        return self._pool.available()

    def set_available(self, available: bool):
        if available:
            self._pool.clear_down()
        else:
            self._pool.mark_down()

    @staticmethod
    def _is_transient_oserror(exc: OSError) -> bool:
        """Return True when an OSError likely indicates connection loss."""
        return is_transient_oserror(exc)

    # ------------------------------------------------------------------ #
    # Connection
    # ------------------------------------------------------------------ #
    def _open_client(self) -> paramiko.SSHClient:
        """Connect and authenticate one SSH client; the pool calls this
        once per transport, from one thread at a time."""
        ssh_client = paramiko.SSHClient()
        try:
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

            try:
                ssh_client.connect(**connect_kwargs)
            except socket.gaierror as lookup_error:
                # paramiko raises this from its getaddrinfo, before it opens
                # a socket, so the client is still unused. The hostname stays
                # in connect_kwargs: paramiko names the host key by it.
                sock = _connect_via_host_resolver(
                    self.host, self.port, self.connect_timeout, lookup_error)
                self.logger.log(
                    logging.DEBUG if self._host_resolver_logged else logging.INFO,
                    "Resolved %s through the host's getent after: %s",
                    self.host, lookup_error)
                self._host_resolver_logged = True
                try:
                    ssh_client.connect(sock=sock, **connect_kwargs)
                except BaseException:
                    # A failure after the handshake leaves paramiko's
                    # transport thread on the socket; close both, and let
                    # neither close hide the connect error.
                    for close in (ssh_client.close, sock.close):
                        try:
                            close()
                        except Exception:
                            pass
                    raise
            # paramiko's transport polls its socket with a short timeout to
            # notice close(); request timeouts are set per channel instead.
            return ssh_client

        except paramiko.AuthenticationException as e:
            self.logger.error("SSH authentication failed for %s@%s:%s: %s",
                              self.username, self.host, self.port, e)
            raise
        except paramiko.SSHException as e:
            self.logger.error("SSH handshake failed for %s@%s:%s: %s",
                              self.username, self.host, self.port, e)
            raise
        except Exception as e:
            self.logger.error("Failed to connect to %s@%s:%s: %s",
                              self.username, self.host, self.port, e)
            raise

    def _with_sftp(self, what: str, virtual_path: str,
                   operation: Callable[[paramiko.SFTPClient], _T], *,
                   timeout: Optional[float] = None, retry: bool = False) -> _T:
        """Run operation on a leased SFTP channel.

        timeout is the channel timeout (connect_timeout by default). With
        retry, an operation whose transport was lost under it is retried once
        on a new connection; use it only for operations that are safe to
        repeat. A request that timed out on a connection that still answers
        is not retried: the member is slow, and the pool marks it down after
        a few of those. A nested call on the same thread reuses the caller's
        channel and leaves the retry to the outermost call.
        """
        if timeout is None:
            timeout = self.connect_timeout
        attempts = 2 if retry else 1
        for attempt in range(1, attempts + 1):
            lease: SFTPLease = self._pool.acquire_sftp(timeout)
            nested = lease.depth > 1
            try:
                result = operation(lease.client)
            except BaseException as exc:
                self._pool.release(lease, exc)
                if not isinstance(exc, Exception) or not is_connection_error(exc):
                    raise
                self.logger.warning("SFTP %s failed on %s attempt=%d/%d: %r",
                                    what, virtual_path, attempt, attempts, exc)
                if nested or attempt == attempts or self._pool.is_current(lease.generation):
                    raise
                continue
            self._pool.release(lease)
            return result
        raise AssertionError("unreachable")

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

    # ------------------------------------------------------------------ #
    # Operations
    # ------------------------------------------------------------------ #
    def exists(self, virtual_path: str) -> bool:
        """Check if a file exists on remote; False when it cannot be reached."""
        remote_path = str(self._resolve_path(virtual_path))
        try:
            self._with_sftp("exists", virtual_path,
                            lambda sftp: sftp.stat(remote_path), retry=True)
            return True
        except FileNotFoundError:
            return False
        except Exception as exc:
            if not is_connection_error(exc):
                self.logger.warning("Error checking existence of %s: %r", virtual_path, exc)
            return False

    def exists_or_raise(self, virtual_path: str) -> bool:
        """Like exists(), but raise when the member cannot be asked."""
        remote_path = str(self._resolve_path(virtual_path))
        try:
            self._with_sftp("exists", virtual_path,
                            lambda sftp: sftp.stat(remote_path), retry=True)
            return True
        except FileNotFoundError:
            return False

    def read_file(self, virtual_path: str, offset: int, size: int) -> bytes:
        """Read data from remote file, retrying once on a connection error."""
        remote_path = str(self._resolve_path(virtual_path))

        def read(sftp):
            with sftp.open(remote_path, 'rb') as f:
                f.seek(offset)
                return f.read(size)

        return self._with_sftp("read", virtual_path, read,
                               timeout=self.sftp_operation_timeout, retry=True)

    def _ensure_parent(self, sftp, remote_path: str):
        parent = str(PurePosixPath(remote_path).parent)
        try:
            sftp.stat(parent)
        except FileNotFoundError:
            self._mkdir_recursive(parent, sftp_client=sftp)

    def write_file(self, virtual_path: str, data: bytes, offset: int):
        """Write data to remote file"""
        remote_path = str(self._resolve_path(virtual_path))

        def write(sftp):
            self._ensure_parent(sftp, remote_path)
            # Never truncate: open for update, and create only exclusively,
            # so a file another writer created in between is opened, not
            # emptied.
            try:
                f = sftp.open(remote_path, 'r+b')
            except FileNotFoundError:
                try:
                    # 'x' alone sets no write flag in paramiko; '+' does.
                    f = sftp.open(remote_path, 'x+b')
                except OSError as create_error:
                    if is_connection_error(create_error):
                        raise
                    # Lost a race to another writer, or the create itself
                    # failed (permissions, quota): only the first is
                    # recovered by opening what is now there.
                    try:
                        f = sftp.open(remote_path, 'r+b')
                    except FileNotFoundError:
                        raise create_error from None
            with f:
                f.seek(offset)
                f.write(data)

        self._with_sftp("write", virtual_path, write, timeout=self.sftp_operation_timeout)

    def create_file(self, virtual_path: str):
        """Create a new empty file on remote"""
        remote_path = str(self._resolve_path(virtual_path))

        def create(sftp):
            self._ensure_parent(sftp, remote_path)
            with sftp.open(remote_path, 'wb'):
                pass

        self._with_sftp("create", virtual_path, create)

    def delete_file(self, virtual_path: str):
        """Delete a file from remote"""
        remote_path = str(self._resolve_path(virtual_path))
        try:
            # Unlinking a large file can take a while on some filesystems.
            self._with_sftp("delete", virtual_path, lambda sftp: sftp.remove(remote_path),
                            timeout=self.sftp_operation_timeout)
        except FileNotFoundError:
            pass

    def truncate_file(self, virtual_path: str):
        """Truncate a file to zero length"""
        remote_path = str(self._resolve_path(virtual_path))

        def truncate(sftp):
            with sftp.open(remote_path, 'wb') as f:
                f.truncate(0)

        self._with_sftp("truncate", virtual_path, truncate,
                        timeout=self.sftp_operation_timeout)

    def get_size(self, virtual_path: str) -> int:
        """Get file size"""
        remote_path = str(self._resolve_path(virtual_path))
        return self._with_sftp("get_size", virtual_path,
                               lambda sftp: sftp.stat(remote_path).st_size, retry=True)

    def mkdir(self, virtual_path: str):
        """Create a directory on remote"""
        remote_path = str(self._resolve_path(virtual_path))
        self._with_sftp("mkdir", virtual_path,
                        lambda sftp: self._mkdir_recursive(remote_path, sftp_client=sftp))

    def _mkdir_recursive(self, remote_path: str, sftp_client: Optional[paramiko.SFTPClient] = None):
        """Recursively create directories on remote"""
        if sftp_client is None:
            self._with_sftp("mkdir", remote_path,
                            lambda sftp: self._mkdir_recursive(remote_path, sftp_client=sftp))
            return

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
        except OSError as exc:
            # May already exist due to a race; a lost connection is not that.
            if is_connection_error(exc):
                raise

    def rmdir(self, virtual_path: str):
        """Remove a directory from remote"""
        remote_path = str(self._resolve_path(virtual_path))
        try:
            self._with_sftp("rmdir", virtual_path, lambda sftp: sftp.rmdir(remote_path))
        except FileNotFoundError:
            pass

    def listdir(self, virtual_path: str) -> List[str]:
        """List directory contents on remote"""
        remote_path = str(self._resolve_path(virtual_path))
        try:
            return self._with_sftp("listdir", virtual_path,
                                   lambda sftp: sftp.listdir(remote_path), retry=True)
        except FileNotFoundError:
            return []

    def listdir_attrs(self, virtual_path: str):
        """List directory contents with attributes using SFTP listdir_attr.

        Returns None when the listing fails; the caller falls back to listdir.
        """
        remote_path = str(self._resolve_path(virtual_path))
        try:
            attrs = self._with_sftp("listdir_attrs", virtual_path,
                                    lambda sftp: sftp.listdir_attr(remote_path), retry=True)
        except Exception:
            return None
        result = []
        for attr in attrs:
            # attr.filename may be bytes on some servers
            name = attr.filename
            if isinstance(name, bytes):
                name = name.decode()
            # SFTPAttributes holds None for a field the server omitted.
            # Mode, size and mtime stay None so getattr probes the member
            # instead of answering from an incomplete listing.
            mtime = getattr(attr, 'st_mtime', None)
            atime = getattr(attr, 'st_atime', None)
            gid = getattr(attr, 'st_gid', None)
            uid = getattr(attr, 'st_uid', None)
            nlink = getattr(attr, 'st_nlink', None)
            result.append((name, {
                'st_mode': getattr(attr, 'st_mode', None),
                'st_size': getattr(attr, 'st_size', None),
                'st_mtime': mtime,
                'st_atime': atime if atime is not None else mtime,
                'st_gid': gid if gid is not None else os.getgid(),
                'st_uid': uid if uid is not None else os.getuid(),
                'st_nlink': nlink if nlink is not None else 1,
            }))
        return result

    def isdir(self, virtual_path: str) -> bool:
        """Check if path is a directory on remote"""
        remote_path = str(self._resolve_path(virtual_path))
        try:
            st = self._with_sftp("isdir", virtual_path,
                                 lambda sftp: sftp.stat(remote_path), retry=True)
        except Exception:
            return False
        return stat_module.S_ISDIR(st.st_mode)

    def copy_to_local(self, virtual_path: str, local_path: Path):
        """Copy a file from remote to local path"""
        remote_path = str(self._resolve_path(virtual_path))

        # Ensure local parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        self._with_sftp("copy_to_local", virtual_path,
                        lambda sftp: sftp.get(remote_path, str(local_path)),
                        timeout=self.sftp_operation_timeout, retry=True)

    def copy_from_local(self, local_path: Path, virtual_path: str):
        """Copy a local file to remote"""
        remote_path = str(self._resolve_path(virtual_path))

        def upload(sftp):
            self._ensure_parent(sftp, remote_path)
            sftp.put(str(local_path), remote_path)

        self._with_sftp("copy_from_local", virtual_path, upload,
                        timeout=self.sftp_operation_timeout)

    def rename(self, old_virtual_path: str, new_virtual_path: str):
        """Rename a file or directory on the remote backend."""
        old_remote = str(self._resolve_path(old_virtual_path))
        new_remote = str(self._resolve_path(new_virtual_path))

        def rename(sftp):
            self._ensure_parent(sftp, new_remote)
            try:
                sftp.rename(old_remote, new_remote)
            except FileNotFoundError:
                # Nothing to rename; best-effort
                pass

        self._with_sftp("rename", old_virtual_path, rename)

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
        return self._with_sftp("stat", virtual_path,
                               lambda sftp: sftp.stat(remote_path), retry=True)

    def close(self):
        """Close the member's transport and channels; it does not reconnect."""
        pool = getattr(self, "_pool", None)
        if pool is not None:
            pool.close()

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

    def get_fast_checksum(self, virtual_path: str, allow_download: bool = True) -> str:
        """
        Compute checksum without downloading the full file.

        Tries xxh128sum on the remote (matches local xxh3_128).
        Falls back to downloading a temp copy and checksumming locally
        so the result is always consistent with local checksums; with
        allow_download False it raises instead (the conflict view's hashes,
        which must not move whole files over the link to decide how a
        directory looks): the pool's own error when the member could not be
        asked, ChecksumUnavailable otherwise (no digest, or a command that
        started and did not finish).
        """
        remote_path = str(self._resolve_path(virtual_path))

        # Try remote xxh128sum first (fast path, avoids download).
        # Use sftp_operation_timeout (not connect_timeout) since hashing
        # large files can take much longer than a connection handshake.
        # Any failure (no xxh128sum, a refused or timed-out channel) falls
        # through to the download-and-hash fallback.
        remote_error = None
        try:
            cmd = f"xxh128sum {self._shell_quote(remote_path)} 2>/dev/null"
            rc, stdout = self._pool.run_command(cmd, self.sftp_operation_timeout)
            output = stdout.decode(errors="replace").strip()
            if rc == 0 and output:
                token = output.split()[0]
                # xxh128sum outputs a 32-char hex digest; validate format.
                if len(token) == 32 and all(c in "0123456789abcdef" for c in token):
                    return token
        except Exception as exc:
            remote_error = exc

        if not allow_download:
            # An error that kept the member from being asked (a connect or
            # channel open that failed or timed out, the transport dropped,
            # no exec channel free or the server refused one) may pass and
            # costs nothing to retry, so it is raised as is. A command that
            # started and did not finish (CommandTimeout) is not: the hash
            # runs too long for this file, or the member runs exec but never
            # answers (ForceCommand internal-sftp), and would every time.
            # That, and a command that ran without a digest, is
            # ChecksumUnavailable.
            if (
                remote_error is not None
                and not isinstance(remote_error, CommandTimeout)
                and is_connection_error(remote_error)
            ):
                raise remote_error
            raise ChecksumUnavailable(
                errno.EIO, f"no remote hash for {virtual_path} on {self.name}") from remote_error

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
            remote_path = str(self._resolve_path(virtual_path))
            value_str = value.decode() if isinstance(value, (bytes, bytearray)) else str(value)
            cmd = (
                f"setfattr -n {self._shell_quote(name)} "
                f"-v {self._shell_quote(value_str)} {self._shell_quote(remote_path)} 2>/dev/null"
            )
            self._pool.run_command(cmd, self.connect_timeout)
        except Exception:
            # Remote might not support xattrs; ignore
            pass

    def get_xattr(self, virtual_path: str, name: str):
        try:
            remote_path = str(self._resolve_path(virtual_path))
            cmd = (
                f"getfattr --absolute-names --only-values "
                f"-n {self._shell_quote(name)} {self._shell_quote(remote_path)} 2>/dev/null"
            )
            rc, data = self._pool.run_command(cmd, self.connect_timeout)
            return data if rc == 0 and data else None
        except Exception:
            return None
