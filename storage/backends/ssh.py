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
import threading
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
import paramiko
from pathlib import Path, PurePosixPath
from typing import Callable, List, Optional, TypeVar

from .base import (
    Backend,
    BackendType,
    ChecksumUnavailable,
    MemberSourceMissing,
    StorageMemberCapabilities,
    WatcherSpec,
)
from .ssh_pool import (
    CommandTimeout, MemberConnection, MemberUnavailable, RootAvailability,
    SFTPLease, is_connection_error, is_transient_oserror, run_channel_command,
)

_T = TypeVar("_T")


# The flake-packaged storage runs on nixpkgs' glibc, which on a non-NixOS host
# cannot load the host's NSS modules (nss-mdns for .local names, or any other
# the host's nsswitch.conf names) and has no nscd to ask, so getaddrinfo fails
# for names the host resolves. getent is the host's own NSS front end, so SSH
# connects fall back to it on a lookup failure. /usr/bin first: a nixpkgs
# getent earlier on PATH has the same blind spot. The pool connects once
# per connection attempt rather than per thread;
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


@dataclass(frozen=True, repr=False)
class _SSHConnectionConfig:
    """Effective Paramiko connection settings, independent of root.

    The configured host is the host-key lookup name. The host-key policy
    is fixed, and the presence of a key file controls agent/key discovery.
    Credentials stay out of the representation and pool diagnostics.
    """

    host: str
    port: int
    username: str
    password: Optional[str]
    key_filename: Optional[str]
    connect_timeout: float
    bulk_capacity_wait_timeout: Optional[float]
    ssh_transport: str = "paramiko"


class SSHProbeCleanupError(RuntimeError):
    """A startup probe could not prove its SSH reader thread stopped."""


def _close_probe_client(client: paramiko.SSHClient, timeout: float) -> None:
    try:
        transport = client.get_transport()
        try:
            client.close()
        finally:
            if transport is not None and transport.is_alive():
                transport.join(timeout)
                if transport.is_alive():
                    raise SSHProbeCleanupError("SSH filesystem probe reader did not stop")
    except SSHProbeCleanupError:
        raise
    except Exception as exc:
        raise SSHProbeCleanupError("Could not stop SSH filesystem probe reader") from exc


class _SSHConnector:
    def __init__(self, config: _SSHConnectionConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._host_resolver_logged = False

    def open(self, *, startup_probe: bool = False) -> paramiko.SSHClient:
        """Connect and authenticate one SSH client; the pool calls this
        once per transport, from one thread at a time."""
        ssh_client = paramiko.SSHClient()
        connected = False
        sock = None
        try:
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_kwargs = {
                'hostname': self.config.host,
                'port': self.config.port,
                'username': self.config.username,
                'timeout': self.config.connect_timeout,
                'banner_timeout': self.config.connect_timeout,
                'auth_timeout': self.config.connect_timeout,
            }

            if self.config.password:
                connect_kwargs['password'] = self.config.password
            if self.config.key_filename:
                connect_kwargs['key_filename'] = self.config.key_filename
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
                    self.config.host, self.config.port, self.config.connect_timeout, lookup_error)
                self.logger.log(
                    logging.DEBUG if self._host_resolver_logged else logging.INFO,
                    "Resolved %s through the host's getent after: %s",
                    self.config.host, lookup_error)
                self._host_resolver_logged = True
                ssh_client.connect(sock=sock, **connect_kwargs)
            # paramiko's transport polls its socket with a short timeout to
            # notice close(); request timeouts are set per channel instead.
            connected = True
            return ssh_client

        except paramiko.AuthenticationException as e:
            self.logger.error("SSH authentication failed for %s@%s:%s: %s",
                              self.config.username, self.config.host, self.config.port, e)
            raise
        except paramiko.SSHException as e:
            self.logger.error("SSH handshake failed for %s@%s:%s: %s",
                              self.config.username, self.config.host, self.config.port, e)
            raise
        except Exception as e:
            self.logger.error("Failed to connect to %s@%s:%s: %s",
                              self.config.username, self.config.host, self.config.port, e)
            raise
        finally:
            if not connected:
                try:
                    if startup_probe:
                        _close_probe_client(ssh_client, self.config.connect_timeout)
                    else:
                        ssh_client.close()
                except SSHProbeCleanupError:
                    raise
                except Exception:
                    pass
                finally:
                    if sock is not None:
                        try:
                            sock.close()
                        except Exception:
                            pass


class _SharedTransport:
    """One pool with a reference for each configured member."""

    def __init__(self, pool):
        self.pool = pool
        self._lock = threading.Lock()
        self._roots: set[RootAvailability] = set()
        self._retired = False

    def attach(self, name: str) -> Optional[RootAvailability]:
        with self._lock:
            if self._retired:
                return None
            root = RootAvailability(name)
            self._roots.add(root)
            return root

    def detach(self, root: RootAvailability) -> None:
        with self._lock:
            if root not in self._roots:
                return
            self.pool.close_root(root)
            self._roots.remove(root)
            if not self._roots:
                self._retired = True
                self.pool.close()


class SSHTransportRegistry:
    """Registers pool owners during construction of one mount's members."""

    def __init__(self):
        self._transports: dict[_SSHConnectionConfig, _SharedTransport] = {}
        self._lock = threading.Lock()

    def attach(self, config: _SSHConnectionConfig, name: str):
        with self._lock:
            group = self._transports.get(config)
            if group is not None:
                root = group.attach(name)
                if root is not None:
                    return group, root
            if config.ssh_transport == "openssh":
                from .openssh_pool import OpenSSHSessionPool
                pool = OpenSSHSessionPool(config)
            else:
                pool = _make_pool(
                    f"{config.host}:{config.port}", config, _SSHConnector(config).open)
            group = _SharedTransport(pool)
            self._transports[config] = group
            return group, group.attach(name)


def _make_pool(name, config: _SSHConnectionConfig, connect) -> MemberConnection:
    return MemberConnection(
        name,
        connect,
        connect_timeout=config.connect_timeout,
        retry_interval=max(1.0, min(5.0, config.connect_timeout / 2.0)),
        # Background work waits as long as a transfer may hold a channel.
        bulk_wait=max(60.0, config.connect_timeout * 6.0),
        bulk_capacity_wait_timeout=config.bulk_capacity_wait_timeout,
    )


class SSHBackend(Backend):
    """Backend for SSH/SFTP remote filesystems.

    Members of one mount can share a pool across roots. Standalone backends
    own a private transport.
    """

    def __init__(self, name: str, host: str, remote_path: str,
                 username: str, password: Optional[str] = None,
                 key_filename: Optional[str] = None,
                 port: int = 22, speed_priority: int = 50,
                 connect_timeout: float = 10.0,
                 bulk_capacity_wait_timeout: Optional[float] = None, *,
                 transport_registry: Optional[SSHTransportRegistry] = None,
                 ssh_transport: str = "paramiko"):
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
            bulk_capacity_wait_timeout: optional bulk SFTP capacity deadline;
                None or zero waits for channel capacity to drain
            transport_registry: the mount's shared Paramiko transports
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

        if ssh_transport not in ("paramiko", "openssh"):
            raise ValueError("ssh_transport must be 'paramiko' or 'openssh'")
        if ssh_transport == "openssh":
            if password is not None:
                raise ValueError("OpenSSH transport does not accept a configured password")
            if transport_registry is None:
                transport_registry = SSHTransportRegistry()
        self._uses_openssh = ssh_transport == "openssh"
        self.host = host
        self.port = port
        self.remote_path = PurePosixPath(remote_path)
        self.username = username
        self.key_filename = key_filename
        self.connect_timeout = connect_timeout
        self.probe_reached_host = False
        # Reads and transfers get a longer timeout than metadata requests,
        # so a large or slow read is not aborted as a dead link.
        self.sftp_operation_timeout = max(60.0, float(connect_timeout) * 6.0)
        self.logger = logging.getLogger(__name__)
        self._reconnect_probe_interval = max(1.0, min(5.0, float(connect_timeout) / 2.0))
        self._connection_config = _SSHConnectionConfig(
            host, port, username, password or None,
            os.fspath(key_filename) if key_filename else None,
            float(connect_timeout), bulk_capacity_wait_timeout, ssh_transport,
        )
        self._probe_connector = _SSHConnector(self._connection_config)
        self._shared_transport = None
        self._root_availability = None
        if transport_registry is None:
            self._pool = _make_pool(name, self._connection_config, self._probe_connector.open)
        else:
            self._shared_transport, self._root_availability = transport_registry.attach(
                self._connection_config, name)
            self._pool = self._shared_transport.pool

    def _open_client(self) -> paramiko.SSHClient:
        return self._probe_connector.open(startup_probe=True)

    def io_capacity_scope(self) -> object:
        return self._pool

    def probe_connection_scope(self) -> object:
        """Opaque identity for the startup probe, where credentials matter."""
        return self._connection_config

    def shares_transport(self) -> bool:
        return self._shared_transport is not None

    def listing_concurrency(self) -> int:
        # Let independent scans use spare channels, keeping headroom for a
        # reader. Each foreground listing still requests an interactive lease.
        return self._pool.nominal_bulk_capacity()

    def sync_upload_capacity(self) -> Optional[int]:
        # Uploads to an ssh:// member share this connection's background
        # SFTP channels with every other background call (copy_from_local
        # runs through _with_sftp).
        return self._pool.nominal_bulk_capacity()

    def background_range_capacity(self) -> int:
        return self._pool.bulk_capacity()

    def run_background_range_read(self, virtual_path, fetch):
        # read_file reuses this lease. A range may be claimed only after
        # capacity is acquired, so a reader can use the interactive reserve
        # while background work is still queued behind other bulk transfers.
        return self._with_sftp("background read", virtual_path, lambda sftp: fetch(),
                               timeout=self.sftp_operation_timeout, retry=True, retry_closed_channel=True)

    # ------------------------------------------------------------------ #
    # Availability
    # ------------------------------------------------------------------ #
    def is_available(self) -> bool:
        """Transport connected, or the retry window after a failed connect has passed."""
        return self._pool.available(self._root_availability)

    def set_available(self, available: bool):
        if self._root_availability is not None:
            self._pool.set_root_available(self._root_availability, available)
        elif available:
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
    def _with_sftp(self, what: str, virtual_path: str,
                   operation: Callable[[paramiko.SFTPClient], _T], *,
                   timeout: Optional[float] = None, retry: bool = False,
                   retry_closed_channel: bool = False) -> _T:
        """Run operation on a leased SFTP channel.

        timeout is the channel timeout (connect_timeout by default). With
        retry, an operation whose transport was lost under it is retried once
        on a new connection; use it only for operations that are safe to
        repeat. A request that timed out on a connection that still answers
        is not retried: the member is slow, and the pool marks it down after
        a few of those. A nested call on the same thread reuses the caller's
        channel and leaves the retry to the outermost call. Read-only stat
        and range operations may also retry a dropped SFTP channel on an
        otherwise healthy transport.
        """
        if timeout is None:
            timeout = self.connect_timeout
        attempts = 2 if retry else 1
        for attempt in range(1, attempts + 1):
            lease: SFTPLease = self._pool.acquire_sftp(timeout, root=self._root_availability)
            nested = lease.depth > 1
            try:
                result = operation(lease.client)
            except BaseException as exc:
                self._pool.release(lease, exc, root=self._root_availability)
                if not isinstance(exc, Exception) or not is_connection_error(exc):
                    raise
                self.logger.warning("SFTP %s failed on %s attempt=%d/%d: %r",
                                    what, virtual_path, attempt, attempts, exc)
                if nested or attempt == attempts:
                    raise
                if self._pool.is_current(lease.generation) and not (
                    retry_closed_channel and isinstance(exc, paramiko.SSHException)
                    and str(exc).startswith("Server connection dropped:")
                ):
                    raise
                continue
            self._pool.release(lease, root=self._root_availability)
            return result
        raise AssertionError("unreachable")

    def _run_command(self, command: str, timeout: float):
        if self._root_availability is None:
            return self._pool.run_command(command, timeout)
        return self._pool.run_command(command, timeout, root=self._root_availability)

    def _resolve_path(self, virtual_path: str) -> PurePosixPath:
        """Convert virtual path to remote path"""
        relative_path = virtual_path.lstrip('/')
        return self.remote_path / relative_path

    def push_invalidation_spec(self) -> Optional[WatcherSpec]:
        return WatcherSpec(
            group_key=((self._pool,) if self._uses_openssh else
                       (self.host, self.port, self.username, self.key_filename)),
            ssh_host=self.host,
            ssh_port=self.port,
            username=self.username,
            key_filename=self.key_filename,
            remote_path=str(self.remote_path),
            session_pool=self._pool if self._uses_openssh else None,
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
                               timeout=self.sftp_operation_timeout, retry=True, retry_closed_channel=True)

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
                # posix-rename@openssh.com replaces an existing target, as
                # rename(2) and the local member do; plain SFTP rename fails
                # on OpenSSH when the target exists (an editor's save).
                try:
                    sftp.posix_rename(old_remote, new_remote)
                except OSError as exc:
                    # paramiko raises a bare OSError without an errno for
                    # SFTP statuses it does not map: an unsupported
                    # extension, or a generic failure (a non-empty target
                    # directory, say). Plain rename still moves onto an
                    # absent target; otherwise its own error is raised. A
                    # timeout (socket.timeout, also errno-less) or a lost
                    # connection is raised as it is, for the pool to see.
                    if (type(exc) is not OSError or exc.errno is not None
                            or is_connection_error(exc)):
                        raise
                    sftp.rename(old_remote, new_remote)
            except FileNotFoundError as exc:
                # Never reported as a move. Named a missing source only when
                # the member says so; any other ENOENT (a destination parent
                # gone, say) keeps its own cause.
                try:
                    sftp.lstat(old_remote)
                except FileNotFoundError:
                    raise MemberSourceMissing(
                        errno.ENOENT, "no source on this member", old_remote) from exc
                except BaseException as diagnostic:
                    if is_connection_error(diagnostic) or not isinstance(diagnostic, Exception):
                        # The member could not be asked (a timeout, a lost
                        # connection), or an interrupt: raised as it is, for
                        # the pool to see, with the rename's error noted.
                        diagnostic.add_note(f"after the rename failed: {exc!r}")
                        raise
                    # A path answer (EACCES, say): no evidence either way.
                    # The rename's own error stands, with the check noted.
                    exc.add_note(f"source check failed: {diagnostic!r}")
                    raise exc from None
                raise

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
                               lambda sftp: sftp.stat(remote_path), retry=True, retry_closed_channel=True)

    def lstat(self, virtual_path: str):
        """Like stat, but a link at the last component is reported as a link.

        The member root is stat'ed: a root configured through a link is the
        directory it names.
        """
        if virtual_path.strip("/") == "":
            return self.stat(virtual_path)
        remote_path = str(self._resolve_path(virtual_path))
        return self._with_sftp("lstat", virtual_path,
                               lambda sftp: sftp.lstat(remote_path), retry=True, retry_closed_channel=True)

    def readlink(self, virtual_path: str) -> str:
        """The link's target, from SFTP readlink (rsync members use it too).

        sftp-server answers readlink of a non-link with a status paramiko
        raises without an errno, as it does other failures (EIO on the
        member); an lstat tells them apart: a path that is there and not a
        link is EINVAL, as readlink(2) gives, and anything else keeps the
        failure as EIO. A missing path keeps paramiko's ENOENT.
        """
        remote_path = str(self._resolve_path(virtual_path))
        try:
            target = self._with_sftp("readlink", virtual_path,
                                     lambda sftp: sftp.readlink(remote_path), retry=True)
        except OSError as exc:
            if exc.errno is not None or is_connection_error(exc):
                raise
            if not stat_module.S_ISLNK(self.lstat(virtual_path).st_mode):
                raise OSError(errno.EINVAL, "not a symlink", virtual_path) from exc
            raise OSError(errno.EIO, f"readlink failed: {exc}", virtual_path) from exc
        if target is None:
            raise OSError(errno.EINVAL, "not a symlink", virtual_path)
        if isinstance(target, bytes):
            target = os.fsdecode(target)
        return target

    def symlink(self, target: str, virtual_path: str):
        """Create the link over SFTP (rsync members too: link creation is
        metadata, like mkdir and rename, which they also do over SFTP).

        sftp-server fails an existing path with a status that carries no
        errno; an lstat that finds something there makes it FileExistsError.
        paramiko's symlink(source, dest) sends source first, the order
        OpenSSH's sftp-server takes as (target, link path).
        """
        remote_path = str(self._resolve_path(virtual_path))

        def create(sftp):
            self._ensure_parent(sftp, remote_path)
            sftp.symlink(target, remote_path)

        try:
            self._with_sftp("symlink", virtual_path, create)
        except OSError as exc:
            if exc.errno is not None or is_connection_error(exc):
                raise
            try:
                self.lstat(virtual_path)
            except FileNotFoundError:
                raise exc from None
            raise FileExistsError(errno.EEXIST, "already exists", virtual_path) from exc

    def close(self):
        """Retire this root and close its transport after the last root."""
        group = getattr(self, "_shared_transport", None)
        if group is not None:
            group.detach(self._root_availability)
        else:
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
    @contextmanager
    def filesystem_probe_connection(self):
        """Open a transient connection and stop its thread before FUSE forks."""
        if self._uses_openssh:
            from .openssh import OpenSSHMaster

            master = OpenSSHMaster(self.username, self.host, self.port, self.key_filename,
                                   connect_timeout=self.connect_timeout)
            try:
                yield master.generation()
            finally:
                master.close()
            return
        client = self._open_client()
        try:
            yield client
        finally:
            _close_probe_client(client, self.connect_timeout)

    def filesystem_identity(self, probe_client=None):
        """The device id of this root (stat -c %d), on this host and port.

        A startup caller can pass a transient client shared by compatible
        roots. The serving pool must stay unopened until FUSE daemonizes.
        """
        self.probe_reached_host = False
        probe = (self.filesystem_probe_connection() if probe_client is None
                 else nullcontext(probe_client))
        try:
            with probe as client:
                self.probe_reached_host = True
                cmd = f"stat -L -c %d -- {self._shell_quote(str(self.remote_path))}"
                deadline = time.monotonic() + self.connect_timeout

                def remaining():
                    left = deadline - time.monotonic()
                    if left <= 0:
                        raise socket.timeout("filesystem probe timed out")
                    return left

                if self._uses_openssh:
                    from .openssh_pool import run_mux_command

                    def open_command(cmd, inp, out, err):
                        return client.open_command(cmd, inp, out, err, remaining())

                    rc, output = run_mux_command(open_command, cmd, remaining, output_limit=64)
                else:
                    channel = client.get_transport().open_session(timeout=remaining())
                    try:
                        rc, output = run_channel_command(channel, cmd, remaining)
                    finally:
                        channel.close()
                text = output.decode(errors="replace").strip()
                if rc != 0 or not text.isdigit():
                    raise ValueError(f"unusable stat output (exit {rc})")
                return (self.host, self.port, int(text))
        except SSHProbeCleanupError:
            raise
        except Exception as exc:
            self.logger.debug("member %s: filesystem probe failed: %s", self.name, exc)
            return None

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
            rc, stdout = self._run_command(cmd, self.sftp_operation_timeout)
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
            self._run_command(cmd, self.connect_timeout)
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
            rc, data = self._run_command(cmd, self.connect_timeout)
            return data if rc == 0 and data else None
        except Exception:
            return None
