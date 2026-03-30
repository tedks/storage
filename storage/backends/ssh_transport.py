"""Paramiko transport lifecycle for SSH-backed storage backends."""

import errno
import socket
import threading
import time
import weakref
from typing import Callable, Dict, List, Optional, Tuple

import paramiko


class SSHTransportManager:
    """Owns per-thread SSH/SFTP clients, reconnect gates, and availability."""

    def __init__(
        self,
        *,
        backend_name: str,
        host: str,
        port: int,
        username: str,
        password: Optional[str],
        key_filename: Optional[str],
        connect_timeout: float,
        sftp_operation_timeout: float,
        is_available: Callable[[], bool],
        set_available: Callable[[bool], None],
        logger,
        paramiko_module=paramiko,
    ):
        self.backend_name = backend_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.key_filename = key_filename
        self.connect_timeout = connect_timeout
        self.sftp_operation_timeout = sftp_operation_timeout
        self.is_available = is_available
        self.set_available = set_available
        self.logger = logger
        self.paramiko = paramiko_module

        self._thread_local = threading.local()
        self._thread_clients: Dict[
            int,
            Tuple[
                paramiko.SSHClient,
                paramiko.SFTPClient,
                weakref.ReferenceType[threading.Thread],
            ],
        ] = {}
        self._thread_clients_lock = threading.Lock()
        self._availability_lock = threading.Lock()
        self._probe_lock = threading.Lock()
        self._reconnect_probe_interval = max(1.0, min(5.0, float(connect_timeout) / 2.0))
        self._next_connect_retry_at = 0.0

    @property
    def thread_clients(self):
        return self._thread_clients

    @property
    def thread_clients_lock(self):
        return self._thread_clients_lock

    @property
    def next_connect_retry_at(self) -> float:
        return self._next_connect_retry_at

    @next_connect_retry_at.setter
    def next_connect_retry_at(self, value: float):
        self._next_connect_retry_at = float(value)

    @staticmethod
    def close_client_pair(
        ssh_client: Optional[paramiko.SSHClient],
        sftp_client: Optional[paramiko.SFTPClient],
    ):
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

    def prune_stale_thread_clients(self, exclude_thread_id: Optional[int] = None):
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
            self.close_client_pair(ssh_client, sftp_client)

    def set_thread_clients(
        self,
        ssh_client: paramiko.SSHClient,
        sftp_client: paramiko.SFTPClient,
    ):
        thread_id = threading.get_ident()
        self.prune_stale_thread_clients(exclude_thread_id=thread_id)

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
            self.close_client_pair(old_pair[0], old_pair[1])

    def get_thread_clients(
        self,
    ) -> Tuple[Optional[paramiko.SSHClient], Optional[paramiko.SFTPClient]]:
        ssh_client = getattr(self._thread_local, "ssh_client", None)
        sftp_client = getattr(self._thread_local, "sftp_client", None)
        return ssh_client, sftp_client

    @staticmethod
    def transport_active(ssh_client: Optional[paramiko.SSHClient]) -> bool:
        if ssh_client is None:
            return False
        try:
            transport = ssh_client.get_transport()
            return bool(transport and transport.is_active())
        except Exception:
            return False

    def mark_available(self):
        with self._availability_lock:
            self._next_connect_retry_at = 0.0
        self.set_available(True)

    def mark_unavailable(self):
        with self._availability_lock:
            self._next_connect_retry_at = max(
                self._next_connect_retry_at,
                time.monotonic() + self._reconnect_probe_interval,
            )
        self.set_available(False)

    @staticmethod
    def is_transient_oserror(exc: OSError) -> bool:
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

    def connect(self):
        """Establish per-thread SSH/SFTP connection."""
        try:
            ssh_client = self.paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(self.paramiko.AutoAddPolicy())

            connect_kwargs = {
                "hostname": self.host,
                "port": self.port,
                "username": self.username,
                "timeout": self.connect_timeout,
                "banner_timeout": self.connect_timeout,
                "auth_timeout": self.connect_timeout,
            }

            if self.password:
                connect_kwargs["password"] = self.password
            if self.key_filename:
                connect_kwargs["key_filename"] = self.key_filename
            else:
                connect_kwargs["allow_agent"] = False
                connect_kwargs["look_for_keys"] = False

            ssh_client.connect(**connect_kwargs)
            sftp_client = ssh_client.open_sftp()

            transport = ssh_client.get_transport()
            if transport and transport.sock:
                try:
                    transport.sock.settimeout(self.sftp_operation_timeout)
                except Exception:
                    pass

            try:
                chan = sftp_client.get_channel()
                if chan:
                    chan.settimeout(self.sftp_operation_timeout)
            except Exception:
                pass

            self.set_thread_clients(ssh_client, sftp_client)
            self.mark_available()
        except self.paramiko.AuthenticationException as exc:
            self.logger.error(
                "SSH authentication failed for %s@%s:%s: %s",
                self.username,
                self.host,
                self.port,
                exc,
            )
            self.mark_unavailable()
            raise
        except self.paramiko.SSHException as exc:
            self.logger.error(
                "SSH handshake failed for %s@%s:%s: %s",
                self.username,
                self.host,
                self.port,
                exc,
            )
            self.mark_unavailable()
            raise
        except Exception as exc:
            self.logger.error(
                "Failed to connect to %s@%s:%s: %s",
                self.username,
                self.host,
                self.port,
                exc,
            )
            self.mark_unavailable()
            raise

    def ensure_connected(self):
        """Ensure current thread has an active SSH/SFTP connection."""
        ssh_client, sftp_client = self.get_thread_clients()
        if sftp_client is not None and self.transport_active(ssh_client):
            return

        with self._availability_lock:
            is_available = self.is_available()
            next_retry_at = self._next_connect_retry_at

        if not is_available and time.monotonic() < next_retry_at:
            raise ConnectionError(
                f"SSH backend {self.backend_name} is temporarily unavailable after a disconnect"
            )

        if not self._probe_lock.acquire(blocking=False):
            raise ConnectionError(
                f"SSH backend {self.backend_name} reconnect probe already in progress"
            )
        try:
            with self._availability_lock:
                is_available = self.is_available()
                next_retry_at = self._next_connect_retry_at

            if not is_available and time.monotonic() < next_retry_at:
                raise ConnectionError(
                    f"SSH backend {self.backend_name} is temporarily unavailable after a disconnect"
                )

            self.reset_connection()
            self.connect()
        finally:
            self._probe_lock.release()

    def current_clients(self) -> Tuple[paramiko.SSHClient, paramiko.SFTPClient]:
        self.prune_stale_thread_clients(exclude_thread_id=threading.get_ident())
        self.ensure_connected()
        ssh_client, sftp_client = self.get_thread_clients()
        if ssh_client is None or sftp_client is None:
            raise RuntimeError("SSH backend has no active thread client")
        return ssh_client, sftp_client

    def reset_connection(self):
        """Drop current thread's SSH/SFTP clients so the next operation reconnects."""
        thread_id = threading.get_ident()
        ssh_client, sftp_client = self.get_thread_clients()

        with self._thread_clients_lock:
            pair = self._thread_clients.pop(thread_id, None)
            if pair:
                ssh_client = pair[0]
                sftp_client = pair[1]

        self.close_client_pair(ssh_client, sftp_client)

        self._thread_local.ssh_client = None
        self._thread_local.sftp_client = None

    def close(self):
        """Close SSH/SFTP clients across all threads."""
        with self._thread_clients_lock:
            clients = [(entry[0], entry[1]) for entry in self._thread_clients.values()]
            self._thread_clients.clear()

        for ssh_client, sftp_client in clients:
            self.close_client_pair(ssh_client, sftp_client)

        self._thread_local.ssh_client = None
        self._thread_local.sftp_client = None
        self.mark_unavailable()
