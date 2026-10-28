"""
Rsync-over-SSH backend implementation.

Uses rsync for file transfer (copy_to_local, copy_from_local) and SSH/SFTP
for metadata operations (stat, listdir, exists, mkdir, etc.).  This gives
us rsync's delta compression and transfer resume while keeping the full
backend interface working.

Rsync advantages over pure SFTP:
- Delta transfer: only sends changed blocks for modified files
- Compression: built-in zlib compression over the wire
- Checksumming: end-to-end verification without separate pass
- Partial transfer resume: --partial-dir keeps partials in a temp dir
  so the destination is never corrupted on failure
"""

import logging
import os
import shlex
import subprocess
from pathlib import Path, PurePosixPath
from typing import List, Optional

from .base import Backend, BackendType
from .ssh import SSHBackend

# Hidden directory on each side for rsync partial transfers.
# rsync writes to this dir and atomically renames to destination on
# success.  On failure, the destination is untouched.
_PARTIAL_DIR = ".storage.rsync-tmp"


class RsyncBackend(SSHBackend):
    """
    Backend that uses rsync for file transfers and SSH for metadata.

    Inherits all metadata operations (stat, listdir, exists, mkdir, etc.)
    from SSHBackend.  Overrides copy_to_local and copy_from_local to use
    rsync subprocess instead of SFTP get/put.

    Parameters
    ----------
    name : str
        Backend identifier.
    host : str
        Remote host address.
    remote_path : str
        Root directory on remote host.
    username : str
        SSH username.
    password : str, optional
        SSH password (note: rsync uses key-based auth by default).
    key_filename : str, optional
        Path to private key file.
    port : int
        SSH port (default 22).
    speed_priority : int
        Speed priority (default 50 for remote).
    connect_timeout : float
        SSH connection timeout for metadata operations.
    rsync_args : list, optional
        Extra arguments passed to rsync (e.g. ['--bwlimit=1000']).
    compress : bool
        Enable rsync compression (-z). Default True for remote transfers.
    """

    def __init__(self, name: str, host: str, remote_path: str,
                 username: str, password: Optional[str] = None,
                 key_filename: Optional[str] = None,
                 port: int = 22, speed_priority: int = 50,
                 connect_timeout: float = 10.0,
                 rsync_args: Optional[List[str]] = None,
                 compress: bool = True):
        super().__init__(
            name=name,
            host=host,
            remote_path=remote_path,
            username=username,
            password=password,
            key_filename=key_filename,
            port=port,
            speed_priority=speed_priority,
            connect_timeout=connect_timeout,
        )
        self.backend_type = BackendType.RSYNC
        self._rsync_args = rsync_args or []
        self._compress = compress
        self.logger = logging.getLogger(__name__)

    def _build_rsync_ssh_cmd(self) -> str:
        """Build the ssh command string for rsync's -e flag."""
        parts = ["ssh"]
        parts.append(f"-p {self.port}")
        if self.key_filename:
            parts.append(f"-i {shlex.quote(self.key_filename)}")
        parts.append("-o BatchMode=yes")
        # Accept unseen host keys (like paramiko's AutoAddPolicy) but refuse
        # a changed one; unlike paramiko this records them in known_hosts.
        parts.append("-o StrictHostKeyChecking=accept-new")
        parts.append(f"-o ConnectTimeout={int(self.connect_timeout)}")
        return " ".join(parts)

    def _remote_spec(self, virtual_path: str) -> str:
        """Build rsync remote file spec: user@host:/path."""
        remote_path = str(self._resolve_path(virtual_path))
        if self.username:
            return f"{self.username}@{self.host}:{remote_path}"
        return f"{self.host}:{remote_path}"

    def _run_rsync(self, src: str, dst: str, extra_args: Optional[List[str]] = None) -> subprocess.CompletedProcess:
        """
        Run an rsync command and return the result.

        Uses --partial-dir so partials go to a temp directory instead
        of overwriting the destination.  On success, rsync atomically
        renames the temp file to the destination.  On failure, the
        destination is untouched and the partial stays in the temp dir
        for future resume.

        Raises RuntimeError on non-zero exit code.
        """
        cmd = ["rsync", "-a", f"--partial-dir={_PARTIAL_DIR}", "-s"]
        if self._compress:
            cmd.append("-z")
        cmd.extend(["-e", self._build_rsync_ssh_cmd()])
        if self._rsync_args:
            cmd.extend(self._rsync_args)
        if extra_args:
            cmd.extend(extra_args)
        cmd.extend([src, dst])

        self.logger.debug("rsync: %s", " ".join(cmd))

        # Use a longer timeout than SFTP: rsync transfers can legitimately
        # take much longer than an SFTP stat/read call, especially for the
        # large files this backend is meant to handle.
        rsync_timeout = max(300.0, self.sftp_operation_timeout * 5)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=rsync_timeout,
        )

        if result.returncode != 0:
            self.logger.warning(
                "rsync failed (rc=%d): %s",
                result.returncode, result.stderr.strip()
            )
            raise RuntimeError(
                f"rsync failed with exit code {result.returncode}: "
                f"{result.stderr.strip()}"
            )

        return result

    # ------------------------------------------------------------------ #
    # Override write transfer to use rsync; reads use inherited SFTP
    # ------------------------------------------------------------------ #

    # copy_to_local is intentionally NOT overridden.  SFTP's persistent
    # per-thread connection is faster for reads (no fork overhead, no
    # SSH handshake per file, progressive download).  rsync is only
    # beneficial for writes where delta compression matters.

    def copy_from_local(self, local_path: Path, virtual_path: str):
        """Upload a file to remote using rsync."""
        # Ensure remote parent directory exists (rsync doesn't create
        # intermediate dirs on the remote like mkdir -p).
        remote_path = str(self._resolve_path(virtual_path))
        parent = str(PurePosixPath(remote_path).parent)
        try:
            _, sftp_client = self._current_clients()
            try:
                sftp_client.stat(parent)
            except FileNotFoundError:
                self._mkdir_recursive(parent, sftp_client=sftp_client)
        except Exception:
            # If SFTP mkdir fails, rsync might still work if dir exists.
            pass

        remote = self._remote_spec(virtual_path)
        try:
            self._run_rsync(str(local_path), remote)
        except subprocess.TimeoutExpired:
            self.logger.warning("rsync copy_from_local timed out for %s", virtual_path)
            self._mark_unavailable()
            raise
        except RuntimeError:
            raise

    def __repr__(self):
        return (
            f"RsyncBackend(name={self.name}, host={self.host}, "
            f"remote_path={self.remote_path})"
        )
