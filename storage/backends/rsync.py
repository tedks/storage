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
import re
import shlex
import subprocess
import tempfile
from pathlib import Path
from typing import List, Optional

from .base import Backend, BackendType
from .ssh import SSHBackend
from .ssh_mux import ControlSockets

# Hidden directory on each side for rsync partial transfers.
# rsync writes to this dir and atomically renames to destination on
# success.  On failure, the destination is untouched.
_PARTIAL_DIR = ".storage.rsync-tmp"

# rsync exit codes that say the connection, not the files, failed (12: the
# protocol stream ended; 255: ssh itself failed). A shared ssh master behind
# one of them may be hung or gone, so it is ended and the next upload starts
# a fresh one.
_CONNECTION_EXIT_CODES = frozenset({12, 255})

# These options cannot redirect a scratch probe or change its file list.
# This is a safety boundary for the probe, not a claim that the options
# satisfy Storage's upload contract: the byte checks decide that.
_PROBE_SAFE_FLAGS = frozenset({
    "--size-only", "--ignore-times", "--ignore-existing",
    "--ignore-non-existing", "--existing", "--update", "-u",
    "--dry-run", "-n",
})


class RsyncError(RuntimeError):
    """rsync exited non-zero."""

    def __init__(self, message: str, returncode: int):
        super().__init__(message)
        self.returncode = returncode


class RsyncUploadContractError(RuntimeError):
    """The configured rsync command cannot be proved safe for selected uploads."""


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
        Extra arguments passed to rsync (e.g. ['--bwlimit=1000']). A mount
        validates their upload behavior and rejects options it cannot
        safely probe.
    compress : bool
        Enable rsync compression (-z). Default True for remote transfers.
    control_sockets : ControlSockets, optional
        Share one system ssh connection per host among rsync runs (and the
        watcher, given the same object) instead of one handshake per
        upload. None connects afresh every time.
    """

    def __init__(self, name: str, host: str, remote_path: str,
                 username: str, password: Optional[str] = None,
                 key_filename: Optional[str] = None,
                 port: int = 22, speed_priority: int = 50,
                 connect_timeout: float = 10.0,
                 rsync_args: Optional[List[str]] = None,
                 compress: bool = True,
                 control_sockets: Optional[ControlSockets] = None):
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
        # A later mutation of the caller's list must not change the options
        # after startup has validated them.
        self._rsync_args = tuple(rsync_args or ())
        self._compress = compress
        self._control_sockets = control_sockets
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
        # A member sharing a hostname may advertise only its own keys.
        # Never let OpenSSH prune other trusted keys after authentication.
        parts.append("-o UpdateHostKeys=no")
        parts.append(f"-o ConnectTimeout={int(self.connect_timeout)}")
        if self._control_sockets is not None:
            options = self._control_sockets.options(
                self.username, self.host, self.port, self.key_filename)
            if options:
                # A shared master that goes quiet must die, not hold every
                # later upload; it takes its options from whoever started it.
                parts.extend(["-o ServerAliveInterval=15", "-o ServerAliveCountMax=2"])
                parts.extend(shlex.quote(option) for option in options)
        return " ".join(parts)

    def _reset_shared_connection(self) -> None:
        if self._control_sockets is not None:
            self._control_sockets.reset(
                self.username, self.host, self.port, self.key_filename)

    def _remote_spec(self, virtual_path: str) -> str:
        """Build rsync remote file spec: user@host:/path."""
        remote_path = str(self._resolve_path(virtual_path))
        if self.username:
            return f"{self.username}@{self.host}:{remote_path}"
        return f"{self.host}:{remote_path}"

    def _rsync_argv(self, src: str, dst: str, *,
                    extra_args=(), ssh_cmd: Optional[str] = None) -> List[str]:
        """Pure argv construction shared by remote uploads and local probes."""
        cmd = ["rsync", "-a", f"--partial-dir={_PARTIAL_DIR}", "-s"]
        if self._compress:
            cmd.append("-z")
        if ssh_cmd is not None:
            cmd.extend(["-e", ssh_cmd])
        cmd.extend(self._rsync_args)
        cmd.extend(extra_args)
        cmd.extend([src, dst])
        return cmd

    def _check_probe_argv(self) -> None:
        """Reject options that cannot be safely tested in isolated scratch."""
        for index, arg in enumerate(self._rsync_args, start=1):
            if isinstance(arg, str) and arg in _PROBE_SAFE_FLAGS:
                continue
            if isinstance(arg, str) and re.fullmatch(r"--bwlimit=[0-9]+[kKmMgG]?", arg):
                continue
            # Do not include values or unknown token text in a mount error:
            # they may contain private paths or credentials.
            label = ""
            if isinstance(arg, str) and arg.startswith("--"):
                name = arg.partition("=")[0]
                if re.fullmatch(r"--[a-z][a-z-]*", name):
                    label = f" ({name})"
            raise RsyncUploadContractError(
                f"rsync member {self.name}: argument {index}{label} cannot be "
                "safely validated for selected uploads"
            )

    def validate_upload_contract(self) -> None:
        """Probe configured upload options locally before a mount can serve.

        Only probe-safe arguments are executed. The command never builds an
        SSH command, addresses the member, or resets a shared connection.
        A local passing probe checks selection semantics, not remote host
        availability or remote rsync compatibility.
        """
        self._check_probe_argv()
        stamp = 1_700_000_000_000_000_000
        cases = (
            ("equal-size-mtime", b"new bytes", b"old bytes", stamp, stamp),
            ("missing-destination", b"new bytes", None, stamp, None),
            ("destination-newer", b"new bytes", b"old bytes", stamp, stamp + 10_000_000_000),
            ("changed-size", b"new longer bytes", b"old", stamp, stamp),
        )
        with tempfile.TemporaryDirectory(prefix="storage-rsync-probe-") as tmp:
            root = Path(tmp)
            for name, intended, prior, source_time, dest_time in cases:
                case_dir = root / name
                case_dir.mkdir()
                src, dst = case_dir / "src", case_dir / "dst"
                src.write_bytes(intended)
                os.utime(src, ns=(source_time, source_time))
                if prior is not None:
                    dst.write_bytes(prior)
                    os.utime(dst, ns=(dest_time, dest_time))
                cmd = self._rsync_argv(
                    str(src), str(dst), extra_args=("--ignore-times",),
                )
                try:
                    result = subprocess.run(
                        cmd, stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                        timeout=10,
                    )
                except FileNotFoundError as exc:
                    raise RsyncUploadContractError(
                        f"rsync member {self.name}: local rsync executable unavailable"
                    ) from exc
                except subprocess.TimeoutExpired as exc:
                    raise RsyncUploadContractError(
                        f"rsync member {self.name}: upload probe {name} timed out"
                    ) from exc
                except OSError as exc:
                    raise RsyncUploadContractError(
                        f"rsync member {self.name}: upload probe {name} could not complete"
                    ) from exc
                if result.returncode != 0:
                    raise RsyncUploadContractError(
                        f"rsync member {self.name}: upload probe {name} "
                        f"exited {result.returncode}"
                    )
                if not dst.is_file() or dst.read_bytes() != intended:
                    raise RsyncUploadContractError(
                        f"rsync member {self.name}: upload probe {name} "
                        "did not write the selected file's bytes"
                    )

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
        cmd = self._rsync_argv(
            src, dst, extra_args=extra_args or (),
            ssh_cmd=self._build_rsync_ssh_cmd(),
        )

        self.logger.debug("rsync: %s", " ".join(cmd))

        # Use a longer timeout than SFTP: rsync transfers can legitimately
        # take much longer than an SFTP stat/read call, especially for the
        # large files this backend is meant to handle.
        rsync_timeout = max(300.0, self.sftp_operation_timeout * 5)
        # Output goes to files, not pipes: a shared ssh master forked from this
        # run's ssh may keep its descriptors, and a pipe would then not reach
        # EOF until the master exits (older OpenSSH releases keep stderr).
        with tempfile.TemporaryFile("w+", errors="replace") as out, \
                tempfile.TemporaryFile("w+", errors="replace") as err:
            completed = subprocess.run(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=out,
                stderr=err,
                timeout=rsync_timeout,
            )
            out.seek(0)
            err.seek(0)
            result = subprocess.CompletedProcess(
                cmd, completed.returncode, out.read(), err.read())

        if result.returncode != 0:
            self.logger.warning(
                "rsync failed (rc=%d): %s",
                result.returncode, result.stderr.strip()
            )
            if result.returncode in _CONNECTION_EXIT_CODES:
                self._reset_shared_connection()
            raise RsyncError(
                f"rsync failed with exit code {result.returncode}: "
                f"{result.stderr.strip()}",
                result.returncode,
            )

        return result

    # ------------------------------------------------------------------ #
    # Override write transfer to use rsync; reads use inherited SFTP
    # ------------------------------------------------------------------ #

    # copy_to_local is intentionally NOT overridden.  SFTP's persistent
    # pooled connection is faster for reads (no fork overhead, no
    # SSH handshake per file, progressive download).  rsync is only
    # beneficial for writes where delta compression matters.

    def copy_from_local(self, local_path: Path, virtual_path: str):
        """Upload a file to remote using rsync."""
        # Ensure remote parent directory exists (rsync doesn't create
        # intermediate dirs on the remote like mkdir -p).
        remote_path = str(self._resolve_path(virtual_path))
        try:
            self._with_sftp("mkdir", virtual_path,
                            lambda sftp: self._ensure_parent(sftp, remote_path))
        except Exception:
            # If SFTP mkdir fails, rsync might still work if dir exists.
            pass

        remote = self._remote_spec(virtual_path)
        try:
            # Storage has already selected this file for upload. Rsync's
            # size/mtime quick check can otherwise report success without
            # replacing different bytes on the member. Rechecking an identical
            # file may add source/destination I/O; rsync still uses wire deltas.
            self._run_rsync(str(local_path), remote, extra_args=["--ignore-times"])
        except subprocess.TimeoutExpired as exc:
            self.logger.warning("rsync copy_from_local timed out for %s", virtual_path)
            # rsync runs over its own ssh; probe the member's transport rather
            # than drop it under the other operations sharing it.
            self._pool.check(exc)
            self._reset_shared_connection()
            raise
        except RuntimeError:
            raise

    def __repr__(self):
        return (
            f"RsyncBackend(name={self.name}, host={self.host}, "
            f"remote_path={self.remote_path})"
        )
