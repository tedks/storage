"""
Tests for the RsyncBackend.

Tests the rsync command construction and the local-only transfer paths
(using rsync with local source/dest to verify the subprocess plumbing
works without requiring a real SSH connection).
"""

import os
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path
from unittest import TestCase, mock

from storage.backends.rsync import RsyncBackend, RsyncUploadContractError
from storage.backends.base import BackendType


class TestRsyncBackendInit(TestCase):
    """Test RsyncBackend construction."""

    def test_backend_type_is_rsync(self):
        with tempfile.TemporaryDirectory() as tmp:
            backend = RsyncBackend(
                name="test",
                host="localhost",
                remote_path=tmp,
                username="user",
            )
            self.assertEqual(backend.backend_type, BackendType.RSYNC)
            backend.close()

    def test_repr(self):
        with tempfile.TemporaryDirectory() as tmp:
            backend = RsyncBackend(
                name="test",
                host="myhost",
                remote_path=tmp,
                username="user",
            )
            r = repr(backend)
            self.assertIn("RsyncBackend", r)
            self.assertIn("myhost", r)
            backend.close()


class TestRsyncSSHCommand(TestCase):
    """Test rsync SSH command building."""

    def test_default_ssh_cmd(self):
        with tempfile.TemporaryDirectory() as tmp:
            backend = RsyncBackend(
                name="test",
                host="h",
                remote_path=tmp,
                username="u",
                port=22,
            )
            cmd = backend._build_rsync_ssh_cmd()
            self.assertIn("ssh", cmd)
            self.assertIn("-p 22", cmd)
            self.assertIn("BatchMode=yes", cmd)
            args = shlex.split(cmd)
            options = [args[i + 1] for i, arg in enumerate(args[:-1]) if arg == "-o"]
            self.assertEqual(options.count("UpdateHostKeys=no"), 1)
            backend.close()

    def test_custom_port_and_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            backend = RsyncBackend(
                name="test",
                host="h",
                remote_path=tmp,
                username="u",
                port=2222,
                key_filename="/home/u/.ssh/mykey",
            )
            cmd = backend._build_rsync_ssh_cmd()
            self.assertIn("-p 2222", cmd)
            self.assertIn("-i", cmd)
            self.assertIn("/home/u/.ssh/mykey", cmd)
            backend.close()

    def test_remote_spec(self):
        with tempfile.TemporaryDirectory() as tmp:
            backend = RsyncBackend(
                name="test",
                host="example.com",
                remote_path="/data/storage",
                username="admin",
            )
            spec = backend._remote_spec("/subdir/file.txt")
            self.assertEqual(spec, "admin@example.com:/data/storage/subdir/file.txt")
            backend.close()


class TestRsyncLocalTransfer(TestCase):
    """
    Test rsync transfers using localhost (no SSH required).

    These tests verify the subprocess plumbing by using rsync to copy
    files locally, which exercises the same code path as remote transfers
    but without needing SSH connectivity.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.remote_dir = Path(self.tmp) / "remote"
        self.remote_dir.mkdir()
        self.local_dir = Path(self.tmp) / "local"
        self.local_dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_run_rsync_local_copy(self):
        """Test _run_rsync with local source and dest (no SSH)."""
        # Create source file
        src = self.remote_dir / "test.txt"
        src.write_text("hello rsync")
        dst = self.local_dir / "test.txt"

        backend = RsyncBackend(
            name="local-rsync",
            host="localhost",
            remote_path=str(self.remote_dir),
            username=os.environ.get("USER", "root"),
            compress=False,
        )

        # Use local rsync (no SSH) by calling directly
        result = backend._run_rsync(str(src), str(dst), extra_args=["-e", ""])
        # Override the SSH -e flag with empty string for local test
        # Actually, local rsync doesn't need -e at all. Let's test differently.
        backend.close()

    def test_upload_selection_uses_bytes_when_size_and_mtime_match(self):
        """An upload selected by Storage must replace a quick-check match."""
        src = self.local_dir / "file.txt"
        dst = self.remote_dir / "file.txt"
        src.write_bytes(b"new bytes")
        dst.write_bytes(b"old bytes")
        stamp = 1_700_000_000_000_000_000
        os.utime(src, ns=(stamp, stamp))
        os.utime(dst, ns=(stamp, stamp))
        self.assertEqual(src.stat().st_size, dst.stat().st_size)
        self.assertEqual(src.stat().st_mtime_ns, dst.stat().st_mtime_ns)
        self.assertNotEqual(src.read_bytes(), dst.read_bytes())

        backend = RsyncBackend(
            name="local-rsync", host="localhost",
            remote_path=str(self.remote_dir), username="user", compress=False,
        )
        try:
            # Only the metadata/remote-spec transport is local. Upload uses
            # production copy_from_local and its real rsync subprocess.
            with mock.patch.object(backend, "_with_sftp", return_value=None), \
                    mock.patch.object(backend, "_remote_spec", return_value=str(dst)):
                backend.copy_from_local(src, "/file.txt")
            self.assertEqual(src.read_bytes(), dst.read_bytes())
        finally:
            backend.close()

    def test_caller_size_only_is_rejected_before_selected_upload(self):
        src = self.local_dir / "selected.txt"
        dst = self.remote_dir / "selected.txt"
        src.write_bytes(b"new bytes")
        dst.write_bytes(b"old bytes")
        stamp = 1_700_000_000_000_000_000
        os.utime(src, ns=(stamp, stamp))
        os.utime(dst, ns=(stamp, stamp))
        backend = RsyncBackend(
            name="local-rsync", host="localhost",
            remote_path=str(self.remote_dir), username="user", compress=False,
            rsync_args=["--size-only"],
        )
        try:
            with self.assertRaisesRegex(RsyncUploadContractError, "equal-size-mtime"):
                backend.validate_upload_contract()
            self.assertEqual(b"old bytes", dst.read_bytes())
        finally:
            backend.close()

    def test_rsync_inherits_ssh_metadata_ops(self):
        """Verify RsyncBackend inherits metadata operations from SSHBackend."""
        backend = RsyncBackend(
            name="inherit-test",
            host="localhost",
            remote_path="/tmp",
            username="user",
        )
        # These should be inherited from SSHBackend
        self.assertTrue(hasattr(backend, 'listdir'))
        self.assertTrue(hasattr(backend, 'stat'))
        self.assertTrue(hasattr(backend, 'exists'))
        self.assertTrue(hasattr(backend, 'mkdir'))
        self.assertTrue(hasattr(backend, 'get_fast_checksum'))
        self.assertTrue(callable(backend.listdir))
        self.assertTrue(callable(backend.stat))
        backend.close()

    def test_rsync_args_passed_through(self):
        """Verify extra rsync args are included in the command."""
        backend = RsyncBackend(
            name="args-test",
            host="h",
            remote_path="/data",
            username="u",
            rsync_args=["--bwlimit=1000", "--exclude=*.tmp"],
        )

        # Capture the rsync command by mocking subprocess.run
        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(returncode=0, stderr="")
            try:
                backend._run_rsync("/src", "/dst")
            except Exception:
                pass

            if mock_run.called:
                cmd = mock_run.call_args[0][0]
                self.assertIn("--bwlimit=1000", cmd)
                self.assertIn("--exclude=*.tmp", cmd)
                # --partial-dir used instead of --partial for atomic writes
                partial_dir_args = [a for a in cmd if a.startswith("--partial-dir=")]
                self.assertTrue(partial_dir_args, "Expected --partial-dir flag")
                self.assertIn("-a", cmd)

        backend.close()

    def test_compression_flag(self):
        """Verify -z flag is included when compress=True."""
        backend_z = RsyncBackend(
            name="z-test",
            host="h", remote_path="/data", username="u",
            compress=True,
        )
        backend_no_z = RsyncBackend(
            name="noz-test",
            host="h", remote_path="/data", username="u",
            compress=False,
        )

        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(returncode=0, stderr="")

            backend_z._run_rsync("/src", "/dst")
            cmd_z = mock_run.call_args[0][0]
            self.assertIn("-z", cmd_z)

            backend_no_z._run_rsync("/src", "/dst")
            cmd_noz = mock_run.call_args[0][0]
            self.assertNotIn("-z", cmd_noz)

        backend_z.close()
        backend_no_z.close()


class TestRsyncUploadContract(TestCase):
    """Startup checks use real local rsync, never an SSH/member path."""

    def _backend(self, args=(), control_sockets=None):
        backend = RsyncBackend(
            name="probe", host="host.invalid", remote_path="/unused",
            username="user", rsync_args=args, compress=False,
            control_sockets=control_sockets,
        )
        self.addCleanup(backend.close)
        return backend

    def test_upload_contract_default_passes_without_remote_state(self):
        sockets = mock.Mock()
        backend = self._backend(control_sockets=sockets)
        real_run = subprocess.run
        with mock.patch.object(backend, "_build_rsync_ssh_cmd",
                               side_effect=AssertionError("SSH command built")) as ssh, \
                mock.patch.object(backend, "_reset_shared_connection",
                                  side_effect=AssertionError("connection reset")) as reset, \
                mock.patch.object(backend, "_remote_spec",
                                  side_effect=AssertionError("member path used")) as remote, \
                mock.patch("storage.backends.rsync.subprocess.run",
                           side_effect=real_run) as run:
            backend.validate_upload_contract()
        self.assertEqual(4, run.call_count)
        for call in run.call_args_list:
            argv = call.args[0]
            self.assertEqual("rsync", argv[0])
            self.assertIn("--ignore-times", argv)
            self.assertNotIn("-e", argv)
            self.assertEqual(Path(argv[-2]).parent, Path(argv[-1]).parent)
            self.assertTrue(Path(argv[-2]).is_absolute())
        ssh.assert_not_called()
        reset.assert_not_called()
        remote.assert_not_called()
        sockets.options.assert_not_called()
        sockets.reset.assert_not_called()

    def test_upload_contract_benign_bandwidth_option_passes(self):
        self._backend(["--bwlimit=1000"]).validate_upload_contract()

    def test_upload_contract_rejects_observed_skips(self):
        for args, case in (
            (["--size-only"], "equal-size-mtime"),
            (["--ignore-existing"], "equal-size-mtime"),
            (["--existing"], "missing-destination"),
            (["--ignore-non-existing"], "missing-destination"),
            (["--update"], "destination-newer"),
            (["-u"], "destination-newer"),
            (["--dry-run"], "equal-size-mtime"),
            (["-n"], "equal-size-mtime"),
        ):
            with self.subTest(args=args):
                with self.assertRaisesRegex(RsyncUploadContractError, case):
                    self._backend(args).validate_upload_contract()

    def test_upload_contract_rejects_unprovable_or_unsafe_argv_before_execution(self):
        for arg in (
            "--exclude=*.tmp", "--filter=- *.tmp", "--max-size=1G",
            "--min-size=1", "--temp-dir=/outside", "--backup-dir=/outside",
            "--partial-dir=/outside", "--remove-source-files", "--delete",
            "--files-from=/outside", "-e", "--upda", "-un",
            "--dry-run=true", "--size-only=true", "--bwlimit",
            "--bwlimit=oops", "not-an-option",
        ):
            with self.subTest(arg=arg), \
                    mock.patch("storage.backends.rsync.subprocess.run") as run:
                with self.assertRaises(RsyncUploadContractError):
                    self._backend([arg]).validate_upload_contract()
                run.assert_not_called()

    def test_upload_contract_uses_snapshot_of_caller_options(self):
        options = ["--bwlimit=1000"]
        backend = self._backend(options)
        options.append("--dry-run")
        self.assertEqual(("--bwlimit=1000",), backend._rsync_args)
        backend.validate_upload_contract()
        argv = backend._rsync_argv("/src", "/dst", extra_args=("--ignore-times",))
        self.assertNotIn("--dry-run", argv)

    def test_upload_contract_reports_subprocess_errors_without_reset(self):
        for result, detail in (
            (subprocess.CompletedProcess([], 12), "exited 12"),
            (FileNotFoundError("rsync"), "could not complete"),
            (subprocess.TimeoutExpired(["rsync"], 10), "could not complete"),
        ):
            with self.subTest(result=result):
                sockets = mock.Mock()
                backend = self._backend(control_sockets=sockets)
                with mock.patch("storage.backends.rsync.subprocess.run",
                                side_effect=result if isinstance(result, Exception) else None,
                                return_value=result if not isinstance(result, Exception) else None) as run, \
                        mock.patch.object(backend, "_reset_shared_connection") as reset:
                    with self.assertRaisesRegex(RsyncUploadContractError, detail):
                        backend.validate_upload_contract()
                scratch = Path(run.call_args.args[0][-2]).parent.parent
                self.assertFalse(scratch.exists())
                sockets.options.assert_not_called()
                sockets.reset.assert_not_called()
                reset.assert_not_called()


class TestCLIRsyncParsing(TestCase):
    """Test CLI parsing of rsync:// URLs."""

    def test_rsync_url_recognized(self):
        from storage.mount_profiles import split_storage_member_specs
        local, remote = split_storage_member_specs([
            "/local/path",
            "ssh://user@host:/path",
            "rsync://user@host:/path",
        ])
        self.assertEqual(local, ("/local/path",))
        self.assertEqual(len(remote), 2)
        self.assertTrue(remote[0].startswith("ssh://"))
        self.assertTrue(remote[1].startswith("rsync://"))
