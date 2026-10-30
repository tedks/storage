"""
Tests for allow_other functionality in FUSE mount
"""

import unittest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock

from storage.fs import mount_storage
from storage.backends.local import LocalBackend
from storage.backends.ssh import SSHBackend


class TestAllowOther(unittest.TestCase):
    """Test allow_other parameter handling"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_root = Path(self.temp_dir) / 'cache'
        self.cache_root.mkdir()
        self.state_db = self.cache_root / 'state.db'
        self.mount_point = Path(self.temp_dir) / 'mount'
        self.mount_point.mkdir()
        self.backend_path = Path(self.temp_dir) / 'backend'
        self.backend_path.mkdir()

        self.backends = {
            'local0': LocalBackend('local0', str(self.backend_path))
        }

    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('storage.fs.FUSE')
    @patch('os.geteuid')
    def test_allow_other_auto_detect_root(self, mock_geteuid, mock_fuse):
        """Test that allow_other=True when running as root"""
        mock_geteuid.return_value = 0  # Simulate root

        mount_storage(
            str(self.mount_point),
            self.cache_root,
            self.state_db,
            self.backends,
            1024 * 1024 * 100,  # 100MB
            foreground=True,
            allow_other=None  # Auto-detect
        )

        # Verify FUSE was called with allow_other=True
        mock_fuse.assert_called_once()
        args, kwargs = mock_fuse.call_args
        self.assertTrue(kwargs['allow_other'])

    @patch('storage.fs.FUSE')
    @patch('os.geteuid')
    def test_allow_other_auto_detect_non_root(self, mock_geteuid, mock_fuse):
        """Test that allow_other=False when running as non-root"""
        mock_geteuid.return_value = 1000  # Simulate non-root user

        with self.assertLogs('storage.fs', level='WARNING') as log:
            mount_storage(
                str(self.mount_point),
                self.cache_root,
                self.state_db,
                self.backends,
                1024 * 1024 * 100,
                foreground=True,
                allow_other=None  # Auto-detect
            )

        # Verify warning was logged
        self.assertTrue(any('allow_other=False' in msg for msg in log.output))

        # Verify FUSE was called with allow_other=False
        mock_fuse.assert_called_once()
        args, kwargs = mock_fuse.call_args
        self.assertFalse(kwargs['allow_other'])

    @patch('storage.fs.FUSE')
    @patch('os.geteuid')
    def test_allow_other_explicit_true_as_root(self, mock_geteuid, mock_fuse):
        """Test explicit allow_other=True as root"""
        mock_geteuid.return_value = 0  # Root

        mount_storage(
            str(self.mount_point),
            self.cache_root,
            self.state_db,
            self.backends,
            1024 * 1024 * 100,
            foreground=True,
            allow_other=True
        )

        # Verify FUSE was called with allow_other=True
        mock_fuse.assert_called_once()
        args, kwargs = mock_fuse.call_args
        self.assertTrue(kwargs['allow_other'])

    @patch('storage.fs.FUSE')
    @patch('os.geteuid')
    def test_allow_other_explicit_false_as_root(self, mock_geteuid, mock_fuse):
        """Test explicit allow_other=False as root (should work)"""
        mock_geteuid.return_value = 0  # Root

        mount_storage(
            str(self.mount_point),
            self.cache_root,
            self.state_db,
            self.backends,
            1024 * 1024 * 100,
            foreground=True,
            allow_other=False
        )

        # Verify FUSE was called with allow_other=False
        mock_fuse.assert_called_once()
        args, kwargs = mock_fuse.call_args
        self.assertFalse(kwargs['allow_other'])

    @patch('storage.fs.FUSE')
    @patch('os.geteuid')
    @patch('builtins.open', new_callable=mock_open, read_data='')
    def test_allow_other_true_non_root_no_user_allow_other(self, mock_file, mock_geteuid, mock_fuse):
        """Test allow_other=True as non-root without user_allow_other in fuse.conf"""
        mock_geteuid.return_value = 1000  # Non-root

        with self.assertRaises(PermissionError) as ctx:
            mount_storage(
                str(self.mount_point),
                self.cache_root,
                self.state_db,
                self.backends,
                1024 * 1024 * 100,
                foreground=True,
                allow_other=True
            )

        self.assertIn('user_allow_other', str(ctx.exception))
        self.assertIn('fuse.conf', str(ctx.exception))

    @patch('storage.fs.FUSE')
    @patch('os.geteuid')
    @patch('builtins.open', new_callable=mock_open, read_data='user_allow_other\n')
    def test_allow_other_true_non_root_with_user_allow_other(self, mock_file, mock_geteuid, mock_fuse):
        """Test allow_other=True as non-root WITH user_allow_other in fuse.conf"""
        mock_geteuid.return_value = 1000  # Non-root

        mount_storage(
            str(self.mount_point),
            self.cache_root,
            self.state_db,
            self.backends,
            1024 * 1024 * 100,
            foreground=True,
            allow_other=True
        )

        # Verify FUSE was called with allow_other=True
        mock_fuse.assert_called_once()
        args, kwargs = mock_fuse.call_args
        self.assertTrue(kwargs['allow_other'])

    @patch('storage.fs.FUSE')
    @patch('os.geteuid')
    @patch('builtins.open', side_effect=FileNotFoundError())
    def test_allow_other_true_non_root_fuse_conf_missing(self, mock_file, mock_geteuid, mock_fuse):
        """Test allow_other=True as non-root when fuse.conf doesn't exist"""
        mock_geteuid.return_value = 1000  # Non-root

        with self.assertRaises(PermissionError) as ctx:
            mount_storage(
                str(self.mount_point),
                self.cache_root,
                self.state_db,
                self.backends,
                1024 * 1024 * 100,
                foreground=True,
                allow_other=True
            )

        self.assertIn('file not found', str(ctx.exception))

    @patch('storage.fs.FUSE')
    @patch('os.geteuid')
    def test_allow_other_false_non_root(self, mock_geteuid, mock_fuse):
        """Test explicit allow_other=False as non-root (should work)"""
        mock_geteuid.return_value = 1000  # Non-root

        mount_storage(
            str(self.mount_point),
            self.cache_root,
            self.state_db,
            self.backends,
            1024 * 1024 * 100,
            foreground=True,
            allow_other=False
        )

        # Verify FUSE was called with allow_other=False
        mock_fuse.assert_called_once()
        args, kwargs = mock_fuse.call_args
        self.assertFalse(kwargs['allow_other'])

    def _mount_with(self, backends, mock_fuse):
        mount_storage(
            str(self.mount_point), self.cache_root, self.state_db, backends,
            1024 * 1024 * 100, foreground=True, allow_other=False,
        )
        mock_fuse.assert_called_once()
        return mock_fuse.call_args.kwargs

    @patch('storage.fs.FUSE')
    @patch('os.geteuid', return_value=1000)
    def test_remote_member_mount_requests_sshfs_subtype(self, mock_geteuid, mock_fuse):
        backends = dict(self.backends)
        backends['ssh0'] = SSHBackend('ssh0', 'host.invalid', '/srv', 'user')
        self.assertEqual(self._mount_with(backends, mock_fuse).get('subtype'), 'sshfs')

    @patch('storage.fs.FUSE')
    @patch('os.geteuid', return_value=1000)
    def test_local_only_mount_requests_no_subtype(self, mock_geteuid, mock_fuse):
        self.assertNotIn('subtype', self._mount_with(self.backends, mock_fuse))

class TestMountKernelCacheOptions(unittest.TestCase):
    """mount_storage hands fuse_mount_options to FUSE, and rejects bad timeouts first."""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        import shutil
        self.addCleanup(shutil.rmtree, self.temp_dir, True)
        self.cache_root = self.temp_dir / 'cache'
        self.cache_root.mkdir()
        self.mount_point = self.temp_dir / 'mount'
        self.mount_point.mkdir()
        (self.temp_dir / 'backend').mkdir()
        self.backends = {'local0': LocalBackend('local0', str(self.temp_dir / 'backend'))}

    def _mount(self, **kwargs):
        mount_storage(str(self.mount_point), self.cache_root, self.cache_root / 'state.db',
                      self.backends, 1024 * 1024, foreground=True, allow_other=False, **kwargs)

    @patch('storage.fs.FUSE')
    def test_options_reach_fuse(self, mock_fuse):
        self._mount(revalidate_after=45.0, negative_timeout=3.0)
        kwargs = mock_fuse.call_args.kwargs
        self.assertEqual(0.0, kwargs['attr_timeout'])
        self.assertEqual(45.0, kwargs['entry_timeout'])
        self.assertEqual(3.0, kwargs['negative_timeout'])
        self.assertIs(True, kwargs['auto_cache'])
        self.assertEqual(0.0, kwargs['ac_attr_timeout'])
        self.assertTrue(kwargs['nothreads'])

    @patch('storage.fs.FUSE')
    def test_bad_timeout_fails_before_the_mount_lock(self, mock_fuse):
        with self.assertRaises(ValueError):
            self._mount(entry_timeout=float('inf'))
        mock_fuse.assert_not_called()
        self.assertEqual([], list(self.cache_root.glob('.storage.mount.*.lock')))


if __name__ == '__main__':
    unittest.main()
