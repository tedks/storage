import tempfile
import unittest

from storage.backends.base import WatcherSpec
from storage.backends.local import LocalBackend
from storage.backends.rsync import RsyncBackend
from storage.backends.ssh import SSHBackend
from storage.fs import fuse_subtype_options


class WatchableLoopbackMember(LocalBackend):
    def push_invalidation_spec(self):
        return WatcherSpec(
            group_key=("watch-host", 22, "user", None),
            ssh_host="watch-host",
            ssh_port=22,
            username="user",
            key_filename=None,
            remote_path="/srv/data",
        )


class TestStorageMemberCapabilities(unittest.TestCase):
    def test_transport_locality_distinct_from_reconnect_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            backend = LocalBackend("local0", tmp)
            backend.is_remote = True

            self.assertTrue(backend.is_remote)
            self.assertFalse(backend.transport_is_remote)
            self.assertTrue(backend.supports_reconnect_probe())

            info = backend.get_info()
            self.assertFalse(info.transport_is_remote)
            self.assertFalse(info.is_remote)

    def test_push_invalidation_capability_follows_spec(self):
        with tempfile.TemporaryDirectory() as tmp:
            backend = WatchableLoopbackMember("member0", tmp)

            self.assertTrue(backend.supports_push_invalidation())
            self.assertIsNotNone(backend.push_invalidation_spec())
            self.assertIsNotNone(backend.watcher_spec())
            self.assertTrue(backend.capabilities.push_invalidation)

    def test_push_invalidation_flag_cannot_drift_from_spec(self):
        with tempfile.TemporaryDirectory() as tmp:
            backend = LocalBackend("local0", tmp)
            backend.update_capabilities(push_invalidation=True)

            self.assertFalse(backend.supports_push_invalidation())
            self.assertIsNone(backend.push_invalidation_spec())
            self.assertIsNone(backend.watcher_spec())
            self.assertFalse(backend.capabilities.push_invalidation)


class TestFuseSubtypeOptions(unittest.TestCase):
    """GLib calls a mount remote only for some types; fuse.sshfs is the FUSE one."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.local = LocalBackend("local0", self._tmp.name)
        self.ssh = SSHBackend("ssh0", "host.invalid", "/srv", "user")
        self.rsync = RsyncBackend("rsync0", "host.invalid", "/srv", "user")

    def test_ssh_member_gets_sshfs_subtype(self):
        self.assertEqual(fuse_subtype_options({"ssh0": self.ssh}), {"subtype": "sshfs"})

    def test_rsync_member_gets_sshfs_subtype(self):
        self.assertEqual(fuse_subtype_options({"rsync0": self.rsync}), {"subtype": "sshfs"})

    def test_local_only_mount_keeps_plain_fuse_type(self):
        self.assertEqual(fuse_subtype_options({"local0": self.local}), {})

    def test_no_members_keeps_plain_fuse_type(self):
        self.assertEqual(fuse_subtype_options({}), {})

    def test_mixed_mount_is_remote(self):
        members = {"local0": self.local, "ssh0": self.ssh}
        self.assertEqual(fuse_subtype_options(members), {"subtype": "sshfs"})

    def test_reconnect_probe_flag_on_local_member_does_not_count(self):
        self.local.is_remote = True
        self.assertEqual(fuse_subtype_options({"local0": self.local}), {})
