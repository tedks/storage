import tempfile
import unittest

from storage.backends.base import WatcherSpec
from storage.backends.local import LocalBackend


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
