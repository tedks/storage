import tempfile
import unittest

from storage.backends.base import WatcherSpec
from storage.backends.local import LocalBackend
from storage.backends.rsync import RsyncBackend
from storage.backends.ssh import SSHBackend
from storage.fs import (
    KERNEL_ATTR_TIMEOUT,
    DEFAULT_NEGATIVE_TIMEOUT,
    WATCHER_KERNEL_TIMEOUT,
    fuse_mount_options,
    fuse_subtype_options,
)


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


class TestFuseMountOptions(unittest.TestCase):
    """Kernel cache options follow the listing's fresh window (docs/browsing_latency_plan.md D2)."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.local = {"local0": LocalBackend("local0", self._tmp.name)}
        self.ssh = {"ssh0": SSHBackend("ssh0", "host.invalid", "/srv", "user")}

    def test_watchers_off_entries_follow_fresh_window(self):
        options = fuse_mount_options(self.local, enable_watchers=False, fresh_window=60.0)
        self.assertEqual(
            {
                "attr_timeout": KERNEL_ATTR_TIMEOUT,
                "entry_timeout": 60.0,
                "negative_timeout": DEFAULT_NEGATIVE_TIMEOUT,
                "auto_cache": True,
                "ac_attr_timeout": 0.0,
            },
            options,
        )
        self.assertEqual(10.0, DEFAULT_NEGATIVE_TIMEOUT)
        # A cached size would truncate reads of a refetched, grown file
        # (TestKernelCacheRefetch in test_sync_system.py).
        self.assertEqual(0.0, KERNEL_ATTR_TIMEOUT)

    def test_watchers_on_keep_one_second_whatever_the_window(self):
        options = fuse_mount_options(self.local, enable_watchers=True, fresh_window=600.0)
        self.assertEqual(KERNEL_ATTR_TIMEOUT, options["attr_timeout"])
        self.assertEqual(WATCHER_KERNEL_TIMEOUT, options["entry_timeout"])
        self.assertEqual(1.0, WATCHER_KERNEL_TIMEOUT)
        self.assertEqual(DEFAULT_NEGATIVE_TIMEOUT, options["negative_timeout"])

    def test_zero_window_asks_every_time(self):
        options = fuse_mount_options(self.local, enable_watchers=False, fresh_window=0)
        self.assertEqual(0.0, options["entry_timeout"])

    def test_never_kernel_cache(self):
        # kernel_cache would keep pages a revalidation refetch replaced.
        for watchers in (False, True):
            options = fuse_mount_options(self.ssh, enable_watchers=watchers, fresh_window=60.0)
            self.assertNotIn("kernel_cache", options)
            self.assertIs(True, options["auto_cache"])
            self.assertEqual(0.0, options["ac_attr_timeout"])

    def test_includes_subtype(self):
        self.assertEqual(
            "sshfs",
            fuse_mount_options(self.ssh, enable_watchers=False, fresh_window=60.0)["subtype"],
        )
        self.assertNotIn(
            "subtype", fuse_mount_options(self.local, enable_watchers=False, fresh_window=60.0)
        )

    def test_overrides_win_with_watchers_on_or_off(self):
        for watchers in (False, True):
            options = fuse_mount_options(
                self.local, enable_watchers=watchers, fresh_window=60.0,
                entry_timeout=7.5, negative_timeout=0,
            )
            self.assertEqual(KERNEL_ATTR_TIMEOUT, options["attr_timeout"])
            self.assertEqual(7.5, options["entry_timeout"])
            self.assertEqual(0.0, options["negative_timeout"])

    def test_attributes_have_no_override(self):
        # Any positive attr_timeout can truncate reads after a refetch.
        with self.assertRaises(TypeError):
            fuse_mount_options(self.local, enable_watchers=False, fresh_window=60.0,
                               attr_timeout=5.0)

    def test_partial_override_leaves_the_rest_derived(self):
        options = fuse_mount_options(
            self.local, enable_watchers=False, fresh_window=60.0, entry_timeout=2.0,
        )
        self.assertEqual(KERNEL_ATTR_TIMEOUT, options["attr_timeout"])
        self.assertEqual(2.0, options["entry_timeout"])
        self.assertEqual(DEFAULT_NEGATIVE_TIMEOUT, options["negative_timeout"])

    def test_rejects_negative_nan_and_infinity(self):
        # fusepy formats with %s and libfuse parses with %lf, which accepts
        # "inf" and "nan"; neither is a timeout the kernel can hold.
        for bad in (-1.0, float("nan"), float("inf"), True, "60"):
            for key in ("entry_timeout", "negative_timeout"):
                with self.subTest(key=key, value=bad), self.assertRaises(ValueError):
                    fuse_mount_options(
                        self.local, enable_watchers=False, fresh_window=60.0, **{key: bad}
                    )
            for watchers in (False, True):
                with self.subTest(fresh_window=bad, watchers=watchers), self.assertRaises(ValueError):
                    fuse_mount_options(self.local, enable_watchers=watchers, fresh_window=bad)

    def test_values_format_as_libfuse_options(self):
        # fusepy turns each keyword into -o key=value (True: a bare flag).
        from fuse import FUSE

        options = fuse_mount_options(self.ssh, enable_watchers=False, fresh_window=60.0)
        rendered = sorted(FUSE._normalize_fuse_options(**options))
        self.assertEqual(
            [
                "ac_attr_timeout=0.0",
                "attr_timeout=0.0",
                "auto_cache",
                "entry_timeout=60.0",
                "negative_timeout=10.0",
                "subtype=sshfs",
            ],
            rendered,
        )
