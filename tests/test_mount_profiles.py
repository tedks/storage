import tempfile
import unittest
from pathlib import Path

import click

from storage.mount_profiles import (
    _load_mount_profile_settings,
    _optional_profile_float,
    _optional_profile_int,
    parse_size,
    resolve_mount_configuration,
    split_backend_specs,
)


class TestParseSize(unittest.TestCase):
    def test_parse_gigabytes(self):
        self.assertEqual(10 * 1024 ** 3, parse_size("10G"))

    def test_parse_megabytes(self):
        self.assertEqual(500 * 1024 ** 2, parse_size("500M"))

    def test_parse_kilobytes(self):
        self.assertEqual(100 * 1024, parse_size("100K"))

    def test_parse_terabytes(self):
        self.assertEqual(1024 ** 4, parse_size("1T"))

    def test_parse_plain_bytes(self):
        self.assertEqual(1024, parse_size("1024"))

    def test_parse_float(self):
        self.assertEqual(int(2.5 * 1024 ** 3), parse_size("2.5G"))

    def test_parse_invalid_raises(self):
        with self.assertRaises(ValueError):
            parse_size("abc")

    def test_parse_empty_raises(self):
        with self.assertRaises(ValueError):
            parse_size("")


class TestSplitBackendSpecs(unittest.TestCase):
    def test_local_paths_separated(self):
        local, remote = split_backend_specs(["/local/path"])
        self.assertEqual(("/local/path",), local)
        self.assertEqual((), remote)

    def test_ssh_urls_separated(self):
        local, remote = split_backend_specs(["ssh://user@host:/path"])
        self.assertEqual((), local)
        self.assertEqual(("ssh://user@host:/path",), remote)

    def test_rsync_urls_separated(self):
        local, remote = split_backend_specs(["rsync://user@host:/path"])
        self.assertEqual((), local)
        self.assertEqual(("rsync://user@host:/path",), remote)

    def test_mixed_specs_split(self):
        specs = ["/local/one", "ssh://user@host:/remote", "/local/two", "rsync://user@host2:/other"]
        local, remote = split_backend_specs(specs)
        self.assertEqual(("/local/one", "/local/two"), local)
        self.assertEqual(("ssh://user@host:/remote", "rsync://user@host2:/other"), remote)


class TestOptionalProfileFloat(unittest.TestCase):
    def test_float_value_returned(self):
        profile = {"ttl": 3.5}
        result = _optional_profile_float(profile, "ttl", "test")
        self.assertEqual(3.5, result)

    def test_int_value_coerced(self):
        profile = {"ttl": 5}
        result = _optional_profile_float(profile, "ttl", "test")
        self.assertIsInstance(result, float)
        self.assertEqual(5.0, result)

    def test_none_returns_none(self):
        profile = {}
        result = _optional_profile_float(profile, "ttl", "test")
        self.assertIsNone(result)

    def test_string_raises(self):
        profile = {"ttl": "fast"}
        with self.assertRaises(click.ClickException):
            _optional_profile_float(profile, "ttl", "test")

    def test_bool_true_raises(self):
        profile = {"ttl": True}
        with self.assertRaises(click.ClickException):
            _optional_profile_float(profile, "ttl", "test")

    def test_bool_false_raises(self):
        profile = {"ttl": False}
        with self.assertRaises(click.ClickException):
            _optional_profile_float(profile, "ttl", "test")


class TestOptionalProfileInt(unittest.TestCase):
    def test_returns_none_when_missing(self):
        self.assertIsNone(_optional_profile_int({}, "workers", "test"))

    def test_returns_int(self):
        self.assertEqual(4, _optional_profile_int({"workers": 4}, "workers", "test"))

    def test_raises_for_float(self):
        with self.assertRaises(click.ClickException):
            _optional_profile_int({"workers": 2.5}, "workers", "test")

    def test_raises_for_string(self):
        with self.assertRaises(click.ClickException):
            _optional_profile_int({"workers": "4"}, "workers", "test")

    def test_bool_true_raises(self):
        with self.assertRaises(click.ClickException):
            _optional_profile_int({"workers": True}, "workers", "test")

    def test_bool_false_raises(self):
        with self.assertRaises(click.ClickException):
            _optional_profile_int({"workers": False}, "workers", "test")


class TestProfileSettingsReaddirTTL(unittest.TestCase):
    def test_readdir_ttl_loaded_from_profile(self):
        toml_content = '''
[mounts.test]
mountpoint = "/mnt/test"
cache = "/tmp/cache"
backends = ["/srv/data"]
readdir_ttl = 5.0
'''
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(toml_content)
            config_path = Path(f.name)

        try:
            settings = _load_mount_profile_settings(config_path, "test")
            self.assertEqual(5.0, settings["readdir_ttl"])
        finally:
            config_path.unlink()

    def test_enable_watchers_loaded_from_profile(self):
        toml_content = '''
[mounts.test]
mountpoint = "/mnt/test"
cache = "/tmp/cache"
backends = ["/srv/data"]
enable_watchers = true
'''
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(toml_content)
            config_path = Path(f.name)

        try:
            settings = _load_mount_profile_settings(config_path, "test")
            self.assertTrue(settings["enable_watchers"])
        finally:
            config_path.unlink()


class TestResolvedMountConfig(unittest.TestCase):
    def _resolve(self, **overrides):
        """Helper to call resolve_mount_configuration with sensible defaults."""
        defaults = dict(
            mount_target="/mnt/test",
            backing_paths=["/srv/data"],
            config_path=None,
            cache="/tmp/cache",
            cache_size=None,
            database=None,
            workers=None,
            remote=[],
            key_file=None,
            foreground=None,
            allow_other=None,
            debug=None,
            streaming_reads=None,
            streaming_chunk_size=None,
            max_streaming_sessions=None,
            readdir_ttl=None,
            enable_watchers=None,
        )
        defaults.update(overrides)
        return resolve_mount_configuration(**defaults)

    def test_readdir_ttl_defaults_to_1(self):
        config = self._resolve()
        self.assertEqual(1.0, config.readdir_ttl)

    def test_enable_watchers_defaults_to_false(self):
        config = self._resolve()
        self.assertFalse(config.enable_watchers)

    def test_readdir_ttl_from_cli_overrides_profile(self):
        toml_content = '''
[mounts.prof]
mountpoint = "/mnt/test"
cache = "/tmp/cache"
backends = ["/srv/data"]
readdir_ttl = 5.0
'''
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(toml_content)
            config_path = Path(f.name)

        try:
            config = self._resolve(
                mount_target="prof",
                backing_paths=[],
                config_path=Path(config_path),
                readdir_ttl=2.0,
            )
            self.assertEqual(2.0, config.readdir_ttl)
        finally:
            Path(config_path).unlink()


if __name__ == "__main__":
    unittest.main()
