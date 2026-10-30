import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from storage.cli import main


class TestMountConfigCLI(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.runner = CliRunner()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_mount_uses_named_profile_from_toml_config(self):
        config_path = self.temp_dir / "storage.toml"
        config_path.write_text(
            """
[mounts.example-media]
mountpoint = "/tmp/example-media"
cache = "/tmp/example-cache"
cache_size = "50G"
key_file = "/home/user/.ssh/id_ed25519"
foreground = true
streaming_reads = true
streaming_chunk_size = "2M"
max_streaming_sessions = 16
backends = [
  "ssh://user@storage-host.example:/srv/raid/media/",
  "ssh://user@storage-host.example:/srv/chunk0/media/",
  "ssh://user@storage-host.example:/srv/chunk1/media/",
]
""".strip()
        )

        with patch("storage.cli_mount.mount_storage") as mock_mount:
            result = self.runner.invoke(
                main,
                ["mount", "--config", str(config_path), "example-media"],
            )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("Loaded mount profile: example-media", result.output)
        mock_mount.assert_called_once()

        args = mock_mount.call_args.args
        kwargs = mock_mount.call_args.kwargs
        self.assertEqual("/tmp/example-media", args[0])
        self.assertEqual(Path("/tmp/example-cache"), args[1])
        self.assertEqual(Path("/tmp/example-cache/.storage.db"), args[2])
        self.assertEqual(50 * 1024 ** 3, args[4])
        self.assertEqual(["remote0", "remote1", "remote2"], sorted(args[3].keys()))
        self.assertEqual("storage-host.example", args[3]["remote0"].host)
        self.assertEqual("/srv/raid/media", str(args[3]["remote0"].remote_path))
        self.assertTrue(kwargs["foreground"])
        self.assertTrue(kwargs["streaming_reads"])
        self.assertEqual(2 * 1024 ** 2, kwargs["streaming_chunk_size"])
        self.assertEqual(16, kwargs["max_streaming_sessions"])

    def test_mount_auto_loads_default_storage_toml(self):
        home_dir = self.temp_dir / "home"
        config_dir = home_dir / ".config" / "storage"
        config_dir.mkdir(parents=True)
        config_path = config_dir / "storage.toml"
        config_path.write_text(
            """
[mounts.auto-profile]
mountpoint = "/tmp/auto-profile"
cache = "/tmp/auto-cache"
backends = [
  "ssh://user@storage-host.example:/srv/raid/media/",
]
""".strip()
        )

        with patch("storage.cli_mount.mount_storage") as mock_mount:
            result = self.runner.invoke(
                main,
                ["mount", "auto-profile"],
                env={"HOME": str(home_dir)},
            )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn(f"Loaded mount profile: auto-profile ({config_path})", result.output)
        mock_mount.assert_called_once()
        self.assertEqual("/tmp/auto-profile", mock_mount.call_args.args[0])
        self.assertEqual(Path("/tmp/auto-cache"), mock_mount.call_args.args[1])

    def test_mount_cli_flags_override_profile_values(self):
        local_backend = self.temp_dir / "local-backend"
        local_backend.mkdir()
        cache_dir = self.temp_dir / "cache"
        mount_dir = self.temp_dir / "mount"
        config_path = self.temp_dir / "storage.toml"
        config_path.write_text(
            f"""
[mounts.override-me]
mountpoint = "{mount_dir}"
cache = "{cache_dir}"
cache_size = "50G"
workers = 2
foreground = false
backends = [
  "ssh://user@storage-host.example:/srv/raid/media/",
]
""".strip()
        )

        with patch("storage.cli_mount.mount_storage") as mock_mount:
            result = self.runner.invoke(
                main,
                [
                    "mount",
                    "--config",
                    str(config_path),
                    "--cache-size",
                    "5G",
                    "--workers",
                    "4",
                    "--foreground",
                    "--remote",
                    "ssh://user@mirror.example:/srv/cache/media/",
                    "override-me",
                    str(local_backend),
                ],
            )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        mock_mount.assert_called_once()

        args = mock_mount.call_args.args
        kwargs = mock_mount.call_args.kwargs
        self.assertEqual(str(mount_dir), args[0])
        self.assertEqual(cache_dir.resolve(), args[1])
        self.assertEqual(5 * 1024 ** 3, args[4])
        self.assertEqual(["local0", "remote0"], sorted(args[3].keys()))
        self.assertEqual(local_backend.resolve(), args[3]["local0"].root_path)
        self.assertEqual("mirror.example", args[3]["remote0"].host)
        self.assertEqual("/srv/cache/media", str(args[3]["remote0"].remote_path))
        self.assertTrue(kwargs["foreground"])
        self.assertEqual(4, kwargs["num_sync_workers"])

    def test_mount_fails_when_named_profile_is_missing(self):
        config_path = self.temp_dir / "storage.toml"
        config_path.write_text(
            """
[mounts.other]
mountpoint = "/tmp/other"
cache = "/tmp/other-cache"
backends = ["ssh://user@storage-host.example:/srv/other/"]
""".strip()
        )

        result = self.runner.invoke(
            main,
            ["mount", "--config", str(config_path), "missing-profile"],
        )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Mount profile 'missing-profile' not found", result.output)


class TestMountStreamingKeepAfterClose(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.runner = CliRunner()
        self.config_path = self.temp_dir / "storage.toml"

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _mount(self, profile_line="", extra_args=()):
        self.config_path.write_text(
            f"""
[mounts.keep]
mountpoint = "{self.temp_dir / 'mnt'}"
cache = "{self.temp_dir / 'cache'}"
{profile_line}
backends = [
  "ssh://user@storage-host.example:/srv/raid/media/",
]
""".strip()
        )
        with patch("storage.cli_mount.mount_storage") as mock_mount:
            result = self.runner.invoke(
                main,
                ["mount", "--config", str(self.config_path), *extra_args, "keep"],
            )
        return result, mock_mount

    def test_unset_passes_none_for_the_default(self):
        result, mock_mount = self._mount()
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIsNone(mock_mount.call_args.kwargs["streaming_keep_after_close"])
        self.assertIn("Streaming keep after close: default (4 chunks or 50%)", result.output)

    def test_profile_value_is_parsed(self):
        result, mock_mount = self._mount('streaming_keep_after_close = "8M,25%"')
        self.assertEqual(result.exit_code, 0, msg=result.output)
        spec = mock_mount.call_args.kwargs["streaming_keep_after_close"]
        self.assertEqual((8 * 1024 ** 2,), spec.byte_thresholds)
        self.assertEqual(2 * 1024 ** 2, spec.threshold(8 * 1024 ** 2))
        self.assertIn("Streaming keep after close: 8M,25%", result.output)

    def test_flag_overrides_profile(self):
        result, mock_mount = self._mount(
            'streaming_keep_after_close = "8M"',
            ["--streaming-keep-after-close", "0"],
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)
        spec = mock_mount.call_args.kwargs["streaming_keep_after_close"]
        self.assertEqual(0, spec.threshold(10 * 1024 ** 3))

    def test_bad_values_rejected_before_mounting(self):
        for profile_line, args in (
            ('streaming_keep_after_close = "150%"', ()),
            ('streaming_keep_after_close = "lots"', ()),
            ('streaming_keep_after_close = 0', ()),
            ("", ["--streaming-keep-after-close", ""]),
            ("", ["--streaming-keep-after-close", "4M,"]),
        ):
            with self.subTest(profile_line=profile_line, args=args):
                result, mock_mount = self._mount(profile_line, args)
                self.assertNotEqual(result.exit_code, 0, msg=result.output)
                self.assertRegex(result.output, "keep.after.close")
                mock_mount.assert_not_called()


class TestMountKernelCacheCLI(unittest.TestCase):
    """The kernel cache timeouts come from the profile and are echoed at mount."""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.temp_dir, True)
        self.runner = CliRunner()
        self.config_path = self.temp_dir / "storage.toml"

    def _mount(self, profile_lines="", extra_args=()):
        self.config_path.write_text(
            f"""
[mounts.kc]
mountpoint = "{self.temp_dir / 'mnt'}"
cache = "{self.temp_dir / 'cache'}"
{profile_lines}
backends = [
  "ssh://user@storage-host.example:/srv/raid/media/",
]
""".strip()
        )
        with patch("storage.cli_mount.mount_storage") as mock_mount:
            result = self.runner.invoke(
                main, ["mount", "--config", str(self.config_path), *extra_args, "kc"],
            )
        return result, mock_mount

    def test_defaults_follow_revalidate_after(self):
        result, mock_mount = self._mount(extra_args=("--revalidate-after", "90"))
        self.assertEqual(result.exit_code, 0, msg=result.output)
        kwargs = mock_mount.call_args.kwargs
        for key in ("entry_timeout", "negative_timeout"):
            self.assertIsNone(kwargs[key])
        self.assertNotIn("attr_timeout", kwargs)
        self.assertIn(
            "Kernel cache: names 90.0s, missing names 10.0s, attributes 0.0s",
            result.output,
        )

    def test_watchers_echo_one_second(self):
        result, _ = self._mount(extra_args=("--enable-watchers",))
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("Kernel cache: names 1.0s, missing names 10.0s, attributes 0.0s",
                      result.output)

    def test_profile_overrides_reach_mount_and_echo(self):
        result, mock_mount = self._mount(
            "entry_timeout = 30\nnegative_timeout = 0"
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)
        kwargs = mock_mount.call_args.kwargs
        self.assertEqual(30.0, kwargs["entry_timeout"])
        self.assertEqual(0.0, kwargs["negative_timeout"])
        self.assertIn("Kernel cache: names 30.0s, missing names 0.0s, attributes 0.0s",
                      result.output)

    def test_bad_timeout_rejected_before_mounting(self):
        result, mock_mount = self._mount("negative_timeout = -1")
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("negative_timeout", result.output)
        mock_mount.assert_not_called()


class TestMountFuseThreads(unittest.TestCase):
    """fuse_threads: profile key and --fuse-threads/--no-fuse-threads, default off."""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.runner = CliRunner()
        self.config_path = self.temp_dir / "storage.toml"

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _mount(self, profile_line="", extra_args=()):
        self.config_path.write_text(
            f"""
[mounts.threads]
mountpoint = "{self.temp_dir / 'mnt'}"
cache = "{self.temp_dir / 'cache'}"
{profile_line}
backends = ["{self.temp_dir}"]
""".strip()
        )
        with patch("storage.cli_mount.mount_storage") as mock_mount:
            result = self.runner.invoke(
                main,
                ["mount", "--config", str(self.config_path), *extra_args, "threads"],
            )
        self.assertEqual(result.exit_code, 0, msg=result.output)
        return result, mock_mount.call_args.kwargs["fuse_threads"]

    def test_off_by_default(self):
        result, fuse_threads = self._mount()
        self.assertIs(False, fuse_threads)
        self.assertIn("FUSE loop: single-threaded", result.output)

    def test_profile_turns_it_on(self):
        result, fuse_threads = self._mount("fuse_threads = true")
        self.assertIs(True, fuse_threads)
        self.assertIn("FUSE loop: multithreaded", result.output)

    def test_flags_override_the_profile(self):
        self.assertIs(False, self._mount("fuse_threads = true", ["--no-fuse-threads"])[1])
        self.assertIs(True, self._mount("", ["--fuse-threads"])[1])
