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
