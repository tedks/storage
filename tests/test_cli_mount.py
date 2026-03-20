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
        config_path = self.temp_dir / "config.toml"
        config_path.write_text(
            """
[mounts.drynwyn-warez]
mountpoint = "/tmp/drynwyn-warez"
cache = "/tmp/drynwyn-cache"
cache_size = "50G"
key_file = "/home/tedks/.ssh/id_ed25519"
foreground = true
streaming_reads = true
streaming_chunk_size = "2M"
max_streaming_sessions = 16
backends = [
  "ssh://tedks@drynwyn.local:/srv/raid/warez/",
  "ssh://tedks@drynwyn.local:/srv/chunk0/warez/",
  "ssh://tedks@drynwyn.local:/srv/chunk1/warez/",
]
""".strip()
        )

        with patch("storage.cli.mount_storage") as mock_mount:
            result = self.runner.invoke(
                main,
                ["mount", "--config", str(config_path), "drynwyn-warez"],
            )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertIn("Loaded mount profile: drynwyn-warez", result.output)
        mock_mount.assert_called_once()

        args = mock_mount.call_args.args
        kwargs = mock_mount.call_args.kwargs
        self.assertEqual("/tmp/drynwyn-warez", args[0])
        self.assertEqual(Path("/tmp/drynwyn-cache"), args[1])
        self.assertEqual(Path("/tmp/drynwyn-cache/.storage.db"), args[2])
        self.assertEqual(50 * 1024 ** 3, args[4])
        self.assertEqual(["remote0", "remote1", "remote2"], sorted(args[3].keys()))
        self.assertEqual("drynwyn.local", args[3]["remote0"].host)
        self.assertEqual("/srv/raid/warez", str(args[3]["remote0"].remote_path))
        self.assertTrue(kwargs["foreground"])
        self.assertTrue(kwargs["streaming_reads"])
        self.assertEqual(2 * 1024 ** 2, kwargs["streaming_chunk_size"])
        self.assertEqual(16, kwargs["max_streaming_sessions"])

    def test_mount_cli_flags_override_profile_values(self):
        local_backend = self.temp_dir / "local-backend"
        local_backend.mkdir()
        cache_dir = self.temp_dir / "cache"
        mount_dir = self.temp_dir / "mount"
        config_path = self.temp_dir / "config.toml"
        config_path.write_text(
            f"""
[mounts.override-me]
mountpoint = "{mount_dir}"
cache = "{cache_dir}"
cache_size = "50G"
workers = 2
foreground = false
backends = [
  "ssh://tedks@drynwyn.local:/srv/raid/warez/",
]
""".strip()
        )

        with patch("storage.cli.mount_storage") as mock_mount:
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
                    "ssh://tedks@mirror.local:/srv/cache/warez/",
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
        self.assertEqual("mirror.local", args[3]["remote0"].host)
        self.assertEqual("/srv/cache/warez", str(args[3]["remote0"].remote_path))
        self.assertTrue(kwargs["foreground"])
        self.assertEqual(4, kwargs["num_sync_workers"])

    def test_mount_fails_when_named_profile_is_missing(self):
        config_path = self.temp_dir / "config.toml"
        config_path.write_text(
            """
[mounts.other]
mountpoint = "/tmp/other"
cache = "/tmp/other-cache"
backends = ["ssh://tedks@drynwyn.local:/srv/other/"]
""".strip()
        )

        result = self.runner.invoke(
            main,
            ["mount", "--config", str(config_path), "missing-profile"],
        )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Mount profile 'missing-profile' not found", result.output)
