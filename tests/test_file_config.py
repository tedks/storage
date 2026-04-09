import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from storage.file_config import FileConfig, get_file_config, get_inherited_config, set_file_config


class TestFileConfigSerialization(unittest.TestCase):
    def test_yaml_round_trip_preserves_fields(self):
        config = FileConfig(
            replica_count=3,
            preferred_backends=["fast", "cold"],
            encrypted=True,
            encryption_backends=["cold"],
            recomputable=True,
            recompute_cmd="make output",
            custom={"tier": "archive"},
        )

        restored = FileConfig.from_yaml(config.to_yaml())

        self.assertEqual(restored, config)


class TestFileConfigXattrs(unittest.TestCase):
    def test_get_and_set_file_config_round_trip_through_xattrs(self):
        path = Path("/tmp/example")
        store = {}

        def fake_setxattr(raw_path, name, value):
            store[(raw_path, name)] = value

        def fake_getxattr(raw_path, name):
            return store[(raw_path, name)]

        config = FileConfig(replica_count=2, preferred_backends=["member0"])

        with patch("storage.file_config.os.setxattr", side_effect=fake_setxattr), patch(
            "storage.file_config.os.getxattr",
            side_effect=fake_getxattr,
        ):
            set_file_config(path, config)
            restored = get_file_config(path)

        self.assertEqual(restored, config)

    def test_get_inherited_config_merges_parent_chain(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            nested_dir = root / "media" / "movies"
            file_path = nested_dir / "film.mkv"
            nested_dir.mkdir(parents=True)
            file_path.touch()

            config_map = {
                root: FileConfig(replica_count=2, custom={"retention": "warm"}),
                root / "media": FileConfig(
                    replica_count=3,
                    preferred_backends=["member1"],
                    encrypted=True,
                    encryption_backends=["member2"],
                ),
                nested_dir: FileConfig(
                    recomputable=True,
                    recompute_cmd="rebuild-movie",
                    custom={"codec": "h264"},
                ),
            }

            with patch(
                "storage.file_config.get_file_config",
                side_effect=lambda path: config_map.get(path),
            ):
                resolved = get_inherited_config(file_path)

        self.assertEqual(resolved.replica_count, 3)
        self.assertEqual(resolved.preferred_backends, ["member1"])
        self.assertTrue(resolved.encrypted)
        self.assertEqual(resolved.encryption_backends, ["member2"])
        self.assertTrue(resolved.recomputable)
        self.assertEqual(resolved.recompute_cmd, "rebuild-movie")
        self.assertEqual(
            resolved.custom,
            {"retention": "warm", "codec": "h264"},
        )

    def test_all_members_replica_setting_survives_inheritance(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            file_path = root / "artifact.bin"
            file_path.touch()

            config_map = {
                root: FileConfig(replica_count=-1),
                file_path: FileConfig(replica_count=2),
            }

            with patch(
                "storage.file_config.get_file_config",
                side_effect=lambda path: config_map.get(path),
            ):
                resolved = get_inherited_config(file_path)

        self.assertEqual(resolved.replica_count, -1)


if __name__ == "__main__":
    unittest.main()
