import json
import shutil
import tempfile
import time
import unittest
from pathlib import Path

from click.testing import CliRunner

from storage.backends.local import LocalBackend
from storage.cli import main
from storage.fs import StorageFS


class TestDebugStreamingStatsCLI(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.runner = CliRunner()
        self.fs = None

    def tearDown(self):
        if self.fs is not None:
            self.fs.destroy("/")
            self.fs = None
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _wait_for_socket(self, socket_path: Path, timeout: float = 2.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if socket_path.exists():
                return
            time.sleep(0.02)
        self.fail(f"Control socket was not created at {socket_path}")

    def test_streaming_stats_fails_without_running_mount(self):
        cache_dir = self.temp_dir / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)

        result = self.runner.invoke(
            main,
            ["debug", "streaming-stats", "--cache", str(cache_dir)],
        )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("No running mount control socket", result.output)

    def test_streaming_stats_returns_json_from_running_mount(self):
        cache_dir = self.temp_dir / "cache"
        backend_dir = self.temp_dir / "backend"
        cache_dir.mkdir(parents=True, exist_ok=True)
        backend_dir.mkdir(parents=True, exist_ok=True)

        self.fs = StorageFS(
            cache_root=cache_dir,
            state_db=self.temp_dir / "state.db",
            backends={"local0": LocalBackend("local0", str(backend_dir))},
            cache_size=8 * 1024 * 1024,
            num_sync_workers=0,
            start_sync=False,
            streaming_reads=True,
        )
        socket_path = cache_dir / ".storage.control.sock"
        self._wait_for_socket(socket_path)

        result = self.runner.invoke(
            main,
            ["debug", "streaming-stats", "--cache", str(cache_dir), "--json-output"],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        payload = json.loads(result.output)
        self.assertEqual(payload["pid"], self.fs._streaming_stats_snapshot()["pid"])
        self.assertTrue(payload["streaming_enabled"])
        self.assertEqual(payload["active_streaming_sessions"], 0)
        self.assertEqual(payload["streaming_sessions_started"], 0)
        self.assertEqual(payload["streaming_fallback_opens"], 0)
        self.assertEqual(payload["foreground_fetches"], 0)
        self.assertEqual(payload["background_fetches"], 0)
