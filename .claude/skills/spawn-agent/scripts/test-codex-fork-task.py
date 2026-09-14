#!/usr/bin/env python3
"""Small failure-boundary checks; no live Codex process or model calls."""

from contextlib import redirect_stderr
import io
from pathlib import Path
import runpy
import subprocess
import unittest
from unittest.mock import patch


HELPER = Path(__file__).with_name("codex-fork-task.py")


class ForkTaskFailures(unittest.TestCase):
    def test_timeout_does_not_echo_task_from_process_arguments(self):
        output = io.StringIO()
        with patch("sys.argv", [str(HELPER), "preflight", "codex", "unused"]), \
                patch("pathlib.Path.read_bytes", return_value=b"short task"), \
                patch("subprocess.run", side_effect=subprocess.TimeoutExpired(
                    ["codex", "queue", "--message=PRIVATE TASK MUST NOT BE LOGGED"], 15)), \
                redirect_stderr(output), self.assertRaises(SystemExit) as stopped:
            runpy.run_path(str(HELPER), run_name="__main__")
        self.assertEqual(stopped.exception.code, 1)
        self.assertIn("timed out", output.getvalue())
        self.assertNotIn("PRIVATE TASK", output.getvalue())


if __name__ == "__main__":
    unittest.main()
