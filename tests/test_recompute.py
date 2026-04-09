"""
Tests for recomputation engine

Tests cover:
- Unit tests: RecomputeEngine, command execution
- Integration tests: Recomputation with filesystem
- System tests: Auto-recompute on file access
"""

import unittest
import tempfile
import time
import os
from pathlib import Path

from storage.state import StateManager
from storage.recompute import RecomputeEngine, RecomputeResult, mark_recomputable, unmark_recomputable


class TestRecomputeEngine(unittest.TestCase):
    """Unit tests for RecomputeEngine"""

    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.db_path = Path(self.temp_dir) / "state.db"

        self.cache_dir.mkdir()

        self.state_manager = StateManager(str(self.db_path))
        self.recompute_engine = RecomputeEngine(self.state_manager, default_timeout=5.0)

    def tearDown(self):
        """Clean up"""
        self.state_manager.close()
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_can_recompute_not_marked(self):
        """Test can_recompute returns False for unmarked files"""
        test_file = self.cache_dir / "test.txt"
        result = self.recompute_engine.can_recompute("/test.txt", test_file)
        self.assertFalse(result)

    def test_can_recompute_marked(self):
        """Test can_recompute returns True for marked files"""
        test_file = self.cache_dir / "test.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.touch()

        # Mark as recomputable
        mark_recomputable(test_file, "echo 'test'", self.state_manager)

        result = self.recompute_engine.can_recompute("/test.txt", test_file)
        self.assertTrue(result)

    def test_simple_recompute(self):
        """Test recomputing a simple file"""
        test_file = self.cache_dir / "test.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)

        # Mark parent directory as recomputable so config is inherited
        cmd = f"echo 'Hello World' > $STORAGE_OUTPUT"
        mark_recomputable(test_file.parent, cmd, self.state_manager)

        # Recompute
        result = self.recompute_engine.recompute_file("/test.txt", test_file, timeout=5.0)

        self.assertTrue(result.success)
        self.assertFalse(result.timed_out)
        self.assertTrue(test_file.exists())
        self.assertIn("Hello World", test_file.read_text())

    def test_recompute_with_environment_vars(self):
        """Test that environment variables are passed correctly"""
        test_file = self.cache_dir / "envtest.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)

        # Command that uses environment variables
        cmd = "echo $STORAGE_PATH > $STORAGE_OUTPUT"
        mark_recomputable(test_file.parent, cmd, self.state_manager)

        result = self.recompute_engine.recompute_file("/envtest.txt", test_file)

        self.assertTrue(result.success)
        self.assertTrue(test_file.exists())
        content = test_file.read_text().strip()
        self.assertEqual(content, "/envtest.txt")

    def test_recompute_timeout(self):
        """Test that long-running commands timeout"""
        test_file = self.cache_dir / "timeout.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)

        # Command that sleeps longer than timeout
        cmd = "sleep 10 && echo 'done' > $STORAGE_OUTPUT"
        mark_recomputable(test_file.parent, cmd, self.state_manager)

        result = self.recompute_engine.recompute_file("/timeout.txt", test_file, timeout=1.0)

        self.assertFalse(result.success)
        self.assertTrue(result.timed_out)
        self.assertIn("Timeout", result.error)

    def test_recompute_command_failure(self):
        """Test handling of failed recompute commands"""
        test_file = self.cache_dir / "fail.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)

        # Command that fails
        cmd = "exit 1"
        mark_recomputable(test_file.parent, cmd, self.state_manager)

        result = self.recompute_engine.recompute_file("/fail.txt", test_file)

        self.assertFalse(result.success)
        self.assertFalse(result.timed_out)

    def test_recompute_no_output_file(self):
        """Test handling when command succeeds but doesn't create file"""
        test_file = self.cache_dir / "nofile.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)

        # Command succeeds but doesn't create file
        cmd = "echo 'not creating file'"
        mark_recomputable(test_file.parent, cmd, self.state_manager)

        result = self.recompute_engine.recompute_file("/nofile.txt", test_file)

        self.assertFalse(result.success)
        self.assertIn("not created", result.error)

    def test_recompute_async(self):
        """Test asynchronous recomputation"""
        test_file = self.cache_dir / "async.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)

        cmd = "sleep 0.5 && echo 'async result' > $STORAGE_OUTPUT"
        mark_recomputable(test_file.parent, cmd, self.state_manager)

        # Start async recompute
        thread = self.recompute_engine.recompute_file_async("/async.txt", test_file)

        # Should be running
        self.assertTrue(self.recompute_engine.is_recomputing("/async.txt"))

        # Wait for completion
        success = self.recompute_engine.wait_for_recompute("/async.txt", timeout=2.0)

        self.assertTrue(success)
        self.assertTrue(test_file.exists())
        self.assertIn("async result", test_file.read_text())
        self.assertFalse(self.recompute_engine.is_recomputing("/async.txt"))

    def test_multiple_async_recomputes(self):
        """Test multiple concurrent recomputes"""
        files = []

        # Mark cache dir as recomputable with a generic command
        mark_recomputable(self.cache_dir, "echo 'placeholder' > $STORAGE_OUTPUT", self.state_manager)

        for i in range(3):
            test_file = self.cache_dir / f"multi{i}.txt"

            # Override with specific command for this file
            subdir = self.cache_dir / f"multi{i}"
            subdir.mkdir(parents=True, exist_ok=True)
            test_file = subdir / "file.txt"

            cmd = f"sleep 0.2 && echo 'file{i}' > $STORAGE_OUTPUT"
            mark_recomputable(subdir, cmd, self.state_manager)

            self.recompute_engine.recompute_file_async(f"/multi{i}/file.txt", test_file)
            files.append(test_file)

        # All should be recomputing
        active = self.recompute_engine.get_active_recomputes()
        self.assertEqual(len(active), 3)

        # Wait for all to complete
        time.sleep(1.0)

        # All files should exist
        for test_file in files:
            self.assertTrue(test_file.exists())

    def test_mark_unmark_recomputable(self):
        """Test marking and unmarking files as recomputable"""
        test_file = self.cache_dir / "mark.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.touch()

        # Initially not recomputable
        self.assertFalse(self.recompute_engine.can_recompute("/mark.txt", test_file))

        # Mark as recomputable
        mark_recomputable(test_file, "echo 'test'", self.state_manager)
        self.assertTrue(self.recompute_engine.can_recompute("/mark.txt", test_file))

        # Unmark
        unmark_recomputable(test_file, self.state_manager)
        self.assertFalse(self.recompute_engine.can_recompute("/mark.txt", test_file))

    def test_recompute_with_callback(self):
        """Test recomputation with progress callback"""
        test_file = self.cache_dir / "callback.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)

        cmd = "echo 'callback test' > $STORAGE_OUTPUT"
        mark_recomputable(test_file.parent, cmd, self.state_manager)

        messages = []

        def callback(msg):
            messages.append(msg)

        result = self.recompute_engine.recompute_file("/callback.txt", test_file, callback=callback)

        self.assertTrue(result.success)
        self.assertGreater(len(messages), 0)
        self.assertTrue(any("Starting" in msg for msg in messages))
        self.assertTrue(any("successful" in msg for msg in messages))


class TestRecomputeIntegration(unittest.TestCase):
    """Integration tests for recomputation with cache and state"""

    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir) / "cache"
        self.db_path = Path(self.temp_dir) / "state.db"

        self.cache_dir.mkdir()

        self.state_manager = StateManager(str(self.db_path))
        self.recompute_engine = RecomputeEngine(self.state_manager)

    def tearDown(self):
        """Clean up"""
        self.state_manager.close()
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_recompute_downloads_file(self):
        """Test recomputing a 'downloaded' file using wget simulation"""
        test_file = self.cache_dir / "download.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)

        # Create a source file to 'download'
        source_file = Path(self.temp_dir) / "source.txt"
        source_file.write_text("Downloaded content")

        # Simulate wget with cp command
        cmd = f"cp {source_file} $STORAGE_OUTPUT"
        mark_recomputable(test_file.parent, cmd, self.state_manager)

        result = self.recompute_engine.recompute_file("/download.txt", test_file)

        self.assertTrue(result.success)
        self.assertEqual(test_file.read_text(), "Downloaded content")

    def test_recompute_generates_file(self):
        """Test recomputing a generated file"""
        test_file = self.cache_dir / "generated.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)

        # Generate file with date command
        cmd = "date > $STORAGE_OUTPUT"
        mark_recomputable(test_file.parent, cmd, self.state_manager)

        result = self.recompute_engine.recompute_file("/generated.txt", test_file)

        self.assertTrue(result.success)
        self.assertTrue(test_file.exists())
        self.assertGreater(len(test_file.read_text()), 0)

    def test_recompute_script_execution(self):
        """Test recomputing by executing a script"""
        test_file = self.cache_dir / "script_output.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)

        # Create a script
        script = Path(self.temp_dir) / "generate.sh"
        script.write_text("""#!/bin/bash
echo "Generated at $(date)" > $STORAGE_OUTPUT
echo "Path: $STORAGE_PATH" >> $STORAGE_OUTPUT
""")
        script.chmod(0o755)

        cmd = f"bash {script}"
        mark_recomputable(test_file.parent, cmd, self.state_manager)

        result = self.recompute_engine.recompute_file("/script_output.txt", test_file)

        self.assertTrue(result.success)
        content = test_file.read_text()
        self.assertIn("Generated at", content)
        self.assertIn("Path: /script_output.txt", content)


if __name__ == '__main__':
    unittest.main()
