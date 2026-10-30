"""ControlSockets: socket directory, options for clients, ending masters."""

import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from storage.backends import ssh_mux
from storage.backends.rsync import RsyncBackend, RsyncError
from storage.backends.ssh_mux import ControlSockets
from storage.watcher import RemoteWatcher


class _FakeSSH:
    """An executable that records its arguments and, like a master told to
    exit, removes nothing itself (ControlSockets unlinks the socket)."""

    def __init__(self, directory: Path, exit_code: int = 0):
        self.log = directory / "invocations.log"
        self.path = directory / "fake-ssh"
        self.path.write_text(
            "#!/bin/sh\n"
            f"printf '%s\\n' \"$*\" >> '{self.log}'\n"
            f"exit {exit_code}\n")
        self.path.chmod(0o755)

    def calls(self):
        return self.log.read_text().splitlines() if self.log.exists() else []


class TestControlSockets(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.parent = self.tmp / "run"
        self.parent.mkdir()
        self.fake = _FakeSSH(self.tmp)

    def _sockets(self, **kwargs):
        sockets = ControlSockets(parent_dir=self.parent, ssh=str(self.fake.path), **kwargs)
        self.addCleanup(sockets.close)
        return sockets

    @staticmethod
    def _control_path(options):
        value = next(o for o in options if o.startswith("ControlPath="))
        return Path(value.split("=", 1)[1])

    def test_options_name_a_socket_in_a_private_directory(self):
        sockets = self._sockets()
        options = sockets.options("user", "host.example", 22, "/k")
        self.assertIn("ControlMaster=auto", options)
        self.assertIn("ControlPersist=60", options)
        path = self._control_path(options)
        self.assertEqual(path.parent.parent, self.parent)
        self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o700)
        self.assertLessEqual(len(str(path)) + 17, 107)

    def test_one_identity_one_socket_and_distinct_identities_distinct_sockets(self):
        sockets = self._sockets()
        same = [self._control_path(sockets.options("u", "h", 22, "/k")) for _ in range(2)]
        self.assertEqual(same[0], same[1])
        others = {
            self._control_path(sockets.options("u2", "h", 22, "/k")),
            self._control_path(sockets.options("u", "h2", 22, "/k")),
            self._control_path(sockets.options("u", "h", 2222, "/k")),
            self._control_path(sockets.options("u", "h", 22, "/other-key")),
        }
        self.assertNotIn(same[0], others)
        self.assertEqual(len(others), 4)

    def test_close_ends_each_master_and_removes_the_directory(self):
        sockets = self._sockets()
        path = self._control_path(sockets.options("u", "h", 2222, None))
        path.write_text("")  # a socket stand-in
        sockets.close()
        calls = self.fake.calls()
        self.assertEqual(len(calls), 1)
        self.assertIn(f"ControlPath={path}", calls[0])
        self.assertIn("-O exit", calls[0])
        self.assertIn("-p 2222", calls[0])
        self.assertIn("u@h", calls[0])
        self.assertFalse(path.parent.exists())
        self.assertEqual(list(self.parent.iterdir()), [])

    def test_reset_ends_only_that_identitys_master_and_unlinks_its_socket(self):
        sockets = self._sockets()
        mine = self._control_path(sockets.options("u", "h", 22, None))
        other = self._control_path(sockets.options("u", "other", 22, None))
        mine.write_text("")
        other.write_text("")
        sockets.reset("u", "h", 22, None)
        self.assertFalse(mine.exists())
        self.assertTrue(other.exists())
        self.assertEqual(len(self.fake.calls()), 1)

    def test_a_master_that_does_not_answer_still_loses_its_socket(self):
        sockets = ControlSockets(parent_dir=self.parent, ssh="/nonexistent/ssh")
        self.addCleanup(sockets.close)
        path = self._control_path(sockets.options("u", "h", 22, None))
        path.write_text("")
        sockets.reset("u", "h", 22, None)
        self.assertFalse(path.exists())

    def test_a_path_too_long_for_a_unix_socket_turns_sharing_off(self):
        deep = self.parent
        for _ in range(12):
            deep = deep / "a-rather-long-directory-name"
            deep.mkdir()
        sockets = ControlSockets(parent_dir=deep, ssh=str(self.fake.path))
        self.addCleanup(sockets.close)
        self.assertEqual(sockets.options("u", "h", 22, None), [])
        self.assertEqual(list(deep.iterdir()), [])  # the directory it made is gone

    def test_close_then_options_makes_a_fresh_directory(self):
        sockets = self._sockets()
        first = self._control_path(sockets.options("u", "h", 22, None)).parent
        sockets.close()
        second = self._control_path(sockets.options("u", "h", 22, None)).parent
        self.assertNotEqual(first, second)
        self.assertTrue(second.is_dir())

    def test_the_environment_switch_turns_the_shared_sockets_off(self):
        with patch.dict(os.environ, {"STORAGE_SSH_MUX": "0"}):
            self.assertIsNone(ssh_mux.shared_control_sockets())


class TestClientsUseTheSockets(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.sockets = ControlSockets(parent_dir=Path(self._tmp.name),
                                      ssh="/nonexistent/ssh")
        self.addCleanup(self.sockets.close)

    def _rsync(self, **kwargs):
        backend = RsyncBackend(name="r", host="h.example", remote_path="/srv",
                               username="u", key_filename="/k", port=2222, **kwargs)
        self.addCleanup(backend.close)
        return backend

    def test_rsync_and_the_watcher_name_the_same_socket(self):
        backend = self._rsync(control_sockets=self.sockets)
        watcher = RemoteWatcher("w", "h.example", 2222, remote_path="/srv",
                                username="u", key_filename="/k",
                                control_sockets=self.sockets)
        rsync_cmd = backend._build_rsync_ssh_cmd()
        watcher_cmd = watcher._build_ssh_command()
        socket = self.sockets.options("u", "h.example", 2222, "/k")[3]
        self.assertIn(socket, rsync_cmd)
        self.assertIn(socket, watcher_cmd)
        self.assertIn("ControlMaster=auto", watcher_cmd)
        self.assertIn("ServerAliveInterval=15", rsync_cmd)

    def test_without_sockets_the_commands_are_unchanged(self):
        backend = self._rsync()
        watcher = RemoteWatcher("w", "h.example", 2222, remote_path="/srv",
                                username="u", key_filename="/k")
        self.assertNotIn("Control", backend._build_rsync_ssh_cmd())
        self.assertNotIn("Control", " ".join(watcher._build_ssh_command()))

    def test_a_connection_failure_resets_the_master_and_a_file_error_does_not(self):
        backend = self._rsync(control_sockets=self.sockets)
        resets = []
        backend._control_sockets = type("Spy", (), {
            "options": staticmethod(lambda *a: []),
            "reset": staticmethod(lambda *a: resets.append(a)),
        })()
        for code, expect_reset in ((255, True), (12, True), (23, False)):
            resets.clear()
            completed = type("R", (), {"returncode": code, "stderr": "boom"})()
            with patch("storage.backends.rsync.subprocess.run", return_value=completed):
                with self.assertRaises(RsyncError) as caught:
                    backend._run_rsync("/a", "u@h:/b")
            self.assertEqual(caught.exception.returncode, code)
            self.assertEqual(bool(resets), expect_reset, code)


if __name__ == "__main__":
    unittest.main()
