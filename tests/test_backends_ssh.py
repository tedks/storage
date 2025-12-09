import unittest
from unittest.mock import patch

import paramiko

from storage.backends.ssh import SSHBackend


class TestSSHBackend(unittest.TestCase):
    def test_connect_fail_fast_on_auth_error(self):
        backend = SSHBackend(
            name="ssh1",
            host="example.invalid",
            remote_path="/",
            username="user",
            password=None,
            key_filename=None,
            connect_timeout=0.2,
        )

        with patch.object(paramiko.SSHClient, "connect", side_effect=paramiko.AuthenticationException("no auth")):
            with self.assertRaises(paramiko.AuthenticationException):
                backend._connect()
        self.assertFalse(backend.is_available())


if __name__ == "__main__":
    unittest.main()
