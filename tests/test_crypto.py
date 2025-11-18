import tempfile
import shutil
import unittest
from pathlib import Path

from cryptography.fernet import Fernet

from storage.crypto import EncryptionKey, KeyManager


class TestKeyPersistence(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.key_file = self.temp_dir / "keys.json"

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_keys_are_saved_and_loaded(self):
        manager = KeyManager(self.key_file)
        key = EncryptionKey("test-key", Fernet.generate_key())
        manager.add_key(key)

        # Persisted on disk
        self.assertTrue(self.key_file.exists())
        self.assertEqual(self.key_file.stat().st_mode & 0o777, 0o600)

        ciphertext = key.encrypt(b"secret")

        # New manager should load the persisted key
        reloaded = KeyManager(self.key_file)
        loaded_key = reloaded.get_key("test-key")
        self.assertIsNotNone(loaded_key)
        self.assertEqual(loaded_key.decrypt(ciphertext), b"secret")


if __name__ == '__main__':
    unittest.main()
