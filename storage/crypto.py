"""
Transparent encryption layer for Storage

Provides encryption/decryption of files transparently during read/write.
Supports per-file and per-backend encryption policies.
"""

import os
import hashlib
from pathlib import Path
from typing import Optional, Dict
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2
from cryptography.hazmat.backends import default_backend

from .state import StateManager, FileConfig


class EncryptionKey:
    """Represents an encryption key"""

    def __init__(self, key_id: str, key_material: bytes):
        """
        Initialize encryption key

        Args:
            key_id: Unique identifier for this key
            key_material: 32-byte key material
        """
        self.key_id = key_id
        self.fernet = Fernet(Fernet.generate_key() if len(key_material) != 44 else key_material)

    @classmethod
    def from_password(cls, key_id: str, password: str, salt: Optional[bytes] = None) -> 'EncryptionKey':
        """
        Derive encryption key from password using PBKDF2

        Args:
            key_id: Unique identifier
            password: Password string
            salt: Optional salt (generates random if None)

        Returns:
            EncryptionKey instance
        """
        if salt is None:
            salt = os.urandom(16)

        kdf = PBKDF2(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )

        key_material = kdf.derive(password.encode())

        # Convert to Fernet-compatible key (base64 encoded)
        import base64
        fernet_key = base64.urlsafe_b64encode(key_material)

        instance = cls(key_id, fernet_key)
        instance.salt = salt
        return instance

    def encrypt(self, data: bytes) -> bytes:
        """Encrypt data"""
        return self.fernet.encrypt(data)

    def decrypt(self, data: bytes) -> bytes:
        """Decrypt data"""
        return self.fernet.decrypt(data)


class KeyManager:
    """Manages encryption keys"""

    def __init__(self, key_storage_path: Optional[Path] = None):
        """
        Initialize key manager

        Args:
            key_storage_path: Path to store keys (optional)
        """
        self.keys: Dict[str, EncryptionKey] = {}
        self.key_storage_path = key_storage_path

        if key_storage_path and key_storage_path.exists():
            self._load_keys()

    def add_key(self, key: EncryptionKey):
        """Add a key to the manager"""
        self.keys[key.key_id] = key

        if self.key_storage_path:
            self._save_keys()

    def get_key(self, key_id: str) -> Optional[EncryptionKey]:
        """Get a key by ID"""
        return self.keys.get(key_id)

    def remove_key(self, key_id: str):
        """Remove a key"""
        if key_id in self.keys:
            del self.keys[key_id]

            if self.key_storage_path:
                self._save_keys()

    def _load_keys(self):
        """Load keys from storage"""
        # TODO: Implement secure key storage
        pass

    def _save_keys(self):
        """Save keys to storage"""
        # TODO: Implement secure key storage
        pass


class CryptoManager:
    """
    Manages encryption/decryption for Storage

    Handles transparent encryption based on file configuration
    and backend policies.
    """

    def __init__(self, state_manager: StateManager, key_manager: KeyManager):
        """
        Initialize crypto manager

        Args:
            state_manager: State manager instance
            key_manager: Key manager instance
        """
        self.state_manager = state_manager
        self.key_manager = key_manager
        self.default_key_id = "default"

        # Create default key if not exists
        if not self.key_manager.get_key(self.default_key_id):
            # Generate default key
            default_key = EncryptionKey(self.default_key_id, Fernet.generate_key())
            self.key_manager.add_key(default_key)

    def should_encrypt(self, virtual_path: str, cache_path: Path, backend_name: Optional[str] = None) -> bool:
        """
        Determine if a file should be encrypted

        Args:
            virtual_path: Virtual path of file
            cache_path: Local cache path
            backend_name: Optional backend name to check backend-specific encryption

        Returns:
            True if file should be encrypted
        """
        config = self.state_manager.get_inherited_config(cache_path)

        # Check if globally encrypted
        if config.encrypted:
            return True

        # Check if specific backend requires encryption
        if backend_name and config.encryption_backends:
            return backend_name in config.encryption_backends

        return False

    def get_key_for_file(self, virtual_path: str, cache_path: Path) -> Optional[EncryptionKey]:
        """
        Get encryption key for a file

        Args:
            virtual_path: Virtual path
            cache_path: Cache path

        Returns:
            EncryptionKey if file is encrypted, None otherwise
        """
        if not self.should_encrypt(virtual_path, cache_path):
            return None

        # Get key ID from metadata
        metadata = self.state_manager.get_file_metadata(virtual_path)

        if metadata and metadata.encryption_key_id:
            key = self.key_manager.get_key(metadata.encryption_key_id)
            if key:
                return key

        # Use default key
        return self.key_manager.get_key(self.default_key_id)

    def encrypt_data(self, data: bytes, virtual_path: str, cache_path: Path) -> bytes:
        """
        Encrypt data for a file

        Args:
            data: Data to encrypt
            virtual_path: Virtual path
            cache_path: Cache path

        Returns:
            Encrypted data (or original if not configured for encryption)
        """
        key = self.get_key_for_file(virtual_path, cache_path)

        if not key:
            return data

        return key.encrypt(data)

    def decrypt_data(self, data: bytes, virtual_path: str, cache_path: Path) -> bytes:
        """
        Decrypt data for a file

        Args:
            data: Encrypted data
            virtual_path: Virtual path
            cache_path: Cache path

        Returns:
            Decrypted data (or original if not encrypted)
        """
        key = self.get_key_for_file(virtual_path, cache_path)

        if not key:
            return data

        try:
            return key.decrypt(data)
        except Exception as e:
            # Decryption failed - possibly wrong key or corrupted data
            raise ValueError(f"Failed to decrypt data: {e}")

    def encrypt_file(self, input_path: Path, output_path: Path, virtual_path: str, cache_path: Path):
        """
        Encrypt a file

        Args:
            input_path: Source file
            output_path: Destination file
            virtual_path: Virtual path
            cache_path: Cache path
        """
        with open(input_path, 'rb') as f:
            data = f.read()

        encrypted = self.encrypt_data(data, virtual_path, cache_path)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'wb') as f:
            f.write(encrypted)

    def decrypt_file(self, input_path: Path, output_path: Path, virtual_path: str, cache_path: Path):
        """
        Decrypt a file

        Args:
            input_path: Encrypted file
            output_path: Destination file
            virtual_path: Virtual path
            cache_path: Cache path
        """
        with open(input_path, 'rb') as f:
            encrypted = f.read()

        decrypted = self.decrypt_data(encrypted, virtual_path, cache_path)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'wb') as f:
            f.write(decrypted)


def enable_encryption(cache_path: Path, state_manager: StateManager, key_id: Optional[str] = None):
    """
    Enable encryption for a file or directory

    Args:
        cache_path: Local cache path
        state_manager: State manager
        key_id: Optional key ID (uses default if None)
    """
    config = state_manager.get_file_config(cache_path) or FileConfig()
    config.encrypted = True
    state_manager.set_file_config(cache_path, config)


def disable_encryption(cache_path: Path, state_manager: StateManager):
    """
    Disable encryption for a file or directory

    Args:
        cache_path: Local cache path
        state_manager: State manager
    """
    config = state_manager.get_file_config(cache_path) or FileConfig()
    config.encrypted = False
    config.encryption_backends = []
    state_manager.set_file_config(cache_path, config)
