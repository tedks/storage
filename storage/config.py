"""
Configuration parser for /etc/storagetab
Similar to /etc/fstab format for defining storage mounts
"""

from typing import List, Optional, Dict
from pathlib import Path
from dataclasses import dataclass


@dataclass
class StorageMount:
    """Represents a single storage mount entry"""

    mountpoint: str  # Where to mount the storage filesystem
    backing_paths: List[str]  # List of backing filesystem paths
    options: Dict[str, str]  # Mount options
    enabled: bool = True  # Whether this mount is enabled

    @classmethod
    def from_line(cls, line: str) -> Optional['StorageMount']:
        """
        Parse a single line from storagetab
        Format: <mountpoint> <backing_paths> <options>

        Example:
        /mnt/storage /mnt/disk1,/mnt/disk2,/mnt/disk3 defaults
        /home/user/docs /mnt/backup1,/mnt/backup2 replication=2
        """
        # Strip comments
        if '#' in line:
            line = line[:line.index('#')]

        line = line.strip()
        if not line:
            return None

        parts = line.split()
        if len(parts) < 2:
            return None

        mountpoint = parts[0]
        backing_paths = parts[1].split(',')

        # Parse options
        options = {}
        if len(parts) >= 3:
            for opt in parts[2].split(','):
                if '=' in opt:
                    key, value = opt.split('=', 1)
                    options[key.strip()] = value.strip()
                else:
                    options[opt.strip()] = 'true'

        return cls(
            mountpoint=mountpoint,
            backing_paths=backing_paths,
            options=options
        )

    def __repr__(self):
        return f"StorageMount(mountpoint={self.mountpoint}, backing_paths={self.backing_paths}, options={self.options})"


class StorageTabConfig:
    """Parser for /etc/storagetab configuration file"""

    DEFAULT_PATH = '/etc/storagetab'

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = Path(config_path or self.DEFAULT_PATH)
        self.mounts: List[StorageMount] = []

    def load(self) -> bool:
        """
        Load configuration from storagetab file
        Returns True on success, False if file doesn't exist
        """
        if not self.config_path.exists():
            return False

        self.mounts = []
        try:
            with open(self.config_path, 'r') as f:
                for line in f:
                    mount = StorageMount.from_line(line)
                    if mount:
                        self.mounts.append(mount)
            return True
        except Exception as e:
            print(f"Error reading {self.config_path}: {e}")
            return False

    def save(self) -> bool:
        """
        Save configuration to storagetab file
        Returns True on success
        """
        try:
            with open(self.config_path, 'w') as f:
                f.write("# Storage mount configuration\n")
                f.write("# Format: <mountpoint> <backing_paths> <options>\n")
                f.write("#\n")
                for mount in self.mounts:
                    backing = ','.join(mount.backing_paths)
                    options = ','.join(
                        f"{k}={v}" if v != 'true' else k
                        for k, v in mount.options.items()
                    ) if mount.options else 'defaults'
                    f.write(f"{mount.mountpoint} {backing} {options}\n")
            return True
        except Exception as e:
            print(f"Error writing {self.config_path}: {e}")
            return False

    def add_mount(self, mount: StorageMount):
        """Add a mount to the configuration"""
        self.mounts.append(mount)

    def get_mount(self, mountpoint: str) -> Optional[StorageMount]:
        """Get a mount by mountpoint"""
        for mount in self.mounts:
            if mount.mountpoint == mountpoint:
                return mount
        return None

    def list_mounts(self) -> List[StorageMount]:
        """List all mounts"""
        return self.mounts.copy()
