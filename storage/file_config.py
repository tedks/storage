"""Xattr-backed file and directory configuration helpers."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml


XATTR_CONFIG_NAME = "user.storage.config"


@dataclass
class FileConfig:
    """Configuration for a file or directory stored in extended attributes."""

    replica_count: int = 1
    preferred_backends: List[str] = field(default_factory=list)
    encrypted: bool = False
    encryption_backends: List[str] = field(default_factory=list)
    recomputable: bool = False
    recompute_cmd: Optional[str] = None
    custom: Dict = field(default_factory=dict)

    def to_yaml(self) -> str:
        """Serialize config for xattr storage."""
        return yaml.dump(
            {
                "replica_count": self.replica_count,
                "preferred_backends": self.preferred_backends,
                "encrypted": self.encrypted,
                "encryption_backends": self.encryption_backends,
                "recomputable": self.recomputable,
                "recompute_cmd": self.recompute_cmd,
                "custom": self.custom,
            },
            default_flow_style=False,
        )

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "FileConfig":
        """Deserialize config stored in xattrs."""
        return cls(**(yaml.safe_load(yaml_str) or {}))


def get_file_config(real_path: Path) -> Optional[FileConfig]:
    """Read configuration from a file or directory xattr."""
    try:
        yaml_data = os.getxattr(str(real_path), XATTR_CONFIG_NAME)
        return FileConfig.from_yaml(yaml_data.decode("utf-8"))
    except (OSError, KeyError):
        return None


def set_file_config(real_path: Path, config: FileConfig):
    """Write configuration to a file or directory xattr."""
    try:
        yaml_data = config.to_yaml().encode("utf-8")
        os.setxattr(str(real_path), XATTR_CONFIG_NAME, yaml_data)
    except OSError as exc:
        print(f"Warning: Failed to set xattr on {real_path}: {exc}")


def get_inherited_config(real_path: Path) -> FileConfig:
    """Resolve configuration for a path by walking parent directories."""
    config = FileConfig()
    current = real_path
    configs = []

    while current != current.parent:
        file_config = get_file_config(current)
        if file_config:
            configs.insert(0, file_config)
        current = current.parent

    for cfg in configs:
        if cfg.replica_count == -1 or config.replica_count == -1:
            config.replica_count = -1
        else:
            config.replica_count = max(config.replica_count, cfg.replica_count)

        if cfg.preferred_backends:
            config.preferred_backends = cfg.preferred_backends
        if cfg.encrypted:
            config.encrypted = cfg.encrypted
        if cfg.encryption_backends:
            config.encryption_backends = cfg.encryption_backends
        if cfg.recomputable:
            config.recomputable = cfg.recomputable
            config.recompute_cmd = cfg.recompute_cmd
        if cfg.custom:
            config.custom.update(cfg.custom)

    return config
