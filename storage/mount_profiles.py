import click
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

from .backends.local import LocalBackend
from .backends.ssh import SSHBackend


DEFAULT_CACHE_SIZE = "10G"
DEFAULT_SYNC_WORKERS = 2
DEFAULT_STREAMING_CHUNK_SIZE = "1M"
DEFAULT_MAX_STREAMING_SESSIONS = 8
DEFAULT_MOUNT_CONFIG_NAME = "storage.toml"


@dataclass(frozen=True)
class ResolvedMountConfig:
    mountpoint: str
    backing_paths: Tuple[str, ...]
    remote: Tuple[str, ...]
    cache: str
    cache_size: str
    database: Optional[str]
    workers: int
    key_file: Optional[str]
    foreground: bool
    allow_other: Optional[bool]
    debug: bool
    streaming_reads: bool
    streaming_chunk_size: str
    max_streaming_sessions: int
    readdir_ttl: float
    enable_watchers: bool
    profile_name: Optional[str] = None
    profile_source: Optional[Path] = None

    @property
    def member_specs(self) -> Tuple[str, ...]:
        return self.backing_paths + self.remote


@dataclass(frozen=True)
class ResolvedMemberConfig:
    member_specs: Tuple[str, ...]
    key_file: Optional[str]
    profile_name: Optional[str] = None
    profile_source: Optional[Path] = None


def default_mount_config_paths() -> Tuple[Path, ...]:
    return (
        Path.home() / ".config" / "storage" / DEFAULT_MOUNT_CONFIG_NAME,
        Path("/etc/storage") / DEFAULT_MOUNT_CONFIG_NAME,
    )


def parse_size(size_str: str) -> int:
    """Parse size string to bytes."""
    size_str = size_str.strip().upper()

    multipliers = {
        "K": 1024,
        "M": 1024 ** 2,
        "G": 1024 ** 3,
        "T": 1024 ** 4,
    }

    for suffix, multiplier in multipliers.items():
        if size_str.endswith(suffix):
            try:
                value = float(size_str[:-1])
                return int(value * multiplier)
            except ValueError as exc:
                raise ValueError(f"Invalid size format: {size_str}") from exc

    try:
        return int(size_str)
    except ValueError as exc:
        raise ValueError(f"Invalid size format: {size_str}") from exc


def _load_mount_profile(config_path: Path, profile_name: str) -> Dict[str, Any]:
    try:
        with config_path.open("rb") as fh:
            data = tomllib.load(fh)
    except FileNotFoundError as exc:
        raise click.ClickException(f"Mount config file not found: {config_path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise click.ClickException(f"Invalid TOML in mount config {config_path}: {exc}") from exc

    profile: Optional[Dict[str, Any]] = None
    mounts = data.get("mounts")
    if isinstance(mounts, dict):
        candidate = mounts.get(profile_name)
        if isinstance(candidate, dict):
            profile = candidate

    if profile is None:
        candidate = data.get(profile_name)
        if isinstance(candidate, dict):
            profile = candidate

    if profile is None:
        raise click.ClickException(
            f"Mount profile '{profile_name}' not found in {config_path}"
        )

    return profile


def _require_profile_string(profile: Dict[str, Any], key: str, profile_name: str) -> str:
    value = profile.get(key)
    if not isinstance(value, str) or not value.strip():
        raise click.ClickException(
            f"Mount profile '{profile_name}' must define a non-empty string '{key}'"
        )
    return value


def _optional_profile_string(profile: Dict[str, Any], key: str, profile_name: str) -> Optional[str]:
    value = profile.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise click.ClickException(
            f"Mount profile '{profile_name}' must define '{key}' as a non-empty string when present"
        )
    return value


def _optional_profile_float(profile: Dict[str, Any], key: str, profile_name: str) -> Optional[float]:
    value = profile.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise click.ClickException(
            f"Mount profile '{profile_name}' must define '{key}' as a number when present"
        )
    return float(value)


def _optional_profile_int(profile: Dict[str, Any], key: str, profile_name: str) -> Optional[int]:
    value = profile.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise click.ClickException(
            f"Mount profile '{profile_name}' must define '{key}' as an integer when present"
        )
    return value


def _optional_profile_bool(profile: Dict[str, Any], key: str, profile_name: str) -> Optional[bool]:
    value = profile.get(key)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise click.ClickException(
            f"Mount profile '{profile_name}' must define '{key}' as a boolean when present"
        )
    return value


def _profile_backends(profile: Dict[str, Any], profile_name: str) -> Tuple[str, ...]:
    raw = profile.get("backends")
    if raw is None:
        return ()
    if not isinstance(raw, list) or not all(isinstance(item, str) and item.strip() for item in raw):
        raise click.ClickException(
            f"Mount profile '{profile_name}' must define 'backends' as a list of non-empty strings"
        )
    return tuple(raw)


def split_storage_member_specs(member_specs: Sequence[str]) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    local_members = []
    remote_members = []
    for spec in member_specs:
        if spec.startswith("ssh://") or spec.startswith("rsync://"):
            remote_members.append(spec)
        else:
            local_members.append(spec)
    return tuple(local_members), tuple(remote_members)


def create_storage_members(
    member_specs: Sequence[str],
    key_file: Optional[str],
    *,
    announce: bool = True,
):
    backends = {}
    local_specs, remote_specs = split_storage_member_specs(member_specs)

    for i, path in enumerate(local_specs):
        try:
            name = f"local{i}"
            backend = LocalBackend(name, path, speed_priority=i * 10)
            backends[name] = backend
            if announce:
                click.echo(f"Added local storage member: {name} -> {path} (priority: {i * 10})")
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc

    for i, remote_spec in enumerate(remote_specs):
        try:
            name = f"remote{i}"
            is_rsync = remote_spec.startswith("rsync://")
            is_ssh = remote_spec.startswith("ssh://")
            if not is_ssh and not is_rsync:
                raise click.ClickException(
                    "Remote backend must start with ssh:// or rsync://"
                )

            prefix_len = 8 if is_rsync else 6  # len("rsync://") or len("ssh://")
            spec = remote_spec[prefix_len:]
            if "@" not in spec or ":" not in spec:
                raise click.ClickException(
                    f"Invalid remote format. Use {'rsync' if is_rsync else 'ssh'}://user@host:/path"
                )

            user_host, remote_path = spec.split(":", 1)
            username, host = user_host.split("@", 1)

            if is_rsync:
                from .backends.rsync import RsyncBackend
                backend = RsyncBackend(
                    name=name,
                    host=host,
                    remote_path=remote_path,
                    username=username,
                    key_filename=key_file,
                    speed_priority=50 + i * 10,
                )
                if announce:
                    click.echo(
                        f"Added rsync storage member: {name} -> "
                        f"{username}@{host}:{remote_path} (priority: {50 + i * 10})"
                    )
            else:
                backend = SSHBackend(
                    name=name,
                    host=host,
                    remote_path=remote_path,
                    username=username,
                    key_filename=key_file,
                    speed_priority=50 + i * 10,
                )
                if announce:
                    click.echo(
                        f"Added SSH storage member: {name} -> "
                        f"{username}@{host}:{remote_path} (priority: {50 + i * 10})"
                    )
            backends[name] = backend
        except click.ClickException:
            raise
        except Exception as exc:
            raise click.ClickException(f"Error adding remote backend {remote_spec}: {exc}") from exc

    if not backends:
        raise click.ClickException("No valid storage members provided")

    return backends


def split_backend_specs(backend_specs: Sequence[str]) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    """Compatibility wrapper for older call sites."""
    return split_storage_member_specs(backend_specs)


def create_backends(backing_paths: Sequence[str], remote_specs: Sequence[str], key_file: Optional[str]):
    """Compatibility wrapper for older call sites."""
    return create_storage_members(tuple(backing_paths) + tuple(remote_specs), key_file)


def resolve_member_configuration(
    profile_name: Optional[str],
    config_path: Optional[Path],
    member_specs: Sequence[str],
    key_file: Optional[str],
) -> ResolvedMemberConfig:
    if profile_name:
        if member_specs:
            raise click.ClickException(
                "Provide either a mount profile or explicit --member options, not both."
            )

        profile_source = config_path
        if profile_source is None:
            available_configs = [candidate for candidate in default_mount_config_paths() if candidate.exists()]
            if not available_configs:
                searched = ", ".join(str(path) for path in default_mount_config_paths())
                raise click.ClickException(
                    f"No default mount config files found while looking for profile '{profile_name}': {searched}"
                )

            for candidate in available_configs:
                try:
                    settings = _load_mount_profile_settings(candidate, profile_name)
                    profile_source = candidate
                    break
                except click.ClickException as exc:
                    if f"Mount profile '{profile_name}' not found" in str(exc):
                        continue
                    raise
            else:
                searched = ", ".join(str(path) for path in available_configs)
                raise click.ClickException(
                    f"Mount profile '{profile_name}' not found in default config files: {searched}"
                )
        else:
            settings = _load_mount_profile_settings(profile_source, profile_name)

        resolved_specs = settings["backing_paths"] + settings["remote"]
        if not resolved_specs:
            raise click.ClickException(
                f"Mount profile '{profile_name}' does not define any storage members"
            )

        return ResolvedMemberConfig(
            member_specs=resolved_specs,
            key_file=key_file or settings["key_file"],
            profile_name=profile_name,
            profile_source=profile_source,
        )

    if config_path is not None:
        raise click.ClickException("Provide the named mount profile when using --config")

    if not member_specs:
        raise click.ClickException("Provide at least one --member or a mount profile name")

    return ResolvedMemberConfig(
        member_specs=tuple(member_specs),
        key_file=key_file,
    )


def _load_mount_profile_settings(config_path: Path, profile_name: str) -> Dict[str, Any]:
    config_profile = _load_mount_profile(config_path, profile_name)
    config_backing_paths, config_remote = split_storage_member_specs(
        _profile_backends(config_profile, profile_name)
    )
    return {
        "mountpoint": _require_profile_string(config_profile, "mountpoint", profile_name),
        "cache": _require_profile_string(config_profile, "cache", profile_name),
        "cache_size": _optional_profile_string(config_profile, "cache_size", profile_name),
        "database": _optional_profile_string(config_profile, "database", profile_name),
        "workers": _optional_profile_int(config_profile, "workers", profile_name),
        "key_file": _optional_profile_string(config_profile, "key_file", profile_name),
        "foreground": _optional_profile_bool(config_profile, "foreground", profile_name),
        "allow_other": _optional_profile_bool(config_profile, "allow_other", profile_name),
        "debug": _optional_profile_bool(config_profile, "debug", profile_name),
        "streaming_reads": _optional_profile_bool(config_profile, "streaming_reads", profile_name),
        "streaming_chunk_size": _optional_profile_string(
            config_profile, "streaming_chunk_size", profile_name
        ),
        "max_streaming_sessions": _optional_profile_int(
            config_profile, "max_streaming_sessions", profile_name
        ),
        "readdir_ttl": _optional_profile_float(config_profile, "readdir_ttl", profile_name),
        "enable_watchers": _optional_profile_bool(config_profile, "enable_watchers", profile_name),
        "backing_paths": config_backing_paths,
        "remote": config_remote,
    }


def _target_looks_like_profile_name(mount_target: Optional[str]) -> bool:
    if not mount_target:
        return False
    if mount_target.startswith(("/", ".", "~")):
        return False
    return "/" not in mount_target


def _can_auto_load_mount_profile(
    mount_target: Optional[str],
    backing_paths: Sequence[str],
    cache: Optional[str],
    cache_size: Optional[str],
    database: Optional[str],
    workers: Optional[int],
    remote: Sequence[str],
    key_file: Optional[str],
    foreground: Optional[bool],
    allow_other: Optional[bool],
    debug: Optional[bool],
    streaming_reads: Optional[bool],
    streaming_chunk_size: Optional[str],
    max_streaming_sessions: Optional[int],
    readdir_ttl: Optional[float],
    enable_watchers: Optional[bool],
) -> bool:
    if not _target_looks_like_profile_name(mount_target):
        return False
    return not any((
        backing_paths,
        cache,
        cache_size,
        database,
        workers is not None,
        remote,
        key_file,
        foreground is not None,
        allow_other is not None,
        debug is not None,
        streaming_reads is not None,
        streaming_chunk_size,
        max_streaming_sessions is not None,
        readdir_ttl is not None,
        enable_watchers is not None,
    ))


def resolve_mount_configuration(
    mount_target: Optional[str],
    backing_paths: Sequence[str],
    config_path: Optional[Path],
    cache: Optional[str],
    cache_size: Optional[str],
    database: Optional[str],
    workers: Optional[int],
    remote: Sequence[str],
    key_file: Optional[str],
    foreground: Optional[bool],
    allow_other: Optional[bool],
    debug: Optional[bool],
    streaming_reads: Optional[bool],
    streaming_chunk_size: Optional[str],
    max_streaming_sessions: Optional[int],
    readdir_ttl: Optional[float],
    enable_watchers: Optional[bool],
) -> ResolvedMountConfig:
    config_values = {
        "mountpoint": None,
        "cache": None,
        "cache_size": None,
        "database": None,
        "workers": None,
        "key_file": None,
        "foreground": None,
        "allow_other": None,
        "debug": None,
        "streaming_reads": None,
        "streaming_chunk_size": None,
        "max_streaming_sessions": None,
        "readdir_ttl": None,
        "enable_watchers": None,
        "backing_paths": (),
        "remote": (),
    }
    profile_name = None

    if config_path is not None:
        if not mount_target:
            raise click.ClickException("Provide the named mount profile when using --config")
        profile_name = mount_target
        config_values = _load_mount_profile_settings(config_path, profile_name)
    elif _can_auto_load_mount_profile(
        mount_target,
        backing_paths,
        cache,
        cache_size,
        database,
        workers,
        remote,
        key_file,
        foreground,
        allow_other,
        debug,
        streaming_reads,
        streaming_chunk_size,
        max_streaming_sessions,
        readdir_ttl,
        enable_watchers,
    ):
        available_configs = [candidate for candidate in default_mount_config_paths() if candidate.exists()]
        if available_configs:
            profile_name = mount_target
            for candidate in available_configs:
                try:
                    config_values = _load_mount_profile_settings(candidate, profile_name)
                    config_path = candidate
                    break
                except click.ClickException as exc:
                    if f"Mount profile '{profile_name}' not found" in str(exc):
                        continue
                    raise
            if config_path is None:
                searched = ", ".join(str(path) for path in available_configs)
                raise click.ClickException(
                    f"Mount profile '{profile_name}' not found in default config files: {searched}"
                )

    mountpoint = config_values["mountpoint"] if config_values["mountpoint"] is not None else mount_target
    if not mountpoint:
        raise click.ClickException("Missing mountpoint. Provide one directly or load it from --config.")

    cache = cache or config_values["cache"]
    if not cache:
        raise click.ClickException("Missing cache directory. Provide --cache or set 'cache' in the mount profile.")

    return ResolvedMountConfig(
        mountpoint=mountpoint,
        backing_paths=tuple(backing_paths) if backing_paths else config_values["backing_paths"],
        remote=tuple(remote) if remote else config_values["remote"],
        cache=cache,
        cache_size=cache_size or config_values["cache_size"] or DEFAULT_CACHE_SIZE,
        database=database or config_values["database"],
        workers=workers if workers is not None else (
            config_values["workers"] if config_values["workers"] is not None else DEFAULT_SYNC_WORKERS
        ),
        key_file=key_file or config_values["key_file"],
        foreground=foreground if foreground is not None else (
            config_values["foreground"] if config_values["foreground"] is not None else False
        ),
        allow_other=allow_other if allow_other is not None else config_values["allow_other"],
        debug=debug if debug is not None else (
            config_values["debug"] if config_values["debug"] is not None else False
        ),
        streaming_reads=streaming_reads if streaming_reads is not None else (
            config_values["streaming_reads"] if config_values["streaming_reads"] is not None else True
        ),
        streaming_chunk_size=(
            streaming_chunk_size or config_values["streaming_chunk_size"] or DEFAULT_STREAMING_CHUNK_SIZE
        ),
        max_streaming_sessions=max_streaming_sessions if max_streaming_sessions is not None else (
            config_values["max_streaming_sessions"]
            if config_values["max_streaming_sessions"] is not None
            else DEFAULT_MAX_STREAMING_SESSIONS
        ),
        readdir_ttl=readdir_ttl if readdir_ttl is not None else (
            config_values["readdir_ttl"]
            if config_values["readdir_ttl"] is not None
            else 1.0
        ),
        enable_watchers=enable_watchers if enable_watchers is not None else (
            config_values["enable_watchers"]
            if config_values["enable_watchers"] is not None
            else False
        ),
        profile_name=profile_name,
        profile_source=config_path,
    )
