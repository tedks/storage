"""
CLI tool for Storage filesystem
"""

import click
import sys
import subprocess
import shutil
import json
import socket
import tomllib
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

from .backends.local import LocalBackend
from .backends.ssh import SSHBackend
from .fs import mount_storage


@click.group()
def main():
    """Storage - A meta-filesystem with local cache and N-way replication"""
    pass


DEFAULT_CACHE_SIZE = "10G"
DEFAULT_SYNC_WORKERS = 2
DEFAULT_STREAMING_CHUNK_SIZE = "1M"
DEFAULT_MAX_STREAMING_SESSIONS = 8
DEFAULT_MOUNT_CONFIG_NAME = "storage.toml"


def _default_mount_config_paths() -> Tuple[Path, ...]:
    return (
        Path.home() / ".config" / "storage" / DEFAULT_MOUNT_CONFIG_NAME,
        Path("/etc/storage") / DEFAULT_MOUNT_CONFIG_NAME,
    )


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
    if not isinstance(value, (int, float)):
        raise click.ClickException(
            f"Mount profile '{profile_name}' must define '{key}' as a number when present"
        )
    return float(value)


def _optional_profile_int(profile: Dict[str, Any], key: str, profile_name: str) -> Optional[int]:
    value = profile.get(key)
    if value is None:
        return None
    if not isinstance(value, int):
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


def _split_backend_specs(backend_specs: Sequence[str]) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    local_backends = []
    remote_backends = []
    for spec in backend_specs:
        if spec.startswith("ssh://") or spec.startswith("rsync://"):
            remote_backends.append(spec)
        else:
            local_backends.append(spec)
    return tuple(local_backends), tuple(remote_backends)


def _create_backends(backing_paths: Sequence[str], remote_specs: Sequence[str], key_file: Optional[str]):
    backends = {}

    for i, path in enumerate(backing_paths):
        try:
            name = f"local{i}"
            backend = LocalBackend(name, path, speed_priority=i * 10)
            backends[name] = backend
            click.echo(f"Added local backend: {name} -> {path} (priority: {i * 10})")
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc

    for i, remote_spec in enumerate(remote_specs):
        try:
            name = f"remote{i}"
            is_rsync = remote_spec.startswith('rsync://')
            is_ssh = remote_spec.startswith('ssh://')
            if not is_ssh and not is_rsync:
                raise click.ClickException(
                    "Remote backend must start with ssh:// or rsync://"
                )

            prefix_len = 8 if is_rsync else 6  # len("rsync://") or len("ssh://")
            spec = remote_spec[prefix_len:]
            if '@' not in spec or ':' not in spec:
                raise click.ClickException(
                    f"Invalid remote format. Use {'rsync' if is_rsync else 'ssh'}://user@host:/path"
                )

            user_host, remote_path = spec.split(':', 1)
            username, host = user_host.split('@', 1)

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
                click.echo(
                    f"Added rsync backend: {name} -> "
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
                click.echo(
                    f"Added SSH backend: {name} -> "
                    f"{username}@{host}:{remote_path} (priority: {50 + i * 10})"
                )
            backends[name] = backend
        except click.ClickException:
            raise
        except Exception as exc:
            raise click.ClickException(f"Error adding remote backend {remote_spec}: {exc}") from exc

    if not backends:
        raise click.ClickException("No valid backing filesystems provided")

    return backends


def _load_mount_profile_settings(config_path: Path, profile_name: str) -> Dict[str, Any]:
    config_profile = _load_mount_profile(config_path, profile_name)
    config_backing_paths, config_remote = _split_backend_specs(_profile_backends(config_profile, profile_name))
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
        "streaming_chunk_size": _optional_profile_string(config_profile, "streaming_chunk_size", profile_name),
        "max_streaming_sessions": _optional_profile_int(config_profile, "max_streaming_sessions", profile_name),
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


@main.command()
@click.argument('mount_target', required=False)
@click.argument('backing_paths', nargs=-1, required=False)
@click.option('--config', 'config_path', type=click.Path(exists=True, dir_okay=False, path_type=Path),
              help='Load a named mount profile from a TOML config file.')
@click.option('-c', '--cache', help='Local cache directory')
@click.option('-s', '--cache-size', help='Cache size (e.g., 10G, 500M)')
@click.option('-d', '--database', help='State database path (default: cache/.storage.db)')
@click.option('-w', '--workers', type=int, help='Number of sync worker threads')
@click.option('-r', '--remote', multiple=True, help='Remote backend (ssh://user@host:/path)')
@click.option('-k', '--key-file', help='SSH private key file')
@click.option('-f', '--foreground', is_flag=True, default=None, help='Run in foreground')
@click.option('--allow-other/--no-allow-other', default=None,
              help='Allow other users to access mount (requires root or fuse.conf setting). Auto-detects if not specified.')
@click.option('--debug', is_flag=True, default=None, help='Enable verbose debug logging')
@click.option('--streaming-reads/--no-streaming-reads', default=None,
              help='Stream uncached reads while prefetching files in the background.')
@click.option('--streaming-chunk-size',
              help='Chunk size for streaming range reads (e.g., 256K, 1M).')
@click.option('--max-streaming-sessions', type=int,
              help='Maximum number of concurrent streaming prefetch sessions.')
@click.option('--readdir-ttl', type=float,
              help='Seconds to cache directory listings (default 1.0). Higher = less backend load.')
@click.option('--enable-watchers/--no-enable-watchers', default=None,
              help='Run inotifywait on SSH backends for push-based cache invalidation.')
def mount(mount_target, backing_paths, config_path, cache, cache_size, database, workers, remote, key_file, foreground,
          allow_other, debug, streaming_reads, streaming_chunk_size, max_streaming_sessions,
          readdir_ttl, enable_watchers):
    """
    Mount a Storage filesystem

    MOUNT_TARGET: Mountpoint path, or a named profile when --config is used
    BACKING_PATHS: Zero or more local backing filesystem paths (optional)

    Remote backends can be specified with --remote:
        --remote ssh://user@host:/remote/path

    Examples:
        # With local and remote backends
        storage mount /mnt/storage /srv/raid /srv/chunk0 \\
          -c /var/cache/storage -s 50G \\
          --remote ssh://user@example.com:/backup

        # Remote-only backends
        storage mount /mnt/storage \\
          -c /var/cache/storage -s 5G \\
          --remote ssh://host1:/path1 \\
          --remote ssh://host2:/path2

        # Auto-discovered profile from ~/.config/storage/storage.toml
        storage mount drynwyn-warez

        # TOML-configured mount
        storage mount --config ~/.config/storage/storage.toml drynwyn-warez
    """
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
        available_configs = [candidate for candidate in _default_mount_config_paths() if candidate.exists()]
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

    cache_size = cache_size or config_values["cache_size"] or DEFAULT_CACHE_SIZE
    database = database or config_values["database"]
    workers = workers if workers is not None else (
        config_values["workers"] if config_values["workers"] is not None else DEFAULT_SYNC_WORKERS
    )
    key_file = key_file or config_values["key_file"]
    foreground = foreground if foreground is not None else (
        config_values["foreground"] if config_values["foreground"] is not None else False
    )
    allow_other = allow_other if allow_other is not None else config_values["allow_other"]
    debug = debug if debug is not None else (
        config_values["debug"] if config_values["debug"] is not None else False
    )
    streaming_reads = (
        streaming_reads if streaming_reads is not None
        else (config_values["streaming_reads"] if config_values["streaming_reads"] is not None else True)
    )
    streaming_chunk_size = (
        streaming_chunk_size or config_values["streaming_chunk_size"] or DEFAULT_STREAMING_CHUNK_SIZE
    )
    max_streaming_sessions = (
        max_streaming_sessions
        if max_streaming_sessions is not None
        else (
            config_values["max_streaming_sessions"]
            if config_values["max_streaming_sessions"] is not None
            else DEFAULT_MAX_STREAMING_SESSIONS
        )
    )
    readdir_ttl = (
        readdir_ttl if readdir_ttl is not None
        else (config_values["readdir_ttl"] if config_values["readdir_ttl"] is not None else 1.0)
    )
    enable_watchers = (
        enable_watchers if enable_watchers is not None
        else (config_values["enable_watchers"] if config_values["enable_watchers"] is not None else False)
    )
    backing_paths = tuple(backing_paths) if backing_paths else config_values["backing_paths"]
    remote = tuple(remote) if remote else config_values["remote"]

    # Parse cache size
    try:
        cache_size_bytes = parse_size(cache_size)
    except ValueError as exc:
        raise click.ClickException(f"Invalid cache size: {exc}") from exc

    try:
        streaming_chunk_bytes = parse_size(streaming_chunk_size)
    except ValueError as exc:
        raise click.ClickException(f"Invalid streaming chunk size: {exc}") from exc

    # Setup paths
    cache_root = Path(cache).resolve()
    cache_root.mkdir(parents=True, exist_ok=True)

    if database:
        db_path = Path(database)
    else:
        db_path = cache_root / '.storage.db'

    mountpoint_path = Path(mountpoint)
    mountpoint_path.mkdir(parents=True, exist_ok=True)

    backends = _create_backends(backing_paths, remote, key_file)

    if profile_name is not None:
        click.echo(f"Loaded mount profile: {profile_name} ({config_path})")
    click.echo(f"\nMounting Storage filesystem:")
    click.echo(f"  Mountpoint: {mountpoint}")
    click.echo(f"  Cache: {cache_root}")
    click.echo(f"  Cache size: {cache_size} ({cache_size_bytes:,} bytes)")
    click.echo(f"  Database: {db_path}")
    click.echo(f"  Backends: {len(backends)}")
    click.echo(f"  Sync workers: {workers}")
    click.echo(f"  Streaming reads: {'enabled' if streaming_reads else 'disabled'}")
    click.echo(f"  Streaming chunk: {streaming_chunk_size} ({streaming_chunk_bytes:,} bytes)")
    click.echo(f"  Max streaming sessions: {max_streaming_sessions}")
    if enable_watchers:
        from storage.backends.ssh import SSHBackend
        non_ssh = [n for n, b in backends.items() if not isinstance(b, SSHBackend)]
        if non_ssh:
            click.echo(f"  Invalidation: push (SSH backends) + pull TTL={readdir_ttl}s ({', '.join(non_ssh)})")
        else:
            click.echo(f"  Invalidation: push (inotifywait)")
    else:
        click.echo(f"  Invalidation: pull (TTL={readdir_ttl}s)")
    click.echo(f"\nPress Ctrl+C to unmount\n")

    # Suppress paramiko noise unless debugging
    import logging
    logging.getLogger("paramiko").setLevel(logging.DEBUG if debug else logging.WARNING)

    try:
        mount_storage(
            str(mountpoint_path),
            cache_root,
            db_path,
            backends,
            cache_size_bytes,
            foreground=foreground,
            num_sync_workers=workers,
            allow_other=allow_other,
            debug=debug,
            streaming_reads=streaming_reads,
            streaming_chunk_size=streaming_chunk_bytes,
            max_streaming_sessions=max_streaming_sessions,
            readdir_ttl=readdir_ttl,
            enable_watchers=enable_watchers,
        )
    except PermissionError as e:
        click.echo(f"Permission error: {e}", err=True)
        click.echo("\nTo fix this, either:", err=True)
        click.echo("  1. Run as root (sudo)", err=True)
        click.echo("  2. Remove the --allow-other flag", err=True)
        click.echo("  3. Add 'user_allow_other' to /etc/fuse.conf", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error mounting filesystem: {e}", err=True)
        sys.exit(1)


@main.command()
@click.argument('path', type=click.Path(exists=True))
@click.option('-r', '--replicas', type=int, help='Number of replicas (-1 for all)')
@click.option('-b', '--backends', help='Preferred backends (comma-separated)')
@click.option('--recomputable/--no-recomputable', default=None, help='Mark as recomputable')
@click.option('--recompute-cmd', help='Command to recompute file (use $STORAGE_OUTPUT for output path)')
@click.option('--encrypt/--no-encrypt', default=None, help='Enable/disable encryption')
@click.option('--encrypt-backends', help='Backends to encrypt for (comma-separated)')
def configure(path, replicas, backends, recomputable, recompute_cmd, encrypt, encrypt_backends):
    """
    Configure storage settings for a file or directory

    Example:
        storage configure /mnt/storage/backups --replicas 3
        storage configure /mnt/storage/downloads --recomputable --recompute-cmd "wget ..."
    """
    from .state import StateManager, FileConfig

    path_obj = Path(path)

    # Get existing config or create new
    config = StateManager.get_file_config(path_obj) or FileConfig()

    # Update config
    if replicas is not None:
        config.replica_count = replicas

    if backends:
        config.preferred_backends = [b.strip() for b in backends.split(',')]

    if recomputable is not None:
        config.recomputable = recomputable

    if recompute_cmd is not None:
        config.recompute_cmd = recompute_cmd

    if encrypt is not None:
        config.encrypted = encrypt

    if encrypt_backends:
        config.encryption_backends = [b.strip() for b in encrypt_backends.split(',')]

    # Save config
    StateManager.set_file_config(path_obj, config)

    click.echo(f"Updated configuration for {path}:")
    click.echo(f"  Replica count: {config.replica_count}")
    if config.preferred_backends:
        click.echo(f"  Preferred backends: {', '.join(config.preferred_backends)}")
    if config.encrypted:
        click.echo(f"  Encryption: enabled")
        if config.encryption_backends:
            click.echo(f"  Encrypt for backends: {', '.join(config.encryption_backends)}")
    if config.recomputable:
        click.echo(f"  Recomputable: yes")
        if config.recompute_cmd:
            click.echo(f"  Recompute command: {config.recompute_cmd}")
            click.echo(f"\nEnvironment variables available:")
            click.echo(f"  $STORAGE_PATH - Virtual path of the file")
            click.echo(f"  $STORAGE_CACHE_PATH - Local cache path")
            click.echo(f"  $STORAGE_OUTPUT - Where to write output (same as CACHE_PATH)")


@main.command()
@click.argument('path', type=click.Path(exists=True))
def info(path):
    """Show storage information for a file or directory"""
    from .state import StateManager

    path_obj = Path(path)

    # Get config
    config = StateManager.get_inherited_config(path_obj)

    click.echo(f"Storage configuration for {path}:")
    click.echo(f"  Replica count: {config.replica_count}")

    if config.preferred_backends:
        click.echo(f"  Preferred backends: {', '.join(config.preferred_backends)}")

    if config.encrypted:
        click.echo(f"  Encrypted: yes")
        if config.encryption_backends:
            click.echo(f"  Encryption backends: {', '.join(config.encryption_backends)}")

    if config.recomputable:
        click.echo(f"  Recomputable: yes")
        if config.recompute_cmd:
            click.echo(f"  Recompute command: {config.recompute_cmd}")

    if config.custom:
        click.echo(f"  Custom config: {config.custom}")


@main.group()
def debug():
    """Debug and diagnostics commands."""
    pass


def _request_control_payload(cache_root: Path, request: str, timeout: float = 2.0) -> dict:
    socket_path = cache_root / ".storage.control.sock"
    if not socket_path.exists():
        raise click.ClickException(f"No running mount control socket at {socket_path}")

    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect(str(socket_path))
            sock.sendall((request + "\n").encode("utf-8"))

            data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
    except (OSError, TimeoutError) as exc:
        raise click.ClickException(f"Failed to query control socket {socket_path}: {exc}") from exc

    if not data:
        raise click.ClickException(f"No response from control socket {socket_path}")

    try:
        payload = json.loads(data.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise click.ClickException(f"Invalid response from control socket {socket_path}: {exc}") from exc

    if isinstance(payload, dict) and payload.get("error"):
        raise click.ClickException(str(payload["error"]))

    return payload


@debug.command("streaming-stats")
@click.option('-c', '--cache', required=True, type=click.Path(), help='Cache directory used by the mount')
@click.option('--timeout', default=2.0, type=float, show_default=True, help='Control socket timeout in seconds')
@click.option('--json-output', is_flag=True, help='Print raw JSON stats')
def debug_streaming_stats(cache, timeout, json_output):
    """Show live streaming-read stats from a running mount."""
    cache_root = Path(cache).resolve()
    payload = _request_control_payload(cache_root, "streaming-stats", timeout=timeout)

    if json_output:
        click.echo(json.dumps(payload, sort_keys=True, indent=2))
        return

    click.echo(f"Streaming stats from {cache_root}:")
    click.echo(f"  PID: {payload.get('pid')}")
    click.echo(f"  Timestamp: {payload.get('timestamp')}")
    click.echo(f"  Streaming enabled: {payload.get('streaming_enabled')}")
    click.echo(f"  Active streaming sessions: {payload.get('active_streaming_sessions')}")
    click.echo(f"  Streaming sessions started: {payload.get('streaming_sessions_started', 0)}")
    click.echo(f"  Fallback opens: {payload.get('streaming_fallback_opens', 0)}")
    click.echo(f"  Foreground fetches: {payload.get('foreground_fetches', 0)}")
    click.echo(f"  Background fetches: {payload.get('background_fetches', 0)}")
    click.echo(f"  Foreground bytes: {payload.get('foreground_remote_bytes', 0)}")
    click.echo(f"  Background bytes: {payload.get('background_remote_bytes', 0)}")


def parse_size(size_str: str) -> int:
    """
    Parse size string to bytes

    Examples: 10G, 500M, 1T, 100K
    """
    size_str = size_str.strip().upper()

    multipliers = {
        'K': 1024,
        'M': 1024 ** 2,
        'G': 1024 ** 3,
        'T': 1024 ** 4,
    }

    for suffix, multiplier in multipliers.items():
        if size_str.endswith(suffix):
            try:
                value = float(size_str[:-1])
                return int(value * multiplier)
            except ValueError:
                raise ValueError(f"Invalid size format: {size_str}")

    # Try parsing as plain number (bytes)
    try:
        return int(size_str)
    except ValueError:
        raise ValueError(f"Invalid size format: {size_str}")


def _scan_conflict_groups(directory: Path):
    """Return list of (base, {backend: path}) conflicts in directory."""
    groups = {}
    for entry in directory.iterdir():
        if entry.is_dir():
            continue
        parts = entry.name.split('.')
        if len(parts) < 2:
            continue
        backend = parts[-1]
        base = '.'.join(parts[:-1])
        if not base:
            continue
        groups.setdefault(base, {})[backend] = entry

    conflicts = []
    for base, files in groups.items():
        if len(files) >= 2:
            conflicts.append((base, files))
    return conflicts


def _attempt_merge_bytes(content_a: bytes, content_b: bytes) -> Optional[bytes]:
    """
    Simple 2-way merge:
    - if identical, return either
    - if one is a prefix of the other, return the longer
    - otherwise, return None to signal conflict
    """
    if content_a == content_b:
        return content_a
    if content_a.startswith(content_b):
        return content_a
    if content_b.startswith(content_a):
        return content_b
    return None


def _select_merge_tool() -> Optional[str]:
    """Pick an external 3-way merge tool if available."""
    for tool in ("git", "diff3"):
        if shutil.which(tool):
            return tool
    return None


def _run_merge_tool(tool: str, local: Path, ancestor: Path, remote: Path) -> Optional[bytes]:
    """
    Invoke an external 3-way merge tool and return merged bytes or None on error.
    """
    try:
        if tool == "git":
            cmd = ["git", "merge-file", "-p", str(local), str(ancestor), str(remote)]
        elif tool == "diff3":
            cmd = ["diff3", "-m", str(local), str(ancestor), str(remote)]
        else:
            raise ValueError(f"Unsupported merge tool {tool}")

        result = subprocess.run(cmd, capture_output=True)
        if result.returncode not in (0, 1):  # 1 can mean conflicts merged with markers
            return None
        return result.stdout
    except Exception:
        return None


@main.command('conflicts')
@click.argument('directory', type=click.Path(exists=True))
@click.option('-f', '--file', 'target_file', type=click.Path(), help='Path (within the directory) to a specific conflict file or its base name')
@click.option('--take', '--use', '--resolve', 'take_backend', help='Backend name to take for the specified conflict')
@click.option('--merge', '--meld', 'merge', is_flag=True, help='Attempt a 3-way merge for the specified conflict')
def conflicts(directory, target_file, take_backend, merge):
    """
    List and resolve checksum conflicts in a mounted directory.

    When files differ across backends they appear as <name>.<backend>.
    Use --take/--use/--resolve BACKEND to pick one version everywhere,
    or --merge/--meld to attempt a trivial merge when possible.
    """
    dir_path = Path(directory)
    conflict_groups = _scan_conflict_groups(dir_path)

    if not conflict_groups:
        click.echo("No conflicts detected.")
        return

    if take_backend and merge:
        click.echo("Choose either --take or --merge, not both.", err=True)
        sys.exit(2)

    # If resolving, a target file is required
    if (take_backend or merge) and not target_file:
        raise click.UsageError("--file is required when using --take/--merge")

    if target_file:
        target_path = Path(target_file)
        base_name = target_path.name

        target = None
        for cg in conflict_groups:
            base, files = cg
            if base == base_name or target_path.name in [p.name for p in files.values()]:
                target = cg
                break

        if not target:
            # Try stripping a backend suffix heuristically
            if '.' in base_name:
                stripped = '.'.join(base_name.split('.')[:-1])
                target = next((cg for cg in conflict_groups if cg[0] == stripped), None)

        if not target:
            raise click.ClickException(f"No conflict found for {base_name}")

        conflict_groups = [target]  # narrow to the requested conflict
    else:
        if not take_backend and not merge:
            click.echo(f"Found {len(conflict_groups)} conflict(s):")
            for base, files in conflict_groups:
                backends = ', '.join(sorted(files.keys()))
                click.echo(f"  {base}: {backends}")
            click.echo("\nUse --file <path> with --take <backend> or --merge to resolve a specific conflict.")
            return
        else:
            raise click.UsageError("--file is required when resolving conflicts")

    for base, files in conflict_groups:
        base_path = dir_path / base

        if take_backend:
            if take_backend not in files:
                click.echo(f"Skipping {base}: no version from {take_backend}")
                continue
            selected = files[take_backend].read_bytes()
            base_path.write_bytes(selected)
            # Overwrite each backend-specific view so checksums converge
            for path in files.values():
                path.write_bytes(selected)
            click.echo(f"{base}: took {take_backend}")
        elif merge:
            merge_tool = _select_merge_tool()
            if not merge_tool:
                click.echo(f"{base}: merge requested but no git or diff3 available")
                continue
            if len(files) < 2:
                click.echo(f"{base}: not enough versions to merge")
                continue
            backends = sorted(files.keys())
            local_path = files[backends[0]]
            remote_path = files[backends[1]]

            # Use existing base if present, else empty temp file
            if base_path.exists():
                ancestor_path = base_path
            else:
                import tempfile
                tmp = tempfile.NamedTemporaryFile(delete=False)
                tmp.close()
                ancestor_path = Path(tmp.name)

            merged = _run_merge_tool(merge_tool, local_path, ancestor_path, remote_path)
            if merged is None:
                click.echo(f"{base}: merge tool {merge_tool} failed")
                continue

            base_path.write_bytes(merged)
            for path in files.values():
                path.write_bytes(merged)

            click.echo(f"{base}: merged with {merge_tool}")

    click.echo("Conflict resolution complete.")

main.add_command(conflicts, name="conflict")


@main.command('resolve')
@click.argument('directory', type=click.Path(exists=True))
@click.option('--auto', type=click.Choice(['local', 'remote', 'both']), help='Automatically resolve all conflicts')
def resolve_conflicts(directory, auto):
    """
    Resolve file conflicts in a directory

    Shows all conflicts and allows interactive or automatic resolution.

    DIRECTORY: Directory to scan for conflicts

    Examples:
        storage resolve /mnt/storage/documents
        storage resolve /mnt/storage --auto local
    """
    from .conflicts import ConflictResolver
    from .state import StateManager

    dir_path = Path(directory)
    state_manager = StateManager(":memory:")  # Temporary for conflict resolution
    resolver = ConflictResolver(state_manager)

    # Find conflicts
    conflicts = resolver.list_conflicts(dir_path)

    if not conflicts:
        click.echo("No conflicts found.")
        return

    click.echo(f"Found {len(conflicts)} conflict(s):\n")

    for base, local_file, remote_file, timestamp in conflicts:
        click.echo(f"Conflict: {base}")
        click.echo(f"  Local:  {local_file.name}")
        click.echo(f"  Remote: {remote_file.name}")
        click.echo(f"  Time:   {timestamp}")

        if auto:
            # Automatic resolution
            if auto == 'local':
                resolver.resolve_with_local(local_file, remote_file, base)
                click.echo(f"  → Resolved with LOCAL version\n")
            elif auto == 'remote':
                resolver.resolve_with_remote(local_file, remote_file, base)
                click.echo(f"  → Resolved with REMOTE version\n")
            elif auto == 'both':
                resolver.resolve_with_both(local_file, remote_file, base)
                click.echo(f"  → Kept BOTH versions\n")
        else:
            # Interactive resolution
            click.echo("\nChoose resolution:")
            click.echo("  l - Use local version")
            click.echo("  r - Use remote version")
            click.echo("  b - Keep both")
            click.echo("  s - Skip this conflict")

            choice = click.prompt("Choice", type=str, default='s').lower()

            if choice == 'l':
                resolver.resolve_with_local(local_file, remote_file, base)
                click.echo("Resolved with local version\n")
            elif choice == 'r':
                resolver.resolve_with_remote(local_file, remote_file, base)
                click.echo("Resolved with remote version\n")
            elif choice == 'b':
                resolver.resolve_with_both(local_file, remote_file, base)
                click.echo("Kept both versions\n")
            else:
                click.echo("Skipped\n")

    click.echo("Conflict resolution complete.")


if __name__ == '__main__':
    main()
