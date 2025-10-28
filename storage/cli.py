"""
CLI tool for Storage filesystem
"""

import click
import sys
from pathlib import Path

from .backends.local import LocalBackend
from .fs import mount_storage


@click.group()
def main():
    """Storage - A meta-filesystem with local cache and N-way replication"""
    pass


@main.command()
@click.argument('mountpoint', type=click.Path())
@click.argument('backing_paths', nargs=-1, required=True)
@click.option('-c', '--cache', required=True, help='Local cache directory')
@click.option('-s', '--cache-size', default='10G', help='Cache size (e.g., 10G, 500M)')
@click.option('-d', '--database', help='State database path (default: cache/.storage.db)')
@click.option('-f', '--foreground', is_flag=True, help='Run in foreground')
def mount(mountpoint, backing_paths, cache, cache_size, database, foreground):
    """
    Mount a Storage filesystem

    MOUNTPOINT: Where to mount the Storage filesystem
    BACKING_PATHS: One or more backing filesystem paths

    Example:
        storage mount /mnt/storage /srv/raid /srv/chunk0 /srv/chunk1 -c /var/cache/storage -s 50G
    """
    # Parse cache size
    try:
        cache_size_bytes = parse_size(cache_size)
    except ValueError as e:
        click.echo(f"Error: Invalid cache size: {e}", err=True)
        sys.exit(1)

    # Setup paths
    cache_root = Path(cache).resolve()
    cache_root.mkdir(parents=True, exist_ok=True)

    if database:
        db_path = Path(database)
    else:
        db_path = cache_root / '.storage.db'

    mountpoint_path = Path(mountpoint)
    mountpoint_path.mkdir(parents=True, exist_ok=True)

    # Create backends
    backends = {}
    for i, path in enumerate(backing_paths):
        try:
            name = f"backend{i}"
            # Local backends get speed priority based on order (assuming first is fastest)
            backend = LocalBackend(name, path, speed_priority=i * 10)
            backends[name] = backend
            click.echo(f"Added backend: {name} -> {path} (priority: {i * 10})")
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)

    if not backends:
        click.echo("Error: No valid backing filesystems provided", err=True)
        sys.exit(1)

    click.echo(f"\nMounting Storage filesystem:")
    click.echo(f"  Mountpoint: {mountpoint}")
    click.echo(f"  Cache: {cache_root}")
    click.echo(f"  Cache size: {cache_size} ({cache_size_bytes:,} bytes)")
    click.echo(f"  Database: {db_path}")
    click.echo(f"  Backends: {len(backends)}")
    click.echo(f"\nPress Ctrl+C to unmount\n")

    try:
        mount_storage(
            str(mountpoint_path),
            cache_root,
            db_path,
            backends,
            cache_size_bytes,
            foreground=foreground
        )
    except Exception as e:
        click.echo(f"Error mounting filesystem: {e}", err=True)
        sys.exit(1)


@main.command()
@click.argument('path', type=click.Path(exists=True))
@click.option('-r', '--replicas', type=int, help='Number of replicas (-1 for all)')
@click.option('-b', '--backends', help='Preferred backends (comma-separated)')
@click.option('--recomputable/--no-recomputable', default=None, help='Mark as recomputable')
@click.option('--recompute-cmd', help='Command to recompute file')
def configure(path, replicas, backends, recomputable, recompute_cmd):
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

    # Save config
    StateManager.set_file_config(path_obj, config)

    click.echo(f"Updated configuration for {path}:")
    click.echo(f"  Replica count: {config.replica_count}")
    if config.preferred_backends:
        click.echo(f"  Preferred backends: {', '.join(config.preferred_backends)}")
    if config.recomputable:
        click.echo(f"  Recomputable: yes")
        if config.recompute_cmd:
            click.echo(f"  Recompute command: {config.recompute_cmd}")


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


if __name__ == '__main__':
    main()
