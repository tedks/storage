"""
CLI tool for Storage filesystem
"""

import click
import sys
from pathlib import Path

from .backends.local import LocalBackend
from .backends.ssh import SSHBackend
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
@click.option('-w', '--workers', default=2, help='Number of sync worker threads')
@click.option('-r', '--remote', multiple=True, help='Remote backend (ssh://user@host:/path)')
@click.option('-k', '--key-file', help='SSH private key file')
@click.option('-f', '--foreground', is_flag=True, help='Run in foreground')
def mount(mountpoint, backing_paths, cache, cache_size, database, workers, remote, key_file, foreground):
    """
    Mount a Storage filesystem

    MOUNTPOINT: Where to mount the Storage filesystem
    BACKING_PATHS: One or more local backing filesystem paths

    Remote backends can be specified with --remote:
        --remote ssh://user@host:/remote/path

    Example:
        storage mount /mnt/storage /srv/raid /srv/chunk0 \\
          -c /var/cache/storage -s 50G \\
          --remote ssh://user@example.com:/backup
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
    backend_count = 0

    # Add local backends
    for i, path in enumerate(backing_paths):
        try:
            name = f"local{i}"
            # Local backends get speed priority based on order (assuming first is fastest)
            backend = LocalBackend(name, path, speed_priority=i * 10)
            backends[name] = backend
            backend_count += 1
            click.echo(f"Added local backend: {name} -> {path} (priority: {i * 10})")
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)

    # Add remote backends
    for i, remote_spec in enumerate(remote):
        try:
            name = f"remote{i}"
            # Parse ssh://user@host:/path
            if not remote_spec.startswith('ssh://'):
                click.echo(f"Error: Remote backend must start with ssh://", err=True)
                sys.exit(1)

            # Remove ssh:// prefix
            spec = remote_spec[6:]

            # Parse user@host:/path
            if '@' not in spec or ':' not in spec:
                click.echo(f"Error: Invalid remote format. Use ssh://user@host:/path", err=True)
                sys.exit(1)

            user_host, remote_path = spec.split(':', 1)
            username, host = user_host.split('@', 1)

            # Create SSH backend
            backend = SSHBackend(
                name=name,
                host=host,
                remote_path=remote_path,
                username=username,
                key_filename=key_file,
                speed_priority=50 + i * 10  # Remote backends are slower
            )
            backends[name] = backend
            backend_count += 1
            click.echo(f"Added SSH backend: {name} -> {username}@{host}:{remote_path} (priority: {50 + i * 10})")

        except Exception as e:
            click.echo(f"Error adding remote backend {remote_spec}: {e}", err=True)
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
    click.echo(f"  Sync workers: {workers}")
    click.echo(f"\nPress Ctrl+C to unmount\n")

    try:
        mount_storage(
            str(mountpoint_path),
            cache_root,
            db_path,
            backends,
            cache_size_bytes,
            foreground=foreground,
            num_sync_workers=workers
        )
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
