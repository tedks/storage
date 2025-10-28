"""
CLI tool for Storage filesystem
"""

import click
import sys
from pathlib import Path

from .backing import BackingManager
from .config import StorageTabConfig, StorageMount
from .fs import mount_storage


@click.group()
def main():
    """Storage - A meta-filesystem that multiplexes over multiple backing filesystems"""
    pass


@main.command()
@click.argument('mountpoint', type=click.Path())
@click.argument('backing_paths', nargs=-1, required=True)
@click.option('-f', '--foreground', is_flag=True, help='Run in foreground')
@click.option('-o', '--options', help='Mount options (comma-separated)')
def mount(mountpoint, backing_paths, foreground, options):
    """
    Mount a Storage filesystem

    MOUNTPOINT: Where to mount the Storage filesystem
    BACKING_PATHS: One or more backing filesystem paths

    Example:
        storage mount /mnt/storage /mnt/disk1 /mnt/disk2 /mnt/disk3
    """
    # Create backing manager
    backing_manager = BackingManager()

    # Add backing filesystems
    for i, path in enumerate(backing_paths):
        try:
            name = f"backing{i}"
            is_default = (i == 0)
            backing_manager.add_filesystem(name, path, is_default=is_default)
            click.echo(f"Added backing filesystem: {name} -> {path}")
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)

    # Parse options if provided
    mount_options = {}
    if options:
        for opt in options.split(','):
            if '=' in opt:
                key, value = opt.split('=', 1)
                mount_options[key.strip()] = value.strip()
            else:
                mount_options[opt.strip()] = 'true'

    # Create mountpoint if it doesn't exist
    mountpoint_path = Path(mountpoint)
    if not mountpoint_path.exists():
        try:
            mountpoint_path.mkdir(parents=True)
            click.echo(f"Created mountpoint: {mountpoint}")
        except OSError as e:
            click.echo(f"Error creating mountpoint: {e}", err=True)
            sys.exit(1)

    click.echo(f"Mounting Storage filesystem at {mountpoint}")
    click.echo(f"Using {len(backing_manager)} backing filesystem(s)")

    try:
        mount_storage(mountpoint, backing_manager, foreground=foreground)
    except Exception as e:
        click.echo(f"Error mounting filesystem: {e}", err=True)
        sys.exit(1)


@main.command()
@click.option('-c', '--config', default='/etc/storagetab', help='Path to storagetab config file')
@click.option('-f', '--foreground', is_flag=True, help='Run in foreground')
@click.argument('mountpoint', required=False)
def mount_all(config, foreground, mountpoint):
    """
    Mount filesystems from /etc/storagetab

    If MOUNTPOINT is specified, only mount that specific entry.
    Otherwise, mount all entries in storagetab.
    """
    config_parser = StorageTabConfig(config)

    if not config_parser.load():
        click.echo(f"Error: Could not read config file: {config}", err=True)
        sys.exit(1)

    mounts = config_parser.list_mounts()
    if not mounts:
        click.echo("No mounts defined in storagetab", err=True)
        sys.exit(1)

    # Filter by mountpoint if specified
    if mountpoint:
        mounts = [m for m in mounts if m.mountpoint == mountpoint]
        if not mounts:
            click.echo(f"Error: No mount found for {mountpoint}", err=True)
            sys.exit(1)

    # Mount each entry
    for mount_entry in mounts:
        click.echo(f"\nMounting {mount_entry.mountpoint}")

        # Create backing manager
        backing_manager = BackingManager()

        # Add backing filesystems
        for i, path in enumerate(mount_entry.backing_paths):
            try:
                name = f"backing{i}"
                is_default = (i == 0)
                backing_manager.add_filesystem(name, path, is_default=is_default)
                click.echo(f"  Added backing: {path}")
            except ValueError as e:
                click.echo(f"  Error: {e}", err=True)
                continue

        # Create mountpoint if needed
        mountpoint_path = Path(mount_entry.mountpoint)
        if not mountpoint_path.exists():
            try:
                mountpoint_path.mkdir(parents=True)
            except OSError as e:
                click.echo(f"  Error creating mountpoint: {e}", err=True)
                continue

        try:
            click.echo(f"  Mounting at {mount_entry.mountpoint}")
            mount_storage(mount_entry.mountpoint, backing_manager, foreground=foreground)
        except Exception as e:
            click.echo(f"  Error: {e}", err=True)
            continue


@main.command()
@click.argument('config_path', default='/etc/storagetab')
def show_config(config_path):
    """Show storagetab configuration"""
    config = StorageTabConfig(config_path)

    if not config.load():
        click.echo(f"No configuration file found at {config_path}")
        return

    click.echo(f"Storage configuration from {config_path}:\n")

    for mount in config.list_mounts():
        click.echo(f"Mountpoint: {mount.mountpoint}")
        click.echo(f"  Backing filesystems:")
        for backing in mount.backing_paths:
            click.echo(f"    - {backing}")
        if mount.options:
            click.echo(f"  Options:")
            for key, value in mount.options.items():
                click.echo(f"    {key}: {value}")
        click.echo()


@main.command()
@click.argument('path', type=click.Path())
def info(path):
    """Show storage metadata for a path"""
    from .xattr import XAttrManager

    xattr_manager = XAttrManager()
    path_obj = Path(path)

    if not path_obj.exists():
        click.echo(f"Error: Path does not exist: {path}", err=True)
        sys.exit(1)

    metadata = xattr_manager.get_metadata(path_obj)

    if metadata:
        click.echo(f"Storage metadata for {path}:")
        click.echo(f"  Backing filesystems: {', '.join(metadata.backing_filesystems) or 'default'}")
        click.echo(f"  Replication count: {metadata.replication_count}")
        if metadata.preferred_filesystem:
            click.echo(f"  Preferred filesystem: {metadata.preferred_filesystem}")
        if metadata.custom_config:
            click.echo(f"  Custom config: {metadata.custom_config}")
    else:
        click.echo(f"No storage metadata found for {path}")

        # Try to get inherited metadata
        inherited = xattr_manager.get_inherited_metadata(path_obj)
        if inherited:
            click.echo(f"\nInherited metadata from parent directory:")
            click.echo(f"  Backing filesystems: {', '.join(inherited.backing_filesystems) or 'default'}")
            click.echo(f"  Replication count: {inherited.replication_count}")


if __name__ == '__main__':
    main()
