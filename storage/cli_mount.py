import logging
import sys
from pathlib import Path
from typing import Optional

import click

from .file_config import FileConfig, get_file_config, get_inherited_config, set_file_config
from .fs import fuse_mount_options, mount_storage
from .mount_profiles import (
    create_storage_members,
    parse_keep_after_close,
    parse_size,
    resolve_mount_configuration,
)


@click.command()
@click.argument("mount_target", required=False)
@click.argument("backing_paths", nargs=-1, required=False)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Load a named mount profile from a TOML config file.",
)
@click.option("-c", "--cache", help="Local cache directory")
@click.option("-s", "--cache-size", help="Cache size (e.g., 10G, 500M)")
@click.option("-d", "--database", help="State database path (default: cache/.storage.db)")
@click.option("-w", "--workers", type=int, help="Number of sync worker threads")
@click.option("-r", "--remote", multiple=True, help="Remote storage member (ssh://user@host:/path)")
@click.option("-k", "--key-file", help="SSH private key file")
@click.option("-f", "--foreground", is_flag=True, default=None, help="Run in foreground")
@click.option(
    "--allow-other/--no-allow-other",
    default=None,
    help="Allow other users to access mount (requires root or fuse.conf setting). Auto-detects if not specified.",
)
@click.option("--debug", is_flag=True, default=None, help="Enable verbose debug logging")
@click.option(
    "--streaming-reads/--no-streaming-reads",
    default=None,
    help="Stream uncached reads while prefetching files in the background.",
)
@click.option(
    "--streaming-chunk-size",
    help=(
        "Unit of streaming read-ahead (e.g., 256K, 1M); read-ahead starts once readers "
        "have read one chunk. Reads fetch only what they ask for."
    ),
)
@click.option(
    "--max-streaming-sessions",
    type=int,
    help="Maximum number of concurrent streaming prefetch sessions.",
)
@click.option(
    "--streaming-keep-after-close",
    help=(
        "Keep prefetching after the last close once readers have read a contiguous "
        "span this long: comma-separated sizes or percentages of the file, whichever "
        "is reached first (default: 4 chunks or 50%; 0 = after any read). Spans include "
        "kernel readahead, so sizes up to about 128K can act like 0."
    ),
)
@click.option(
    "--readdir-ttl",
    type=float,
    help="Seconds to cache directory listings (default 1.0). Higher = less backend load.",
)
@click.option(
    "--enable-watchers/--no-enable-watchers",
    default=None,
    help="Run inotifywait on SSH backends for push-based cache invalidation.",
)
@click.option(
    "--revalidate-after",
    type=float,
    help=(
        "Seconds after which opening a cached, synced file stats its member and "
        "refetches it if the member's copy changed (default 60; 0 = every open)."
    ),
)
def mount(
    mount_target,
    backing_paths,
    config_path,
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
    streaming_keep_after_close,
    readdir_ttl,
    enable_watchers,
    revalidate_after,
):
    """
    Mount a Storage filesystem

    MOUNT_TARGET: Mountpoint path, or a named profile when --config is used
    BACKING_PATHS: Zero or more local storage member roots (optional)

    Remote storage members can be specified with --remote:
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
        storage mount example-media

        # TOML-configured mount
        storage mount --config ~/.config/storage/storage.toml example-media
    """
    resolved = resolve_mount_configuration(
        mount_target,
        backing_paths,
        config_path,
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
        streaming_keep_after_close=streaming_keep_after_close,
        revalidate_after=revalidate_after,
    )

    try:
        cache_size_bytes = parse_size(resolved.cache_size)
    except ValueError as exc:
        raise click.ClickException(f"Invalid cache size: {exc}") from exc

    try:
        streaming_chunk_bytes = parse_size(resolved.streaming_chunk_size)
    except ValueError as exc:
        raise click.ClickException(f"Invalid streaming chunk size: {exc}") from exc

    keep_after_close = None
    if resolved.streaming_keep_after_close is not None:
        try:
            keep_after_close = parse_keep_after_close(resolved.streaming_keep_after_close)
        except ValueError as exc:
            raise click.ClickException(f"Invalid streaming keep-after-close: {exc}") from exc

    cache_root = Path(resolved.cache).resolve()
    cache_root.mkdir(parents=True, exist_ok=True)

    if resolved.database:
        db_path = Path(resolved.database)
    else:
        db_path = cache_root / ".storage.db"

    mountpoint_path = Path(resolved.mountpoint)
    mountpoint_path.mkdir(parents=True, exist_ok=True)

    backends = create_storage_members(resolved.member_specs, resolved.key_file)

    if resolved.profile_name is not None:
        click.echo(f"Loaded mount profile: {resolved.profile_name} ({resolved.profile_source})")
    click.echo("\nMounting Storage filesystem:")
    click.echo(f"  Mountpoint: {resolved.mountpoint}")
    click.echo(f"  Cache: {cache_root}")
    click.echo(f"  Cache size: {resolved.cache_size} ({cache_size_bytes:,} bytes)")
    click.echo(f"  Database: {db_path}")
    click.echo(f"  Storage members: {len(backends)}")
    click.echo(f"  Sync workers: {resolved.workers}")
    click.echo(f"  Streaming reads: {'enabled' if resolved.streaming_reads else 'disabled'}")
    click.echo(
        f"  Streaming chunk: {resolved.streaming_chunk_size} ({streaming_chunk_bytes:,} bytes)"
    )
    click.echo(f"  Max streaming sessions: {resolved.max_streaming_sessions}")
    click.echo(
        "  Streaming keep after close: "
        f"{resolved.streaming_keep_after_close if keep_after_close else 'default (4 chunks or 50%)'}"
    )
    if resolved.enable_watchers:
        non_watchable = [n for n, b in backends.items() if not b.supports_push_invalidation()]
        if non_watchable:
            click.echo(
                f"  Invalidation: push (watch-capable members) + pull TTL={resolved.readdir_ttl}s "
                f"({', '.join(non_watchable)})"
            )
        else:
            click.echo("  Invalidation: push (inotifywait)")
    else:
        click.echo(f"  Invalidation: pull (TTL={resolved.readdir_ttl}s)")
    click.echo(f"  Revalidate cached files after: {resolved.revalidate_after}s")
    kernel = fuse_mount_options(
        backends,
        enable_watchers=resolved.enable_watchers,
        fresh_window=resolved.revalidate_after,
        attr_timeout=resolved.attr_timeout,
        entry_timeout=resolved.entry_timeout,
        negative_timeout=resolved.negative_timeout,
    )
    click.echo(
        f"  Kernel cache: attributes {kernel['attr_timeout']}s, entries {kernel['entry_timeout']}s, "
        f"missing names {kernel['negative_timeout']}s, pages kept while unchanged"
    )
    click.echo("\nPress Ctrl+C to unmount\n")

    logging.getLogger("paramiko").setLevel(logging.DEBUG if resolved.debug else logging.WARNING)

    try:
        mount_storage(
            str(mountpoint_path),
            cache_root,
            db_path,
            backends,
            cache_size_bytes,
            foreground=resolved.foreground,
            num_sync_workers=resolved.workers,
            allow_other=resolved.allow_other,
            debug=resolved.debug,
            streaming_reads=resolved.streaming_reads,
            streaming_chunk_size=streaming_chunk_bytes,
            max_streaming_sessions=resolved.max_streaming_sessions,
            streaming_keep_after_close=keep_after_close,
            readdir_ttl=resolved.readdir_ttl,
            enable_watchers=resolved.enable_watchers,
            revalidate_after=resolved.revalidate_after,
            attr_timeout=resolved.attr_timeout,
            entry_timeout=resolved.entry_timeout,
            negative_timeout=resolved.negative_timeout,
        )
    except PermissionError as exc:
        click.echo(f"Permission error: {exc}", err=True)
        click.echo("\nTo fix this, either:", err=True)
        click.echo("  1. Run as root (sudo)", err=True)
        click.echo("  2. Remove the --allow-other flag", err=True)
        click.echo("  3. Add 'user_allow_other' to /etc/fuse.conf", err=True)
        sys.exit(1)
    except Exception as exc:
        click.echo(f"Error mounting filesystem: {exc}", err=True)
        sys.exit(1)


@click.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("-r", "--replicas", type=int, help="Number of replicas (-1 for all)")
@click.option("-b", "--backends", help="Preferred backends (comma-separated)")
@click.option("--recomputable/--no-recomputable", default=None, help="Mark as recomputable")
@click.option(
    "--recompute-cmd",
    help="Command to recompute file (use $STORAGE_OUTPUT for output path)",
)
@click.option("--encrypt/--no-encrypt", default=None, help="Enable/disable encryption")
@click.option("--encrypt-backends", help="Backends to encrypt for (comma-separated)")
def configure(path, replicas, backends, recomputable, recompute_cmd, encrypt, encrypt_backends):
    """
    Configure storage settings for a file or directory

    Example:
        storage configure /mnt/storage/backups --replicas 3
        storage configure /mnt/storage/downloads --recomputable --recompute-cmd "wget ..."
    """
    path_obj = Path(path)
    config = get_file_config(path_obj) or FileConfig()

    if replicas is not None:
        config.replica_count = replicas

    if backends:
        config.preferred_backends = [backend.strip() for backend in backends.split(",")]

    if recomputable is not None:
        config.recomputable = recomputable

    if recompute_cmd is not None:
        config.recompute_cmd = recompute_cmd

    if encrypt is not None:
        config.encrypted = encrypt

    if encrypt_backends:
        config.encryption_backends = [backend.strip() for backend in encrypt_backends.split(",")]

    set_file_config(path_obj, config)

    click.echo(f"Updated configuration for {path}:")
    click.echo(f"  Replica count: {config.replica_count}")
    if config.preferred_backends:
        click.echo(f"  Preferred backends: {', '.join(config.preferred_backends)}")
    if config.encrypted:
        click.echo("  Encryption: enabled")
        if config.encryption_backends:
            click.echo(f"  Encrypt for backends: {', '.join(config.encryption_backends)}")
    if config.recomputable:
        click.echo("  Recomputable: yes")
        if config.recompute_cmd:
            click.echo(f"  Recompute command: {config.recompute_cmd}")
            click.echo("\nEnvironment variables available:")
            click.echo("  $STORAGE_PATH - Virtual path of the file")
            click.echo("  $STORAGE_CACHE_PATH - Local cache path")
            click.echo("  $STORAGE_OUTPUT - Where to write output (same as CACHE_PATH)")


@click.command()
@click.argument("path", type=click.Path(exists=True))
def info(path):
    """Show storage information for a file or directory"""
    path_obj = Path(path)
    config = get_inherited_config(path_obj)

    click.echo(f"Storage configuration for {path}:")
    click.echo(f"  Replica count: {config.replica_count}")

    if config.preferred_backends:
        click.echo(f"  Preferred backends: {', '.join(config.preferred_backends)}")

    if config.encrypted:
        click.echo("  Encrypted: yes")
        if config.encryption_backends:
            click.echo(f"  Encryption backends: {', '.join(config.encryption_backends)}")

    if config.recomputable:
        click.echo("  Recomputable: yes")
        if config.recompute_cmd:
            click.echo(f"  Recompute command: {config.recompute_cmd}")

    if config.custom:
        click.echo(f"  Custom config: {config.custom}")
