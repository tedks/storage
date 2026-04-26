import json
import socket
from pathlib import Path

import click

from .index_audit import (
    rebuild_index_from_cache,
    reprobe_member_locations,
    verify_index_against_cache,
)
from .mount_profiles import create_storage_members, resolve_member_configuration
from .state import StateManager


@click.group()
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
@click.option(
    "-c",
    "--cache",
    required=True,
    type=click.Path(),
    help="Cache directory used by the mount",
)
@click.option(
    "--timeout",
    default=2.0,
    type=float,
    show_default=True,
    help="Control socket timeout in seconds",
)
@click.option("--json-output", is_flag=True, help="Print raw JSON stats")
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


def _with_state_manager(database: Path) -> StateManager:
    database.parent.mkdir(parents=True, exist_ok=True)
    return StateManager(str(database))


def _close_storage_members(backends):
    for backend in backends.values():
        close = getattr(backend, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass


@debug.command("rebuild-index")
@click.option(
    "-c",
    "--cache",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Cache directory to scan",
)
@click.option(
    "-d",
    "--database",
    required=True,
    type=click.Path(path_type=Path),
    help="SQLite database path to rebuild",
)
@click.option("--json-output", is_flag=True, help="Print raw JSON report")
def debug_rebuild_index(cache: Path, database: Path, json_output: bool):
    """Rebuild the local SQLite index from cache files and cache metadata xattrs."""
    state_manager = _with_state_manager(database.resolve())
    try:
        report = rebuild_index_from_cache(cache.resolve(), state_manager)
    finally:
        state_manager.close()

    payload = report.to_dict()
    if json_output:
        click.echo(json.dumps(payload, sort_keys=True, indent=2))
        return

    click.echo(f"Rebuilt index at {database}:")
    click.echo(f"  Scanned files: {payload['scanned_files']}")
    click.echo(f"  Rebuilt files: {payload['rebuilt_files']}")
    click.echo(f"  Inferred metadata files: {payload['inferred_metadata_files']}")
    click.echo(f"  Normalized xattrs: {payload['normalized_xattrs']}")
    click.echo(f"  Skipped files: {payload['skipped_files']}")
    click.echo(f"  Issues: {payload['issue_count']}")


@debug.command("verify-index")
@click.option(
    "-c",
    "--cache",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Cache directory to scan",
)
@click.option(
    "-d",
    "--database",
    required=True,
    type=click.Path(path_type=Path),
    help="SQLite database path to verify",
)
@click.option("--json-output", is_flag=True, help="Print raw JSON report")
def debug_verify_index(cache: Path, database: Path, json_output: bool):
    """Verify cache files, cache metadata xattrs, and SQLite index rows agree."""
    state_manager = _with_state_manager(database.resolve())
    try:
        report = verify_index_against_cache(cache.resolve(), state_manager)
    finally:
        state_manager.close()

    payload = report.to_dict()
    if json_output:
        click.echo(json.dumps(payload, sort_keys=True, indent=2))
    else:
        click.echo(f"Verified index at {database}:")
        click.echo(f"  Scanned files: {payload['scanned_files']}")
        click.echo(f"  Issues: {payload['issue_count']}")

    if payload["issue_count"] > 0:
        raise click.exceptions.Exit(1)


@debug.command("reprobe-locations")
@click.argument("profile_name", required=False)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Load storage members from a named mount profile in a TOML config file.",
)
@click.option(
    "-m",
    "--member",
    "member_specs",
    multiple=True,
    help="Storage member root or URI (local path, ssh://user@host:/path, rsync://user@host:/path).",
)
@click.option(
    "-k",
    "--key-file",
    help="SSH private key file used for SSH/rsync storage members.",
)
@click.option(
    "-d",
    "--database",
    required=True,
    type=click.Path(path_type=Path),
    help="SQLite database path to update",
)
@click.option("--json-output", is_flag=True, help="Print raw JSON report")
def debug_reprobe_locations(
    profile_name: str | None,
    config_path: Path | None,
    member_specs: tuple[str, ...],
    key_file: str | None,
    database: Path,
    json_output: bool,
):
    """Probe storage members to rebuild non-cache location rows for known files."""
    resolved = resolve_member_configuration(profile_name, config_path, member_specs, key_file)
    backends = create_storage_members(resolved.member_specs, resolved.key_file, announce=not json_output)

    state_manager = _with_state_manager(database.resolve())
    try:
        report = reprobe_member_locations(state_manager, backends)
    finally:
        state_manager.close()
        _close_storage_members(backends)

    payload = report.to_dict()
    if json_output:
        click.echo(json.dumps(payload, sort_keys=True, indent=2))
        return

    if resolved.profile_name is not None:
        click.echo(f"Loaded mount profile: {resolved.profile_name} ({resolved.profile_source})")
    click.echo(f"Reprobed storage members into {database}:")
    click.echo(f"  Files considered: {payload['files_considered']}")
    click.echo(f"  Members requested: {payload['members_requested']}")
    click.echo(f"  Members reprobed: {payload['members_reprobed']}")
    click.echo(f"  Members unavailable: {payload['members_unavailable']}")
    click.echo(f"  Remote locations found: {payload['remote_locations_found']}")
    click.echo(f"  Issues: {payload['issue_count']}")
