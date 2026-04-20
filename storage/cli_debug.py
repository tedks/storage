import json
import socket
from pathlib import Path

import click

from .index_audit import rebuild_index_from_cache, verify_index_against_cache
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


@debug.command("rebuild-index")
@click.option(
    "-c",
    "--cache",
    required=True,
    type=click.Path(file_okay=False, path_type=Path),
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
    type=click.Path(file_okay=False, path_type=Path),
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
