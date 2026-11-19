"""Operator commands for a running Storage mount."""

import json
from dataclasses import dataclass
from pathlib import Path

import click

from .control import ControlError, REQUEST_TIMEOUT, request_control_payload
from .mount_profiles import resolve_member_configuration


@dataclass(frozen=True)
class ControlTarget:
    cache: Path | None
    profile: str | None
    config_path: Path | None
    timeout: float
    json_output: bool

    def _resolve_cache(self) -> Path:
        if bool(self.cache) == bool(self.profile):
            raise click.UsageError("Specify one of --cache or --profile")
        if self.config_path is not None and not self.profile:
            raise click.UsageError("--config requires --profile")
        if self.profile:
            return resolve_member_configuration(self.profile, self.config_path, (), None, None).cache_root
        return self.cache.resolve()

    def request(self, command: str, **arguments) -> dict:
        request = json.dumps({"command": command, **arguments}).encode("ascii")
        try:
            payload = request_control_payload(self._resolve_cache(), request, self.timeout)
        except ControlError as exc:
            raise click.ClickException(str(exc)) from exc
        if self.json_output:
            click.echo(json.dumps(payload, sort_keys=True, indent=2))
        return payload


@click.group(invoke_without_command=True, no_args_is_help=True)
@click.option("-c", "--cache", type=click.Path(file_okay=False, path_type=Path),
              help="Cache directory used by the running mount.")
@click.option("--profile", help="Name of a TOML mount profile, used to find its cache.")
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False, path_type=Path),
              help="TOML file containing --profile. Defaults to the usual mount config files.")
@click.option("--timeout", default=REQUEST_TIMEOUT, show_default=True,
              help="Control request timeout in seconds.")
@click.option("--json-output", is_flag=True, help="Print the command's JSON response.")
@click.pass_context
def control(ctx, cache, profile, config_path, timeout, json_output):
    """Inspect and control a running mount. Defaults to status."""
    ctx.obj = ControlTarget(cache, profile, config_path, timeout, json_output)
    if ctx.invoked_subcommand is None:
        ctx.invoke(control_status)


def _print_parameters(payload: dict):
    click.echo("Live streaming settings")
    for setting, value in payload["limits"].items():
        display = "auto" if setting == "max_download_workers" and value is None else json.dumps(value, sort_keys=True)
        click.echo(f"  {setting} = {display}")
    click.echo("Streaming settings that require a restart")
    for setting, value in payload["restart_only"].items():
        click.echo(f"  {setting} = {json.dumps(value, sort_keys=True)}")


@control.command("status")
@click.pass_obj
def control_status(target):
    """Show aggregate download activity and effective streaming settings."""
    payload = target.request("status")
    if target.json_output:
        return
    stats = payload["stats"]
    click.echo(f"Mount PID {stats['pid']}")
    click.echo(f"  Streaming files with readers: {stats['active_reader_sessions']}")
    click.echo(f"  Retained downloads: {stats['retained_streaming_sessions']}")
    click.echo(f"  Download workers running: {stats['download_workers_running']}/{stats['download_workers']}")
    click.echo(f"  Queued downloads: {stats['download_sessions_queued']}")
    _print_parameters(payload)


@control.command("get")
@click.pass_obj
def control_get(target):
    """Show effective streaming settings."""
    payload = target.request("streaming.get")
    if not target.json_output:
        _print_parameters(payload)


@control.command("set")
@click.option("--max-download-workers", metavar="COUNT|auto",
              help="Background worker throttle, or auto to use runnable demand.")
@click.option("--streaming-keep-after-close",
              help="Read span that qualifies for retention, e.g., all:32M,30%.")
@click.option("--streaming-read-ahead-window",
              help="Speculative bytes before qualification, e.g., 4M or 0.")
@click.pass_obj
def control_set(target, **limits):
    """Apply live streaming limits together. Changes last until unmount."""
    changes = {key: value for key, value in limits.items() if value is not None}
    if not changes:
        raise click.UsageError("Provide at least one live streaming setting to set")
    if "max_download_workers" in changes:
        limit = changes["max_download_workers"]
        try:
            changes["max_download_workers"] = (None if limit == "auto" else
                                               click.IntRange(min=1).convert(limit, None, None))
        except click.BadParameter as exc:
            raise click.BadParameter(exc.message, param_hint="--max-download-workers") from exc
    payload = target.request("streaming.set", limits=changes)
    if not target.json_output:
        _print_parameters(payload)


@control.command("downloads")
@click.pass_obj
def control_downloads(target):
    """List downloads with their session IDs, paths, and progress."""
    payload = target.request("downloads.list")
    if target.json_output:
        return
    downloads = payload["downloads"]
    if not downloads:
        click.echo("No downloads")
        return
    for row in downloads:
        click.echo(
            f"{row['id']}  {row['status']}  {row['readers']} readers  "
            f"{row['downloaded_bytes']}/{row['size_bytes']} bytes  {row['path']!r}"
        )


@control.command("cancel")
@click.argument("session_id", metavar="DOWNLOAD_ID")
@click.pass_obj
def control_cancel(target, session_id):
    """Cancel a reader-free background download by its exact session ID."""
    payload = target.request("downloads.cancel", id=session_id)
    outcome = payload["result"]
    if not target.json_output:
        if outcome == "cancelled":
            click.echo(f"Cancelled download {session_id}")
        elif outcome == "busy":
            click.echo(f"Download {session_id} is busy. Close its readers or wait for finalization.", err=True)
        elif outcome == "not-found":
            click.echo(f"Download {session_id} is no longer present. Run 'downloads' for current IDs.", err=True)
        else:
            raise click.ClickException("Unexpected cancellation result")
    if outcome != "cancelled":
        raise click.exceptions.Exit(1)
