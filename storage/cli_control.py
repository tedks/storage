"""Operator commands for a running Storage mount."""

import json
import os
from dataclasses import dataclass
from pathlib import Path

import click

from .control import (
    ControlError, REQUEST_TIMEOUT, canonical_mountpoint, open_cache_directory, request_control_payload,
)
from .mount_profiles import resolve_control_profile


@dataclass(frozen=True)
class ControlTarget:
    cache: Path | None
    profile: str | None
    config_path: Path | None
    timeout: float
    json_output: bool

    def _resolve(self) -> tuple[Path, str | None]:
        """The cache to reach and, with --profile, the mountpoint it must answer for."""
        if bool(self.cache) == bool(self.profile):
            raise click.UsageError("Specify one of --cache or --profile")
        if self.config_path is not None and not self.profile:
            raise click.UsageError("--config requires --profile")
        if self.profile:
            cache, mountpoint = resolve_control_profile(self.profile, self.config_path)
            try:
                return cache, canonical_mountpoint(mountpoint)
            except ControlError as exc:
                raise click.ClickException(str(exc)) from exc
        return self.cache.resolve(), None

    @staticmethod
    def _open_cache(cache: Path) -> int:
        # The cache directory, not the mount, so following symlinks is safe.
        try:
            return open_cache_directory(cache)
        except OSError as exc:
            raise click.ClickException(f"Cannot open cache directory {str(cache)!r}: {exc.strerror}") from exc

    def _send(self, cache: Path, cache_fd: int, request: dict) -> dict:
        try:
            return request_control_payload(cache, json.dumps(request).encode("ascii"), self.timeout,
                                           cache_fd=cache_fd)
        except ControlError as exc:
            raise click.ClickException(str(exc)) from exc

    @staticmethod
    def _require_mount(payload: dict, mountpoint: str | None, cache: list[int]):
        mount = payload.get("mount")
        if not isinstance(mount, dict) or not isinstance(mount.get("mountpoint"), (str, type(None))):
            raise click.ClickException("The control socket did not report which mount it belongs to")
        # A socket moved here from another cache still reports its own.
        if mount.get("cache") != cache:
            raise click.ClickException(
                "The control socket in this cache was published in a different cache directory"
            )
        if mountpoint is None:
            return
        # Two mounts can share a cache; only one of them owns its socket.
        reported = mount["mountpoint"]
        if not isinstance(reported, str):
            raise click.ClickException(
                "The mount reached through this profile's cache reports no mountpoint, so it "
                "cannot be matched to the profile; use --cache to reach it"
            )
        if reported != mountpoint:
            raise click.ClickException(
                f"The control socket in this profile's cache belongs to the mount at {reported!r}, "
                f"not this profile's mountpoint {mountpoint!r}"
            )

    def request(self, command: str, *, mutation: bool = False, **arguments) -> dict:
        cache, mountpoint = self._resolve()
        # One open of the cache directory gives both the identity sent with
        # every request and the directory every connect goes through.
        cache_fd = self._open_cache(cache)
        try:
            entry = os.fstat(cache_fd)
            # Every request names the cache directory (and, with --profile,
            # the mountpoint) it means; the mount refuses one naming another.
            identity = {"cache": [entry.st_dev, entry.st_ino]}
            if mountpoint is not None:
                identity["mountpoint"] = mountpoint
            if mutation:
                # Confirm the mount before changing anything, even if the
                # server were to ignore the identity sent with the change.
                self._require_mount(self._send(cache, cache_fd, {"command": "status", **identity}),
                                    mountpoint, identity["cache"])
            payload = self._send(cache, cache_fd, {"command": command, **arguments, **identity})
        finally:
            os.close(cache_fd)
        if command == "status":
            self._require_mount(payload, mountpoint, identity["cache"])
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
    click.echo("Live streaming limits")
    for setting, value in payload["limits"].items():
        click.echo(f"  {setting} = {value}")
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
    # Quoted like download paths, so control characters cannot forge lines.
    mountpoint = payload["mount"]["mountpoint"]
    click.echo("  Mountpoint: " + ("(not reported; reach it with --cache)" if mountpoint is None
                                   else repr(mountpoint)))
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
@click.option("--max-streaming-sessions", type=int,
              help="Concurrent streaming files with readers (1–1024).")
@click.option("--max-retained-streaming-sessions", type=int,
              help="Retained downloads after the last read handle closes (0–1024).")
@click.option("--max-download-workers", type=int,
              help="Background download worker ceiling (1–32).")
@click.pass_obj
def control_set(target, **limits):
    """Apply live streaming limits together. Changes last until unmount."""
    changes = {key: value for key, value in limits.items() if value is not None}
    if not changes:
        raise click.UsageError("Provide at least one live streaming limit to set")
    payload = target.request("streaming.set", mutation=True, limits=changes)
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


BUSY_REASONS = {
    "readers": "it has open readers. Close its readers and retry.",
    "finalizing": "all of it has arrived and it is being finalized; it cannot be cancelled now.",
    "stopping": "finishing a failed or cancelled download; retry shortly.",
    None: "close its readers or wait for finalization, then retry.",
}
# 1 is any error and 2 a usage error, as for other commands.
CANCEL_EXIT_CODES = {"busy": 3, "not-found": 4}


@control.command("cancel")
@click.argument("session_id", metavar="DOWNLOAD_ID")
@click.pass_obj
def control_cancel(target, session_id):
    """Cancel a reader-free background download by its exact session ID."""
    payload = target.request("downloads.cancel", mutation=True, id=session_id)
    outcome = payload["result"]
    if not target.json_output:
        if outcome == "cancelled":
            click.echo(f"Cancelled download {session_id}")
        elif outcome == "cleanup-failed":
            reason = payload.get("reason") or "could not remove its partial cache file"
            click.echo(f"Stopped download {session_id}, but {reason}", err=True)
        elif outcome == "busy":
            reason = BUSY_REASONS.get(payload.get("reason"), BUSY_REASONS[None])
            click.echo(f"Download {session_id} is busy: {reason}", err=True)
        elif outcome == "not-found":
            click.echo(f"Download {session_id} is no longer present. Run 'downloads' for current IDs.", err=True)
        else:
            raise click.ClickException("Unexpected cancellation result")
    if outcome != "cancelled":
        raise click.exceptions.Exit(CANCEL_EXIT_CODES.get(outcome, 1))
