import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

import click


def _scan_conflict_groups(directory: Path):
    """Return list of (base, {backend: path}) conflicts in directory."""
    groups = {}
    for entry in directory.iterdir():
        if entry.is_dir():
            continue
        parts = entry.name.split(".")
        if len(parts) < 2:
            continue
        backend = parts[-1]
        base = ".".join(parts[:-1])
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
    """Invoke an external 3-way merge tool and return merged bytes or None on error."""
    try:
        if tool == "git":
            cmd = ["git", "merge-file", "-p", str(local), str(ancestor), str(remote)]
        elif tool == "diff3":
            cmd = ["diff3", "-m", str(local), str(ancestor), str(remote)]
        else:
            raise ValueError(f"Unsupported merge tool {tool}")

        result = subprocess.run(cmd, capture_output=True)
        if result.returncode not in (0, 1):
            return None
        return result.stdout
    except Exception:
        return None


# Set on a directory of a storage mount to hash its names on several members
# again; read for {"pending", "verified", "unverified"} (StorageFS.VERIFY_XATTR).
VERIFY_XATTR = "user.storage.verify"


def _verify_directory(directory: Path, poll_interval: float = 0.5) -> dict:
    """Have the mount list directory and hash again every same-size copy of
    a name in it found on several members, and wait until none is pending.

    The status is read before anything is set, so a directory outside a
    storage mount gets no xattr written to it.
    """
    path = str(directory)
    try:
        json.loads(os.getxattr(path, VERIFY_XATTR))
    except (OSError, ValueError) as exc:
        raise click.ClickException(f"{directory} is not on a storage mount") from exc
    try:
        os.setxattr(path, VERIFY_XATTR, b"1")
    except OSError as exc:
        raise click.ClickException(f"Could not verify {directory}: {exc}") from exc
    while True:
        try:
            status = json.loads(os.getxattr(path, VERIFY_XATTR))
        except (OSError, ValueError) as exc:
            raise click.ClickException(f"Lost track of the verification of {directory}: {exc}") from exc
        if not status.get("pending"):
            return status
        time.sleep(poll_interval)


@click.command("conflicts")
@click.argument("directory", type=click.Path(exists=True, file_okay=False))
@click.option(
    "-f",
    "--file",
    "target_file",
    type=click.Path(),
    help="Path (within the directory) to a specific conflict file or its base name",
)
@click.option(
    "--take",
    "--use",
    "--resolve",
    "take_backend",
    help="Backend name to take for the specified conflict",
)
@click.option(
    "--merge",
    "--meld",
    "merge",
    is_flag=True,
    help="Attempt a 3-way merge for the specified conflict",
)
@click.option(
    "--verify",
    is_flag=True,
    help=(
        "Hash every copy of a name found on several members with the same size again "
        "first; otherwise a recorded hash is trusted while the member reports the same "
        "size and timestamps"
    ),
)
def conflicts(directory, target_file, take_backend, merge, verify):
    """
    List and resolve checksum conflicts in a mounted directory.

    When files differ across backends they appear as <name>.<backend>.
    Use --take/--use/--resolve BACKEND to pick one version everywhere,
    or --merge/--meld to attempt a trivial merge when possible.
    """
    dir_path = Path(directory)
    if verify:
        status = _verify_directory(dir_path)
        click.echo(
            f"Verified {status.get('verified', 0)} name(s) on several members; "
            f"{status.get('unverified', 0)} could not be verified."
        )
    conflict_groups = _scan_conflict_groups(dir_path)

    if not conflict_groups:
        click.echo("No conflicts detected.")
        return

    if take_backend and merge:
        click.echo("Choose either --take or --merge, not both.", err=True)
        sys.exit(2)

    if (take_backend or merge) and not target_file:
        raise click.UsageError("--file is required when using --take/--merge")

    if target_file:
        target_path = Path(target_file)
        base_name = target_path.name

        target = None
        for candidate in conflict_groups:
            base, files = candidate
            if base == base_name or target_path.name in [path.name for path in files.values()]:
                target = candidate
                break

        if not target and "." in base_name:
            stripped = ".".join(base_name.split(".")[:-1])
            target = next((candidate for candidate in conflict_groups if candidate[0] == stripped), None)

        if not target:
            raise click.ClickException(f"No conflict found for {base_name}")

        conflict_groups = [target]
    else:
        if not take_backend and not merge:
            click.echo(f"Found {len(conflict_groups)} conflict(s):")
            for base, files in conflict_groups:
                backends = ", ".join(sorted(files.keys()))
                click.echo(f"  {base}: {backends}")
            click.echo(
                "\nUse --file <path> with --take <backend> or --merge to resolve a specific conflict."
            )
            return
        raise click.UsageError("--file is required when resolving conflicts")

    for base, files in conflict_groups:
        base_path = dir_path / base

        if take_backend:
            if take_backend not in files:
                click.echo(f"Skipping {base}: no version from {take_backend}")
                continue
            selected = files[take_backend].read_bytes()
            base_path.write_bytes(selected)
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

            if base_path.exists():
                ancestor_path = base_path
            else:
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
