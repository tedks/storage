#!/usr/bin/env python3
"""Bounded task delivery for an interactive Codex fork (no TUI task paste)."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import uuid


def task_text(path):
    raw = Path(path).read_bytes()
    if not raw or len(raw) >= 32768 or b"\0" in raw:
        raise ValueError("fork task must be nonempty UTF-8, without NUL, and under 32768 bytes")
    return raw.decode("utf-8")


def child_id(path, parent):
    with Path(path).open("rb") as stream:
        line = stream.readline(1024 * 1024)
    if not line.endswith(b"\n"):
        raise ValueError("child session metadata is incomplete or too large")
    record = json.loads(line)
    payload = record["payload"]
    child = str(uuid.UUID(payload["id"]))
    parent = str(uuid.UUID(parent))
    if (record["type"] != "session_meta" or child == parent
            or str(uuid.UUID(payload["forked_from_id"])) != parent):
        raise ValueError("rollout is not a distinct fork of the requested parent")
    return child


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    preflight = sub.add_parser("preflight")
    preflight.add_argument("executable")
    preflight.add_argument("task")
    prepare = sub.add_parser("compact")
    prepare.add_argument("policy")
    prepare.add_argument("task")
    prepare.add_argument("output")
    identity = sub.add_parser("child")
    identity.add_argument("rollout")
    identity.add_argument("parent")
    queue = sub.add_parser("queue")
    queue.add_argument("executable")
    queue.add_argument("rollout")
    queue.add_argument("parent")
    queue.add_argument("task")
    args = parser.parse_args()
    if args.action == "preflight":
        task_text(args.task)
        result = subprocess.run([args.executable, "queue", "--help"],
                                capture_output=True, text=True, timeout=15)
        if result.returncode or not all(flag in result.stdout for flag in ("--thread", "--message")):
            raise ValueError("this Codex executable does not support native queue --thread/--message; no fork launched")
    elif args.action == "compact":
        policy = Path(args.policy).read_text(encoding="utf-8")
        task = task_text(args.task)
        Path(args.output).write_text(
            policy + "\n\nThe next assignment follows. Use it to decide what inherited context to preserve. "
            "Summarize context for this assignment; do not execute it during compaction. "
            "It will be delivered as a new user message after compaction.\n\n" + task,
            encoding="utf-8")
    elif args.action == "child":
        print(child_id(args.rollout, args.parent))
    else:
        child = child_id(args.rollout, args.parent)
        task = task_text(args.task)
        # Native queue currently accepts argv, not stdin. Pass one exact UTF-8
        # argument, including trailing newlines, without shell interpolation.
        result = subprocess.run([args.executable, "queue", "--thread", child, "--message=" + task],
                                capture_output=True, timeout=15)
        if result.returncode:
            raise ValueError("Codex queue rejected task delivery; inspect the child before retrying (no automatic paste fallback)")
        print(child)


if __name__ == "__main__":
    try:
        main()
    except subprocess.TimeoutExpired:
        # TimeoutExpired's string includes argv, which would echo the task.
        print("Error: Codex command timed out; inspect the existing child before retrying task delivery.", file=sys.stderr)
        sys.exit(1)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
