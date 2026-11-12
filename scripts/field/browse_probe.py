#!/usr/bin/env python3
"""Bounded, read-only browse-latency probe: a Storage mount versus one member's GVFS SFTP mount.

Run on the client host, fed over ssh stdin so nothing is written there:

    ssh HOST python3 - ARGS < scripts/field/browse_probe.py > private.jsonl

It does metadata operations only: os.listdir (names), os.lstat of a bounded
set of entries both trees list, and `gio list -n` asking listing-level
attributes (never standard::content-type, which sniffs file contents). It
never opens a file, follows a symlink, writes, or sets an xattr.

Every operation runs in its own child process with a timeout, so a hung
FUSE or GVFS call costs one timeout, not the run. Output is JSON lines on
stdout and contains file names: keep it private and publish only
aggregates (browse_summary.py).

Bounds (defaults): 10 selected directories per subtree, depth <= 3 below
the subtree, <= 200 lstats per directory, directories with more than 10000
entries are measured for listing only, 30 s per operation, 18 min wall.

Selection lists the member tree through the GVFS POSIX bridge, which warms
that side before measurement; run 1 starts with GIO on the Storage side so
Storage's first observation is not preceded by any probe of that directory
in this run (it may still be warm from earlier use).
"""
import argparse
import json
import os
import stat
import subprocess
import sys
import time

GIO_ATTRS = ",".join([
    "standard::name", "standard::type", "standard::size", "standard::is-hidden",
    "standard::is-symlink", "standard::fast-content-type", "time::modified",
    "unix::mode", "access::can-read",
])

CHILD = r"""
import json, os, sys, time
op, path = sys.argv[1], sys.argv[2]
if op == "list":
    t = time.perf_counter(); names = os.listdir(path); dt = time.perf_counter() - t
    print(json.dumps({"seconds": dt, "names": names}))
elif op == "lstat":
    names = json.loads(sys.stdin.read())
    errors = 0; t = time.perf_counter()
    for n in names:
        try: os.lstat(os.path.join(path, n))
        except OSError: errors += 1
    print(json.dumps({"seconds": time.perf_counter() - t, "count": len(names), "errors": errors}))
elif op == "scan":
    # selection only: name, is-real-dir, mtime for up to N entries
    limit = int(sys.argv[3]); out = []; t = time.perf_counter()
    names = os.listdir(path)
    names_sorted = sorted(names); step = max(1, len(names_sorted) // limit)
    for n in names_sorted[::step][:limit]:
        try: st = os.lstat(os.path.join(path, n))
        except OSError: continue
        out.append([n, (st.st_mode & 0o170000) == 0o040000, st.st_mtime])
    print(json.dumps({"seconds": time.perf_counter() - t, "total": len(names), "entries": out}))
"""


def emit(**rec):
    rec["t"] = time.time()
    print(json.dumps(rec), flush=True)


def child(op, path, timeout, stdin=None, extra=()):
    cmd = [sys.executable, "-I", "-S", "-c", CHILD, op, path, *extra]
    try:
        p = subprocess.run(cmd, input=stdin, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"error": "timeout"}
    if p.returncode != 0:
        return {"error": (p.stderr.strip().splitlines() or ["rc %d" % p.returncode])[-1][:200]}
    return json.loads(p.stdout)


def gio_list(uri, timeout):
    t = time.perf_counter()
    try:
        p = subprocess.run(["gio", "list", "-n", "-h", "-a", GIO_ATTRS, uri],
                           capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"error": "timeout", "seconds": time.perf_counter() - t}
    dt = time.perf_counter() - t
    if p.returncode != 0:
        return {"error": p.stderr.strip()[:200], "seconds": dt}
    names = [line.split("\t", 1)[0] for line in p.stdout.splitlines() if line]
    return {"seconds": dt, "names": names}


def select(sub, member_root, args, deadline):
    """Up to max_dirs directories under sub (relative paths), chosen on the member tree."""
    chosen = [sub]
    top = child("scan", os.path.join(member_root, sub), args.timeout, extra=(str(args.max_stats),))
    emit(kind="select-scan", dir=sub, **top)
    if "error" in top:
        return chosen
    dirs = [(n, m) for n, isdir, m in top["entries"] if isdir]
    recent = [n for n, _ in sorted(dirs, key=lambda e: -e[1])]
    for n in recent[:4]:
        chosen.append(f"{sub}/{n}")
    # density: list a bounded sample of the remaining dirs, keep the densest
    rest = recent[4:]
    step = max(1, len(rest) // 8)
    sample = rest[::step][:8]
    counts = []
    for n in sample:
        if time.monotonic() > deadline:
            break
        r = child("list", os.path.join(member_root, sub, n), args.timeout)
        if "names" in r:
            counts.append((len(r["names"]), n))
    for _, n in sorted(counts, reverse=True)[:3]:
        chosen.append(f"{sub}/{n}")
    # depth: the most recent grandchild and great-grandchild of the most recent child
    cur = chosen[1] if len(chosen) > 1 else None
    for _ in range(2):
        if cur is None or len(chosen) >= args.max_dirs or time.monotonic() > deadline:
            break
        s = child("scan", os.path.join(member_root, cur), args.timeout, extra=(str(args.max_stats),))
        sub_dirs = sorted([(m, n) for n, isdir, m in s.get("entries", []) if isdir], reverse=True)
        if not sub_dirs:
            break
        cur = f"{cur}/{sub_dirs[0][1]}"
        chosen.append(cur)
    seen, out = set(), []
    for d in chosen:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out[: args.max_dirs]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--storage", required=True, help="Storage mountpoint")
    ap.add_argument("--member", required=True, help="member tree via GVFS POSIX bridge")
    ap.add_argument("--member-uri", required=True, help="member tree as a GIO URI")
    ap.add_argument("--subdirs", default="Videos,Music")
    ap.add_argument("--dirs", default="", help="explicit ;-separated relative dirs (skips selection)")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--max-dirs", type=int, default=10)
    ap.add_argument("--max-stats", type=int, default=200)
    ap.add_argument("--max-entries", type=int, default=10000)
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--budget", type=float, default=1080.0)
    args = ap.parse_args()
    args.runs = max(1, min(args.runs, 5))
    args.max_dirs = min(args.max_dirs, 10)
    args.max_stats = min(args.max_stats, 200)
    deadline = time.monotonic() + args.budget
    sides = {
        "storage": (args.storage, "file://" + args.storage.rstrip("/")),
        "member": (args.member, args.member_uri.rstrip("/")),
    }
    emit(kind="start", argv=sys.argv[1:], gio_attrs=GIO_ATTRS)

    if args.dirs:
        targets = [d for d in args.dirs.split(";") if d]
    else:
        targets = []
        for sub in args.subdirs.split(","):
            targets += select(sub, args.member, args, deadline)
    emit(kind="selected", dirs=targets)

    for d in targets:
        for run in range(1, args.runs + 1):
            if time.monotonic() > deadline:
                emit(kind="budget-exhausted", dir=d, run=run)
                return
            order = ["storage", "member"] if run % 2 else ["member", "storage"]
            names = {}
            for side in order:
                root, uri = sides[side]
                g = gio_list(f"{uri}/{d}", args.timeout)
                emit(kind="gio-list", dir=d, run=run, side=side, **g)
            for side in order:
                root, _ = sides[side]
                r = child("list", os.path.join(root, d), args.timeout)
                emit(kind="posix-list", dir=d, run=run, side=side, **r)
                names[side] = r.get("names")
            if names.get("storage") is None or names.get("member") is None:
                continue
            common = sorted(set(names["storage"]) & set(names["member"]))
            if max(len(names["storage"]), len(names["member"])) > args.max_entries:
                emit(kind="lstat-skipped", dir=d, run=run, reason="over max entries")
                continue
            # deterministic spread over the common names, capped
            step = max(1, len(common) // args.max_stats)
            pick = common[::step][: args.max_stats]
            for side in order:
                root, _ = sides[side]
                r = child("lstat", os.path.join(root, d), args.timeout, stdin=json.dumps(pick))
                emit(kind="posix-lstat", dir=d, run=run, side=side, **r)
    emit(kind="done")


if __name__ == "__main__":
    main()
