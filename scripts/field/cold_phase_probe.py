#!/usr/bin/env python3
"""Split one directory visit into phases: lookup, readdir, per-entry lstat, per-entry access.

Fed over ssh stdin like browse_probe.py (nothing is written on the host):

    ssh HOST python3 - --storage S --member M --dirs 'a/b;c/d' < cold_phase_probe.py

For each directory, in order: Storage (first observation in this probe),
member (GVFS POSIX bridge), Storage again (warm). Each visit runs in its
own child with a timeout. Metadata only; at most 200 entries are statted.
Output contains names only in the private per-entry error field (never
emitted); records carry counts and seconds.
"""
import argparse
import json
import subprocess
import sys
import time

CHILD = r"""
import json, os, sys, time
d = sys.argv[1]; cap = int(sys.argv[2]); out = {}
t = time.perf_counter(); os.lstat(d); out["lookup_ms"] = (time.perf_counter() - t) * 1000
t = time.perf_counter(); names = os.listdir(d); out["readdir_ms"] = (time.perf_counter() - t) * 1000
out["entries"] = len(names)
pick = sorted(names)[:cap]; out["statted"] = len(pick)
t = time.perf_counter(); errs = 0
for n in pick:
    try: os.lstat(os.path.join(d, n))
    except OSError: errs += 1
out["lstat_ms"] = (time.perf_counter() - t) * 1000; out["lstat_errors"] = errs
t = time.perf_counter()
for n in pick: os.access(os.path.join(d, n), os.R_OK)
out["access_ms"] = (time.perf_counter() - t) * 1000
print(json.dumps(out))
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--storage", required=True)
    ap.add_argument("--member", required=True)
    ap.add_argument("--dirs", required=True)
    ap.add_argument("--cap", type=int, default=200)
    ap.add_argument("--timeout", type=float, default=30.0)
    a = ap.parse_args()
    for i, d in enumerate(x for x in a.dirs.split(";") if x):
        for visit, root in (("storage-first", a.storage), ("member", a.member), ("storage-warm", a.storage)):
            cmd = [sys.executable, "-I", "-S", "-c", CHILD, f"{root}/{d}", str(min(a.cap, 200))]
            try:
                p = subprocess.run(cmd, capture_output=True, text=True, timeout=a.timeout)
                rec = json.loads(p.stdout) if p.returncode == 0 else {"error": "child failed"}
            except subprocess.TimeoutExpired:
                rec = {"error": "timeout"}
            print(json.dumps({"dir_index": i, "visit": visit, "t": time.time(), **rec}), flush=True)


if __name__ == "__main__":
    main()
