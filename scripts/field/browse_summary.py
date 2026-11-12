#!/usr/bin/env python3
"""Aggregate browse_probe.py output into an anonymized summary (no names).

    python3 browse_summary.py private.jsonl [--labels private-labels.json] [--member-label remote2]

Directories are labelled by subtree and order of selection (Videos#0 is
the subtree root, Music#3 the fourth selected under Music). --labels
writes the label -> real path map to a private file.

For each directory and run it reports entry counts and seconds per
operation and side. Names the member lists that the Storage listing lacks
are classified, so absence is only claimed after the other explanations:
  conflict  - Storage lists name.<member label> (a conflict view split)
  hidden    - name matches Storage's hidden-entry rules
  absent    - none of the above
Storage-only names are counted: Storage lists the union of its members, so
it is expected to list names this one member lacks.
"""
import argparse
import json
from collections import defaultdict

PARTIAL_SUFFIX = ".storage.partial"
HIDDEN_NAMES = {".storage.rsync-tmp", ".storage.control.sock"}
HIDDEN_PREFIXES = (".storage.mount.",)


def hidden(name):
    return name in HIDDEN_NAMES or name.endswith(PARTIAL_SUFFIX) or name.startswith(HIDDEN_PREFIXES)


def classify(member_names, storage_names, member_labels):
    s = set(storage_names)
    out = defaultdict(int)
    absent = []
    for n in set(member_names) - s:
        if any(f"{n}.{lab}" in s for lab in member_labels):
            out["conflict"] += 1
        elif hidden(n):
            out["hidden"] += 1
        else:
            out["absent"] += 1
            absent.append(n)
    # canonical names present only through their conflict-split variants are
    # accounted above; names listed by Storage but not the member:
    out["storage_only"] = len(s - set(member_names))
    return dict(out), absent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("jsonl")
    ap.add_argument("--labels")
    ap.add_argument("--absent-out", help="private file for absent names by directory")
    ap.add_argument("--member-labels", default="remote0,remote1,remote2")
    args = ap.parse_args()
    member_labels = args.member_labels.split(",")
    recs = [json.loads(l) for l in open(args.jsonl) if l.strip()]
    selected = next((r["dirs"] for r in recs if r["kind"] == "selected"), [])
    label, counter = {}, defaultdict(int)
    for d in selected:
        sub = d.split("/", 1)[0]
        label[d] = f"{sub}#{counter[sub]}(depth {d.count('/')})"
        counter[sub] += 1
    if args.labels:
        json.dump(label, open(args.labels, "w"), indent=1)

    table = defaultdict(dict)  # (dir, run) -> {(op, side): rec}
    for r in recs:
        if r["kind"] in ("gio-list", "posix-list", "posix-lstat"):
            table[(r["dir"], r["run"])][(r["kind"], r["side"])] = r
    absent_all = {}
    rows = []
    for d in selected:
        runs = sorted(run for (dd, run) in table if dd == d)
        for run in runs:
            t = table[(d, run)]
            row = {"dir": label[d], "run": run}
            for op in ("gio-list", "posix-list", "posix-lstat"):
                for side in ("storage", "member"):
                    rec = t.get((op, side), {})
                    key = f"{op}:{side}"
                    if "error" in rec:
                        row[key] = {"error": rec["error"][:60] if rec["error"] == "timeout" else "error"}
                        continue
                    n = len(rec["names"]) if "names" in rec else rec.get("count")
                    row[key] = {"n": n, "ms": round(rec.get("seconds", 0) * 1000, 1)}
                    if op == "posix-lstat":
                        row[key]["errors"] = rec.get("errors")
            for op in ("gio-list", "posix-list"):
                sn = t.get((op, "storage"), {}).get("names")
                mn = t.get((op, "member"), {}).get("names")
                if sn is not None and mn is not None:
                    cls, absent = classify(mn, sn, member_labels)
                    row[f"{op}:diff"] = cls
                    if absent:
                        absent_all.setdefault(d, {}).setdefault(f"{op}:run{run}", sorted(absent))
            rows.append(row)
    if args.absent_out:
        json.dump(absent_all, open(args.absent_out, "w"), indent=1)
    for row in rows:
        print(json.dumps(row))


if __name__ == "__main__":
    main()
