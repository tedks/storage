"""Fail when BUILD does not list, and run, every storage/ and test module.

BUILD lists sources explicitly instead of globbing, so a module nobody adds
there silently drops out: a test module never runs, a storage module is
missing from storage_lib. The build_srcs genquery records what BUILD really
lists (per target, as XML). This check compares that with the source tree,
which it reaches by resolving the MODULE.bazel runfile back to the
workspace; that is why its target is tagged "local".
"""

import collections
import os
import sys
import xml.etree.ElementTree as ET

LIBRARY = "//:storage_lib"

# Not Bazel targets (see AGENTS.md), so their test files are not checked.
UNCHECKED_TEST_DIRS = {"tests/benchmarks"}


def is_test_module(path):
    """pytest's default test file names: test_*.py and *_test.py."""
    name = os.path.basename(path)
    return name.endswith(".py") and (name.startswith("test_") or name.endswith("_test.py"))


def listed_rules(query_xml):
    """Map each rule label in the genquery output to its (srcs, args)."""
    rules = {}
    for rule in ET.parse(query_xml).getroot().iter("rule"):
        srcs, args = [], []
        for attr in rule.findall("list"):
            if attr.get("name") == "srcs":
                for label in attr.findall("label"):
                    value = label.get("value")
                    if not value.startswith("//:"):
                        raise ValueError(f"{rule.get('name')}: unexpected src label {value}")
                    srcs.append(value[len("//:"):])
            elif attr.get("name") == "args":
                args.extend(s.get("value") for s in attr.findall("string"))
        rules[rule.get("name")] = (srcs, args)
    return rules


def python_files(root, top, skip_dirs=()):
    """Relative paths of .py files under root/top, skipping hidden entries,
    __pycache__, and the given directories (relative to root)."""
    found = set()
    for dirpath, dirnames, filenames in os.walk(os.path.join(root, top)):
        rel_dir = os.path.relpath(dirpath, root)
        dirnames[:] = sorted(
            d
            for d in dirnames
            if not d.startswith(".")
            and d != "__pycache__"
            and os.path.join(rel_dir, d) not in skip_dirs
        )
        for name in filenames:
            # Hidden names include editor lock files such as Emacs's .#fs.py.
            if name.endswith(".py") and not name.startswith("."):
                found.add(os.path.join(rel_dir, name))
    return found


def problems(rules, tests_on_disk, storage_on_disk):
    if LIBRARY not in rules or len(rules) < 2:
        return [f"genquery output lists {sorted(rules)}; expected {LIBRARY} and the test targets"]
    found = []
    missing_lib = storage_on_disk - set(rules[LIBRARY][0])
    found += [f"{path}: not in {LIBRARY} srcs" for path in sorted(missing_lib)]

    owners = collections.defaultdict(list)
    for rule, (srcs, args) in sorted(rules.items()):
        if rule == LIBRARY:
            continue
        in_srcs = {path for path in srcs if is_test_module(path)}
        in_args = {arg.split("::")[0] for arg in args if arg.split("::")[0].endswith(".py")}
        for path in sorted(in_srcs - in_args):
            found.append(f"{path}: in {rule} srcs but not its args, so pytest never runs it")
        for path in sorted(in_args - in_srcs):
            found.append(f"{path}: in {rule} args but not its srcs")
        for path in in_srcs:
            owners[path].append(rule)
    found += [
        f"{path}: not in any py_test that the fast or mounted gate runs"
        for path in sorted(tests_on_disk - owners.keys())
    ]
    found += [
        f"{path}: listed by more than one py_test that the gates run: {', '.join(sorted(names))}"
        for path, names in sorted(owners.items())
        if len(names) > 1
    ]
    return found


def main():
    # BUILD passes the query output first. Anything after it is a --test_arg
    # meant for the pytest targets in the same invocation; ignore it.
    if len(sys.argv) < 2:
        print("usage: build_srcs_check.py BUILD_SRCS_XML", file=sys.stderr)
        return 2
    root = os.path.dirname(os.path.realpath("MODULE.bazel"))
    if not os.path.isdir(os.path.join(root, "storage")):
        print(f"build_srcs_check: MODULE.bazel resolved to {root}, which is not the source tree", file=sys.stderr)
        return 2
    tests_on_disk = {
        path
        for path in python_files(root, "tests", skip_dirs=UNCHECKED_TEST_DIRS)
        if is_test_module(path)
    }
    found = problems(listed_rules(sys.argv[1]), tests_on_disk, python_files(root, "storage"))
    for line in found:
        print(f"build_srcs_check: {line}", file=sys.stderr)
    if found:
        print("build_srcs_check: list these modules explicitly in BUILD (see AGENTS.md)", file=sys.stderr)
        return 1
    print(f"build_srcs_check: BUILD lists every module under {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
