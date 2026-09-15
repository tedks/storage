"""Fail when BUILD does not list every storage/ and tests/test_*.py module.

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


def listed_srcs(query_xml):
    """Map each rule label in the genquery output to its srcs paths."""
    srcs = {}
    for rule in ET.parse(query_xml).getroot().iter("rule"):
        paths = []
        for attr in rule.findall("list"):
            if attr.get("name") == "srcs":
                for label in attr.iter("label"):
                    value = label.get("value")
                    if not value.startswith("//:"):
                        raise ValueError(f"{rule.get('name')}: unexpected src label {value}")
                    paths.append(value[len("//:"):])
        srcs[rule.get("name")] = paths
    return srcs


def source_tree_modules(root):
    """Return (test modules, storage modules) found on disk under root."""
    tests_dir = os.path.join(root, "tests")
    tests = {
        f"tests/{name}"
        for name in os.listdir(tests_dir)
        if name.startswith("test_") and name.endswith(".py")
    }
    storage = set()
    for dirpath, dirnames, filenames in os.walk(os.path.join(root, "storage")):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        for name in filenames:
            if name.endswith(".py"):
                storage.add(os.path.relpath(os.path.join(dirpath, name), root))
    return tests, storage


def problems(srcs, tests_on_disk, storage_on_disk):
    if LIBRARY not in srcs or len(srcs) < 2:
        return [f"genquery output lists {sorted(srcs)}; expected {LIBRARY} and the test targets"]
    found = []
    missing_lib = storage_on_disk - set(srcs[LIBRARY])
    found += [f"{path}: not in {LIBRARY} srcs" for path in sorted(missing_lib)]

    owners = collections.defaultdict(list)
    for rule, paths in srcs.items():
        if rule == LIBRARY:
            continue
        for path in paths:
            if path.startswith("tests/test_"):
                owners[path].append(rule)
    found += [
        f"{path}: not in any py_test that the fast or mounted gate runs"
        for path in sorted(tests_on_disk - owners.keys())
    ]
    found += [
        f"{path}: listed by more than one py_test: {', '.join(sorted(rules))}"
        for path, rules in sorted(owners.items())
        if len(rules) > 1
    ]
    return found


def main():
    if len(sys.argv) != 2:
        print("usage: build_srcs_check.py BUILD_SRCS_XML", file=sys.stderr)
        return 2
    root = os.path.dirname(os.path.realpath("MODULE.bazel"))
    if not os.path.isdir(os.path.join(root, "storage")):
        print(f"build_srcs_check: MODULE.bazel resolved to {root}, which is not the source tree", file=sys.stderr)
        return 2
    found = problems(listed_srcs(sys.argv[1]), *source_tree_modules(root))
    for line in found:
        print(f"build_srcs_check: {line}", file=sys.stderr)
    if found:
        print("build_srcs_check: list these modules explicitly in BUILD (see AGENTS.md)", file=sys.stderr)
        return 1
    print(f"build_srcs_check: BUILD lists every module under {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
