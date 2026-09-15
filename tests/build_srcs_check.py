"""Fail if a tests/test_*.py or storage/ module is not listed in BUILD.

BUILD lists sources instead of globbing them, so a new module has to be
added by hand; a forgotten test module would otherwise never run. The first
argument is the :build_srcs query output, one label per line.
"""

import os
import sys


def main() -> int:
    with open(sys.argv[1]) as query_output:
        listed = {line.strip().removeprefix("//:") for line in query_output if line.strip()}

    # MODULE.bazel is a runfile symlinked from the source tree.
    root = os.path.dirname(os.path.realpath("MODULE.bazel"))
    on_disk = {
        f"tests/{name}"
        for name in os.listdir(os.path.join(root, "tests"))
        if name.startswith("test_") and name.endswith(".py")
    }
    for dirpath, _, names in os.walk(os.path.join(root, "storage")):
        on_disk.update(
            os.path.relpath(os.path.join(dirpath, name), root)
            for name in names
            # Skip hidden files such as Emacs lock files (.#fs.py).
            if name.endswith(".py") and not name.startswith(".")
        )

    missing = sorted(on_disk - listed)
    for path in missing:
        print(f"{path} is not in a gate's py_test or storage_lib in BUILD", file=sys.stderr)
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
