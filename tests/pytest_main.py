import sys

import pytest


def main() -> int:
    # Per-module Bazel targets pass one test file. Aggregate Python targets
    # may omit it and collect the modules in their runfiles. --test_arg flags
    # follow either form.
    args = sys.argv[1:]
    test_path = args.pop(0) if args and args[0].startswith("tests/test_") else "tests"
    return pytest.main([test_path, *args])


if __name__ == "__main__":
    raise SystemExit(main())
