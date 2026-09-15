import sys

import pytest


def main() -> int:
    # Each py_test in BUILD passes its test modules first, then any
    # --test_arg flags. Refuse to run unless the first argument is a module
    # path (optionally a node id): with none, pytest would collect whatever
    # it finds under the runfiles directory.
    args = sys.argv[1:]
    if not args or args[0].startswith("-") or not args[0].split("::")[0].endswith(".py"):
        print("pytest_main: the first argument must be a test module; list them in the py_test args", file=sys.stderr)
        return 4
    return pytest.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
