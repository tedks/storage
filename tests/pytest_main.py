import sys

import pytest


def main() -> int:
    # A py_test's runfiles hold only its own srcs, so "tests" collects exactly
    # the test modules BUILD lists for that target. --test_arg flags follow.
    return pytest.main(["tests", *sys.argv[1:]])


if __name__ == "__main__":
    raise SystemExit(main())
