import sys

import pytest


def main() -> int:
    # Each py_test in BUILD passes its test modules as arguments (plus any
    # --test_arg flags). Refuse to run with none: pytest would then collect
    # whatever it finds under the runfiles directory.
    args = sys.argv[1:]
    if not any(arg.endswith(".py") for arg in args):
        print("pytest_main: no test modules given; list them in the py_test args", file=sys.stderr)
        return 2
    return pytest.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
