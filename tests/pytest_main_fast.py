import pytest


def main() -> int:
    # Fast gate: everything except mounted system tests.
    return pytest.main([
        "tests",
        "--ignore=tests/test_streaming_system.py",
        "--ignore=tests/test_sync_system.py",
    ])


if __name__ == "__main__":
    raise SystemExit(main())
