import pytest


def main() -> int:
    # Mounted filesystem integration gate.
    return pytest.main([
        "tests/test_streaming_system.py",
        "tests/test_sync_system.py",
    ])


if __name__ == "__main__":
    raise SystemExit(main())
