import pytest


def main() -> int:
    # Fast gate: everything except mounted system tests.
    return pytest.main([
        "tests",
        "-k",
        "not test_streaming_system and not test_sync_system",
    ])


if __name__ == "__main__":
    raise SystemExit(main())
