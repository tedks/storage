import pytest


def main() -> int:
    # Nightly resilience-focused gate.
    return pytest.main([
        "tests/test_resilience_gradient.py",
        "tests/test_streaming_system.py",
    ])


if __name__ == "__main__":
    raise SystemExit(main())
