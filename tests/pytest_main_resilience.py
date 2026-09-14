import pytest


def main() -> int:
    # Resilience-focused gate (run locally; see AGENTS.md "Build and test").
    return pytest.main([
        "tests/test_resilience_gradient.py",
        "tests/test_streaming_system.py",
    ])


if __name__ == "__main__":
    raise SystemExit(main())
