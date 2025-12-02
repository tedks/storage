import pytest


def main() -> int:
    return pytest.main(["tests"])


if __name__ == "__main__":
    raise SystemExit(main())
