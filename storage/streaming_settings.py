"""Size and policy values shared by mount profiles and live controls."""

import re
from fractions import Fraction

from .models import StreamingKeepAfterClose

DEFAULT_STREAMING_KEEP_AFTER_CLOSE = "all:32M,30%"
DEFAULT_STREAMING_READ_AHEAD_WINDOW = 4 * 1024 ** 2


def parse_size(size_str: str) -> int:
    """Parse size string to bytes."""
    size_str = size_str.strip().upper()

    multipliers = {
        "K": 1024,
        "M": 1024 ** 2,
        "G": 1024 ** 3,
        "T": 1024 ** 4,
    }

    for suffix, multiplier in multipliers.items():
        if size_str.endswith(suffix):
            try:
                value = float(size_str[:-1])
                return int(value * multiplier)
            except (ValueError, OverflowError) as exc:
                raise ValueError(f"Invalid size format: {size_str}") from exc

    try:
        return int(size_str)
    except ValueError as exc:
        raise ValueError(f"Invalid size format: {size_str}") from exc


_SIZE = re.compile(r"\d+(\.\d+)?[KMGT]?", re.IGNORECASE | re.ASCII)
_KEEP_AFTER_CLOSE_PERCENT = re.compile(r"(\d+(\.\d+)?)%", re.ASCII)
_KEEP_AFTER_CLOSE_ALL = re.compile(r"all\s*:", re.IGNORECASE | re.ASCII)


def parse_keep_after_close(text: str) -> StreamingKeepAfterClose:
    """Parse sizes and percentages; an optional all: prefix requires every term.

    The prefix is case-insensitive and may have whitespace before its colon.
    """
    if not isinstance(text, str):
        raise ValueError("expected a string of sizes or percentages such as all:32M,30%")
    text = text.strip()
    prefix = _KEEP_AFTER_CLOSE_ALL.match(text)
    match_all = prefix is not None
    body = text[prefix.end():] if prefix else text
    terms = [term.strip() for term in body.split(",")]
    if not all(terms):
        raise ValueError(f"expected comma-separated sizes or percentages, got {text!r}")

    byte_thresholds = []
    percent_thresholds = []
    for term in terms:
        percent = _KEEP_AFTER_CLOSE_PERCENT.fullmatch(term)
        if percent:
            value = Fraction(percent.group(1))
            if value > 100:
                raise ValueError(f"percentage above 100%: {term!r}")
            percent_thresholds.append(value)
        elif _SIZE.fullmatch(term):
            byte_thresholds.append(parse_size(term))
        else:
            raise ValueError(
                f"not a size or percentage: {term!r}; expected sizes or "
                "percentages such as all:32M,30% or 4M,50%"
            )

    return StreamingKeepAfterClose(tuple(byte_thresholds), tuple(percent_thresholds), match_all)


def parse_read_ahead_window(text: str) -> int:
    if not isinstance(text, str) or not _SIZE.fullmatch(text.strip()):
        raise ValueError("expected a non-negative size such as 4M or 0")
    return parse_size(text)
