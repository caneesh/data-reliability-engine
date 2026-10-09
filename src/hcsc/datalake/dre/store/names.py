"""Validation of names that are interpolated into dq store SQL."""

from __future__ import annotations

import re

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def validate_identifier(name: str, what: str) -> str:
    """Return name if it is a plain SQL identifier, else raise ValueError."""
    if not isinstance(name, str) or not _IDENTIFIER.fullmatch(name):
        raise ValueError(
            f"{what} must be a plain identifier (letters, digits and underscores, "
            f"not starting with a digit); got {name!r}"
        )
    return name


def validate_dq_database(name: str) -> str:
    """The configured dq database name: a plain identifier, no dots, quotes or spaces."""
    return validate_identifier(name, "dq database name")
