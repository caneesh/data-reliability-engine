"""Key and column expressions with key_normalise applied (spec sections 3 and 6).

Normalisers: strip_leading_zeros, and {parse_date: <format>}, which reads a date string
in that format as an ISO date (yyyy-MM-dd), so keys written differently in two layers
compare equal.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.times import sql_string

if TYPE_CHECKING:
    from hcsc.datalake.dre.config.models import Dataset


def normalised(column: str, normaliser: Any) -> str:
    """SQL for one (validated identifier) column under a normaliser."""
    if normaliser == "strip_leading_zeros":
        return f"regexp_replace({column}, '^0+', '')"
    if isinstance(normaliser, dict) and "parse_date" in normaliser:
        return f"date_format(to_date({column}, {sql_string(normaliser['parse_date'])}), 'yyyy-MM-dd')"
    raise ValueError(f"unknown normaliser {normaliser!r}")


def column_expr(dataset: Dataset, column: str) -> str:
    """The column, normalised if the dataset configures a normaliser for it."""
    return normalised(column, dataset.key_normalise[column]) if column in dataset.key_normalise else column


def key_exprs(dataset: Dataset) -> list[str]:
    """One SQL expression per key column, in key order."""
    return [column_expr(dataset, c) for c in dataset.key]


def key_string(exprs: list[str]) -> str:
    """One canonical string for a (normalised) key: parts joined with '|', null written as <null>."""
    parts = [f"coalesce(CAST({e} AS STRING), '<null>')" for e in exprs]
    return f"concat_ws('|', {', '.join(parts)})"
