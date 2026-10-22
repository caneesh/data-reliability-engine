"""Partition pruning for windowed scans (spec section 3, `partition_column` and `partition_format`).

Every window predicate parses the load time (`checks/times.utc_expr`), which Spark cannot push
down to a partition column. When a dataset says which column its table is partitioned by and how
the partition value is written (a Spark date pattern such as `yyyy-MM-dd` or `yyyyMMddHH`), the
engine adds a second predicate, `partition_column IN ('…', '…')`, listing every partition value
the window could touch, widened by one granule on each side. It only narrows what is read: the
parsed-time predicate stays, so a partition value that does not match its rows' load times can
make the scan slower, never the result wrong. Values are written with Python's strftime from a
translation of the Spark pattern; a pattern the translation cannot express gets no predicate.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from hcsc.datalake.dre.checks.times import as_utc, column_timezone, sql_string

if TYPE_CHECKING:
    from hcsc.datalake.dre.config.models import Dataset

MAX_VALUES = 1000  # a span listing more partitions than this gets no predicate (the parsed-time predicate still holds)

# Spark (Java DateTimeFormatter) pattern letters the engine can translate, longest first.
_LETTERS: list[tuple[str, str]] = [
    ("yyyy", "%Y"), ("MM", "%m"), ("dd", "%d"), ("HH", "%H"), ("yy", "%y"),
]
_PATTERN = re.compile("|".join(re.escape(k) for k, _ in _LETTERS) + r"|'[^']*'|[^a-zA-Z]+|[a-zA-Z]+")


def strftime_pattern(spark_pattern: str) -> str | None:
    """The strftime equivalent of a Spark date pattern, or None when it uses letters the engine
    does not translate (then no partition predicate is rendered)."""
    out = []
    for piece in _PATTERN.findall(spark_pattern):
        known = dict(_LETTERS).get(piece)
        if known is not None:
            out.append(known)
        elif piece.startswith("'"):
            out.append(piece[1:-1].replace("%", "%%"))
        elif piece.isalpha():
            return None
        else:
            out.append(piece.replace("%", "%%"))
    return "".join(out)


def granule(spark_pattern: str) -> timedelta | None:
    """The finest unit the pattern writes: an hour, a day, or None (months and coarser get no predicate)."""
    if "HH" in spark_pattern:
        return timedelta(hours=1)
    if "dd" in spark_pattern:
        return timedelta(days=1)
    return None


def partition_values(ds: Dataset, start: datetime, end: datetime, default_tz: str) -> list[str] | None:
    """Every partition value a load time in [start, end) could sit in, widened by one granule on
    each side, in the partition's time zone (the load time's zone, or the default); None when the
    dataset has no partition_column and partition_format, the pattern cannot be translated, or
    the span is too long to list."""
    if ds.partition_column is None or ds.partition_format is None:
        return None
    fmt, step = strftime_pattern(ds.partition_format), granule(ds.partition_format)
    if fmt is None or step is None:
        return None
    tz = ZoneInfo(column_timezone(ds.load_time, default_tz) if ds.load_time is not None else default_tz)
    first, last = as_utc(start) - step, as_utc(end) + step
    if (last - first) / step > MAX_VALUES:
        return None
    values: list[str] = []
    at = first
    while at <= last:
        value = at.astimezone(tz).strftime(fmt)
        if not values or values[-1] != value:
            values.append(value)
        at += step
    return values


def partition_filter(ds: Dataset, start: datetime, end: datetime, default_tz: str) -> str | None:
    """`partition_column IN (...)` for the span, or None (see partition_values)."""
    values = partition_values(ds, start, end, default_tz)
    if not values:
        return None
    return f"{ds.partition_column} IN ({', '.join(sql_string(v) for v in values)})"
