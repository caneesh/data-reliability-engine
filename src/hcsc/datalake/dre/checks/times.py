"""Time columns read as UTC, and window bounds truncated to a column's granularity.

Source columns are never assumed to be UTC: each is parsed with its configured
format, then converted from its configured time zone (or the default in
defaults.yaml) to UTC before any comparison (spec section 4). The Spark session
runs in UTC.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from hcsc.datalake.dre.config.models import TimeColumn


def sql_string(value: str) -> str:
    """A Spark SQL string literal for a config value (escapes backslashes and quotes)."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def column_timezone(column: TimeColumn, default_tz: str) -> str:
    return column.timezone or default_tz


def utc_expr(column: TimeColumn, default_tz: str) -> str:
    """SQL expression for the column as a UTC TIMESTAMP."""
    if column.format:
        parsed = f"to_timestamp({column.column}, {sql_string(column.format)})"
    else:
        parsed = f"CAST({column.column} AS TIMESTAMP)"
    return f"to_utc_timestamp({parsed}, {sql_string(column_timezone(column, default_tz))})"


def as_utc(value: datetime) -> datetime:
    """Aware UTC datetime; a naive value (as Spark returns in a UTC session) is taken as UTC."""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def truncate(value: datetime, granularity: str | None, tz: str) -> datetime:
    """Floor a UTC time to the granularity, counted in the column's own time zone."""
    value = as_utc(value)
    if granularity is None:
        return value
    local = value.astimezone(ZoneInfo(tz))
    if granularity == "second":
        local = local.replace(microsecond=0)
    elif granularity == "minute":
        local = local.replace(second=0, microsecond=0)
    elif granularity == "hour":
        local = local.replace(minute=0, second=0, microsecond=0)
    elif granularity == "day":
        local = local.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"unknown granularity {granularity!r}")
    return local.astimezone(timezone.utc)
