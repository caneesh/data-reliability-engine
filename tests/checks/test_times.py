"""Time columns are converted from their own zone to UTC; bounds truncate in that zone."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from hcsc.datalake.dre.checks.times import sql_string, truncate, utc_expr
from hcsc.datalake.dre.config.models import TimeColumn

UTC = timezone.utc


def utc_of(spark, column: TimeColumn, value, default_tz: str = "America/Chicago") -> datetime:
    kind = "STRING" if isinstance(value, str) else "TIMESTAMP"
    df = spark.createDataFrame([(value,)], f"{column.column} {kind}")
    df.createOrReplaceTempView("time_input")
    return spark.sql(f"SELECT {utc_expr(column, default_tz)} AS t FROM time_input").collect()[0].t


@pytest.mark.parametrize(
    ("local", "utc"),
    [
        ("2026-01-15 00:30:00:000000", datetime(2026, 1, 15, 6, 30)),  # CST, UTC-6
        ("2026-07-15 00:30:00:000000", datetime(2026, 7, 15, 5, 30)),  # CDT, UTC-5
    ],
)
def test_formatted_column_is_converted_from_its_zone(spark, local: str, utc: datetime) -> None:
    column = TimeColumn(column="gld_lcts", format="yyyy-MM-dd HH:mm:ss:SSSSSS", granularity="minute")
    assert utc_of(spark, column, local) == utc  # the session runs in UTC; naive results are UTC


def test_column_zone_overrides_the_default(spark) -> None:
    column = TimeColumn(column="t", format="yyyy-MM-dd HH:mm:ss", timezone="Asia/Kolkata")
    assert utc_of(spark, column, "2026-01-15 12:00:00") == datetime(2026, 1, 15, 6, 30)


def test_timestamp_column_without_format_is_still_converted(spark) -> None:
    column = TimeColumn(column="t")  # already TIMESTAMP, written as Chicago wall time
    assert utc_of(spark, column, datetime(2026, 1, 15, 0, 30)) == datetime(2026, 1, 15, 6, 30)


def test_utc_column_stays_put(spark) -> None:
    column = TimeColumn(column="t", format="yyyy-MM-dd HH:mm:ss", timezone="UTC")
    assert utc_of(spark, column, "2026-01-15 00:30:00") == datetime(2026, 1, 15, 0, 30)


@pytest.mark.parametrize(
    ("granularity", "expected"),
    [
        (None, datetime(2026, 1, 15, 6, 37, 42, 123456, tzinfo=UTC)),
        ("second", datetime(2026, 1, 15, 6, 37, 42, tzinfo=UTC)),
        ("minute", datetime(2026, 1, 15, 6, 37, tzinfo=UTC)),
        ("hour", datetime(2026, 1, 15, 6, 0, tzinfo=UTC)),
        ("day", datetime(2026, 1, 15, 6, 0, tzinfo=UTC)),  # midnight in Chicago is 06:00 UTC in winter
    ],
)
def test_truncate_in_the_columns_zone(granularity, expected) -> None:
    value = datetime(2026, 1, 15, 6, 37, 42, 123456, tzinfo=UTC)
    assert truncate(value, granularity, "America/Chicago") == expected


def test_truncate_day_before_local_midnight() -> None:
    # 03:00 UTC on the 15th is still the 14th in Chicago.
    assert truncate(datetime(2026, 1, 15, 3, 0, tzinfo=UTC), "day", "America/Chicago") == datetime(
        2026, 1, 14, 6, 0, tzinfo=UTC)


def test_sql_string_escapes_quotes() -> None:
    assert sql_string("yyyy-MM-dd'T'HH:mm") == "'yyyy-MM-dd\\'T\\'HH:mm'"


def test_quoted_format_parses(spark) -> None:
    column = TimeColumn(column="t", format="yyyy-MM-dd'T'HH:mm:ss", timezone="UTC")
    assert utc_of(spark, column, "2026-01-15T00:30:00") == datetime(2026, 1, 15, 0, 30)
