"""Retention for dq store tables: drop run_date partitions older than retention.

This is the only module allowed to drop partitions, and only on the dq store
(guard tests enforce both). It refuses to drop any
partition that is still inside the configured retention period.
"""

from __future__ import annotations

import calendar
from datetime import date
from typing import TYPE_CHECKING

from hcsc.datalake.dre.store.names import validate_dq_database, validate_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class RetentionError(ValueError):
    """A drop was requested for a partition still inside retention."""


def retention_cutoff(today: date, retention_months: int) -> date:
    """First run_date that must be kept: today minus retention_months.

    Partitions with run_date before the cutoff may be dropped. The day is
    clamped to the end of a shorter month (for example 31 March - 1 month is
    28 or 29 February).
    """
    if not isinstance(retention_months, int) or retention_months < 1:
        raise RetentionError(f"retention_months must be a positive integer; got {retention_months!r}")
    month_index = today.year * 12 + today.month - 1 - retention_months
    year, month = divmod(month_index, 12)
    month += 1
    day = min(today.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def drop_partition_sql(
    dq_database: str, table: str, run_date: date, retention_months: int, today: date
) -> str:
    """SQL to drop one run_date partition; raises RetentionError if it is inside retention."""
    validate_dq_database(dq_database)
    validate_identifier(table, "dq table name")
    cutoff = retention_cutoff(today, retention_months)
    if run_date >= cutoff:
        raise RetentionError(
            f"refusing to drop {dq_database}.{table} partition run_date={run_date.isoformat()}: "
            f"newer than the {retention_months}-month retention (keep from {cutoff.isoformat()})"
        )
    return (
        f"ALTER TABLE {dq_database}.{table} DROP IF EXISTS PARTITION "
        f"(run_date = DATE'{run_date.isoformat()}')"
    )


def expired_partitions(run_dates: list[date], retention_months: int, today: date) -> list[date]:
    """The run_dates older than retention, oldest first."""
    cutoff = retention_cutoff(today, retention_months)
    return sorted(d for d in set(run_dates) if d < cutoff)


def apply_retention(
    spark: SparkSession, dq_database: str, table: str, retention_months: int, today: date
) -> list[date]:
    """Drop expired run_date partitions of one dq table; return the dates dropped."""
    validate_dq_database(dq_database)
    validate_identifier(table, "dq table name")
    rows = spark.sql(f"SHOW PARTITIONS {dq_database}.{table}").collect()
    run_dates = [date.fromisoformat(r[0].split("=", 1)[1]) for r in rows]
    dropped = expired_partitions(run_dates, retention_months, today)
    for run_date in dropped:
        spark.sql(drop_partition_sql(dq_database, table, run_date, retention_months, today))
    return dropped
