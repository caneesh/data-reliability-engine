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


def partition_dates(spark: SparkSession, dq_database: str, table: str) -> list[date]:
    """The run_date partitions of one dq table."""
    validate_dq_database(dq_database)
    validate_identifier(table, "dq table name")
    rows = spark.sql(f"SHOW PARTITIONS {dq_database}.{table}").collect()
    return [date.fromisoformat(r[0].split("=", 1)[1]) for r in rows]


def apply_retention(
    spark: SparkSession, dq_database: str, table: str, retention_months: int, today: date
) -> list[date]:
    """Drop expired run_date partitions of one dq table; return the dates dropped."""
    dropped = expired_partitions(partition_dates(spark, dq_database, table), retention_months, today)
    for run_date in dropped:
        spark.sql(drop_partition_sql(dq_database, table, run_date, retention_months, today))
    return dropped


def retention_command(spark: SparkSession, dq_database: str, retention_months: dict[str, int], apply: bool,
                      today: date) -> int:
    """`dre retention [--apply]`: list (or, with --apply, drop) each dq table's run_date partitions
    older than its retention_months. Exit 0, or 1 if a table could not be read or dropped."""
    from hcsc.datalake.dre.store.names import DQ_TABLES

    failed = 0
    for table in DQ_TABLES:
        months = retention_months[table]
        try:
            if apply:
                dates = apply_retention(spark, dq_database, table, months, today)
            else:
                dates = expired_partitions(partition_dates(spark, dq_database, table), months, today)
        except Exception as exc:  # report the table, go on with the others
            print(f"{dq_database}.{table}: not done ({type(exc).__name__})")
            failed += 1
            continue
        verb = "dropped" if apply else "would drop"
        kept_from = retention_cutoff(today, months).isoformat()
        shown = f" ({dates[0].isoformat()} to {dates[-1].isoformat()})" if dates else ""
        print(f"{dq_database}.{table}: {verb} {len(dates)} partition(s){shown}; keeping run_date from {kept_from}")
    if not apply:
        print("dre retention: nothing dropped; run with --apply to drop these partitions")
    return 1 if failed else 0
