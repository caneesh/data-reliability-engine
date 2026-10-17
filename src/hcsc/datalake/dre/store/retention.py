"""Retention for dq store tables: drop run_date partitions older than retention.

This is the only module allowed to drop partitions, and only on the dq store
(guard tests enforce both). It refuses to drop any
partition that is still inside the configured retention period, any partition holding rows a
commit promised to keep longer (dq_commit.evidence_kept_until, checkpoint 2 corrections R1: the
deadline a result's evidence claim rests on, even if retention_months was shortened since), and
any partition under an investigation hold (defaults.retention_holds).
"""

from __future__ import annotations

import calendar
from datetime import date
from typing import TYPE_CHECKING, Any

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


# The rows of each table a commit vouches for (the commit's own rows for dq_commit).
_VOUCHED = {
    "dq_check_result": "kind = 'result'",
    "dq_key_event": "kind = 'result' AND key_event_rows > 0",
    "dq_partition_state": "kind = 'result' AND partition_state_rows > 0",
    "dq_cause_result": "kind = 'cause'",
    "dq_commit": "TRUE",
}


def evidence_deadlines(spark: SparkSession, dq_database: str, table: str, run_dates: list[date]) -> dict[date, date]:
    """run_date -> the last day a commit of that run_date promised to keep rows of `table`
    (evidence_kept_until), for the given run_dates; tables no commit vouches for have none."""
    validate_dq_database(dq_database)
    if table not in _VOUCHED or not run_dates:
        return {}
    days = ", ".join(f"DATE'{d.isoformat()}'" for d in sorted(set(run_dates)))
    rows = spark.sql(
        f"SELECT run_date, MAX(evidence_kept_until) AS kept FROM {dq_database}.dq_commit "
        f"WHERE run_date IN ({days}) AND {_VOUCHED[table]} AND evidence_kept_until IS NOT NULL "
        f"GROUP BY run_date").collect()
    return {r.run_date: r.kept for r in rows}


def droppable(spark: SparkSession, dq_database: str, table: str, retention_months: int, today: date,
              holds: list[Any] | None = None) -> tuple[list[date], dict[date, str]]:
    """(the expired run_dates that may be dropped, the expired ones kept and why): kept when a hold
    covers the date or a commit promised to keep its rows past today."""
    expired = expired_partitions(partition_dates(spark, dq_database, table), retention_months, today)
    deadlines = evidence_deadlines(spark, dq_database, table, expired)
    kept: dict[date, str] = {}
    for run_date in expired:
        held = [h for h in holds or [] if h.holds(run_date, today)]
        if held:
            kept[run_date] = f"investigation hold ({held[0].reason})"
        elif run_date in deadlines and deadlines[run_date] >= today:
            kept[run_date] = f"evidence promised until {deadlines[run_date].isoformat()}"
    return [d for d in expired if d not in kept], kept


def apply_retention(
    spark: SparkSession, dq_database: str, table: str, retention_months: int, today: date,
    holds: list[Any] | None = None,
) -> list[date]:
    """Drop expired run_date partitions of one dq table, except held or promised ones; return the
    dates dropped."""
    dropped, _ = droppable(spark, dq_database, table, retention_months, today, holds)
    for run_date in dropped:
        spark.sql(drop_partition_sql(dq_database, table, run_date, retention_months, today))
    return dropped


def retention_command(spark: SparkSession, dq_database: str, retention_months: dict[str, int], apply: bool,
                      today: date, holds: list[Any] | None = None) -> int:
    """`dre retention [--apply]`: list (or, with --apply, drop) each dq table's run_date partitions
    older than its retention_months. Exit 0, or 1 if a table could not be read or dropped."""
    from hcsc.datalake.dre.store.names import DQ_TABLES

    failed = 0
    for table in DQ_TABLES:
        months = retention_months[table]
        try:
            dates, kept = droppable(spark, dq_database, table, months, today, holds)
            if apply:
                for run_date in dates:
                    spark.sql(drop_partition_sql(dq_database, table, run_date, months, today))
        except Exception as exc:  # report the table, go on with the others
            print(f"{dq_database}.{table}: not done ({type(exc).__name__})")
            failed += 1
            continue
        verb = "dropped" if apply else "would drop"
        kept_from = retention_cutoff(today, months).isoformat()
        shown = f" ({dates[0].isoformat()} to {dates[-1].isoformat()})" if dates else ""
        print(f"{dq_database}.{table}: {verb} {len(dates)} partition(s){shown}; keeping run_date from {kept_from}")
        for run_date, why in sorted(kept.items()):
            print(f"{dq_database}.{table}: keeping run_date {run_date.isoformat()}: {why}")
    if not apply:
        print("dre retention: nothing dropped; run with --apply to drop these partitions")
    return 1 if failed else 0
