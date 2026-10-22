"""When a gold rule is due (spec section 6).

frequency every_run: every run. daily: the first run at or after rule_run_at (local time in
the defaults.yaml timezone) each day. weekly: the first run at or after rule_run_at on the
dataset's full_sweep_day. A rule is due when it has no completed NORMAL result since its
latest due moment; a result held back by a PLATFORM or BUDGET reason does not count, so the
next run tries again. Rules always read the whole table after feed_filter.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from hcsc.datalake.dre.checks.events import blocking_sql
from hcsc.datalake.dre.checks.times import as_utc
from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

WEEKDAYS = ("MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY")


def due_moment(frequency: str, run_start: datetime, run_at: str, timezone: str, weekday: str) -> datetime | None:
    """The latest moment (UTC) at or before run_start when the rule fell due; None for every_run."""
    if frequency == "every_run":
        return None
    tz = ZoneInfo(timezone)
    local = as_utc(run_start).astimezone(tz)
    hour, minute = (int(part) for part in run_at.split(":"))
    moment = datetime.combine(local.date(), time(hour, minute), tz)
    if moment > local:
        moment = datetime.combine(local.date() - timedelta(days=1), time(hour, minute), tz)
    if frequency == "weekly":
        back = (moment.weekday() - WEEKDAYS.index(weekday)) % 7
        moment = datetime.combine(moment.date() - timedelta(days=back), time(hour, minute), tz)
    return as_utc(moment)


def last_completed_sql(dq_database: str, since: date | None = None) -> str:
    """The query behind last_completed: rule results only (the rows with a severity), from the
    run_date partitions on or after `since`."""
    validate_dq_database(dq_database)
    lookback = f" AND run_date >= DATE'{since.isoformat()}'" if since is not None else ""
    return (
        f"SELECT dataset, check_id, MAX(evaluated_at) AS evaluated_at FROM {dq_database}.v_committed_result "
        f"WHERE execution_type = 'NORMAL' AND severity IS NOT NULL{lookback} "
        f"AND NOT {blocking_sql()} "
        f"GROUP BY dataset, check_id"
    )


def last_completed(spark: SparkSession, dq_database: str, since: date | None = None) -> dict[tuple[str, str], datetime]:
    """(dataset, rule id) -> latest evaluated_at of a NORMAL rule result not held by PLATFORM or
    BUDGET, within the history lookback (since; None: all of them)."""
    if not spark.catalog.tableExists(f"{dq_database}.dq_check_result"):
        return {}
    rows = spark.sql(last_completed_sql(dq_database, since)).collect()
    return {(r.dataset, r.check_id): as_utc(r.evaluated_at) for r in rows if r.evaluated_at is not None}


def is_due(frequency: str, run_start: datetime, run_at: str, timezone: str, weekday: str,
           completed: datetime | None) -> bool:
    moment = due_moment(frequency, run_start, run_at, timezone, weekday)
    return moment is None or completed is None or completed < moment
