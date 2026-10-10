"""Evaluation events, windows and identities (spec section 4).

Release 1 has one kind of event, a batch load event: one dataset and one load
window [window_start, window_end) in UTC, including its start and excluding
its end.

- window_end = the run's start minus settle_minutes and the feed's check_delay_minutes.
- window_start = the latest window_end among the dataset's NORMAL events,
  skipping events that had any check DID_NOT_RUN for a PLATFORM, BUDGET or
  CONFIGURATION reason (those windows are evaluated again); on a dataset's
  first run, window_end minus initial_lookback_hours.
- Both bounds are truncated to load_time.granularity in the load time's zone.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.times import as_utc, column_timezone, truncate
from hcsc.datalake.dre.store.names import validate_dq_database, validate_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.config.models import Dataset, Settings

# DID_NOT_RUN categories that keep a window open for the next run.
BLOCKING_CATEGORIES = ("PLATFORM", "BUDGET", "CONFIGURATION")


def _sha256(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Event:
    dataset: str
    window_start: datetime
    window_end: datetime

    @property
    def event_id(self) -> str:
        return _sha256(self.dataset, as_utc(self.window_start).isoformat(), as_utc(self.window_end).isoformat())


def canonical_groups(group_values: dict[str, str | None]) -> str:
    """Group values in a fixed order, so the same groups always give the same id."""
    return ",".join(f"{k}={'' if v is None else v}" for k, v in sorted(group_values.items()))


def evaluation_id(event_id: str, check_id: str, expectation_version: int, engine_version: str,
                  group_values: dict[str, str | None] | None = None) -> str:
    """One id per (event, check, version, engine, group): each group row of a grouped check has its own."""
    return _sha256(event_id, check_id, str(expectation_version), engine_version, canonical_groups(group_values or {}))


def window_for(dataset: Dataset, run_start: datetime, previous_end: datetime | None,
               settings: Settings, default_tz: str, check_delay_minutes: int = 0) -> Event:
    """The dataset's event for a run starting at run_start, given its previous window end.
    The feed's check_delay_minutes moves the end back further, so a slot is judged only once
    its deadline plus the delay has passed."""
    end = as_utc(run_start) - timedelta(minutes=settings.settle_minutes + check_delay_minutes)
    previous = previous_end
    start = as_utc(previous) if previous is not None else end - timedelta(hours=settings.initial_lookback_hours)
    if dataset.load_time is not None:
        tz = column_timezone(dataset.load_time, default_tz)
        start = truncate(start, dataset.load_time.granularity, tz)
        end = truncate(end, dataset.load_time.granularity, tz)
    return Event(dataset.dataset, min(start, end), end)


def previous_window_end(spark: SparkSession, dq_database: str, dataset: str) -> datetime | None:
    """Latest window_end of the dataset's NORMAL events that no blocking DID_NOT_RUN held back."""
    validate_dq_database(dq_database)
    validate_identifier(dataset, "dataset id")
    if not spark.catalog.tableExists(f"{dq_database}.dq_check_result"):
        return None
    blocking = ", ".join(f"'{c}'" for c in BLOCKING_CATEGORIES)
    row = spark.sql(
        f"SELECT MAX(window_end) AS window_end FROM ("
        f"  SELECT event_id, MAX(window_end) AS window_end,"
        f"         MAX(CASE WHEN state = 'DID_NOT_RUN' AND reason_category IN ({blocking}) THEN 1 ELSE 0 END) AS blocked"
        f"  FROM {dq_database}.dq_check_result"
        f"  WHERE dataset = '{dataset}' AND execution_type = 'NORMAL'"
        f"  GROUP BY event_id"
        f") events WHERE blocked = 0"
    ).collect()[0]
    return None if row.window_end is None else as_utc(row.window_end)
