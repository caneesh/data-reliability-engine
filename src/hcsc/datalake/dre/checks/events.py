"""Evaluation events, windows and identities (spec section 4).

Release 1 has one kind of event, a batch load event: one dataset and one load
window [window_start, window_end) in UTC, including its start and excluding
its end.

- window_end = the run's start minus settle_minutes and the feed's check_delay_minutes.
- window_start = the latest window_end among the dataset's NORMAL events,
  skipping events that had any check DID_NOT_RUN for a PLATFORM or BUDGET reason
  (those windows are held: evaluated again); on a dataset's first run, window_end
  minus initial_lookback_hours. CONFIGURATION reasons never hold a window.
- A hold lasts at most max_window_hours, counted from the end of the first held
  event. Then the held span is given up: one WINDOW_GAP result (DID_NOT_RUN /
  window_gap) records it, and the window starts where the last held event ended.
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

# DID_NOT_RUN categories that hold a window for the next run (up to max_window_hours).
BLOCKING_CATEGORIES = ("PLATFORM", "BUDGET")
GAP_CHECK_ID = "WINDOW_GAP"  # the result row recording a held span that was given up


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


@dataclass(frozen=True)
class WindowHistory:
    """A dataset's NORMAL events as far as windows are concerned."""

    previous_end: datetime | None = None    # latest end of an event nothing held
    held_start: datetime | None = None      # held events after previous_end: earliest start,
    first_held_end: datetime | None = None  # earliest end,
    last_held_end: datetime | None = None   # and latest end
    held_by: tuple[str, ...] = ()           # the reason codes that held them


def window_history(spark: SparkSession, dq_database: str, dataset: str,
                   exclude_checks: tuple[str, ...] = ()) -> WindowHistory:
    """Read the dataset's window history from dq_check_result. Results of exclude_checks (gold
    rules, which run on their own schedule over the whole table) neither move nor hold a window."""
    validate_dq_database(dq_database)
    validate_identifier(dataset, "dataset id")
    if not spark.catalog.tableExists(f"{dq_database}.dq_check_result"):
        return WindowHistory()
    blocking = ", ".join(f"'{c}'" for c in BLOCKING_CATEGORIES)
    excluded = ", ".join(f"'{validate_identifier(c, 'check id')}'" for c in exclude_checks)
    exclude = f" AND check_id NOT IN ({excluded})" if exclude_checks else ""
    row = spark.sql(
        f"WITH events AS ("
        f"  SELECT event_id, MIN(window_start) AS window_start, MAX(window_end) AS window_end,"
        f"         MAX(CASE WHEN state = 'DID_NOT_RUN' AND reason_category IN ({blocking}) THEN 1 ELSE 0 END) AS held,"
        f"         collect_set(CASE WHEN state = 'DID_NOT_RUN' AND reason_category IN ({blocking})"
        f"                          THEN reason_code END) AS held_by"
        f"  FROM {dq_database}.dq_check_result"
        f"  WHERE dataset = '{dataset}' AND execution_type = 'NORMAL'{exclude}"
        f"  GROUP BY event_id"
        f"), good AS (SELECT MAX(window_end) AS previous_end FROM events WHERE held = 0) "
        f"SELECT g.previous_end, MIN(e.window_start) AS held_start, MIN(e.window_end) AS first_held_end,"
        f"       MAX(e.window_end) AS last_held_end, flatten(collect_list(e.held_by)) AS held_by "
        f"FROM good g LEFT JOIN events e ON e.held = 1 AND (g.previous_end IS NULL OR e.window_end > g.previous_end) "
        f"GROUP BY g.previous_end"
    ).collect()[0]
    utc = (lambda t: None if t is None else as_utc(t))
    return WindowHistory(utc(row.previous_end), utc(row.held_start), utc(row.first_held_end),
                         utc(row.last_held_end), tuple(sorted(set(row.held_by or []))))


def previous_window_end(spark: SparkSession, dq_database: str, dataset: str) -> datetime | None:
    """Latest window_end of the dataset's NORMAL events that no blocking DID_NOT_RUN held back."""
    return window_history(spark, dq_database, dataset).previous_end


def release_hold(history: WindowHistory, dataset: str, window_end: datetime,
                 max_window_hours: float) -> tuple[datetime | None, Event | None]:
    """(where this window starts from, the given-up span or None). A held window is evaluated
    again until max_window_hours have passed since the first held event ended; then the held
    span becomes a gap and the window starts where the last held event ended."""
    if history.first_held_end is None:
        return history.previous_end, None
    if as_utc(window_end) - history.first_held_end < timedelta(hours=max_window_hours):
        return history.previous_end, None
    start = history.previous_end or history.held_start
    return history.last_held_end, Event(dataset, start, history.last_held_end)
