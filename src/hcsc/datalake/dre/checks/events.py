"""Evaluation events, windows and identities (spec section 4).

Release 1 has one kind of event, a batch load event: one dataset and one load
window [window_start, window_end) in UTC, including its start and excluding
its end.

- window_end = the run's start minus settle_minutes and the feed's check_delay_minutes.
- window_start = the latest window_end among the dataset's NORMAL events,
  skipping events that had any check DID_NOT_RUN for a PLATFORM or BUDGET reason, or that
  were evaluated by a run that did not finish COMPLETED (those windows are held: evaluated
  again, so evidence a broken run could not store is recovered); on a dataset's first run, window_end
  minus initial_lookback_hours. CONFIGURATION reasons never hold a window.
- A hold lasts at most max_window_hours, counted from the end of the first held
  event. Then the held span is given up: one WINDOW_GAP result (DID_NOT_RUN /
  window_gap) records it, and the window starts where the last held event ended.
- Both bounds are truncated to load_time.granularity in the load time's zone.

RERUN and REPLAY name historical windows instead (fixed_window, replay_windows); their results
never move or hold a window.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
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


def history_sql(dq_database: str, datasets: list[str], since: date | None = None) -> str:
    """The one query behind window_histories (a function so a test can EXPLAIN it)."""
    validate_dq_database(dq_database)
    names = ", ".join(f"'{validate_identifier(d, 'dataset id')}'" for d in sorted(set(datasets)))
    blocking = ", ".join(f"'{c}'" for c in BLOCKING_CATEGORIES)
    lookback = f" AND r.run_date >= DATE'{since.isoformat()}'" if since is not None else ""
    # A run that did not finish COMPLETED (PARTIAL, FAILED, or died while STARTED) may not have
    # stored all its evidence: its events are held too. A result with no run row is not held.
    # This reads the table, not v_committed_result (ADR-07): an uncommitted row belongs to a run that
    # did not finish COMPLETED (a run is COMPLETED only once every evaluation is committed), so it is
    # held here, which keeps its window and the max_window_hours limit on holding.
    incomplete = "coalesce(u.status, 'COMPLETED') <> 'COMPLETED'"
    return (
        f"WITH events AS ("
        f"  SELECT r.dataset, r.event_id, MIN(r.window_start) AS window_start, MAX(r.window_end) AS window_end,"
        f"         MAX(CASE WHEN (r.state = 'DID_NOT_RUN' AND r.reason_category IN ({blocking})) OR {incomplete}"
        f"                  THEN 1 ELSE 0 END) AS held,"
        f"         collect_set(CASE WHEN r.state = 'DID_NOT_RUN' AND r.reason_category IN ({blocking})"
        f"                          THEN r.reason_code"
        f"                          WHEN {incomplete} THEN concat('run_', lower(u.status)) END) AS held_by"
        f"  FROM {dq_database}.dq_check_result r"
        f"  LEFT JOIN {dq_database}.v_latest_run u ON u.run_id = r.run_id"
        f"  WHERE r.dataset IN ({names}) AND r.execution_type = 'NORMAL' AND r.severity IS NULL{lookback}"
        f"  GROUP BY r.dataset, r.event_id"
        f"), good AS (SELECT dataset, MAX(CASE WHEN held = 0 THEN window_end END) AS previous_end"
        f"            FROM events GROUP BY dataset) "
        f"SELECT g.dataset, g.previous_end, MIN(e.window_start) AS held_start, MIN(e.window_end) AS first_held_end,"
        f"       MAX(e.window_end) AS last_held_end, flatten(collect_list(e.held_by)) AS held_by "
        f"FROM good g LEFT JOIN events e ON e.dataset = g.dataset AND e.held = 1"
        f"  AND (g.previous_end IS NULL OR e.window_end > g.previous_end) "
        f"GROUP BY g.dataset, g.previous_end"
    )


def window_histories(spark: SparkSession, dq_database: str, datasets: list[str],
                     since: date | None = None) -> dict[str, WindowHistory]:
    """Read the datasets' window histories from dq_check_result in one pass, from the run_date
    partitions on or after `since` (the history lookback; None reads them all). A dataset whose
    last NORMAL event is older than the lookback windows as on a first run. Gold rule results
    (the only results with a severity) neither move nor hold a window: rules run on their own
    schedule over the whole table. They are told apart by the row, not by today's rule ids, so a
    renamed or deleted rule's old results stay out too."""
    validate_dq_database(dq_database)
    if not datasets:
        return {}
    found = {d: WindowHistory() for d in datasets}
    if not spark.catalog.tableExists(f"{dq_database}.dq_check_result"):
        return found
    rows = spark.sql(history_sql(dq_database, datasets, since)).collect()
    utc = (lambda t: None if t is None else as_utc(t))
    for row in rows:
        found[row.dataset] = WindowHistory(utc(row.previous_end), utc(row.held_start), utc(row.first_held_end),
                                           utc(row.last_held_end), tuple(sorted(set(row.held_by or []))))
    return found


def window_history(spark: SparkSession, dq_database: str, dataset: str) -> WindowHistory:
    """One dataset's window history (window_histories)."""
    return window_histories(spark, dq_database, [dataset])[dataset]


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
        # held: start where the held span starts (its first event's start when the dataset has no
        # good event yet, so a held first window is not re-anchored by the lookback and narrowed)
        return history.previous_end or history.held_start, None
    start = history.previous_end or history.held_start
    return history.last_held_end, Event(dataset, start, history.last_held_end)


def fixed_window(dataset: Dataset, start: datetime, end: datetime, default_tz: str) -> Event:
    """A given (historical) window for the dataset, bounds truncated as window_for truncates them,
    so naming a NORMAL event's bounds gives that event (and its event_id) again."""
    start, end = as_utc(start), as_utc(end)
    if dataset.load_time is not None:
        tz = column_timezone(dataset.load_time, default_tz)
        start = truncate(start, dataset.load_time.granularity, tz)
        end = truncate(end, dataset.load_time.granularity, tz)
    return Event(dataset.dataset, min(start, end), end)


def recorded_windows(spark: SparkSession, dq_database: str, datasets: list[str], start: datetime,
                     end: datetime) -> dict[str, list[tuple[datetime, datetime]]]:
    """Each dataset's NORMAL event windows (not rule results, not WINDOW_GAP records) lying inside
    [start, end), oldest first."""
    validate_dq_database(dq_database)
    found: dict[str, list[tuple[datetime, datetime]]] = {d: [] for d in datasets}
    if not datasets or not spark.catalog.tableExists(f"{dq_database}.dq_check_result"):
        return found
    names = ", ".join(f"'{validate_identifier(d, 'dataset id')}'" for d in sorted(set(datasets)))
    lo = as_utc(start).strftime("%Y-%m-%d %H:%M:%S.%f")
    hi = as_utc(end).strftime("%Y-%m-%d %H:%M:%S.%f")
    rows = spark.sql(
        f"SELECT DISTINCT dataset, window_start, window_end FROM {dq_database}.dq_check_result "
        f"WHERE dataset IN ({names}) AND execution_type = 'NORMAL' AND severity IS NULL "
        f"AND check_id <> '{GAP_CHECK_ID}' "
        f"AND window_start >= TIMESTAMP '{lo}' AND window_end <= TIMESTAMP '{hi}' "
        f"ORDER BY dataset, window_start, window_end"
    ).collect()
    for r in rows:
        found[r.dataset].append((as_utc(r.window_start), as_utc(r.window_end)))
    return found


def replay_windows(dataset: Dataset, start: datetime, end: datetime, recorded: list[tuple[datetime, datetime]],
                   default_tz: str) -> list[Event]:
    """The windows a REPLAY of [start, end) evaluates for the dataset, oldest first: its recorded
    NORMAL windows in the range, exactly (so each keeps its event_id), and every span no recorded
    window covers as one window of its own. A window held and evaluated again is replayed once, as
    its longest evaluation (the windows starting where an earlier one did are skipped)."""
    events: list[Event] = []
    cursor = as_utc(start)
    for w_start, w_end in sorted(recorded, key=lambda w: (w[0], -w[1].timestamp())):
        if w_start < cursor:
            continue
        if w_start > cursor:
            events.append(fixed_window(dataset, cursor, w_start, default_tz))
        events.append(Event(dataset.dataset, w_start, w_end))
        cursor = w_end
    if cursor < as_utc(end):
        events.append(fixed_window(dataset, cursor, end, default_tz))
    return [e for e in events if e.window_start < e.window_end]
