"""Event windows: settle time, first-run lookback, truncation, what moves the window on, and
how a held window is released after max_window_hours."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from hcsc.datalake.dre.checks.events import (
    Event, WindowHistory, evaluation_id, previous_window_end, release_hold, window_for, window_history,
)
from hcsc.datalake.dre.config.models import Dataset, TimeColumn
from hcsc.datalake.dre.store.local_setup import create_store
from tests.fixtures.settings import SETTINGS
from tests.store.conftest import append_rows

UTC = timezone.utc
RUN_START = datetime(2026, 1, 15, 12, 7, 31, tzinfo=UTC)


def dataset(granularity: str | None = "minute", tz: str | None = None) -> Dataset:
    load_time = TimeColumn(column="gld_lcts", format="yyyy-MM-dd HH:mm:ss:SSSSSS", granularity=granularity,
                           timezone=tz) if granularity else None
    return Dataset(dataset="gold", table="gold_db.member_coverage", layer="GOLD", key=["sub_id"], load_time=load_time)


def test_first_window_ends_settle_minutes_before_the_run_and_looks_back() -> None:
    event = window_for(dataset(), RUN_START, None, SETTINGS, "America/Chicago")
    assert event.window_end == datetime(2026, 1, 15, 11, 52, tzinfo=UTC)  # 12:07:31 - 15 min, to the minute
    assert event.window_start == datetime(2026, 1, 14, 11, 52, tzinfo=UTC)  # minus 24 hours


def test_next_window_starts_at_the_previous_end() -> None:
    previous = datetime(2026, 1, 15, 8, 0, tzinfo=UTC)
    event = window_for(dataset(), RUN_START, previous, SETTINGS, "America/Chicago")
    assert (event.window_start, event.window_end) == (previous, datetime(2026, 1, 15, 11, 52, tzinfo=UTC))


def test_settle_and_lookback_come_from_settings() -> None:
    settings = SETTINGS.model_copy(update={"settle_minutes": 0, "initial_lookback_hours": 2})
    event = window_for(dataset(granularity=None), RUN_START, None, settings, "UTC")
    assert (event.window_start, event.window_end) == (datetime(2026, 1, 15, 10, 7, 31, tzinfo=UTC), RUN_START)


def test_day_granularity_truncates_in_the_load_time_zone() -> None:
    event = window_for(dataset("day"), RUN_START, None, SETTINGS, "America/Chicago")
    # Midnight in Chicago (CST) is 06:00 UTC.
    assert (event.window_start, event.window_end) == (
        datetime(2026, 1, 14, 6, 0, tzinfo=UTC), datetime(2026, 1, 15, 6, 0, tzinfo=UTC))
    own_zone = window_for(dataset("day", tz="UTC"), RUN_START, None, SETTINGS, "America/Chicago")
    assert own_zone.window_end == datetime(2026, 1, 15, 0, 0, tzinfo=UTC)


def test_window_never_runs_backwards() -> None:
    later = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)  # a previous end after this run's end
    event = window_for(dataset(), RUN_START, later, SETTINGS, "America/Chicago")
    assert event.window_start == event.window_end  # an empty window, [end, end)


def test_evaluation_id_includes_sorted_group_values() -> None:
    event = Event("gold", datetime(2026, 1, 14, tzinfo=UTC), datetime(2026, 1, 15, tzinfo=UTC))
    a = evaluation_id(event.event_id, "T1_KEY_DUPLICATES", 1, "0.1.0", {"src_sys_nm": "SRC_A", "region": "N"})
    a_reordered = evaluation_id(event.event_id, "T1_KEY_DUPLICATES", 1, "0.1.0", {"region": "N", "src_sys_nm": "SRC_A"})
    b = evaluation_id(event.event_id, "T1_KEY_DUPLICATES", 1, "0.1.0", {"src_sys_nm": "SRC_B", "region": "N"})
    ungrouped = evaluation_id(event.event_id, "T1_KEY_DUPLICATES", 1, "0.1.0")
    assert a == a_reordered
    assert len({a, b, ungrouped}) == 3


def _result(event_id: str, end_hour: int, state: str = "PASSED", category: str | None = None,
            execution_type: str = "NORMAL", check_id: str = "T1_KEY_DUPLICATES", code: str | None = None,
            dataset: str = "gold") -> dict:
    return {"evaluation_id": f"{event_id}-{state}-{check_id}", "run_id": event_id, "event_id": event_id,
            "window_start": datetime(2026, 1, 15, end_hour - 1), "window_end": datetime(2026, 1, 15, end_hour),
            "execution_type": execution_type, "dataset": dataset, "check_id": check_id,
            "state": state, "reason_category": category, "reason_code": code, "run_date": date(2026, 1, 15)}


def test_blocked_events_do_not_move_the_window(spark) -> None:
    create_store(spark, "dq_windows")
    rows = [
        _result("e1", 1),                                                   # PASSED
        _result("e2", 2, "DID_NOT_RUN", "DATA_UNAVAILABLE"),                # empty population: counts
        _result("e3", 3), _result("e3", 3, "DID_NOT_RUN", "CONFIGURATION"),  # CONFIGURATION never holds: counts
        _result("e4", 4, "DID_NOT_RUN", "PLATFORM", code="query_failed"),   # held
        _result("e5", 5, "DID_NOT_RUN", "BUDGET", code="budget_exceeded"),  # held
        _result("e6", 6, execution_type="REPLAY"),                          # replays never count
    ]
    append_rows(spark, "dq_windows", "dq_check_result", rows)
    assert previous_window_end(spark, "dq_windows", "gold") == datetime(2026, 1, 15, 3, tzinfo=UTC)
    history = window_history(spark, "dq_windows", "gold")
    assert history == WindowHistory(
        previous_end=datetime(2026, 1, 15, 3, tzinfo=UTC), held_start=datetime(2026, 1, 15, 3, tzinfo=UTC),
        first_held_end=datetime(2026, 1, 15, 4, tzinfo=UTC), last_held_end=datetime(2026, 1, 15, 5, tzinfo=UTC),
        held_by=("budget_exceeded", "query_failed"))
    assert previous_window_end(spark, "dq_windows", "other") is None


def test_rule_results_neither_move_nor_hold_a_window(spark) -> None:
    create_store(spark, "dq_windows_rules")
    append_rows(spark, "dq_windows_rules", "dq_check_result", [
        _result("e1", 1),
        _result("e2", 2, check_id="one_row_per_coverage"),                               # a rule: ignored
        _result("e3", 3, "DID_NOT_RUN", "PLATFORM", check_id="one_row_per_coverage"),   # ignored too
    ])
    history = window_history(spark, "dq_windows_rules", "gold", exclude_checks=("one_row_per_coverage",))
    assert history == WindowHistory(previous_end=datetime(2026, 1, 15, 1, tzinfo=UTC))


HELD = WindowHistory(previous_end=datetime(2026, 1, 15, 3, tzinfo=UTC), held_start=datetime(2026, 1, 15, 3, tzinfo=UTC),
                     first_held_end=datetime(2026, 1, 15, 4, tzinfo=UTC),
                     last_held_end=datetime(2026, 1, 15, 9, tzinfo=UTC), held_by=("query_failed",))


def test_a_held_window_is_evaluated_again_until_max_window_hours() -> None:
    # 71 hours after the first held event ended: still held, the window starts at the last good end.
    end = HELD.first_held_end + timedelta(hours=71)
    assert release_hold(HELD, "gold", end, 72) == (HELD.previous_end, None)
    # Nothing held: the window follows on.
    assert release_hold(WindowHistory(previous_end=HELD.previous_end), "gold", end, 72) == (HELD.previous_end, None)


def test_a_hold_past_max_window_hours_is_released_as_a_gap() -> None:
    end = HELD.first_held_end + timedelta(hours=72)
    start, gap = release_hold(HELD, "gold", end, 72)
    assert start == HELD.last_held_end
    assert gap == Event("gold", HELD.previous_end, HELD.last_held_end)
    # A dataset held since its first run: the gap starts where the first held window started.
    first = WindowHistory(held_start=datetime(2026, 1, 14, tzinfo=UTC), first_held_end=HELD.first_held_end,
                          last_held_end=HELD.last_held_end)
    assert release_hold(first, "gold", end, 72)[1] == Event("gold", datetime(2026, 1, 14, tzinfo=UTC),
                                                            HELD.last_held_end)
