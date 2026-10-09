"""Event windows: settle time, first-run lookback, truncation, and what moves the window on."""

from __future__ import annotations

from datetime import date, datetime, timezone

from hcsc.datalake.dre.checks.events import Event, evaluation_id, previous_window_end, window_for
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
            execution_type: str = "NORMAL") -> dict:
    return {"evaluation_id": f"{event_id}-{state}", "run_id": event_id, "event_id": event_id,
            "window_start": datetime(2026, 1, 15, end_hour - 1), "window_end": datetime(2026, 1, 15, end_hour),
            "execution_type": execution_type, "dataset": "gold", "check_id": "T1_KEY_DUPLICATES",
            "state": state, "reason_category": category, "run_date": date(2026, 1, 15)}


def test_blocked_events_do_not_move_the_window(spark) -> None:
    create_store(spark, "dq_windows")
    rows = [
        _result("e1", 1),                                                   # PASSED
        _result("e2", 2, "DID_NOT_RUN", "DATA_UNAVAILABLE"),                # empty population: counts
        _result("e3", 3), _result("e3", 3, "DID_NOT_RUN", "CONFIGURATION"),  # one check blocked: skipped
        _result("e4", 4, "DID_NOT_RUN", "PLATFORM"),                        # skipped
        _result("e5", 5, "DID_NOT_RUN", "BUDGET"),                          # skipped
        _result("e6", 6, execution_type="REPLAY"),                          # replays never count
    ]
    append_rows(spark, "dq_windows", "dq_check_result", rows)
    assert previous_window_end(spark, "dq_windows", "gold") == datetime(2026, 1, 15, 2, tzinfo=UTC)
    assert previous_window_end(spark, "dq_windows", "other") is None
