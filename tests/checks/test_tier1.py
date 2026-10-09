"""T1_ON_TIME, T1_ZERO_ROWS, T1_VOLUME, T1_SCHEMA_DRIFT and T1_KEY_NULLS can PASS, FAIL and be DID_NOT_RUN.

Fixed window [2026-01-15 12:00, 2026-01-16 12:00) UTC. Cadence 00:30 and 12:30 Chicago
(06:30 and 18:30 UTC in January), SLA 8 hours.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.events import Event, window_for
from hcsc.datalake.dre.checks.tier1.key_nulls import KeyNulls
from hcsc.datalake.dre.checks.tier1.on_time import OnTime
from hcsc.datalake.dre.checks.tier1.schema_drift import SchemaDrift
from hcsc.datalake.dre.checks.tier1.volume import Volume
from hcsc.datalake.dre.checks.tier1.zero_rows import ZeroRows
from hcsc.datalake.dre.config.models import Cadence, Dataset, Feed, TimeColumn
from hcsc.datalake.dre.store.local_setup import create_store
from tests.fixtures.layers import GOLD_COLUMNS, create_table, gold_row
from tests.fixtures.settings import SETTINGS
from tests.store.conftest import append_rows

UTC = timezone.utc
EVENT = Event("gold", datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 1, 16, 12, tzinfo=UTC))
DQ = "dq_tier1"
FEED = Feed(feed="example_realtime", expectation_version=1, pattern="TABLE_MERGE", owner="membership-gold",
            cadence=Cadence(kind="times", times=["00:30", "12:30"], timezone="America/Chicago"),
            datasets=["gold"])
LOAD_TIME = TimeColumn(column="gld_lcts", format="yyyy-MM-dd HH:mm:ss:SSSSSS", granularity="minute")


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


def gold(table: str, **extra) -> Dataset:
    fields = {"dataset": "gold", "table": table, "layer": "GOLD", "load_time": LOAD_TIME,
              "key": ["sub_id", "mem_nbr"], **extra}
    return Dataset(**fields)


def run(spark, check, ds: Dataset, feed: Feed | None = FEED, event: Event = EVENT):
    ctx = CheckContext(spark, ds, feed, SETTINGS, "America/Chicago", DQ)
    results, _ = run_check(check, ctx, event)
    return results


def local(text: str) -> str:
    """A gld_lcts value: Chicago wall time, minute granular."""
    return f"{text}:00:000000"


# --- T1_ON_TIME: slots due by deadline in the window: 06:30 and 18:30 UTC on the 15th ---

def test_on_time_passes_when_every_due_slot_loaded(spark) -> None:
    rows = [gold_row(loaded=local("2026-01-15 01:00")), gold_row(loaded=local("2026-01-15 13:00"))]
    create_table(spark, "t1.on_time_ok", GOLD_COLUMNS, rows)
    [r] = run(spark, OnTime(), gold("t1.on_time_ok"))
    assert (r.state, r.population, r.violations) == ("PASSED", 2, 0)


def test_on_time_fails_on_a_missed_slot(spark) -> None:
    # Loaded after the 00:30 slot, but nothing within 8 hours of the 12:30 slot.
    create_table(spark, "t1.on_time_late", GOLD_COLUMNS, [gold_row(loaded=local("2026-01-15 01:00"))])
    [r] = run(spark, OnTime(), gold("t1.on_time_late"))
    assert (r.state, r.population, r.violations) == ("FAILED", 2, 1)


def test_on_time_did_not_run_without_a_due_slot_or_load_time(spark) -> None:
    create_table(spark, "t1.on_time_none", GOLD_COLUMNS)
    short = Event("gold", datetime(2026, 1, 15, 15, tzinfo=UTC), datetime(2026, 1, 15, 16, tzinfo=UTC))
    [r] = run(spark, OnTime(), gold("t1.on_time_none"), event=short)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")
    [r] = run(spark, OnTime(), gold("t1.on_time_none", load_time=None))
    assert (r.reason_category, r.reason_code) == ("CONFIGURATION", "invalid_config")


# --- T1_ZERO_ROWS: judges the same slots as T1_ON_TIME (06:30 and 18:30 UTC on the 15th) ---

def test_zero_rows_passes_and_fails(spark) -> None:
    rows = [gold_row(loaded=local("2026-01-15 01:00")), gold_row(loaded=local("2026-01-15 13:00"))]
    create_table(spark, "t1.zero_ok", GOLD_COLUMNS, rows)
    [r] = run(spark, ZeroRows(), gold("t1.zero_ok"))
    assert (r.state, r.population, r.violations) == ("PASSED", 2, 0)
    # The 12:30 load wrote nothing: one of the two due loads is short.
    create_table(spark, "t1.zero_bad", GOLD_COLUMNS, [gold_row(loaded=local("2026-01-15 01:00"))])
    [r] = run(spark, ZeroRows(), gold("t1.zero_bad"))
    assert (r.state, r.population, r.violations) == ("FAILED", 2, 1)
    assert r.observed == "1 of 2 due loads wrote fewer rows than the minimum"


def test_zero_rows_counts_rows_against_min_rows_per_load(spark) -> None:
    rows = [gold_row(loaded=local("2026-01-15 01:00")), gold_row(loaded=local("2026-01-15 13:00"))]
    create_table(spark, "t1.zero_min", GOLD_COLUMNS, rows)
    settings = SETTINGS.model_copy(update={"min_rows_per_load": 2})
    results, _ = run_check(ZeroRows(), CheckContext(spark, gold("t1.zero_min"), FEED, settings,
                                                    "America/Chicago", DQ), EVENT)
    assert [(r.state, r.violations) for r in results] == [("FAILED", 2)]


def test_zero_rows_did_not_run_when_no_load_due(spark) -> None:
    create_table(spark, "t1.zero_none", GOLD_COLUMNS)
    short = Event("gold", datetime(2026, 1, 15, 15, tzinfo=UTC), datetime(2026, 1, 15, 16, tzinfo=UTC))
    [r] = run(spark, ZeroRows(), gold("t1.zero_none"), event=short)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


def test_a_load_counts_until_its_deadline_and_not_after(spark) -> None:
    # The 00:30 Chicago slot (06:30 UTC) has until 14:30 UTC; a load at exactly 08:30 Chicago is late.
    create_table(spark, "t1.zero_edge", GOLD_COLUMNS,
                 [gold_row(loaded=local("2026-01-15 08:30")), gold_row(loaded=local("2026-01-15 13:00"))])
    for check in (OnTime(), ZeroRows()):
        [r] = run(spark, check, gold("t1.zero_edge"))
        assert (r.state, r.violations) == ("FAILED", 1), check.check_id


def test_load_not_yet_due_is_not_judged(spark) -> None:
    # Load due at 08:00 with an 8-hour SLA; DRE runs at 09:00 and nothing has loaded yet today.
    # Neither check may fail: today's slot is judged only once its deadline (16:00) has passed.
    feed = FEED.model_copy(update={"cadence": Cadence(kind="times", times=["08:00"], timezone="America/Chicago")})
    create_table(spark, "t1.not_due", GOLD_COLUMNS, [gold_row(loaded=local("2026-01-14 08:30"))])  # yesterday's load
    ds = gold("t1.not_due")
    run_start = datetime(2026, 1, 15, 15, 0, tzinfo=UTC)  # 09:00 in Chicago
    event = window_for(ds, run_start, None, SETTINGS, "America/Chicago")
    for check in (OnTime(), ZeroRows()):
        [r] = run(spark, check, ds, feed=feed, event=event)
        assert r.state == "PASSED", (check.check_id, r)
        assert r.population == 1, check.check_id  # yesterday's slot only; today's is not due


# --- T1_VOLUME: one result per slot whose period ends in the window ---
# Cadence every 4 hours in UTC; window [2026-01-15 12:00, 2026-01-16 12:00) holds six periods,
# 12:00-16:00 on the 15th through 08:00-12:00 on the 16th.

UTC_LOAD = TimeColumn(column="gld_lcts", format="yyyy-MM-dd HH:mm:ss:SSSSSS", granularity="minute", timezone="UTC")
SIX_SLOTS = FEED.model_copy(update={"cadence": Cadence(
    kind="times", times=["00:00", "04:00", "08:00", "12:00", "16:00", "20:00"], timezone="UTC")})
PERIODS = [datetime(2026, 1, 15, 12, tzinfo=UTC) + timedelta(hours=4 * i) for i in range(6)]


def label(slot: datetime) -> str:
    return slot.strftime("%H:%M")


def volume_history(spark, dataset: str, counts_per_slot: list[int]) -> None:
    rows = []
    for slot in PERIODS:
        for i, n in enumerate(counts_per_slot):
            day = date(2026, 1, 1 + i)
            rows.append({"evaluation_id": f"{dataset}-{label(slot)}-{i}", "run_id": f"r{i}", "event_id": f"e{i}",
                         "dataset": dataset, "check_id": "T1_VOLUME", "execution_type": "NORMAL",
                         "state": "PASSED", "population": n, "violations": 0,
                         "group_values": {"slot": label(slot), "slot_time": f"{day} {label(slot)}"},
                         "window_start": datetime(2026, 1, 1 + i, 11), "window_end": datetime(2026, 1, 1 + i, 12),
                         "run_date": day})
    append_rows(spark, DQ, "dq_check_result", rows)


def loads_per_slot(sizes: dict[str, int]) -> list[dict]:
    rows = []
    for slot in PERIODS:
        when = (slot + timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:00:000000")
        rows += [gold_row(sub_id=f"{label(slot)}-{i}", loaded=when) for i in range(sizes.get(label(slot), 10))]
    return rows


def test_volume_judges_each_slot_and_flags_the_short_load(spark) -> None:
    volume_history(spark, "vol_six", [10] * 7)
    # Six loads in the window; the 20:00 load wrote less than half its usual 10 rows.
    create_table(spark, "t1.vol_six", GOLD_COLUMNS, loads_per_slot({"20:00": 4}))
    results = run(spark, Volume(), gold("t1.vol_six", dataset="vol_six", load_time=UTC_LOAD), feed=SIX_SLOTS)
    by_slot = {r.group_values["slot"]: r for r in results}
    assert sorted(by_slot) == ["00:00", "04:00", "08:00", "12:00", "16:00", "20:00"]
    assert by_slot["20:00"].group_values["slot_time"] == "2026-01-15 20:00"
    assert (by_slot["20:00"].state, by_slot["20:00"].population, by_slot["20:00"].violations) == ("FAILED", 4, 1)
    assert by_slot["20:00"].expected == "5 to 15 rows (median 10 of the last 7 loads)"
    assert all(r.state == "PASSED" and r.population == 10 for slot, r in by_slot.items() if slot != "20:00")


def test_volume_needs_seven_earlier_loads_per_slot(spark) -> None:
    volume_history(spark, "vol_new", [10] * 3)
    create_table(spark, "t1.vol_new", GOLD_COLUMNS, loads_per_slot({}))
    results = run(spark, Volume(), gold("t1.vol_new", dataset="vol_new", load_time=UTC_LOAD), feed=SIX_SLOTS)
    assert len(results) == 6
    for r in results:
        assert (r.state, r.reason_category, r.reason_code) == ("DID_NOT_RUN", "BASELINE", "insufficient_history")
        assert r.population == 10  # recorded, so the history builds up


def test_volume_without_rows_is_empty_population(spark) -> None:
    create_table(spark, "t1.vol_empty", GOLD_COLUMNS)
    [r] = run(spark, Volume(), gold("t1.vol_empty", dataset="vol_empty", load_time=UTC_LOAD), feed=SIX_SLOTS)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


# --- T1_KEY_NULLS: rows in the window only, per group ---

def test_key_nulls_pass_fail_and_empty(spark) -> None:
    in_window, outside = local("2026-01-15 13:00"), local("2026-01-14 13:00")
    rows = [gold_row(loaded=in_window), gold_row(sub_id=None, loaded=in_window, source="SRC_B"),
            gold_row(mem_nbr="  ", loaded=in_window, source="SRC_B"), gold_row(sub_id=None, loaded=outside)]
    create_table(spark, "t1.nulls", GOLD_COLUMNS, rows)
    results = run(spark, KeyNulls(), gold("t1.nulls", group_by=["src_sys_nm"]))
    by_group = {r.group_values["src_sys_nm"]: r for r in results}
    assert (by_group["SRC_A"].state, by_group["SRC_A"].population) == ("PASSED", 1)  # its null row is outside
    assert (by_group["SRC_B"].state, by_group["SRC_B"].violations) == ("FAILED", 2)
    create_table(spark, "t1.nulls_empty", GOLD_COLUMNS, [gold_row(loaded=outside)])
    [r] = run(spark, KeyNulls(), gold("t1.nulls_empty"))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


# --- T1_SCHEMA_DRIFT: baseline, unchanged, changed ---

def record(spark, result, dataset: str) -> None:
    append_rows(spark, DQ, "dq_check_result", [{
        "evaluation_id": f"sd-{dataset}-{result.observed}", "run_id": "r", "event_id": "e", "dataset": dataset,
        "check_id": "T1_SCHEMA_DRIFT", "execution_type": "NORMAL", "state": result.state,
        "observed": result.observed, "detail": result.detail, "evaluated_at": datetime.now(UTC),
        "run_date": date(2026, 1, 16)}])


def test_schema_drift_baseline_then_pass_then_fail(spark) -> None:
    create_table(spark, "t1.drift", GOLD_COLUMNS, [gold_row()])
    [first] = run(spark, SchemaDrift(), gold("t1.drift"))
    assert (first.state, first.reason_code) == ("DID_NOT_RUN", "insufficient_history")
    record(spark, first, "gold")

    # Another dataset over the same table sees the same history.
    [same] = run(spark, SchemaDrift(), gold("t1.drift", dataset="gold_all"))
    assert (same.state, same.population, same.expected) == ("PASSED", len(GOLD_COLUMNS), first.observed)

    columns = [c for c in GOLD_COLUMNS if c[0] != "covrg_agrmt_id"] + [("src_lcts_v2", "STRING")]
    columns = [(n, "INT") if n == "mem_nbr" else (n, k) for n, k in columns]
    create_table(spark, "t1.drift", columns)
    [changed] = run(spark, SchemaDrift(), gold("t1.drift"))
    assert (changed.state, changed.violations) == ("FAILED", 3)
    detail = json.loads(changed.detail)
    assert (detail["added"], detail["removed"], detail["retyped"]) == (["src_lcts_v2"], ["covrg_agrmt_id"], ["mem_nbr"])
