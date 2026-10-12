"""T1_FRESHNESS and T1_COLUMN_RANGE (spec section 6): each can FAIL, PASS and be DID_NOT_RUN."""

from __future__ import annotations

from datetime import datetime, timezone

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.registry import CHECKS
from hcsc.datalake.dre.config.models import Dataset
from tests.fixtures.layers import GOLD_COLUMNS, create_table, gold_row
from tests.fixtures.settings import SETTINGS

UTC = timezone.utc
# gold_row: record 2026-01-02 09:00, loaded 2026-01-02 10:15 (UTC in these tests)
EVENT = Event("gold", datetime(2026, 1, 2, 0, tzinfo=UTC), datetime(2026, 1, 3, 0, tzinfo=UTC))  # end: 15 h after
LOAD = {"column": "gld_lcts", "format": "yyyy-MM-dd HH:mm:ss:SSSSSS", "granularity": "minute"}


def gold(table: str, params: dict, check: str, **extra) -> Dataset:
    fields = {"dataset": "gold", "table": table, "layer": "GOLD", "key": ["sub_id"], "group_by": ["src_sys_nm"],
              "record_time": {"column": "src_lcts"}, "load_time": LOAD, "check_params": {check: params}, **extra}
    return Dataset(**fields)


def run(spark, check: str, ds: Dataset) -> dict:
    results, _ = run_check(CHECKS[check], CheckContext(spark, ds, None, SETTINGS, "UTC"), EVENT)
    return {r.group_values.get("src_sys_nm"): r for r in results}


# --- T1_FRESHNESS ---

def test_freshness_fails_when_the_latest_record_is_too_old_and_passes_within_the_age(spark) -> None:
    create_table(spark, "fresh.g", GOLD_COLUMNS, [
        gold_row(source="SRC_A"), gold_row(sub_id="2", source="SRC_A", record=datetime(2025, 12, 1)),
        gold_row(source="SRC_B", record=datetime(2026, 1, 2, 20))])
    by = run(spark, "T1_FRESHNESS", gold("fresh.g", {"max_age_hours": 12}, "T1_FRESHNESS"))
    a, b = by["SRC_A"], by["SRC_B"]
    assert (a.state, a.population, a.violations) == ("FAILED", 2, 1)
    assert a.observed == "latest record time 2026-01-02T09:00:00+00:00, 15.0 hours before the window end"
    assert a.expected == "latest record time within 12 hours of the window end"
    assert (b.state, b.violations) == ("PASSED", 0)
    by = run(spark, "T1_FRESHNESS", gold("fresh.g", {"max_age_hours": 16}, "T1_FRESHNESS"))
    assert by["SRC_A"].state == "PASSED"


def test_freshness_can_measure_the_load_time(spark) -> None:
    create_table(spark, "fresh.l", GOLD_COLUMNS, [gold_row()])
    [r] = run(spark, "T1_FRESHNESS", gold("fresh.l", {"max_age_hours": 13, "time": "load"}, "T1_FRESHNESS")).values()
    assert r.state == "FAILED" and r.observed.startswith("latest load time 2026-01-02T10:15:00+00:00, 13.8 hours")
    [r] = run(spark, "T1_FRESHNESS", gold("fresh.l", {"max_age_hours": 14, "time": "load"}, "T1_FRESHNESS")).values()
    assert r.state == "PASSED"


def test_freshness_did_not_run_paths(spark) -> None:
    create_table(spark, "fresh.empty", GOLD_COLUMNS, [])
    [r] = run(spark, "T1_FRESHNESS", gold("fresh.empty", {"max_age_hours": 1}, "T1_FRESHNESS")).values()
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")
    ds = gold("fresh.empty", {"max_age_hours": 1, "time": "load"}, "T1_FRESHNESS", load_time=None)
    [r] = run(spark, "T1_FRESHNESS", ds).values()
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "invalid_config")
    ds = gold("fresh.empty", {"max_age_hours": 1}, "T1_FRESHNESS", record_time={"column": "gone"})
    [r] = run(spark, "T1_FRESHNESS", ds).values()
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "column_missing")


def test_freshness_with_no_usable_time_is_a_failure_not_a_pass(spark) -> None:
    create_table(spark, "fresh.nulls", GOLD_COLUMNS, [gold_row(record=None), gold_row(sub_id="2", record=None)])
    [r] = run(spark, "T1_FRESHNESS", gold("fresh.nulls", {"max_age_hours": 1000}, "T1_FRESHNESS")).values()
    assert (r.state, r.population, r.violations, r.observed) == ("FAILED", 2, 1, "no usable record time in 2 rows")


# --- T1_COLUMN_RANGE ---

ROWS = [gold_row(mem_nbr="01"), gold_row(sub_id="2", mem_nbr="07"), gold_row(sub_id="3", mem_nbr=None),
        gold_row(sub_id="4", mem_nbr="03", source="SRC_B"),
        gold_row(sub_id="5", mem_nbr="99", loaded="2026-01-05 00:00:00:000000")]  # outside the window


def test_column_range_numeric_bounds(spark) -> None:
    create_table(spark, "range.g", GOLD_COLUMNS, ROWS)
    by = run(spark, "T1_COLUMN_RANGE", gold("range.g", {"column": "mem_nbr", "min": 1, "max": 5}, "T1_COLUMN_RANGE"))
    a, b = by["SRC_A"], by["SRC_B"]
    assert (a.state, a.population, a.violations) == ("FAILED", 2, 1)  # the null row is left out, 07 is over
    assert a.observed == "1 of 2 values of mem_nbr are out of range"
    assert a.expected == "every mem_nbr at least 1, at most 5"
    assert (b.state, b.population, b.violations) == ("PASSED", 1, 0)
    by = run(spark, "T1_COLUMN_RANGE", gold("range.g", {"column": "mem_nbr", "max": 10}, "T1_COLUMN_RANGE"))
    assert by["SRC_A"].state == "PASSED"


def test_column_range_allowed_set_and_nulls(spark) -> None:
    create_table(spark, "range.s", GOLD_COLUMNS, ROWS)
    by = run(spark, "T1_COLUMN_RANGE", gold("range.s", {"column": "mem_nbr", "allowed": ["01", "03"]}, "T1_COLUMN_RANGE"))
    assert (by["SRC_A"].state, by["SRC_A"].violations, by["SRC_B"].state) == ("FAILED", 1, "PASSED")
    by = run(spark, "T1_COLUMN_RANGE", gold("range.s", {"column": "mem_nbr", "allowed": ["01", "07"], "nulls_fail": True},
                                            "T1_COLUMN_RANGE"))
    a = by["SRC_A"]
    assert (a.state, a.population, a.violations) == ("FAILED", 3, 1)  # the null row counts, and fails
    assert a.expected == "every mem_nbr in ['01', '07'], none null"


def test_column_range_did_not_run_paths(spark) -> None:
    create_table(spark, "range.empty", GOLD_COLUMNS, [gold_row(mem_nbr=None)])
    [r] = run(spark, "T1_COLUMN_RANGE", gold("range.empty", {"column": "mem_nbr", "min": 0}, "T1_COLUMN_RANGE")).values()
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")  # only a null value: nothing to judge
    [r] = run(spark, "T1_COLUMN_RANGE", gold("range.empty", {"column": "nope", "min": 0}, "T1_COLUMN_RANGE")).values()
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "column_missing")
    [r] = run(spark, "T1_COLUMN_RANGE", gold("range.empty", {"column": "mem_nbr"}, "T1_COLUMN_RANGE")).values()
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "invalid_config")  # no bound
    ds = gold("range.empty", {"column": "mem_nbr", "min": 0}, "T1_COLUMN_RANGE", load_time=None)
    [r] = run(spark, "T1_COLUMN_RANGE", ds).values()
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "invalid_config")
