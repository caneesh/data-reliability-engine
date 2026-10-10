"""The seven gold rule templates can PASS, FAIL and be DID_NOT_RUN, grouped by the rule's group_by."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.keys import key_exprs
from hcsc.datalake.dre.checks.rules.rule_check import RuleCheck
from hcsc.datalake.dre.config.models import Dataset, Rule
from tests.fixtures.layers import GOLD_COLUMNS, create_table, gold_row
from tests.fixtures.settings import SETTINGS

UTC = timezone.utc
EVENT = Event("gold", datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 1, 16, 12, tzinfo=UTC))
OPEN = "mbr_mbrshp_covrg_end_dt = '9999-12-31'"
PERIOD = [("member_id", "STRING"), ("start_dt", "STRING"), ("end_dt", "STRING")]


def gold(table: str, **extra) -> Dataset:
    fields = {"dataset": "gold", "table": table, "layer": "GOLD",
              "key": ["sub_id", "mem_nbr", "mbr_mbrshp_covrg_eff_dt", "covrg_agrmt_id"],
              "key_normalise": {"sub_id": "strip_leading_zeros"}, **extra}
    return Dataset(**fields)


def rule(template: str, params: dict, group_by: tuple[str, ...] = ("src_sys_nm",)) -> Rule:
    return Rule(rule=f"test_{template}", template=template, dataset="gold", params=params,
                group_by=list(group_by), owner="membership-gold", status="approved", severity="high")


def run(spark, check: RuleCheck, ds: Dataset) -> list:
    results, _ = run_check(check, CheckContext(spark, ds, None, SETTINGS, "UTC"), EVENT)
    return sorted(results, key=lambda r: sorted((r.group_values or {}).items()))


def states(results) -> list[tuple]:
    return [(r.group_values, r.state, r.population, r.violations) for r in results]


def table(spark, name: str, rows: list[dict]) -> Dataset:
    create_table(spark, f"rules.{name}", GOLD_COLUMNS, rows)
    return gold(f"rules.{name}")


# --- max_rows_per_key ---

def test_max_rows_per_key(spark) -> None:
    check = RuleCheck(rule("max_rows_per_key", {"max": 1}))
    # Zero-padded and unpadded ids are the same key after key_normalise.
    ds = table(spark, "mrk", [gold_row(sub_id="000123401"), gold_row(sub_id="123401"),
                              gold_row(sub_id="7", source="SRC_B")])
    assert states(run(spark, check, ds)) == [({"src_sys_nm": "SRC_A"}, "FAILED", 1, 1),
                                             ({"src_sys_nm": "SRC_B"}, "PASSED", 1, 0)]
    assert check.severity == "high" and check.check_id == "test_max_rows_per_key"


def test_max_rows_per_key_did_not_run_on_an_empty_table(spark) -> None:
    [r] = run(spark, RuleCheck(rule("max_rows_per_key", {"max": 1})), table(spark, "mrk_empty", []))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


# --- max_open_rows_per_key ---

def test_max_open_rows_per_key(spark) -> None:
    check = RuleCheck(rule("max_open_rows_per_key", {"open_when": OPEN, "max": 1}))
    ds = table(spark, "morp", [gold_row(), gold_row(end="2026-06-30"),                 # one open: fine
                               gold_row(sub_id="2", source="SRC_B"), gold_row(sub_id="2", source="SRC_B")])
    assert states(run(spark, check, ds)) == [({"src_sys_nm": "SRC_A"}, "PASSED", 1, 0),
                                             ({"src_sys_nm": "SRC_B"}, "FAILED", 1, 1)]
    [r] = run(spark, check, table(spark, "morp_empty", []))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


# --- column_order ---

def test_column_order_parses_dates_and_ignores_nulls(spark) -> None:
    params = {"lower": "mbr_mbrshp_covrg_eff_dt", "upper": "mbr_mbrshp_covrg_end_dt", "format": "yyyy-MM-dd"}
    ds = table(spark, "co", [gold_row(eff="2026-01-01", end="2026-06-30"),
                             gold_row(eff="2026-03-01", end="2026-02-01"),        # end before start
                             gold_row(eff="2026-01-01", end=None)])               # null: left out
    [r] = run(spark, RuleCheck(rule("column_order", params)), ds)
    assert states([r]) == [({"src_sys_nm": "SRC_A"}, "FAILED", 2, 1)]
    [r] = run(spark, RuleCheck(rule("column_order", {**params, "nulls_fail": True})), ds)
    assert (r.state, r.population, r.violations) == ("FAILED", 3, 2)
    ok = table(spark, "co_ok", [gold_row(eff="2026-01-01", end="2026-06-30")])
    [r] = run(spark, RuleCheck(rule("column_order", params)), ok)
    assert r.state == "PASSED"


def test_column_order_never_compares_strings(spark) -> None:
    # As strings '9/1/2026' > '10/1/2026'; parsed, the end is after the start.
    params = {"lower": "mbr_mbrshp_covrg_eff_dt", "upper": "mbr_mbrshp_covrg_end_dt", "format": "M/d/yyyy"}
    ds = table(spark, "co_fmt", [gold_row(eff="9/1/2026", end="10/1/2026")])
    [r] = run(spark, RuleCheck(rule("column_order", params)), ds)
    assert (r.state, r.violations) == ("PASSED", 0)


def test_column_order_did_not_run_when_every_pair_is_null(spark) -> None:
    params = {"lower": "mbr_mbrshp_covrg_eff_dt", "upper": "mbr_mbrshp_covrg_end_dt", "format": "yyyy-MM-dd"}
    [r] = run(spark, RuleCheck(rule("column_order", params)), table(spark, "co_null", [gold_row(end=None)]))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


# --- superseded_still_open ---

SUPERSEDED = {"group_key": ["sub_id", "mem_nbr"], "order_column": "mbr_mbrshp_covrg_eff_dt",
              "format": "yyyy-MM-dd", "open_when": OPEN}


def test_superseded_still_open(spark) -> None:
    check = RuleCheck(rule("superseded_still_open", SUPERSEDED))
    ds = table(spark, "sso", [gold_row(eff="2026-01-01", end="2026-02-28"), gold_row(eff="2026-03-01"),  # fine
                              gold_row(sub_id="2", eff="2026-01-01", source="SRC_B"),
                              gold_row(sub_id="2", eff="2026-03-01", source="SRC_B")])               # older open
    assert states(run(spark, check, ds)) == [({"src_sys_nm": "SRC_A"}, "PASSED", 1, 0),
                                             ({"src_sys_nm": "SRC_B"}, "FAILED", 2, 1)]
    [r] = run(spark, check, table(spark, "sso_none_open", [gold_row(end="2026-02-28")]))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


# --- child_within_parent ---

def test_child_within_parent(spark) -> None:
    create_table(spark, "rules.cwp_parent", PERIOD, [
        {"member_id": "123401", "start_dt": "2026-01-01", "end_dt": "2026-06-30"},
        {"member_id": "555", "start_dt": "2026-01-01", "end_dt": None}])                   # open-ended
    parent = Dataset(dataset="example_parent", table="rules.cwp_parent", layer="GOLD", key=["member_id"])
    params = {"parent_dataset": "example_parent", "join": {"sub_id": "member_id"},
              "child_start": "mbr_mbrshp_covrg_eff_dt", "parent_start": "start_dt", "parent_end": "end_dt",
              "format": "yyyy-MM-dd"}
    check = RuleCheck(rule("child_within_parent", params), parent)
    ds = table(spark, "cwp", [gold_row(sub_id="000123401", eff="2026-03-01"),               # within (normalised)
                              gold_row(sub_id="555", eff="2030-01-01", source="SRC_B"),     # open-ended parent
                              gold_row(sub_id="123401", eff="2026-08-01", source="SRC_B"),  # after the end
                              gold_row(sub_id="999", source="SRC_B")])                      # no parent: left out
    assert states(run(spark, check, ds)) == [({"src_sys_nm": "SRC_A"}, "PASSED", 1, 0),
                                             ({"src_sys_nm": "SRC_B"}, "FAILED", 2, 1)]
    [r] = run(spark, RuleCheck(rule("child_within_parent", params), None), ds)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "invalid_config")
    [r] = run(spark, check, table(spark, "cwp_orphans", [gold_row(sub_id="999")]))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


# --- value_format ---

def test_value_format(spark) -> None:
    check = RuleCheck(rule("value_format", {"column": "mbr_mbrshp_covrg_end_dt", "pattern": r"^\d{4}-\d{2}-\d{2}$"}))
    ds = table(spark, "vf", [gold_row(), gold_row(end=None),                               # null: left out
                             gold_row(sub_id="2", source="SRC_B"), gold_row(sub_id="3", end="12/31/9999",
                                                                            source="SRC_B")])
    assert states(run(spark, check, ds)) == [({"src_sys_nm": "SRC_A"}, "PASSED", 1, 0),
                                             ({"src_sys_nm": "SRC_B"}, "FAILED", 2, 1)]
    [r] = run(spark, check, table(spark, "vf_nulls", [gold_row(end=None)]))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


# --- null_rate_max ---

def test_null_rate_max(spark) -> None:
    check = RuleCheck(rule("null_rate_max", {"column": "mbr_mbrshp_covrg_end_dt", "max_rate": 0.25}))
    ds = table(spark, "nrm", [gold_row(), gold_row(sub_id="2"), gold_row(sub_id="3"), gold_row(sub_id="4", end=None),
                              gold_row(source="SRC_B"), gold_row(sub_id="2", end=None, source="SRC_B")])
    results = run(spark, check, ds)
    assert states(results) == [({"src_sys_nm": "SRC_A"}, "PASSED", 4, 0),     # 25%: not over
                               ({"src_sys_nm": "SRC_B"}, "FAILED", 2, 1)]     # 50%
    assert results[1].observed == "50.00% null (1 of 2 rows)"
    ungrouped = RuleCheck(rule("null_rate_max", {"column": "mbr_mbrshp_covrg_end_dt", "max_rate": 0.25}, ()))
    [r] = run(spark, ungrouped, table(spark, "nrm_empty", []))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


# --- every template ---

def test_feed_filter_applies_to_every_template(spark) -> None:
    ds = table(spark, "filtered", [gold_row(), gold_row(source="SRC_B"), gold_row(source="SRC_B")])
    [r] = run(spark, RuleCheck(rule("max_rows_per_key", {"max": 1}, ())),
              gold(ds.table, feed_filter="src_sys_nm = 'SRC_A'"))
    assert (r.state, r.population) == ("PASSED", 1)


def test_missing_rule_column_is_did_not_run(spark) -> None:
    ds = table(spark, "missing_col", [gold_row()])
    [r] = run(spark, RuleCheck(rule("value_format", {"column": "not_a_column", "pattern": "x"})), ds)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "column_missing")


def test_parse_date_normaliser_matches_differently_written_dates(spark) -> None:
    ds = gold("rules.unused", key=["mbr_mbrshp_covrg_eff_dt"],
              key_normalise={"mbr_mbrshp_covrg_eff_dt": {"parse_date": "MM/dd/yyyy"}})
    [expr] = key_exprs(ds)
    [row] = spark.sql(f"SELECT {expr.replace('mbr_mbrshp_covrg_eff_dt', chr(39) + '01/02/2026' + chr(39))} AS v"
                      ).collect()
    assert row.v == "2026-01-02"


@pytest.mark.parametrize("value", ["parse_date", {"parse_date": ""}, {"parse_date": "MM'dd"},
                                   {"parse_date": "MM/dd/yyyy", "other": 1}])
def test_bad_normalisers_are_rejected(value) -> None:
    with pytest.raises(ValueError, match="not allowed"):
        gold("rules.unused", key_normalise={"sub_id": value})
