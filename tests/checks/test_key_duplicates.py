"""T1_KEY_DUPLICATES can PASS, FAIL and be DID_NOT_RUN."""

from __future__ import annotations

from datetime import datetime, timezone

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.registry import CHECKS, checks_for, pattern_check_ids
from hcsc.datalake.dre.checks.tier1.key_duplicates import KeyDuplicates
from hcsc.datalake.dre.config.models import Dataset
from tests.fixtures.settings import SETTINGS
from tests.fixtures.layers import GOLD_COLUMNS, create_table, gold_row

EVENT = Event("gold", datetime(2025, 12, 31, tzinfo=timezone.utc), datetime(2026, 1, 1, tzinfo=timezone.utc))
KEY = ["sub_id", "mem_nbr", "mbr_mbrshp_covrg_eff_dt", "covrg_agrmt_id"]


def gold(table: str, **extra) -> Dataset:
    fields = {"dataset": "gold", "table": table, "layer": "GOLD", "key": KEY, "key_unique": True,
              "key_normalise": {"sub_id": "strip_leading_zeros"}, **extra}
    return Dataset(**fields)


def run(spark, ds: Dataset):
    results, _ = run_check(KeyDuplicates(), CheckContext(spark, ds, None, SETTINGS), EVENT)
    return results


def test_passes_with_unique_keys(spark) -> None:
    create_table(spark, "chk_dup.unique", GOLD_COLUMNS, [gold_row(), gold_row(sub_id="000123402")])
    [result] = run(spark, gold("chk_dup.unique"))
    assert (result.state, result.population, result.violations) == ("PASSED", 2, 0)


def test_fails_on_duplicates_after_key_normalise(spark) -> None:
    # 000123401 and 123401 are the same subscriber once leading zeros are stripped.
    rows = [gold_row(sub_id="000123401"), gold_row(sub_id="123401"), gold_row(sub_id="000123402")]
    create_table(spark, "chk_dup.padded", GOLD_COLUMNS, rows)
    [result] = run(spark, gold("chk_dup.padded"))
    assert (result.state, result.population, result.violations) == ("FAILED", 2, 1)
    assert result.observed == "1 of 2 keys have more than one row"
    assert result.expected == "0 keys with more than one row"


def test_groups_and_feed_filter(spark) -> None:
    rows = [gold_row(source="SRC_A"), gold_row(source="SRC_A"), gold_row(source="SRC_B"), gold_row(source="SRC_C")]
    create_table(spark, "chk_dup.grouped", GOLD_COLUMNS, rows)
    results = run(spark, gold("chk_dup.grouped", group_by=["src_sys_nm"], feed_filter="src_sys_nm <> 'SRC_C'"))
    by_group = {r.group_values["src_sys_nm"]: r for r in results}
    assert set(by_group) == {"SRC_A", "SRC_B"}
    assert (by_group["SRC_A"].state, by_group["SRC_A"].violations) == ("FAILED", 1)
    assert by_group["SRC_B"].state == "PASSED"


def test_group_column_may_also_be_a_key_column(spark) -> None:
    create_table(spark, "chk_dup.keygroup", GOLD_COLUMNS, [gold_row(), gold_row(source="SRC_B")])
    results = run(spark, gold("chk_dup.keygroup", key=["src_sys_nm", *KEY], group_by=["src_sys_nm"]))
    assert sorted((r.group_values["src_sys_nm"], r.state) for r in results) == [("SRC_A", "PASSED"), ("SRC_B", "PASSED")]


def test_did_not_run_on_empty_table(spark) -> None:
    create_table(spark, "chk_dup.empty", GOLD_COLUMNS)
    for ds in (gold("chk_dup.empty"), gold("chk_dup.empty", group_by=["src_sys_nm"])):
        [result] = run(spark, ds)
        assert (result.state, result.reason_code) == ("DID_NOT_RUN", "empty_population")


def test_did_not_run_on_missing_group_column(spark) -> None:
    create_table(spark, "chk_dup.nogroup", GOLD_COLUMNS, [gold_row()], rename={"src_sys_nm": "source"})
    [result] = run(spark, gold("chk_dup.nogroup", group_by=["src_sys_nm"]))
    assert (result.state, result.reason_code) == ("DID_NOT_RUN", "column_missing")


def test_applies_only_to_key_unique_datasets() -> None:
    assert CHECKS["T1_KEY_DUPLICATES"] in checks_for(gold("x.y"), "FILE_CYCLIC")
    assert CHECKS["T1_KEY_DUPLICATES"] not in checks_for(gold("x.y", key_unique=False), "FILE_CYCLIC")
    assert [c.check_id for c in checks_for(gold("x.y"), None)] == [  # table-wide: patterns/table_wide.yaml
        "T1_SCHEMA_DRIFT", "T1_KEY_NULLS", "T1_KEY_DUPLICATES"]


def test_every_pattern_lists_known_checks() -> None:
    for pattern in ("FILE_CYCLIC", "FILE_PERIODIC", "TABLE_MERGE", "TABLE_WIDE"):
        assert "T1_KEY_DUPLICATES" in pattern_check_ids(pattern)
