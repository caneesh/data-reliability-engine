"""T1_SOURCE_ATTRIBUTION (build step 5): each row of a union target is owned by exactly one source.

A union target is one physical table read by several configured datasets (its siblings), each through
its feed_filter and each with a union_partition contract. The check is shown able to FAIL (rows no
predicate matches, rows two predicates match), to PASS and to be DID_NOT_RUN (no rows, a sibling with
no feed_filter, an error), with reconciliation model example 9.2 as a test. Window and helpers are those
of test_hop.py.
"""

from __future__ import annotations

import json

import pytest

from hcsc.datalake.dre.checks.hop.attribution import SourceAttribution, attribution_sql, siblings
from hcsc.datalake.dre.config.models import Dataset, EdgeContract
from hcsc.datalake.dre.runner import Planned, once_per_table_checks
from hcsc.datalake.dre.store.local_setup import create_store
from tests.checks.test_hop import DQ, IN_WINDOW, gold, run
from tests.fixtures.layers import GOLD_COLUMNS, create_table, gold_row

UNION = EdgeContract(mapping="union_partition")


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


def sibling(table: str, name: str, source_filter: str | None, upstream: str) -> Dataset:
    """A dataset reading `table` as the partition of `upstream`, through `source_filter`."""
    return gold(table, dataset=name, feed_filter=source_filter, upstream=[upstream],
                key_map={upstream: gold(table).key_map["example_curated_enrollment"]},
                contracts={upstream: UNION})


def target(spark, name: str, sources: list[str | None]) -> tuple[Dataset, Dataset]:
    """The union target hop.<name> with one row per entry of `sources` (src_sys_nm), and its two
    siblings: SRC_A's and SRC_B's."""
    table = f"hop.{name}"
    rows = [gold_row(sub_id=f"{i:04d}", source=s, record=IN_WINDOW) for i, s in enumerate(sources)]
    create_table(spark, table, GOLD_COLUMNS, rows)
    return (sibling(table, f"{name}_a", "src_sys_nm = 'SRC_A'", "src_a_enrollment"),
            sibling(table, f"{name}_b", "src_sys_nm = 'SRC_B'", "src_b_enrollment"))


def attribution(spark, ds: Dataset, *others: Dataset):
    [r] = run(spark, SourceAttribution(), ds, None, datasets=list(others))
    return r


def test_model_example_9_2_two_unattributed_rows_fail(spark) -> None:
    """Reconciliation model 9.2: 100 rows, 60 SRC_A, 38 SRC_B, 1 with src_sys_nm NULL and 1 SRC_C:
    FAILED, the 2 unattributed rows counted with the rows each predicate owns. NULL never matches."""
    a, b = target(spark, "attr_model", ["SRC_A"] * 60 + ["SRC_B"] * 38 + [None, "SRC_C"])
    r = attribution(spark, a, b)
    assert (r.state, r.population, r.violations) == ("FAILED", 100, 2)
    assert r.observed == "100 rows: attr_model_a 60, attr_model_b 38; unattributed 2, overlap 0"
    detail = json.loads(r.detail)
    assert (detail["rows"], detail["unattributed"], detail["overlap"], detail["verified"]) == (100, 2, 0, False)
    assert [(p["dataset"], p["upstreams"], p["rows"], p["overlap"]) for p in detail["per_predicate"]] == [
        ("attr_model_a", ["src_a_enrollment"], 60, 0), ("attr_model_b", ["src_b_enrollment"], 38, 0)]
    # the same result whichever sibling carries the check
    assert attribution(spark, b, a).observed == r.observed


def test_disjoint_and_complete_predicates_pass(spark) -> None:
    a, b = target(spark, "attr_ok", ["SRC_A"] * 3 + ["SRC_B"] * 2)
    r = attribution(spark, a, b)
    assert (r.state, r.population, r.violations) == ("PASSED", 5, 0)
    assert json.loads(r.detail)["verified"] is True


def test_rows_two_predicates_match_overlap_and_fail(spark) -> None:
    a, b = target(spark, "attr_overlap", ["SRC_A"] * 3 + ["SRC_B"] * 2)
    wide = b.model_copy(update={"feed_filter": "src_sys_nm IN ('SRC_A', 'SRC_B')"})
    r = attribution(spark, a, wide)
    assert (r.state, r.population, r.violations) == ("FAILED", 5, 3)
    assert r.observed == ("5 rows: attr_overlap_a 3 (3 overlapping), attr_overlap_b 5 (3 overlapping); "
                          "unattributed 0, overlap 3")


def test_an_empty_table_is_did_not_run(spark) -> None:
    a, b = target(spark, "attr_empty", [])
    r = attribution(spark, a, b)
    assert (r.state, r.reason_category, r.reason_code) == ("DID_NOT_RUN", "DATA_UNAVAILABLE", "empty_population")


def test_a_sibling_without_a_feed_filter_is_invalid_config_naming_it(spark) -> None:
    a, b = target(spark, "attr_nofilter", ["SRC_A"])
    r = attribution(spark, a, b.model_copy(update={"feed_filter": None}))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "invalid_config")
    assert "attr_nofilter_b" in r.detail and "set each one's feed_filter" in r.detail


def test_an_error_inside_the_check_is_did_not_run(spark) -> None:
    a, b = target(spark, "attr_error", ["SRC_A"])
    r = attribution(spark, a, b.model_copy(update={"feed_filter": "no_such_column = 'SRC_B'"}))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "column_missing")


def test_siblings_are_the_union_datasets_on_the_same_table(spark) -> None:
    a, b = target(spark, "attr_siblings", ["SRC_A"])
    table_wide = gold(a.table, dataset="attr_siblings_all", key_map={}, owned_columns={}, upstream=[])
    elsewhere = sibling("hop.attr_other_table", "attr_elsewhere", "src_sys_nm = 'SRC_C'", "src_c_enrollment")
    assert [d.dataset for d in siblings([b, table_wide, elsewhere], a)] == ["attr_siblings_a", "attr_siblings_b"]
    assert siblings([], a) == [a]  # a context that knows no other dataset still has the dataset itself
    check = SourceAttribution()
    assert check.applies_to(a, "TABLE_MERGE") and not check.applies_to(table_wide, None)
    assert not check.applies_to(gold(a.table), "TABLE_MERGE")  # a dedup hop is not a union
    sql = attribution_sql(a.table, [a, b])
    assert "(src_sys_nm = 'SRC_A')" in sql and "(src_sys_nm = 'SRC_B')" in sql and "GROUP BY" not in sql


def test_no_key_value_leaves_the_check(spark) -> None:
    a, b = target(spark, "attr_private", ["SRC_A", "SRC_C"])
    r = attribution(spark, a, b)
    assert r.state == "FAILED" and "0001" not in json.dumps([r.observed, r.detail])


def test_one_evaluation_per_physical_table_and_reruns_keep_it(spark) -> None:
    """Selected once per physical table (runner.once_per_table) like the table checks, but it is not one
    of TABLE_CHECKS, which RERUN and REPLAY leave out: it judges a window's evidence."""
    from hcsc.datalake.dre import runner

    a, b = target(spark, "attr_plan", [])
    check = SourceAttribution()
    work = [Planned(None, a, check), Planned(None, b, check)]
    assert [p.dataset.dataset for p in once_per_table_checks(work)] == ["attr_plan_a"]
    assert check.check_id not in runner.TABLE_CHECKS
