"""Hop checks read a hop's exclusions through its contract (build step 1 of the Phase 1 design).

The release 1 `hop_exclusions` entry is used exactly as written. A contract exclusion matches only
upstream rows loaded inside its effective span, so a test member loaded before its rule took
effect is still MISSING (proof scenario 2: an approved rule accounts for a record; a record the
rule did not cover yet does not pass). Window and helpers are those of test_hop.py.
"""

from __future__ import annotations

from datetime import date

import pytest

from hcsc.datalake.dre.checks.hop.common import exclusion_condition
from hcsc.datalake.dre.checks.hop.file_completeness import FileCompleteness
from hcsc.datalake.dre.checks.hop.key_currency import KeyCurrency
from hcsc.datalake.dre.config.models import Dataset, EdgeContract, Effective, Exclusion
from hcsc.datalake.dre.store.local_setup import create_store
from tests.checks.test_hop import DQ, IN_WINDOW, file_tables, run, tables
from tests.fixtures.layers import curated_row, gold_row

UP = "example_curated_enrollment"


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


def rule(rule_id: str, condition: str, starts: date | None = None, version: int = 1) -> Exclusion:
    return Exclusion(id=rule_id, version=version, condition=condition, effective=Effective(**{"from": starts}),
                     approved_by="CHG-0000", max_share=0.5)


def with_contract(ds: Dataset, up: str, *exclusions: Exclusion) -> Dataset:
    return ds.model_copy(update={"contracts": {up: EdgeContract(mapping="dedup", exclusions=list(exclusions))}})


def test_a_contract_exclusion_counts_only_rows_loaded_while_it_is_effective(spark) -> None:
    """Keys 9001 and 9002 are test members, both loaded on 2026-01-15. drop_old took effect on
    2026-01-01, so 9001 is excluded; drop_new takes effect on 2026-01-16, after 9002 was loaded, so
    9002 is still expected and is MISSING. Key 1 reached gold."""
    up, dn = tables(spark, "contract_excl",
                    [curated_row(sub_id="1", updated=IN_WINDOW), curated_row(sub_id="9001", updated=IN_WINDOW),
                     curated_row(sub_id="9002", updated=IN_WINDOW)],
                    [gold_row(sub_id="0001", record=IN_WINDOW)])
    contracted = with_contract(dn, UP, rule("drop_old", "subscriberidnumber = '9001'", date(2026, 1, 1)),
                               rule("drop_new", "subscriberidnumber = '9002'", date(2026, 1, 16)))
    [r] = run(spark, KeyCurrency(), contracted, up)
    assert (r.state, r.population, r.violations) == ("FAILED", 2, 1)
    assert r.observed == "1 missing and 0 stale of 2 keys; 1 key excluded by exclusions drop_old@1, drop_new@1"
    # once drop_new is effective from the start, both test members are excluded and the hop passes
    effective = with_contract(dn, UP, rule("drop_old", "subscriberidnumber = '9001'", date(2026, 1, 1)),
                              rule("drop_new", "subscriberidnumber = '9002'", date(2026, 1, 1)))
    [ok] = run(spark, KeyCurrency(), effective, up)
    assert (ok.state, ok.population, ok.violations) == ("PASSED", 1, 0)
    assert ok.observed.endswith("2 keys excluded by exclusions drop_old@1, drop_new@1")


def test_the_alias_and_an_equivalent_contract_judge_alike(spark) -> None:
    """hop_exclusions and a contract exclusion with the same condition and no dates give the same
    judgement; only the name in `observed` differs."""
    up, dn = tables(spark, "contract_alias",
                    [curated_row(sub_id="1", updated=IN_WINDOW), curated_row(sub_id="9001", updated=IN_WINDOW),
                     curated_row(sub_id="3", updated=IN_WINDOW)],
                    [gold_row(sub_id="0001", record=IN_WINDOW)])
    legacy = dn.model_copy(update={"hop_exclusions": {UP: "subscriberidnumber LIKE '9%'"}})
    contract = with_contract(dn, UP, rule("drop_test_members", "subscriberidnumber LIKE '9%'"))
    [a] = run(spark, KeyCurrency(), legacy, up)
    [b] = run(spark, KeyCurrency(), contract, up)
    assert (a.state, a.population, a.violations) == (b.state, b.population, b.violations) == ("FAILED", 2, 1)
    assert a.observed.endswith("1 key excluded by hop_exclusions")
    assert b.observed.endswith("1 key excluded by exclusions drop_test_members@1")


def test_the_legacy_condition_is_used_exactly_as_written() -> None:
    from tests.checks.test_hop import curated, gold

    up, dn = curated("t.up"), gold("t.dn")
    legacy = dn.model_copy(update={"hop_exclusions": {UP: "status = 'TEST'"}})
    assert exclusion_condition(legacy, UP, up, "UTC") == "status = 'TEST'"
    assert exclusion_condition(dn, UP, up, "UTC") is None
    dated = exclusion_condition(with_contract(dn, UP, rule("r", "status = 'TEST'", date(2026, 9, 1))),
                                UP, up, "America/Chicago")
    assert dated.startswith("((status = 'TEST') AND ") and "2026-09-01 05:00:00" in dated  # Chicago midnight in UTC


def test_a_contract_that_lists_no_exclusions_keeps_hop_exclusions() -> None:
    """Declaring only a hop's kind never drops its release 1 exclusion."""
    from tests.checks.test_hop import curated, gold

    up, dn = curated("t.up"), gold("t.dn")
    kind_only = dn.model_copy(update={"hop_exclusions": {UP: "status = 'TEST'"},
                                      "contracts": {UP: EdgeContract(mapping="merge")}})
    assert exclusion_condition(kind_only, UP, up, "UTC") == "status = 'TEST'"


def test_file_completeness_reads_the_contract_exclusions_too(spark) -> None:
    up, dn = file_tables(spark, "fc_contract",
                         [("a", IN_WINDOW, "rt_0001.seq"), ("x1", IN_WINDOW, "rt_0001.seq")],
                         [("a", IN_WINDOW, "rt_0001.seq")])
    contracted = with_contract(dn, "file_up", rule("drop_x", "id LIKE 'x%'"))
    [r] = run(spark, FileCompleteness(), contracted, up)
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)
    assert r.observed == "0 of 1 upstream files have fewer rows here; 1 row excluded by exclusions drop_x@1"
