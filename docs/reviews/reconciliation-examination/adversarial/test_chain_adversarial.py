"""Adversarial tests of CHAIN_COMPLETENESS (examination, not part of the project's suite).

The chain's fixtures are tests/checks/test_chain.py's: raw -> curated -> gold, curated sla 4 h and gold's
8 h, window [2026-01-15 12:00, 2026-01-16 12:00) UTC. Each docstring says CORRECT or FINDING.
"""

from __future__ import annotations

from tests.checks.test_chain import (
    EDGE1, EDGE2, SECRET, chain_of, layers, raw_record, scope, through,
)
from tests.fixtures.layers import curated_row, gold_row
from tests.checks.test_chain import CURATED_AT, GOLD_AT


def outcomes(r) -> dict:
    return {k: v for k, v in scope(r)["outcomes"].items() if k in ("due", "reached", "lost", "accounted", "unobservable")}


def test_c01_equal_counts_at_every_layer_with_different_records_fails_at_the_first_edge(spark) -> None:
    """CORRECT: raw {1, 2}, curated {1, 3}, gold {1, 3}: counts agree at every layer, but record 2 never
    left raw. Lost 1 at raw -> curated; record 3 is not a source record, so the chain does not count it."""
    r1, c1, g1 = through("123401")
    datasets = layers(spark, "cx_c01", [r1, raw_record("123402")],
                      [c1, curated_row(sub_id="123403", updated=CURATED_AT)],
                      [g1, gold_row(sub_id="000123403", record=GOLD_AT)])
    r = chain_of(spark, datasets, secret=SECRET)
    assert (r.state, r.violations) == ("FAILED", 1)
    assert scope(r)["first_observed_gap"]["edge"] == EDGE1


def test_c02_a_record_that_disappears_and_reappears_downstream_is_lost_where_it_disappeared(spark) -> None:
    """CORRECT: raw {1}, curated {}, gold {1}. The chain follows each edge: record 1 is lost at raw ->
    curated, even though gold holds a row with its key (another writer, or a stale row)."""
    r1, _, g1 = through("123401")
    datasets = layers(spark, "cx_c02", [r1], [], [g1])
    r = chain_of(spark, datasets, secret=SECRET)
    assert r.state == "FAILED" and scope(r)["first_observed_gap"]["edge"] == EDGE1


def test_c03_different_records_lost_at_different_edges_are_each_counted(spark) -> None:
    """CORRECT: record 2 lost at raw -> curated, record 3 lost at curated -> gold: lost 2; the first observed
    gap is raw -> curated; by_edge names both edges."""
    r1, c1, g1 = through("123401")
    r3, c3, _ = through("123403")
    datasets = layers(spark, "cx_c03", [r1, raw_record("123402"), r3], [c1, c3], [g1])
    r = chain_of(spark, datasets, secret=SECRET)
    assert (r.state, outcomes(r)["lost"]) == ("FAILED", 2)
    assert set(scope(r)["by_edge"]) >= {EDGE1, EDGE2}
    assert scope(r)["first_observed_gap"]["edge"] == EDGE1


def test_c04_a_wrong_owned_value_at_the_last_edge_is_lost_not_reached(spark) -> None:
    """CORRECT: gold holds record 1 at its version with a wrong end date (an owned column): divergent at
    curated -> gold, which the chain counts as lost."""
    r1, c1, _ = through("123401")
    datasets = layers(spark, "cx_c04", [r1], [c1], [gold_row(sub_id="000123401", record=GOLD_AT, end="1999-01-01")])
    r = chain_of(spark, datasets, secret=SECRET)
    assert r.state == "FAILED" and outcomes(r)["lost"] == 1


def test_c05_a_key_separator_collision_passes_through_the_chain(spark) -> None:
    """FINDING F-02 (false PASS) end to end: raw holds (sub 1, member '01|X') and (sub '1|01', member 'X').
    Both read as one key at every layer; curated and gold hold only the first: PASSED, due 1."""
    raw_a = {**raw_record("1"), "membernumber": "01|X"}
    raw_b = {**raw_record("1|01"), "membernumber": "X"}
    cur_a = curated_row(sub_id="1", mem_nbr="01|X", updated=CURATED_AT)
    gold_a = gold_row(sub_id="0001", mem_nbr="01|X", record=GOLD_AT)
    datasets = layers(spark, "cx_c05", [raw_a, raw_b], [cur_a], [gold_a])
    r = chain_of(spark, datasets, secret=SECRET)
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)
