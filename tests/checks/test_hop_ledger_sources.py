"""HOP_LEDGER on multi-source targets (build step 5; reconciliation model sections 6.1 and 6.3).

Union partition: a hop's attribution is `verified`, and `attribution_unverified` no longer qualifies
its claim, only when its own evaluation proves the siblings' feed_filters disjoint and complete over
the table, with the SQL T1_SOURCE_ATTRIBUTION runs; an overlapping or unattributed table keeps the
qualification and is never healthy. Survivorship: a key a higher-precedence upstream delivers as of the
same cutoff is superseded[<winner>] on a lower edge; otherwise the lower edge judges it normally, and a
target row is counted once, by the winner. Window and helpers are those of test_hop.py and
test_hop_ledger.py.
"""

from __future__ import annotations

import json
from datetime import date, datetime

import pytest

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.hop.attribution import SourceAttribution
from hcsc.datalake.dre.checks.hop.ledger import DISPOSITIONS, HopLedger
from hcsc.datalake.dre.config.models import Dataset, EdgeContract
from hcsc.datalake.dre.store.local_setup import create_store
from tests.checks.test_hop import DQ, EARLIER, EVENT, GOLD_LOAD, IN_WINDOW, curated, gold, run
from tests.checks.test_hop_ledger import LOADED_HERE, UP, ledger_of, lines, rule, scope
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_row, gold_row
from tests.fixtures.settings import SETTINGS
from tests.store.conftest import append_rows

A, B = "src_a_enrollment", "src_b_enrollment"
LATER_IN_WINDOW = datetime(2026, 1, 15, 11, 0)  # a newer version, still due in the window
BUCKETS = {d for d, _, _ in DISPOSITIONS}


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


# --- union partition: attribution verified by the evaluation's own proof ---

def union_target(spark, name: str, sources: list[str]) -> tuple[Dataset, Dataset, Dataset]:
    """(upstream, SRC_A's union dataset, its SRC_B sibling) over hop.<name>_gold: key 1 is SRC_A's and
    delivered; one more row per entry of `sources`, of that source."""
    create_table(spark, f"hop.{name}_cur", CURATED_COLUMNS, [curated_row(sub_id="1", updated=IN_WINDOW)])
    rows = [gold_row(sub_id="0001", record=IN_WINDOW, loaded=LOADED_HERE)]
    rows += [gold_row(sub_id=f"07{i:02d}", record=IN_WINDOW, loaded=LOADED_HERE, source=s) for i, s in enumerate(sources)]
    create_table(spark, f"hop.{name}_gold", GOLD_COLUMNS, rows)
    union = gold(f"hop.{name}_gold", dataset=f"gold_{name}", load_time=GOLD_LOAD, feed_filter="src_sys_nm = 'SRC_A'",
                 contracts={UP: EdgeContract(mapping="union_partition")})
    sibling = union.model_copy(update={"dataset": f"gold_{name}_b", "feed_filter": "src_sys_nm = 'SRC_B'",
                                       "upstream": ["other_source"], "contracts": {
                                           "other_source": EdgeContract(mapping="union_partition")}})
    return curated(f"hop.{name}_cur"), union, sibling


def healthy(spark, result, name: str) -> bool:
    """Whether v_latest_result counts this PASSED result healthy once stored (it is the one definition)."""
    at = datetime(2026, 1, 16, 12)
    append_rows(spark, DQ, "dq_check_result", [{
        "evaluation_id": f"src-{name}", "run_id": f"run-{name}", "dataset": name, "check_id": "HOP_LEDGER",
        "state": result.state, "window_end": at, "evaluated_at": at,
        "qualifications": ",".join(result.qualifications) or None, "run_date": date(2026, 1, 16)}])
    [row] = spark.table(f"{DQ}.v_latest_result").where(f"evaluation_id = 'src-{name}'").collect()
    return row.healthy


def test_a_union_whose_siblings_partition_the_table_is_verified_and_contract_proven(spark) -> None:
    up, union, sibling = union_target(spark, "lgs_verified", ["SRC_B"])
    r = ledger_of(spark, union, up, datasets=[sibling])
    assert (r.state, r.observed) == ("PASSED", "due 1 (pending 0): delivered 1; orphans 0")
    assert (scope(r)["attribution"], r.qualifications, scope(r)["claim"]) == ("verified", (), "contract-proven")
    proof = scope(r)["attribution_proof"]
    assert (proof["verified"], proof["rows"], proof["unattributed"], proof["overlap"], proof["siblings"]) == (
        True, 2, 0, 0, ["gold_lgs_verified", "gold_lgs_verified_b"])
    assert healthy(spark, r, "verified")
    # the check and the ledger read the same SQL: they agree
    [t1] = run(spark, SourceAttribution(), union, None, datasets=[sibling])
    assert (t1.state, t1.population) == ("PASSED", 2)


@pytest.mark.parametrize("case, sources, sibling_filter", [
    ("unattributed", ["SRC_B", "SRC_C"], "src_sys_nm = 'SRC_B'"),
    ("overlap", ["SRC_B"], "src_sys_nm IN ('SRC_A', 'SRC_B')"),
])
def test_an_unattributed_or_overlapping_table_keeps_the_qualification_and_is_never_healthy(
        spark, case: str, sources: list[str], sibling_filter: str) -> None:
    up, union, sibling = union_target(spark, f"lgs_{case}", sources)
    sibling = sibling.model_copy(update={"feed_filter": sibling_filter})
    r = ledger_of(spark, union, up, datasets=[sibling])
    assert r.state == "PASSED"  # the ledger itself balances and passes ...
    assert (scope(r)["attribution"], r.qualifications) == ("feed_filter_unverified", ("attribution_unverified",))
    assert scope(r)["claim"] == "declared contract, qualified: attribution_unverified"
    proof = scope(r)["attribution_proof"]
    assert proof["verified"] is False and "match no sibling's feed_filter" in proof["why"]
    assert (proof["unattributed"], proof["overlap"]) == ((1, 0) if case == "unattributed" else (0, 1))
    assert not healthy(spark, r, case)  # ... and is never healthy
    [t1] = run(spark, SourceAttribution(), union, None, datasets=[sibling])
    assert (t1.state, t1.violations) == ("FAILED", 1)


def test_a_proof_that_cannot_run_leaves_the_attribution_unverified_and_says_why(spark) -> None:
    up, union, sibling = union_target(spark, "lgs_broken", ["SRC_B"])
    broken = sibling.model_copy(update={"feed_filter": "no_such_column = 'SRC_B'"})
    r = ledger_of(spark, union, up, datasets=[broken])
    assert r.state == "PASSED" and r.qualifications == ("attribution_unverified",)
    assert scope(r)["attribution"] == "feed_filter_unverified"
    assert scope(r)["attribution_proof"]["why"].startswith("the attribution proof could not run")
    assert scope(r)["attribution_proof"]["rows"] is None


def test_an_empty_table_proves_nothing(spark) -> None:
    create_table(spark, "hop.lgs_empty_cur", CURATED_COLUMNS, [curated_row(sub_id="1", updated=datetime(2026, 1, 16, 6))])
    create_table(spark, "hop.lgs_empty_gold", GOLD_COLUMNS, [])
    union = gold("hop.lgs_empty_gold", dataset="gold_lgs_empty", load_time=GOLD_LOAD, feed_filter="src_sys_nm = 'SRC_A'",
                 contracts={UP: EdgeContract(mapping="union_partition")})
    r = ledger_of(spark, union, curated("hop.lgs_empty_cur"))
    assert (r.state, r.reason_code, r.qualifications) == ("DID_NOT_RUN", "empty_population", ("attribution_unverified",))
    assert "has no rows" in scope(r)["attribution_proof"]["why"]


# --- survivorship (model 9.2) ---

def survivorship(spark, name: str, a_rows: list, b_rows: list, gold_rows: list,
                 b_contract: dict | None = None) -> tuple[Dataset, Dataset, Dataset]:
    """(A, B, gold): gold keeps one record per key, by precedence [A, B]."""
    for side, rows in (("a", a_rows), ("b", b_rows)):
        create_table(spark, f"hop.{name}_{side}", CURATED_COLUMNS, rows)
    create_table(spark, f"hop.{name}_gold", GOLD_COLUMNS, gold_rows)
    a = curated(f"hop.{name}_a").model_copy(update={"dataset": A})
    b = curated(f"hop.{name}_b").model_copy(update={"dataset": B})
    km = gold("x.y").key_map[UP]
    precedence = [A, B]
    dn = gold(f"hop.{name}_gold", dataset=f"gold_{name}", upstream=[A, B], key_map={A: km, B: km}, contracts={
        A: EdgeContract(mapping="survivorship", precedence=precedence),
        B: EdgeContract(**{"mapping": "survivorship", "precedence": precedence, **(b_contract or {})})})
    return a, b, dn


def judged(spark, dn: Dataset, *ups: Dataset) -> dict[str, object]:
    ctx = CheckContext(spark, dn, None, SETTINGS, "UTC", DQ, upstreams={u.dataset: u for u in ups})
    results, _ = run_check(HopLedger(), ctx, EVENT)
    return {r.group_values["upstream"]: r for r in results}


def test_the_winner_delivers_and_the_loser_is_superseded(spark) -> None:
    """Model 9.2: member M in SRC_A and SRC_B, precedence [SRC_A, SRC_B], gold holds SRC_A's version.
    SRC_A's edge: delivered. SRC_B's edge: superseded[SRC_A] (its own, older version, which gold does
    not hold, would otherwise be divergent). The row is counted once, by the winner."""
    a, b, dn = survivorship(spark, "sv_a_wins", [curated_row(sub_id="1", updated=IN_WINDOW, end="2026-06-30")],
                            [curated_row(sub_id="1", updated=EARLIER)],
                            [gold_row(sub_id="0001", record=IN_WINDOW, end="2026-06-30")])
    got = judged(spark, dn, a, b)
    assert (got[A].state, got[A].observed) == ("PASSED", "due 1 (pending 0): delivered 1")
    assert (got[B].state, got[B].observed) == ("PASSED", f"due 1 (pending 0): superseded {A} 1")
    assert lines(got[B])[("superseded", A)] == 1 and lines(got[B])[("delivered", None)] == 0
    assert sum(lines(r)[("delivered", None)] for r in got.values()) == 1  # counted once, by the winner
    assert scope(got[B])["precedence"] == [A, B] and scope(got[B])["evidence_level"] == "observed"


@pytest.mark.parametrize("b_version, a_reads", [(EARLIER, "stale"), (LATER_IN_WINDOW, "divergent")])
def test_when_gold_holds_the_loser_the_winner_is_stale_or_divergent_and_nothing_is_superseded(
        spark, b_version: datetime, a_reads: str) -> None:
    """Model 9.2, the other way: gold holds SRC_B's version. SRC_A's edge reads it as stale (older than
    SRC_A's version) or divergent (newer, other values); SRC_B's edge does not count it superseded."""
    a, b, dn = survivorship(spark, f"sv_b_held_{a_reads}",
                            [curated_row(sub_id="1", updated=IN_WINDOW, end="2026-06-30")],
                            [curated_row(sub_id="1", updated=b_version)],
                            [gold_row(sub_id="0001", record=b_version)])
    got = judged(spark, dn, a, b)
    assert (got[A].state, got[A].observed) == ("FAILED", f"due 1 (pending 0): {a_reads} 1; fails: 1 {a_reads}")
    assert (got[B].state, got[B].observed) == ("PASSED", "due 1 (pending 0): delivered 1")
    assert lines(got[B])[("superseded", None)] == 0


def test_the_survivorship_ledger_balances_with_superseded_among_the_dispositions(spark) -> None:
    """SRC_B's edge, five due keys: 1 superseded (gold holds SRC_A's version); 9001 too, though SRC_B's
    exclusion matches it (superseded is ranked after deleted, so the loser's presence rules do not judge
    the winner's record here: an overlap diagnostic, not a breach); 2 delivered (SRC_B's alone);
    3 missing; 4 missing, because the winner's version is missing too, so SRC_B's edge judges it
    normally. Each due record in exactly one bucket."""
    a, b, dn = survivorship(spark, "sv_balance", [
        curated_row(sub_id="1", updated=IN_WINDOW, end="2026-06-30"),
        curated_row(sub_id="4", updated=IN_WINDOW),
        curated_row(sub_id="9001", updated=IN_WINDOW, end="2026-06-30"),
    ], [
        curated_row(sub_id="1", updated=EARLIER), curated_row(sub_id="2", updated=EARLIER),
        curated_row(sub_id="3", updated=EARLIER), curated_row(sub_id="4", updated=EARLIER),
        curated_row(sub_id="9001", updated=EARLIER),
    ], [
        gold_row(sub_id="0001", record=IN_WINDOW, end="2026-06-30"), gold_row(sub_id="0002", record=EARLIER),
        gold_row(sub_id="9001", record=IN_WINDOW, end="2026-06-30"),
    ], b_contract={"exclusions": [rule("drop_test", "subscriberidnumber LIKE '900%'", 0.9)]})
    got = judged(spark, dn, a, b)
    loser = got[B]
    assert (loser.state, loser.population, loser.violations) == ("FAILED", 5, 2)
    buckets = {x["line"]: x["records"] for x in loser.reconciliation if x["line"] in BUCKETS}
    assert buckets == {"unassessable": 0, "deleted": 0, "superseded": 2, "deleted_not_applied": 0, "excluded": 0,
                       "excluded_but_present": 0, "rejected": 0, "delivered": 1, "divergent": 0, "stale": 0,
                       "missing": 2}
    assert sum(buckets.values()) == loser.population == lines(loser)[("due", None)]
    assert lines(loser)[("overlap", "superseded>excluded_but_present")] == 1
    winner = got[A]
    assert (winner.state, winner.observed) == ("FAILED", "due 3 (pending 0): delivered 2, missing 1; fails: 1 missing")


def test_a_winner_that_cannot_be_read_makes_the_loser_did_not_run(spark) -> None:
    a, b, dn = survivorship(spark, "sv_no_winner", [], [curated_row(sub_id="1", updated=EARLIER)],
                            [gold_row(sub_id="0001", record=EARLIER)])
    got = judged(spark, dn, b)  # SRC_A is not configured
    assert (got[B].state, got[B].reason_code) == ("DID_NOT_RUN", "invalid_config")
    assert A in got[B].detail
    assert (got[A].state, got[A].reason_code) == ("DID_NOT_RUN", "invalid_config")


def test_no_key_value_leaves_a_survivorship_ledger(spark) -> None:
    a, b, dn = survivorship(spark, "sv_private", [curated_row(sub_id="555123", updated=IN_WINDOW)],
                            [curated_row(sub_id="555123", updated=EARLIER)],
                            [gold_row(sub_id="555123", record=IN_WINDOW)])
    got = judged(spark, dn, a, b)
    assert "555123" not in json.dumps([[r.observed, r.detail, list(r.reconciliation)] for r in got.values()],
                                      default=str)
