"""HOP_LEDGER (Phase 1 build step 3): every due upstream record in exactly one disposition.

Each rule is shown able to FAIL, to PASS and to be DID_NOT_RUN. Window and helpers are those of
test_hop.py: [2026-01-15 12:00, 2026-01-16 12:00) UTC with an 8-hour SLA, so an upstream key is
due when its version under judgement was loaded in [2026-01-15 04:00, 2026-01-16 04:00). Curated's
load time is its record time; gold's sub_id is zero-padded and normalised.

Proof scenarios of the Phase 1 assessment, at check level: 1 (a lost record is FAILED, never
PASSED), 2 (an approved exclusion accounts for a record, an over-broad one fails its guard),
3 (a reject accounts for a record only under an authorising policy), 6 (an unapplied deletion is a
breach) and 10 (an inferred contract never claims contract-proven completeness).
"""

from __future__ import annotations

import json
from datetime import date, datetime

import pytest

from hcsc.datalake.dre.checks.hop import ledger as ledger_module
from hcsc.datalake.dre.checks.hop.ledger import DISPOSITIONS, HopLedger, counted
from hcsc.datalake.dre.config.models import (
    Dataset, Deletion, EdgeContract, Effective, Exclusion, RejectReason, RejectsSource, Tolerance,
)
from hcsc.datalake.dre.store.local_setup import create_store
from tests.checks.test_hop import BEFORE, DQ, EARLIER, EVENT, GOLD_LOAD, IN_WINDOW, curated, gold, run, tables
from tests.fixtures.layers import CURATED_COLUMNS, create_table, curated_row, gold_row

UP = "example_curated_enrollment"
PENDING = datetime(2026, 1, 16, 6, 0)    # loaded upstream after the last due load time, before the cutoff
LATE = datetime(2026, 1, 16, 13, 0)      # loaded upstream after the window end: not seen as of the cutoff
REJECT_COLUMNS = [*CURATED_COLUMNS[:4], ("reason_code", "STRING"), ("rejected_at", "TIMESTAMP")]


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


def ledger_of(spark, ds: Dataset, up: Dataset | None, **kwargs):
    [r] = run(spark, HopLedger(), ds, up, **kwargs)
    assert r.group_values == {"upstream": UP}
    return r


def lines(result) -> dict[tuple[str, str | None], int | None]:
    return {(x["line"], x.get("label")): x["records"] for x in result.reconciliation}


def scope(result) -> dict:
    return json.loads(result.detail)


def contract(ds: Dataset, **fields) -> Dataset:
    return ds.model_copy(update={"contracts": {UP: EdgeContract(**{"mapping": "dedup", **fields})}})


def rule(rule_id: str, condition: str, max_share: float = 0.5, starts: date | None = None) -> Exclusion:
    return Exclusion(id=rule_id, version=1, condition=condition, effective=Effective(**{"from": starts}),
                     approved_by="CHG-0000", max_share=max_share)


# --- dispositions, the identity and the outcome ---

def test_every_due_record_delivered_passes_and_the_ledger_balances(spark) -> None:
    up, dn = tables(spark, "lg_ok", [curated_row(sub_id="1", updated=IN_WINDOW),
                                     curated_row(sub_id="2", updated=IN_WINDOW),
                                     curated_row(sub_id="3", updated=PENDING),
                                     curated_row(sub_id="4", updated=LATE),
                                     curated_row(sub_id="5", updated=BEFORE)],
                    [gold_row(sub_id="0001", record=IN_WINDOW), gold_row(sub_id="0002", record=IN_WINDOW)])
    r = ledger_of(spark, dn, up)
    assert (r.state, r.population, r.violations) == ("PASSED", 2, 0)
    assert r.observed == "due 2 (pending 1): delivered 2"
    got = lines(r)
    assert got[("due", None)] == 2 and got[("pending", None)] == 1 and got[("untimed", None)] == 0
    # every disposition has a line, zeros too, so the identity can be read off the store
    assert {x["line"] for x in r.reconciliation} >= {d for d, _, _ in DISPOSITIONS}
    assert sum(x["records"] for x in r.reconciliation if x["line"] in {d for d, _, _ in DISPOSITIONS}) == 2
    assert all(x["scope"] == r.detail for x in r.reconciliation)
    assert scope(r)["evidence_level"] == "observed"


def test_missing_stale_and_divergent_records_fail(spark) -> None:
    """Proof scenario 1: a lost record is FAILED, never PASSED."""
    up, dn = tables(spark, "lg_bad", [curated_row(sub_id="1", updated=IN_WINDOW),
                                      curated_row(sub_id="2", updated=IN_WINDOW),
                                      curated_row(sub_id="3", updated=IN_WINDOW, end="2026-06-30"),
                                      curated_row(sub_id="4", updated=IN_WINDOW)],
                    [gold_row(sub_id="0002", record=EARLIER), gold_row(sub_id="0003", record=IN_WINDOW),
                     gold_row(sub_id="0004", record=IN_WINDOW)])
    r = ledger_of(spark, dn, up)
    assert (r.state, r.population, r.violations) == ("FAILED", 4, 3)
    assert r.observed == ("due 4 (pending 0): delivered 1, divergent 1, stale 1, missing 1; "
                          "fails: 1 missing; 1 stale; 1 divergent")
    assert lines(r)[("missing", None)] == 1


def test_a_tolerance_lets_named_deficits_pass_up_to_its_count(spark) -> None:
    up, dn = tables(spark, "lg_tol", [curated_row(sub_id="1", updated=IN_WINDOW),
                                      curated_row(sub_id="2", updated=IN_WINDOW),
                                      curated_row(sub_id="3", updated=IN_WINDOW)],
                    [gold_row(sub_id="0003", record=IN_WINDOW)])
    one = ledger_of(spark, contract(dn, tolerance=Tolerance(missing=1)), up)
    assert (one.state, one.violations) == ("FAILED", 2)
    assert one.observed.endswith("fails: 2 missing (tolerated 1)")
    two = ledger_of(spark, contract(dn, tolerance=Tolerance(missing=2)), up)
    assert (two.state, two.violations) == ("PASSED", 2)  # tolerated deficits still count as violations


def test_the_latest_upstream_version_is_judged_and_a_later_one_waits(spark) -> None:
    """A version loaded after the cutoff belongs to a later window: the key is judged on the version
    it had at the cutoff. A version loaded inside the SLA makes the key pending."""
    up, dn = tables(spark, "lg_versions", [curated_row(sub_id="1", updated=IN_WINDOW),
                                           curated_row(sub_id="1", updated=LATE, end="2026-06-30"),
                                           curated_row(sub_id="2", updated=IN_WINDOW),
                                           curated_row(sub_id="2", updated=PENDING)],
                    [gold_row(sub_id="0001", record=IN_WINDOW), gold_row(sub_id="0002", record=IN_WINDOW)])
    r = ledger_of(spark, dn, up)
    assert (r.state, r.population, r.observed) == ("PASSED", 1, "due 1 (pending 1): delivered 1")


def test_nothing_due_is_did_not_run_and_still_records_its_lines(spark) -> None:
    up, dn = tables(spark, "lg_empty", [curated_row(sub_id="1", updated=PENDING)], [])
    r = ledger_of(spark, dn, up)
    assert (r.state, r.reason_category, r.reason_code) == ("DID_NOT_RUN", "DATA_UNAVAILABLE", "empty_population")
    assert lines(r)[("pending", None)] == 1 and lines(r)[("due", None)] == 0


def test_an_unbalanced_ledger_is_did_not_run_never_a_verdict(spark, monkeypatch) -> None:
    """The identity is asserted: a record in no bucket is an engine defect (PLATFORM), never PASSED."""
    up, dn = tables(spark, "lg_unbalanced", [curated_row(sub_id="1", updated=IN_WINDOW)],
                    [gold_row(sub_id="0001", record=IN_WINDOW)])
    real = ledger_module.counted
    monkeypatch.setattr(ledger_module, "counted", lambda rows: {**real(rows), "balanced": False})
    r = ledger_of(spark, dn, up)
    assert (r.state, r.reason_category, r.reason_code) == ("DID_NOT_RUN", "PLATFORM", "ledger_unbalanced")
    assert "1 in buckets for 1 due" in r.observed and r.reconciliation


def test_counted_finds_a_record_in_no_bucket_or_in_two() -> None:
    def row(line, disposition, label, n, keys):
        return type("Row", (), {"line": line, "disposition": disposition, "label": label, "n": n, "keys": keys})

    assert counted([row("due", "delivered", None, 2, 2)])["balanced"]
    assert not counted([row("due", "delivered", None, 1, 1), row("due", None, None, 1, 1)])["balanced"]
    assert not counted([row("due", "delivered", None, 2, 1)])["balanced"]  # a key twice in one bucket


# --- unassessable ---

def test_a_key_collision_after_normalisation_is_unassessable(spark) -> None:
    """Gold's 0001 and 001 are distinct raw keys that normalise to one key."""
    up, dn = tables(spark, "lg_collide", [curated_row(sub_id="1", updated=IN_WINDOW),
                                          curated_row(sub_id="2", updated=IN_WINDOW)],
                    [gold_row(sub_id="0001", record=IN_WINDOW), gold_row(sub_id="001", record=IN_WINDOW),
                     gold_row(sub_id="0002", record=IN_WINDOW)])
    r = ledger_of(spark, dn, up)
    assert (r.state, r.violations) == ("FAILED", 1)
    assert r.observed == "due 2 (pending 0): unassessable key collision 1, delivered 1; fails: 1 unassessable"
    assert lines(r)[("unassessable", "key collision")] == 1


def test_no_record_time_and_no_key_are_unassessable(spark) -> None:
    up, dn = tables(spark, "lg_unknown", [curated_row(sub_id="1", updated=IN_WINDOW),
                                          {**curated_row(sub_id=None, mem_nbr=None, eff=None, agreement=None,
                                                         updated=IN_WINDOW)}],
                    [{**gold_row(sub_id="0001"), "src_lcts": None}])
    r = ledger_of(spark, dn, up)
    assert (r.state, r.population, r.violations) == ("FAILED", 2, 2)
    assert lines(r)[("unassessable", "no record time")] == 1 and lines(r)[("unassessable", "no key")] == 1


def test_a_key_with_no_load_time_is_untimed_then_unassessable_on_the_full_sweep(spark) -> None:
    rows = [curated_row(sub_id="1", updated=IN_WINDOW), curated_row(sub_id="2", updated=BEFORE)]
    up, dn = tables(spark, "lg_untimed", rows, [gold_row(sub_id="0001", record=IN_WINDOW),
                                                gold_row(sub_id="0002", record=BEFORE)])
    no_load = curated(up.table, load_time={"column": "cdc_upd_ts", "format": "yyyy-MM-dd HH:mm:ss:SSSSSS",
                                           "timezone": "UTC"})
    create_table(spark, up.table, CURATED_COLUMNS, [{**rows[0], "cdc_upd_ts": "2026-01-15 10:00:00:000000"},
                                                    {**rows[1], "cdc_upd_ts": None}])
    normal = ledger_of(spark, dn, no_load)
    assert normal.observed == "due 1 (pending 0): delivered 1"  # key 2 was never loaded in the range
    sweep = ledger_of(spark, dn, no_load, full_sweep=True)
    assert (sweep.state, sweep.population) == ("FAILED", 2)
    assert lines(sweep)[("unassessable", "no load time")] == 1


# --- exclusions (proof scenario 2) ---

def test_an_approved_exclusion_accounts_for_an_absent_record(spark) -> None:
    up, dn = tables(spark, "lg_excl", [curated_row(sub_id="1", updated=IN_WINDOW),
                                       curated_row(sub_id="9001", updated=IN_WINDOW)],
                    [gold_row(sub_id="0001", record=IN_WINDOW)])
    r = ledger_of(spark, contract(dn, exclusions=[rule("drop_test", "subscriberidnumber LIKE '9%'")]), up)
    assert (r.state, r.population, r.violations) == ("PASSED", 2, 0)
    assert r.observed == "due 2 (pending 0): excluded drop_test@1 1, delivered 1"
    assert lines(r)[("excluded", "drop_test@1")] == 1
    assert scope(r)["evidence_level"] == "reconstructed"  # the claim ceiling: the weakest evidence it rests on
    # before the rule took effect the record is not covered, and stays missing
    later = ledger_of(spark, contract(dn, exclusions=[rule("drop_test", "subscriberidnumber LIKE '9%'",
                                                           starts=date(2026, 1, 16))]), up)
    assert (later.state, later.observed) == ("FAILED", "due 2 (pending 0): delivered 1, missing 1; fails: 1 missing")


def test_an_excluded_record_present_downstream_is_a_breach(spark) -> None:
    up, dn = tables(spark, "lg_excl_present", [curated_row(sub_id="9001", updated=IN_WINDOW),
                                               curated_row(sub_id="1", updated=IN_WINDOW)],
                    [gold_row(sub_id="9001", record=IN_WINDOW), gold_row(sub_id="0001", record=IN_WINDOW)])
    r = ledger_of(spark, contract(dn, exclusions=[rule("drop_test", "subscriberidnumber LIKE '9%'")]), up)
    assert (r.state, r.violations) == ("FAILED", 1)
    assert r.observed.endswith("fails: 1 excluded_but_present")


def test_an_exclusion_over_its_max_share_fails_its_volume_guard(spark) -> None:
    up, dn = tables(spark, "lg_guard", [curated_row(sub_id="9001", updated=IN_WINDOW),
                                        curated_row(sub_id="9002", updated=IN_WINDOW),
                                        curated_row(sub_id="1", updated=IN_WINDOW)],
                    [gold_row(sub_id="0001", record=IN_WINDOW)])
    broad = ledger_of(spark, contract(dn, exclusions=[rule("drop_test", "subscriberidnumber LIKE '9%'", 0.5)]), up)
    assert (broad.state, broad.violations) == ("FAILED", 0)
    assert "exclusion drop_test@1 covers 2 of 3 due (66.7%), over its max_share 50.0%" in broad.observed
    assert [x["class"] for x in broad.reconciliation if x["line"] == "guard"] == ["guard"]
    ok = ledger_of(spark, contract(dn, exclusions=[rule("drop_test", "subscriberidnumber LIKE '9%'", 0.7)]), up)
    assert ok.state == "PASSED"


def test_overlapping_rules_are_recorded_as_diagnostics(spark) -> None:
    """A record two exclusions match lands in the first; the second is an overlap, not a second bucket."""
    up, dn = tables(spark, "lg_overlap", [curated_row(sub_id="9001", updated=IN_WINDOW),
                                          curated_row(sub_id="1", updated=IN_WINDOW)],
                    [gold_row(sub_id="0001", record=IN_WINDOW)])
    rules = [rule("a", "subscriberidnumber = '9001'", 0.9), rule("b", "subscriberidnumber LIKE '9%'", 0.9)]
    rejects = rejects_table(spark, "lg_overlap", [("9001", "INVALID_CODE", datetime(2026, 1, 15, 11))])
    r = ledger_of(spark, contract(dn, exclusions=rules, rejects=rejects,
                                  reject_policy={"INVALID_CODE": RejectReason(disposition="authorised",
                                                                              max_share=0.9)}), up)
    assert (r.state, r.population) == ("PASSED", 2)
    assert lines(r)[("excluded", "a@1")] == 1 and ("excluded", "b@1") not in lines(r)
    assert lines(r)[("overlap", "excluded>rejected")] == 1
    assert r.observed.endswith("; overlaps 1")


# --- rejects (proof scenario 3) ---

def rejects_table(spark, name: str, rows: list[tuple[str, str, datetime]]) -> RejectsSource:
    table = f"hop.{name}_rejects"
    create_table(spark, table, REJECT_COLUMNS, [
        {"subscriberidnumber": sub, "membernumber": "01", "effectivedate": "2026-01-01",
         "qualifiedhealthplanid": "AGR-A", "reason_code": reason, "rejected_at": at} for sub, reason, at in rows])
    return RejectsSource(table=table)


def test_a_reject_accounts_for_a_record_only_under_an_authorising_policy(spark) -> None:
    up, dn = tables(spark, "lg_rej", [curated_row(sub_id="1", updated=IN_WINDOW),
                                      curated_row(sub_id="2", updated=IN_WINDOW),
                                      curated_row(sub_id="3", updated=IN_WINDOW),
                                      curated_row(sub_id="4", updated=IN_WINDOW)],
                    [gold_row(sub_id="0001", record=IN_WINDOW)])
    rejects = rejects_table(spark, "lg_rej", [("2", "INVALID_CODE", datetime(2026, 1, 15, 11)),
                                              ("3", "PARSE_ERROR", datetime(2026, 1, 15, 11)),
                                              ("4", "INVALID_CODE", datetime(2026, 1, 14, 11))])  # an older version
    policy = {"INVALID_CODE": RejectReason(disposition="authorised", max_share=0.5)}
    r = ledger_of(spark, contract(dn, rejects=rejects, reject_policy=policy), up)
    assert (r.state, r.population, r.violations) == ("FAILED", 4, 2)
    assert r.observed == ("due 4 (pending 0): rejected INVALID_CODE (authorised) 1, rejected PARSE_ERROR (defect) 1, "
                          "delivered 1, missing 1; fails: 1 rejected for a defect reason; 1 missing")
    classes = {x["label"]: x["class"] for x in r.reconciliation if x["line"] == "rejected"}
    assert classes == {"INVALID_CODE": "authorised", "PARSE_ERROR": "defect"}
    assert scope(r)["evidence_level"] == "reported"
    # with the defect gone and the old reject now covering a newer one, the authorised rejects pass
    rejects = rejects_table(spark, "lg_rej", [("2", "INVALID_CODE", datetime(2026, 1, 15, 11)),
                                              ("3", "INVALID_CODE", datetime(2026, 1, 15, 11)),
                                              ("4", "INVALID_CODE", datetime(2026, 1, 15, 11))])
    ok = ledger_of(spark, contract(dn, rejects=rejects, reject_policy={
        "INVALID_CODE": RejectReason(disposition="authorised", max_share=0.75)}), up)
    assert (ok.state, ok.violations) == ("PASSED", 0)
    guarded = ledger_of(spark, contract(dn, rejects=rejects, reject_policy=policy), up)
    assert guarded.state == "FAILED" and "rejects INVALID_CODE cover 3 of 4 due (75.0%)" in guarded.observed


def test_without_rejects_evidence_a_rejected_record_is_missing_and_said_so(spark) -> None:
    up, dn = tables(spark, "lg_norej", [curated_row(sub_id="1", updated=IN_WINDOW)], [])
    r = ledger_of(spark, contract(dn), up)
    assert r.state == "FAILED"
    assert any("no rejects evidence" in s for s in scope(r)["blind_spots"])


def test_a_declared_rejects_table_that_is_gone_is_insufficient_evidence(spark) -> None:
    up, dn = tables(spark, "lg_rejgone", [curated_row(sub_id="1", updated=IN_WINDOW)], [])
    r = ledger_of(spark, contract(dn, rejects=RejectsSource(table="hop.lg_no_such_rejects")), up)
    assert (r.state, r.reason_category, r.reason_code) == ("DID_NOT_RUN", "DEPENDENCY", "insufficient_evidence")


# --- deletions (proof scenario 6) ---

VOID = "to_date(enddate, 'yyyy-MM-dd') = to_date(effectivedate, 'yyyy-MM-dd')"
GOLD_VOID = "to_date(mbr_mbrshp_covrg_end_dt, 'yyyy-MM-dd') = to_date(mbr_mbrshp_covrg_eff_dt, 'yyyy-MM-dd')"


def test_a_soft_deletion_applied_is_deleted_and_one_not_applied_is_a_breach(spark) -> None:
    up, dn = tables(spark, "lg_soft", [curated_row(sub_id="1", updated=IN_WINDOW, end="2026-01-01"),
                                       curated_row(sub_id="2", updated=IN_WINDOW, end="2026-01-01")],
                    [gold_row(sub_id="0001", record=IN_WINDOW, end="2026-01-01"),
                     gold_row(sub_id="0002", record=EARLIER)])
    soft = contract(dn, mapping="merge", deletion=Deletion(tombstone=VOID, applied_as="soft", soft_when=GOLD_VOID))
    r = ledger_of(spark, soft, up)
    assert (r.state, r.violations) == ("FAILED", 1)
    assert r.observed == ("due 2 (pending 0): deleted 1, deleted_not_applied 1; fails: 1 deleted_not_applied")
    # without the deletion rule the unapplied void is only a stale version: the label is what the rule adds
    plain = ledger_of(spark, contract(dn, mapping="merge"), up)
    assert plain.observed.startswith("due 2 (pending 0): delivered 1, stale 1")


def test_deletions_pass_when_applied_soft_or_hard(spark) -> None:
    up, dn = tables(spark, "lg_del_ok", [curated_row(sub_id="1", updated=IN_WINDOW, end="2026-01-01"),
                                         curated_row(sub_id="2", updated=IN_WINDOW)],
                    [gold_row(sub_id="0001", record=IN_WINDOW, end="2026-01-01"),
                     gold_row(sub_id="0002", record=IN_WINDOW)])
    soft = ledger_of(spark, contract(dn, mapping="merge", deletion=Deletion(tombstone=VOID, applied_as="soft",
                                                                           soft_when=GOLD_VOID)), up)
    assert (soft.state, soft.observed) == ("PASSED", "due 2 (pending 0): deleted 1, delivered 1")
    hard = contract(dn, mapping="merge", deletion=Deletion(tombstone=VOID, applied_as="hard"))
    present = ledger_of(spark, hard, up)
    assert (present.state, present.violations) == ("FAILED", 1)  # hard: still present is not applied
    _, gone = tables(spark, "lg_del_hard", [], [gold_row(sub_id="0002", record=IN_WINDOW)])
    absent = ledger_of(spark, hard.model_copy(update={"table": gone.table}), up)
    assert (absent.state, absent.observed) == ("PASSED", "due 2 (pending 0): deleted 1, delivered 1")


# --- orphans ---

def test_a_row_here_no_upstream_row_maps_to_is_an_orphan(spark) -> None:
    up, dn = tables(spark, "lg_orphan", [curated_row(sub_id="1", updated=IN_WINDOW)],
                    [gold_row(sub_id="0001", record=IN_WINDOW, loaded="2026-01-15 14:00:00:000000"),
                     gold_row(sub_id="0077", record=IN_WINDOW, loaded="2026-01-15 14:00:00:000000"),
                     gold_row(sub_id="0078", record=IN_WINDOW, loaded="2026-01-10 14:00:00:000000")])
    loaded = dn.model_copy(update={"load_time": GOLD_LOAD})
    r = ledger_of(spark, loaded, up)
    assert (r.state, r.violations) == ("FAILED", 1)
    assert r.observed == "due 1 (pending 0): delivered 1; orphans 1; fails: 1 orphan"
    assert lines(r)[("orphan", None)] == 1 and scope(r)["orphans"] == "checked"
    _, clean = tables(spark, "lg_orphan_ok", [], [gold_row(sub_id="0001", record=IN_WINDOW,
                                                           loaded="2026-01-15 14:00:00:000000")])
    ok = ledger_of(spark, loaded.model_copy(update={"table": clean.table}), up)
    assert (ok.state, ok.observed) == ("PASSED", "due 1 (pending 0): delivered 1; orphans 0")


def test_orphans_are_not_checked_where_rows_here_cannot_be_attributed(spark) -> None:
    """A union_partition target is attributed to its source by its feed_filter (decision D5); without
    one, or with several upstreams, the rows here cannot be attributed and orphans are not counted."""
    up, dn = tables(spark, "lg_orphan_union", [curated_row(sub_id="1", updated=IN_WINDOW)],
                    [gold_row(sub_id="0001", record=IN_WINDOW, loaded="2026-01-15 14:00:00:000000"),
                     gold_row(sub_id="0077", record=IN_WINDOW, loaded="2026-01-15 14:00:00:000000", source="SRC_B")])
    union = contract(dn.model_copy(update={"load_time": GOLD_LOAD}), mapping="union_partition")
    r = ledger_of(spark, union, up)
    assert r.state == "PASSED" and ("orphan", None) not in lines(r)
    assert scope(r)["orphans"] == "not checked"
    assert any(s.startswith("orphans not checked: union_partition without a feed_filter")
               for s in scope(r)["blind_spots"])
    attributed = ledger_of(spark, union.model_copy(update={"feed_filter": "src_sys_nm = 'SRC_A'"}), up)
    assert (attributed.state, scope(attributed)["orphans"]) == ("PASSED", "checked")  # SRC_B's row is not ours
    assert lines(attributed)[("orphan", None)] == 0
    with_b = ledger_of(spark, union.model_copy(update={"feed_filter": "src_sys_nm IN ('SRC_A', 'SRC_B')"}), up)
    assert (with_b.state, lines(with_b)[("orphan", None)]) == ("FAILED", 1)
    two = union.model_copy(update={"key_map": {**union.key_map, "other_source": union.key_map[UP]}})
    [first, _] = run(spark, HopLedger(), two, up)
    assert any(s.startswith("orphans not checked: gold_lg_orphan_union has several upstreams")
               for s in scope(first)["blind_spots"])
    plain = ledger_of(spark, dn, up)  # no load_time here: orphans cannot be placed in a window
    assert any(s.startswith("orphans not checked: gold_lg_orphan_union has no load_time")
               for s in scope(plain)["blind_spots"])


# --- kinds, scope and claim (proof scenario 10) ---

@pytest.mark.parametrize("kind", ["aggregate", "join_contribution", "survivorship", "fan_out"])
def test_kinds_the_ledger_does_not_judge_are_unsupported(spark, kind: str) -> None:
    up, dn = tables(spark, f"lg_kind_{kind}", [curated_row(updated=IN_WINDOW)], [gold_row(record=IN_WINDOW)])
    r = ledger_of(spark, contract(dn, mapping=kind), up)
    assert (r.state, r.reason_category, r.reason_code) == ("DID_NOT_RUN", "CONFIGURATION", "unsupported_mapping")
    assert scope(r)["mapping"] == kind and scope(r)["note"].startswith(f"{kind} (declared)")
    assert not r.reconciliation


def test_an_inferred_aggregate_is_never_reconciled(spark) -> None:
    claims = Dataset(dataset="example_claims", table="hop.lg_claims", layer="CURATED", key=["memberid"])
    derived = Dataset(dataset=UP, layer="CURATED", key=["subscriberidnumber"],
                      derived={"from": "example_claims", "group_by": {"subscriberidnumber": "memberid"},
                               "columns": {"n": "count(*)"}})
    dn = gold("hop.lg_agg_gold", key_map={UP: {"sub_id": "subscriberidnumber"}}, key=["sub_id"], owned_columns={})
    ctx_up = {"example_claims": claims, UP: derived}
    from hcsc.datalake.dre.checks.base import CheckContext, run_check
    from tests.fixtures.settings import SETTINGS

    create_table(spark, "hop.lg_agg_gold", [("sub_id", "STRING"), ("src_lcts", "TIMESTAMP")], [])
    [r], _ = run_check(HopLedger(), CheckContext(spark, dn, None, SETTINGS, "UTC", DQ, upstreams=ctx_up), EVENT)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "unsupported_mapping")
    assert scope(r)["note"].startswith("aggregate (inferred from key_map)")
    assert scope(r)["claim"] == "inferred contract"


def test_only_a_declared_contract_claims_contract_proven_completeness(spark) -> None:
    up, dn = tables(spark, "lg_claim", [curated_row(updated=IN_WINDOW)], [gold_row(record=IN_WINDOW)])
    inferred = ledger_of(spark, dn, up)
    declared = ledger_of(spark, contract(dn), up)
    legacy = ledger_of(spark, contract(dn.model_copy(update={"hop_exclusions": {UP: "1 = 0"}})), up)
    assert inferred.state == declared.state == legacy.state == "PASSED"
    assert scope(inferred)["claim"] == "inferred contract"
    assert scope(inferred)["contract"] == "inferred from key_map"
    assert scope(declared)["claim"] == "contract-proven"
    assert scope(legacy)["claim"].endswith("not contract-proven")
    # the hash names the exact contract: a changed contract is a different hash
    assert len({scope(x)["contract_hash"] for x in (inferred, declared, legacy)}) == 3
    s = scope(declared)
    assert s["edge"] == f"{UP}->gold_lg_claim" and s["cutoff"] == EVENT.window_end.isoformat()
    assert s["grain"] == {"key": list(dn.key), "version": "src_lcts", "history": None}
    assert set(s) >= {"population", "window", "full_sweep", "evidence_levels", "blind_spots", "unverified_columns"}


def test_a_missing_upstream_or_configuration_is_did_not_run(spark) -> None:
    up, dn = tables(spark, "lg_dnr", [curated_row(updated=IN_WINDOW)], [])
    r = ledger_of(spark, dn, None)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "invalid_config")
    r = ledger_of(spark, dn, curated(up.table, load_time=None))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "invalid_config")
    r = ledger_of(spark, dn, curated("hop.lg_no_such_table"))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "table_missing")


def test_a_failure_inside_the_hop_is_did_not_run_with_a_reason(spark, monkeypatch) -> None:
    up, dn = tables(spark, "lg_boom", [curated_row(updated=IN_WINDOW)], [gold_row(record=IN_WINDOW)])

    def boom(*_args, **_kwargs):
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr(ledger_module, "counted", boom)
    r = ledger_of(spark, dn, up)
    assert r.state == "DID_NOT_RUN" and r.reason_code is not None and "synthetic" not in (r.detail or "")


def test_the_full_sweep_judges_every_key_past_its_deadline(spark) -> None:
    up, dn = tables(spark, "lg_sweep", [curated_row(sub_id="1", updated=BEFORE),
                                        curated_row(sub_id="2", updated=IN_WINDOW)],
                    [gold_row(sub_id="0002", record=IN_WINDOW)])
    normal = ledger_of(spark, dn, up)
    assert (normal.state, normal.population) == ("PASSED", 1)
    sweep = ledger_of(spark, dn, up, full_sweep=True)
    assert (sweep.state, sweep.population, sweep.violations) == ("FAILED", 2, 1)
    assert scope(sweep)["full_sweep"] is True


def test_no_key_value_leaves_the_check(spark) -> None:
    """Hard constraint 6: lines, observed text and scope carry counts, codes and labels only."""
    up, dn = tables(spark, "lg_private", [curated_row(sub_id="555123", updated=IN_WINDOW)], [])
    r = ledger_of(spark, dn, up)
    text = json.dumps([r.observed, r.detail, list(r.reconciliation)], default=str)
    assert "555123" not in text
