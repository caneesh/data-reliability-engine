"""Adversarial tests of HOP_LEDGER at check level (examination, not part of the project's suite).

Each test asserts the behaviour the examination observed. Its docstring says whether that behaviour
is CORRECT (the engine does what a reconciliation must) or a FINDING (see 08-findings-register.md).
Fixtures are the project's synthetic layers (tests/fixtures/layers.py): curated -> gold, window
[2026-01-15 12:00, 2026-01-16 12:00) UTC, sla 8 hours, so upstream versions loaded in
[2026-01-15 04:00, 2026-01-16 04:00) are due. No dq store: these judge one window in isolation.
"""

from __future__ import annotations

import json
from datetime import datetime

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.hop.ledger import HopLedger
from hcsc.datalake.dre.config.models import Deletion, Effective, Exclusion, TimeColumn, Tolerance
from tests.checks.test_hop import EARLIER, EVENT, GOLD_LOAD, IN_WINDOW, curated, gold, tables
from tests.checks.test_hop_ledger import ALL_MAY_BE, LOADED_HERE, VOID, contract, rejects_table
from tests.fixtures.layers import CURATED_COLUMNS, CURATED_LOAD_FORMAT, create_table, curated_row, gold_row
from tests.fixtures.settings import SETTINGS

UP = "example_curated_enrollment"
LATER = datetime(2026, 1, 15, 11, 0)        # a newer version than IN_WINDOW, still due


def ledger(spark, dn, up):
    ctx = CheckContext(spark, dn, None, SETTINGS, "UTC", None, upstreams={UP: up},
                       datasets={dn.dataset: dn, up.dataset: up})
    [r], _ = run_check(HopLedger(), ctx, EVENT)
    return r


def declared(dn, **fields):
    """A declared dedup contract whose orphans can be checked (gold's load time configured)."""
    return contract(dn.model_copy(update={"load_time": GOLD_LOAD}), **fields)


def g(sub: str, record=IN_WINDOW, **kw):
    return gold_row(sub_id="000" + sub, record=record, loaded=LOADED_HERE, **kw)


def c(sub: str, updated=IN_WINDOW, **kw):
    return curated_row(sub_id=sub, updated=updated, **kw)


def counts(r) -> dict:
    return {(x["line"], x.get("label")): x["records"] for x in r.reconciliation if x["records"]}


def scope(r) -> dict:
    return json.loads(r.detail)


# --- the basic matrix ---

def test_a01_every_record_delivered_passes_healthy(spark) -> None:
    """CORRECT."""
    up, dn = tables(spark, "x_a01", [c("1"), c("2")], [g("1"), g("2")])
    r = ledger(spark, declared(dn), up)
    assert (r.state, r.population, r.violations, r.qualifications) == ("PASSED", 2, 0, ())


def test_a02_one_record_missing_fails(spark) -> None:
    """CORRECT."""
    up, dn = tables(spark, "x_a02", [c("1"), c("2")], [g("1")])
    r = ledger(spark, declared(dn), up)
    assert (r.state, r.violations) == ("FAILED", 1) and counts(r)[("missing", None)] == 1


def test_a03_equal_counts_different_keys_fails(spark) -> None:
    """CORRECT: key-level, not count-level: 1 missing and 1 orphan. Without gold's load time the orphan
    is not counted, but the missing record still fails the ledger."""
    up, dn = tables(spark, "x_a03", [c("1"), c("2")], [g("1"), g("3")])
    r = ledger(spark, declared(dn), up)
    assert (r.state, r.violations) == ("FAILED", 2) and counts(r)[("orphan", None)] == 1
    blind = ledger(spark, contract(dn), up)
    assert (blind.state, blind.violations) == ("FAILED", 1) and "orphans_unchecked" in blind.qualifications


def test_a04_wrong_owned_value_is_divergent(spark) -> None:
    """CORRECT: enddate is an owned column, compared."""
    up, dn = tables(spark, "x_a04", [c("1", end="2026-12-31")], [g("1", end="2027-06-30")])
    r = ledger(spark, declared(dn), up)
    assert r.state == "FAILED" and counts(r)[("divergent", None)] == 1


def test_a05_wrong_value_in_a_column_not_owned_is_delivered(spark) -> None:
    """FINDING F-01: only owned_columns are compared. gold's src_sys_nm (not owned) is wrong: delivered,
    PASSED, healthy (no qualification); the scope lists unverified columns, but health ignores them."""
    up, dn = tables(spark, "x_a05", [c("1")], [g("1", source="WRONG_SYSTEM")])
    r = ledger(spark, declared(dn), up)
    assert (r.state, r.qualifications) == ("PASSED", ())
    assert "src_sys_nm" in scope(r)["unverified_columns"]


def test_a06_with_no_owned_columns_any_value_is_delivered(spark) -> None:
    """FINDING F-01: no owned_columns at all: a gold row with the key and a record time at or after the
    upstream's is delivered whatever its values; PASSED with no qualification (values coverage none)."""
    up, dn = tables(spark, "x_a06", [c("1", end="2026-12-31")], [g("1", end="1900-01-01", source="WRONG")])
    r = ledger(spark, declared(dn.model_copy(update={"owned_columns": {}})), up)
    assert (r.state, r.qualifications) == ("PASSED", ())
    assert scope(r)["values"]["coverage"] == "none"


def test_a07_old_target_version_is_stale(spark) -> None:
    """CORRECT."""
    up, dn = tables(spark, "x_a07", [c("1")], [g("1", record=EARLIER)])
    r = ledger(spark, declared(dn), up)
    assert r.state == "FAILED" and counts(r)[("stale", None)] == 1


def test_a08_target_with_a_later_record_time_and_a_wrong_owned_value_is_divergent(spark) -> None:
    """CORRECT: a newer target version is not taken as delivered unless the owned values agree."""
    up, dn = tables(spark, "x_a08", [c("1", end="2026-12-31")], [g("1", record=LATER, end="2027-01-01")])
    r = ledger(spark, declared(dn), up)
    assert counts(r)[("divergent", None)] == 1


# --- exclusions, rejects, deletions ---

APPROVED = Exclusion(id="drop_test", version=1, condition="qualifiedhealthplanid = 'AGR-TEST'",
                     effective=Effective(**{"from": None}), approved_by="CHG-0000", max_share=0.6)


def test_a09_an_approved_exclusion_accounts_and_one_present_is_a_breach(spark) -> None:
    """CORRECT."""
    up, dn = tables(spark, "x_a09", [c("1"), c("2", agreement="AGR-TEST")], [g("1")])
    r = ledger(spark, declared(dn, exclusions=[APPROVED]), up)
    assert r.state == "PASSED" and counts(r)[("excluded", "drop_test@1")] == 1
    _, present = tables(spark, "x_a09b", [c("1"), c("2", agreement="AGR-TEST")], [g("1"), g("2", agreement="AGR-TEST")])
    r = ledger(spark, declared(present, exclusions=[APPROVED]), up)
    assert r.state == "FAILED" and counts(r)[("excluded_but_present", "drop_test@1")] == 1


def test_a10_an_unapproved_filter_is_missing(spark) -> None:
    """CORRECT: a record the pipeline filtered without a contract exclusion is missing."""
    up, dn = tables(spark, "x_a10", [c("1"), c("2", agreement="AGR-TEST")], [g("1")])
    r = ledger(spark, declared(dn), up)
    assert r.state == "FAILED" and counts(r)[("missing", None)] == 1


def test_a11_an_exclusion_over_its_max_share_fails(spark) -> None:
    """CORRECT: denominator = the due population, excluded records included."""
    up, dn = tables(spark, "x_a11", [c("1"), c("2", agreement="AGR-TEST"), c("3", agreement="AGR-TEST")], [g("1")])
    guarded = APPROVED.model_copy(update={"max_share": 0.5})
    r = ledger(spark, declared(dn, exclusions=[guarded]), up)
    assert r.state == "FAILED" and any("max_share" in f for f in r.observed.split("fails: ")[1].split("; "))


def test_a12_a_hard_delete_not_applied_fails(spark) -> None:
    """CORRECT."""
    up, dn = tables(spark, "x_a12", [c("1", end="2026-01-01")], [g("1", end="2026-01-01")])
    r = ledger(spark, declared(dn, mapping="merge", deletion=Deletion(tombstone=VOID, applied_as="hard")), up)
    assert r.state == "FAILED" and counts(r)[("deleted_not_applied", None)] == 1


def test_a13_an_authorised_reject_hides_a_divergent_record_present_downstream(spark) -> None:
    """FINDING F-04: the pipeline reports it refused key 1's version, yet gold holds that very version
    with a wrong owned value. `rejected` ranks above the residual, so the record is accounted for as
    rejected (authorised): PASSED, qualified only evidence_reported. The divergence is not reported."""
    up, dn = tables(spark, "x_a13", [c("1", end="2026-12-31")], [g("1", end="2027-06-30")])
    rejects = rejects_table(spark, "x_a13", [("1", "INVALID_CODE", IN_WINDOW)])
    r = ledger(spark, declared(dn, rejects=rejects, reject_policy=ALL_MAY_BE), up)
    assert r.state == "PASSED"
    assert counts(r)[("rejected", "INVALID_CODE")] == 1 and ("divergent", None) not in counts(r)
    assert r.qualifications == ("evidence_reported",)


# --- versions and CDC ---

def test_a14_duplicate_identical_cdc_events_are_one_record(spark) -> None:
    """CORRECT: two identical upstream rows of one version are one due record."""
    up, dn = tables(spark, "x_a14", [c("1"), c("1")], [g("1")])
    r = ledger(spark, declared(dn), up)
    assert (r.state, r.population) == ("PASSED", 1)


def test_a15_conflicting_rows_at_one_version_are_divergent(spark) -> None:
    """CORRECT (fail closed): two upstream rows tie at the version with different owned values; gold holds
    one: the value sets differ, divergent."""
    up, dn = tables(spark, "x_a15", [c("1", end="2026-12-31"), c("1", end="2027-12-31")], [g("1", end="2026-12-31")])
    r = ledger(spark, declared(dn), up)
    assert counts(r)[("divergent", None)] == 1


def test_a16_out_of_order_cdc_judges_the_latest_record_time(spark) -> None:
    """CORRECT: the newer version (11:00) was loaded first, the older one (10:00) loaded later. The
    version judged is the latest by record time; gold holding the newer passes, the older is stale."""
    rows = [c("1", updated=LATER, loaded=datetime(2026, 1, 15, 11, 5), end="2027-01-01"),
            c("1", updated=IN_WINDOW, loaded=datetime(2026, 1, 15, 13, 0), end="2026-12-31")]
    up, dn = tables(spark, "x_a16", rows, [g("1", record=LATER, end="2027-01-01")])
    up = up.model_copy(update={"load_time": TimeColumn(column="cdc_upd_ts", format="yyyy-MM-dd HH:mm:ss:SSSSSS",
                                                       granularity="minute", timezone="UTC")})
    assert ledger(spark, declared(dn), up).state == "PASSED"
    _, old = tables(spark, "x_a16b", rows, [g("1", record=IN_WINDOW, end="2026-12-31")])
    r = ledger(spark, declared(old), up)
    assert counts(r)[("stale", None)] == 1


def test_a17_intermediate_versions_are_not_required(spark) -> None:
    """CORRECT for current state, and a LIMITATION: ACTIVE -> TERMINATED -> ACTIVE in one window. Gold
    holds only the final version: delivered. Nothing proves the intermediate TERMINATED was processed
    (no event-completeness claim)."""
    rows = [c("1", updated=datetime(2026, 1, 15, 9, 0), end="9999-12-31"),
            c("1", updated=datetime(2026, 1, 15, 10, 0), end="2026-01-15"),
            c("1", updated=LATER, end="9999-12-31")]
    up, dn = tables(spark, "x_a17", rows, [g("1", record=LATER, end="9999-12-31")])
    r = ledger(spark, declared(dn), up)
    assert (r.state, r.population) == ("PASSED", 1)


# --- tolerance ---

def test_a18_a_tolerated_missing_record_is_an_unqualified_healthy_pass(spark) -> None:
    """FINDING F-03: tolerance.missing = 1 and one record missing: PASSED with no qualification, so
    v_latest_result.healthy would be true; only `observed` shows 'missing 1'."""
    up, dn = tables(spark, "x_a18", [c("1"), c("2")], [g("1")])
    r = ledger(spark, declared(dn, tolerance=Tolerance(missing=1)), up)
    assert (r.state, r.qualifications) == ("PASSED", ())
    assert "missing 1" in r.observed


# --- identity ---

def test_a19_a_separator_inside_a_key_part_collapses_two_records(spark) -> None:
    """FINDING F-02 (false PASS): key parts are joined with '|' (checks/keys.py key_string). Upstream
    has two different records, (sub 1, member '01|X') and (sub '1|01', member 'X'); both read as
    '1|01|X|2026-01-01|AGR-A'. Gold holds only the first: the second is never judged: PASSED, due 1."""
    rows = [c("1", mem_nbr="01|X"), c("1|01", mem_nbr="X")]
    up, dn = tables(spark, "x_a19", rows, [gold_row(sub_id="0001", mem_nbr="01|X", record=IN_WINDOW, loaded=LOADED_HERE)])
    r = ledger(spark, declared(dn), up)
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)


def test_a20_a_null_key_part_collapses_with_the_literal_null_marker(spark) -> None:
    """FINDING F-02 (false PASS): a null part is written '<null>'; a record whose member number is the
    string '<null>' is the same key as one whose member number is null. Gold holds one: PASSED, due 1."""
    rows = [c("1", mem_nbr=None), c("1", mem_nbr="<null>")]
    up, dn = tables(spark, "x_a20", rows, [gold_row(sub_id="0001", mem_nbr="<null>", record=IN_WINDOW, loaded=LOADED_HERE)])
    r = ledger(spark, declared(dn), up)
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)


def test_a21_unnormalised_whitespace_is_a_false_failure_not_a_false_pass(spark) -> None:
    """CORRECT direction (fails closed), a configuration matter: a padded upstream key without a trim
    normaliser reads as another key: missing (and gold's row an orphan)."""
    up, dn = tables(spark, "x_a21", [c(" 1")], [g("1")])
    r = ledger(spark, declared(dn), up)
    assert r.state == "FAILED" and counts(r)[("missing", None)] == 1


def test_a22_duplicate_rows_downstream_pass_under_dedup_and_fail_under_identity(spark) -> None:
    """LIMITATION (by contract kind): two identical gold rows of one key and version are one delivered
    record under dedup (duplicates there are T1_KEY_DUPLICATES' to find); identity rejects them."""
    up, dn = tables(spark, "x_a22", [c("1")], [g("1"), g("1")])
    assert ledger(spark, declared(dn), up).state == "PASSED"
    r = ledger(spark, declared(dn, mapping="identity"), up)
    assert r.state == "FAILED" and counts(r)[("unassessable", "not one-to-one")] == 1


# --- failures inside the check ---

def test_a23_a_failing_sql_is_did_not_run_never_pass(spark) -> None:
    """CORRECT: an exclusion condition naming a column that does not exist: the SQL fails, DID_NOT_RUN."""
    up, dn = tables(spark, "x_a23", [c("1")], [g("1")])
    broken = APPROVED.model_copy(update={"condition": "no_such_column = 'X'"})
    r = ledger(spark, declared(dn, exclusions=[broken]), up)
    assert r.state == "DID_NOT_RUN" and r.reason_category is not None


def test_a24_a_missing_upstream_table_and_an_empty_upstream_never_pass(spark) -> None:
    """CORRECT."""
    up, dn = tables(spark, "x_a24", [], [g("1")])
    r = ledger(spark, declared(dn), up)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")
    gone = curated("hop.x_a24_no_such_table")
    r = ledger(spark, declared(dn), gone)
    assert r.state == "DID_NOT_RUN"


def test_a25_gold_read_as_it_stands_now_not_as_of_the_cutoff(spark) -> None:
    """LIMITATION (documented: NORMAL and CURRENT_RECHECK read the current target): a gold row loaded
    after the window's end still delivers the record, so a late delivery is not a deficit."""
    up, dn = tables(spark, "x_a25", [c("1")], [gold_row(sub_id="0001", record=IN_WINDOW,
                                                         loaded="2026-01-20 09:00:00:000000")])
    r = ledger(spark, declared(dn), up)
    assert r.state == "PASSED"


def test_a26_a_wrong_partition_value_in_the_key_is_missing_and_orphan(spark) -> None:
    """CORRECT: gold wrote the record under a wrong effective date (a key part): missing + orphan."""
    up, dn = tables(spark, "x_a26", [c("1")], [g("1", eff="2026-02-01")])
    r = ledger(spark, declared(dn), up)
    assert r.state == "FAILED" and counts(r)[("missing", None)] == 1 and counts(r)[("orphan", None)] == 1


def test_a27_curated_load_format_constant_is_the_fixture_one() -> None:
    """Fixture sanity for a16: the curated load string format."""
    assert CURATED_LOAD_FORMAT == "%Y-%m-%d %H:%M:%S:%f" and CURATED_COLUMNS[6][0] == "cdc_upd_ts"
