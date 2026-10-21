"""Executable checks of the static review's ledger candidates (examination, not part of the project's suite).

Same fixtures as test_ledger_adversarial.py. Each test asserts the behaviour observed; its docstring
names the finding (08-findings-register.md) or says the candidate was not confirmed.
"""

from __future__ import annotations

from datetime import datetime

from hcsc.datalake.dre.config.models import Deletion, TimeColumn
from tests.checks.test_hop import BEFORE, EVENT, IN_WINDOW, tables
from tests.checks.test_hop_ledger import LOADED_HERE, VOID
from tests.fixtures.layers import gold_row

from test_ledger_adversarial import APPROVED, c, counts, declared, g, ledger

CDC_LOAD = TimeColumn(column="cdc_upd_ts", format="yyyy-MM-dd HH:mm:ss:SSSSSS", granularity="minute", timezone="UTC")


def test_l01_a_newer_version_with_no_record_time_drops_the_key_from_the_ledger(spark) -> None:
    """FINDING F-05 (false PASS): key 1's new row (loaded in the range) has no record time; its old version
    (record and load 2026-01-10) has one. MAX(record_ts) ignores the null, so the old version is judged:
    loaded before the range, the key is neither due nor pending nor untimed. Gold lacks key 1 entirely,
    yet the ledger PASSES on key 2 alone (due 1)."""
    rows = [c("1", updated=BEFORE, loaded=BEFORE), c("1", updated=None, loaded=IN_WINDOW), c("2", loaded=IN_WINDOW)]
    up, dn = tables(spark, "x_l01", rows, [g("2")])
    up = up.model_copy(update={"load_time": CDC_LOAD})
    r = ledger(spark, declared(dn), up)
    assert (r.state, r.population) == ("PASSED", 1)
    assert not any(k[0] in ("pending", "untimed") for k in counts(r))


def test_l02_an_orphan_in_a_window_with_nothing_due_is_not_reported(spark) -> None:
    """FINDING F-06 (hidden breach): nothing upstream is due; gold received a row (key 9, loaded in the
    window) that no upstream row has. The result is DID_NOT_RUN / empty_population, a covering result,
    so the orphan's window is never judged again; the orphan is not counted."""
    up, dn = tables(spark, "x_l02", [c("1", updated=BEFORE)], [g("9")])
    r = ledger(spark, declared(dn), up)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")
    assert r.coverage is not None  # covering: the range moves on


def test_l03_one_tied_row_matching_an_exclusion_excludes_the_whole_key(spark) -> None:
    """FINDING F-07 (false PASS): key 1 has two rows tied at one record time; only one matches the
    approved exclusion (a non-key column, enddate = '2099-12-31', marks a test row). Gold lacks key 1:
    the whole key is excluded (MAX over the tied rows), PASSED, though the other tied row is live."""
    excl = APPROVED.model_copy(update={"condition": "enddate = '2099-12-31'"})
    rows = [c("1", end="2026-12-31"), c("1", end="2099-12-31"), c("2"), c("3")]
    up, dn = tables(spark, "x_l03", rows, [g("2"), g("3")])
    r = ledger(spark, declared(dn, exclusions=[excl]), up)
    assert r.state == "PASSED" and counts(r)[("excluded", "drop_test@1")] == 1


def test_l04_one_tied_tombstone_row_deletes_the_whole_key(spark) -> None:
    """FINDING F-07: same for a hard deletion: one of two tied rows is a tombstone; gold lacks the key:
    deleted (authorised), PASSED, though the other tied row is a live record."""
    rows = [c("1", end="2026-01-01"), c("1", end="2026-12-31"), c("2")]
    up, dn = tables(spark, "x_l04", rows, [g("2")])
    r = ledger(spark, declared(dn, mapping="merge", deletion=Deletion(tombstone=VOID, applied_as="hard")), up)
    assert r.state == "PASSED" and counts(r)[("deleted", None)] == 1


def test_l05_an_unparseable_key_part_matches_a_null_key_part(spark) -> None:
    """FINDING F-02 variant: with parse_date on the effective date, upstream '2026-13-01' (not a date)
    and gold NULL both read '<null>': delivered, PASSED; the model calls an unparseable key unassessable."""
    norm = {"effectivedate": {"parse_date": "yyyy-MM-dd"}}
    up, dn = tables(spark, "x_l05", [c("1", eff="2026-13-01")], [g("1", eff=None)])
    up = up.model_copy(update={"key_normalise": norm})
    dn = dn.model_copy(update={"key_normalise": {**dn.key_normalise, "mbr_mbrshp_covrg_eff_dt": {"parse_date": "yyyy-MM-dd"}}})
    r = ledger(spark, declared(dn), up)
    assert (r.state, r.population) == ("PASSED", 1)


def test_l06_a_breach_reloaded_downstream_is_not_judged_by_the_ledger(spark) -> None:
    """FINDING F-08: key 3 is excluded (a test record, loaded upstream 2026-01-10, judged then). Gold
    loads key 3 in this window: an excluded_but_present breach. The ledger's due population is driven by
    upstream loads only, and key 3 has an upstream row, so it is not an orphan either: PASSED."""
    excl = APPROVED.model_copy(update={"condition": "enddate = '2099-12-31'"})
    rows = [c("1"), c("3", updated=BEFORE, end="2099-12-31")]
    up, dn = tables(spark, "x_l06", rows, [g("1"), g("3", record=BEFORE, end="2099-12-31")])
    r = ledger(spark, declared(dn, exclusions=[excl]), up)
    assert (r.state, r.population) == ("PASSED", 1)
    sweep_ctx_note = "the full sweep would judge key 3"  # documented in 02-ledger-correctness.md
    assert sweep_ctx_note


def test_l07_gold_rows_with_no_load_time_are_never_orphans(spark) -> None:
    """FINDING F-06 variant: a gold row whose load time does not parse is outside every window's orphan
    scan (load_ts IS NULL), so a substituted row with an unreadable load time is never counted."""
    bad = gold_row(sub_id="0009", record=IN_WINDOW, loaded="not a time")
    up, dn = tables(spark, "x_l07", [c("1")], [g("1"), bad])
    r = ledger(spark, declared(dn), up)
    assert r.state == "PASSED" and ("orphan", None) not in counts(r)


def test_l08_event_constant_sanity() -> None:
    assert EVENT.window_start < datetime(2026, 1, 16, tzinfo=EVENT.window_start.tzinfo) and LOADED_HERE
