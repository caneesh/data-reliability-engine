"""Executable checks of the static review's mapping-kind and time candidates (examination only).

Fixtures are the project's (tests/checks/test_hop*.py, test_chain.py, test_hop_contribution.py).
Each test asserts the behaviour observed; its docstring names the finding or says CORRECT.
"""

from __future__ import annotations

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.hop.contribution import HopContribution
from hcsc.datalake.dre.checks.hop.key_currency import KeyCurrency
from hcsc.datalake.dre.config.models import Deletion, EdgeContract, TimeColumn
from tests.checks import test_hop_contribution as hc
from tests.checks.test_hop import BEFORE, EARLIER, EVENT, IN_WINDOW, tables
from tests.checks.test_hop_ledger_sources import A, B, LATER_IN_WINDOW, judged, survivorship
from tests.fixtures.layers import curated_row, gold_row
from tests.fixtures.settings import SETTINGS

from test_ledger_adversarial import c, counts, declared, g, ledger

UP = "example_curated_enrollment"
CDC_LOAD = TimeColumn(column="cdc_upd_ts", format="yyyy-MM-dd HH:mm:ss:SSSSSS", granularity="minute", timezone="UTC")


def currency(spark, dn, up, full_sweep=False):
    ctx = CheckContext(spark, dn, None, SETTINGS, "UTC", None, upstreams={UP: up}, full_sweep=full_sweep)
    [r], _ = run_check(KeyCurrency(), ctx, EVENT)
    return r


def test_m01_survivorship_without_owned_columns_accepts_the_wrong_winner(spark) -> None:
    """FINDING F-09 (false PASS): precedence [A, B]; A holds key 1 at 10:00, B at 11:00; gold holds B's
    row; A's value (end 2026-06-30) differs from B's. With owned columns (the project's test) A's edge is
    divergent. Without them, A's edge reads gold's
    newer row as delivered and B's as superseded by A: both PASS, though the lower-precedence source won."""
    a, b, dn = survivorship(spark, "xm01", [curated_row(sub_id="1", updated=IN_WINDOW, end="2026-06-30")],
                            [curated_row(sub_id="1", updated=LATER_IN_WINDOW)],
                            [gold_row(sub_id="0001", record=LATER_IN_WINDOW)])
    with_values = judged(spark, dn, a, b)
    assert with_values[A].state == "FAILED"
    got = judged(spark, dn.model_copy(update={"owned_columns": {}}), a, b)
    assert (got[A].state, got[B].state) == ("PASSED", "PASSED")


def test_m02_a_contributor_update_that_never_reaches_gold_is_not_detected(spark) -> None:
    """FINDING F-10 (false PASS): gold's row M1 was loaded before the window with provider name 'Old';
    the provider changed to 'New' inside the window and gold was never reloaded. HOP_CONTRIBUTION judges
    only rows loaded here in the window: DID_NOT_RUN / empty_population; nothing reports the stale value."""
    here, provider = hc.hop(spark, "xm02", [hc.here_row("M1", "P1", "Old", loaded=hc.BEFORE)],
                            [hc.provider_row("P1", "Old", loaded=hc.BEFORE), hc.provider_row("P1", "New", loaded=hc.IN_WINDOW)])
    ctx = CheckContext(spark, here, None, SETTINGS, "UTC", None, upstreams={hc.PROVIDER: provider})
    [r], _ = run_check(HopContribution(), ctx, EVENT)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


def test_m03_an_absent_optional_contributor_fails(spark) -> None:
    """FINDING F-11 (false FAIL): a member with no provider (NULL reference, as a LEFT JOIN writes it) is
    unresolved; there is no notion of an optional contributor."""
    here, provider = hc.hop(spark, "xm03", [hc.here_row("M1", None, None)], [hc.provider_row("P1", "Any")])
    ctx = CheckContext(spark, here, None, SETTINGS, "UTC", None, upstreams={hc.PROVIDER: provider})
    [r], _ = run_check(HopContribution(), ctx, EVENT)
    assert (r.state, r.violations) == ("FAILED", 1)


def test_m04_a_newer_upstream_version_with_no_record_time_is_invisible_to_key_currency(spark) -> None:
    """FINDING F-05 for HOP_KEY_CURRENCY: key 1's newer row has no record time; its older version, which
    gold holds, has one. Even the full sweep judges the older version: CURRENT, PASSED."""
    rows = [c("1", updated=BEFORE, loaded=BEFORE), c("1", updated=None, loaded=IN_WINDOW, end="2027-01-01")]
    up, dn = tables(spark, "xm04", rows, [g("1", record=BEFORE)])
    up = up.model_copy(update={"load_time": CDC_LOAD})
    r = currency(spark, dn, up, full_sweep=True)
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)


def test_m05_a_key_with_an_unparseable_load_time_vanishes_from_key_currency_even_on_the_full_sweep(spark) -> None:
    """FINDING F-12 (false PASS): key 1's only row has a load time that does not parse; gold lacks it.
    HOP_KEY_CURRENCY never judges it, not even on the full sweep (PASSED on key 2); HOP_LEDGER's full
    sweep, by contrast, makes it unassessable."""
    bad = {**c("1"), "cdc_upd_ts": "not a time"}
    up, dn = tables(spark, "xm05", [bad, c("2", loaded=IN_WINDOW)], [g("2")])
    up = up.model_copy(update={"load_time": CDC_LOAD})
    r = currency(spark, dn, up, full_sweep=True)
    assert (r.state, r.population) == ("PASSED", 1)
    from hcsc.datalake.dre.checks.hop.ledger import HopLedger

    ctx = CheckContext(spark, declared(dn), None, SETTINGS, "UTC", None, upstreams={UP: up}, full_sweep=True,
                       datasets={dn.dataset: dn, UP: up})
    [lg], _ = run_check(HopLedger(), ctx, EVENT)
    assert lg.state == "FAILED" and counts(lg)[("unassessable", "no load time")] == 1


def test_m06_a_history_keeping_target_is_judged_by_its_newest_record_time_not_its_current_row(spark) -> None:
    """FINDING F-13 (false PASS, configuration-dependent): gold keeps history. Its row of version 10:00
    (correct) was loaded first; a regression later loaded the 09:00 version again, which is what consumers
    read as current. The ledger judges gold's newest record time: delivered, PASSED."""
    rows_gold = [gold_row(sub_id="0001", record=IN_WINDOW, loaded="2026-01-15 11:00:00:000000"),
                 gold_row(sub_id="0001", record=EARLIER, end="2026-06-30", loaded="2026-01-15 15:00:00:000000")]
    up, dn = tables(spark, "xm06", [c("1")], rows_gold)
    r = ledger(spark, declared(dn), up)
    assert r.state == "PASSED" and counts(r)[("delivered", None)] == 1


def test_m07_the_chain_stops_following_a_record_at_an_authorised_soft_delete(spark) -> None:
    """FINDING F-14 (false PASS): raw voids key 1 (a void file), curated applies the void softly (end date =
    effective date), gold still holds the live row. The chain accounts for the record at raw -> curated
    (deleted, authorised) and stops following it: PASSED. Gold's own HOP_LEDGER finds deleted_not_applied."""
    from tests.checks.test_chain import SECRET, chain_of, layers, raw_record

    raw = {**raw_record("123401"), "src_file_nm": "void_0001.seq"}
    cur = curated_row(sub_id="123401", updated=EARLIER, end="2026-01-01")
    gold_live = gold_row(sub_id="000123401", record=EARLIER, end="9999-12-31")
    soft_cur = EdgeContract(mapping="merge", deletion=Deletion(
        tombstone="src_file_nm LIKE 'void%'", applied_as="soft", soft_when="enddate = '2026-01-01'"))
    soft_gold = EdgeContract(mapping="merge", deletion=Deletion(
        tombstone="enddate = '2026-01-01'", applied_as="soft", soft_when="mbr_mbrshp_covrg_end_dt = '2026-01-01'"))
    datasets = layers(spark, "xm07", [raw], [cur], [gold_live],
                      cur_extra={"contracts": {"example_raw_enrollment": soft_cur}},
                      gold_extra={"contracts": {UP: soft_gold}})
    r = chain_of(spark, datasets, secret=SECRET)
    assert r.state == "PASSED"
    import json

    assert json.loads(r.detail)["outcomes"]["accounted"] == 1


def test_m08_constants() -> None:
    assert B and EARLIER < IN_WINDOW < LATER_IN_WINDOW and isinstance(EdgeContract, type)
