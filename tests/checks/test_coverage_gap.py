"""Final conditional approval, CA-01: an unresolved coverage gap never supports a healthy PASS.

A hop check judges the records due in its deadline range, which continues from the hop's committed
coverage (checks/hop/coverage.py). When it cannot continue from trusted committed coverage that may
have existed (only INVALID results retained, or nothing retained in a store older than the retention,
which may have dropped it), the records due before the range are judged only by the full sweep. Such
a PASS is qualified (`coverage_gap`), so never healthy (v_latest_result), and the
qualification is carried by every later window that continues from it, until a full sweep, which
judges every record due before its range end, covers the hop. Continuity that is established
(coverage inside the lookback, or retained before it), and a first evaluation in a store that has lost
nothing to retention, leave a clean PASS healthy, as before.

Window and helpers are those of test_hop.py: [2026-01-15 12:00, 2026-01-16 12:00) UTC, sla 8 hours.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from hcsc.datalake.dre.checks.base import CheckContext, CheckResult, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.hop.key_currency import KeyCurrency
from hcsc.datalake.dre.checks.hop.ledger import HopLedger
from hcsc.datalake.dre.checks.hop.value_agreement import ValueAgreement
from hcsc.datalake.dre.store.local_setup import create_store
from tests.checks.test_hop import EVENT, GOLD_LOAD, IN_WINDOW, tables
from tests.checks.test_hop_ledger import LOADED_HERE, contract
from tests.fixtures.layers import curated_row, gold_row
from tests.fixtures.settings import SETTINGS
from tests.store.conftest import append_rows

UTC = timezone.utc
DQ = "dq_coverage_gap"
UP = "example_curated_enrollment"
SLA = timedelta(hours=8)
W1 = Event("gold", EVENT.window_start - timedelta(days=1), EVENT.window_start)
OLD = Event("gold", datetime(2025, 5, 31, 12, tzinfo=UTC), datetime(2025, 6, 1, 12, tzinfo=UTC))
OLD_TO = datetime(2025, 6, 1, 4, tzinfo=UTC)
LOOKBACK = date(2025, 12, 1)       # history_since: the hourly reads look back to here
RETAINED = date(2020, 1, 1)        # coverage_since: dq_check_result keeps everything since
NOT_RETAINED = date(2025, 7, 1)    # coverage_since after OLD: the old coverage is past retention
GAP = "coverage_gap"


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


def window(days: int) -> Event:
    """The window `days` after EVENT."""
    return Event("gold", EVENT.window_start + timedelta(days=days), EVENT.window_end + timedelta(days=days))


def hop(spark, name: str) -> tuple:
    """A declared dedup hop with its orphans checkable (nothing unproven): one key, delivered, due in
    EVENT's range; its clean PASS is healthy."""
    up, dn = tables(spark, name, [curated_row(sub_id="1", updated=IN_WINDOW)],
                    [gold_row(sub_id="0001", record=IN_WINDOW, loaded=LOADED_HERE)])
    return up, contract(dn.model_copy(update={"load_time": GOLD_LOAD}))


def ctx_of(spark, up, dn, coverage_since: date = RETAINED, full_sweep: bool = False) -> CheckContext:
    return CheckContext(spark, dn, None, SETTINGS, "UTC", DQ, upstreams={UP: up}, history_since=LOOKBACK,
                        coverage_since=coverage_since, full_sweep=full_sweep)


def covered(spark, dn, event: Event, coverage_to: datetime | None, run_id: str, check_id: str = "HOP_LEDGER",
            state: str = "PASSED", qualifications: str | None = None) -> None:
    """A committed NORMAL result of the hop for `event`, covering up to coverage_to (None: nothing)."""
    append_rows(spark, DQ, "dq_check_result", [{
        "evaluation_id": f"e-{dn.dataset}-{check_id}-{run_id}", "run_id": run_id, "event_id": "x",
        "window_start": event.window_start, "window_end": event.window_end, "execution_type": "NORMAL",
        "dataset": dn.dataset, "check_id": check_id, "state": state, "group_values": {"upstream": UP},
        "evaluated_at": event.window_end, "qualifications": qualifications,
        "coverage_from": None if coverage_to is None else event.window_start - SLA, "coverage_to": coverage_to,
        "run_date": event.window_end.date()}])


def stored(spark, dn, result: CheckResult, event: Event, run_id: str) -> None:
    """Store a result as the runner would (runner.result_rows): its coverage and qualifications."""
    append_rows(spark, DQ, "dq_check_result", [{
        "evaluation_id": f"e-{dn.dataset}-HOP_LEDGER-{run_id}", "run_id": run_id, "event_id": "x",
        "window_start": event.window_start, "window_end": event.window_end, "execution_type": "NORMAL",
        "dataset": dn.dataset, "check_id": "HOP_LEDGER", "state": result.state,
        "reason_code": result.reason_code, "group_values": result.group_values, "evaluated_at": event.window_end,
        "qualifications": ",".join(result.qualifications) or None,
        "coverage_from": result.coverage[0] if result.coverage else None,
        "coverage_to": result.coverage[1] if result.coverage else None, "run_date": event.window_end.date()}])


def invalidate(spark, dn, event: Event, run_id: str) -> None:
    """An integrity sweep found the result's evidence changed after publication: INVALID."""
    append_rows(spark, DQ, "dq_integrity", [{
        "run_id": f"sweep-{run_id}", "record": "EVIDENCE_MISMATCH", "subject_run_id": run_id,
        "subject_run_date": event.window_end.date(), "evaluation_id": f"e-{dn.dataset}-HOP_LEDGER-{run_id}",
        "commit_kind": "result", "recorded_at": EVENT.window_start, "run_date": EVENT.window_start.date()}])


def ledger(ctx: CheckContext, event: Event = EVENT) -> CheckResult:
    [r] = run_check(HopLedger(), ctx, event)[0]
    return r


def basis(r: CheckResult) -> str:
    return json.loads(r.detail)["coverage_basis"]


def healthy(spark, dn) -> bool:
    [row] = spark.sql(f"SELECT healthy FROM {DQ}.v_latest_result WHERE dataset = '{dn.dataset}' "
                      "AND check_id = 'HOP_LEDGER'").collect()
    return row.healthy


# --- 1. established continuity: a clean PASS stays healthy ---

def test_coverage_inside_the_lookback_leaves_a_clean_pass_healthy(spark) -> None:
    up, dn = hop(spark, "gap_inside")
    covered(spark, dn, W1, EVENT.window_start - SLA, "r-w1")
    r = ledger(ctx_of(spark, up, dn))
    assert (r.state, r.population, r.qualifications) == ("PASSED", 1, ())
    assert r.coverage[0] == EVENT.window_start - SLA
    assert basis(r) == "committed coverage"
    stored(spark, dn, r, EVENT, "r-now")
    assert healthy(spark, dn)


# --- 2. continuity before the lookback, still retained ---

def test_coverage_before_the_lookback_but_retained_leaves_a_clean_pass_healthy(spark) -> None:
    up, dn = hop(spark, "gap_retained")
    covered(spark, dn, OLD, OLD_TO, "r-old")
    r = ledger(ctx_of(spark, up, dn))
    assert (r.state, r.qualifications) == ("PASSED", ())
    assert r.coverage[0] == OLD_TO  # the idle span is judged, not skipped
    assert basis(r) == "committed coverage before the lookback, from 2025-06-01"


# --- 3. no authoritative boundary within retention ---

def test_no_coverage_retained_is_a_qualified_pass_never_healthy(spark) -> None:
    up, dn = hop(spark, "gap_none")
    covered(spark, dn, OLD, OLD_TO, "r-old")
    ctx = ctx_of(spark, up, dn, coverage_since=NOT_RETAINED)
    r = ledger(ctx)
    assert r.state == "PASSED" and GAP in r.qualifications
    scope = json.loads(r.detail)
    assert scope["claim"].startswith("declared contract, qualified:") and GAP in scope["qualifications"]
    assert basis(r).startswith("unresolved coverage gap: no committed coverage retained since 2025-07-01")
    stored(spark, dn, r, EVENT, "r-now")
    assert not healthy(spark, dn)
    # the hop's other checks share the range, so they carry the same qualification
    for check in (KeyCurrency(), ValueAgreement()):
        [other] = run_check(check, ctx, EVENT)[0]
        assert other.state == "PASSED" and GAP in other.qualifications, check.check_id


def test_a_first_evaluation_in_a_store_that_lost_nothing_is_healthy(spark) -> None:
    """The hop has no result, and no partition of the store is at or before coverage_since, so
    retention has dropped nothing: no history is missing. Its clean PASS stays healthy (proof scenarios
    such as PS2 rest on this)."""
    up, dn = hop(spark, "gap_first")
    r = ledger(ctx_of(spark, up, dn))  # coverage_since 2020-01-01: the store's partitions are all later
    assert (r.state, r.qualifications) == ("PASSED", ())
    assert basis(r) == "first evaluation (the hop has no result, and the store has lost none to retention)"
    stored(spark, dn, r, EVENT, "r-first")
    assert healthy(spark, dn)


def test_a_hop_with_nothing_retained_in_a_store_older_than_the_retention_is_a_gap(spark) -> None:
    """The hop has no result retained, and the store holds a partition at or before coverage_since: it
    is older than the retention, so the hop's coverage may have been dropped. Indistinguishable from a
    first evaluation there, so qualified (fail closed)."""
    up, dn = hop(spark, "gap_dropped")
    covered(spark, hop(spark, "gap_other")[1], OLD, OLD_TO, "r-other-old")  # another hop's old result
    r = ledger(ctx_of(spark, up, dn, coverage_since=NOT_RETAINED))
    assert r.state == "PASSED" and GAP in r.qualifications
    assert basis(r).startswith("unresolved coverage gap: no committed coverage retained since 2025-07-01")


# --- 4. previous evidence INVALID ---

def test_invalid_evidence_with_trusted_coverage_before_it_leaves_a_clean_pass_healthy(spark) -> None:
    up, dn = hop(spark, "gap_invalid_trusted")
    covered(spark, dn, OLD, OLD_TO, "r-old")
    covered(spark, dn, W1, EVENT.window_start - SLA, "r-bad")
    invalidate(spark, dn, W1, "r-bad")
    r = ledger(ctx_of(spark, up, dn))
    assert (r.state, r.qualifications) == ("PASSED", ())
    assert r.coverage[0] == OLD_TO  # the INVALID result's span is judged again


def test_invalid_evidence_with_no_trusted_coverage_retained_is_a_coverage_gap(spark) -> None:
    """The only retained result is INVALID: its judgement is not trusted and covers nothing. The range
    falls back to its window, but nothing establishes what was judged before it."""
    up, dn = hop(spark, "gap_invalid_only")
    covered(spark, dn, W1, EVENT.window_start - SLA, "r-bad")
    invalidate(spark, dn, W1, "r-bad")
    r = ledger(ctx_of(spark, up, dn))
    assert r.coverage[0] == W1.window_start - SLA
    assert r.state == "PASSED" and GAP in r.qualifications


# --- 5. later windows after an unresolved gap ---

def test_later_windows_carry_the_gap_until_a_full_sweep_covers_the_hop(spark) -> None:
    """A later successful window continues from the gap's coverage, so it carries the qualification:
    it cannot erase the gap. A full sweep that did not cover (DID_NOT_RUN, not stored with coverage)
    resolves nothing; one that covers judges every record due before its range end, and clears it."""
    up, dn = hop(spark, "gap_carried")
    covered(spark, dn, OLD, OLD_TO, "r-old")
    ctx = ctx_of(spark, up, dn, coverage_since=NOT_RETAINED)

    gap = ledger(ctx)
    assert GAP in gap.qualifications
    stored(spark, dn, gap, EVENT, "r-gap")

    later = ledger(ctx, window(1))
    assert later.coverage[0] == gap.coverage[1]  # continues: nothing judged twice
    assert later.reason_code == "empty_population" and GAP in later.qualifications
    assert basis(later).startswith("committed coverage, continuing an unresolved coverage gap")
    stored(spark, dn, later, window(1), "r-later")
    assert not healthy(spark, dn)

    covered(spark, dn, window(2), None, "r-sweep-failed", state="DID_NOT_RUN")  # the full sweep covered nothing
    still = ledger(ctx, window(3))
    assert GAP in still.qualifications
    stored(spark, dn, still, window(3), "r-still")

    sweep = ledger(ctx_of(spark, up, dn, coverage_since=NOT_RETAINED, full_sweep=True), window(4))
    assert sweep.state == "PASSED" and sweep.population == 1  # the key due before the gap's range: judged now
    assert GAP not in sweep.qualifications
    stored(spark, dn, sweep, window(4), "r-sweep")
    assert healthy(spark, dn)

    after = ledger(ctx, window(5))
    assert GAP not in after.qualifications and basis(after) == "committed coverage"
