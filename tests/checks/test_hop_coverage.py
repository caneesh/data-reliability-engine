"""Coverage continuity of the hop checks (checkpoint 3, L1; checks/hop/coverage.py).

A hop's deadline range starts where its latest committed covering result of an earlier window
ended, so no record is lost or judged twice when sla_hours changes, and a window that covered
nothing is judged by the next one. One shared calculation serves HOP_FILE_COMPLETENESS,
HOP_KEY_CURRENCY, HOP_VALUE_AGREEMENT and HOP_LEDGER (and the cause engine, through hop_params).

Windows are a day long, UTC: W1 = [01-14 12:00, 01-15 12:00), W2 = [01-15 12:00, 01-16 12:00).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from hcsc.datalake.dre.checks.base import CheckContext, CheckResult, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.hop.coverage import judged_range
from hcsc.datalake.dre.checks.hop.file_completeness import FileCompleteness
from hcsc.datalake.dre.checks.hop.key_currency import KeyCurrency
from hcsc.datalake.dre.checks.hop.ledger import HopLedger
from hcsc.datalake.dre.store.local_setup import create_store
from tests.checks.test_hop import file_tables, tables
from tests.fixtures.layers import curated_row, gold_row
from tests.fixtures.settings import SETTINGS
from tests.store.conftest import append_rows

UTC = timezone.utc
DQ = "dq_hop_coverage"
UP = "example_curated_enrollment"
W1 = Event("gold", datetime(2026, 1, 14, 12, tzinfo=UTC), datetime(2026, 1, 15, 12, tzinfo=UTC))
W2 = Event("gold", datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 1, 16, 12, tzinfo=UTC))
IN_GAP = datetime(2026, 1, 15, 2, 0)       # after W1's range with sla 12, before W2's with sla 8
AFTER_W1 = datetime(2026, 1, 15, 6, 0)     # in W2's range under either SLA


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


def settings(sla: float):
    return SETTINGS.model_copy(update={"sla_hours": sla})


def judge(spark, check, ds, up, event=W2, sla: float = 8, full_sweep=False) -> list[CheckResult]:
    ctx = CheckContext(spark, ds, None, settings(sla), "UTC", DQ, upstreams={up.dataset: up}, full_sweep=full_sweep)
    results, _ = run_check(check, ctx, event)
    return results


def covered(spark, ds, check_id: str, event: Event, upstream: str, coverage_to: datetime | None,
            state: str = "PASSED", reason: str | None = None, run_id: str = "r-earlier") -> None:
    """A committed NORMAL result of the hop for `event`, covering up to coverage_to (None: it covered nothing)."""
    append_rows(spark, DQ, "dq_check_result", [{
        "evaluation_id": f"e-{ds.dataset}-{check_id}-{event.window_end:%d%H}-{run_id}", "run_id": run_id,
        "event_id": "x", "window_start": event.window_start, "window_end": event.window_end,
        "execution_type": "NORMAL", "dataset": ds.dataset, "check_id": check_id, "state": state,
        "reason_category": "CONFIGURATION" if reason else None, "reason_code": reason,
        "group_values": {"upstream": upstream}, "evaluated_at": event.window_end,
        "coverage_from": None if coverage_to is None else event.window_start - timedelta(hours=12),
        "coverage_to": coverage_to, "run_date": event.window_end.date()}])


def test_a_shorter_sla_leaves_no_gap(spark) -> None:
    """W1 judged under sla 12, so its range ended at 01-15 00:00. W2 runs under sla 8: on its own its
    range would start at 01-15 04:00, and a key loaded at 02:00 would never be judged. It starts
    where W1's coverage ended, for every hop check."""
    up, dn = tables(spark, "cov_shorter", [curated_row(sub_id="1", updated=IN_GAP),
                                           curated_row(sub_id="2", updated=AFTER_W1)], [])
    for check_id in ("HOP_KEY_CURRENCY", "HOP_LEDGER"):
        covered(spark, dn, check_id, W1, UP, datetime(2026, 1, 15, 0, tzinfo=UTC))
    [currency] = judge(spark, KeyCurrency(), dn, up)
    [ledger] = judge(spark, HopLedger(), dn, up)
    assert (currency.population, ledger.population) == (2, 2)  # both keys: the gap's and W2's own
    assert currency.coverage == (datetime(2026, 1, 15, 0, tzinfo=UTC), datetime(2026, 1, 16, 4, tzinfo=UTC))
    ctx = CheckContext(spark, dn, None, settings(8), "UTC", DQ, upstreams={UP: up})
    assert judged_range(ctx, W2, "HOP_VALUE_AGREEMENT", UP) == (W2.window_start - timedelta(hours=8),
                                                                W2.window_end - timedelta(hours=8))  # its own coverage


def test_a_longer_sla_judges_nothing_twice(spark) -> None:
    """W1 judged under sla 8, up to 01-15 04:00. W2 runs under sla 12: its range is
    [01-15 04:00, 01-16 00:00). The key loaded at 02:00 was judged in W1: not judged again, not pending."""
    up, dn = tables(spark, "cov_longer", [curated_row(sub_id="1", updated=IN_GAP),
                                          curated_row(sub_id="2", updated=AFTER_W1)], [])
    for check_id in ("HOP_KEY_CURRENCY", "HOP_LEDGER"):
        covered(spark, dn, check_id, W1, UP, datetime(2026, 1, 15, 4, tzinfo=UTC))
    [currency] = judge(spark, KeyCurrency(), dn, up, sla=12)
    [ledger] = judge(spark, HopLedger(), dn, up, sla=12)
    assert (currency.population, ledger.population) == (1, 1)
    assert ledger.observed.startswith("due 1 (pending 0): missing 1")


def test_a_longer_sla_with_nothing_due_yet_never_moves_coverage_back(spark) -> None:
    """Coverage reached 01-15 16:00 under a short SLA; W2 under sla 24 ends its range at 01-15 12:00,
    before that: nothing is due, and the result records the coverage it found, not an earlier one."""
    up, dn = tables(spark, "cov_back", [curated_row(sub_id="1", updated=AFTER_W1)], [])
    covered(spark, dn, "HOP_LEDGER", W1, UP, datetime(2026, 1, 15, 16, tzinfo=UTC))
    [ledger] = judge(spark, HopLedger(), dn, up, sla=24)
    assert (ledger.state, ledger.reason_code) == ("DID_NOT_RUN", "empty_population")
    assert ledger.coverage == (datetime(2026, 1, 15, 16, tzinfo=UTC), datetime(2026, 1, 15, 16, tzinfo=UTC))


def test_a_window_that_covered_nothing_is_judged_by_the_next(spark) -> None:
    """W1's hop result was DID_NOT_RUN (a configuration error): it covered nothing. W2 starts at W1's
    start minus sla_hours, so the keys due in W1 are judged now, not lost."""
    up, dn = tables(spark, "cov_missed", [curated_row(sub_id="1", updated=datetime(2026, 1, 14, 10, 0)),
                                          curated_row(sub_id="2", updated=AFTER_W1)], [])
    for check_id in ("HOP_KEY_CURRENCY", "HOP_LEDGER"):
        covered(spark, dn, check_id, W1, UP, None, state="DID_NOT_RUN", reason="table_missing")
    [currency] = judge(spark, KeyCurrency(), dn, up)
    [ledger] = judge(spark, HopLedger(), dn, up)
    assert (currency.population, ledger.population) == (2, 2)
    assert ledger.coverage[0] == W1.window_start - timedelta(hours=8)


def test_a_held_window_judged_again_starts_where_its_first_attempt_did(spark) -> None:
    """A result of W2 itself (an earlier attempt of a held window) never moves W2's own start."""
    up, dn = tables(spark, "cov_held", [curated_row(sub_id="1", updated=AFTER_W1)], [])
    covered(spark, dn, "HOP_LEDGER", W1, UP, datetime(2026, 1, 15, 4, tzinfo=UTC))
    covered(spark, dn, "HOP_LEDGER", W2, UP, datetime(2026, 1, 16, 4, tzinfo=UTC), run_id="r-attempt")
    [ledger] = judge(spark, HopLedger(), dn, up)
    assert ledger.population == 1 and ledger.coverage[0] == datetime(2026, 1, 15, 4, tzinfo=UTC)


def test_invalid_results_cover_nothing(spark) -> None:
    """Coverage of a result whose evidence was found changed after publication is not trusted."""
    up, dn = tables(spark, "cov_invalid", [curated_row(sub_id="1", updated=IN_GAP)], [])
    covered(spark, dn, "HOP_LEDGER", W1, UP, datetime(2026, 1, 15, 16, tzinfo=UTC))  # would skip key 1
    [skipped] = judge(spark, HopLedger(), dn, up)
    assert skipped.reason_code == "empty_population"
    append_rows(spark, DQ, "dq_integrity", [{
        "run_id": "sweep", "record": "EVIDENCE_MISMATCH", "subject_run_id": "r-earlier",
        "subject_run_date": W1.window_end.date(), "evaluation_id": f"e-{dn.dataset}-HOP_LEDGER-1512-r-earlier",
        "commit_kind": "result", "recorded_at": W2.window_start, "run_date": W2.window_start.date()}])
    [judged] = judge(spark, HopLedger(), dn, up)
    assert judged.population == 1  # back to W1's start minus sla_hours


def test_file_completeness_continues_from_its_coverage_too(spark) -> None:
    up, dn = file_tables(spark, "cov_files", [("a", IN_GAP, "rt_0001.seq")], [])
    [alone] = judge(spark, FileCompleteness(), dn, up)
    assert alone.reason_code == "empty_population"  # 02:00 is before W2's own range
    covered(spark, dn, "HOP_FILE_COMPLETENESS", W1, "file_up", datetime(2026, 1, 15, 0, tzinfo=UTC))
    [r] = judge(spark, FileCompleteness(), dn, up)
    assert (r.state, r.population) == ("FAILED", 1)


def test_the_range_bounds_are_exact(spark) -> None:
    """Due from the range's start (inclusive) to its end (exclusive); a version loaded exactly at the
    end is pending; one before the start (judged earlier) is neither."""
    start, end = W2.window_start - timedelta(hours=8), W2.window_end - timedelta(hours=8)
    naive = lambda t: t.replace(tzinfo=None)  # noqa: E731
    up, dn = tables(spark, "cov_bounds", [curated_row(sub_id="1", updated=naive(start)),
                                          curated_row(sub_id="2", updated=naive(end)),
                                          curated_row(sub_id="3", updated=naive(start) - timedelta(minutes=1))],
                    [gold_row(sub_id="0001", record=naive(start))])
    [ledger] = judge(spark, HopLedger(), dn, up)
    assert ledger.observed == "due 1 (pending 1): delivered 1"


def test_a_late_arriving_older_version_does_not_change_the_version_judged(spark) -> None:
    """Curated loads (cdc_upd_ts) apart from record time here. A version with an older record time
    loaded after a newer one: the newer one is still the version judged, and gold, which holds it,
    is delivered; the late arrival's load time does not make the key due again."""
    from tests.checks.test_hop import curated

    up, dn = tables(spark, "cov_late", [
        curated_row(sub_id="1", updated=datetime(2026, 1, 15, 6), end="2026-06-30", loaded=datetime(2026, 1, 15, 6)),
        curated_row(sub_id="1", updated=datetime(2026, 1, 15, 5), loaded=datetime(2026, 1, 15, 7))],
        [gold_row(sub_id="0001", record=datetime(2026, 1, 15, 6), end="2026-06-30")])
    by_load = curated(up.table, load_time={"column": "cdc_upd_ts", "format": "yyyy-MM-dd HH:mm:ss:SSSSSS",
                                           "timezone": "UTC"})
    [ledger] = judge(spark, HopLedger(), dn, by_load)
    assert (ledger.state, ledger.observed) == ("PASSED", "due 1 (pending 0): delivered 1")


def test_only_covering_results_store_coverage_and_a_marker_never_does() -> None:
    """Missing or insufficient evidence never advances coverage: the runner stores coverage on
    PASSED, FAILED and DID_NOT_RUN / empty_population results only, and the evidence_incomplete
    marker that replaces a result whose evidence was not stored carries none."""
    from types import SimpleNamespace

    from hcsc.datalake.dre import runner
    from hcsc.datalake.dre.checks.base import did_not_run
    from hcsc.datalake.dre.store import runs

    span = (datetime(2026, 1, 15, 4, tzinfo=UTC), datetime(2026, 1, 16, 4, tzinfo=UTC))
    results = [CheckResult("PASSED", 1, 0, coverage=span, group_values={"upstream": "a"}),
               CheckResult("FAILED", 1, 1, coverage=span, group_values={"upstream": "b"}),
               replace_(did_not_run("empty_population", group_values={"upstream": "c"}), span),
               replace_(did_not_run("insufficient_evidence", group_values={"upstream": "d"}), span),
               replace_(did_not_run("budget_exceeded", group_values={"upstream": "e"}), span)]
    planned = SimpleNamespace(check=SimpleNamespace(check_id="HOP_LEDGER", severity=None), expectation_version=1,
                              feed=None, dataset=SimpleNamespace(dataset="gold"))
    evaluation = SimpleNamespace(planned=planned, event=W2, results=results, evaluated_at=W2.window_end,
                                 duration_ms=1)
    rows = runner.result_rows(evaluation, runs.new_run(now=W2.window_end), "NORMAL")
    assert [r["coverage_to"] for r in rows] == [span[1], span[1], span[1], None, None]
    marker = runner.incomplete_rows(SimpleNamespace(results=[CheckResult("PASSED", reconciliation=({},))]),
                                    rows[:1], W2.window_end)
    assert (marker[0]["state"], marker[0]["coverage_from"], marker[0]["coverage_to"]) == ("DID_NOT_RUN", None, None)


def replace_(result: CheckResult, span) -> CheckResult:
    from dataclasses import replace

    return replace(result, coverage=span)
