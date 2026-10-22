"""KI-19: HOP_KEY_CURRENCY's open keys after a long gap in evaluation.

The hop judges again every key still open (latest committed event FLAGGED or STILL_FLAGGED), and
every evaluation records it again (STILL_FLAGGED, or CLEARED once CURRENT), so while the hop's own
coverage continues from inside history_lookback_days, its open keys' latest events are inside the
lookback too and the read stays bounded there. When the hop's coverage did not continue from inside
the lookback (it was not evaluated for longer), the read reaches back to dq_key_event's retention
(key_events_since), as the chain's carry does (CA-02); a key opened before that retention is
disclosed, and judged again only by the full sweep.

The hop flagged key 1 MISSING in OLD (2025-05-31/06-01); it is next evaluated in EVENT
(2026-01-15/16), with a lookback from 2025-12-01. Times are UTC; sla 8 hours.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from hcsc.datalake.dre.checks.base import CheckContext, CheckResult, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.hop.common import hop_label, key_hash
from hcsc.datalake.dre.checks.hop.key_currency import KeyCurrency
from hcsc.datalake.dre.store.commits import Commit, append_commits
from hcsc.datalake.dre.store.key_events import append_key_events
from hcsc.datalake.dre.store.local_setup import create_store
from tests.checks.test_hop import EVENT, SECRET, tables
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_row, gold_row
from tests.fixtures.settings import SETTINGS
from tests.store.conftest import append_rows

UTC = timezone.utc
DQ = "dq_open_keys_idle"
UP = "example_curated_enrollment"
OLD = Event("gold", datetime(2025, 5, 31, 12, tzinfo=UTC), datetime(2025, 6, 1, 12, tzinfo=UTC))
FLAGGED_AT = datetime(2025, 5, 31, 10, 0)   # key 1's load: due in OLD's range
IDLE_AT = datetime(2025, 9, 1, 10, 0)       # key 2's load, while the hop was not evaluated
LOOKBACK = date(2025, 12, 1)
KEY1, KEY2 = "1|01|2026-01-01|AGR-A", "2|01|2026-01-01|AGR-A"
NEXT = Event("gold", EVENT.window_end, EVENT.window_end + timedelta(days=1))


@pytest.fixture(autouse=True)
def store(spark):
    spark.sql(f"DROP DATABASE IF EXISTS {DQ} CASCADE")
    create_store(spark, DQ)


def ctx_of(spark, up, dn, key_events_since: date | None = None, full_sweep: bool = False,
           history_since: date | None = LOOKBACK) -> CheckContext:
    return CheckContext(spark, dn, None, SETTINGS, "UTC", DQ, upstreams={UP: up}, key_secret=SECRET,
                        history_since=history_since, key_events_since=key_events_since, full_sweep=full_sweep)


def judge(ctx: CheckContext, event: Event) -> CheckResult:
    [r] = run_check(KeyCurrency(), ctx, event)[0]
    return r


def events(result: CheckResult) -> list[tuple[str, str, str | None]]:
    if result.key_events is None:
        return []
    return sorted((r.event, r.state_detail, r.key_value) for r in result.key_events.collect())


def publish(spark, dn, result: CheckResult, event: Event, run_id: str, commit: bool = True,
            invalid: bool = False) -> None:
    """Store the result (its coverage) and its key events under one commit, as the runner does."""
    ev, day, hop = f"e-{run_id}", event.window_end.date(), hop_label(UP, dn.dataset)
    append_rows(spark, DQ, "dq_check_result", [{
        "evaluation_id": ev, "run_id": run_id, "event_id": "x", "window_start": event.window_start,
        "window_end": event.window_end, "execution_type": "NORMAL", "dataset": dn.dataset,
        "check_id": "HOP_KEY_CURRENCY", "state": result.state, "reason_code": result.reason_code,
        "group_values": {"upstream": UP}, "evaluated_at": event.window_end,
        "qualifications": ",".join(result.qualifications) or None,
        "coverage_from": result.coverage[0], "coverage_to": result.coverage[1], "run_date": day}], commit=False)
    n = 0 if result.key_events is None else append_key_events(
        result.key_events, DQ, run_id, ev, dn.dataset, "HOP_KEY_CURRENCY", hop, event.window_end, day)
    if commit:
        append_commits(spark, DQ, [Commit(run_id, ev, dn.dataset, "HOP_KEY_CURRENCY", 1, key_event_rows=n)],
                       event.window_end, day)
    if invalid:
        append_rows(spark, DQ, "dq_integrity", [{
            "run_id": f"sweep-{run_id}", "record": "EVIDENCE_MISMATCH", "subject_run_id": run_id,
            "subject_run_date": day, "evaluation_id": ev, "commit_kind": "result",
            "recorded_at": EVENT.window_start, "run_date": EVENT.window_start.date()}])


def flagged_long_ago(spark, name: str, **kwargs):
    """OLD: key 1 is due and missing in gold, so FLAGGED; published. Returns the hop's tables."""
    up, dn = tables(spark, name, [curated_row(sub_id="1", updated=FLAGGED_AT)], [])
    old = judge(ctx_of(spark, up, dn, history_since=None), OLD)
    assert events(old) == [("FLAGGED", "MISSING", KEY1)]
    publish(spark, dn, old, OLD, "r-old", **kwargs)
    return up, dn


def recovered(spark, dn) -> None:
    """Gold now holds key 1 at its latest version."""
    create_table(spark, dn.table, GOLD_COLUMNS, [gold_row(sub_id="0001", record=FLAGGED_AT)])


def said(r: CheckResult) -> str:
    """What the result tells the reader: observed, and detail (a result that judged nothing has no observed)."""
    return f"{r.observed or ''} {r.detail or ''}"


def open_in_view(spark, dn) -> int:
    return spark.sql(f"SELECT * FROM {DQ}.v_open_keys WHERE dataset = '{dn.dataset}'").count()


# --- the two outcomes after a long gap ---

def test_a_key_that_recovered_while_the_hop_was_idle_is_cleared(spark) -> None:
    """KI-19: the FLAGGED event is older than the lookback. The hop's coverage does not continue from
    inside the lookback, so its open keys are read back to dq_key_event's retention: key 1 is judged
    again, CURRENT, CLEARED; no longer open."""
    up, dn = flagged_long_ago(spark, "oki_recovered")
    recovered(spark, dn)
    r = judge(ctx_of(spark, up, dn), EVENT)
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)
    assert events(r) == [("CLEARED", "CURRENT", KEY1)]
    publish(spark, dn, r, EVENT, "r-now")
    assert open_in_view(spark, dn) == 0


def test_a_key_still_missing_after_the_hop_was_idle_stays_open(spark) -> None:
    """KI-19: key 1 is still missing: judged again as an open key, STILL_FLAGGED, FAILED."""
    up, dn = flagged_long_ago(spark, "oki_missing")
    r = judge(ctx_of(spark, up, dn), EVENT)
    assert (r.state, r.population, r.violations) == ("FAILED", 1, 1)
    assert events(r) == [("STILL_FLAGGED", "MISSING", KEY1)]
    assert "1 key open from earlier runs" in r.observed


# --- continuity: the bounded read ---

def test_with_continuous_evaluations_the_open_key_read_stays_inside_the_lookback(spark, monkeypatch) -> None:
    """The hop was evaluated in EVENT (inside the lookback), which recorded key 1 again, so NEXT's read
    is bounded at history_since, and key 1 is still found there."""
    up, dn = flagged_long_ago(spark, "oki_bounded")
    now = judge(ctx_of(spark, up, dn), EVENT)
    publish(spark, dn, now, EVENT, "r-now")
    seen: list[str] = []
    real_sql = spark.sql
    monkeypatch.setattr(spark, "sql", lambda text: seen.append(text) or real_sql(text))
    nxt = judge(ctx_of(spark, up, dn, key_events_since=date(2019, 1, 1)), NEXT)
    reads = [s for s in seen if "v_committed_key_event" in s]
    assert reads and all(f"run_date >= DATE'{LOOKBACK.isoformat()}'" in s for s in reads)
    assert events(nxt) == [("STILL_FLAGGED", "MISSING", KEY1)]


# --- untrusted evidence ---

def test_a_key_flagged_by_invalid_evidence_is_not_open_but_is_judged_again(spark) -> None:
    """OLD's evaluation is found INVALID: its FLAGGED event is not trusted (key 1 is not open) and its
    coverage covers nothing, so EVENT judges OLD's range again from the layers: key 1 is FLAGGED anew,
    never STILL_FLAGGED on untrusted evidence."""
    up, dn = flagged_long_ago(spark, "oki_invalid", invalid=True)
    r = judge(ctx_of(spark, up, dn), EVENT)
    assert events(r) == [("FLAGGED", "MISSING", KEY1)]
    assert "open from earlier runs" not in said(r)


def test_uncommitted_key_events_are_not_open(spark) -> None:
    """OLD's run died before its commit: neither its coverage nor its FLAGGED event is visible."""
    up, dn = flagged_long_ago(spark, "oki_uncommitted", commit=False)
    r = judge(ctx_of(spark, up, dn), EVENT)
    assert "open from earlier runs" not in said(r)
    assert all(e != "STILL_FLAGGED" for e, _, _ in events(r))


# --- repeated evaluations ---

def test_a_cleared_key_is_cleared_once(spark) -> None:
    """After CLEARED, later evaluations neither judge key 1 as open nor record it again."""
    up, dn = flagged_long_ago(spark, "oki_once")
    recovered(spark, dn)
    r = judge(ctx_of(spark, up, dn), EVENT)
    assert events(r) == [("CLEARED", "CURRENT", KEY1)]
    publish(spark, dn, r, EVENT, "r-now")
    for i, event in enumerate((NEXT, Event("gold", NEXT.window_end, NEXT.window_end + timedelta(days=1)))):
        later = judge(ctx_of(spark, up, dn, key_events_since=date(2019, 1, 1)), event)
        assert later.reason_code == "empty_population" and events(later) == []
        publish(spark, dn, later, event, f"r-later-{i}")


# --- beyond retention ---

def test_an_open_key_past_dq_key_event_retention_is_disclosed_and_the_full_sweep_judges_it(spark) -> None:
    """Key 1's FLAGGED event is past dq_key_event's retention (key_events_since after it), in a store
    old enough to have dropped it: the hop cannot see it as open, and says so; the full sweep, which
    reads every upstream key, finds it still missing and flags it."""
    up, dn = flagged_long_ago(spark, "oki_retention")
    r = judge(ctx_of(spark, up, dn, key_events_since=date(2025, 7, 1)), EVENT)
    assert "open from earlier runs" not in said(r)
    assert "open keys are read back to 2025-07-01, dq_key_event's retention" in said(r)
    sweep = judge(ctx_of(spark, up, dn, key_events_since=date(2025, 7, 1), full_sweep=True), EVENT)
    assert (sweep.state, sweep.violations) == ("FAILED", 1)
    assert events(sweep) == [("FLAGGED", "MISSING", KEY1)]


# --- a long idle period, then catch-up ---

def test_catch_up_after_a_long_idle_period_omits_nothing(spark) -> None:
    """EVENT: key 1 (open since OLD, still missing) is judged as open, and key 2 (loaded while the hop
    was idle) is judged in the idle span, which the range covers (P1-02): both missing. NEXT, inside
    the lookback again: key 1 is still open; key 2, flagged in EVENT, is open too; nothing judged twice
    as new."""
    up, dn = flagged_long_ago(spark, "oki_catch_up")
    create_table(spark, up.table, CURATED_COLUMNS, [curated_row(sub_id="1", updated=FLAGGED_AT), curated_row(sub_id="2", updated=IDLE_AT)])
    now = judge(ctx_of(spark, up, dn, key_events_since=date(2019, 1, 1)), EVENT)
    assert (now.state, now.population, now.violations) == ("FAILED", 2, 2)
    assert events(now) == [("FLAGGED", "MISSING", KEY2), ("STILL_FLAGGED", "MISSING", KEY1)]
    publish(spark, dn, now, EVENT, "r-now")
    nxt = judge(ctx_of(spark, up, dn, key_events_since=date(2019, 1, 1)), NEXT)
    assert events(nxt) == [("STILL_FLAGGED", "MISSING", KEY1), ("STILL_FLAGGED", "MISSING", KEY2)]
    assert key_hash(SECRET, KEY1) != key_hash(SECRET, KEY2)
