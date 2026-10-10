"""Hourly runs evaluate only feeds with something due (spec section 8)."""

from __future__ import annotations

from datetime import datetime, timezone

from hcsc.datalake.dre.checks.events import Event, window_for
from hcsc.datalake.dre.config.loader import load
from hcsc.datalake.dre.runner import due_feeds, only_due, plan
from tests.config.conftest import SAMPLE_CONF
from tests.fixtures.settings import SETTINGS

UTC = timezone.utc


def config():
    loaded, errors = load(SAMPLE_CONF)
    assert errors == []
    return loaded


def events(work, start: datetime, end: datetime) -> dict[str, Event]:
    return {p.dataset.dataset: Event(p.dataset.dataset, start, end) for p in work}


def test_feed_is_due_when_a_slot_deadline_passed_in_its_window() -> None:
    cfg = config()
    work = plan(cfg)
    # example_realtime: slots every 4 hours (Chicago), SLA 8h. The 00:30 Chicago slot (06:30 UTC)
    # on 15 Jan has its deadline at 14:30 UTC.
    passed = events(work, datetime(2026, 1, 15, 14, tzinfo=UTC), datetime(2026, 1, 15, 15, tzinfo=UTC))
    assert "example_realtime" in due_feeds(cfg, work, passed, {}, set())
    quiet = events(work, datetime(2026, 1, 15, 14, 45, tzinfo=UTC), datetime(2026, 1, 15, 15, 45, tzinfo=UTC))
    assert "example_realtime" not in due_feeds(cfg, work, quiet, {}, set())


def test_monthly_feed_is_due_once_a_month() -> None:
    cfg = config()
    work = plan(cfg)
    # provider_roster_monthly: 1st of the month 06:00 UTC, SLA 48h, so due when 3 Feb 06:00 passes.
    due = events(work, datetime(2026, 2, 3, 5, tzinfo=UTC), datetime(2026, 2, 3, 6, 1, tzinfo=UTC))
    assert "provider_roster_monthly" in due_feeds(cfg, work, due, {}, set())
    mid_month = events(work, datetime(2026, 2, 15, tzinfo=UTC), datetime(2026, 2, 15, 1, tzinfo=UTC))
    assert due_feeds(cfg, work, mid_month, {}, set()) == set()


def test_new_landed_files_or_force_make_a_feed_due() -> None:
    cfg = config()
    work = plan(cfg)
    mid_month = events(work, datetime(2026, 2, 15, 14, 45, tzinfo=UTC), datetime(2026, 2, 15, 15, 45, tzinfo=UTC))
    assert due_feeds(cfg, work, mid_month, {"provider_roster_monthly": 1}, set()) == {"provider_roster_monthly"}
    assert due_feeds(cfg, work, mid_month, {"provider_roster_monthly": 0}, set()) == set()
    assert due_feeds(cfg, work, mid_month, {}, {"provider_directory_merge"}) == {"provider_directory_merge"}


def test_only_due_keeps_due_feeds_and_their_table_wide_datasets() -> None:
    cfg = config()
    work = plan(cfg)
    kept = {(p.feed.feed if p.feed else None, p.dataset.dataset) for p in only_due(work, {"example_realtime"})}
    assert ("example_realtime", "gold_member_coverage") in kept
    # The table-wide dataset shares the gold table with a due feed: it runs too.
    assert (None, "gold_member_coverage_all") in kept
    assert not {k for k in kept if k[0] in ("provider_roster_monthly", "provider_directory_merge")}
    nothing = {(p.feed.feed if p.feed else None, p.dataset.dataset) for p in only_due(work, set())}
    assert nothing == set()  # its table's feed is not due, so the table-wide dataset waits too


def test_check_delay_moves_the_window_end_back() -> None:
    cfg = config()
    ds = cfg.datasets["gold_member_coverage"]
    run_start = datetime(2026, 1, 15, 12, 7, tzinfo=UTC)
    plain = window_for(ds, run_start, None, SETTINGS, "America/Chicago")
    delayed = window_for(ds, run_start, None, SETTINGS, "America/Chicago", check_delay_minutes=60)
    assert plain.window_end == datetime(2026, 1, 15, 11, 52, tzinfo=UTC)
    assert delayed.window_end == datetime(2026, 1, 15, 10, 52, tzinfo=UTC)
