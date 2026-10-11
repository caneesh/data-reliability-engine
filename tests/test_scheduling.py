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


def test_schema_drift_is_kept_on_a_due_dataset_of_the_table() -> None:
    """Review finding 10: two feeds' datasets share one raw table; only feed B is due. Choosing the
    one T1_SCHEMA_DRIFT per table after dropping feeds that are not due keeps B's."""
    from types import SimpleNamespace

    from hcsc.datalake.dre.runner import SCHEMA_DRIFT, Planned, once_per_table

    def planned(feed: str, dataset: str, check: str) -> Planned:
        return Planned(SimpleNamespace(feed=feed), SimpleNamespace(dataset=dataset, table="raw_db.shared"),
                       SimpleNamespace(check_id=check))

    work = [planned("feed_a", "raw_a", SCHEMA_DRIFT), planned("feed_a", "raw_a", "T1_ON_TIME"),
            planned("feed_b", "raw_b", SCHEMA_DRIFT), planned("feed_b", "raw_b", "T1_ON_TIME")]
    kept = once_per_table(only_due(work, {"feed_b"}), SCHEMA_DRIFT)
    assert [(p.dataset.dataset, p.check.check_id) for p in kept] == [("raw_b", SCHEMA_DRIFT), ("raw_b", "T1_ON_TIME")]
    # Choosing first (the old order) would have dropped B's drift check along with feed A.
    assert [p.dataset.dataset for p in only_due(once_per_table(work, SCHEMA_DRIFT), {"feed_b"})
            if p.check.check_id == SCHEMA_DRIFT] == []
