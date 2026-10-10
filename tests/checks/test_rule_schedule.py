"""When gold rules are due: every_run, daily at rule_run_at, weekly on full_sweep_day."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from hcsc.datalake.dre.checks.rules.schedule import due_moment, is_due

UTC = timezone.utc
CHICAGO = "America/Chicago"
# Wednesday 2026-01-14 15:00 UTC = 09:00 in Chicago (UTC-6 in January).
RUN = datetime(2026, 1, 14, 15, 0, tzinfo=UTC)


def test_daily_rules_fall_due_at_run_at_local_time() -> None:
    assert due_moment("daily", RUN, "06:00", CHICAGO, "SUNDAY") == datetime(2026, 1, 14, 12, tzinfo=UTC)
    # Before today's run_at, the due moment is yesterday's.
    assert due_moment("daily", RUN, "10:00", CHICAGO, "SUNDAY") == datetime(2026, 1, 13, 16, tzinfo=UTC)


def test_weekly_rules_fall_due_on_the_sweep_day() -> None:
    assert due_moment("weekly", RUN, "06:00", CHICAGO, "SUNDAY") == datetime(2026, 1, 11, 12, tzinfo=UTC)
    assert due_moment("weekly", RUN, "06:00", CHICAGO, "WEDNESDAY") == datetime(2026, 1, 14, 12, tzinfo=UTC)
    assert due_moment("weekly", RUN, "10:00", CHICAGO, "WEDNESDAY") == datetime(2026, 1, 7, 16, tzinfo=UTC)


@pytest.mark.parametrize(("frequency", "completed", "due"), [
    ("every_run", RUN - timedelta(minutes=5), True),
    ("daily", None, True),                                    # never ran
    ("daily", datetime(2026, 1, 14, 11, 59, tzinfo=UTC), True),   # ran before today's 06:00
    ("daily", datetime(2026, 1, 14, 12, 0, tzinfo=UTC), False),   # ran at or after it
    ("weekly", datetime(2026, 1, 12, tzinfo=UTC), False),         # ran since Sunday 06:00
    ("weekly", datetime(2026, 1, 10, tzinfo=UTC), True),
])
def test_is_due(frequency: str, completed: datetime | None, due: bool) -> None:
    assert is_due(frequency, RUN, "06:00", CHICAGO, "SUNDAY", completed) is due
