"""Cadence slots in UTC from local times, calendars and kinds."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from hcsc.datalake.dre.checks.cadence import UnsupportedCalendar, slot_label, slots
from hcsc.datalake.dre.config.models import Cadence

UTC = timezone.utc


def cadence(**fields) -> Cadence:
    base = {"kind": "times", "times": ["00:30", "12:30"], "timezone": "America/Chicago"}
    return Cadence(**{**base, **fields})


def test_times_are_local_and_converted_to_utc_across_dst() -> None:
    winter = slots(cadence(), datetime(2026, 1, 15, tzinfo=UTC), datetime(2026, 1, 16, tzinfo=UTC))
    assert winter == [datetime(2026, 1, 15, 6, 30, tzinfo=UTC), datetime(2026, 1, 15, 18, 30, tzinfo=UTC)]
    summer = slots(cadence(), datetime(2026, 7, 15, tzinfo=UTC), datetime(2026, 7, 16, tzinfo=UTC))
    assert summer == [datetime(2026, 7, 15, 5, 30, tzinfo=UTC), datetime(2026, 7, 15, 17, 30, tzinfo=UTC)]


def test_window_is_half_open() -> None:
    slot = datetime(2026, 1, 15, 6, 30, tzinfo=UTC)
    assert slots(cadence(), slot, datetime(2026, 1, 15, 7, tzinfo=UTC)) == [slot]
    assert slots(cadence(), datetime(2026, 1, 15, 6, tzinfo=UTC), slot) == []


def test_weekdays_skip_the_weekend() -> None:
    # 2026-01-17 and 18 are Saturday and Sunday.
    found = slots(cadence(calendar="WEEKDAYS", times=["08:00"]),
                  datetime(2026, 1, 16, tzinfo=UTC), datetime(2026, 1, 20, tzinfo=UTC))
    assert [s.date() for s in found] == [date(2026, 1, 16), date(2026, 1, 19)]


def test_interval_and_calendar_dates() -> None:
    hourly = slots(cadence(kind="interval", interval_minutes=360, times=None),
                   datetime(2026, 1, 15, 6, tzinfo=UTC), datetime(2026, 1, 16, 6, tzinfo=UTC))
    assert len(hourly) == 4  # every 6 hours from local midnight
    dated = slots(cadence(kind="calendar_dates", dates=[date(2026, 1, 15)], times=["09:00"]),
                  datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC))
    assert dated == [datetime(2026, 1, 15, 15, 0, tzinfo=UTC)]


def test_named_calendar_is_not_supported_yet() -> None:
    with pytest.raises(UnsupportedCalendar):
        slots(cadence(calendar="holidays.yaml"), datetime(2026, 1, 15, tzinfo=UTC), datetime(2026, 1, 16, tzinfo=UTC))


def test_slot_label_is_local_time_of_day() -> None:
    assert slot_label(cadence(), datetime(2026, 1, 15, 6, 30, tzinfo=UTC)) == "00:30"
