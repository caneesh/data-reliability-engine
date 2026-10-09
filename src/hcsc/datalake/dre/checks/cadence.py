"""Cadence slots: when loads are expected (spec section 3, feed `cadence`).

Slots are local times in the cadence's time zone, converted to UTC. Calendars:
EVERYDAY and WEEKDAYS; a named calendar file is not supported yet.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from hcsc.datalake.dre.checks.times import as_utc

if TYPE_CHECKING:
    from hcsc.datalake.dre.config.models import Cadence


class UnsupportedCalendar(ValueError):
    pass


def _runs_on(day: date, calendar: str) -> bool:
    if calendar == "EVERYDAY":
        return True
    if calendar == "WEEKDAYS":
        return day.weekday() < 5
    raise UnsupportedCalendar(f"calendar {calendar!r} is not supported yet (EVERYDAY, WEEKDAYS)")


def _local_times(cadence: Cadence) -> list[time]:
    if cadence.kind == "interval":
        step = cadence.interval_minutes or 0
        return [time(m // 60, m % 60) for m in range(0, 24 * 60, step)]
    return [time.fromisoformat(t) for t in (cadence.times or ["00:00"])]


def slots(cadence: Cadence, start: datetime, end: datetime) -> list[datetime]:
    """UTC slot times in [start, end), oldest first."""
    start, end = as_utc(start), as_utc(end)
    if start >= end:
        return []
    zone = ZoneInfo(cadence.timezone)
    first_day = start.astimezone(zone).date() - timedelta(days=1)
    last_day = end.astimezone(zone).date() + timedelta(days=1)
    if cadence.kind == "calendar_dates":
        days = sorted(d for d in (cadence.dates or []) if first_day <= d <= last_day)
    else:
        days = [first_day + timedelta(days=i) for i in range((last_day - first_day).days + 1)]
    found = []
    for day in days:
        if cadence.kind != "calendar_dates" and not _runs_on(day, cadence.calendar):
            continue
        for local in _local_times(cadence):
            slot = datetime.combine(day, local, tzinfo=zone).astimezone(timezone.utc)
            if start <= slot < end:
                found.append(slot)
    return sorted(set(found))


def slot_label(cadence: Cadence, slot: datetime) -> str:
    """The slot's local time of day, e.g. 04:00: the key T1_VOLUME compares like with like on."""
    return as_utc(slot).astimezone(ZoneInfo(cadence.timezone)).strftime("%H:%M")
