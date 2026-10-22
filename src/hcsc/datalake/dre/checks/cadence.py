"""Cadence slots: when loads are expected (spec section 3, feed `cadence`).

Slots are local times in the cadence's time zone, converted to UTC. Kinds: times
(every day at each time), interval (every N minutes from local midnight),
calendar_dates (listed dates), monthly (listed days of the month; a day past the
month's end is skipped). Calendars, for times and interval: EVERYDAY, WEEKDAYS and the named
calendars from conf/calendars (checks/calendars.py).
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
    from hcsc.datalake.dre.checks.calendars import CALENDARS

    found = CALENDARS.get(calendar)
    if found is None:
        raise UnsupportedCalendar(f"calendar {calendar!r} is not known (packaged: EVERYDAY, WEEKDAYS; or conf/calendars)")
    return found.runs_on(day)


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
    elif cadence.kind == "monthly":
        days = [first_day + timedelta(days=i) for i in range((last_day - first_day).days + 1)]
        days = [d for d in days if d.day in set(cadence.days_of_month or [])]
    else:
        days = [first_day + timedelta(days=i) for i in range((last_day - first_day).days + 1)]
    found = []
    for day in days:
        if cadence.kind in ("times", "interval") and not _runs_on(day, cadence.calendar):
            continue
        for local in _local_times(cadence):
            slot = datetime.combine(day, local, tzinfo=zone).astimezone(timezone.utc)
            if start <= slot < end:
                found.append(slot)
    return sorted(set(found))


def previous_slot(cadence: Cadence, before: datetime) -> datetime | None:
    """The latest slot strictly before `before`, looking back up to two years (monthly and
    calendar_dates cadences can have slots weeks apart)."""
    for days in (8, 63, 740):
        found = slots(cadence, as_utc(before) - timedelta(days=days), before)
        if found:
            return found[-1]
    return None


def slot_label(cadence: Cadence, slot: datetime) -> str:
    """The slot's local time of day, e.g. 04:00: the key T1_VOLUME compares like with like on."""
    return as_utc(slot).astimezone(ZoneInfo(cadence.timezone)).strftime("%H:%M")
