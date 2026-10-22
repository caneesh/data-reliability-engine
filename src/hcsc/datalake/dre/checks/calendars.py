"""Cadence calendars (spec section 3, feed `cadence.calendar`): which days a feed runs on.

EVERYDAY and WEEKDAYS are packaged. A deployed instance adds named calendars under
conf/calendars/<name>.yaml: weekdays only or not, plus listed holidays, so a feed that does not
load on holidays is not reported late on them. Calendars are a registry like the other
capabilities; the config loader registers the conf ones (`CALENDARS.replace_conf`) before the feeds load.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable, Literal

from hcsc.datalake.dre.config.models import Model, NonEmptyStr
from hcsc.datalake.dre.registry import Registry


@dataclass(frozen=True)
class Calendar:
    name: str
    runs_on: Callable[[date], bool]
    origin: Literal["packaged", "conf", "extension"] = "packaged"


CALENDARS: Registry[Calendar] = Registry("calendar")
CALENDARS.register("EVERYDAY", Calendar("EVERYDAY", lambda day: True))
CALENDARS.register("WEEKDAYS", Calendar("WEEKDAYS", lambda day: day.weekday() < 5))


class CalendarFile(Model):
    """One conf/calendars/<name>.yaml file."""

    calendar: NonEmptyStr  # the name feeds use, upper case by convention
    weekdays_only: bool = True
    holidays: list[date] = []  # days the feed does not run on, on top of weekends when weekdays_only


def calendar_from(file: CalendarFile) -> Calendar:
    holidays = frozenset(file.holidays)

    def runs_on(day: date) -> bool:
        if file.weekdays_only and day.weekday() >= 5:
            return False
        return day not in holidays

    return Calendar(file.calendar, runs_on, "conf")


def runs_on(day: date, calendar: str) -> bool:
    """Whether a feed on this calendar runs on the day; raises KeyError for an unknown calendar."""
    return CALENDARS[calendar].runs_on(day)
