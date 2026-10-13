"""Named calendars (spec section 3): conf/calendars/<name>.yaml names the days a feed does not
load on, so cadence slots skip them; unknown calendars are config errors with file and line."""

from __future__ import annotations

import shutil
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from hcsc.datalake.dre.checks.cadence import UnsupportedCalendar, slots
from hcsc.datalake.dre.checks.calendars import CALENDARS, CalendarFile, calendar_from, runs_on
from hcsc.datalake.dre.config.loader import load
from hcsc.datalake.dre.config.models import Cadence
from hcsc.datalake.dre.config.validate import validate_conf
from tests.config.conftest import SAMPLE_CONF, edit, line_with

UTC = timezone.utc
FEED = "feeds/example_realtime.yaml"
US_BUSINESS = """calendar: US_BUSINESS
weekdays_only: true
holidays: [2026-01-01, 2026-01-19, 2026-07-03]
"""


@pytest.fixture
def conf(tmp_path: Path) -> Path:
    target = tmp_path / "conf"
    shutil.copytree(SAMPLE_CONF, target)
    (target / "calendars").mkdir()
    (target / "calendars" / "us_business.yaml").write_text(US_BUSINESS, encoding="utf-8")
    return target


def cadence(calendar: str) -> Cadence:
    return Cadence(kind="times", times=["06:00"], timezone="UTC", calendar=calendar)


def test_the_packaged_calendars() -> None:
    assert CALENDARS.ids() == ("EVERYDAY", "WEEKDAYS")
    assert runs_on(date(2026, 1, 17), "EVERYDAY") and not runs_on(date(2026, 1, 17), "WEEKDAYS")  # a Saturday
    assert runs_on(date(2026, 1, 19), "WEEKDAYS")


def test_a_conf_calendar_skips_its_holidays_and_weekends(conf: Path) -> None:
    with pytest.raises(ValueError, match="not a calendar"):
        cadence("US_BUSINESS")  # not until the conf is loaded
    _, errors = load(conf)
    assert errors == [], "\n".join(map(str, errors))
    assert CALENDARS["US_BUSINESS"].origin == "conf"
    found = slots(cadence("US_BUSINESS"), datetime(2026, 1, 15, tzinfo=UTC), datetime(2026, 1, 21, tzinfo=UTC))
    # 15 Thu, 16 Fri, (17, 18 weekend), (19 holiday), 20 Tue
    assert [s.date() for s in found] == [date(2026, 1, 15), date(2026, 1, 16), date(2026, 1, 20)]
    CALENDARS.replace_conf({"ALL_BUT_NEW_YEAR": calendar_from(
        CalendarFile(calendar="ALL_BUT_NEW_YEAR", weekdays_only=False, holidays=[date(2026, 1, 1)]))})
    found = slots(cadence("ALL_BUT_NEW_YEAR"), datetime(2025, 12, 31, tzinfo=UTC), datetime(2026, 1, 4, tzinfo=UTC))
    assert [s.date() for s in found] == [date(2025, 12, 31), date(2026, 1, 2), date(2026, 1, 3)]


def test_a_feed_on_a_conf_calendar_validates_and_an_unknown_one_does_not(conf: Path) -> None:
    edit(conf, FEED, "calendar: EVERYDAY", "calendar: US_BUSINESS")
    _, errors, _ = validate_conf(conf)
    assert errors == [], "\n".join(map(str, errors))
    edit(conf, FEED, "calendar: US_BUSINESS", "calendar: HOLIDAYS_XX")
    _, errors, _ = validate_conf(conf)
    [e] = [e for e in errors if e.field == "cadence.calendar"]
    assert "'HOLIDAYS_XX' is not a calendar" in e.problem and "US_BUSINESS" in e.fix and "conf/calendars" in e.fix
    assert e.line == line_with(conf, FEED, "calendar: HOLIDAYS_XX")


CASES = [
    ("bad-date", "holidays: [2026-01-01, 2026-01-19, 2026-07-03]", "holidays: ['2026-13-01']", "holidays[0]"),
    ("bad-yaml-date", "holidays: [2026-01-01, 2026-01-19, 2026-07-03]", "holidays: [2026-13-01]", ""),  # PyYAML itself rejects it
    ("packaged-name", "calendar: US_BUSINESS", "calendar: WEEKDAYS", "calendar"),
    ("unknown-field", "weekdays_only: true", "weekdays_only: true\nextra: 1", "extra"),
]


@pytest.mark.parametrize(("case", "old", "new", "field"), CASES, ids=[c[0] for c in CASES])
def test_calendar_file_errors_name_file_line_and_field(conf: Path, case, old, new, field) -> None:
    path = conf / "calendars" / "us_business.yaml"
    path.write_text(path.read_text(encoding="utf-8").replace(old, new), encoding="utf-8")
    _, errors, _ = validate_conf(conf)
    [e] = [e for e in errors if e.file == str(path)]
    assert e.field == field and e.line >= 1
    if case == "packaged-name":
        assert "already a calendar (packaged)" in e.problem
    if case == "bad-yaml-date":
        assert "month must be in 1..12" in e.problem and "quote it" in e.fix


def test_an_unknown_calendar_at_run_time_is_still_guarded() -> None:
    unchecked = Cadence.model_construct(kind="times", times=["00:30"], interval_minutes=None, dates=None,
                                        timezone="America/Chicago", calendar="GONE")
    with pytest.raises(UnsupportedCalendar):
        slots(unchecked, datetime(2026, 1, 15, tzinfo=UTC), datetime(2026, 1, 16, tzinfo=UTC))
