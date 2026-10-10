"""Pydantic models for feeds, datasets, rules and defaults (spec section 3).

Every model rejects unknown fields, so a misspelt field is an error rather than
a silently ignored setting. Names that end up in SQL (columns, tables) must be
plain identifiers; SQL fragments from config may not contain `;`.

Checks that need more than one object (references, key_map coverage, settings
resolution) are in validate.py.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import PurePosixPath, PureWindowsPath
from typing import Annotated, Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import AfterValidator, BaseModel, BeforeValidator, ConfigDict, Field, model_validator
from pydantic_core import PydanticCustomError

from hcsc.datalake.dre.store.names import DQ_TABLES, validate_dq_database

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_HHMM = re.compile(r"([01]\d|2[0-3]):[0-5]\d")
_ORDER_TERM = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(\s+(ASC|DESC))?", re.I)
_EMAIL = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")


def invalid(problem: str, fix: str) -> PydanticCustomError:
    """A validation error that carries its own fix (read back by the loader)."""
    return PydanticCustomError("dre_invalid", "{problem}", {"problem": problem, "fix": fix})


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise invalid(
            f"{value!r} is not a plain identifier",
            "use letters, digits and underscores only, not starting with a digit",
        )
    return value


def _table(value: str) -> str:
    parts = value.split(".")
    if len(parts) != 2 or not all(_IDENTIFIER.fullmatch(p) for p in parts):
        raise invalid(f"{value!r} is not <database>.<table>", "write it as database.table, e.g. gold_db.member_coverage")
    return value


def _sql_fragment(value: str) -> str:
    if ";" in value:
        raise invalid("SQL fragment contains ';'", "use a single SQL expression without ';'")
    return value


def _hhmm(value: str) -> str:
    if not _HHMM.fullmatch(value):
        raise invalid(f"{value!r} is not a time of day", 'use quoted 24-hour HH:MM, e.g. "04:00"')
    return value


def _order_term(value: str) -> str:
    if not _ORDER_TERM.fullmatch(value.strip()):
        raise invalid(f"{value!r} is not an order term", 'use "<column> ASC" or "<column> DESC"')
    return value


def _email(value: str) -> str:
    if not _EMAIL.fullmatch(value):
        raise invalid(f"{value!r} is not an email address", "use name@domain")
    return value


def _timezone(value: str) -> str:
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError):
        raise invalid(f"{value!r} is not a known time zone", "use an IANA name, e.g. America/Chicago") from None
    return value


def _dq_database(value: str) -> str:
    try:
        return validate_dq_database(value)
    except ValueError as exc:
        raise invalid(str(exc), "use a plain identifier, e.g. dq") from None


def _regex(value: str) -> str:
    try:
        re.compile(value)
    except re.error as exc:
        raise invalid(f"pattern {value!r} does not compile: {exc}", "fix the regular expression") from None
    return value


def _calendar(value: str) -> str:
    if value not in ("EVERYDAY", "WEEKDAYS"):
        raise invalid(
            f"calendar {value!r} looks like a named calendar file, which release 1 does not support",
            "use EVERYDAY or WEEKDAYS, or list the run dates with kind: calendar_dates",
        )
    return value


def _normaliser(value: Any) -> Any:
    """strip_leading_zeros, or {parse_date: <Spark date format>} (a date string read as an ISO date)."""
    if value == "strip_leading_zeros":
        return value
    if isinstance(value, dict) and set(value) == {"parse_date"} and isinstance(value["parse_date"], str) \
            and value["parse_date"] and "'" not in value["parse_date"]:
        return value
    raise invalid(f"{value!r} is not allowed",
                  "use strip_leading_zeros, or { parse_date: <format> } such as { parse_date: MM/dd/yyyy }")


def _str_to_list(value: Any) -> Any:
    return [value] if isinstance(value, str) else value


Identifier = Annotated[str, AfterValidator(_identifier)]
TableName = Annotated[str, AfterValidator(_table)]
SqlFragment = Annotated[str, AfterValidator(_sql_fragment)]
TimeOfDay = Annotated[str, AfterValidator(_hhmm)]
OrderTerm = Annotated[str, AfterValidator(_order_term)]
Email = Annotated[str, AfterValidator(_email)]
TimeZone = Annotated[str, AfterValidator(_timezone)]
NonEmptyStr = Annotated[str, Field(min_length=1)]

Pattern = Literal["FILE_CYCLIC", "FILE_PERIODIC", "TABLE_MERGE"]
FileFormat = Literal["sequence", "text", "xml", "csv", "parquet", "orc"]
CadenceKind = Literal["times", "interval", "calendar_dates", "monthly"]
Layer = Literal["RAW", "CURATED", "CDC", "GOLD"]
Granularity = Literal["second", "minute", "hour", "day"]
Weekday = Literal["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]
Template = Literal[
    "max_rows_per_key",
    "max_open_rows_per_key",
    "column_order",
    "superseded_still_open",
    "child_within_parent",
    "value_format",
    "null_rate_max",
]
RuleStatus = Literal["proposed", "approved", "retired"]
RuleFrequency = Literal["every_run", "daily", "weekly"]
Severity = Literal["high", "medium", "low"]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# --- Settings: fields that may be set in defaults.yaml, a feed or a dataset ---


class SettingsOverride(Model):
    """Settings at one level. null (or absent) means: inherit from the level above."""

    sla_hours: float | None = Field(default=None, gt=0)
    email_sample_keys: bool | None = None
    min_rows_per_load: int | None = Field(default=None, ge=0)
    volume_tolerance_pct: float | None = Field(default=None, gt=0)
    compute_budget_minutes: float | None = Field(default=None, gt=0)
    full_sweep_day: Weekday | None = None
    settle_minutes: int | None = Field(default=None, ge=0)
    initial_lookback_hours: float | None = Field(default=None, gt=0)
    max_window_hours: float | None = Field(default=None, gt=0)


SETTING_FIELDS: tuple[str, ...] = tuple(SettingsOverride.model_fields)


class Settings(Model):
    """Settings after resolving defaults, then feed, then dataset. All required."""

    sla_hours: float = Field(gt=0)
    email_sample_keys: bool
    min_rows_per_load: int = Field(ge=0)
    volume_tolerance_pct: float = Field(gt=0)
    compute_budget_minutes: float = Field(gt=0)
    full_sweep_day: Weekday
    settle_minutes: int = Field(ge=0)
    initial_lookback_hours: float = Field(gt=0)
    max_window_hours: float = Field(gt=0)


# --- defaults.yaml ---


class EmailServer(Model):
    """Where the digest is sent from (spec section 8). smtp_host null: digests are built but not
    sent (the run says so). Values come from the platform team (spec section 11)."""

    smtp_host: NonEmptyStr | None = None
    smtp_port: int = Field(default=25, ge=1, le=65535)
    sender: Email | None = None


class Defaults(SettingsOverride):
    dq_database: Annotated[str, AfterValidator(_dq_database)]
    environment: NonEmptyStr
    timezone: TimeZone  # for time columns that do not set their own
    hmac_secret_file: str | None = None
    retention_months: dict[str, Annotated[int, Field(ge=1)]]
    recipients: dict[NonEmptyStr, Annotated[list[Email], Field(min_length=1)]] = {}
    email: EmailServer = EmailServer()
    # Spec defaults (sections 6 and 8, constraint 6).
    email_sample_keys: bool | None = False
    volume_tolerance_pct: float | None = Field(default=50, gt=0)
    full_sweep_day: Weekday | None = "SUNDAY"
    # Event windows (spec section 4).
    settle_minutes: int | None = Field(default=15, ge=0)
    initial_lookback_hours: float | None = Field(default=24, gt=0)
    max_window_hours: float | None = Field(default=72, gt=0)  # longest a PLATFORM or BUDGET hold lasts
    # Gold rules (spec section 6): daily rules run at the first run at or after this local time
    # (defaults.yaml timezone); weekly rules on the dataset's full_sweep_day at this time.
    rule_run_at: TimeOfDay = "06:00"
    # The watchdog (spec section 8) expects the hourly run's dq_run rows this long after the hour.
    watchdog_grace_minutes: int = Field(default=30, ge=1, le=59)

    @model_validator(mode="after")
    def _check(self) -> Defaults:
        missing = [t for t in DQ_TABLES if t not in self.retention_months]
        unknown = [t for t in self.retention_months if t not in DQ_TABLES]
        if missing or unknown:
            raise invalid(
                f"retention_months must list exactly the dq tables; missing {missing}, unknown {unknown}",
                f"set retention_months for each of {list(DQ_TABLES)}",
            )
        if self.hmac_secret_file is not None:
            path = self.hmac_secret_file
            if not (PurePosixPath(path).is_absolute() or PureWindowsPath(path).is_absolute()):
                raise invalid(
                    "hmac_secret_file must be an absolute path",
                    "point it at a protected file outside the repository; never commit the secret",
                )
        return self


# --- feeds ---


class Landing(Model):
    roots: Annotated[list[NonEmptyStr], Field(min_length=1)]
    file_format: FileFormat
    file_name_pattern: NonEmptyStr = "*"


class Cadence(Model):
    kind: CadenceKind
    times: list[TimeOfDay] | None = None
    interval_minutes: int | None = Field(default=None, gt=0)
    dates: list[date] | None = None
    days_of_month: list[Annotated[int, Field(ge=1, le=31)]] | None = None  # kind monthly
    timezone: TimeZone
    calendar: Annotated[str, AfterValidator(_calendar)] = "EVERYDAY"  # EVERYDAY | WEEKDAYS (named files: not in release 1)

    @model_validator(mode="after")
    def _check(self) -> Cadence:
        needs = {"times": "times", "interval": "interval_minutes", "calendar_dates": "dates",
                 "monthly": "days_of_month"}[self.kind]
        if not getattr(self, needs):
            raise invalid(f"cadence kind {self.kind} needs `{needs}`", f"add `{needs}` under cadence")
        return self


class Feed(SettingsOverride):
    feed: Identifier
    expectation_version: int = Field(ge=1)
    pattern: Pattern
    owner: NonEmptyStr
    landing: Landing | None = None
    cadence: Cadence
    datasets: Annotated[list[Identifier], Field(min_length=1)]
    check_delay_minutes: int = Field(default=0, ge=0)  # evaluate a slot this long after its deadline
    # Cause probe parameters by config key (spec section 7; checked against the pattern by validate).
    probes: dict[Identifier, dict[str, Any] | list[dict[str, Any]] | None] = {}

    @model_validator(mode="after")
    def _check(self) -> Feed:
        if self.pattern.startswith("FILE_") and self.landing is None:
            raise invalid(
                f"pattern {self.pattern} needs `landing`",
                "add landing: with roots and file_format",
            )
        return self


# --- datasets ---


class TimeColumn(Model):
    """A time column and how to read it.

    format null: the column is already DATE or TIMESTAMP. timezone: the IANA zone
    the values are written in (null: defaults.yaml `timezone`); values are
    converted to UTC before any comparison.
    """

    column: Identifier
    format: NonEmptyStr | None = None
    granularity: Granularity | None = None
    timezone: TimeZone | None = None


class WinnerRule(Model):
    order_by: Annotated[list[OrderTerm], Field(min_length=1)]


class Dataset(SettingsOverride):
    dataset: Identifier
    table: TableName
    layer: Layer
    upstream: list[Identifier] = []
    feed_filter: SqlFragment | None = None
    key: Annotated[list[Identifier], Field(min_length=1)]
    key_normalise: dict[Identifier, Annotated[Any, AfterValidator(_normaliser)]] = {}
    owner: NonEmptyStr | None = None  # only for table-wide datasets that no feed lists
    expectation_version: int | None = Field(default=None, ge=1)  # only for table-wide datasets
    record_time: TimeColumn | None = None
    load_time: TimeColumn | None = None
    partition_column: Identifier | None = None
    file_name_column: Identifier | None = None
    key_unique: bool = False
    group_by: list[Identifier] = []
    key_map: dict[Identifier, dict[Identifier, Identifier]] = {}
    owned_columns: dict[Identifier, Identifier] = {}
    mismatch_probe_drop: list[Identifier] = []
    winner_rule: WinnerRule | None = None


# --- rules ---


class Rule(Model):
    rule: Identifier
    template: Template
    dataset: Identifier
    params: dict[str, Any] = {}
    group_by: list[Identifier] = []
    owner: NonEmptyStr
    status: RuleStatus
    severity: Severity
    frequency: RuleFrequency = "daily"


class MaxRowsPerKey(Model):
    max: int = Field(ge=0)


class MaxOpenRowsPerKey(Model):
    open_when: SqlFragment
    max: int = Field(ge=0)


class ColumnOrder(Model):
    lower: Identifier
    upper: Identifier
    nulls_fail: bool = False
    format: NonEmptyStr | None = None  # parse both columns as times with this format (never compare as strings)


class SupersededStillOpen(Model):
    group_key: Annotated[list[Identifier], Field(min_length=1), BeforeValidator(_str_to_list)]
    order_column: Identifier
    open_when: SqlFragment
    format: NonEmptyStr | None = None  # parse order_column as a time with this format


class ChildWithinParent(Model):
    parent_dataset: Identifier
    join: Annotated[dict[Identifier, Identifier], Field(min_length=1)]  # child column: parent column
    child_start: Identifier
    parent_start: Identifier
    parent_end: Identifier
    format: NonEmptyStr | None = None  # parse the three time columns with this format


class ValueFormat(Model):
    column: Identifier
    pattern: Annotated[str, Field(min_length=1), AfterValidator(_regex)]


class NullRateMax(Model):
    column: Identifier
    max_rate: float = Field(ge=0, le=1)


TEMPLATE_PARAMS: dict[str, type[Model]] = {
    "max_rows_per_key": MaxRowsPerKey,
    "max_open_rows_per_key": MaxOpenRowsPerKey,
    "column_order": ColumnOrder,
    "superseded_still_open": SupersededStillOpen,
    "child_within_parent": ChildWithinParent,
    "value_format": ValueFormat,
    "null_rate_max": NullRateMax,
}
