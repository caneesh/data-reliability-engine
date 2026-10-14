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

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator
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
    from hcsc.datalake.dre.config.sqlcheck import forbidden_sql

    if ";" in value:
        raise invalid("SQL fragment contains ';'", "use a single SQL expression without ';'")
    problem = forbidden_sql(value)
    if problem is not None:
        raise invalid(problem.problem, problem.fix)
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


def _sink_config(value: Any) -> Any:
    from hcsc.datalake.dre.notify.sinks import validate_sink_config

    try:
        return validate_sink_config(value)
    except ValueError as exc:
        raise invalid(str(exc), "write { kind: email | stdout, ... }; see docs/spec.md section 8") from None


def _stem(value: str) -> str:
    name = value.lower()
    return name[:-5] if name.endswith(".yaml") else name[:-4] if name.endswith(".yml") else name


def _calendar(value: str) -> str:
    """EVERYDAY, WEEKDAYS, or a named calendar from conf/calendars (checks/calendars.py)."""
    from hcsc.datalake.dre.checks.calendars import CALENDARS

    if value not in CALENDARS:
        raise invalid(f"{value!r} is not a calendar",
                      f"use one of {list(CALENDARS.ids())}, add conf/calendars/{_stem(value)}.yaml, "
                      "or list the run dates with kind: calendar_dates")
    return value


def _normaliser(value: Any) -> Any:
    """A registered key normaliser (checks/keys.py): a bare name, or {name: argument}."""
    from hcsc.datalake.dre.checks.keys import NormaliserError, validate_normaliser

    try:
        return validate_normaliser(value)
    except NormaliserError as exc:
        raise invalid(exc.problem, exc.fix) from None


def _pattern(value: str) -> str:
    """A feed pattern: packaged, or from the conf directory's patterns/ (checks/registry.py)."""
    from hcsc.datalake.dre.checks.patterns import patterns

    if value not in patterns():
        raise invalid(f"{value!r} is not allowed", f"use one of {list(patterns())}")
    return value


def _template(value: str) -> str:
    """A registered gold rule template (checks/rules/template.py)."""
    from hcsc.datalake.dre.checks.rules.template import TEMPLATES

    if value not in TEMPLATES:
        raise invalid(f"{value!r} is not allowed", f"use one of {list(TEMPLATES.ids())}")
    return value


Identifier = Annotated[str, AfterValidator(_identifier)]
TableName = Annotated[str, AfterValidator(_table)]
SqlFragment = Annotated[str, AfterValidator(_sql_fragment)]
TimeOfDay = Annotated[str, AfterValidator(_hhmm)]
OrderTerm = Annotated[str, AfterValidator(_order_term)]
Email = Annotated[str, AfterValidator(_email)]
TimeZone = Annotated[str, AfterValidator(_timezone)]
NonEmptyStr = Annotated[str, Field(min_length=1)]
Regex = Annotated[str, Field(min_length=1), AfterValidator(_regex)]  # a regular expression that compiles

Pattern = Annotated[str, AfterValidator(_pattern)]  # a packaged feed pattern
FileFormat = Literal["sequence", "text", "xml", "csv", "parquet", "orc"]
CadenceKind = Literal["times", "interval", "calendar_dates", "monthly"]
Layer = Literal["RAW", "CURATED", "CDC", "GOLD"]
Granularity = Literal["second", "minute", "hour", "day"]
Weekday = Literal["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]
Template = Annotated[str, AfterValidator(_template)]  # a registered gold rule template
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
    # An owner's digest with nothing failing or not run ("all clear"): sent every run (spec
    # default), only from the run starting in the hour from rule_run_at (daily), or never.
    all_clear_digest: Literal["every_run", "daily", "never"] = "every_run"


class Defaults(SettingsOverride):
    dq_database: Annotated[str, AfterValidator(_dq_database)]
    environment: NonEmptyStr
    timezone: TimeZone  # for time columns that do not set their own
    hmac_secret_file: str | None = None
    retention_months: dict[str, Annotated[int, Field(ge=1)]]
    recipients: dict[NonEmptyStr, Annotated[list[Email], Field(min_length=1)]] = {}
    email: EmailServer = EmailServer()
    # Notification sinks in order (spec section 8); empty: one email sink from `email`. Each entry
    # is validated by its kind's model (notify/sinks.py).
    notify: list[Annotated[Any, AfterValidator(_sink_config)]] = []
    # Extension packages to load, by entry-point name in the group dre.extensions (spec section 2).
    extensions: list[NonEmptyStr] = []
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
    rule_weekly_day: Weekday | None = None  # weekly rules' day; null: the dataset's full_sweep_day
    # The watchdog (spec section 8) expects the hourly run's dq_run rows this long after the hour.
    watchdog_grace_minutes: int = Field(default=30, ge=1, le=59)
    # How far back a run looks for FAILED results whose cause investigation never finished, to retry it.
    investigation_retry_days: int = Field(default=7, ge=1)
    # How far back (run_date) the hourly reads of the dq store look: the window history, the rule
    # schedule and the digest's earlier results. Bounds every hourly scan of dq_check_result to
    # recent partitions; a dataset with no NORMAL result inside it windows as on a first run.
    history_lookback_days: int = Field(default=400, ge=1)
    watchdog_owner: NonEmptyStr | None = None  # whose recipients get watchdog alerts; null: every recipient
    # Compute budget (spec section 8): each check gets its dataset's compute_budget_minutes
    # ("check"), or the dataset's checks in a run share it ("dataset").
    compute_budget_scope: Literal["check", "dataset"] = "check"
    # With a throughput figure, a check whose table's estimated scan would take longer than its
    # budget is refused before running (DID_NOT_RUN / budget_exceeded); null: never refused up front.
    scan_mb_per_minute: float | None = Field(default=None, gt=0)

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
        longest_hold = (self.max_window_hours or 0) / 24
        if self.history_lookback_days < max(longest_hold, self.investigation_retry_days):
            raise invalid(
                f"history_lookback_days ({self.history_lookback_days}) is shorter than max_window_hours "
                f"({self.max_window_hours} hours) or investigation_retry_days ({self.investigation_retry_days})",
                "the lookback must cover the longest hold and the investigation retry span; raise it",
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
    calendar: Annotated[str, AfterValidator(_calendar)] = "EVERYDAY"  # EVERYDAY | WEEKDAYS | a conf/calendars name

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
    # Parameters of parametrised checks, by check id, for every dataset of the feed (a dataset's
    # own check_params override these field by field). Validated against each check's params model.
    check_params: dict[Identifier, dict[str, Any]] = {}

    @model_validator(mode="after")
    def _check(self) -> Feed:
        from hcsc.datalake.dre.checks.patterns import is_file_pattern

        try:
            file_kind = is_file_pattern(self.pattern)
        except ValueError:  # a malformed conf pattern: dre validate reports the file
            return self
        if file_kind and self.landing is None:
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
    """How the merge chooses among upstream versions tied at the latest record time. Terms are
    upstream columns. A column that is the upstream's record_time or load_time is read with that
    column's format and time zone; `formats` gives the Spark datetime pattern of any other time
    column written as a string, so it is ordered as a time, never as a string."""

    order_by: Annotated[list[OrderTerm], Field(min_length=1)]
    formats: dict[Identifier, NonEmptyStr] = {}


class Join(Model):
    """One joined dataset of a derived dataset. `match` maps a column of `from` to a column of
    the joined dataset (not `on`: YAML reads an unquoted `on` as a boolean). type left: unmatched
    rows stay (a dropped row downstream is MISSING, and JOIN_UNMATCHED can explain it); inner:
    unmatched rows are not expected downstream."""

    dataset: Identifier
    match: Annotated[dict[Identifier, Identifier], Field(min_length=1)]
    type: Literal["left", "inner"] = "left"


class Derived(Model):
    """A derived dataset's query, built by the engine (spec section 3, release 2): the rows of
    `from` (after its feed_filter), joined to each of `joins` (after theirs), filtered by `where`,
    grouped by `group_by` (output name -> expression), with `columns` (output name -> aggregate
    expression). The engine adds record_ts and load_ts: the latest record and load time of the
    `from` rows in each group, in UTC. Tables are referenced by their dataset id in expressions.
    Expressions are SQL fragments, validated and parsed like feed_filter."""

    from_: Identifier = Field(alias="from")
    joins: list[Join] = []
    where: SqlFragment | None = None
    group_by: Annotated[dict[Identifier, SqlFragment], Field(min_length=1)]
    columns: dict[Identifier, SqlFragment] = {}

    @property
    def sources(self) -> list[str]:
        return [self.from_, *(j.dataset for j in self.joins)]


DERIVED_RECORD = TimeColumn(column="record_ts", timezone="UTC")
DERIVED_LOAD = TimeColumn(column="load_ts", granularity="minute", timezone="UTC")


class Dataset(SettingsOverride):
    dataset: Identifier
    table: TableName | None = None    # a real table; or
    derived: Derived | None = None    # a query over other datasets (release 2): an upstream only
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
    # Business-rule exclusions per hop: upstream id -> SQL condition on the upstream's rows that
    # this dataset is not expected to receive (a legitimate filter, not data loss). Hop checks
    # leave those rows out of what they expect.
    hop_exclusions: dict[Identifier, SqlFragment] = {}
    winner_rule: WinnerRule | None = None
    # Parameters of parametrised checks (T1_FRESHNESS, T1_COLUMN_RANGE, ...), by check id. A
    # parametrised check runs only where its pattern, feed or dataset configures it.
    check_params: dict[Identifier, dict[str, Any]] = {}

    @model_validator(mode="before")
    @classmethod
    def _derived_times(cls, data: Any) -> Any:
        """A derived dataset's times are the engine's record_ts and load_ts (UTC), unless set."""
        if isinstance(data, dict) and data.get("derived") is not None:
            data = dict(data)
            data.setdefault("record_time", DERIVED_RECORD.model_dump())
            data.setdefault("load_time", DERIVED_LOAD.model_dump())
            data.setdefault("upstream", [])
        return data

    @model_validator(mode="after")
    def _table_or_derived(self) -> Dataset:
        if (self.table is None) == (self.derived is None):
            raise invalid("a dataset is either a table or derived, not both or neither",
                          "set table: database.table, or derived: {from, group_by, ...}")
        return self


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
