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


def _column_or_mapping(value: Any) -> Any:
    return {"column": value} if isinstance(value, str) else value


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
CadenceKind = Literal["times", "interval", "calendar_dates"]
Layer = Literal["RAW", "CURATED", "CDC", "GOLD"]
Normaliser = Literal["strip_leading_zeros"]
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


SETTING_FIELDS: tuple[str, ...] = tuple(SettingsOverride.model_fields)


class Settings(Model):
    """Settings after resolving defaults, then feed, then dataset. All required."""

    sla_hours: float = Field(gt=0)
    email_sample_keys: bool
    min_rows_per_load: int = Field(ge=0)
    volume_tolerance_pct: float = Field(gt=0)
    compute_budget_minutes: float = Field(gt=0)
    full_sweep_day: Weekday


# --- defaults.yaml ---


class Defaults(SettingsOverride):
    dq_database: Annotated[str, AfterValidator(_dq_database)]
    environment: NonEmptyStr
    hmac_secret_file: str | None = None
    retention_months: dict[str, Annotated[int, Field(ge=1)]]
    recipients: dict[NonEmptyStr, Annotated[list[Email], Field(min_length=1)]] = {}
    # Spec defaults (sections 6 and 8, constraint 6).
    email_sample_keys: bool | None = False
    volume_tolerance_pct: float | None = Field(default=50, gt=0)
    full_sweep_day: Weekday | None = "SUNDAY"

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
    timezone: TimeZone
    calendar: NonEmptyStr = "EVERYDAY"

    @model_validator(mode="after")
    def _check(self) -> Cadence:
        needs = {"times": "times", "interval": "interval_minutes", "calendar_dates": "dates"}[self.kind]
        if not getattr(self, needs):
            raise invalid(f"cadence kind {self.kind} needs `{needs}`", f"add `{needs}` under cadence")
        return self


class FilterRule(Model):
    id: Identifier
    condition: SqlFragment
    code_ref: NonEmptyStr
    owner: NonEmptyStr
    expected_daily_volume: int = Field(ge=0)


class CauseInputs(Model):
    """Optional inputs for cause checks; null means NOT_READY (spec section 7)."""

    stopper_file: str | None = None
    partition_handoff_file: str | None = None
    job_log_path: str | None = None
    rejects_table: TableName | None = None
    filter_rules: list[FilterRule] | None = None


class Feed(SettingsOverride):
    feed: Identifier
    expectation_version: int = Field(ge=1)
    pattern: Pattern
    owner: NonEmptyStr
    landing: Landing | None = None
    cadence: Cadence
    datasets: Annotated[list[Identifier], Field(min_length=1)]
    cause_inputs: CauseInputs = CauseInputs()

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
    column: Identifier
    format: NonEmptyStr | None = None
    granularity: Granularity | None = None


class WinnerRule(Model):
    order_by: Annotated[list[OrderTerm], Field(min_length=1)]


class Dataset(SettingsOverride):
    dataset: Identifier
    table: TableName
    layer: Layer
    upstream: list[Identifier] = []
    feed_filter: SqlFragment | None = None
    key: Annotated[list[Identifier], Field(min_length=1)]
    key_normalise: dict[Identifier, Normaliser] = {}
    record_time: Annotated[TimeColumn | None, BeforeValidator(_column_or_mapping)] = None
    load_time: Annotated[TimeColumn | None, BeforeValidator(_column_or_mapping)] = None
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


class MaxRowsPerKey(Model):
    max: int = Field(ge=0)


class MaxOpenRowsPerKey(Model):
    open_when: SqlFragment
    max: int = Field(ge=0)


class ColumnOrder(Model):
    lower: Identifier
    upper: Identifier
    nulls_fail: bool = False


class SupersededStillOpen(Model):
    group_key: Annotated[list[Identifier], Field(min_length=1), BeforeValidator(_str_to_list)]
    order_column: Identifier
    open_when: SqlFragment


class ChildWithinParent(Model):
    parent_dataset: Identifier
    join: Annotated[dict[Identifier, Identifier], Field(min_length=1)]  # child column: parent column
    child_start: Identifier
    parent_start: Identifier
    parent_end: Identifier


class ValueFormat(Model):
    column: Identifier
    pattern: NonEmptyStr


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
