"""Check interface, results and the denominator rule (spec sections 4 and 6).

A check renders a Jinja2 SQL template from checks/sql/, runs it through
spark.sql and turns the rows into results. `run_check` wraps every check: it
runs the preconditions first and turns any exception into DID_NOT_RUN, so a
check never stops the run.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from jinja2 import Environment, PackageLoader, StrictUndefined

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset, Feed, Settings

PASSED, FAILED, DID_NOT_RUN = "PASSED", "FAILED", "DID_NOT_RUN"

# DID_NOT_RUN reason code -> category (spec section 4).
REASONS: dict[str, str] = {
    "partition_missing": "DATA_UNAVAILABLE",
    "landing_unreadable": "DATA_UNAVAILABLE",
    "empty_population": "DATA_UNAVAILABLE",
    "window_gap": "DATA_UNAVAILABLE",  # a held span given up after max_window_hours: never checked
    "metastore_unavailable": "PLATFORM",
    "hdfs_unavailable": "PLATFORM",
    "query_failed": "PLATFORM",
    "table_missing": "CONFIGURATION",
    "column_missing": "CONFIGURATION",
    "incompatible_type": "CONFIGURATION",
    "invalid_config": "CONFIGURATION",
    "budget_exceeded": "BUDGET",
    "upstream_check_failed": "DEPENDENCY",
    "premise_unconfirmed": "DEPENDENCY",
    "insufficient_history": "BASELINE",
}

# Spark error classes (prefix) -> reason code; anything else is query_failed.
_SPARK_ERRORS = (
    ("UNRESOLVED_COLUMN", "column_missing"),
    ("TABLE_OR_VIEW_NOT_FOUND", "table_missing"),
    ("DATATYPE_MISMATCH", "incompatible_type"),
    ("CAST_INVALID_INPUT", "incompatible_type"),
)


@dataclass(frozen=True)
class CheckResult:
    """One result row: exactly one state, with counts or a reason (spec section 4)."""

    state: str
    population: int | None = None
    violations: int | None = None
    observed: str | None = None
    expected: str | None = None
    reason_category: str | None = None
    reason_code: str | None = None
    group_values: dict[str, str | None] = field(default_factory=dict)
    detail: str | None = None
    # Key events to append to dq_key_event (a DataFrame of key_hash, key_value, event, state_detail);
    # written by the runner after the result, never by the check.
    key_events: Any = field(default=None, compare=False, repr=False)


def did_not_run(code: str, detail: str | None = None, group_values: dict[str, str | None] | None = None) -> CheckResult:
    return CheckResult(DID_NOT_RUN, reason_category=REASONS[code], reason_code=code,
                       group_values=group_values or {}, detail=detail)


def from_counts(
    population: int | None,
    violations: int | None,
    observed: str,
    expected: str,
    group_values: dict[str, str | None] | None = None,
) -> CheckResult:
    """The denominator rule: an empty population is DID_NOT_RUN, never PASSED."""
    groups = group_values or {}
    if not population:
        return did_not_run("empty_population", group_values=groups)
    state = FAILED if violations else PASSED
    return CheckResult(state, population=population, violations=violations or 0,
                       observed=observed, expected=expected, group_values=groups)


@dataclass(frozen=True)
class CheckContext:
    spark: SparkSession
    dataset: Dataset
    feed: Feed | None
    settings: Settings
    default_timezone: str = "UTC"  # defaults.yaml timezone, for time columns without their own
    dq_database: str | None = None  # for checks that read their own history (T1_VOLUME, T1_SCHEMA_DRIFT)
    landing_problem: CheckResult | None = None  # the feed's landing roots could not be listed this run
    upstreams: dict[str, Dataset] = field(default_factory=dict)  # datasets named in this dataset's key_map
    key_secret: bytes | None = None  # HMAC secret for key_hash (from hmac_secret_file); None: no key events
    full_sweep: bool = False  # today is full_sweep_day: hop checks read every key, not only changed ones


class Check(ABC):
    """A check (spec section 6). Subclasses render SQL, run it and build results."""

    check_id: str
    severity: str | None = None  # set for gold rules

    def applies_to(self, dataset: Dataset, pattern: str | None) -> bool:
        return True

    @abstractmethod
    def required_columns(self, dataset: Dataset) -> list[str]:
        """Configured columns the check reads; a missing one makes it DID_NOT_RUN."""

    @abstractmethod
    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        """One result per group (one in total for an ungrouped check)."""


def needs_load_time(ctx: CheckContext) -> CheckResult | None:
    """DID_NOT_RUN / invalid_config when the dataset has no load_time (dre validate warns about this)."""
    if ctx.dataset.load_time is None:
        return did_not_run("invalid_config", detail=f"load_time is not set for dataset {ctx.dataset.dataset}")
    return None


def utc_literal(value) -> str:
    """A UTC TIMESTAMP literal for a window bound or slot (the session runs in UTC)."""
    from hcsc.datalake.dre.checks.times import as_utc

    return "TIMESTAMP '" + as_utc(value).strftime("%Y-%m-%d %H:%M:%S.%f") + "'"


_SQL = Environment(loader=PackageLoader("hcsc.datalake.dre.checks", "sql"), undefined=StrictUndefined,
                   autoescape=False, trim_blocks=True, lstrip_blocks=True)


def render_sql(template: str, **params: Any) -> str:
    return _SQL.get_template(template).render(**params)


def group_values(row: Any, group_by: list[str]) -> dict[str, str | None]:
    """Group columns come back as g_1, g_2, ... in group_by order."""
    return {g: (None if row[f"g_{i}"] is None else str(row[f"g_{i}"])) for i, g in enumerate(group_by, 1)}


def error_detail(exc: BaseException) -> str:
    """Exception type and Spark error class only: messages can carry data values."""
    error_class = _error_class(exc)
    return type(exc).__name__ + (f" {error_class}" if error_class else "")


def from_exception(exc: BaseException) -> CheckResult:
    error_class = _error_class(exc) or ""
    code = next((c for prefix, c in _SPARK_ERRORS if error_class.startswith(prefix)), "query_failed")
    return did_not_run(code, detail=error_detail(exc))


def _error_class(exc: BaseException) -> str | None:
    getter = getattr(exc, "getErrorClass", None)
    try:
        return getter() if getter else None
    except Exception:
        return None


def preconditions(spark: SparkSession, dataset: Dataset, columns: list[str]) -> CheckResult | None:
    """Table exists and every configured column the check reads exists (spec section 3)."""
    try:
        if not spark.catalog.tableExists(dataset.table):
            return did_not_run("table_missing", detail=f"table {dataset.table} not found")
        actual = {c.name.lower() for c in spark.catalog.listColumns(dataset.table)}
    except Exception as exc:
        return did_not_run("metastore_unavailable", detail=error_detail(exc))
    missing = sorted({c for c in columns if c.lower() not in actual})
    if missing:
        return did_not_run("column_missing", detail=f"column(s) {missing} not in {dataset.table}")
    return None


def run_check(check: Check, ctx: CheckContext, event: Event) -> tuple[list[CheckResult], int]:
    """(results, duration_ms). Never raises: a failure is a DID_NOT_RUN result. The check runs
    within its dataset's compute_budget_minutes (checks/budget.py)."""
    from hcsc.datalake.dre.checks.budget import BudgetExceeded, estimated_scan_bytes, within_budget

    started = time.monotonic()

    def work() -> list[CheckResult]:
        failed = preconditions(ctx.spark, ctx.dataset, check.required_columns(ctx.dataset))
        if failed is not None:
            return [failed]
        return check.evaluate(ctx, event) or [did_not_run("empty_population")]

    try:
        results = within_budget(ctx.spark, ctx.settings.compute_budget_minutes, work)
    except BudgetExceeded as exc:
        size = estimated_scan_bytes(ctx.spark, ctx.dataset.table)
        scan = f"; estimated scan {size} bytes" if size is not None else "; no table statistics for a scan estimate"
        results = [did_not_run("budget_exceeded", detail=f"{exc}{scan}")]
    except Exception as exc:
        results = [from_exception(exc)]
    return results, int((time.monotonic() - started) * 1000)
