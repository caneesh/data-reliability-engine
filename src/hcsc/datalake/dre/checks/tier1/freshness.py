"""T1_FRESHNESS: the dataset's latest record (or load) time is no older than `max_age_hours` at
the window end (spec section 6). A parametrised check: it runs where `check_params` configures it.

Population: rows after feed_filter whose time is before the window end (per group), plus rows with
no usable time; violations: 1 when the latest time is too old, or when no row has a usable time.
Rows timed at or after the window end are left out and the age is measured against the window
end, not the clock, so a RERUN or REPLAY of a historical window answers as of that window.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field

from hcsc.datalake.dre.checks.base import FAILED, PASSED, Check, CheckContext, CheckResult, did_not_run, group_values, render_sql, utc_literal
from hcsc.datalake.dre.checks.registry import register_check
from hcsc.datalake.dre.checks.times import as_utc, utc_expr
from hcsc.datalake.dre.config.models import Model

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset


class FreshnessParams(Model):
    max_age_hours: float = Field(gt=0)
    time: Literal["record", "load"] = "record"  # which of the dataset's times to measure


def _time_column(dataset: Dataset, params: FreshnessParams | None) -> Any:
    which = params.time if params else "record"
    return dataset.record_time if which == "record" else dataset.load_time


class Freshness(Check):
    check_id = "T1_FRESHNESS"
    params_model = FreshnessParams

    def required_columns(self, dataset: Dataset, params: Any = None) -> list[str]:
        column = _time_column(dataset, params)
        return [*dataset.group_by, *([column.column] if column else [])]

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        ds, p = ctx.dataset, ctx.params
        column = _time_column(ds, p)
        if column is None:
            return [did_not_run("invalid_config", detail=f"{p.time}_time is not set for dataset {ds.dataset}")]
        window_end = as_utc(event.window_end)
        sql = render_sql("t1_freshness.sql.j2", table=ds.table, group_by=ds.group_by, feed_filter=ds.feed_filter,
                         time_expr=utc_expr(column, ctx.default_timezone), window_end=utc_literal(window_end))
        results = []
        for row in ctx.spark.sql(sql).collect():
            groups = group_values(row, ds.group_by)
            if not row.population:
                results.append(did_not_run("empty_population", group_values=groups))
                continue
            expected = f"latest {p.time} time within {p.max_age_hours:g} hours of the window end"
            if row.latest is None:
                results.append(CheckResult(FAILED, population=row.population, violations=1, group_values=groups,
                                           observed=f"no usable {p.time} time in {row.population} rows",
                                           expected=expected))
                continue
            age = (window_end - as_utc(row.latest)).total_seconds() / 3600
            stale = age > p.max_age_hours
            results.append(CheckResult(FAILED if stale else PASSED, population=row.population,
                                       violations=1 if stale else 0, group_values=groups,
                                       observed=f"latest {p.time} time {as_utc(row.latest).isoformat()}, "
                                                f"{age:.1f} hours before the window end", expected=expected))
        return results


register_check(Freshness())
