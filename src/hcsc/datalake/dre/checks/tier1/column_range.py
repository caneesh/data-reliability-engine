"""T1_COLUMN_RANGE: values of a column loaded in the window are within bounds or in an allowed set
(spec section 6). A parametrised check: it runs where `check_params` configures it.

Population: rows loaded in the window with a non-null value (every row when nulls_fail), per
group. `min` and `max` are numbers, compared numerically; `allowed` is a set of strings. Null
values are left out unless nulls_fail, when each is a violation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import model_validator

from hcsc.datalake.dre.checks.base import (
    Check, CheckContext, CheckResult, double, from_counts, group_values, needs_load_time, render_sql, utc_literal,
)
from hcsc.datalake.dre.checks.registry import register_check
from hcsc.datalake.dre.checks.times import sql_string, utc_expr
from hcsc.datalake.dre.config.models import Identifier, Model, invalid

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset


class ColumnRangeParams(Model):
    column: Identifier
    min: float | None = None
    max: float | None = None
    allowed: list[str] | None = None
    nulls_fail: bool = False

    @model_validator(mode="after")
    def _bounded(self) -> ColumnRangeParams:
        if self.min is None and self.max is None and self.allowed is None:
            raise invalid("needs a bound", "set min, max or allowed")
        if self.min is not None and self.max is not None and self.min > self.max:
            raise invalid("min is above max", "swap them")
        if self.allowed is not None and not self.allowed:
            raise invalid("allowed is empty", "list the allowed values, or remove it")
        return self


class ColumnRange(Check):
    check_id = "T1_COLUMN_RANGE"
    params_model = ColumnRangeParams

    def required_columns(self, dataset: Dataset, params: Any = None) -> list[str]:
        load = [dataset.load_time.column] if dataset.load_time else []
        return [*dataset.group_by, *load, *([params.column] if params else [])]

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        missing = needs_load_time(ctx)
        if missing:
            return [missing]
        ds, p = ctx.dataset, ctx.params
        sql = render_sql(
            "t1_column_range.sql.j2", table=ds.table, group_by=ds.group_by, feed_filter=ds.feed_filter,
            column=p.column, nulls_fail=p.nulls_fail,
            min=None if p.min is None else double(p.min), max=None if p.max is None else double(p.max),
            allowed=None if p.allowed is None else [sql_string(v) for v in p.allowed],
            load_expr=utc_expr(ds.load_time, ctx.default_timezone),
            window_start=utc_literal(event.window_start), window_end=utc_literal(event.window_end),
        )
        bounds = [b for b in (f"at least {p.min:g}" if p.min is not None else None,
                              f"at most {p.max:g}" if p.max is not None else None,
                              f"in {p.allowed}" if p.allowed is not None else None) if b]
        return [
            from_counts(row.population, row.violations,
                        observed=f"{row.violations or 0} of {row.population} values of {p.column} are out of range",
                        expected=f"every {p.column} {', '.join(bounds)}" + (", none null" if p.nulls_fail else ""),
                        group_values=group_values(row, ds.group_by))
            for row in ctx.spark.sql(sql).collect()
        ]


register_check(ColumnRange())
