"""T1_KEY_NULLS: rows in the event window with a null or empty key column, per group (spec section 6).

Population: rows loaded in the window (per group).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import (
    Check, CheckContext, CheckResult, from_counts, group_values, needs_load_time, render_sql, utc_literal,
)
from hcsc.datalake.dre.checks.times import utc_expr

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset


class KeyNulls(Check):
    check_id = "T1_KEY_NULLS"

    def required_columns(self, dataset: Dataset) -> list[str]:
        load = [dataset.load_time.column] if dataset.load_time else []
        return [*dataset.key, *dataset.group_by, *load]

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        missing = needs_load_time(ctx)
        if missing:
            return [missing]
        ds = ctx.dataset
        sql = render_sql(
            "t1_key_nulls.sql.j2",
            table=ds.table,
            key=ds.key,
            group_by=ds.group_by,
            load_expr=utc_expr(ds.load_time, ctx.default_timezone),
            window_start=utc_literal(event.window_start),
            window_end=utc_literal(event.window_end),
            feed_filter=ds.feed_filter,
        )
        return [
            from_counts(row.population, row.violations,
                        observed=f"{row.violations or 0} of {row.population} rows have a null or empty key column",
                        expected="0 rows with a null or empty key column",
                        group_values=group_values(row, ds.group_by))
            for row in ctx.spark.sql(sql).collect()
        ]
