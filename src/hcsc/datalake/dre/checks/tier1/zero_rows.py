"""T1_ZERO_ROWS: a load due in the window wrote at least min_rows_per_load rows (spec section 6).

Population: cadence slots (loads due) in the window. With a load due and fewer rows
than min_rows_per_load loaded in the window: FAILED. No load due: DID_NOT_RUN /
empty_population. An empty table with a load due is FAILED, never PASSED.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import (
    Check, CheckContext, CheckResult, did_not_run, from_counts, needs_load_time, render_sql, utc_literal,
)
from hcsc.datalake.dre.checks.cadence import UnsupportedCalendar, slots
from hcsc.datalake.dre.checks.times import utc_expr

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset


def rows_in_window(ctx: CheckContext, event: Event) -> int:
    ds = ctx.dataset
    sql = render_sql(
        "rows_in_window.sql.j2",
        table=ds.table,
        load_expr=utc_expr(ds.load_time, ctx.default_timezone),
        window_start=utc_literal(event.window_start),
        window_end=utc_literal(event.window_end),
        feed_filter=ds.feed_filter,
    )
    return ctx.spark.sql(sql).collect()[0].row_count


class ZeroRows(Check):
    check_id = "T1_ZERO_ROWS"

    def required_columns(self, dataset: Dataset) -> list[str]:
        return [dataset.load_time.column] if dataset.load_time else []

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        missing = needs_load_time(ctx)
        if missing:
            return [missing]
        if ctx.feed is None:
            return [did_not_run("invalid_config", detail="T1_ZERO_ROWS needs a feed cadence")]
        try:
            due = slots(ctx.feed.cadence, event.window_start, event.window_end)
        except UnsupportedCalendar as exc:
            return [did_not_run("invalid_config", detail=str(exc))]
        if not due:
            return [did_not_run("empty_population", detail="no load due in the window")]
        loaded = rows_in_window(ctx, event)
        minimum = ctx.settings.min_rows_per_load
        return [from_counts(len(due), 1 if loaded < minimum else 0,
                            observed=f"{loaded} rows loaded in the window ({len(due)} loads due)",
                            expected=f"at least {minimum} rows")]
