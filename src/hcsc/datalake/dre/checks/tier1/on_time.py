"""T1_ON_TIME: every cadence slot due in the window has a load within sla_hours (spec section 6).

Population: slots whose SLA deadline (slot + sla_hours) falls in the window, so each
slot is judged once. No slot due: DID_NOT_RUN / empty_population.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import (
    Check, CheckContext, CheckResult, did_not_run, from_counts, needs_load_time, render_sql, utc_literal,
)
from hcsc.datalake.dre.checks.cadence import UnsupportedCalendar, slots
from hcsc.datalake.dre.checks.times import utc_expr

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset


class OnTime(Check):
    check_id = "T1_ON_TIME"

    def required_columns(self, dataset: Dataset) -> list[str]:
        return [dataset.load_time.column] if dataset.load_time else []

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        missing = needs_load_time(ctx)
        if missing:
            return [missing]
        if ctx.feed is None:
            return [did_not_run("invalid_config", detail="T1_ON_TIME needs a feed cadence")]
        sla = timedelta(hours=ctx.settings.sla_hours)
        try:
            due = slots(ctx.feed.cadence, event.window_start - sla, event.window_end - sla)
        except UnsupportedCalendar as exc:
            return [did_not_run("invalid_config", detail=str(exc))]
        if not due:
            return [did_not_run("empty_population", detail="no load due in the window")]
        ds = ctx.dataset
        sql = render_sql(
            "t1_on_time.sql.j2",
            slots=[utc_literal(s) for s in due],
            load_expr=utc_expr(ds.load_time, ctx.default_timezone),
            table=ds.table,
            feed_filter=ds.feed_filter,
            sla_minutes=int(sla.total_seconds() // 60),
        )
        row = ctx.spark.sql(sql).collect()[0]
        return [from_counts(row.population, row.violations,
                            observed=f"{row.violations or 0} of {row.population} due loads had no load within the SLA",
                            expected=f"a load within {ctx.settings.sla_hours:g} hours of each cadence slot")]
