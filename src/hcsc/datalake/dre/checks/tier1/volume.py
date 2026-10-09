"""T1_VOLUME: the window's row count is within volume_tolerance_pct of the median of the
last 14 events for the same cadence slot (spec section 6).

Population: rows loaded in the window. Fewer than 7 earlier events for the slot:
DID_NOT_RUN / insufficient_history, still recording the count so history builds up.
The slot is the latest cadence slot before the window end, recorded in `detail`.
"""

from __future__ import annotations

from datetime import timedelta
from statistics import median
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import (
    DID_NOT_RUN, FAILED, PASSED, REASONS, Check, CheckContext, CheckResult, did_not_run, needs_load_time,
    render_sql, utc_literal,
)
from hcsc.datalake.dre.checks.cadence import UnsupportedCalendar, slot_label, slots
from hcsc.datalake.dre.checks.tier1.zero_rows import rows_in_window

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset

HISTORY_EVENTS = 14
MIN_HISTORY = 7


class Volume(Check):
    check_id = "T1_VOLUME"

    def required_columns(self, dataset: Dataset) -> list[str]:
        return [dataset.load_time.column] if dataset.load_time else []

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        missing = needs_load_time(ctx)
        if missing:
            return [missing]
        if ctx.feed is None or ctx.dq_database is None:
            return [did_not_run("invalid_config", detail="T1_VOLUME needs a feed cadence and the dq store")]
        try:
            recent = slots(ctx.feed.cadence, event.window_end - timedelta(days=8), event.window_end)
        except UnsupportedCalendar as exc:
            return [did_not_run("invalid_config", detail=str(exc))]
        slot_detail = f"slot={slot_label(ctx.feed.cadence, recent[-1])}" if recent else "slot=none"
        count = rows_in_window(ctx, event)
        if count == 0:
            return [did_not_run("empty_population", detail=slot_detail)]
        history = [r.row_count for r in ctx.spark.sql(render_sql(
            "t1_volume_history.sql.j2", dq_database=ctx.dq_database, dataset=ctx.dataset.dataset,
            slot_detail=slot_detail, before=utc_literal(event.window_start), limit=HISTORY_EVENTS,
        )).collect()]
        observed = f"{count} rows"
        if len(history) < MIN_HISTORY:
            return [CheckResult(DID_NOT_RUN, population=count, observed=observed,
                                reason_category=REASONS["insufficient_history"], reason_code="insufficient_history",
                                detail=slot_detail)]
        mid = median(history)
        tolerance = ctx.settings.volume_tolerance_pct / 100
        low, high = mid * (1 - tolerance), mid * (1 + tolerance)
        outside = not (low <= count <= high)
        return [CheckResult(FAILED if outside else PASSED, population=count, violations=1 if outside else 0,
                            observed=observed,
                            expected=f"{low:g} to {high:g} rows (median {mid:g} of the last {len(history)} events)",
                            detail=slot_detail)]
