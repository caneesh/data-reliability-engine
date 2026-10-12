"""T1_VOLUME: each slot's load is within volume_tolerance_pct of the median of the last 14
loads for the same slot (spec section 6).

One result per slot (group: `slot` = local HH:MM, `slot_time` = the slot's local date and
time). A slot's load is the rows loaded in [slot, next slot); the slot is judged once, in the
window where that period ends, so a load is never split across windows. Population: the
slot's rows. Slots with no rows give no result (T1_ZERO_ROWS reports them); no slot with
rows: one DID_NOT_RUN / empty_population. Fewer than 7 earlier loads for the slot:
DID_NOT_RUN / insufficient_history, still recording the count so the history builds up. A load
judged again (its window was held) counts once in the history. The median and the band are
computed in SQL (t1_volume.sql.j2).
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from hcsc.datalake.dre.checks.base import (
    DID_NOT_RUN, FAILED, PASSED, REASONS, Check, CheckContext, CheckResult, did_not_run, double,
    needs_load_time, render_sql, utc_literal,
)
from hcsc.datalake.dre.checks.cadence import UnsupportedCalendar, previous_slot, slot_label, slots
from hcsc.datalake.dre.checks.registry import register_check
from hcsc.datalake.dre.checks.times import utc_expr

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
        cadence = ctx.feed.cadence
        try:
            # Periods ending in (window_start, window_end]: the slots up to the window end, plus the
            # slot before the window (however far back: a monthly feed's is weeks earlier).
            ends = slots(cadence, event.window_start, event.window_end + timedelta(microseconds=1))
            before = previous_slot(cadence, event.window_start)
        except UnsupportedCalendar as exc:
            return [did_not_run("invalid_config", detail=str(exc))]
        around = ([before] if before is not None else []) + ends
        zone = ZoneInfo(cadence.timezone)
        periods = [
            {"label": slot_label(cadence, start), "slot_time": start.astimezone(zone).strftime("%Y-%m-%d %H:%M"),
             "start": utc_literal(start), "end": utc_literal(end)}
            for start, end in zip(around, around[1:])
            if event.window_start < end <= event.window_end
        ]
        if not periods:
            return [did_not_run("empty_population", detail="no slot period ends in the window")]
        ds = ctx.dataset
        tolerance = ctx.settings.volume_tolerance_pct / 100
        rows = ctx.spark.sql(render_sql(
            "t1_volume.sql.j2", periods=periods, load_expr=utc_expr(ds.load_time, ctx.default_timezone),
            table=ds.table, feed_filter=ds.feed_filter, dq_database=ctx.dq_database, dataset=ds.dataset,
            before=utc_literal(event.window_start), limit=HISTORY_EVENTS, min_history=MIN_HISTORY,
            low_factor=double(1 - tolerance), high_factor=double(1 + tolerance),
        )).collect()
        if not rows:
            return [did_not_run("empty_population", detail="no rows loaded for the slots judged in the window")]
        return [self._result(r) for r in rows]

    @staticmethod
    def _result(row: Any) -> CheckResult:
        groups = {"slot": row.g_1, "slot_time": row.g_2}
        observed = f"{row.row_count} rows"
        if row.outside is None:
            return CheckResult(DID_NOT_RUN, population=row.row_count, observed=observed, group_values=groups,
                               reason_category=REASONS["insufficient_history"], reason_code="insufficient_history")
        return CheckResult(FAILED if row.outside else PASSED, population=row.row_count, violations=row.outside,
                           observed=observed, group_values=groups,
                           expected=f"{row.low:g} to {row.high:g} rows (median {row.median_rows:g} "
                                    f"of the last {row.loads} loads)")


register_check(Volume())
