"""T1_VOLUME: each slot's load is within volume_tolerance_pct of the median of the last 14
loads for the same slot (spec section 6).

One result per slot (group: `slot` = local HH:MM, `slot_time` = the slot's local date and
time). A slot's load is the rows loaded in [slot, next slot); the slot is judged once, in the
window where that period ends, so a load is never split across windows. Population: the
slot's rows. Slots with no rows give no result (T1_ZERO_ROWS reports them); no slot with
rows: one DID_NOT_RUN / empty_population. Fewer than 7 earlier loads for the slot:
DID_NOT_RUN / insufficient_history, still recording the count so the history builds up.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from statistics import median
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from hcsc.datalake.dre.checks.base import (
    DID_NOT_RUN, FAILED, PASSED, REASONS, Check, CheckContext, CheckResult, did_not_run, needs_load_time,
    render_sql, utc_literal,
)
from hcsc.datalake.dre.checks.cadence import UnsupportedCalendar, slot_label, slots
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
            around = slots(cadence, event.window_start - timedelta(days=8), event.window_end + timedelta(days=8))
        except UnsupportedCalendar as exc:
            return [did_not_run("invalid_config", detail=str(exc))]
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
        rows = [r for r in ctx.spark.sql(render_sql(
            "t1_volume.sql.j2", periods=periods, load_expr=utc_expr(ds.load_time, ctx.default_timezone),
            table=ds.table, feed_filter=ds.feed_filter,
        )).collect() if r.row_count > 0]
        if not rows:
            return [did_not_run("empty_population", detail="no rows loaded for the slots judged in the window")]
        history: dict[str, list[int]] = defaultdict(list)
        for h in ctx.spark.sql(render_sql(
            "t1_volume_history.sql.j2", dq_database=ctx.dq_database, dataset=ds.dataset,
            labels=sorted({r.g_1 for r in rows}), before=utc_literal(event.window_start), limit=HISTORY_EVENTS,
        )).collect():
            history[h.label].append(h.row_count)
        return [self._judge(r.row_count, {"slot": r.g_1, "slot_time": r.g_2}, history[r.g_1], ctx) for r in rows]

    @staticmethod
    def _judge(count: int, groups: dict[str, str], history: list[int], ctx: CheckContext) -> CheckResult:
        observed = f"{count} rows"
        if len(history) < MIN_HISTORY:
            return CheckResult(DID_NOT_RUN, population=count, observed=observed, group_values=groups,
                               reason_category=REASONS["insufficient_history"], reason_code="insufficient_history")
        mid = median(history)
        tolerance = ctx.settings.volume_tolerance_pct / 100
        low, high = mid * (1 - tolerance), mid * (1 + tolerance)
        outside = not (low <= count <= high)
        return CheckResult(FAILED if outside else PASSED, population=count, violations=1 if outside else 0,
                           observed=observed, group_values=groups,
                           expected=f"{low:g} to {high:g} rows (median {mid:g} of the last {len(history)} loads)")
