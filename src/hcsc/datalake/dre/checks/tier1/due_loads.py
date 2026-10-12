"""Shared by T1_ON_TIME and T1_ZERO_ROWS: judge each cadence slot whose SLA deadline falls
in the window (spec section 6).

A slot is judged once, in the window where slot + sla_hours falls, never earlier: a load
due at 08:00 with an 8-hour SLA is not judged by a run at 09:00. Rows loaded in [slot,
slot + SLA) count for the slot. No slot due: DID_NOT_RUN / empty_population. A FAILED
result lists the short slots (UTC, up to 100) in `detail`, for the cause engine.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import (
    CheckContext, CheckResult, did_not_run, from_counts, needs_load_time, render_sql, utc_literal,
)
from hcsc.datalake.dre.checks.cadence import UnsupportedCalendar, slots
from hcsc.datalake.dre.checks.times import as_utc, utc_expr

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event

LISTED = 100


def judge_due_slots(ctx: CheckContext, event: Event, check_id: str, minimum: int,
                    observed: str, expected: str) -> list[CheckResult]:
    """observed/expected are formats taking {violations} and {population}."""
    missing = needs_load_time(ctx)
    if missing:
        return [missing]
    if ctx.feed is None:
        return [did_not_run("invalid_config", detail=f"{check_id} needs a feed cadence")]
    sla = timedelta(hours=ctx.settings.sla_hours)
    try:
        due = slots(ctx.feed.cadence, event.window_start - sla, event.window_end - sla)
    except UnsupportedCalendar as exc:
        return [did_not_run("invalid_config", detail=str(exc))]
    if not due:
        return [did_not_run("empty_population", detail="no load due in the window")]
    ds = ctx.dataset
    sql = render_sql(
        "t1_due_loads.sql.j2",
        slots=[utc_literal(s) for s in due],
        load_expr=utc_expr(ds.load_time, ctx.default_timezone),
        table=ds.table,
        feed_filter=ds.feed_filter,
        sla_minutes=int(sla.total_seconds() // 60),
        minimum=minimum,
    )
    row = ctx.spark.sql(sql).collect()[0]
    counts = {"violations": row.violations or 0, "population": row.population}
    result = from_counts(row.population, row.violations, observed=observed.format(**counts), expected=expected)
    if result.state == "FAILED":
        short = [as_utc(s).isoformat() for s in (row.short_slots or [])][:LISTED]
        result = replace(result, detail=json.dumps({"slots": short}))
    return [result]
