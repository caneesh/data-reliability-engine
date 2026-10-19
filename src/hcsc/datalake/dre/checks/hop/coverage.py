"""Where a hop's judged range starts: from the coverage its earlier results committed (checkpoint 3, L1).

Every hop check (HOP_FILE_COMPLETENESS, HOP_KEY_CURRENCY, HOP_VALUE_AGREEMENT, HOP_LEDGER)
judges the upstream records whose deadline (load time + sla_hours) falls in a range. A covering
result (PASSED, FAILED, or DID_NOT_RUN / empty_population) records the range it judged in
dq_check_result (coverage_from, coverage_to; the runner writes it, runner.result_rows); any other
outcome covers nothing. The range of a window starts where the hop's latest committed covering
NORMAL result of an earlier window ended, and ends at the window end minus sla_hours. So:

- a shorter sla_hours never leaves a gap, and a longer one never judges a record twice (the range
  is empty until the new deadline passes what was covered);
- a window whose result covered nothing (an error, missing or insufficient evidence) is judged by
  the next window that covers;
- a re-evaluation of a held window starts where the first attempt did, since only results of
  earlier windows (window_end at or before this window's start) count, never the attempt itself.

Without committed coverage the range starts at the start of the hop's first evaluated window in
the store, minus sla_hours; on the hop's first evaluation, at this window's start minus sla_hours.
Reads are bounded by history_since (the run_date lookback); coverage older than it is not seen, and
the weekly full sweep judges every record past its deadline in any case. INVALID results cover
nothing (their judgement is not trusted), but still count as evaluations of the hop, so the range
falls back to their start rather than to this window's.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import utc_literal
from hcsc.datalake.dre.checks.times import as_utc

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.base import CheckContext
    from hcsc.datalake.dre.checks.events import Event


def judged_range(ctx: CheckContext, event: Event, check_id: str, up_id: str) -> tuple[datetime, datetime]:
    """(start, end) of the deadline range this hop judges in this window, in UTC. start can be at
    or after end (nothing is due yet after a longer sla_hours)."""
    sla = timedelta(hours=ctx.settings.sla_hours)
    start, end = event.window_start - sla, event.window_end - sla
    if ctx.dq_database is None or not ctx.spark.catalog.tableExists(f"{ctx.dq_database}.dq_check_result"):
        return start, end
    since = f" AND run_date >= DATE'{ctx.history_since.isoformat()}'" if ctx.history_since is not None else ""
    row = ctx.spark.sql(
        f"SELECT MAX(CASE WHEN evidence_state <> 'INVALID' THEN coverage_to END) AS covered_to, "
        f"MIN(window_start) AS first_start "
        f"FROM {ctx.dq_database}.v_committed_result "
        f"WHERE dataset = '{ctx.dataset.dataset}' AND check_id = '{check_id}' AND group_values['upstream'] = '{up_id}' "
        f"AND execution_type = 'NORMAL' AND window_end <= {utc_literal(event.window_start)}{since}").collect()[0]
    if row.covered_to is not None:
        return as_utc(row.covered_to), end
    if row.first_start is not None:
        return min(as_utc(row.first_start) - sla, start), end
    return start, end


def coverage_of(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    """What a covering result records: from the range's start to its end, never moving back."""
    return start, max(start, end)
