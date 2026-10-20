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
Reads are bounded. The hourly read looks back to history_since (history_lookback_days). When it
finds no trusted coverage (the hop was idle for longer, or its only results inside the lookback are
INVALID), a second read looks at the history before it, back to coverage_since (the retention of
dq_check_result), so a hop that was idle for longer than the lookback continues where it last
covered and the idle span is judged (final review, P1-02). Only when nothing is retained either does
the range restart at this window, and the result's basis says so: an unresolved coverage gap, judged
only by the weekly full sweep. INVALID results cover nothing (their judgement is not trusted), but
still count as evaluations of the hop, so the range falls back to their start rather than to this
window's.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import utc_literal
from hcsc.datalake.dre.checks.times import as_utc

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.base import CheckContext
    from hcsc.datalake.dre.checks.events import Event


@dataclass(frozen=True)
class Judged:
    start: datetime
    end: datetime
    basis: str  # where start comes from, for the scope


def _read(ctx: CheckContext, event: Event, check_id: str, up_id: str, group: str,
          since: date | None, before: date | None) -> tuple[datetime | None, datetime | None, date | None]:
    """(the latest trusted coverage_to, the first window_start, the run_date of that coverage) among
    the hop's committed NORMAL results of earlier windows, with run_date in [since, before)."""
    bounds = (f" AND run_date >= DATE'{since.isoformat()}'" if since is not None else "") + (
        f" AND run_date < DATE'{before.isoformat()}'" if before is not None else "")
    row = ctx.spark.sql(
        f"SELECT MAX(CASE WHEN evidence_state <> 'INVALID' THEN coverage_to END) AS covered_to, "
        f"MAX_BY(run_date, CASE WHEN evidence_state <> 'INVALID' THEN coverage_to END) AS covered_on, "
        f"MIN(window_start) AS first_start "
        f"FROM {ctx.dq_database}.v_committed_result "
        f"WHERE dataset = '{ctx.dataset.dataset}' AND check_id = '{check_id}' AND group_values['{group}'] = '{up_id}' "
        f"AND execution_type = 'NORMAL' AND window_end <= {utc_literal(event.window_start)}{bounds}").collect()[0]
    covered = as_utc(row.covered_to) if row.covered_to is not None else None
    first = as_utc(row.first_start) if row.first_start is not None else None
    return covered, first, row.covered_on


def judged(ctx: CheckContext, event: Event, check_id: str, up_id: str, group: str = "upstream",
           sla_hours: float | None = None) -> Judged:
    """The deadline range this hop judges in this window, in UTC, and its basis. start can be at or
    after end (nothing is due yet after a longer sla_hours). `group` is the group_values key the
    results are grouped by and `up_id` its value; `sla_hours` replaces the dataset's (CHAIN_COMPLETENESS:
    group `chain`, `<source>-><terminal>`, and the sum of the edge SLAs along the path)."""
    sla = timedelta(hours=ctx.settings.sla_hours if sla_hours is None else sla_hours)
    start, end = event.window_start - sla, event.window_end - sla
    if ctx.dq_database is None or not ctx.spark.catalog.tableExists(f"{ctx.dq_database}.dq_check_result"):
        return Judged(start, end, "no dq store")
    covered, first, _ = _read(ctx, event, check_id, up_id, group, ctx.history_since, None)
    if covered is not None:
        return Judged(covered, end, "committed coverage")
    if ctx.history_since is not None and (ctx.coverage_since is None or ctx.coverage_since < ctx.history_since):
        older, older_first, on = _read(ctx, event, check_id, up_id, group, ctx.coverage_since, ctx.history_since)
        if older is not None:  # trusted coverage before the lookback: continue from it (never later than an
            # untrusted result's start inside the lookback, which may have judged nothing reliable)
            begin = older if first is None else min(older, first - sla)
            return Judged(begin, end, f"committed coverage before the lookback, from {on.isoformat()}")
        first = first or older_first
    if first is not None:
        return Judged(min(first - sla, start), end, "the hop's first evaluated window")
    if ctx.history_since is None:
        return Judged(start, end, "first evaluation")
    retained = ctx.coverage_since.isoformat() if ctx.coverage_since is not None else "the store began"
    return Judged(start, end, f"unresolved coverage gap: no committed coverage retained since {retained} "
                              "(a first evaluation, or coverage older than the retention); records before the "
                              "range are judged only by the full sweep")


def judged_range(ctx: CheckContext, event: Event, check_id: str, up_id: str, group: str = "upstream",
                 sla_hours: float | None = None) -> tuple[datetime, datetime]:
    """(start, end) of judged(): the deadline range this hop judges in this window."""
    j = judged(ctx, event, check_id, up_id, group, sla_hours)
    return j.start, j.end


def coverage_of(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    """What a covering result records: from the range's start to its end, never moving back."""
    return start, max(start, end)
