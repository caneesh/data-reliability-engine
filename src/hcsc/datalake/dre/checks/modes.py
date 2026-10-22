"""Evaluation modes (ADR-05; reconciliation model section 8; build step 4).

- NORMAL: the scheduled run; reads the data as it is at the run, and is DRE's historical record.
- CURRENT_RECHECK: a RERUN or REPLAY of a past window; reads the data as it is now. It says "this
  window, as the data stands now", never what was true then.
- AS_OF_REPLAY: a past window read as the data stood at its cutoff C (the window end). Allowed only
  when it can be proven (`as_of_refusal`); otherwise the check is DID_NOT_RUN / no_as_of_evidence.

The proof, for every dataset the evaluation reads (the dataset and its upstreams):

1. it declares `grain.history: append_only` (a mutable table has lost what it held: rows merged over
   are gone; `snapshot_partitioned` needs a snapshot reader that is not built, so it is refused too);
2. it has a load time, so its rows loaded after C can be left out;
3. T1_PARTITION_DRIFT shows its table unchanged since C: a committed NORMAL evaluation that recorded
   the table's partitions at or before C (the baseline), and every committed NORMAL evaluation since
   C PASSED with evidence that is not INVALID, at least one of them. A table T1_PARTITION_DRIFT does
   not watch has no such evidence.

A hop whose contract reads a rejects table is refused as well: the rejects table declares no history,
so what it held at C cannot be shown. The proof covers C to the latest drift evaluation after it; the
result's scope says so ("drift checked through ...").
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import did_not_run, utc_literal
from hcsc.datalake.dre.checks.times import as_utc

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult
    from hcsc.datalake.dre.config.models import Dataset

NORMAL, CURRENT_RECHECK, AS_OF_REPLAY = "NORMAL", "CURRENT_RECHECK", "AS_OF_REPLAY"
DRIFT = "T1_PARTITION_DRIFT"


def mode_of(execution_type: str) -> str:
    """The evaluation mode an execution type evaluates in."""
    if execution_type == "NORMAL":
        return NORMAL
    if execution_type == AS_OF_REPLAY:
        return AS_OF_REPLAY
    return CURRENT_RECHECK  # RERUN, REPLAY


def read_datasets(ctx: CheckContext) -> list[Dataset]:
    """The datasets an evaluation reads that hold rows: the dataset and its upstreams (a derived
    upstream is a query over its base datasets, which are among the upstreams)."""
    return [d for d in (ctx.dataset, *ctx.upstreams.values()) if d.table is not None]


def drift_proof(ctx: CheckContext, table: str, cutoff: datetime) -> tuple[str | None, datetime | None]:
    """(why the table cannot be shown unchanged since the cutoff, or None; the latest drift evaluation
    after the cutoff), from the committed T1_PARTITION_DRIFT results of the datasets whose partition
    states name the table."""
    db = ctx.dq_database
    if db is None:
        return "no dq store to read T1_PARTITION_DRIFT evidence from", None
    since = f" AND r.run_date >= DATE'{ctx.history_since.isoformat()}'" if ctx.history_since is not None else ""
    c = utc_literal(cutoff)
    row = ctx.spark.sql(
        f"SELECT "
        f"  MAX(CASE WHEN r.evaluated_at <= {c} AND r.evidence_state <> 'INVALID' AND (r.state IN ('PASSED', 'FAILED') "
        f"      OR r.reason_code = 'insufficient_history') THEN 1 ELSE 0 END) AS baseline, "
        f"  SUM(CASE WHEN r.evaluated_at > {c} AND r.state = 'PASSED' AND r.evidence_state <> 'INVALID' "
        f"      THEN 1 ELSE 0 END) AS passed, "
        f"  SUM(CASE WHEN r.evaluated_at > {c} AND (r.state = 'FAILED' OR r.evidence_state = 'INVALID' "
        f"      OR r.reason_code = 'evidence_invalid') THEN 1 ELSE 0 END) AS against, "
        f"  MAX(CASE WHEN r.evaluated_at > {c} AND r.state = 'PASSED' THEN r.evaluated_at END) AS through "
        f"FROM {db}.v_committed_result r "
        f"WHERE r.check_id = '{DRIFT}' AND r.execution_type = 'NORMAL'{since} "
        f"AND r.dataset IN (SELECT DISTINCT dataset FROM {db}.v_committed_partition_state "
        f"                  WHERE table_name = '{table}')").collect()[0]
    if not row.baseline:
        return f"{table}: no {DRIFT} evaluation recorded its partitions at or before the cutoff", None
    if row.against:
        return f"{table}: {DRIFT} found the table changed (or its evidence invalid) since the cutoff", None
    if not row.passed:
        return f"{table}: no {DRIFT} evaluation since the cutoff shows it unchanged", None
    return None, as_utc(row.through)


def as_of_refusal(ctx: CheckContext, check: Check) -> tuple[CheckResult | None, str | None]:
    """(DID_NOT_RUN / no_as_of_evidence naming why, None) when the evaluation cannot read its data as
    it stood at ctx.as_of; otherwise (None, what the reading rests on)."""
    cutoff = ctx.as_of
    assert cutoff is not None

    def refused(why: str) -> tuple[CheckResult, None]:
        return did_not_run("no_as_of_evidence", detail=f"as of {as_utc(cutoff).isoformat()}: {why}"), None

    if not check.as_of_capable:
        return refused(f"{check.check_id} reads the tables as they are now; it cannot read them as of a cutoff")
    if any(c.rejects is not None for c in ctx.dataset.contracts.values()):
        return refused("a rejects table declares no history, so what it held at the cutoff cannot be shown")
    proven, through = [], []
    for ds in read_datasets(ctx):
        if ds.grain.history != "append_only":
            return refused(f"{ds.dataset} is {ds.grain.history or 'of undeclared history'}, not append_only")
        if ds.load_time is None:
            return refused(f"{ds.dataset} has no load_time to leave out rows loaded after the cutoff")
        why, last = drift_proof(ctx, ds.table, cutoff)
        if why is not None:
            return refused(why)
        proven.append(ds.dataset)
        through.append(last)
    return None, (f"read as the data stood at {as_utc(cutoff).isoformat()}: {', '.join(proven)} append_only, "
                  f"{DRIFT} unchanged since, drift checked through {min(through).isoformat()}")
