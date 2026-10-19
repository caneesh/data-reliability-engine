"""What DRE learns about its own store after publishing (checkpoint 2 corrections R1 and R2):
the dq_integrity table and the integrity sweep.

The consistency model (spec section 5, "Commit protocol"): DRE is the only writer of the dq store,
append only, and every row carries the run id of the one process that wrote it; a unit's rows are
counted against its commits before they are written. So nothing in DRE changes a unit's rows
after publication. Two things outside the model can: retention (documented, per table, and
recorded on each commit as evidence_kept_until) and a change made outside DRE (files deleted or
added by hand). The sweep finds the second kind, and records it here:

- EVIDENCE_MISMATCH: a commit's stored rows no longer match it, before its evidence_kept_until.
  The unit's evidence state becomes INVALID (store/ddl.sql, v_commit_state): its result stays
  visible but is never healthy, and its evidence is never used as a baseline.
- COMMIT_CONFLICT: one commit identity stored with different contents (also recorded by the
  publisher when it reads one back).
- SWEEP: each sweep attempt, COMPLETED or FAILED, with its scope. A failed sweep is a transient
  verification failure, not a loss: what it covers is UNKNOWN until a later sweep completes.
- REBASELINED: a later evaluation re-established the baseline an INVALID unit fed (the hop's open
  keys by a full sweep, the partition states by a new recording), so it no longer compromises
  the checks that read it.
- LEDGER_UNBALANCED (checkpoint 3, L5): a HOP_LEDGER evaluation whose dispositions did not add up to
  its due population, an engine defect. Its result is DID_NOT_RUN / PLATFORM / ledger_unbalanced (fail
  closed, the window held); the record makes it an integrity incident the digest and the watchdog
  report. It does not make any unit INVALID: no stored evidence changed.

These rows are observations, like dq_run and dq_file: they are not commit-gated.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.times import as_utc
from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.store.runs import Run

SWEEP, MISMATCH, CONFLICT, REBASELINED = "SWEEP", "EVIDENCE_MISMATCH", "COMMIT_CONFLICT", "REBASELINED"
UNBALANCED = "LEDGER_UNBALANCED"
INCIDENTS = (MISMATCH, CONFLICT, UNBALANCED)  # what the digest and the watchdog report as incidents
DAILY, FULL = "DAILY", "FULL"
COMPLETED, FAILED = "COMPLETED", "FAILED"
# The checks that build on earlier evidence: HOP_KEY_CURRENCY judges the hop's open keys from its
# key events; T1_PARTITION_DRIFT compares with the latest partition states.
BASELINE_CHECKS = ("HOP_KEY_CURRENCY", "T1_PARTITION_DRIFT")
# A completed sweep older than this is stale (the sweep runs daily): reported by the digest and
# the watchdog.
STALE_AFTER = timedelta(hours=26)


def append_records(spark: SparkSession, dq_database: str, rows: list[dict[str, Any]]) -> None:
    """Append rows (dicts keyed by column; missing columns are null) to dq_integrity."""
    validate_dq_database(dq_database)
    if not rows:
        return
    schema = spark.table(f"{dq_database}.dq_integrity").schema
    data = [tuple(row.get(f.name) for f in schema.fields) for row in rows]
    writer.append(spark.createDataFrame(data, schema).coalesce(1), dq_database, "dq_integrity")


def record(run: Run, kind: str, **fields: Any) -> dict[str, Any]:
    """One dq_integrity row recorded by `run`."""
    return {"run_id": run.run_id, "record": kind, "recorded_at": datetime.now(timezone.utc),
            "run_date": run.run_date, **fields}


def unbalanced_records(run: Run, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A LEDGER_UNBALANCED record for each stored result row that says its ledger did not balance
    (counts only, from the row's observed text)."""
    return [record(run, UNBALANCED, subject_run_id=r["run_id"], subject_run_date=r["run_date"],
                   evaluation_id=r["evaluation_id"], commit_kind="result", dataset=r["dataset"],
                   check_id=r["check_id"], detail=r.get("observed"))
            for r in rows if r.get("reason_code") == "ledger_unbalanced"]


def conflict_record(run: Run, commit: Any, stored: set[tuple]) -> dict[str, Any]:
    """A commit identity the publisher read back with contents other than its own."""
    return record(run, CONFLICT, subject_run_id=commit.run_id, subject_run_date=run.run_date,
                  evaluation_id=commit.evaluation_id, commit_kind=commit.kind, dataset=commit.dataset,
                  check_id=commit.check_id, detail=f"{len(stored)} different contents stored for one commit")


@dataclass(frozen=True)
class SweepOutcome:
    mismatches: int
    conflicts: int


def sweep(spark: SparkSession, dq_database: str, run: Run, scope: str, scope_from: date, scope_to: date,
          today: date) -> SweepOutcome:
    """Check every commit with run_date in [scope_from, scope_to] against the store, record what
    is new (mismatches before evidence_kept_until, conflicting identities), then a SWEEP row. A
    failure records a FAILED sweep (a transient verification failure) and is raised."""
    validate_dq_database(dq_database)
    span = f"run_date BETWEEN DATE'{scope_from.isoformat()}' AND DATE'{scope_to.isoformat()}'"
    try:
        known = (f"SELECT subject_run_id, subject_run_date, evaluation_id, commit_kind FROM {dq_database}.dq_integrity "
                 f"WHERE record = '{{kind}}'")
        lost = spark.sql(
            f"SELECT i.* FROM {dq_database}.v_commit_integrity i "
            f"LEFT ANTI JOIN ({known.format(kind=MISMATCH)}) k ON k.subject_run_id = i.run_id "
            f"  AND k.subject_run_date = i.run_date AND k.evaluation_id = i.evaluation_id AND k.commit_kind = i.kind "
            f"WHERE i.{span} AND (i.evidence_kept_until IS NULL OR i.evidence_kept_until >= DATE'{today.isoformat()}')"
        ).collect()
        conflicting = spark.sql(
            f"SELECT c.run_id, c.run_date, c.evaluation_id, c.kind, c.dataset, c.check_id, c.versions "
            f"FROM {dq_database}.v_commit_state c "
            f"LEFT ANTI JOIN ({known.format(kind=CONFLICT)}) k ON k.subject_run_id = c.run_id "
            f"  AND k.subject_run_date = c.run_date AND k.evaluation_id = c.evaluation_id AND k.commit_kind = c.kind "
            f"WHERE c.{span} AND c.versions > 1").collect()
        rows = [record(run, MISMATCH, subject_run_id=r.run_id, subject_run_date=r.run_date,
                       evaluation_id=r.evaluation_id, commit_kind=r.kind, dataset=r.dataset, check_id=r.check_id,
                       detail=_mismatch_detail(r)) for r in lost]
        rows += [record(run, CONFLICT, subject_run_id=r.run_id, subject_run_date=r.run_date,
                        evaluation_id=r.evaluation_id, commit_kind=r.kind, dataset=r.dataset, check_id=r.check_id,
                        detail=f"{r.versions} different contents stored for one commit") for r in conflicting]
        rows.append(record(run, SWEEP, status=COMPLETED, scope=scope, scope_from=scope_from, scope_to=scope_to,
                           detail=f"{len(lost)} new mismatch(es), {len(conflicting)} new conflict(s)"))
        append_records(spark, dq_database, rows)
        return SweepOutcome(len(lost), len(conflicting))
    except Exception as exc:
        try:
            append_records(spark, dq_database, [record(run, SWEEP, status=FAILED, scope=scope, scope_from=scope_from,
                                                       scope_to=scope_to, detail=type(exc).__name__)])
        except Exception:  # the store cannot take it either: the freshness report shows the gap
            pass
        raise


def _mismatch_detail(r: Any) -> str:
    """Counts only (hard rule 6): what was declared and what is stored now."""
    if r.kind == "cause":
        pairs = [("cause rows", r.cause_rows, r.stored_cause_rows)]
    else:
        pairs = [("result rows", r.result_rows, r.stored_result_rows)]
        if r.key_event_rows:
            pairs.append(("key events", r.key_event_rows, r.stored_key_event_rows))
        if r.partition_state_rows:
            pairs.append(("partition states", r.partition_state_rows, r.stored_partition_state_rows))
        if r.reconciliation_rows:
            pairs.append(("ledger rows", r.reconciliation_rows, r.stored_reconciliation_rows))
    return "; ".join(f"{what}: {declared} declared, {stored} stored" for what, declared, stored in pairs
                     if declared != stored)


@dataclass(frozen=True)
class Compromised:
    """The INVALID units that still feed a check's baseline, per (dataset, check_id): HOP_KEY_CURRENCY
    then runs as a full sweep and T1_PARTITION_DRIFT cannot pass, until each is REBASELINED.
    unknown: the store could not be read, so every such check is treated as compromised."""

    units: dict[tuple[str, str], list[tuple[str, date, str]]]
    unknown: bool = False

    def of(self, dataset: str, check_id: str) -> list[tuple[str, date, str]] | None:
        """The units compromising (dataset, check_id), [] when unknown, None when it is clean."""
        if check_id not in BASELINE_CHECKS:
            return None
        if self.unknown:
            return []
        return self.units.get((dataset, check_id))


def compromised_baselines(spark: SparkSession, dq_database: str) -> Compromised:
    """INVALID result units of the baseline checks that carried evidence and are not REBASELINED.
    If the store cannot be read, every baseline check counts as compromised (fail closed)."""
    validate_dq_database(dq_database)
    checks = ", ".join(f"'{c}'" for c in BASELINE_CHECKS)
    try:
        rows = spark.sql(
            f"SELECT c.dataset, c.check_id, c.run_id, c.run_date, c.evaluation_id FROM {dq_database}.v_commit_state c "
            f"LEFT ANTI JOIN (SELECT subject_run_id, subject_run_date, evaluation_id FROM {dq_database}.dq_integrity "
            f"  WHERE record = '{REBASELINED}') b ON b.subject_run_id = c.run_id AND b.subject_run_date = c.run_date "
            f"  AND b.evaluation_id = c.evaluation_id "
            f"WHERE c.kind = 'result' AND c.evidence_state = 'INVALID' AND c.check_id IN ({checks}) "
            f"AND (c.key_event_rows > 0 OR c.partition_state_rows > 0)").collect()
    except Exception:
        return Compromised({}, unknown=True)
    units: dict[tuple[str, str], list[tuple[str, date, str]]] = {}
    for r in rows:
        units.setdefault((r.dataset, r.check_id), []).append((r.run_id, r.run_date, r.evaluation_id))
    return Compromised(units)


def rebaseline_records(run: Run, dataset: str, check_id: str, units: list[tuple[str, date, str]]) -> list[dict[str, Any]]:
    return [record(run, REBASELINED, subject_run_id=run_id, subject_run_date=run_date, evaluation_id=evaluation_id,
                   commit_kind="result", dataset=dataset, check_id=check_id,
                   detail="baseline re-established by a later evaluation of this check")
            for run_id, run_date, evaluation_id in units]


@dataclass(frozen=True)
class Freshness:
    """The latest integrity sweep attempts: when the last one completed, whether the latest failed,
    and whether verification is stale (no completed sweep within STALE_AFTER)."""

    last_completed: datetime | None
    last_failed: datetime | None
    stale: bool
    new_incidents: int  # mismatches and conflicts recorded in the last day

    def line(self) -> str:
        if self.last_completed is None:
            verified = "store integrity: not verified yet (no completed integrity sweep)"
        else:
            verified = f"store integrity: last verified {self.last_completed:%Y-%m-%d %H:%M} UTC"
        notes = []
        if self.stale and self.last_completed is not None:
            notes.append("stale: no completed sweep in the last 26 hours")
        if self.last_failed is not None and (self.last_completed is None or self.last_failed > self.last_completed):
            notes.append(f"the latest sweep failed at {self.last_failed:%Y-%m-%d %H:%M} UTC (evidence it covers is UNKNOWN)")
        if self.new_incidents:
            notes.append(f"{self.new_incidents} new integrity incident(s) in the last day")
        return verified + ("; " + "; ".join(notes) if notes else "")


def freshness(spark: SparkSession, dq_database: str, now: datetime) -> Freshness:
    validate_dq_database(dq_database)
    now = as_utc(now)
    day = (now - timedelta(days=1)).date().isoformat()
    row = spark.sql(
        f"SELECT MAX(CASE WHEN record = '{SWEEP}' AND status = '{COMPLETED}' THEN recorded_at END) AS completed, "
        f"MAX(CASE WHEN record = '{SWEEP}' AND status = '{FAILED}' THEN recorded_at END) AS failed, "
        f"SUM(CASE WHEN record IN ({', '.join(repr(k) for k in INCIDENTS)}) AND run_date >= DATE'{day}' "
        f"  AND recorded_at >= TIMESTAMP '{(now - timedelta(days=1)):%Y-%m-%d %H:%M:%S}' THEN 1 ELSE 0 END) AS incidents "
        f"FROM {dq_database}.dq_integrity").collect()[0]
    completed = as_utc(row.completed) if row.completed else None
    failed = as_utc(row.failed) if row.failed else None
    stale = completed is None or now - completed > STALE_AFTER
    return Freshness(completed, failed, stale, int(row.incidents or 0))
