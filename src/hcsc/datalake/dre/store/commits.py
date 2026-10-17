"""The dq_commit table (Phase 1 design, ADR-07): an evaluation, or a cause investigation, becomes
visible once its commit rows are stored.

An evaluation id's evidence rows (key events, partition states) and its result rows are written
first, an investigation's cause rows first; the commit row last, in a batch with other units' (one
append per batch, written as one file, so the store does not gain a small file per evaluation and
a batch is stored whole or not at all). Before a batch is written, the rows its commits vouch for
are counted in the store (`stored_counts`), and a unit whose stored rows differ from its
declaration is withheld. Every view of current state reads only committed rows (store/ddl.sql,
`v_committed_*`), so a run that dies, or a write that fails, anywhere before the commit leaves
nothing visible, and the window is evaluated again. The evidence counts say what a commit vouches
for: an evidence_incomplete marker commits its result rows with counts of 0, so whatever evidence
rows the failed write left stay invisible. A cause commit names the run whose committed result it
explains (result_run_id): causes attach to a result by identity, never by time.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.times import as_utc
from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

SCHEMA = ("run_id STRING, evaluation_id STRING, dataset STRING, check_id STRING, kind STRING, "
          "result_run_id STRING, result_rows INT, key_event_rows BIGINT, partition_state_rows INT, "
          "cause_rows INT, committed_at TIMESTAMP, run_date DATE")

RESULT, CAUSE = "result", "cause"
_RUN_ID = re.compile(r"^[0-9a-fA-F-]{1,64}$")  # a run id is a UUID (store/runs.py)


def _this_run(run_id: str, run_date: date) -> str:
    if not _RUN_ID.match(run_id):
        raise ValueError("unexpected run id")
    return f"run_date = DATE'{run_date.isoformat()}' AND run_id = '{run_id}'"


@dataclass(frozen=True)
class Commit:
    """What one evaluation id's commit (kind result) or one investigation's commit (kind cause)
    vouches for. A result commit's result_run_id is its own run (the default)."""

    run_id: str
    evaluation_id: str
    dataset: str
    check_id: str
    result_rows: int
    key_event_rows: int = 0
    partition_state_rows: int = 0
    kind: str = RESULT
    result_run_id: str | None = None
    cause_rows: int = 0

    @property
    def identity(self) -> tuple[str, str, str]:
        """A commit's identity: storing it twice publishes nothing more (the views ask whether a
        matching commit exists)."""
        return self.run_id, self.evaluation_id, self.kind


def append_commits(spark: SparkSession, dq_database: str, commits: list[Commit], committed_at: datetime,
                   run_date: date) -> int:
    """Append one row per commit, in one write of one file; return how many."""
    validate_dq_database(dq_database)
    if not commits:
        return 0
    rows = [(c.run_id, c.evaluation_id, c.dataset, c.check_id, c.kind, c.result_run_id or c.run_id,
             c.result_rows, c.key_event_rows, c.partition_state_rows, c.cause_rows, as_utc(committed_at), run_date)
            for c in commits]
    # One file: a batch is stored whole or not at all, never a subset of its rows.
    writer.append(spark.createDataFrame(rows, SCHEMA).coalesce(1), dq_database, "dq_commit")
    return len(rows)


def stored_commits(spark: SparkSession, dq_database: str, run_id: str, run_date: date) -> set[tuple[str, str, str]]:
    """The identities of the run's commit rows in the store (read back after a failed write)."""
    validate_dq_database(dq_database)
    rows = spark.sql(f"SELECT DISTINCT run_id, evaluation_id, kind FROM {dq_database}.dq_commit "
                     f"WHERE {_this_run(run_id, run_date)}").collect()
    return {(r.run_id, r.evaluation_id, r.kind) for r in rows}


def stored_counts(spark: SparkSession, dq_database: str, run_id: str, run_date: date,
                  commits: list[Commit]) -> dict[tuple[str, str], int]:
    """How many rows of each kind the run has stored, for what `commits` vouch for, in one query
    over the run's own rows: ("result" | "key_event" | "cause", evaluation id) and
    ("partition_state", dataset) -> count. A table no commit declares rows in is not read."""
    validate_dq_database(dq_database)
    run = _this_run(run_id, run_date)
    parts = []
    if any(c.kind == RESULT for c in commits):
        parts.append(f"SELECT 'result' AS what, evaluation_id AS id, COUNT(*) AS n FROM {dq_database}.dq_check_result "
                     f"WHERE {run} GROUP BY evaluation_id")
    if any(c.kind == RESULT and c.key_event_rows > 0 for c in commits):
        parts.append(f"SELECT 'key_event' AS what, evaluation_id AS id, COUNT(*) AS n FROM {dq_database}.dq_key_event "
                     f"WHERE {run} GROUP BY evaluation_id")
    if any(c.kind == RESULT and c.partition_state_rows > 0 for c in commits):
        parts.append(f"SELECT 'partition_state' AS what, dataset AS id, COUNT(*) AS n "
                     f"FROM {dq_database}.dq_partition_state WHERE {run} GROUP BY dataset")
    if any(c.kind == CAUSE for c in commits):
        parts.append(f"SELECT 'cause' AS what, evaluation_id AS id, COUNT(*) AS n FROM {dq_database}.dq_cause_result "
                     f"WHERE {run} GROUP BY evaluation_id")
    if not parts:
        return {}
    return {(r.what, r.id): r.n for r in spark.sql(" UNION ALL ".join(parts)).collect()}


def mismatches(commit: Commit, counts: dict[tuple[str, str], int]) -> list[str]:
    """What the store holds that differs from what the commit declares (empty when it matches)."""
    if commit.kind == CAUSE:
        checks = [("cause rows", ("cause", commit.evaluation_id), commit.cause_rows)]
    else:
        checks = [("result rows", ("result", commit.evaluation_id), commit.result_rows)]
        if commit.key_event_rows > 0:
            checks.append(("key events", ("key_event", commit.evaluation_id), commit.key_event_rows))
        if commit.partition_state_rows > 0:
            checks.append(("partition states", ("partition_state", commit.dataset), commit.partition_state_rows))
    return [f"{what}: {counts.get(key, 0)} stored, {declared} declared"
            for what, key, declared in checks if counts.get(key, 0) != declared]
