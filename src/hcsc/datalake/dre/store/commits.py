"""The dq_commit table (Phase 1 design, ADR-07): an evaluation becomes visible once its commit row
is stored.

An evaluation id's evidence rows (key events, partition states) and its result rows are written
first; its commit row last, in a batch with other evaluations' (one append per batch, so the
store does not gain a small file per evaluation). Every view of current state reads only committed
rows (store/ddl.sql, `v_committed_*`), so a run that dies, or a write that fails, anywhere before
the commit leaves nothing visible, and the window is evaluated again. The evidence counts say what
a commit vouches for: an evidence_incomplete marker commits its result rows with counts of 0, so
whatever evidence rows the failed write left stay invisible.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.times import as_utc
from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

SCHEMA = ("run_id STRING, evaluation_id STRING, dataset STRING, check_id STRING, result_rows INT, "
          "key_event_rows BIGINT, partition_state_rows INT, committed_at TIMESTAMP, run_date DATE")


@dataclass(frozen=True)
class Commit:
    """What one evaluation id's commit vouches for."""

    run_id: str
    evaluation_id: str
    dataset: str
    check_id: str
    result_rows: int
    key_event_rows: int = 0
    partition_state_rows: int = 0


def append_commits(spark: SparkSession, dq_database: str, commits: list[Commit], committed_at: datetime,
                   run_date: date) -> int:
    """Append one row per commit, in one write; return how many."""
    validate_dq_database(dq_database)
    if not commits:
        return 0
    rows = [(c.run_id, c.evaluation_id, c.dataset, c.check_id, c.result_rows, c.key_event_rows,
             c.partition_state_rows, as_utc(committed_at), run_date) for c in commits]
    writer.append(spark.createDataFrame(rows, SCHEMA), dq_database, "dq_commit")
    return len(rows)
