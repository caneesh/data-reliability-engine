"""Append HOP_LEDGER's accounting to dq_reconciliation through the writer (Phase 1 build step 3).

One row per line of an evaluation's ledger (checks/hop/ledger.py builds them). They are evidence
of the evaluation's result: the runner writes them before the result, and the evaluation's commit
vouches for how many there are (ADR-07), so a ledger is visible only whole and only once committed
(store/ddl.sql, v_committed_reconciliation). Counts, codes and rule labels only: never a key.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.times import as_utc
from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

SCHEMA = ("run_id STRING, evaluation_id STRING, dataset STRING, check_id STRING, upstream STRING, line STRING, "
          "label STRING, class STRING, records BIGINT, evidence_level STRING, scope STRING, recorded_at TIMESTAMP, "
          "run_date DATE")


def append_ledger(spark: SparkSession, dq_database: str, lines: list[dict[str, Any]], run_id: str,
                  evaluation_id: str, dataset: str, check_id: str, recorded_at: datetime, run_date: date) -> int:
    """Append one row per ledger line ({upstream, line, label, class, records, evidence_level,
    scope}); return how many."""
    validate_dq_database(dq_database)
    if not lines:
        return 0
    rows = [(run_id, evaluation_id, dataset, check_id, x["upstream"], x["line"], x.get("label"), x["class"],
             x.get("records"), x.get("evidence_level"), x.get("scope"), as_utc(recorded_at), run_date) for x in lines]
    writer.append(spark.createDataFrame(rows, SCHEMA), dq_database, "dq_reconciliation")
    return len(rows)
