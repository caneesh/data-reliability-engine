"""Append cause results to dq_cause_result through the writer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def append_cause_results(spark: SparkSession, dq_database: str, rows: list[dict[str, Any]]) -> None:
    """Append rows (dicts keyed by column; missing columns are null) to dq_cause_result."""
    validate_dq_database(dq_database)
    if not rows:
        return
    schema = spark.table(f"{dq_database}.dq_cause_result").schema
    data = [tuple(row.get(f.name) for f in schema.fields) for row in rows]
    writer.append(spark.createDataFrame(data, schema), dq_database, "dq_cause_result")
