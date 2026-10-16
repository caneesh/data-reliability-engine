"""The dq_partition_state table: what each partition of a table looked like at a run (spec
section 5, release 4), read and appended by T1_PARTITION_DRIFT through the writer.

A row per partition per evaluation: the directory's file count, total bytes and latest
modification time, and the latest known row count (counted when the partition was first seen
or when its files changed, carried forward otherwise). `v_latest_partition_state` gives the
latest row per dataset and partition.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.times import as_utc
from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.names import validate_dq_database, validate_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass(frozen=True)
class PartitionState:
    partition_value: str  # the partition spec as SHOW PARTITIONS prints it: col=value[/col2=value2]
    file_count: int
    total_bytes: int
    latest_modified_at: datetime | None  # UTC; None for an empty directory
    row_count: int | None  # the latest known count; None only when counting failed
    counted_at: datetime | None  # UTC; when row_count was counted

    def same_files(self, other: PartitionState) -> bool:
        return (self.file_count, self.total_bytes, self.latest_modified_at) == (
            other.file_count, other.total_bytes, other.latest_modified_at)


def latest_state(spark: SparkSession, dq_database: str, dataset: str) -> dict[str, PartitionState]:
    """partition_value -> its latest recorded state for the dataset (empty on first sight)."""
    validate_dq_database(dq_database)
    validate_identifier(dataset, "dataset id")
    rows = spark.sql(
        f"SELECT partition_value, file_count, total_bytes, latest_modified_at, row_count, counted_at "
        f"FROM {dq_database}.v_latest_partition_state WHERE dataset = '{dataset}'"
    ).collect()
    return {r.partition_value: PartitionState(
        r.partition_value, r.file_count, r.total_bytes,
        as_utc(r.latest_modified_at) if r.latest_modified_at else None,
        r.row_count, as_utc(r.counted_at) if r.counted_at else None) for r in rows}


def record_state(spark: SparkSession, dq_database: str, dataset: str, table: str, states: list[PartitionState],
                 run_id: str, observed_at: datetime, run_date: date) -> int:
    """Append one row per partition; return how many."""
    validate_dq_database(dq_database)
    if not states:
        return 0
    rows = [(dataset, table, s.partition_value, s.file_count, s.total_bytes, s.latest_modified_at, s.row_count,
             s.counted_at, as_utc(observed_at), run_id, run_date) for s in states]
    schema = ("dataset STRING, table_name STRING, partition_value STRING, file_count INT, total_bytes BIGINT, "
              "latest_modified_at TIMESTAMP, row_count BIGINT, counted_at TIMESTAMP, observed_at TIMESTAMP, "
              "run_id STRING, run_date DATE")
    writer.append(spark.createDataFrame(rows, schema), dq_database, "dq_partition_state")
    return len(rows)
