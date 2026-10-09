"""The dq_file registry: landed files and when DRE first saw them (spec section 5).

A run appends a row for each file that is new, or whose size or modified time
changed since its latest row, so `v_file_status` keeps the first-seen time and
the latest size, and a size change after first sight stays visible.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.names import validate_dq_database, validate_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.sources.hdfs import FileInfo
    from hcsc.datalake.dre.store.runs import Run


def as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def known_files(spark: SparkSession, dq_database: str, feed: str) -> dict[str, tuple[datetime, int, datetime]]:
    """path -> (first_seen_at, latest size, latest modified_at) for the feed's registered files."""
    validate_dq_database(dq_database)
    validate_identifier(feed, "feed id")
    rows = spark.sql(
        f"SELECT path, first_seen_at, latest_size_bytes, latest_modified_at "
        f"FROM {dq_database}.v_file_status WHERE feed = '{feed}'"
    ).collect()
    return {r.path: (as_utc(r.first_seen_at), r.latest_size_bytes,
                     as_utc(r.latest_modified_at) if r.latest_modified_at else None) for r in rows}


def register_files(spark: SparkSession, dq_database: str, feed: str, files: list[FileInfo], run: Run,
                   now: datetime) -> int:
    """Append new and changed files; return how many rows were appended."""
    known = known_files(spark, dq_database, feed)
    rows = []
    for f in files:
        previous = known.get(f.path)
        if previous is not None and previous[1] == f.size_bytes and previous[2] == as_utc(f.modified_at):
            continue
        first_seen = previous[0] if previous is not None else as_utc(now)
        rows.append((f.path, feed, first_seen, f.size_bytes, as_utc(f.modified_at), as_utc(now), run.run_id,
                     run.run_date))
    if rows:
        schema = ("path STRING, feed STRING, first_seen_at TIMESTAMP, size_bytes BIGINT, modified_at TIMESTAMP, "
                  "observed_at TIMESTAMP, run_id STRING, run_date DATE")
        writer.append(spark.createDataFrame(rows, schema), dq_database, "dq_file")
    return len(rows)
