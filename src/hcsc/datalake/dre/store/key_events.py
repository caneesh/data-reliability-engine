"""Append key events to dq_key_event through the writer (spec section 5).

The events DataFrame (key_hash, key_value, event, state_detail; optionally key_encoding) comes from
a hop check;
the run adds where it came from, including the hop (`<upstream>-><dataset>`): a key is open
or cleared per hop, so two upstreams of one dataset never clear each other's keys. key_value
is restricted: it stays in the dq store and is never logged or emailed.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import release_checkpoint
from hcsc.datalake.dre.checks.keys import KEY_ENCODING
from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def append_key_events(events: DataFrame, dq_database: str, run_id: str, evaluation_id: str, dataset: str,
                      check_id: str, hop: str, observed_at: datetime, run_date: date) -> int:
    """Append the events with their run, evaluation, dataset, check and hop; return how many."""
    from pyspark.sql import functions as F

    validate_dq_database(dq_database)
    # Release N computes key encoding 1 only. An event names another encoding only when it is about an
    # identity of that encoding: the REKEYED a release N engine writes for an encoding-2 identity it
    # found open (rollback from N+1). Release N names no predecessor.
    encoding = (F.coalesce(F.col("key_encoding").cast("int"), F.lit(KEY_ENCODING)) if "key_encoding" in events.columns
                else F.lit(KEY_ENCODING))
    frame = events.select(
        F.lit(run_id).alias("run_id"), F.lit(evaluation_id).alias("evaluation_id"),
        F.lit(dataset).alias("dataset"), F.lit(check_id).alias("check_id"), F.lit(hop).alias("hop"),
        "key_hash", "key_value", "event", "state_detail",
        F.lit(observed_at).cast("timestamp").alias("observed_at"),
        encoding.alias("key_encoding"), F.lit(None).cast("string").alias("predecessor_hash"),
        F.lit(run_date).cast("date").alias("run_date"),
    ).localCheckpoint()  # fix the events before writing: they were computed from dq_key_event itself
    try:
        count = frame.count()
        if count:
            writer.append(frame, dq_database, "dq_key_event")
    finally:
        release_checkpoint(frame)
    return count
