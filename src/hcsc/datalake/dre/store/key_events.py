"""Append key events to dq_key_event through the writer (spec section 5).

The events DataFrame (key_hash, key_value, event, state_detail) comes from a hop check;
the run adds where it came from. key_value is restricted: it stays in the dq store and is
never logged or emailed.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def append_key_events(events: DataFrame, dq_database: str, run_id: str, evaluation_id: str, dataset: str,
                      check_id: str, observed_at: datetime, run_date: date) -> int:
    """Append the events with their run, evaluation, dataset and check; return how many."""
    from pyspark.sql import functions as F

    validate_dq_database(dq_database)
    frame = events.select(
        F.lit(run_id).alias("run_id"), F.lit(evaluation_id).alias("evaluation_id"),
        F.lit(dataset).alias("dataset"), F.lit(check_id).alias("check_id"),
        "key_hash", "key_value", "event", "state_detail",
        F.lit(observed_at).cast("timestamp").alias("observed_at"), F.lit(run_date).cast("date").alias("run_date"),
    ).localCheckpoint()  # fix the events before writing: they were computed from dq_key_event itself
    count = frame.count()
    if count:
        writer.append(frame, dq_database, "dq_key_event")
    return count
