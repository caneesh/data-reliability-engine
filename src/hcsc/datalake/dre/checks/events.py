"""Evaluation events and identities (spec section 4).

Release 1 has one kind of event, a batch load event: one dataset and one load
window. The window ends at this run's start; it starts where the dataset's
previous NORMAL evaluation ended, which is the start time of the latest run
that wrote results for the dataset (none on the first run).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from hcsc.datalake.dre.store.names import validate_dq_database, validate_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _sha256(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _iso(value: datetime | None) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True)
class Event:
    dataset: str
    window_start: datetime | None
    window_end: datetime

    @property
    def event_id(self) -> str:
        return _sha256(self.dataset, _iso(self.window_start), _iso(self.window_end))


def evaluation_id(event_id: str, check_id: str, expectation_version: int, engine_version: str) -> str:
    return _sha256(event_id, check_id, str(expectation_version), engine_version)


def previous_window_end(spark: SparkSession, dq_database: str, dataset: str) -> datetime | None:
    """Start of the latest run that wrote NORMAL results for the dataset, if any."""
    validate_dq_database(dq_database)
    validate_identifier(dataset, "dataset id")
    if not spark.catalog.tableExists(f"{dq_database}.dq_check_result"):
        return None
    row = spark.sql(
        f"SELECT MAX(r.started_at) AS window_end "
        f"FROM {dq_database}.dq_check_result c JOIN {dq_database}.v_latest_run r ON c.run_id = r.run_id "
        f"WHERE c.dataset = '{dataset}' AND c.execution_type = 'NORMAL'"
    ).collect()[0]
    return row.window_end
