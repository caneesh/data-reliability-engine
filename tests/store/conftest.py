"""A fresh dq store per test module, plus row builders for synthetic store rows."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.local_setup import create_store


@pytest.fixture(scope="module")
def store(spark, request) -> str:
    """Name of a dq database created for this test module."""
    name = "dq_" + request.module.__name__.rsplit(".", 1)[-1]
    create_store(spark, name)
    return name


def ts(day: int, hour: int = 0) -> datetime:
    return datetime(2026, 1, day, hour, tzinfo=timezone.utc)


def append_rows(spark, database: str, table: str, rows: list[dict[str, Any]]) -> None:
    """Append rows (missing columns become null) through the engine's writer."""
    schema = spark.table(f"{database}.{table}").schema
    data = [tuple(row.get(f.name) for f in schema.fields) for row in rows]
    writer.append(spark.createDataFrame(data, schema), database, table)
