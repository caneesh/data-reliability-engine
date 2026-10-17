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


def append_rows(spark, database: str, table: str, rows: list[dict[str, Any]], commit: bool = True) -> None:
    """Append rows (missing columns become null) through the engine's writer. Result, key-event and
    partition-state rows stand for a stored evaluation, so they are committed too (ADR-07: only
    committed rows are visible); commit=False leaves them uncommitted, as a run that died would."""
    schema = spark.table(f"{database}.{table}").schema
    data = [tuple(row.get(f.name) for f in schema.fields) for row in rows]
    writer.append(spark.createDataFrame(data, schema), database, table)
    if commit:
        commit_rows(spark, database, table, rows)


def commit_rows(spark, database: str, table: str, rows: list[dict[str, Any]]) -> None:
    """The dq_commit rows that publish synthetic rows of `table`, as the run would write them."""
    from collections import Counter

    from hcsc.datalake.dre.store.commits import Commit, append_commits

    if table == "dq_check_result":
        keyed = Counter((r.get("run_id"), r.get("evaluation_id"), r.get("dataset"), r.get("check_id"), r.get("run_date"))
                        for r in rows)
        make = lambda run, ev, ds, check, n: Commit(run, ev, ds, check, n)  # noqa: E731
    elif table == "dq_key_event":
        keyed = Counter((r.get("run_id"), r.get("evaluation_id"), r.get("dataset"), r.get("check_id"), r.get("run_date"))
                        for r in rows)
        make = lambda run, ev, ds, check, n: Commit(run, ev, ds, check, 0, key_event_rows=n)  # noqa: E731
    elif table == "dq_partition_state":
        keyed = Counter((r.get("run_id"), f"partition-state-{r.get('run_id')}", r.get("dataset"), "T1_PARTITION_DRIFT",
                         r.get("run_date")) for r in rows)
        make = lambda run, ev, ds, check, n: Commit(run, ev, ds, check, 0, partition_state_rows=n)  # noqa: E731
    else:
        return
    by_day: dict[Any, list[Commit]] = {}
    for (run, ev, ds, check, day), n in keyed.items():
        by_day.setdefault(day, []).append(make(run, ev, ds, check, n))
    for day, commits in by_day.items():
        append_commits(spark, database, commits, datetime(2026, 1, 1, tzinfo=timezone.utc), day)
