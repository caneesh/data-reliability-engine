"""T1_PARTITION_DRIFT (spec section 6, release 4): a partition rewritten outside the window is FAILED,
one rewritten with the same rows or inside the window PASSES, the first sight records the baseline,
and the DID_NOT_RUN paths (not partitioned, a view, an unreadable directory) say why."""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.registry import CHECKS
from hcsc.datalake.dre.checks.tier1.partition_drift import parse_spec
from hcsc.datalake.dre.config.models import Dataset
from hcsc.datalake.dre.store.local_setup import create_store
from hcsc.datalake.dre.store.partitions import latest_state, record_state
from tests.fixtures.settings import SETTINGS

UTC = timezone.utc
DQ = "dq_drift"
# The window: 10 Jan 00:00 to 01:00 UTC, so partitions 9, 10 and 11 January are expected to change.
EVENT = Event("drift", datetime(2026, 1, 10, 0, tzinfo=UTC), datetime(2026, 1, 10, 1, tzinfo=UTC))
NOW = datetime(2026, 1, 10, 1, 5, tzinfo=UTC)


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


def partitioned(spark, table: str, rows_per_day: dict[str, int]) -> None:
    """An ORC table partitioned by dt (yyyy-MM-dd) with that many rows in each partition."""
    database = table.split(".")[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {table} (id INT) PARTITIONED BY (dt STRING) STORED AS ORC")
    for day, n in rows_per_day.items():
        overwrite(spark, table, day, n)


def overwrite(spark, table: str, day: str, n: int) -> None:
    """Rewrite one partition with n rows (what an insert-overwrite does)."""
    values = ", ".join(f"({i})" for i in range(1, n + 1)) or "(NULL)"
    if n:
        spark.sql(f"INSERT OVERWRITE TABLE {table} PARTITION (dt = '{day}') VALUES {values}")
    else:
        spark.sql(f"INSERT OVERWRITE TABLE {table} PARTITION (dt = '{day}') SELECT id FROM {table} WHERE 1 = 0")


def dataset(table: str, name: str = "drift", tolerance: int = 0, **extra) -> Dataset:
    fields = {"dataset": name, "table": table, "layer": "GOLD", "key": ["id"], "partition_column": "dt",
              "partition_format": "yyyy-MM-dd", "check_params": {"T1_PARTITION_DRIFT": {"tolerance_rows": tolerance}},
              **extra}
    return Dataset(**fields)


RUNS = {"n": 0}


def run(spark, ds: Dataset, record: bool = True):
    """One evaluation; the state rows are appended as the runner would (each run with its own id
    and time), so the next run compares."""
    [result], _ = run_check(CHECKS["T1_PARTITION_DRIFT"], CheckContext(spark, ds, None, SETTINGS, "UTC", DQ), EVENT)
    if record and result.partition_state:
        RUNS["n"] += 1
        at = NOW + timedelta(minutes=RUNS["n"])
        record_state(spark, DQ, ds.dataset, ds.table, list(result.partition_state), f"run-{RUNS['n']}", at, at.date())
    return result


def test_first_sight_records_the_baseline_and_compares_nothing(spark) -> None:
    partitioned(spark, "drift.base", {"2026-01-02": 3, "2026-01-09": 2, "2026-01-10": 1})
    result = run(spark, dataset("drift.base", "base"))
    assert (result.state, result.reason_code, result.population) == ("DID_NOT_RUN", "insufficient_history", 3)
    assert json.loads(result.detail)["partitions"] == 3
    state = latest_state(spark, DQ, "base")
    assert {spec: s.row_count for spec, s in state.items()} == {"dt=2026-01-02": 3, "dt=2026-01-09": 2, "dt=2026-01-10": 1}
    assert all(s.file_count >= 1 and s.total_bytes > 0 and s.latest_modified_at is not None for s in state.values())


def test_a_partition_rewritten_outside_the_window_with_fewer_rows_fails(spark) -> None:
    partitioned(spark, "drift.lost", {"2026-01-02": 3, "2026-01-03": 4, "2026-01-10": 1})
    ds = dataset("drift.lost", "lost")
    run(spark, ds)
    overwrite(spark, "drift.lost", "2026-01-02", 1)   # an old partition re-written short
    overwrite(spark, "drift.lost", "2026-01-10", 5)   # the window's partition grows: expected
    result = run(spark, ds)
    assert (result.state, result.population, result.violations) == ("FAILED", 2, 1)
    assert json.loads(result.detail)["changed"] == [{"partition": "dt=2026-01-02", "before": 3, "after": 1}]
    assert result.observed == ("3 partitions: 1 expected in the window, 1 unchanged, 0 rewritten with the same rows, "
                               "1 changed, 0 dropped, 0 new outside the window")
    assert result.expected == "no partition outside the window changes its row count by more than 0 or disappears"
    # The stored count follows the rewrite, so the next run compares against 1, not 3.
    assert latest_state(spark, DQ, "lost")["dt=2026-01-02"].row_count == 1
    assert run(spark, ds).state == "PASSED"


def test_only_the_window_partitions_changing_passes(spark) -> None:
    partitioned(spark, "drift.ok", {"2026-01-02": 3, "2026-01-09": 2, "2026-01-10": 1})
    ds = dataset("drift.ok", "ok")
    run(spark, ds)
    overwrite(spark, "drift.ok", "2026-01-09", 7)
    overwrite(spark, "drift.ok", "2026-01-11", 2)  # a new partition inside the window
    result = run(spark, ds)
    assert (result.state, result.population, result.violations) == ("PASSED", 1, 0)
    assert result.observed.startswith("4 partitions: 3 expected in the window, 1 unchanged")


def test_a_partition_rewritten_with_the_same_rows_is_reported_not_failed(spark) -> None:
    partitioned(spark, "drift.same", {"2026-01-02": 3, "2026-01-10": 1})
    ds = dataset("drift.same", "same")
    run(spark, ds)
    overwrite(spark, "drift.same", "2026-01-02", 3)  # a compaction or an idempotent re-run
    result = run(spark, ds)
    assert (result.state, result.population, result.violations) == ("PASSED", 1, 0)
    assert "1 rewritten with the same rows" in result.observed
    assert json.loads(result.detail)["rewritten_same"] == ["dt=2026-01-02"]


def test_tolerance_rows_allows_a_small_move(spark) -> None:
    partitioned(spark, "drift.tol", {"2026-01-02": 10, "2026-01-10": 1})
    ds = dataset("drift.tol", "tol", tolerance=2)
    run(spark, ds)
    overwrite(spark, "drift.tol", "2026-01-02", 9)
    assert run(spark, ds).state == "PASSED"
    overwrite(spark, "drift.tol", "2026-01-02", 6)
    result = run(spark, ds)
    assert (result.state, result.violations) == ("FAILED", 1)
    assert json.loads(result.detail)["changed"] == [{"partition": "dt=2026-01-02", "before": 9, "after": 6}]


def test_a_dropped_partition_outside_the_window_fails(spark) -> None:
    partitioned(spark, "drift.gone", {"2026-01-02": 3, "2026-01-03": 2, "2026-01-10": 1})
    ds = dataset("drift.gone", "gone")
    run(spark, ds)
    spark.sql("ALTER TABLE drift.gone DROP PARTITION (dt = '2026-01-03')")
    result = run(spark, ds)
    assert (result.state, result.population, result.violations) == ("FAILED", 2, 1)
    assert json.loads(result.detail)["dropped"] == [{"partition": "dt=2026-01-03", "before": 2}]
    assert "1 dropped" in result.observed


def test_every_partition_inside_the_window_is_empty_population(spark) -> None:
    partitioned(spark, "drift.young", {"2026-01-10": 1})
    ds = dataset("drift.young", "young")
    run(spark, ds)
    result = run(spark, ds)
    assert (result.state, result.reason_code) == ("DID_NOT_RUN", "empty_population")


def test_did_not_run_paths_name_the_problem(spark) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS drift")
    spark.sql("DROP TABLE IF EXISTS drift.flat")
    spark.sql("CREATE TABLE drift.flat (id INT, dt STRING) STORED AS ORC")
    flat = run(spark, dataset("drift.flat", "flat"))
    assert (flat.state, flat.reason_code) == ("DID_NOT_RUN", "invalid_config")
    assert "not partitioned by dt" in flat.detail
    missing = run(spark, dataset("drift.flat", "flat2", partition_format=None))
    assert (missing.state, missing.reason_code) == ("DID_NOT_RUN", "invalid_config")
    assert "partition_column and partition_format" in missing.detail
    spark.sql("CREATE OR REPLACE VIEW drift.v AS SELECT id, dt FROM drift.flat")
    view = run(spark, dataset("drift.v", "view"))
    assert (view.state, view.reason_code) == ("DID_NOT_RUN", "invalid_config")
    assert "view" in view.detail


def test_an_unreadable_partition_directory_is_hdfs_unavailable(spark) -> None:
    partitioned(spark, "drift.broken", {"2026-01-02": 3, "2026-01-10": 1})
    ds = dataset("drift.broken", "broken")
    run(spark, ds)
    location = next(r.data_type for r in spark.sql("DESCRIBE FORMATTED drift.broken").collect()
                    if (r.col_name or "").strip() == "Location")
    shutil.rmtree(Path(location.replace("file:", "")) / "dt=2026-01-02")  # the directory goes, the metastore row stays
    result = run(spark, ds)
    assert (result.state, result.reason_code, result.reason_category) == ("DID_NOT_RUN", "hdfs_unavailable", "PLATFORM")


def test_parse_spec_unescapes_hive_values() -> None:
    assert parse_spec("dt=2026-01-02") == {"dt": "2026-01-02"}
    assert parse_spec("region=EU/ts=2026-01-02 10%3A00") == {"region": "EU", "ts": "2026-01-02 10:00"}
