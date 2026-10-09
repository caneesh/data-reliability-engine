"""store/retention.py: drops only partitions older than the configured retention."""

from __future__ import annotations

from datetime import date

import pytest

from hcsc.datalake.dre.store.retention import (
    RetentionError,
    apply_retention,
    drop_partition_sql,
    expired_partitions,
    retention_cutoff,
)

TODAY = date(2026, 10, 9)


def test_cutoff_counts_back_whole_months() -> None:
    assert retention_cutoff(TODAY, 13) == date(2025, 9, 9)
    assert retention_cutoff(TODAY, 84) == date(2019, 10, 9)  # 7 years
    assert retention_cutoff(date(2026, 3, 31), 1) == date(2026, 2, 28)


@pytest.mark.parametrize("months", [0, -1, 1.5, None])
def test_non_positive_retention_refused(months: object) -> None:
    with pytest.raises(RetentionError):
        retention_cutoff(TODAY, months)  # type: ignore[arg-type]


def test_refuses_to_drop_partition_inside_retention() -> None:
    # Cutoff for 13 months is 2025-09-09: that day and anything newer must be kept.
    for run_date in (date(2025, 9, 9), date(2026, 1, 1), TODAY, date(2027, 1, 1)):
        with pytest.raises(RetentionError, match="refusing to drop"):
            drop_partition_sql("dq", "dq_file", run_date, 13, TODAY)


def test_drop_sql_for_expired_partition() -> None:
    sql = drop_partition_sql("dq", "dq_file", date(2025, 9, 8), 13, TODAY)
    assert sql == "ALTER TABLE dq.dq_file DROP IF EXISTS PARTITION (run_date = DATE'2025-09-08')"


@pytest.mark.parametrize(("database", "table"), [("dq.x", "dq_file"), ("dq", "dq_file; DROP TABLE y")])
def test_drop_sql_rejects_non_identifiers(database: str, table: str) -> None:
    with pytest.raises(ValueError, match="plain identifier"):
        drop_partition_sql(database, table, date(2020, 1, 1), 13, TODAY)


def test_expired_partitions_selects_only_older_than_cutoff() -> None:
    dates = [date(2025, 9, 9), date(2024, 1, 1), date(2025, 9, 8), date(2026, 10, 1)]
    assert expired_partitions(dates, 13, TODAY) == [date(2024, 1, 1), date(2025, 9, 8)]


def test_apply_retention_drops_only_expired_partitions(spark) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS dq_retention_test")
    spark.sql(
        "CREATE TABLE dq_retention_test.dq_file (path STRING) "
        "PARTITIONED BY (run_date DATE) STORED AS ORC"
    )
    for d in ("2024-01-01", "2025-09-08", "2025-09-09", "2026-10-01"):
        spark.sql(f"INSERT INTO dq_retention_test.dq_file PARTITION (run_date = DATE'{d}') VALUES ('f')")

    dropped = apply_retention(spark, "dq_retention_test", "dq_file", 13, TODAY)

    assert dropped == [date(2024, 1, 1), date(2025, 9, 8)]
    left = sorted(r[0] for r in spark.sql("SHOW PARTITIONS dq_retention_test.dq_file").collect())
    assert left == ["run_date=2025-09-09", "run_date=2026-10-01"]
