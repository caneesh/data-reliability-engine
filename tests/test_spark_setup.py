"""Smoke test for the local Spark setup used by every later test."""

from __future__ import annotations


def test_hive_orc_partitioned_table_round_trip(spark) -> None:
    # The dq store (spec section 5) needs Hive DDL: ORC tables partitioned by run_date.
    spark.sql("CREATE DATABASE IF NOT EXISTS dq")
    spark.sql(
        "CREATE TABLE dq.smoke (run_id STRING, population BIGINT) "
        "PARTITIONED BY (run_date DATE) STORED AS ORC"
    )
    spark.sql(
        "INSERT INTO dq.smoke PARTITION (run_date = DATE'2026-01-01') "
        "VALUES ('r1', 3), ('r2', 0)"
    )
    rows = spark.sql(
        "SELECT run_id, population FROM dq.smoke WHERE run_date = DATE'2026-01-01' ORDER BY run_id"
    ).collect()
    assert [(r.run_id, r.population) for r in rows] == [("r1", 3), ("r2", 0)]
    spark.sql("DROP TABLE dq.smoke")


def test_spark_times_read_back_as_utc_on_a_non_utc_host(spark) -> None:
    """PySpark returns TIMESTAMP values in the driver's local time zone; get_spark runs the
    process in UTC, so a value collected from Spark is the UTC value (review finding 1)."""
    import os
    import time
    from datetime import datetime, timezone

    from hcsc.datalake.dre.checks.times import as_utc
    from hcsc.datalake.dre.session import get_spark

    sql = "SELECT TIMESTAMP '2026-01-01 00:00:00' AS t"
    saved = os.environ.get("TZ")
    try:
        os.environ["TZ"] = "America/Chicago"
        time.tzset()
        shifted = as_utc(spark.sql(sql).collect()[0].t)
        assert shifted == datetime(2025, 12, 31, 18, 0, tzinfo=timezone.utc)  # the bug, on a Chicago host
        session = get_spark()
        assert as_utc(session.sql(sql).collect()[0].t) == datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    finally:
        if saved is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = saved
        time.tzset()
