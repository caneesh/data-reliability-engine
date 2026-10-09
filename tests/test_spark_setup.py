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
