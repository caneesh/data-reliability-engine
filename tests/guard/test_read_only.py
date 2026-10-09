"""Guard: engine code writes only to the dq store (hard rule 1, spec section 9)."""

from __future__ import annotations

import pytest

from tests.conftest import SRC_ROOT
from tests.guard.scan import scan_text, scan_tree


def test_src_writes_only_to_dq() -> None:
    violations = scan_tree(SRC_ROOT)
    assert not violations, "Write statements outside the dq store:\n" + "\n".join(map(str, violations))


# The scanner itself must catch writes outside dq and let dq writes through.
@pytest.mark.parametrize(
    "snippet",
    [
        "INSERT INTO gold_db.member_coverage VALUES (1)",
        "insert overwrite table raw_db.enrollment select * from x",
        "MERGE INTO gold_db.member_coverage t USING s ON t.k = s.k",
        "UPDATE gold_db.member_coverage SET end_dt = NULL",
        "DELETE FROM curated_db.enrollment WHERE 1 = 1",
        "DROP TABLE IF EXISTS gold_db.member_coverage",
        "DROP DATABASE gold_db",
        "ALTER TABLE gold_db.member_coverage ADD COLUMNS (x INT)",
        "TRUNCATE TABLE gold_db.member_coverage",
        "CREATE TABLE gold_db.copy AS SELECT 1",
        "CREATE DATABASE other_db",
        "INSERT INTO dq_backup.dq_run VALUES (1)",
        "INSERT INTO {{ table }} SELECT 1",
        'spark.sql(f"INSERT INTO {table} SELECT 1")',
        'spark.sql("DELETE FROM " + table)',
        'df.write.saveAsTable("gold_db.member_coverage")',
        "df.write.insertInto(table_name)",
        "hdfs dfs -rm /data/landing/example_feed/file1",
        "hadoop fs -mv /data/landing/a /data/landing/b",
        'subprocess.run(["hdfs", "dfs", "-rm", path])',
        "INSERT INTO\n    gold_db.member_coverage\nVALUES (1)",
    ],
)
def test_scanner_flags_writes_outside_dq(snippet: str) -> None:
    assert scan_text(snippet), f"scanner missed: {snippet!r}"


@pytest.mark.parametrize(
    "snippet",
    [
        "INSERT INTO dq.dq_check_result PARTITION (run_date) SELECT * FROM v",
        "INSERT INTO TABLE `dq`.`dq_run` VALUES (1)",
        "INSERT INTO {{ dq_database }}.dq_check_result SELECT 1",
        'spark.sql(f"INSERT INTO {dq_database}.dq_run SELECT 1")',
        "CREATE TABLE IF NOT EXISTS dq.dq_run (run_id STRING) STORED AS ORC",
        "CREATE OR REPLACE VIEW dq.v_latest_result AS SELECT 1",
        "CREATE DATABASE IF NOT EXISTS dq",
        "ALTER TABLE dq.dq_file DROP IF EXISTS PARTITION (run_date = DATE'2020-01-01')",
        'df.write.insertInto("dq.dq_key_event")',
        "SELECT * FROM gold_db.member_coverage WHERE updated_flag = 1",
        "hdfs dfs -ls /data/landing/example_feed",
        "# Update the watermark after the run; delete nothing.",
    ],
)
def test_scanner_allows_dq_writes_and_reads(snippet: str) -> None:
    assert scan_text(snippet) == []
