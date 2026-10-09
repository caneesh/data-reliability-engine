"""Guard: engine code is read-only outside dq and append-only inside it.

Hard rules 1 and 5, spec section 9, and the guard rules in docs/decisions.md.
"""

from __future__ import annotations

import pytest

from tests.conftest import SRC_ROOT
from tests.guard.scan import DDL_FILE, LOCAL_SETUP_MODULE, RETENTION_MODULE, WRITER_MODULE, scan_text, scan_tree

RETENTION = f"src/{RETENTION_MODULE}"
WRITER = f"src/{WRITER_MODULE}"
LOCAL_SETUP = f"src/{LOCAL_SETUP_MODULE}"
DDL = f"src/{DDL_FILE}"
OTHER = "src/hcsc/datalake/dre/checks/base.py"


def test_src_is_read_only_and_append_only() -> None:
    violations = scan_tree(SRC_ROOT)
    assert not violations, "Guard violations:\n" + "\n".join(map(str, violations))


# (rule, snippet, path): the scanner must flag each of these.
CAUGHT = [
    # INSERT INTO only into dq
    ("insert-outside-dq", "INSERT INTO gold_db.member_coverage VALUES (1)", WRITER),
    ("insert-near-miss-db", "INSERT INTO dq_backup.dq_run VALUES (1)", WRITER),
    ("insert-unreadable-jinja", "INSERT INTO {{ table }} SELECT 1", WRITER),
    ("insert-unreadable-fstring", 'spark.sql(f"INSERT INTO {table} SELECT 1")', WRITER),
    ("insert-multiline", "INSERT INTO\n    gold_db.member_coverage\nVALUES (1)", WRITER),
    ("insertInto-outside-dq", "df.write.insertInto(table_name)", WRITER),
    # banned everywhere, dq included
    ("insert-overwrite-dq", "INSERT OVERWRITE TABLE dq.dq_check_result SELECT * FROM v", WRITER),
    ("merge-dq", "MERGE INTO dq.dq_run t USING s ON t.run_id = s.run_id", WRITER),
    ("update-dq", "UPDATE dq.dq_run SET status = 'COMPLETED'", WRITER),
    ("delete-dq", "DELETE FROM dq.dq_key_event WHERE run_id = 'r1'", WRITER),
    ("truncate-dq", "TRUNCATE TABLE dq.dq_file", WRITER),
    ("delete-outside", 'spark.sql("DELETE FROM " + table)', WRITER),
    ("drop-table-dq", "DROP TABLE IF EXISTS dq.dq_run", WRITER),
    ("drop-database", "DROP DATABASE gold_db", WRITER),
    ("overwrite-mode", 'df.write.mode("overwrite").insertInto("dq.dq_run")', WRITER),
    ("overwrite-kwarg", 'df.write.insertInto("dq.dq_run", overwrite=True)', WRITER),
    ("saveAsTable-dq", 'df.write.saveAsTable("dq.dq_run")', WRITER),
    # DDL on dq only
    ("create-table-outside", "CREATE TABLE gold_db.copy AS SELECT 1", DDL),
    ("create-view-outside", "CREATE OR REPLACE VIEW gold_db.v AS SELECT 1", DDL),
    ("create-table-dq-outside-install", "CREATE TABLE IF NOT EXISTS dq.dq_run (run_id STRING) STORED AS ORC", WRITER),
    ("create-view-dq-outside-install", "CREATE VIEW IF NOT EXISTS {{ dq_database }}.v_latest_run AS SELECT 1", OTHER),
    ("create-database-outside", "CREATE DATABASE other_db", LOCAL_SETUP),
    ("create-database-dq-not-setup", "CREATE DATABASE IF NOT EXISTS dq", WRITER),
    # DataFrame write APIs only in store/writer.py
    ("write-api-outside-writer", 'df.write.mode("append").insertInto("dq.dq_run")', OTHER),
    ("write-split-chain-outside-writer", "w = df.write\nw.save(p)", OTHER),
    ("writeTo-outside-writer", 'df.writeTo("dq.dq_run").append()', OTHER),
    ("insertInto-outside-writer", 'frame_writer.insertInto("dq.dq_run")', OTHER),
    ("file-write-outside-writer", "handle.write(text)", OTHER),
    ("writeTo-outside-dq", 'df.writeTo("gold_db.member_coverage").append()', WRITER),
    ("writeTo-overwrite-dq", 'df.writeTo("dq.dq_run").overwritePartitions()', WRITER),
    ("writeTo-createOrReplace-dq", 'df.writeTo("dq.dq_run").using("orc").createOrReplace()', WRITER),
    ("alter-add-columns-dq", "ALTER TABLE dq.dq_run ADD COLUMNS (x INT)", WRITER),
    ("alter-outside", "ALTER TABLE gold_db.member_coverage SET TBLPROPERTIES ('a'='b')", WRITER),
    # DROP PARTITION only in retention, only on dq
    ("drop-partition-not-retention", "ALTER TABLE dq.dq_run DROP IF EXISTS PARTITION (run_date = DATE'2019-01-01')", WRITER),
    ("drop-partition-outside-dq", "ALTER TABLE gold_db.member_coverage DROP PARTITION (load_dt = '2019-01-01')", RETENTION),
    ("alter-other-in-retention", "ALTER TABLE dq.dq_run ADD PARTITION (run_date = DATE'2026-01-01')", RETENTION),
    # DataFrameWriter path writes
    ("path-save", 'df.write.mode("append").format("orc").save("/data/landing/example_feed")', WRITER),
    ("path-orc", "df.write.orc(path)", WRITER),
    ("path-parquet", 'df.write.partitionBy("run_date").parquet(out_dir)', WRITER),
    ("path-csv-multiline", 'df.write \\\n    .option("header", True) \\\n    .csv("/data/out")', WRITER),
    ("path-save-no-arg", 'df.write.format("orc").option("path", p).save()', WRITER),
    # Hadoop FileSystem API
    ("fs-delete", "fs.delete(hadoop_path, True)", WRITER),
    ("fs-rename", "fs.rename(src_path, dst_path)", WRITER),
    ("fs-mkdirs", 'fs.mkdirs(jvm.org.apache.hadoop.fs.Path("/data/new"))', WRITER),
    # os / shutil
    ("os-remove", "os.remove(local_file)", WRITER),
    ("shutil-rmtree", "shutil.rmtree(landing_dir)", WRITER),
    ("shutil-move", "shutil.move(src, dst)", WRITER),
    # hdfs CLI
    ("hdfs-rm", "hdfs dfs -rm /data/landing/example_feed/file1", WRITER),
    ("hadoop-mv", "hadoop fs -mv /data/landing/a /data/landing/b", WRITER),
    ("hdfs-rm-argv", 'subprocess.run(["hdfs", "dfs", "-rm", path])', WRITER),
]

# (rule, snippet, path): the scanner must let each of these through.
ALLOWED = [
    ("insert-dq", "INSERT INTO dq.dq_check_result PARTITION (run_date) SELECT * FROM v", WRITER),
    ("insert-dq-backticks", "INSERT INTO TABLE `dq`.`dq_run` VALUES (1)", WRITER),
    ("insert-dq-jinja", "INSERT INTO {{ dq_database }}.dq_check_result SELECT 1", WRITER),
    ("insert-dq-fstring", 'spark.sql(f"INSERT INTO {dq_database}.dq_run SELECT 1")', WRITER),
    ("insertInto-dq-append", 'df.write.mode("append").insertInto("dq.dq_key_event")', WRITER),
    ("create-table-dq-install", "CREATE TABLE IF NOT EXISTS {{ dq_database }}.dq_run (run_id STRING) STORED AS ORC", DDL),
    ("create-view-dq-install", "CREATE VIEW IF NOT EXISTS {{ dq_database }}.v_latest_result AS SELECT 1", DDL),
    ("create-database-dq-setup", 'spark.sql(f"CREATE DATABASE IF NOT EXISTS {dq_database}")', LOCAL_SETUP),
    ("write-api-in-writer", 'df.select(*cols).write.insertInto(f"{dq_database}.{table}")', WRITER),
    ("writeTo-append-in-writer", 'df.writeTo("dq.dq_run").append()', WRITER),
    ("write-text-not-write-api", "path.write_text(body)", OTHER),
    ("drop-partition-retention", "ALTER TABLE dq.dq_file DROP IF EXISTS PARTITION (run_date = DATE'2019-01-01')", RETENTION),
    ("drop-partition-retention-fstring", 'f"ALTER TABLE {dq_database}.{table} DROP IF EXISTS PARTITION "', RETENTION),
    ("read-select", "SELECT * FROM gold_db.member_coverage WHERE updated_flag = 1", WRITER),
    ("read-orc", "spark.read.orc(path)", WRITER),
    ("read-parquet-chain", 'spark.read.option("mergeSchema", True).parquet("/data/landing/example_feed")', WRITER),
    ("read-hdfs-ls", "hdfs dfs -ls /data/landing/example_feed", WRITER),
    ("read-fs-list", "fs.listStatus(hadoop_path)", WRITER),
    ("read-os", "os.listdir(landing_dir); os.path.exists(p)", WRITER),
    ("prose", "# Update the watermark after the run; delete nothing.", WRITER),
]


@pytest.mark.parametrize(("rule", "snippet", "path"), CAUGHT, ids=[c[0] for c in CAUGHT])
def test_scanner_catches(rule: str, snippet: str, path: str) -> None:
    assert scan_text(snippet, path), f"{rule}: scanner missed {snippet!r}"


@pytest.mark.parametrize(("rule", "snippet", "path"), ALLOWED, ids=[a[0] for a in ALLOWED])
def test_scanner_allows(rule: str, snippet: str, path: str) -> None:
    assert scan_text(snippet, path) == [], f"{rule}: false positive"
