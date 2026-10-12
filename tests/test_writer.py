"""store/writer.py: append reorders columns to the table's order before insertInto."""

from __future__ import annotations

from datetime import date

import pytest

from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.local_setup import create_database

DB = "dq_writer_test"


@pytest.fixture(scope="module")
def result_table(spark):
    create_database(spark, DB)
    spark.sql(
        f"CREATE TABLE {DB}.results (run_id STRING, check_id STRING, state STRING) "
        "PARTITIONED BY (run_date DATE) STORED AS ORC"
    )
    return f"{DB}.results"


def test_writer_exposes_only_append() -> None:
    assert writer.__all__ == ["append"]


def test_out_of_order_columns_land_in_the_right_columns(spark, result_table) -> None:
    # Same types in every column, so a positional insert would silently swap values.
    df = spark.createDataFrame(
        [("PASSED", date(2026, 1, 2), "T1_ZERO_ROWS", "run-1")],
        "state STRING, run_date DATE, check_id STRING, run_id STRING",
    )
    writer.append(df, DB, "results")
    row = spark.table(result_table).where("run_id = 'run-1'").collect()
    assert len(row) == 1
    assert (row[0].run_id, row[0].check_id, row[0].state, row[0].run_date) == (
        "run-1",
        "T1_ZERO_ROWS",
        "PASSED",
        date(2026, 1, 2),
    )


@pytest.mark.parametrize(
    "schema",
    [
        "run_id STRING, check_id STRING, run_date DATE",  # missing state
        "run_id STRING, check_id STRING, state STRING, extra STRING, run_date DATE",
    ],
)
def test_column_mismatch_refused(spark, result_table, schema: str) -> None:
    n = len(schema.split(","))
    values = tuple("x" if "DATE" not in col else date(2026, 1, 2) for col in schema.split(","))
    df = spark.createDataFrame([values[:n]], schema)
    with pytest.raises(ValueError, match="columns do not match"):
        writer.append(df, DB, "results")


@pytest.mark.parametrize(("database", "table"), [("dq.x", "results"), (DB, "results; DROP TABLE y")])
def test_writer_rejects_non_identifiers(spark, database: str, table: str) -> None:
    df = spark.createDataFrame([("x",)], "run_id STRING")
    with pytest.raises(ValueError, match="plain identifier"):
        writer.append(df, database, table)
