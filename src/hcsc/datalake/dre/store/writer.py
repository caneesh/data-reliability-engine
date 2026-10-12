"""Append-only writes to the dq store.

`append` is the only public function and the only DataFrame write path in the
engine (guard tests allow `.write` and `insertInto` in this module alone).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from hcsc.datalake.dre.store.names import validate_dq_database, validate_identifier

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = ["append"]


def append(df: DataFrame, dq_database: str, table: str) -> None:
    """Append df to <dq_database>.<table>.

    insertInto matches columns by position, so the frame is first reordered to
    the table's column order (data columns, then partition columns). The frame
    must carry exactly the table's columns.
    """
    validate_dq_database(dq_database)
    validate_identifier(table, "dq table name")
    spark = df.sparkSession
    table_columns = spark.table(f"{dq_database}.{table}").columns
    have = {c.lower() for c in df.columns}
    want = {c.lower() for c in table_columns}
    missing = [c for c in table_columns if c.lower() not in have]
    extra = [c for c in df.columns if c.lower() not in want]
    if missing or extra:
        raise ValueError(
            f"columns do not match {dq_database}.{table}: missing {missing}, unexpected {extra}"
        )
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    df.select(*table_columns).write.insertInto(f"{dq_database}.{table}")
