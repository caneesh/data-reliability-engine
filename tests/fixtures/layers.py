"""Synthetic layer tables with the quirks of the real ones (spec section 9).

Gold subscriber ids are zero-padded; curated ones are not. All values are made up.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

GOLD_COLUMNS = [
    ("src_sys_nm", "STRING"), ("sub_id", "STRING"), ("mem_nbr", "STRING"),
    ("mbr_mbrshp_covrg_eff_dt", "STRING"), ("covrg_agrmt_id", "STRING"),
    ("mbr_mbrshp_covrg_end_dt", "STRING"), ("src_lcts", "TIMESTAMP"), ("gld_lcts", "STRING"),
]
CURATED_COLUMNS = [
    ("subscriberidnumber", "STRING"), ("membernumber", "STRING"), ("effectivedate", "STRING"),
    ("qualifiedhealthplanid", "STRING"), ("enddate", "STRING"), ("sourcelastupdatets", "TIMESTAMP"),
]


def gold_row(sub_id: str = "000123401", mem_nbr: str = "01", eff: str = "2026-01-01", agreement: str = "AGR-A",
             end: str = "9999-12-31", source: str = "SRC_A") -> dict[str, Any]:
    return {"src_sys_nm": source, "sub_id": sub_id, "mem_nbr": mem_nbr, "mbr_mbrshp_covrg_eff_dt": eff,
            "covrg_agrmt_id": agreement, "mbr_mbrshp_covrg_end_dt": end,
            "src_lcts": datetime(2026, 1, 2, 9, 0), "gld_lcts": "2026-01-02 10:15:00:000000"}


def curated_row(sub_id: str = "123401", mem_nbr: str = "01", eff: str = "2026-01-01", agreement: str = "AGR-A",
                end: str = "9999-12-31") -> dict[str, Any]:
    return {"subscriberidnumber": sub_id, "membernumber": mem_nbr, "effectivedate": eff,
            "qualifiedhealthplanid": agreement, "enddate": end, "sourcelastupdatets": datetime(2026, 1, 2, 8, 0)}


def create_table(spark, table: str, columns: list[tuple[str, str]], rows: list[dict[str, Any]] = (),
                 rename: dict[str, str] | None = None) -> None:
    """(Re)create an ORC table with these columns, optionally renaming some, and insert rows."""
    rename = rename or {}
    cols = [(rename.get(name, name), kind) for name, kind in columns]
    database = table.split(".")[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {table} ({', '.join(f'{n} {k}' for n, k in cols)}) STORED AS ORC")
    if rows:
        data = [tuple(row.get(name) for name, _ in columns) for row in rows]
        schema = ", ".join(f"{n} {k}" for n, k in cols)
        spark.createDataFrame(data, schema).write.insertInto(table)
