"""Synthetic layer tables with the quirks of the real ones (spec section 9).

Gold subscriber ids are zero-padded; curated ones are not. All values are made up.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

GOLD_LOAD_FORMAT = "%Y-%m-%d %H:%M:%S:000000"  # gld_lcts, minute-granular, written in Chicago time
CHICAGO = ZoneInfo("America/Chicago")


def gold_load_time(hours_ago: float, now: datetime | None = None) -> str:
    """gld_lcts for a load hours_ago before now, as the gold layer writes it (local time)."""
    moment = (now or datetime.now(timezone.utc)) - timedelta(hours=hours_ago)
    return moment.astimezone(CHICAGO).replace(second=0).strftime(GOLD_LOAD_FORMAT)


def curated_load_time(hours_ago: float, now: datetime | None = None) -> datetime:
    """A curated TIMESTAMP hours_ago before now, as Chicago wall time."""
    moment = (now or datetime.now(timezone.utc)) - timedelta(hours=hours_ago)
    return moment.astimezone(CHICAGO).replace(tzinfo=None)

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
             end: str = "9999-12-31", source: str = "SRC_A", loaded: str = "2026-01-02 10:15:00:000000") -> dict[str, Any]:
    return {"src_sys_nm": source, "sub_id": sub_id, "mem_nbr": mem_nbr, "mbr_mbrshp_covrg_eff_dt": eff,
            "covrg_agrmt_id": agreement, "mbr_mbrshp_covrg_end_dt": end,
            "src_lcts": datetime(2026, 1, 2, 9, 0), "gld_lcts": loaded}


def curated_row(sub_id: str = "123401", mem_nbr: str = "01", eff: str = "2026-01-01", agreement: str = "AGR-A",
                end: str = "9999-12-31", updated: datetime = datetime(2026, 1, 2, 8, 0)) -> dict[str, Any]:
    return {"subscriberidnumber": sub_id, "membernumber": mem_nbr, "effectivedate": eff,
            "qualifiedhealthplanid": agreement, "enddate": end, "sourcelastupdatets": updated}


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
