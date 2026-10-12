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

# Synthetic raw layer (test-only columns): one raw table holds two feeds, told apart by file name.
RAW_COLUMNS = [
    ("subscriberidnumber", "STRING"), ("membernumber", "STRING"), ("effectivedate", "STRING"),
    ("src_file_nm", "STRING"), ("file_date", "STRING"),
]


def raw_row(file_name: str, sub_id: str = "123401", eff: str = "01/01/2026") -> dict[str, Any]:
    """Raw dates are MM/DD/YYYY."""
    return {"subscriberidnumber": sub_id, "membernumber": "01", "effectivedate": eff,
            "src_file_nm": file_name, "file_date": "01/15/2026"}


def raw_dataset_yaml(table: str) -> str:
    """A synthetic raw dataset for tests (the sample config has none until its columns are confirmed)."""
    return (
        "dataset: example_raw_enrollment\n"
        f"table: {table}\n"
        "layer: RAW\n"
        "key: [subscriberidnumber, membernumber, effectivedate]\n"
        "file_name_column: src_file_nm\n"
        "feed_filter: \"src_file_nm LIKE 'rt_%'\"   # real-time files; batch files share the table\n"
    )


def gold_row(sub_id: str = "000123401", mem_nbr: str = "01", eff: str = "2026-01-01", agreement: str = "AGR-A",
             end: str = "9999-12-31", source: str = "SRC_A", loaded: str = "2026-01-02 10:15:00:000000",
             record: datetime = datetime(2026, 1, 2, 9, 0)) -> dict[str, Any]:
    """record: src_lcts, the source version (Chicago wall time); loaded: gld_lcts as gold writes it."""
    return {"src_sys_nm": source, "sub_id": sub_id, "mem_nbr": mem_nbr, "mbr_mbrshp_covrg_eff_dt": eff,
            "covrg_agrmt_id": agreement, "mbr_mbrshp_covrg_end_dt": end,
            "src_lcts": record, "gld_lcts": loaded}


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


# Second synthetic feed: a monthly provider roster (CSV, flat folder, UTC, single-column key).
PROVIDER_RAW_COLUMNS = [
    ("provider_id", "STRING"), ("provider_name", "STRING"), ("specialty", "STRING"),
    ("roster_effective_ts", "TIMESTAMP"), ("loaded_at", "TIMESTAMP"), ("source_file", "STRING"),
]
PROVIDER_GOLD_COLUMNS = [
    ("provider_id", "STRING"), ("provider_name", "STRING"), ("specialty", "STRING"),
    ("roster_effective_ts", "TIMESTAMP"), ("merged_at", "TIMESTAMP"),
]


def provider_raw_row(provider_id: str, loaded_at: datetime, source_file: str = "roster_2026_01.csv") -> dict[str, Any]:
    """UTC times (naive values are UTC in the test session)."""
    return {"provider_id": provider_id, "provider_name": f"Provider {provider_id}", "specialty": "general",
            "roster_effective_ts": (loaded_at - timedelta(days=1)).replace(tzinfo=None),
            "loaded_at": loaded_at.replace(tzinfo=None),
            "source_file": source_file}


def provider_gold_row(provider_id: str, merged_at: datetime, effective: datetime | None = None) -> dict[str, Any]:
    """effective: the roster version merged (the raw row's roster_effective_ts); default two days before the merge."""
    effective = effective if effective is not None else merged_at - timedelta(days=2)
    return {"provider_id": provider_id, "provider_name": f"Provider {provider_id}", "specialty": "general",
            "roster_effective_ts": effective.replace(tzinfo=None),
            "merged_at": merged_at.replace(tzinfo=None)}
