"""Partition pruning (spec section 3, partition_column + partition_format): a windowed scan gets a
second predicate on the partition column, listing the partitions the window can touch, beside the
parsed load-time predicate, so Spark prunes the scan; without the format it reads every partition
and still counts right."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.partitions import granule, partition_filter, partition_values, strftime_pattern
from hcsc.datalake.dre.checks.tier1.key_nulls import KeyNulls
from hcsc.datalake.dre.config.models import Dataset, TimeColumn
from tests.fixtures.layers import GOLD_COLUMNS, gold_row
from tests.fixtures.settings import SETTINGS

UTC = timezone.utc
EVENT = Event("gold", datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 1, 16, 12, tzinfo=UTC))
LOAD_TIME = TimeColumn(column="gld_lcts", format="yyyy-MM-dd HH:mm:ss:SSSSSS", granularity="minute",
                       timezone="America/Chicago")


def gold(table: str, **extra) -> Dataset:
    return Dataset(dataset="gold", table=table, layer="GOLD", load_time=LOAD_TIME, key=["sub_id", "mem_nbr"], **extra)


def test_spark_patterns_translate_to_strftime_or_not_at_all() -> None:
    assert strftime_pattern("yyyy-MM-dd") == "%Y-%m-%d"
    assert strftime_pattern("yyyyMMddHH") == "%Y%m%d%H"
    assert strftime_pattern("MM/dd/yyyy") == "%m/%d/%Y"
    assert strftime_pattern("'dt='yyyy-MM-dd") == "dt=%Y-%m-%d"
    assert strftime_pattern("yyyy-'W'ww") is None          # week numbers: not translated
    assert (granule("yyyy-MM-dd"), granule("yyyyMMddHH"), granule("yyyy-MM")) == (
        timedelta(days=1), timedelta(hours=1), None)


def test_partition_values_cover_the_window_in_the_load_time_zone_widened_by_a_granule() -> None:
    ds = gold("x.y", partition_column="load_dt", partition_format="yyyy-MM-dd")
    # 12:00Z on the 15th to 12:00Z on the 16th is the 15th and 16th in Chicago; one day either side.
    assert partition_values(ds, EVENT.window_start, EVENT.window_end, "UTC") == [
        "2026-01-14", "2026-01-15", "2026-01-16", "2026-01-17"]
    assert partition_filter(ds, EVENT.window_start, EVENT.window_end, "UTC") == (
        "load_dt IN ('2026-01-14', '2026-01-15', '2026-01-16', '2026-01-17')")
    hourly = gold("x.y", partition_column="load_hr", partition_format="yyyyMMddHH")
    values = partition_values(hourly, EVENT.window_start, EVENT.window_end, "UTC")
    assert values[0] == "2026011505" and values[-1] == "2026011607" and len(values) == 27  # 24 + 1 + 1 either side
    assert partition_values(gold("x.y", partition_column="load_dt"), EVENT.window_start, EVENT.window_end, "UTC") is None
    assert partition_values(gold("x.y"), EVENT.window_start, EVENT.window_end, "UTC") is None
    long_span = partition_values(hourly, EVENT.window_start, EVENT.window_start + timedelta(days=60), "UTC")
    assert long_span is None  # more than 1000 partitions: the parsed-time predicate alone


def _partitioned_gold(spark, table: str) -> None:
    """A gold table partitioned by load_dt (the Chicago date of gld_lcts), three days of rows."""
    database = table.split(".")[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    columns = ", ".join(f"{n} {k}" for n, k in GOLD_COLUMNS)
    spark.sql(f"CREATE TABLE {table} ({columns}) PARTITIONED BY (load_dt STRING) STORED AS ORC")
    rows = []
    for day, nulls in (("2026-01-14", 1), ("2026-01-15", 2), ("2026-01-16", 3)):
        loaded = f"{day} 13:00:00:000000"
        rows += [gold_row(loaded=loaded)] + [gold_row(sub_id=None, loaded=loaded) for _ in range(nulls)]
        for r in rows[-(nulls + 1):]:
            r["load_dt"] = day
    data = [tuple(r.get(n) for n, _ in GOLD_COLUMNS) + (r["load_dt"],) for r in rows]
    schema = ", ".join(f"{n} {k}" for n, k in GOLD_COLUMNS) + ", load_dt STRING"
    spark.createDataFrame(data, schema).write.insertInto(table)


def test_a_windowed_check_prunes_to_the_window_s_partitions_and_counts_the_same(spark, monkeypatch) -> None:
    """With partition_format the plan prunes on load_dt; without it every partition is read.
    The counts are identical: the window is [15th 12:00Z, 16th 12:00Z) = 13:00 Chicago on the 15th
    only (1 row + 2 nulls), the day before and after fall outside."""
    _partitioned_gold(spark, "prune.gold")
    seen: list[str] = []
    real_sql = spark.sql
    monkeypatch.setattr(spark, "sql", lambda text: seen.append(text) or real_sql(text))

    pruned = gold("prune.gold", partition_column="load_dt", partition_format="yyyy-MM-dd")
    [r] = run_check(KeyNulls(), CheckContext(spark, pruned, None, SETTINGS, "UTC"), EVENT)[0]
    assert (r.state, r.population, r.violations) == ("FAILED", 3, 2)
    rendered = [s for s in seen if "prune.gold" in s][-1]
    assert "load_dt IN ('2026-01-14', '2026-01-15', '2026-01-16', '2026-01-17')" in rendered
    plan = real_sql("EXPLAIN FORMATTED " + rendered).collect()[0][0]
    filters = plan.split("PartitionFilters: [", 1)[1].split("]", 1)[0]
    assert "load_dt" in filters

    unpruned = gold("prune.gold", partition_column="load_dt")  # no format: nothing to list
    seen.clear()
    [r] = run_check(KeyNulls(), CheckContext(spark, unpruned, None, SETTINGS, "UTC"), EVENT)[0]
    assert (r.state, r.population, r.violations) == ("FAILED", 3, 2)
    rendered = [s for s in seen if "prune.gold" in s][-1]
    assert "load_dt IN" not in rendered
    plan = real_sql("EXPLAIN FORMATTED " + rendered).collect()[0][0]
    assert "PartitionFilters: [load_dt" not in plan  # nothing to prune on
