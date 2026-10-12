"""store/ddl.sql matches spec section 5, is idempotent, and uses ORC partitioned by run_date."""

from __future__ import annotations

import re

import pytest

from hcsc.datalake.dre.store.names import DQ_TABLES
from hcsc.datalake.dre.store.install import apply_ddl
from hcsc.datalake.dre.store.schema import VIEWS, render_ddl
from tests.conftest import REPO_ROOT


def spec_tables() -> dict[str, list[tuple[str, str]]]:
    """Columns per table, from the DDL in docs/spec.md section 5 (partition column last)."""
    spec = (REPO_ROOT / "docs" / "spec.md").read_text(encoding="utf-8")
    section = spec[spec.index("## 5. Data model"):spec.index("## 6.")]
    sql = re.sub(r"--[^\n]*", "", section)
    tables = {}
    for name, body, part_col, part_type in re.findall(
        r"CREATE TABLE dq\.(\w+) \((.*?)\) PARTITIONED BY \((\w+) (\w+)\)", sql, re.S
    ):
        cols = re.findall(r"(\w+) ([A-Z]+(?:<[^>]*>)?)", body)
        tables[name] = [(c, t.lower()) for c, t in cols] + [(part_col, part_type.lower())]
    return tables


def test_spec_lists_every_dq_table() -> None:
    assert sorted(spec_tables()) == sorted(DQ_TABLES)


@pytest.mark.parametrize("table", DQ_TABLES)
def test_table_matches_spec(spark, store, table: str) -> None:
    schema = spark.table(f"{store}.{table}").schema
    actual = [(f.name, f.dataType.simpleString().replace(" ", "")) for f in schema.fields]
    assert actual == spec_tables()[table]


@pytest.mark.parametrize("table", DQ_TABLES)
def test_table_is_orc_partitioned_by_run_date(spark, store, table: str) -> None:
    info = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE EXTENDED {store}.{table}").collect()}
    assert "orc" in info.get("Serde Library", "").lower()
    partitions = spark.sql(f"DESCRIBE TABLE {store}.{table}").collect()
    marker = [r.col_name for r in partitions].index("# Partition Information")
    assert [r.col_name for r in partitions[marker + 2:]] == ["run_date"]


def test_views_exist(spark, store) -> None:
    names = {r.viewName for r in spark.sql(f"SHOW VIEWS IN {store}").collect()}
    assert set(VIEWS) <= names


def test_apply_ddl_is_idempotent_and_keeps_rows(spark, store) -> None:
    spark.sql(f"INSERT INTO {store}.dq_file PARTITION (run_date = DATE'2026-01-01') "
              "VALUES ('/data/landing/example_feed/f1', 'example_realtime', NULL, 1, NULL, NULL, 'r')")
    apply_ddl(spark, store)
    apply_ddl(spark, store)
    assert spark.table(f"{store}.dq_file").count() == 1


def test_render_rejects_non_identifier() -> None:
    with pytest.raises(ValueError, match="plain identifier"):
        render_ddl("dq; DROP TABLE x")


def test_rendered_ddl_targets_only_the_dq_database() -> None:
    for statement in render_ddl("dq_custom"):
        target = re.search(r"(?:TABLE|VIEW) IF NOT EXISTS (\S+)", statement).group(1)
        assert target.startswith("dq_custom."), statement
