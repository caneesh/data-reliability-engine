"""Q-2: CHAR(n) behaviour across a Spark datasource table and a Hive SerDe table (ORC), Spark 3.5.1.
Assertions record observed behaviour."""

from __future__ import annotations

import pytest

DB = "q2_char"


@pytest.fixture(scope="module")
def tables(spark):
    spark.sql(f"DROP DATABASE IF EXISTS {DB} CASCADE")
    spark.sql(f"CREATE DATABASE {DB}")
    spark.sql(f"CREATE TABLE {DB}.hive_orc (k CHAR(3), v VARCHAR(5), s STRING) STORED AS ORC")       # Hive SerDe
    spark.sql(f"CREATE TABLE {DB}.ds_parquet (k CHAR(3), v VARCHAR(5), s STRING) USING parquet")     # datasource
    spark.sql(f"CREATE TABLE {DB}.hive_orc5 (k CHAR(5)) STORED AS ORC")
    for t in ("hive_orc", "ds_parquet"):
        spark.sql(f"INSERT INTO {DB}.{t} VALUES ('a', 'a ', 'a '), ('a ', 'a', 'a'), (' a', ' a', ' a')")
    spark.sql(f"INSERT INTO {DB}.hive_orc5 VALUES ('a')")
    yield
    spark.sql(f"DROP DATABASE {DB} CASCADE")


@pytest.mark.parametrize("table", ["hive_orc", "ds_parquet"])
def test_char_values_are_padded_on_read_and_trailing_spaces_are_not_kept(spark, tables, table) -> None:
    """'a' and 'a ' written to CHAR(3) read back identically as 'a  ': the type itself keeps no
    trailing-space information, so removing the pad loses nothing. A leading space is kept."""
    rows = spark.sql(f"SELECT k, octet_length(k) AS n FROM {DB}.{table}").collect()
    assert sorted((r.k, r.n) for r in rows) == [(" a ", 3), ("a  ", 3), ("a  ", 3)]


@pytest.mark.parametrize("table", ["hive_orc", "ds_parquet"])
def test_varchar_and_string_keep_trailing_spaces(spark, tables, table) -> None:
    rows = spark.sql(f"SELECT v, s FROM {DB}.{table}").collect()
    assert sorted(r.v for r in rows) == [" a", "a", "a "] and sorted(r.s for r in rows) == [" a", "a", "a "]


def test_char_of_different_lengths_render_differently_unless_the_pad_is_removed(spark, tables) -> None:
    """CHAR(3) 'a' and CHAR(5) 'a' are the same business value; cast to STRING they differ ('a  ' vs
    'a    '), so encoding 1 (and LPB over the cast) would call them different keys: a false FAIL.
    Removing only trailing U+0020 (rtrim with an explicit ' ') makes them equal and keeps ' a'."""
    r = spark.sql(f"""SELECT (SELECT CAST(k AS STRING) FROM {DB}.hive_orc WHERE k = 'a' LIMIT 1) AS c3,
                             (SELECT CAST(k AS STRING) FROM {DB}.hive_orc5 LIMIT 1) AS c5,
                             (SELECT rtrim(' ', CAST(k AS STRING)) FROM {DB}.hive_orc WHERE k = 'a' LIMIT 1) AS t3,
                             (SELECT rtrim(' ', CAST(k AS STRING)) FROM {DB}.hive_orc5 LIMIT 1) AS t5,
                             (SELECT rtrim(' ', CAST(k AS STRING)) FROM {DB}.hive_orc WHERE k = ' a' LIMIT 1) AS lead""").collect()[0]
    assert (r.c3, r.c5) == ("a  ", "a    ")
    assert r.t3 == r.t5 == "a" and r.lead == " a"


def test_rtrim_with_an_explicit_space_removes_only_u0020(spark) -> None:
    """Tabs, NBSP and other whitespace are data, not pad: kept."""
    r = spark.sql("SELECT rtrim(' ', 'a\t  ') AS tab, rtrim(' ', concat('a', char(160), ' ')) AS nbsp").collect()[0]
    assert r.tab == "a\t" and r.nbsp == "a "
