"""Q-6 / NF-1: the TIE_RESOLVED_BY_RULE evidence counts distinct owned-value combinations among tied rows
with concat_ws('|', coalesce(v, '<null>')) (checks/sql/cause_key_ties.sql.j2:27). Distinct combinations
collide; causes/merge.py:202 confirms the cause only when variants > 1, so a collision turns a real
tie into RULED_OUT. Reproduced on the expression itself (no engine code changed)."""

from __future__ import annotations

EXPR = "COUNT(DISTINCT concat_ws('|', coalesce(a, '<null>'), coalesce(b, '<null>')))"


def variants(spark, rows) -> int:
    spark.createDataFrame(rows, "a STRING, b STRING").createOrReplaceTempView("nf1")
    return spark.sql(f"SELECT {EXPR} AS n FROM nf1").collect()[0].n


def test_a_separator_inside_a_value_merges_two_combinations(spark) -> None:
    assert variants(spark, [("x|y", "z"), ("x", "y|z")]) == 1


def test_null_and_the_text_null_merge(spark) -> None:
    assert variants(spark, [(None, "z"), ("<null>", "z")]) == 1


def test_ordinary_values_are_counted_correctly(spark) -> None:
    assert variants(spark, [("open", "2026-12-31"), ("term", "2026-06-30")]) == 2


def test_a_length_prefixed_count_does_not_merge(spark) -> None:
    lp = ("COUNT(DISTINCT concat(CASE WHEN a IS NULL THEN '~;' ELSE concat(octet_length(a), ':', a, ';') END, "
          "CASE WHEN b IS NULL THEN '~;' ELSE concat(octet_length(b), ':', b, ';') END))")
    spark.createDataFrame([("x|y", "z"), ("x", "y|z"), (None, "q"), ("<null>", "q")], "a STRING, b STRING"
                          ).createOrReplaceTempView("nf1b")
    assert spark.sql(f"SELECT {lp} AS n FROM nf1b").collect()[0].n == 4
