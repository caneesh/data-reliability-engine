"""Q-3: DECIMAL canonical text. Spark's CAST(decimal AS STRING) keeps the scale and switches to
scientific notation for small values; a numeric canonical form (plain notation, no trailing
fractional zeros, no negative zero) built from the cast with string functions is exact and
scale-independent. (format_string('%f') is unusable: it receives Spark's Decimal, not a BigDecimal.) Assertions record observed behaviour (Spark 3.5.1)."""

from __future__ import annotations

from decimal import Decimal

import pytest


def numeric_text(expr: str, scale: int | None = None) -> str:
    """The proposed canonical text of a DECIMAL part: Spark's cast, with its scientific form expanded
    (Spark decimals have scale >= 0, so the cast's exponent is only ever negative), trailing fractional
    zeros and a bare point removed, and negative zero written 0. String functions only."""
    t = f"CAST({expr} AS STRING)"
    mant = f"substring_index({t}, 'E', 1)"
    exp = f"CAST(substring_index({t}, 'E', -1) AS INT)"
    neg = f"(substr({mant}, 1, 1) = '-')"
    digits = f"replace(replace({mant}, '-', ''), '.', '')"
    plain = (f"CASE WHEN instr({t}, 'E') = 0 THEN {t} ELSE concat(CASE WHEN {neg} THEN '-' ELSE '' END, '0.', "
             f"repeat('0', -{exp} - 1), {digits}) END")
    stripped = f"CASE WHEN instr({plain}, '.') > 0 THEN regexp_replace(regexp_replace({plain}, '0+$', ''), '[.]$', '') ELSE {plain} END"
    return f"CASE WHEN {stripped} = '-0' THEN '0' ELSE {stripped} END"


def py_numeric_text(d: Decimal) -> str:
    """The Python mirror: exact, plain, no trailing zeros, no negative zero (no context rounding)."""
    t = format(d, "f")
    if "." in t:
        t = t.rstrip("0").rstrip(".")
    return "0" if t in ("-0", "") else t


CASES = [("CAST(1 AS DECIMAL(10,2))", 2, "1"), ("CAST(1 AS DECIMAL(10,0))", 0, "1"),
         ("CAST(1 AS DECIMAL(20,10))", 10, "1"), ("CAST(-0.5 AS DECIMAL(10,2))", 2, "-0.5"),
         ("CAST(0.00000001 AS DECIMAL(20,10))", 10, "0.00000001"),
         ("CAST(-0.000 AS DECIMAL(10,3))", 3, "0"), ("CAST(0 AS DECIMAL(10,0))", 0, "0"),
         ("CAST(12345678901234567890.12 AS DECIMAL(38,2))", 2, "12345678901234567890.12"),
         ("CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))", 0, "99999999999999999999999999999999999999"),
         ("CAST('0.00000000000000000000000000000000000001' AS DECIMAL(38,38))", 38, "0.00000000000000000000000000000000000001"),
         ("CAST(10 AS DECIMAL(10,2))", 2, "10"), ("CAST(100 AS DECIMAL(10,0))", 0, "100")]


def test_format_string_cannot_format_a_spark_decimal(spark) -> None:
    with pytest.raises(Exception, match="Decimal"):
        spark.sql("SELECT format_string('%.2f', CAST(1 AS DECIMAL(10,2)))").collect()


def test_cast_keeps_scale_and_uses_scientific_notation(spark) -> None:
    r = spark.sql("SELECT CAST(CAST(1 AS DECIMAL(10,2)) AS STRING) a, CAST(CAST(1 AS DECIMAL(10,0)) AS STRING) b, "
                  "CAST(CAST(0.00000001 AS DECIMAL(20,10)) AS STRING) c").collect()[0]
    assert (r.a, r.b, r.c) == ("1.00", "1", "1.00E-8")


@pytest.mark.parametrize("expr,scale,want", CASES)
def test_numeric_text_is_plain_exact_and_scale_independent(spark, expr, scale, want) -> None:
    got = spark.sql(f"SELECT {numeric_text(expr, scale)} AS t").collect()[0].t
    assert got == want == py_numeric_text(Decimal(want))


def test_numeric_text_ignores_ansi_mode(spark) -> None:
    prior = spark.conf.get("spark.sql.ansi.enabled")
    try:
        for ansi in ("true", "false"):
            spark.conf.set("spark.sql.ansi.enabled", ansi)
            assert spark.sql(f"SELECT {numeric_text('CAST(1234.5 AS DECIMAL(10,2))', 2)} AS t").collect()[0].t == "1234.5"
    finally:
        spark.conf.set("spark.sql.ansi.enabled", prior)


def test_numeric_text_is_injective_within_a_column(spark) -> None:
    """Within one DECIMAL(p, s) column, distinct values give distinct texts (2,001 values)."""
    spark.range(-1000, 1001).selectExpr("CAST(id / 100000000 AS DECIMAL(20, 12)) AS d").createOrReplaceTempView("q3_vals")
    r = spark.sql(f"SELECT COUNT(DISTINCT d) AS n, COUNT(DISTINCT {numeric_text('d', 4)}) AS t FROM q3_vals").collect()[0]
    assert r.n == r.t == 2001  # includes values below 10^-6, which the cast writes in scientific form


def test_numeric_text_matches_python_on_generated_values(spark) -> None:
    import random
    rnd = random.Random(3)
    vals = [Decimal(rnd.randint(-10**15, 10**15)).scaleb(-rnd.randint(0, 18)) for _ in range(2000)]
    spark.createDataFrame([(str(v),) for v in vals], "v STRING").selectExpr("CAST(v AS DECIMAL(38, 18)) AS d"
                                                                            ).createOrReplaceTempView("q3_gen")
    got = [r.t for r in spark.sql(f"SELECT {numeric_text('d')} AS t FROM q3_gen").collect()]
    assert got == [py_numeric_text(v) for v in vals]
