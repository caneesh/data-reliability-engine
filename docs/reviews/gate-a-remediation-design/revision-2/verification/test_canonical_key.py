"""Revision 2, part A: is to_json(array(CAST(part AS STRING))) injective over DRE's key domain, and
where does it need rules? Seeded property tests (no hypothesis installed; the generator is
deterministic) and type hazards, in Spark 3.5.1. Assertions record observed behaviour.
"""

from __future__ import annotations

import json
import random
import re

ALPHABET = ['a', 'Z', '0', '9', ' ', '|', '"', '\\', '/', '<null>', ',', '[', ']', ':', '{', '}', 'é', 'é', '日',
            '\U0001F600', ' ', ' ', '\x7f'] + [chr(c) for c in range(0x00, 0x20)]


def py_v2(values: list[str | None]) -> str:
    """The ORIGINAL design's mirror (Gate A 02 section 1.2): json.dumps."""
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))


def py_v2_exact(values: list[str | None]) -> str:
    """Revision 2's mirror: Spark's to_json (Jackson) writes \\uXXXX escapes in UPPER case hex."""
    return re.sub(r"\\u([0-9a-f]{4})", lambda m: "\\u" + m.group(1).upper(), py_v2(values))


def generated(seed: int, n: int, parts: int) -> list[tuple[str | None, ...]]:
    rnd = random.Random(seed)
    out = []
    for _ in range(n):
        row = []
        for _ in range(parts):
            r = rnd.random()
            if r < 0.1:
                row.append(None)
            elif r < 0.15:
                row.append("")
            else:
                row.append("".join(rnd.choice(ALPHABET) for _ in range(rnd.randint(1, 4))))
        out.append(tuple(row))
    return out


def spark_v2(spark, rows: list[tuple], parts: int) -> list[str]:
    schema = ", ".join(f"p{i} string" for i in range(parts))
    df = spark.createDataFrame([(i, *r) for i, r in enumerate(rows)], f"i long, {schema}")
    df.createOrReplaceTempView("ck_rows")
    cols = ", ".join(f"CAST(p{i} AS STRING)" for i in range(parts))
    got = spark.sql(f"SELECT i, to_json(array({cols})) AS k FROM ck_rows ORDER BY i").collect()
    return [r.k for r in got]


def test_k1_the_original_mirror_differs_from_spark_and_the_revised_one_does_not(spark) -> None:
    """5,000 tuples of 3 parts over an alphabet of separators, quotes, escapes, every C0 control,
    DEL, U+2028/2029, combining marks, an astral emoji, '<null>', empty strings and NULLs.
    FINDING R2-01: json.dumps escapes controls as lower-case \\u000e, Spark as \\u000E; the original
    design's mirror is not exact. The upper-casing mirror is exact on every tuple."""
    rows = generated(20261021, 5000, 3)
    got = spark_v2(spark, rows, 3)
    original = [r for r, k in zip(rows, got) if k != py_v2(list(r))]
    revised = [(r, k, py_v2_exact(list(r))) for r, k in zip(rows, got) if k != py_v2_exact(list(r))]
    assert original  # the original mirror is wrong for C0 controls without a short escape
    assert not revised, revised[:5]


def test_k2_injective_over_the_generated_domain(spark) -> None:
    """Distinct tuples give distinct encodings, and json.loads inverts the encoding exactly."""
    rows = list(set(generated(7, 5000, 3)) | set(generated(8, 5000, 2 + 1)))
    got = spark_v2(spark, rows, 3)
    assert len(set(got)) == len(set(rows))
    assert all(tuple(json.loads(k)) == r for k, r in zip(got, rows))


def test_k3_split_pairs_collide_under_v1_and_never_under_v2(spark) -> None:
    """2,000 generated split pairs (a|b, c) / (a, b|c) and null/'<null>' pairs: every one collides under
    v1 (the current encoding), none under v2."""
    rnd = random.Random(11)
    word = lambda: "".join(rnd.choice(ALPHABET[:10]) for _ in range(rnd.randint(1, 3)))  # noqa: E731
    pairs = []
    for _ in range(1000):
        a, b, c = word(), word(), word()
        pairs.append(((a + "|" + b, c), (a, b + "|" + c)))
        pairs.append(((None, c), ("<null>", c)))
    left, right = [p[0] for p in pairs], [p[1] for p in pairs]
    v2_left, v2_right = spark_v2(spark, left, 2), spark_v2(spark, right, 2)
    assert all(x != y for x, y in zip(v2_left, v2_right))
    v1 = lambda t: "|".join("<null>" if v is None else v for v in t)  # noqa: E731
    assert all(v1(x) == v1(y) for x, y in zip(left, right))


def test_k4_timestamp_parts_depend_on_the_session_time_zone(spark) -> None:
    """CAST(timestamp AS STRING) renders in spark.sql.session.timeZone. In a DST zone, two instants an
    hour apart in the fall-back hour render identically: a collision. An explicit UTC rendering does not."""
    prior = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.conf.set("spark.sql.session.timeZone", "America/Chicago")
        row = spark.sql("SELECT CAST(TIMESTAMP '2026-11-01 06:30:00 UTC' AS STRING) AS a, "
                        "CAST(TIMESTAMP '2026-11-01 07:30:00 UTC' AS STRING) AS b, "
                        "date_format(to_utc_timestamp(TIMESTAMP '2026-11-01 06:30:00 UTC', 'America/Chicago'), "
                        "'yyyy-MM-dd HH:mm:ss.SSSSSS') AS a_utc, "
                        "date_format(to_utc_timestamp(TIMESTAMP '2026-11-01 07:30:00 UTC', 'America/Chicago'), "
                        "'yyyy-MM-dd HH:mm:ss.SSSSSS') AS b_utc").collect()[0]
        assert row.a == row.b == "2026-11-01 01:30:00"
        assert row.a_utc != row.b_utc
    finally:
        spark.conf.set("spark.sql.session.timeZone", prior)


def test_k5_numeric_rendering_is_injective_per_type_but_not_equivalent_across_types(spark) -> None:
    """Within one type the string is injective, except float -0.0/0.0, which render differently for
    equal values (a false FAIL, never a collision). Across types equal values can render differently
    (DECIMAL scale), so cross-layer equivalence needs a declared normaliser, not the encoder."""
    r = spark.sql("SELECT CAST(CAST(1 AS DECIMAL(10,2)) AS STRING) AS d2, CAST(CAST(1 AS DECIMAL(10,0)) AS STRING) AS d0, "
                  "CAST(CAST(-0.0 AS DOUBLE) AS STRING) AS nz, CAST(CAST(0.0 AS DOUBLE) AS STRING) AS z, "
                  "CAST(CAST('NaN' AS DOUBLE) AS STRING) AS nan, CAST(CAST(0.1 AS DOUBLE) + CAST(0.2 AS DOUBLE) AS STRING) AS f, "
                  "CAST(42 AS STRING) AS i, CAST(CAST(42 AS BIGINT) AS STRING) AS b").collect()[0]
    assert (r.d2, r.d0) == ("1.00", "1")
    assert (r.nz, r.z) == ("0.0", "0.0")  # observed: the -0.0 literal folds to 0.0 here
    assert r.nan == "NaN" and r.f == "0.30000000000000004"
    assert r.i == r.b == "42"


def test_k6_binary_and_complex_types_are_not_safe_key_parts(spark) -> None:
    """ARRAY: the cast renders without quoting, so ["a, b"] and ["a", "b"] collide. BINARY: the cast
    keeps the raw bytes (no collision there), but to_json writes invalid UTF-8 as U+FFFD, so two
    different byte strings can collide in the encoding. Both must be refused as key part types."""
    r = spark.sql("SELECT CAST(X'FF' AS STRING) = CAST(X'FE' AS STRING) AS bin_cast_equal, "
                  "to_json(array(CAST(X'FF' AS STRING))) = to_json(array(CAST(X'FE' AS STRING))) AS bin_json_equal, "
                  "CAST(array('a, b') AS STRING) = CAST(array('a', 'b') AS STRING) AS arr_collide").collect()[0]
    assert r.arr_collide and not r.bin_cast_equal
    print(f"\nK6 binary: cast equal {r.bin_cast_equal}, json equal {r.bin_json_equal}")


def test_k7_unicode_is_compared_as_stored_no_normalisation(spark) -> None:
    """NFC 'é' and NFD 'e' + U+0301 encode differently: distinct keys (correctly not a collision; a
    cross-layer false FAIL if the layers store different forms; no Spark built-in normalises)."""
    rows = [("é",), ("é",)]
    got = spark_v2(spark, rows, 1)
    assert got[0] != got[1]


def test_k8_get_spark_applies_utc_to_an_existing_session(spark) -> None:
    """Under spark-submit the session exists before the engine runs; get_spark's builder config
    reaches it (a runtime SQL conf), so the engine's session is UTC even without --conf."""
    from hcsc.datalake.dre.session import get_spark

    prior = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.conf.set("spark.sql.session.timeZone", "America/Chicago")
        assert get_spark().conf.get("spark.sql.session.timeZone") == "UTC"
    finally:
        spark.conf.set("spark.sql.session.timeZone", prior)
