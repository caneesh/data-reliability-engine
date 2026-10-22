"""Revision 2, part A (continued): the length-prefixed byte encoding (LPB) that replaces the JSON
proposal, and the hash input. Each part: NULL -> '~;', else '<octet_length>:<bytes>;'.
"""

from __future__ import annotations

import hashlib
import hmac
import random
import time

from test_canonical_key import ALPHABET, generated, py_v2_exact


def lpb_sql(parts: list[str]) -> str:
    terms = [f"CASE WHEN {p} IS NULL THEN '~;' ELSE concat(octet_length({p}), ':', {p}, ';') END" for p in parts]
    return f"concat({', '.join(terms)})"


def lpb_py(values: list[str | None]) -> str:
    return "".join("~;" if v is None else f"{len(v.encode('utf-8'))}:{v};" for v in values)


def spark_lpb(spark, rows: list[tuple], parts: int) -> list[str]:
    schema = ", ".join(f"p{i} string" for i in range(parts))
    spark.createDataFrame([(i, *r) for i, r in enumerate(rows)], f"i long, {schema}").createOrReplaceTempView("lpb_rows")
    cast = [f"CAST(p{i} AS STRING)" for i in range(parts)]
    return [r.k for r in spark.sql(f"SELECT i, {lpb_sql(cast)} AS k FROM lpb_rows ORDER BY i").collect()]


def test_l1_mirror_exact_and_injective_on_the_generated_domain(spark) -> None:
    rows = list(set(generated(20261021, 5000, 3)) | set(generated(5, 5000, 3)))
    got = spark_lpb(spark, rows, 3)
    assert all(k == lpb_py(list(r)) for k, r in zip(got, rows))
    assert len(set(got)) == len(rows)


def test_l2_split_and_null_pairs_never_collide(spark) -> None:
    rnd = random.Random(11)
    word = lambda: "".join(rnd.choice(ALPHABET[:10]) for _ in range(rnd.randint(1, 3)))  # noqa: E731
    left, right = [], []
    for _ in range(1000):
        a, b, c = word(), word(), word()
        left += [(a + "|" + b, c), (None, c), ("", c), (a + ";", c)]
        right += [(a, b + "|" + c), ("<null>", c), (None, c), (a, ";" + c)]
    lk, rk = spark_lpb(spark, left, 2), spark_lpb(spark, right, 2)
    assert all(x != y for x, y in zip(lk, rk))


def test_l3_invalid_utf8_bytes_stay_distinct_under_lpb_and_collide_under_json(spark) -> None:
    """The decisive case: two STRING values that differ only in invalid UTF-8 bytes (X'FF', X'FE')."""
    r = spark.sql(f"""SELECT {lpb_sql(["CAST(X'FF' AS STRING)"])} = {lpb_sql(["CAST(X'FE' AS STRING)"])} AS lpb_equal,
                             to_json(array(CAST(X'FF' AS STRING))) = to_json(array(CAST(X'FE' AS STRING))) AS json_equal,
                             hex({lpb_sql(["CAST(X'FF' AS STRING)"])}) AS lpb_hex""").collect()[0]
    assert (r.lpb_equal, r.json_equal) == (False, True)
    assert r.lpb_hex == "313AFF3B"  # '1:' 0xFF ';' : the raw byte is kept


def test_l4_the_hash_must_take_hex_of_the_key_not_the_key(spark) -> None:
    """A Python UDF receives a decoded str, so invalid bytes arrive as U+FFFD: an HMAC over k collides
    for those keys; an HMAC over hex(k) (ASCII) does not."""
    secret = b"synthetic-test-secret"
    spark.udf.register("rev2_hash", lambda s: None if s is None else hmac.new(secret, s.encode("utf-8"),
                                                                           hashlib.sha256).hexdigest())
    k_ff, k_fe = lpb_sql(["CAST(X'FF' AS STRING)"]), lpb_sql(["CAST(X'FE' AS STRING)"])
    r = spark.sql(f"SELECT rev2_hash({k_ff}) = rev2_hash({k_fe}) AS on_k, "
                  f"rev2_hash(hex({k_ff})) = rev2_hash(hex({k_fe})) AS on_hex").collect()[0]
    assert (r.on_k, r.on_hex) == (True, False)


def test_l5_lpb_joins_and_groups_byte_wise(spark) -> None:
    """Joins, GROUP BY and IN use byte equality on UTF8String: a key read back from a stored STRING column
    matches the computed one exactly (ORC round trip)."""
    spark.sql("DROP TABLE IF EXISTS default.rev2_lpb_rt")
    spark.sql("CREATE TABLE default.rev2_lpb_rt (k STRING) STORED AS ORC")
    spark.sql(f"INSERT INTO default.rev2_lpb_rt SELECT {lpb_sql(['CAST(X' + chr(39) + 'FF41' + chr(39) + ' AS STRING)', chr(39) + 'x|y' + chr(39)])}")
    r = spark.sql(f"SELECT COUNT(*) AS n FROM default.rev2_lpb_rt WHERE k = "
                  f"{lpb_sql(['CAST(X' + chr(39) + 'FF41' + chr(39) + ' AS STRING)', chr(39) + 'x|y' + chr(39)])}").collect()[0]
    spark.sql("DROP TABLE default.rev2_lpb_rt")
    assert r.n == 1


def test_l6_cost_against_v1_and_json(spark) -> None:
    """Indicative (local, 2 cores): COUNT(DISTINCT key) over 1,000,000 four-part keys."""
    spark.range(1_000_000).selectExpr("CAST(id AS STRING) AS a", "'01' AS b", "'2026-01-01' AS c",
                                      "CAST(id % 7 AS STRING) AS d").createOrReplaceTempView("lpb_cost")
    exprs = {"v1": "concat_ws('|', coalesce(a, '<null>'), coalesce(b, '<null>'), coalesce(c, '<null>'), coalesce(d, '<null>'))",
             "json": "to_json(array(a, b, c, d))", "lpb": lpb_sql(["a", "b", "c", "d"])}
    out = {}
    for name, e in exprs.items():
        spark.sql(f"SELECT COUNT(DISTINCT {e}) FROM lpb_cost").collect()
        started = time.monotonic()
        row = spark.sql(f"SELECT COUNT(DISTINCT {e}) AS n, AVG(octet_length({e})) AS w FROM lpb_cost").collect()[0]
        out[name] = (round(time.monotonic() - started, 2), round(row.w, 1), row.n)
    print(f"\nL6 (seconds, avg bytes, distinct): {out}")
    assert {v[2] for v in out.values()} == {1_000_000}


def test_l7_the_json_mirror_is_kept_only_for_contrast() -> None:
    assert py_v2_exact(["\x0e"]) == '["\\u000E"]'


def test_l8_a_binary_input_udf_hashes_the_raw_bytes(spark) -> None:
    """Alternative to hex(k): pass CAST(k AS BINARY) to a UDF declared on bytes. Invalid bytes stay
    distinct, and for a valid UTF-8 key the hash equals HMAC(secret, k as UTF-8): spec section 5's
    definition (HMAC of the canonical key string) holds unchanged."""
    secret = b"synthetic-test-secret"
    h = lambda b: None if b is None else hmac.new(secret, bytes(b), hashlib.sha256).hexdigest()  # noqa: E731
    spark.udf.register("rev2_hash_bytes", h)
    k_ff, k_fe, k_ok = (lpb_sql([e]) for e in ("CAST(X'FF' AS STRING)", "CAST(X'FE' AS STRING)", "'é|x'"))
    r = spark.sql(f"SELECT rev2_hash_bytes(CAST({k_ff} AS BINARY)) = rev2_hash_bytes(CAST({k_fe} AS BINARY)) AS same, "
                  f"rev2_hash_bytes(CAST({k_ok} AS BINARY)) AS ok").collect()[0]
    assert r.same is False
    assert r.ok == hmac.new(secret, lpb_py(["é|x"]).encode("utf-8"), hashlib.sha256).hexdigest()
