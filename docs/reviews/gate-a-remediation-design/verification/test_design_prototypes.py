"""Prototypes that prove the Gate A design's assumptions in Spark 3.5.1 (no engine code changed).

V1-V3: F-02 identity encoding v2 = to_json(array(<normalised parts as STRING>)).
V4:    F-05 any-untimed-row detection and the due load over all rows.
V5:    F-07 unanimity of rule predicates over tied rows.
V6:    F-07 deterministic reject choice (defect before authorised, then reason, then rule).
V7:    cost of v2 against v1 on 1,000,000 four-part keys (local, indicative only).
"""

from __future__ import annotations

import json
import time

V1_KEY = "concat_ws('|', {})"  # the current encoding (checks/keys.py key_string), for contrast


def v1(parts: list[str]) -> str:
    return V1_KEY.format(", ".join(f"coalesce(CAST({p} AS STRING), '<null>')" for p in parts))


def v2(parts: list[str]) -> str:
    return f"to_json(array({', '.join(f'CAST({p} AS STRING)' for p in parts)}))"


def py_v2(values: list[str | None]) -> str:
    """The Python mirror the design proposes (checks/keys.py canonical_key)."""
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))


COLLIDING = [  # pairs that collide under v1 (the examination's F-02 cases)
    (("A|B", "C"), ("A", "B|C")),
    ((None, "x"), ("<null>", "x")),
    (("1", "01|X"), ("1|01", "X")),
]


def test_v1_v1_collides_and_v2_does_not(spark) -> None:
    for a, b in COLLIDING:
        row = spark.sql(f"SELECT {v1(['a1', 'a2'])} AS k1a, {v1(['b1', 'b2'])} AS k1b, "
                        f"{v2(['a1', 'a2'])} AS k2a, {v2(['b1', 'b2'])} AS k2b FROM "
                        f"(SELECT {lit(a[0])} AS a1, {lit(a[1])} AS a2, {lit(b[0])} AS b1, {lit(b[1])} AS b2)").collect()[0]
        assert row.k1a == row.k1b, (a, b)  # the defect, reproduced
        assert row.k2a != row.k2b, (a, b, row.k2a, row.k2b)


def lit(v: str | None) -> str:
    return "CAST(NULL AS STRING)" if v is None else "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"


def test_v2_python_mirror_is_exact_for_hard_strings(spark) -> None:
    """quotes, backslashes, controls, unicode, the old separator and marker, empty strings, nulls."""
    cases = [['a"b', "c\\d"], ["tab\there", "nl\nx"], ["ünïcödé", "日本"], ["|", "<null>"], ["", None], [None, None],
             ["000123401", "01"]]
    for values in cases:
        parts = ", ".join(lit(v) for v in values)
        got = spark.sql(f"SELECT to_json(array({', '.join(f'CAST(p{i} AS STRING)' for i in range(len(values)))})) AS k "
                        f"FROM (SELECT {', '.join(f'{lit(v)} AS p{i}' for i, v in enumerate(values))})").collect()[0].k
        assert got == py_v2(values), (values, got, py_v2(values), parts)


def test_v3_v2_matches_across_types_as_v1_does(spark) -> None:
    """Cross-layer comparison is by normalised string (an INT column against a STRING column): unchanged."""
    a, b = v2(["CAST(42 AS INT)", "'x'"]), v2(["'42'", "'x'"])
    row = spark.sql(f"SELECT {a} AS a, {b} AS b").collect()[0]
    assert row.a == row.b == '["42","x"]'


def test_v4_any_untimed_row_is_detected_and_the_due_load_covers_all_rows(spark) -> None:
    """Key 1: an old timed version (loaded 01-10) and a newer row with no record time (loaded 01-15).
    Current logic judges the 01-10 version, not due; the prototype counts the untimed row and dates
    the key's due load by its latest row, so the key is due in the 01-15 range and unassessable."""
    spark.createDataFrame([("1", "2026-01-10 10:00:00", "2026-01-10 10:00:00"), ("1", None, "2026-01-15 10:00:00"),
                           ("2", "2026-01-15 10:00:00", "2026-01-15 10:00:00")],
                          "k string, rec string, load string").createOrReplaceTempView("v4_rows")
    rows = {r.k: r for r in spark.sql("""
        WITH r AS (SELECT k, to_timestamp(rec) AS record_ts, to_timestamp(load) AS load_ts FROM v4_rows),
        ranked AS (SELECT r.*, MAX(record_ts) OVER (PARTITION BY k) AS top_ts,
                          COUNT(*) OVER (PARTITION BY k) - COUNT(record_ts) OVER (PARTITION BY k) AS untimed_rows,
                          MAX(load_ts) OVER (PARTITION BY k) AS latest_load FROM r)
        SELECT k, MAX(untimed_rows) AS untimed_rows,
               MAX(CASE WHEN record_ts = top_ts THEN load_ts END) AS current_due_load, MAX(latest_load) AS proposed_due_load
        FROM ranked GROUP BY k""").collect()}
    assert rows["1"].untimed_rows == 1 and str(rows["1"].current_due_load) == "2026-01-10 10:00:00"
    assert str(rows["1"].proposed_due_load) == "2026-01-15 10:00:00"
    assert rows["2"].untimed_rows == 0 and rows["2"].current_due_load == rows["2"].proposed_due_load


def test_v5_tied_rows_must_agree_on_a_rule(spark) -> None:
    """Key 1: two rows at its latest record time, one matching the exclusion. MAX (current) says
    excluded; MIN/MAX disagreement (proposed) says ambiguous. Key 2: both match: excluded either way."""
    spark.createDataFrame([("1", True), ("1", False), ("2", True), ("2", True)], "k string, x boolean") \
        .createOrReplaceTempView("v5_rows")
    got = {r.k: r for r in spark.sql("""
        SELECT k, MAX(CASE WHEN x THEN 1 ELSE 0 END) AS current_x,
               MIN(CASE WHEN x THEN 1 ELSE 0 END) AS x_all, MAX(CASE WHEN x THEN 1 ELSE 0 END) AS x_any
        FROM v5_rows GROUP BY k""").collect()}
    assert got["1"].current_x == 1 and (got["1"].x_all, got["1"].x_any) == (0, 1)  # proposed: ambiguous
    assert (got["2"].x_all, got["2"].x_any) == (1, 1)  # proposed: excluded


def test_v6_reject_choice_is_deterministic_defect_first(spark) -> None:
    """Two rejects of one version at one time: max_by may pick either; an explicit order never does."""
    spark.createDataFrame([("1", "INVALID_CODE", "authorised", "2026-01-15 11:00:00"),
                           ("1", "PARSE_ERROR", "defect", "2026-01-15 11:00:00")],
                          "k string, reason string, cls string, t string").createOrReplaceTempView("v6_rows")
    for _ in range(3):
        [r] = spark.sql("""SELECT k, max_by(named_struct('reason', reason, 'cls', cls),
                                  named_struct('t', to_timestamp(t), 'defect', CASE WHEN cls = 'defect' THEN 1 ELSE 0 END,
                                               'reason', reason)) AS latest
                           FROM v6_rows GROUP BY k""").collect()
        assert r.latest.reason == "PARSE_ERROR"


def test_v7_cost_of_v2_against_v1(spark) -> None:
    """Indicative only: group 1,000,000 four-part keys by each encoding (local, 2 cores)."""
    spark.range(1_000_000).selectExpr("CAST(id AS STRING) AS a", "'01' AS b", "'2026-01-01' AS c",
                                      "CAST(id % 7 AS STRING) AS d").createOrReplaceTempView("v7_rows")
    timings = {}
    for name, expr in (("v1", v1(["a", "b", "c", "d"])), ("v2", v2(["a", "b", "c", "d"]))):
        spark.sql(f"SELECT COUNT(DISTINCT {expr}) FROM v7_rows").collect()  # warm
        started = time.monotonic()
        n = spark.sql(f"SELECT COUNT(DISTINCT {expr}) AS n, AVG(length({expr})) AS w FROM v7_rows").collect()[0]
        timings[name] = (round(time.monotonic() - started, 2), round(n.w, 1), n.n)
    print(f"\nV7 timings (seconds, avg key length, distinct keys): {timings}")
    assert timings["v1"][2] == timings["v2"][2] == 1_000_000
