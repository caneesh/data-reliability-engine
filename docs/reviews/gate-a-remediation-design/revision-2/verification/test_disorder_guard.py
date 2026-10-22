"""Revision 2, part B (continued): the falsification guard for a declared disorder bound D (03 §3.2).

Per key, a timed row OLDER than the version but loaded more than D AFTER it is a version pair out of
order by more than D: evidence that D is false. When any key in the evaluation shows it, the evaluation
applies the strict rule (no untimed row is set aside). Modelled here as: refuted -> classify with
disorder_hours=None. Verification code, not engine code.
"""

from __future__ import annotations

from datetime import timedelta

from proto_ledger import Params, classify, key, load
from test_rules_model import CUT, END, IN, OLD, START, ts


def refuted_keys(spark, d_hours: int) -> int:
    return spark.sql(f"""
        WITH r AS (SELECT k1, eff, to_timestamp(rec) AS record_ts, load_ts FROM up),
        w AS (SELECT r.*, MAX(record_ts) OVER (PARTITION BY k1, eff) AS top_ts FROM r),
        v AS (SELECT w.*, MAX(CASE WHEN record_ts = top_ts THEN load_ts END) OVER (PARTITION BY k1, eff) AS v_load FROM w)
        SELECT COUNT(DISTINCT k1, eff) AS n FROM (
          SELECT k1, eff, MAX(CASE WHEN record_ts < top_ts THEN load_ts END) AS older_latest_load, MAX(v_load) AS v_load
          FROM v GROUP BY k1, eff)
        WHERE older_latest_load > v_load + make_dt_interval(0, {d_hours}, 0, 0)""").collect()[0].n


def guarded(spark, up, dn, d_hours: int) -> tuple[int, dict]:
    load(spark, up, dn)
    n = refuted_keys(spark, d_hours)
    return n, classify(spark, Params(CUT, START, END, disorder_hours=None if n else d_hours))


def test_t1_an_in_order_feed_keeps_the_bound_and_the_old_row_is_set_aside(spark) -> None:
    up = [{"k1": "1", "rec": None, "load_ts": OLD}, {"k1": "1", "rec": ts(IN - timedelta(days=5)), "load_ts": IN - timedelta(days=5)},
          {"k1": "1", "rec": ts(IN), "load_ts": IN}]
    n, out = guarded(spark, up, [{"k1": "1", "rec": ts(IN)}], 24)
    k = out["keys"][key("1")]
    assert (n, k["disposition"], k["set_aside"]) == (0, "delivered", 1)


def test_t2_a_backfill_beyond_the_bound_refutes_it_and_the_evaluation_is_strict(spark) -> None:
    """Key 2 carries a backfill (an older version loaded 30 days after the current one): D is refuted for the
    evaluation, so key 1's old untimed row is relevant again: unassessable, not delivered."""
    up = [{"k1": "1", "rec": None, "load_ts": OLD}, {"k1": "1", "rec": ts(IN), "load_ts": IN},
          {"k1": "2", "rec": ts(IN - timedelta(days=40)), "load_ts": IN},
          {"k1": "2", "rec": ts(IN - timedelta(days=30)), "load_ts": IN - timedelta(days=30)}]
    n, out = guarded(spark, up, [{"k1": "1", "rec": ts(IN)}, {"k1": "2", "rec": ts(IN - timedelta(days=30))}], 24)
    assert n == 1
    assert out["keys"][key("1")]["disposition"] == "unassessable:no record time"


def test_t3_disorder_within_the_bound_does_not_refute_it(spark) -> None:
    up = [{"k1": "1", "rec": ts(IN - timedelta(hours=3)), "load_ts": IN + timedelta(hours=2)},
          {"k1": "1", "rec": ts(IN), "load_ts": IN}]
    load(spark, up, [])
    assert refuted_keys(spark, 24) == 0 and refuted_keys(spark, 1) == 1
