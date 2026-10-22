"""Revision 2, parts B, C, D, G: the proposed rules, exercised on the model (proto_ledger.py).

Window: cutoff 2026-01-16 12:00; due range [2026-01-15 04:00, 2026-01-16 04:00) on the due load.
Each test asserts the outcome the revision-2 design specifies; the docstring names the scenario.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from proto_ledger import Params, classify, key, load

CUT, START, END = datetime(2026, 1, 16, 12), datetime(2026, 1, 15, 4), datetime(2026, 1, 16, 4)
P = Params(CUT, START, END)
IN = datetime(2026, 1, 15, 10)
OLD = datetime(2024, 1, 10, 10)


def ts(t: datetime) -> str:
    return t.strftime("%Y-%m-%d %H:%M:%S")


def run(spark, up, dn, rej=None, p: Params = P) -> dict:
    load(spark, up, dn, rej)
    return classify(spark, p)


def disp(out: dict, k1: str, eff: str | None = "2026-01-01") -> str:
    return out["keys"][key(k1, eff)]["disposition"]


def line(out: dict, k1: str) -> str:
    return out["keys"][key(k1)]["line"]


# --- B: temporal eligibility ---

def test_b01_untimed_newest_row_is_due_and_unassessable(spark) -> None:
    out = run(spark, [{"k1": "1", "rec": ts(OLD), "load_ts": OLD}, {"k1": "1", "rec": None, "load_ts": IN}],
              [{"k1": "1", "rec": ts(OLD)}])
    assert (line(out, "1"), disp(out, "1")) == ("due", "unassessable:no record time")


def test_b02_old_untimed_row_without_a_declared_bound_keeps_the_key_unassessable(spark) -> None:
    """The user's example: a malformed row two years ago, valid updates since, gold correct. Without a
    declaration DRE cannot prove the old row is older than the current version (out-of-order CDC):
    unassessable (the safe inconclusive outcome)."""
    up = [{"k1": "1", "rec": None, "load_ts": OLD}, {"k1": "1", "rec": ts(IN), "load_ts": IN}]
    out = run(spark, up, [{"k1": "1", "rec": ts(IN)}])
    assert disp(out, "1") == "unassessable:no record time"


def test_b03_old_untimed_row_beyond_a_declared_disorder_bound_is_set_aside_and_counted(spark) -> None:
    """With a declared, approved disorder bound D (loads at most D out of version order), a row loaded more
    than D before the current version's load cannot be newer: set aside (counted), key judged: delivered."""
    up = [{"k1": "1", "rec": None, "load_ts": OLD}, {"k1": "1", "rec": ts(IN), "load_ts": IN}]
    out = run(spark, up, [{"k1": "1", "rec": ts(IN)}], p=Params(CUT, START, END, disorder_hours=24))
    k = out["keys"][key("1")]
    assert (k["disposition"], k["set_aside"]) == ("delivered", 1)


def test_b04_untimed_row_within_the_disorder_bound_stays_relevant(spark) -> None:
    up = [{"k1": "1", "rec": None, "load_ts": IN - timedelta(hours=2)}, {"k1": "1", "rec": ts(IN), "load_ts": IN}]
    out = run(spark, up, [{"k1": "1", "rec": ts(IN)}], p=Params(CUT, START, END, disorder_hours=24))
    assert disp(out, "1") == "unassessable:no record time"


def test_b05_untimed_row_with_no_load_time_is_always_relevant(spark) -> None:
    up = [{"k1": "1", "rec": None, "load_ts": None}, {"k1": "1", "rec": ts(IN), "load_ts": IN}]
    out = run(spark, up, [{"k1": "1", "rec": ts(IN)}], p=Params(CUT, START, END, disorder_hours=24))
    assert disp(out, "1") == "unassessable:no record time"


def test_b06_malformed_record_time_is_labelled_unparseable(spark) -> None:
    for bad in ("", "garbage", "2026-02-30 10:00:00", "15/01/2026 10:00"):
        out = run(spark, [{"k1": "1", "rec": ts(OLD), "load_ts": OLD}, {"k1": "1", "rec": bad, "load_ts": IN}],
                  [{"k1": "1", "rec": ts(OLD)}])
        assert disp(out, "1") == "unassessable:unparseable record time", bad


def test_b07_valid_record_time_with_no_load_time_is_untimed_not_judged(spark) -> None:
    """Unchanged rule: the version has no load time, so no deadline: the untimed line (the full sweep
    makes it unassessable in the engine). Shown with a second row loaded in range so the key is a candidate."""
    up = [{"k1": "1", "rec": ts(IN), "load_ts": None}, {"k1": "1", "rec": ts(OLD), "load_ts": IN}]
    out = run(spark, up, [{"k1": "1", "rec": ts(IN)}])
    assert line(out, "1") == "untimed"


def test_b08_invalid_record_time_with_a_valid_load_time_is_due_by_its_load(spark) -> None:
    out = run(spark, [{"k1": "1", "rec": None, "load_ts": IN}], [])
    assert (line(out, "1"), disp(out, "1")) == ("due", "unassessable:no record time")


def test_b09_late_arrival_after_the_cutoff_belongs_to_a_later_window(spark) -> None:
    up = [{"k1": "1", "rec": ts(IN), "load_ts": IN}, {"k1": "1", "rec": None, "load_ts": CUT + timedelta(hours=1)}]
    out = run(spark, up, [{"k1": "1", "rec": ts(IN)}])
    assert disp(out, "1") == "delivered"


def test_b10_backfill_and_historical_correction_do_not_re_judge_the_current_version(spark) -> None:
    """An older valid version loaded now: not the version; the key's due load is its version's (old)."""
    up = [{"k1": "1", "rec": ts(IN - timedelta(days=30)), "load_ts": IN - timedelta(days=30)},
          {"k1": "1", "rec": ts(IN - timedelta(days=40)), "load_ts": IN}]
    out = run(spark, up, [{"k1": "1", "rec": ts(IN - timedelta(days=30))}])
    assert line(out, "1") == "judged_before"


def test_b11_point_in_time_replay_sees_only_rows_loaded_before_its_cutoff(spark) -> None:
    up = [{"k1": "1", "rec": ts(IN), "load_ts": IN}, {"k1": "1", "rec": None, "load_ts": IN + timedelta(hours=3)}]
    early = Params(IN + timedelta(hours=1), START, IN + timedelta(minutes=30))
    out = run(spark, up, [{"k1": "1", "rec": ts(IN)}], p=early)
    assert disp(out, "1") == "delivered"
    assert disp(run(spark, up, [{"k1": "1", "rec": ts(IN)}]), "1") == "unassessable:no record time"


# --- C: ties ---

def tied(**kw):
    return {"k1": "1", "rec": ts(IN), "load_ts": IN, **kw}


def test_c01_identical_tied_rows_are_one_record(spark) -> None:
    assert disp(run(spark, [tied(), tied()], [{"k1": "1", "rec": ts(IN)}]), "1") == "delivered"


def test_c02_tied_rows_with_different_values_keep_the_approved_set_rule(spark) -> None:
    up = [tied(val="open"), tied(val="term")]
    assert disp(run(spark, up, [{"k1": "1", "rec": ts(IN), "val": "open"}, {"k1": "1", "rec": ts(IN), "val": "term"}]),
                "1") == "delivered"
    assert disp(run(spark, up, [{"k1": "1", "rec": ts(IN), "val": "open"}]), "1") == "divergent"  # R06-like


def test_c03_live_versus_excluded_or_deleted_is_ambiguous(spark) -> None:
    for rule in ({"x1": True}, {"tomb": True}):
        assert disp(run(spark, [tied(), tied(val="w", **rule)], []), "1") == "unassessable:tied rows disagree", rule


def test_c04_deleted_versus_excluded_agree_on_absence(spark) -> None:
    assert disp(run(spark, [tied(tomb=True), tied(val="w", x1=True)], []), "1") == "deleted:mixed"
    assert disp(run(spark, [tied(tomb=True), tied(val="w", x1=True)], [{"k1": "1", "rec": ts(IN)}]),
                "1") == "deleted_not_applied"


def test_c05_two_different_approved_exclusions_each_covering_a_row(spark) -> None:
    assert disp(run(spark, [tied(x1=True), tied(val="w", x2=True)], []), "1") == "excluded"


def test_c06_two_deletes_agree(spark) -> None:
    assert disp(run(spark, [tied(tomb=True), tied(val="w", tomb=True)], []), "1") == "deleted"


def test_c07_rejects_at_one_time_are_ordered_defect_first(spark) -> None:
    rej = [{"k1": "1", "version": ts(IN), "reason": "INVALID_CODE", "defect": False, "rejected_ts": IN},
           {"k1": "1", "version": ts(IN), "reason": "PARSE_ERROR", "defect": True, "rejected_ts": IN}]
    for _ in range(3):
        assert disp(run(spark, [tied()], [], rej), "1") == "rejected:defect"


def test_c08_a_version_reject_among_differing_tied_rows_is_ambiguous(spark) -> None:
    rej = [{"k1": "1", "version": ts(IN), "reason": "INVALID_CODE", "defect": False, "rejected_ts": IN}]
    assert disp(run(spark, [tied(val="a"), tied(val="b")], [], rej), "1") == "unassessable:reject ambiguous among tied rows"


# --- D: downstream key integrity ---

def test_d01_an_unparseable_target_key_never_matches_and_is_counted(spark) -> None:
    """Gold wrote record 1 with a garbled effective date: record 1 is missing (never a false match), and
    the garbled row is counted (an orphan 'unparseable key' when orphans are checked)."""
    out = run(spark, [tied()], [{"k1": "1", "rec": ts(IN), "eff": "2026-13-01"}])
    assert disp(out, "1") == "missing" and out["orphan_unparseable"] == 1


def test_d02_an_unparseable_target_part_never_matches_a_genuinely_null_upstream_part(spark) -> None:
    """The examination's l05, at the model: upstream eff NULL, gold eff unparseable."""
    out = run(spark, [{"k1": "1", "eff": None, "rec": ts(IN), "load_ts": IN}], [{"k1": "1", "rec": ts(IN), "eff": "2026-13-01"}])
    assert disp(out, "1", None) == "missing"


def test_d03_duplicate_unparseable_target_rows_are_each_counted(spark) -> None:
    out = run(spark, [tied()], [{"k1": "1", "rec": ts(IN), "eff": "x"}, {"k1": "1", "rec": ts(IN), "eff": "y"}],
              p=Params(CUT, START, END, orphans_checked=False))
    assert out["scope_unparseable_target_rows"] == 2


def test_d04_an_untimed_target_row_beside_a_timed_one_is_unassessable_here(spark) -> None:
    out = run(spark, [tied()], [{"k1": "1", "rec": ts(IN)}, {"k1": "1", "rec": None, "val": "z"}])
    assert disp(out, "1") == "unassessable:no record time here"


# --- G: combined failures and invariants ---

def test_g01_collision_plus_untimed_row_are_two_keys_each_judged(spark) -> None:
    """('A|B', NULL) and ('A', 'B|') collide under v1. Under LPB they are two keys; the unparseable 'B|'
    keeps its own identity ('!' marker), so it merges with neither ('A', NULL) nor anything else."""
    up = [{"k1": "A|B", "eff": None, "rec": None, "load_ts": IN}, {"k1": "A", "eff": "B|", "rec": ts(IN), "load_ts": IN}]
    out = run(spark, up, [])
    assert len(out["keys"]) == 2
    assert out["keys"][key("A|B", None)]["disposition"] == "unassessable:no record time"
    assert out["keys"][key("A", "B|")]["disposition"] == "unassessable:unparseable key"  # 'B|' is not a date
    assert key("A", "B|") != key("A", None)


def test_g01b_an_unparseable_part_never_merges_with_a_null_part(spark) -> None:
    """Upstream holds (A, NULL), a genuine record delivered, and (A, '2026-13-01'), unparseable. Two keys:
    the first delivered, the second unassessable; neither is collateral of the other."""
    up = [{"k1": "A", "eff": None, "rec": ts(IN), "load_ts": IN}, {"k1": "A", "eff": "2026-13-01", "rec": ts(IN), "load_ts": IN}]
    out = run(spark, up, [{"k1": "A", "eff": None, "rec": ts(IN)}])
    assert out["keys"][key("A", None)]["disposition"] == "delivered"
    assert out["keys"][key("A", "2026-13-01")]["disposition"] == "unassessable:unparseable key"


def test_g02_untimed_row_plus_conflicting_exclusion_reports_the_time_first(spark) -> None:
    out = run(spark, [tied(), tied(val="w", x1=True), {"k1": "1", "rec": None, "load_ts": IN}], [])
    assert disp(out, "1") == "unassessable:no record time"


def test_g03_tie_plus_malformed_target_key_is_missing(spark) -> None:
    out = run(spark, [tied(val="a"), tied(val="b")], [{"k1": "1", "rec": ts(IN), "eff": "bad"}])
    assert disp(out, "1") == "missing" and out["orphan_unparseable"] == 1


def test_g04_missing_evidence_plus_approved_exclusion_is_not_excluded(spark) -> None:
    """An excluded row whose record time is missing does not account for the key."""
    out = run(spark, [{"k1": "1", "rec": None, "load_ts": IN, "x1": True}], [])
    assert disp(out, "1") == "unassessable:no record time"


def test_g05_several_invalid_keys_in_one_window_are_each_reported(spark) -> None:
    up = [{"k1": str(i), "rec": None if i % 2 else "garbage", "load_ts": IN} for i in range(6)]
    out = run(spark, up, [])
    assert sorted(v["disposition"] for v in out["keys"].values()) == sorted(
        ["unassessable:no record time"] * 3 + ["unassessable:unparseable record time"] * 3)


def generated_population(seed: int) -> tuple[list[dict], list[dict]]:
    rnd = random.Random(seed)
    up, dn = [], []
    for i in range(300):
        k1 = str(rnd.randint(0, 80))
        rec = rnd.choice([ts(IN), ts(IN - timedelta(hours=rnd.randint(1, 900))), None, "garbage"])
        load = rnd.choice([IN, IN - timedelta(hours=rnd.randint(1, 900)), None, CUT + timedelta(hours=2)])
        up.append({"k1": k1, "rec": rec, "load_ts": load, "val": rnd.choice("ab"), "tomb": rnd.random() < 0.1,
                   "x1": rnd.random() < 0.1, "eff": rnd.choice(["2026-01-01", "2026-01-01", "bad"])})
        if rnd.random() < 0.7:
            dn.append({"k1": k1, "rec": rec if rnd.random() < 0.8 else ts(IN), "val": rnd.choice("ab"),
                       "eff": rnd.choice(["2026-01-01", "2026-01-01", "oops"])})
    return up, dn


def test_g06_coverage_every_candidate_key_has_exactly_one_line_and_every_untimed_row_is_counted(spark) -> None:
    for seed in (1, 2, 3):
        up, dn = generated_population(seed)
        out = run(spark, up, dn, p=Params(CUT, START, END, disorder_hours=72))
        keys = out["keys"]
        assert all(v["line"] in ("due", "pending", "untimed", "judged_before") for v in keys.values())
        cand = {key(r["k1"], r["eff"] if r["eff"] != "bad" else None) for r in up}  # superset check below
        assert len(keys) <= len(cand) + len(up)
        # every untimed upstream row of a candidate key is counted: relevant (in an unassessable key) or set aside
        counted = sum(v["untimed_relevant"] + v["set_aside"] for v in keys.values())
        cand_untimed = spark.sql(
            f"SELECT COUNT(*) AS n FROM up u WHERE (u.load_ts IS NULL OR u.load_ts < TIMESTAMP '{ts(CUT)}') "
            f"AND to_timestamp(u.rec) IS NULL AND concat(CASE WHEN k1 IS NULL THEN '~;' ELSE concat(octet_length(k1), ':', k1, ';') END, "
            f"CASE WHEN date_format(to_date(eff, 'yyyy-MM-dd'), 'yyyy-MM-dd') IS NULL THEN '~;' ELSE "
            f"concat(octet_length(date_format(to_date(eff, 'yyyy-MM-dd'), 'yyyy-MM-dd')), ':', "
            f"date_format(to_date(eff, 'yyyy-MM-dd'), 'yyyy-MM-dd'), ';') END) IN "
            f"(SELECT DISTINCT concat(CASE WHEN k1 IS NULL THEN '~;' ELSE concat(octet_length(k1), ':', k1, ';') END, "
            f"CASE WHEN date_format(to_date(eff, 'yyyy-MM-dd'), 'yyyy-MM-dd') IS NULL THEN '~;' ELSE "
            f"concat(octet_length(date_format(to_date(eff, 'yyyy-MM-dd'), 'yyyy-MM-dd')), ':', "
            f"date_format(to_date(eff, 'yyyy-MM-dd'), 'yyyy-MM-dd'), ';') END) FROM up "
            f"WHERE load_ts >= TIMESTAMP '{ts(START)}' AND load_ts < TIMESTAMP '{ts(CUT)}')").collect()[0].n
        assert counted == cand_untimed, (seed, counted, cand_untimed)
        # no PASS-class disposition on a key with relevant untimed rows
        assert all(v["disposition"].startswith("unassessable") for v in keys.values() if v["untimed_relevant"])


def test_g07_determinism_across_shuffle_partitions(spark) -> None:
    up, dn = generated_population(9)
    prior = spark.conf.get("spark.sql.shuffle.partitions")
    results = []
    try:
        for n in ("1", "8"):
            spark.conf.set("spark.sql.shuffle.partitions", n)
            out = run(spark, up, dn, p=Params(CUT, START, END, disorder_hours=72))
            results.append({k: (v["line"], v["disposition"]) for k, v in out["keys"].items()})
    finally:
        spark.conf.set("spark.sql.shuffle.partitions", prior)
    assert results[0] == results[1]
