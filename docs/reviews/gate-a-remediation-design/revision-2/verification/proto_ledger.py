"""A MODEL of the revision-2 ledger rules in Spark SQL: verification only, NOT engine code.

It implements, on a deliberately small schema, the rules proposed in revision 2:
  identity   LPB key encoding; raw-variant guard; unparseable key parts (02)
  time       relevant untimed rows (with an optional declared disorder bound D, load vs load) (03)
  ties       tied version rows must agree on expected presence; rejects in a total order (04)
  downstream unparseable target keys never match; counted (05)
so that the rules and their combinations can be exercised and the invariants checked (07).

Schema (temp views):
  up(id INT, k1 STRING, eff STRING, rec STRING, load_ts TIMESTAMP, val STRING, tomb BOOLEAN, x1 BOOLEAN, x2 BOOLEAN)
  dn(id INT, k1 STRING, eff STRING, rec STRING, val STRING)
  rej(k1 STRING, eff STRING, version STRING, reason STRING, defect BOOLEAN, rejected_ts TIMESTAMP)
Key = (k1, eff); eff is normalised with parse_date 'yyyy-MM-dd'. The due range is on the due load
(sla folded in): [range_start, range_end). cutoff = the window end.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


def lpb(parts: list[str]) -> str:
    terms = [f"CASE WHEN {p} IS NULL THEN '~;' ELSE concat(octet_length({p}), ':', {p}, ';') END" for p in parts]
    return f"concat({', '.join(terms)})"


NORM_EFF = "date_format(to_date(eff, 'yyyy-MM-dd'), 'yyyy-MM-dd')"
# A normalised part whose raw value is not NULL but whose normalised value is: it keeps its own identity,
# marked '!' with its raw bytes, so it never merges with a genuinely NULL part (02, rule U2).
EFF_PART = (f"CASE WHEN eff IS NOT NULL AND {NORM_EFF} IS NULL THEN concat('!', octet_length(eff), ':', eff, ';') "
            f"ELSE (CASE WHEN {NORM_EFF} IS NULL THEN '~;' ELSE concat(octet_length({NORM_EFF}), ':', {NORM_EFF}, ';') END) END")


def key_sql() -> str:
    return f"concat(CASE WHEN k1 IS NULL THEN '~;' ELSE concat(octet_length(k1), ':', k1, ';') END, {EFF_PART})"


@dataclass
class Params:
    cutoff: datetime
    range_start: datetime
    range_end: datetime
    disorder_hours: float | None = None  # declared bound D (None: not declared)
    orphans_checked: bool = True


def classify(spark, p: Params) -> dict:
    lit = lambda t: f"TIMESTAMP '{t:%Y-%m-%d %H:%M:%S}'"  # noqa: E731
    relevant = ("TRUE" if p.disorder_hours is None
                else f"load_ts >= v_load - make_dt_interval(0, {int(p.disorder_hours)}, 0, 0)")
    sql = f"""
WITH up_rows AS (
  SELECT id, {key_sql()} AS k, {lpb(['k1', 'eff'])} AS raw_k,
         (eff IS NULL OR to_date(eff, 'yyyy-MM-dd') IS NOT NULL) AS parsed,
         to_timestamp(rec) AS record_ts, rec, load_ts, val, tomb, x1, x2
  FROM up),
up_cut AS (SELECT * FROM up_rows WHERE load_ts IS NULL OR load_ts < {lit(p.cutoff)}),
cand AS (SELECT DISTINCT k FROM up_cut WHERE load_ts >= {lit(p.range_start)}),
r1 AS (SELECT u.*, MAX(record_ts) OVER w AS top_ts, size(collect_set(raw_k) OVER w) AS raw_variants,
              MIN(CASE WHEN parsed THEN 1 ELSE 0 END) OVER w AS key_parsed
       FROM up_cut u JOIN cand c ON c.k = u.k WINDOW w AS (PARTITION BY u.k)),
r2 AS (SELECT r1.*, MAX(CASE WHEN record_ts = top_ts THEN load_ts END) OVER (PARTITION BY k) AS v_load FROM r1),
r3 AS (SELECT r2.*, record_ts IS NULL AS untimed,
              CASE WHEN record_ts IS NOT NULL THEN NULL
                   WHEN top_ts IS NULL OR load_ts IS NULL OR v_load IS NULL THEN TRUE
                   ELSE {relevant} END AS relevant,
              record_ts = top_ts AS is_version,
              CASE WHEN tomb OR x1 OR x2 THEN 'absent' ELSE 'present' END AS expected
       FROM r2),
keys AS (
  SELECT k, MAX(raw_variants) AS raw_variants, MIN(key_parsed) AS key_parsed, MAX(top_ts) AS up_record_ts,
         SUM(CASE WHEN untimed AND relevant THEN 1 ELSE 0 END) AS untimed_relevant,
         SUM(CASE WHEN untimed AND relevant AND rec IS NOT NULL THEN 1 ELSE 0 END) AS unparsed_relevant,
         SUM(CASE WHEN untimed AND NOT relevant THEN 1 ELSE 0 END) AS set_aside,
         COUNT(*) AS key_rows,
         greatest(MAX(v_load), MAX(CASE WHEN untimed AND relevant THEN load_ts END)) AS due_load,
         COUNT(DISTINCT CASE WHEN is_version THEN expected END) AS expected_states,
         MIN(CASE WHEN is_version THEN CASE WHEN tomb THEN 1 ELSE 0 END END) AS all_tomb,
         MAX(CASE WHEN is_version THEN CASE WHEN tomb THEN 1 ELSE 0 END END) AS any_tomb,
         MIN(CASE WHEN is_version THEN CASE WHEN tomb OR x1 OR x2 THEN 1 ELSE 0 END END) AS all_absent,
         MAX(CASE WHEN is_version THEN CASE WHEN tomb OR x1 OR x2 THEN 1 ELSE 0 END END) AS any_rule,
         COUNT(DISTINCT CASE WHEN is_version THEN to_json(named_struct('v', val, 't', tomb, 'a', x1, 'b', x2)) END) AS signatures,
         array_sort(collect_set(CASE WHEN is_version THEN coalesce(val, '~') END)) AS up_vals
  FROM r3 GROUP BY k),
dn_rows AS (SELECT {key_sql()} AS k, (eff IS NULL OR to_date(eff, 'yyyy-MM-dd') IS NOT NULL) AS parsed,
                   to_timestamp(rec) AS record_ts, val FROM dn),
dn_ok AS (SELECT * FROM dn_rows WHERE parsed),
dn_keys AS (
  SELECT k, MAX(record_ts) AS dn_record_ts, COUNT(*) - COUNT(record_ts) AS dn_untimed FROM dn_ok GROUP BY k),
dn_vals AS (
  SELECT d.k, array_sort(collect_set(coalesce(d.val, '~'))) AS dn_vals
  FROM dn_ok d JOIN dn_keys n ON n.k = d.k WHERE d.record_ts = n.dn_record_ts GROUP BY d.k),
rej_rows AS (SELECT {key_sql()} AS k, to_timestamp(version) AS version_ts, reason, defect, rejected_ts FROM rej),
rejected AS (
  SELECT r.k, max_by(named_struct('reason', r.reason, 'defect', r.defect),
                     named_struct('t', r.rejected_ts, 'd', CASE WHEN r.defect THEN 1 ELSE 0 END, 'r', r.reason)) AS latest
  FROM rej_rows r JOIN keys u ON u.k = r.k AND r.version_ts = u.up_record_ts GROUP BY r.k),
judged AS (
  SELECT u.*, n.k IS NOT NULL AS present, n.dn_record_ts, n.dn_untimed, v.dn_vals, j.latest,
         CASE WHEN u.due_load IS NULL THEN 'untimed'
              WHEN u.due_load >= {lit(p.range_end)} THEN 'pending'
              WHEN u.due_load >= {lit(p.range_start)} THEN 'due'
              ELSE 'judged_before' END AS line
  FROM keys u LEFT JOIN dn_keys n ON n.k = u.k LEFT JOIN dn_vals v ON v.k = u.k LEFT JOIN rejected j ON j.k = u.k)
SELECT k, line, key_rows, set_aside, untimed_relevant,
  CASE
    WHEN raw_variants > 1 THEN 'unassessable:key collision'
    WHEN key_parsed = 0 THEN 'unassessable:unparseable key'
    WHEN unparsed_relevant > 0 THEN 'unassessable:unparseable record time'
    WHEN untimed_relevant > 0 THEN 'unassessable:no record time'
    WHEN present AND dn_untimed > 0 THEN 'unassessable:no record time here'
    WHEN any_rule = 1 AND expected_states > 1 THEN 'unassessable:tied rows disagree'
    WHEN latest IS NOT NULL AND signatures > 1 THEN 'unassessable:reject ambiguous among tied rows'
    WHEN all_tomb = 1 AND NOT present THEN 'deleted'
    WHEN all_tomb = 1 AND present THEN 'deleted_not_applied'
    WHEN all_absent = 1 AND NOT present THEN CASE WHEN any_tomb = 1 THEN 'deleted:mixed' ELSE 'excluded' END
    WHEN all_absent = 1 AND present THEN CASE WHEN any_tomb = 1 THEN 'deleted_not_applied' ELSE 'excluded_but_present' END
    WHEN latest IS NOT NULL THEN CASE WHEN latest.defect THEN 'rejected:defect' ELSE 'rejected:authorised' END
    WHEN NOT present THEN 'missing'
    WHEN dn_record_ts < up_record_ts THEN 'stale'
    WHEN up_vals <> dn_vals THEN 'divergent'
    ELSE 'delivered' END AS disposition
FROM judged
"""
    rows = {r.k: r.asDict() for r in spark.sql(sql).collect()}
    dn_unparsed = spark.sql("SELECT COUNT(*) AS n FROM dn WHERE eff IS NOT NULL AND to_date(eff, 'yyyy-MM-dd') IS NULL"
                            ).collect()[0].n
    up_untimed = spark.sql(f"SELECT COUNT(*) AS n FROM up WHERE (load_ts IS NULL OR load_ts < {lit(p.cutoff)}) "
                           f"AND to_timestamp(rec) IS NULL").collect()[0].n
    return {"keys": rows, "orphan_unparseable" if p.orphans_checked else "scope_unparseable_target_rows": dn_unparsed,
            "up_untimed_rows": up_untimed}


def load(spark, up: list[dict], dn: list[dict], rej: list[dict] | None = None) -> None:
    up_schema = "id INT, k1 STRING, eff STRING, rec STRING, load_ts TIMESTAMP, val STRING, tomb BOOLEAN, x1 BOOLEAN, x2 BOOLEAN"
    defaults = {"eff": "2026-01-01", "val": "v", "tomb": False, "x1": False, "x2": False}
    rows = [{**defaults, **r, "id": i} for i, r in enumerate(up)]
    cols = [c.split()[0] for c in up_schema.split(", ")]
    spark.createDataFrame([tuple(r.get(c) for c in cols) for r in rows], up_schema).createOrReplaceTempView("up")
    dn_schema = "id INT, k1 STRING, eff STRING, rec STRING, val STRING"
    dcols = [c.split()[0] for c in dn_schema.split(", ")]
    drows = [{"eff": "2026-01-01", "val": "v", **r, "id": i} for i, r in enumerate(dn)]
    spark.createDataFrame([tuple(r.get(c) for c in dcols) for r in drows], dn_schema).createOrReplaceTempView("dn")
    rj_schema = "k1 STRING, eff STRING, version STRING, reason STRING, defect BOOLEAN, rejected_ts TIMESTAMP"
    rcols = [c.split()[0] for c in rj_schema.split(", ")]
    rrows = [{"eff": "2026-01-01", **r} for r in (rej or [])]
    spark.createDataFrame([tuple(r.get(c) for c in rcols) for r in rrows], rj_schema).createOrReplaceTempView("rej")


def key(k1: str, eff: str | None = "2026-01-01") -> str:
    """The LPB key of (k1, eff) as the model computes it (Python mirror, with the '!' unparseable marker)."""
    from datetime import datetime as _dt

    def part(v: str | None) -> str:
        return "~;" if v is None else f"{len(v.encode('utf-8'))}:{v};"
    if eff is None:
        return part(k1) + "~;"
    try:
        norm = _dt.strptime(eff, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return part(k1) + f"!{len(eff.encode('utf-8'))}:{eff};"
    return part(k1) + part(norm)
