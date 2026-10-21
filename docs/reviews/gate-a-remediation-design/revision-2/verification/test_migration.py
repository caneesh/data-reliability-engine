"""Revision 2, part E: migration and rollback. Failure injection on the REAL writer (scratch store),
then prototypes of the proposed tolerant append and of the key-event state machine (REKEYED events,
predecessor lineage, remap with collision fan-out, interruption, resume, rollback, re-upgrade).
Prototypes are verification code, not engine code.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.local_setup import create_store

DQ = "dq_rev2_migration"


@pytest.fixture(scope="module")
def store(spark):
    spark.sql(f"DROP DATABASE IF EXISTS {DQ} CASCADE")
    create_store(spark, DQ)
    yield DQ
    spark.sql(f"DROP DATABASE IF EXISTS {DQ} CASCADE")


def event_frame(spark, with_encoding: bool):
    cols = [c for c in spark.table(f"{DQ}.dq_key_event").columns if with_encoding or c != "key_encoding"]
    schema = spark.table(f"{DQ}.dq_key_event").select(*cols).schema
    row = {c: None for c in cols}
    row.update({"run_id": "r1", "evaluation_id": "e1", "dataset": "d", "check_id": "HOP_KEY_CURRENCY", "hop": "u->d",
                "key_hash": "h", "key_value": "k", "event": "FLAGGED", "state_detail": "MISSING",
                "observed_at": datetime(2026, 1, 16), "run_date": datetime(2026, 1, 16).date()})
    return spark.createDataFrame([tuple(row[c] for c in cols)], schema)


# --- failure injection on the real writer ---

def test_m1_the_current_writer_fails_after_a_column_is_added(spark, store) -> None:
    """An engine that predates migration 2 (its frames lack key_encoding) cannot append to a migrated
    dq_key_event: the real store/writer.append raises. Fail closed (no evidence written), not a rollback."""
    frame = event_frame(spark, with_encoding=False)
    writer.append(frame, DQ, "dq_key_event")  # before the migration: fine
    spark.sql(f"ALTER TABLE {DQ}.dq_key_event ADD COLUMNS (key_encoding INT)")  # what migration 2 would do
    with pytest.raises(ValueError, match="missing \\['key_encoding'\\]"):
        writer.append(event_frame(spark, with_encoding=False), DQ, "dq_key_event")


def tolerant_append(spark, df, db: str, table: str, added: dict[str, set[str]]) -> None:
    """Prototype of release N's writer (revision 2, 06): a column absent from the frame is filled with NULL
    only when a migration added it; any other missing column, or any extra column, still raises."""
    from pyspark.sql import functions as F

    table_columns = spark.table(f"{db}.{table}").columns
    have = {c.lower() for c in df.columns}
    missing = [c for c in table_columns if c.lower() not in have]
    extra = [c for c in df.columns if c.lower() not in {t.lower() for t in table_columns}]
    tolerated = [c for c in missing if c.lower() in added.get(table, set())]
    if extra or set(missing) - set(tolerated):
        raise ValueError(f"columns do not match {db}.{table}: missing {missing}, unexpected {extra}")
    types = dict(spark.table(f"{db}.{table}").dtypes)
    for c in tolerated:
        df = df.withColumn(c, F.lit(None).cast(types[c]))
    df.select(*table_columns).write.insertInto(f"{db}.{table}")


def test_m2_the_tolerant_append_writes_null_only_for_a_migration_added_column(spark, store) -> None:
    added = {"dq_key_event": {"key_encoding"}}
    tolerant_append(spark, event_frame(spark, with_encoding=False), DQ, "dq_key_event", added)
    got = spark.sql(f"SELECT key_encoding FROM {DQ}.dq_key_event WHERE run_id = 'r1'").collect()
    assert [r.key_encoding for r in got][-1] is None
    with pytest.raises(ValueError):  # another missing column still fails
        tolerant_append(spark, event_frame(spark, with_encoding=False).drop("state_detail"), DQ, "dq_key_event", added)
    with pytest.raises(ValueError):  # an extra column still fails
        from pyspark.sql import functions as F
        tolerant_append(spark, event_frame(spark, with_encoding=True).withColumn("x", F.lit(1)), DQ, "dq_key_event", added)


# --- the key-event state machine (prototype over a temp view) ---

EVENTS = "hash STRING, key_value STRING, event STRING, detail STRING, enc INT, predecessor STRING, at TIMESTAMP, committed BOOLEAN"
OPEN = ("FLAGGED", "STILL_FLAGGED")
T0 = datetime(2026, 1, 10)


def events_view(spark, rows: list[tuple]) -> None:
    spark.createDataFrame(rows, EVENTS).createOrReplaceTempView("ev")


def latest(spark, where: str, ignore_rekeyed: bool) -> dict[str, str]:
    filt = " AND event <> 'REKEYED'" if ignore_rekeyed else ""
    rows = spark.sql(f"""SELECT hash, event FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY hash ORDER BY at DESC) rn
                         FROM ev WHERE committed AND {where}{filt}) WHERE rn = 1""").collect()
    return {r.hash: r.event for r in rows}


def open_for_new_engine(spark) -> set[str]:
    return {h for h, e in latest(spark, "enc = 2", False).items() if e in OPEN}


def legacy_needing_remap(spark) -> set[str]:
    return {h for h, e in latest(spark, "coalesce(enc, 1) = 1", False).items() if e in OPEN}


def open_for_release_n(spark) -> set[str]:
    """Release N (rollback target): reads encoding 1 only and does not take REKEYED as a resolution."""
    return {h for h, e in latest(spark, "coalesce(enc, 1) = 1", True).items() if e in OPEN}


def foreign_for_release_n(spark) -> set[str]:
    return {h for h, e in latest(spark, "enc = 2", False).items() if e in OPEN}


def v1(parts):  # the legacy encoding
    return "|".join("<null>" if p is None else p for p in parts)


def v2(parts):  # LPB
    return "".join("~;" if p is None else f"{len(p.encode())}:{p};" for p in parts)


def remap(legacy_open: dict[str, str], upstream_keys: list[tuple], still_wrong: set[tuple], at: datetime) -> list[tuple]:
    """N+1's transition for one hop (prototype): map each legacy open hash to the business keys whose v1
    string equals its key_value (computed over every upstream row). 1:1 -> REKEYED + continue under v2
    (predecessor = the legacy hash, age inherited); 1:n (a legacy collision) -> REKEYED 'ambiguous successor'
    and each still-wrong key FLAGGED anew (predecessor recorded, age not inherited); 1:0 -> CLEARED 'no
    longer upstream' (the remap read every upstream row, so this is the full-sweep conclusion)."""
    out = []
    for h, kv in legacy_open.items():
        succ = [k for k in upstream_keys if v1(k) == kv]
        if not succ:
            out.append((h, kv, "CLEARED", "no longer upstream", 1, None, at, True))
            continue
        out.append((h, kv, "REKEYED", "ambiguous successor" if len(succ) > 1 else "key encoding 2", 1, None, at, True))
        for k in succ:
            if k in still_wrong:
                ev = "STILL_FLAGGED" if len(succ) == 1 else "FLAGGED"
                out.append((f"h2:{v2(k)}", v2(k), ev, "MISSING", 2, h, at + timedelta(seconds=1), True))
    return out


K1, K2, K3, K4 = ("1", "01"), ("2|x", "y"), ("2", "x|y"), ("4", "01")   # K2 and K3 collide under v1


def legacy_state() -> list[tuple]:
    return [(f"h1:{v1(K1)}", v1(K1), "FLAGGED", "MISSING", None, None, T0, True),
            (f"h1:{v1(K2)}", v1(K2), "FLAGGED", "MISSING", None, None, T0, True),       # shared by K2 and K3
            (f"h1:{v1(K4)}", v1(K4), "FLAGGED", "MISSING", None, None, T0, True)]       # K4 has left upstream


def test_m3_upgrade_remap_preserves_open_discrepancies_and_lineage(spark) -> None:
    rows = legacy_state()
    events_view(spark, rows)
    legacy = {h: kv for h, kv, *_ in rows}
    assert legacy_needing_remap(spark) == set(legacy)
    rows += remap(legacy, [K1, K2, K3], still_wrong={K1, K3}, at=T0 + timedelta(days=1))
    events_view(spark, rows)
    assert open_for_new_engine(spark) == {f"h2:{v2(K1)}", f"h2:{v2(K3)}"}   # K1 continued, K3 flagged; K2 is fine
    assert legacy_needing_remap(spark) == set()                             # transition complete: nothing forced again
    lineage = {r[0]: r[5] for r in rows if r[4] == 2}
    assert lineage[f"h2:{v2(K1)}"] == f"h1:{v1(K1)}" and lineage[f"h2:{v2(K3)}"] == f"h1:{v1(K2)}"
    details = {r[0]: r[3] for r in rows if r[2] in ("REKEYED", "CLEARED") and r[4] == 1}
    assert details[f"h1:{v1(K2)}"] == "ambiguous successor" and details[f"h1:{v1(K4)}"] == "no longer upstream"


def test_m4_an_interrupted_transition_is_invisible_and_the_rerun_is_idempotent(spark) -> None:
    rows = legacy_state()
    legacy = {h: kv for h, kv, *_ in rows}
    attempt = [(*r[:7], False) for r in remap(legacy, [K1, K2, K3], {K1, K3}, T0 + timedelta(days=1))]  # never committed
    events_view(spark, rows + attempt)
    assert legacy_needing_remap(spark) == set(legacy) and open_for_new_engine(spark) == set()  # nothing half-applied
    done = remap(legacy, [K1, K2, K3], {K1, K3}, T0 + timedelta(days=2))
    events_view(spark, rows + attempt + done)
    assert legacy_needing_remap(spark) == set()
    assert open_for_new_engine(spark) == {f"h2:{v2(K1)}", f"h2:{v2(K3)}"}


def test_m5_rollback_to_release_n_keeps_every_open_discrepancy_visible(spark) -> None:
    """After N+1 ran, roll back to N. N reads encoding 1, ignores REKEYED (not a resolution), and sees
    N+1's open encoding-2 keys as foreign: it forces a full sweep (prototype: re-flag under v1) and closes
    them with REKEYED. Every discrepancy open before the rollback is open after it."""
    rows = legacy_state()
    legacy = {h: kv for h, kv, *_ in rows}
    rows += remap(legacy, [K1, K2, K3], {K1, K3}, T0 + timedelta(days=1))
    events_view(spark, rows)
    before = {r[1] for r in rows if r[0] in open_for_new_engine(spark)}            # business keys open (v2 strings)
    assert open_for_release_n(spark) == {f"h1:{v1(K1)}", f"h1:{v1(K2)}"}        # REKEYED ignored by N; K4 cleared
    foreign = foreign_for_release_n(spark)
    at = T0 + timedelta(days=3)
    back = []
    for h in foreign:
        kv = next(r[1] for r in rows if r[0] == h)
        back.append((h, kv, "REKEYED", "key encoding 1 (rollback)", 2, None, at, True))
        k = next(k for k in (K1, K2, K3) if v2(k) == kv)
        back.append((f"h1:{v1(k)}", v1(k), "STILL_FLAGGED", "MISSING", 1, h, at + timedelta(seconds=1), True))
    events_view(spark, rows + back)
    after_hashes = open_for_release_n(spark)
    assert foreign_for_release_n(spark) == set()
    covered = {k for k in (K1, K2, K3) if f"h1:{v1(k)}" in after_hashes}
    assert {v2(k) for k in covered} | {v2(K2)} >= before  # K3 is open under its shared legacy identity (h1 of K2/K3)


def test_m6_re_upgrade_after_a_rollback_remaps_again(spark) -> None:
    rows = legacy_state()
    legacy = {h: kv for h, kv, *_ in rows}
    rows += remap(legacy, [K1, K2, K3], {K1, K3}, T0 + timedelta(days=1))
    rows.append((f"h1:{v1(K1)}", v1(K1), "STILL_FLAGGED", "MISSING", 1, None, T0 + timedelta(days=3), True))  # N ran
    events_view(spark, rows)
    assert f"h1:{v1(K1)}" in legacy_needing_remap(spark)


def test_m7_every_transition_event_is_auditable(spark) -> None:
    rows = legacy_state()
    legacy = {h: kv for h, kv, *_ in rows}
    new = remap(legacy, [K1, K2, K3], {K1, K3}, T0 + timedelta(days=1))
    assert all(r[4] in (1, 2) for r in new)                       # every new event names its encoding
    assert all(r[5] for r in new if r[4] == 2)                    # every continued or re-flagged key names its predecessor
    assert all(r[0] in legacy for r in new if r[4] == 1)          # legacy events only ever close legacy identities
