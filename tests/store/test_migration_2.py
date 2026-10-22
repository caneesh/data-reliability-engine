"""Gate A remediation, release N: migration 2 and the writer's tolerance of migration-added columns.

Migration 2 adds `key_encoding INT` and `predecessor_hash STRING` to `dq_key_event` (spec section 5).
A stored row reads them as NULL, which means key encoding 1. The writer fills a column that a
migration added, and that a frame lacks, with a typed NULL; any other missing or extra column still
raises (spec section 5; decisions, "Gate A remediation").
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone

import pytest

from hcsc.datalake.dre.store import migrations, writer
from hcsc.datalake.dre.store.install import PENDING, check_store, install_command, missing_objects
from hcsc.datalake.dre.store.key_events import append_key_events
from hcsc.datalake.dre.store.local_setup import create_database, create_store
from hcsc.datalake.dre.store.schema import expected_objects, render_ddl

DAY = date(2026, 3, 1)
AT = datetime(2026, 3, 1, 12, tzinfo=timezone.utc)
ADDED = ("key_encoding", "predecessor_hash")


def release_n_minus_1_store(spark, db: str, keep: tuple[str, ...] = ()) -> None:
    """A store as the release before N left it: dq_key_event without migration 2's columns (but those in
    `keep`, for a migration interrupted part-way)."""
    create_database(spark, db)
    for statement in render_ddl(db):
        if statement.upper().startswith("CREATE TABLE"):
            for column in ADDED:
                if column not in keep:
                    statement = re.sub(rf",?\s*{column} \w+", "", statement)
            spark.sql(statement)


def legacy_event_rows(spark, db: str) -> None:
    """Two key events written by the release before N (no migration 2 columns in the frame)."""
    cols = [c for c in spark.table(f"{db}.dq_key_event").columns if c not in ADDED]
    schema = spark.table(f"{db}.dq_key_event").select(*cols).schema
    base = {c: None for c in cols}
    rows = [{**base, "run_id": "r1", "evaluation_id": "e1", "dataset": "d", "check_id": "HOP_KEY_CURRENCY",
             "hop": "u->d", "key_hash": h, "key_value": f"synthetic-{h}", "event": "FLAGGED",
             "state_detail": "MISSING", "observed_at": AT, "run_date": DAY} for h in ("h1", "h2")]
    writer.append(spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], schema), db, "dq_key_event")


def test_migration_2_is_declared_after_migration_1() -> None:
    m = migrations.MIGRATIONS[-1]
    assert (m.version, m.table, m.columns) == (2, "dq_key_event", (("key_encoding", "INT"),
                                                                    ("predecessor_hash", "STRING")))


def test_a_fresh_store_has_the_final_key_event_columns(spark) -> None:
    create_store(spark, "dq_mig2_fresh")
    tables, views = expected_objects("dq_mig2_fresh")
    assert [c for c, _ in tables["dq_key_event"].columns][-3:] == ["key_encoding", "predecessor_hash", "run_date"]
    actual = [f.name for f in spark.table("dq_mig2_fresh.dq_key_event").schema.fields]
    assert actual == [c for c, _ in tables["dq_key_event"].columns]
    assert {"v_open_keys", "v_key_lineage"} <= set(views)
    assert migrations.pending(spark, "dq_mig2_fresh") == []


def test_an_existing_store_is_migrated_without_rewriting_a_row(spark, capsys) -> None:
    db = "dq_mig2_old"
    release_n_minus_1_store(spark, db)
    legacy_event_rows(spark, db)
    assert [m.version for m in migrations.pending(spark, db)] == [2]
    assert f"{db}.dq_key_event (migration pending)" in missing_objects(spark, db)  # dre run refuses to start
    assert {f.name: f.status for f in check_store(spark, db)}[f"{db}.dq_key_event"] == PENDING
    assert install_command(spark, db, "check") == 1

    assert install_command(spark, db, "apply") == 0
    assert ("applied migration 2: dq_key_event ADD COLUMNS (key_encoding INT, predecessor_hash STRING)"
            in capsys.readouterr().out)
    assert migrations.pending(spark, db) == [] and install_command(spark, db, "check") == 0
    rows = spark.table(f"{db}.dq_key_event").orderBy("key_hash").collect()
    assert [(r.key_hash, r.event, r.key_encoding, r.predecessor_hash) for r in rows] == [
        ("h1", "FLAGGED", None, None), ("h2", "FLAGGED", None, None)]  # kept as written; NULL means encoding 1
    fresh, _ = expected_objects(db)
    assert [f.name for f in spark.table(f"{db}.dq_key_event").schema.fields] == [
        c for c, _ in fresh["dq_key_event"].columns]  # same columns, same order, as a new store


def test_reapplying_migration_2_changes_nothing(spark) -> None:
    db = "dq_mig2_again"
    release_n_minus_1_store(spark, db)
    legacy_event_rows(spark, db)
    assert install_command(spark, db, "apply") == 0
    before = sorted(map(tuple, spark.table(f"{db}.dq_key_event").collect()))
    assert install_command(spark, db, "apply") == 0
    assert migrations.apply_pending(spark, db) == []
    assert sorted(map(tuple, spark.table(f"{db}.dq_key_event").collect())) == before


def test_a_migration_interrupted_after_one_column_is_detected_and_completed(spark) -> None:
    """ALTER TABLE ADD COLUMNS runs once per migration; a store left with only key_encoding (an interrupted
    or hand-applied change) is still pending, refused by dre run, and completed by the next apply."""
    db = "dq_mig2_partial"
    release_n_minus_1_store(spark, db, keep=("key_encoding",))
    assert [m.version for m in migrations.pending(spark, db)] == [2]
    assert f"{db}.dq_key_event (migration pending)" in missing_objects(spark, db)
    assert install_command(spark, db, "apply") == 0
    assert migrations.pending(spark, db) == []
    names = [f.name for f in spark.table(f"{db}.dq_key_event").schema.fields]
    assert names[-3:] == ["key_encoding", "predecessor_hash", "run_date"]


def test_key_events_are_written_with_key_encoding_1(spark) -> None:
    db = "dq_mig2_events"
    create_store(spark, db)
    events = spark.createDataFrame([("h1", "synthetic-1", "FLAGGED", "MISSING")],
                                   "key_hash STRING, key_value STRING, event STRING, state_detail STRING")
    assert append_key_events(events, db, "r1", "e1", "d", "HOP_KEY_CURRENCY", "u->d", AT, DAY) == 1
    [row] = spark.table(f"{db}.dq_key_event").collect()
    assert (row.key_hash, row.key_encoding, row.predecessor_hash) == ("h1", 1, None)


def test_a_frame_naming_its_encoding_keeps_it(spark) -> None:
    """The rollback path writes REKEYED for an identity of a later encoding with that identity's encoding."""
    db = "dq_mig2_events_enc"
    create_store(spark, db)
    events = spark.createDataFrame([("h2", "synthetic-2", "REKEYED", "key encoding 1 (rollback)", 2)],
                                   "key_hash STRING, key_value STRING, event STRING, state_detail STRING, "
                                   "key_encoding INT")
    append_key_events(events, db, "r1", "e1", "d", "HOP_KEY_CURRENCY", "u->d", AT, DAY)
    [row] = spark.table(f"{db}.dq_key_event").collect()
    assert (row.event, row.key_encoding, row.predecessor_hash) == ("REKEYED", 2, None)


# --- the writer ---

@pytest.fixture(scope="module")
def migrated(spark) -> str:
    db = "dq_mig2_writer"
    create_store(spark, db)
    return db


def frame_without(spark, db: str, drop: tuple[str, ...], extra: bool = False):
    from pyspark.sql import functions as F

    cols = [c for c in spark.table(f"{db}.dq_key_event").columns if c not in drop]
    schema = spark.table(f"{db}.dq_key_event").select(*cols).schema
    row = {c: None for c in cols}
    row.update({"run_id": "w1", "evaluation_id": "we1", "key_hash": "wh", "event": "FLAGGED", "run_date": DAY})
    frame = spark.createDataFrame([tuple(row[c] for c in cols)], schema)
    return frame.withColumn("unexpected", F.lit(1)) if extra else frame


def test_the_writer_fills_a_migration_added_column_with_null(spark, migrated) -> None:
    writer.append(frame_without(spark, migrated, ADDED), migrated, "dq_key_event")
    [row] = spark.table(f"{migrated}.dq_key_event").filter("run_id = 'w1'").collect()
    assert (row.key_encoding, row.predecessor_hash) == (None, None)


def test_the_writer_still_refuses_any_other_missing_column(spark, migrated) -> None:
    with pytest.raises(ValueError, match=r"missing \['state_detail'\]"):
        writer.append(frame_without(spark, migrated, ("state_detail",)), migrated, "dq_key_event")
    with pytest.raises(ValueError, match=r"missing \['state_detail'\]"):
        writer.append(frame_without(spark, migrated, ("state_detail", *ADDED)), migrated, "dq_key_event")


def test_the_writer_still_refuses_an_extra_column(spark, migrated) -> None:
    with pytest.raises(ValueError, match=r"unexpected \['unexpected'\]"):
        writer.append(frame_without(spark, migrated, (), extra=True), migrated, "dq_key_event")


def test_the_writer_tolerates_migration_1_too(spark, migrated) -> None:
    """Migration 1's column (dq_check_result.evaluation_mode) is tolerated the same way."""
    cols = [c for c in spark.table(f"{migrated}.dq_check_result").columns if c != "evaluation_mode"]
    schema = spark.table(f"{migrated}.dq_check_result").select(*cols).schema
    row = {c: None for c in cols}
    row.update({"run_id": "w2", "evaluation_id": "we2", "dataset": "d", "check_id": "T1_ZERO_ROWS",
                "state": "PASSED", "run_date": DAY})
    writer.append(spark.createDataFrame([tuple(row[c] for c in cols)], schema), migrated, "dq_check_result")
    [stored] = spark.table(f"{migrated}.dq_check_result").filter("run_id = 'w2'").collect()
    assert stored.evaluation_mode is None


def test_a_release_n_minus_1_writer_cannot_write_to_a_migrated_store() -> None:
    """Rollback below N once migration 2 is applied is unsupported: the release before N required a frame
    to carry exactly the table's columns. Documented, not code: the spec says so (section 6, Key events)."""
    import pathlib

    spec = (pathlib.Path(__file__).resolve().parents[2] / "docs" / "spec.md").read_text(encoding="utf-8")
    assert "Rollback below release N, once migration 2 is applied, is unsupported" in spec
