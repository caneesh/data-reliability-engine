"""Evaluation modes (build step 4; ADR-05; reconciliation model section 8).

NORMAL is DRE's historical record. A RERUN or REPLAY is a CURRENT_RECHECK: it reads the tables as
they are now, is stored and labelled as such, and never stands for a historical result. An
AS_OF_REPLAY reads a past window as the data stood at its end, only where that can be proven: every
dataset read append_only, with T1_PARTITION_DRIFT showing it unchanged since the cutoff. Otherwise it
is DID_NOT_RUN / no_as_of_evidence (proof scenario 7).

The store gains `dq_check_result.evaluation_mode` through migration 1, the first versioned, additive
migration: an existing store keeps every row, which reads its mode from its execution type.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone

import pytest

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.hop.ledger import HopLedger
from hcsc.datalake.dre.checks.modes import mode_of
from hcsc.datalake.dre.checks.tier1.key_nulls import KeyNulls
from hcsc.datalake.dre.cli import main
from hcsc.datalake.dre.config.models import (
    EdgeContract, Grain, RejectLineage, RejectsSource, TimeColumn,
)
from hcsc.datalake.dre.store import migrations
from hcsc.datalake.dre.store.install import PENDING, apply_ddl, check_store, install_command, missing_objects
from hcsc.datalake.dre.store.local_setup import create_database, create_store
from hcsc.datalake.dre.store.schema import render_ddl
from tests.checks.test_hop import EVENT, GOLD_LOAD, IN_WINDOW, curated, gold
from tests.fixtures.settings import SETTINGS
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.replay.conftest import latest, replay_conf
from tests.store.conftest import append_rows

UTC = timezone.utc
DQ = "dq_modes"
CUTOFF = EVENT.window_end  # 2026-01-16 12:00 UTC


def test_each_execution_type_has_its_mode() -> None:
    assert [mode_of(t) for t in ("NORMAL", "RERUN", "REPLAY", "AS_OF_REPLAY")] == [
        "NORMAL", "CURRENT_RECHECK", "CURRENT_RECHECK", "AS_OF_REPLAY"]


# --- migration 1 ---

def checkpoint3_store(spark, db: str) -> None:
    """A store as `dre install --apply` left it at checkpoint 3: the tables without evaluation_mode
    (views are replaced on every apply, so the old ones do not matter)."""
    create_database(spark, db)
    for statement in render_ddl(db):
        if statement.upper().startswith("CREATE TABLE"):
            spark.sql(re.sub(r",\s*evaluation_mode STRING[^\n]*", "", statement))


def test_a_checkpoint_3_store_is_migrated_without_rewriting_a_row(spark, capsys) -> None:
    db = "dq_mig_old"
    checkpoint3_store(spark, db)
    day = date(2026, 3, 1)
    at = datetime(2026, 3, 1, 12, tzinfo=UTC)
    append_rows(spark, db, "dq_check_result", [
        {"evaluation_id": "n", "run_id": "r1", "dataset": "a", "check_id": "T1_ZERO_ROWS", "state": "PASSED",
         "execution_type": "NORMAL", "window_end": at, "evaluated_at": at, "run_date": day},
        {"evaluation_id": "r", "run_id": "r2", "dataset": "b", "check_id": "T1_ZERO_ROWS", "state": "FAILED",
         "execution_type": "RERUN", "window_end": at, "evaluated_at": at, "run_date": day}])
    assert [m.version for m in migrations.pending(spark, db)] == [1]
    findings = {f.name: f for f in check_store(spark, db)}
    assert findings[f"{db}.dq_check_result"].status == PENDING
    assert f"{db}.dq_check_result (migration pending)" in missing_objects(spark, db)  # dre run refuses to start
    assert install_command(spark, db, "check") == 1

    assert install_command(spark, db, "apply") == 0
    assert "applied migration 1: dq_check_result ADD COLUMNS (evaluation_mode STRING)" in capsys.readouterr().out
    assert migrations.pending(spark, db) == []
    assert install_command(spark, db, "check") == 0  # same columns, same order, as a new store
    stored = {r.evaluation_id: (r.state, r.evaluation_mode) for r in spark.table(f"{db}.dq_check_result").collect()}
    assert stored == {"n": ("PASSED", None), "r": ("FAILED", None)}  # kept as written
    modes = {r.dataset: r.evaluation_mode for r in spark.table(f"{db}.v_latest_result").collect()}
    assert modes == {"a": "NORMAL", "b": "CURRENT_RECHECK"}

    append_rows(spark, db, "dq_check_result", [
        {"evaluation_id": "x", "run_id": "r3", "dataset": "c", "check_id": "T1_ZERO_ROWS", "state": "DID_NOT_RUN",
         "execution_type": "AS_OF_REPLAY", "evaluation_mode": "AS_OF_REPLAY", "window_end": at, "evaluated_at": at,
         "run_date": day}])
    assert install_command(spark, db, "apply") == 0  # idempotent: nothing pending, nothing rewritten
    assert spark.table(f"{db}.dq_check_result").count() == 3
    assert {r.dataset: r.evaluation_mode for r in spark.table(f"{db}.v_latest_result").collect()}["c"] == "AS_OF_REPLAY"


def test_a_new_store_has_nothing_to_migrate(spark) -> None:
    create_store(spark, "dq_mig_new")
    assert migrations.pending(spark, "dq_mig_new") == []
    assert all(f.status == "OK" for f in check_store(spark, "dq_mig_new"))
    apply_ddl(spark, "dq_mig_new")  # safe to repeat
    assert migrations.pending(spark, "dq_mig_new") == []


def test_migrations_only_add_columns() -> None:
    for m in migrations.MIGRATIONS:
        assert m.columns and all(re.fullmatch(r"\w+", c) and re.fullmatch(r"[A-Z]+", t) for c, t in m.columns)
    assert [m.version for m in migrations.MIGRATIONS] == list(range(1, len(migrations.MIGRATIONS) + 1))


# --- current rechecks ---

def test_a_rerun_is_stored_and_labelled_as_a_current_recheck(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "modes_recheck")
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(), gold_row()])  # a duplicated key
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row()])
    assert main(["run", "--conf", str(replay.conf)]) == 0
    [normal] = latest(spark, replay)[("gold_member_coverage", "T1_KEY_DUPLICATES")]
    assert normal.evaluation_mode == "NORMAL"
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()])  # fixed since
    capsys.readouterr()
    assert main(["run", "--conf", str(replay.conf), "--execution-type", "RERUN",
                 "--window-start", normal.window_start.isoformat(), "--window-end", normal.window_end.isoformat()]) == 0
    out = capsys.readouterr().out
    assert "is a current recheck" in out and "never stand for a historical PASS" in out
    assert "gold_member_coverage T1_KEY_DUPLICATES [src_sys_nm #1] [current recheck]: PASSED" in out
    stored = {(r.execution_type, r.evaluation_mode, r.state) for r in spark.table(f"{replay.dq}.dq_check_result")
              .where("dataset = 'gold_member_coverage' AND check_id = 'T1_KEY_DUPLICATES'").collect()}
    assert stored == {("NORMAL", "NORMAL", "FAILED"), ("RERUN", "CURRENT_RECHECK", "PASSED")}
    [now] = latest(spark, replay)[("gold_member_coverage", "T1_KEY_DUPLICATES")]
    assert (now.evaluation_mode, now.state) == ("CURRENT_RECHECK", "PASSED")  # labelled wherever it is read


# --- AS_OF_REPLAY (proof scenario 7) ---

def test_an_as_of_replay_on_a_mutable_dataset_has_no_as_of_evidence(spark, tmp_path) -> None:
    """Proof scenario 7: gold is mutable (merged in place), so it cannot be read as it stood at a past
    cutoff. Every check of the window is DID_NOT_RUN / no_as_of_evidence; nothing passes, and the
    NORMAL window does not move."""
    replay = replay_conf(spark, tmp_path, "modes_mutable")
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(record=curated_load_time(12), loaded=gold_load_time(11))])
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(updated=curated_load_time(12))])
    assert main(["run", "--conf", str(replay.conf)]) == 0
    [normal] = latest(spark, replay)[("gold_member_coverage", "HOP_LEDGER")]
    assert main(["run", "--conf", str(replay.conf), "--execution-type", "AS_OF_REPLAY",
                 "--window-start", normal.window_start.isoformat(), "--window-end", normal.window_end.isoformat()]) == 0
    as_of = spark.table(f"{replay.dq}.dq_check_result").where("execution_type = 'AS_OF_REPLAY'").collect()
    assert as_of and {(r.evaluation_mode, r.state) for r in as_of} == {("AS_OF_REPLAY", "DID_NOT_RUN")}
    assert {r.reason_code for r in as_of if r.dataset == "gold_member_coverage"} == {"no_as_of_evidence"}
    [ledger] = [r for r in as_of if r.check_id == "HOP_LEDGER" and r.dataset == "gold_member_coverage"]
    assert "gold_member_coverage is mutable, not append_only" in ledger.detail
    assert not [r for r in spark.table(f"{replay.dq}.v_latest_result").collect()
                if r.evaluation_mode == "AS_OF_REPLAY" and r.healthy]


@pytest.fixture(scope="module")
def store(spark):
    create_store(spark, DQ)


def append_only(ds):
    return ds.model_copy(update={"grain": Grain(history="append_only")})


def as_of_ledger(spark, ds, up, as_of=CUTOFF, check=None):
    ctx = CheckContext(spark, ds, None, SETTINGS, "UTC", DQ, upstreams={up.dataset: up}, as_of=as_of)
    [result], _ = run_check(check or HopLedger(), ctx, EVENT)
    return result


def drift(spark, run_id: str, ds_id: str, table: str, evaluated: datetime, state: str = "PASSED",
          reason: str | None = None) -> None:
    """A committed NORMAL T1_PARTITION_DRIFT evaluation of `table`, with the partition state it recorded."""
    day = evaluated.date()
    append_rows(spark, DQ, "dq_check_result", [
        {"evaluation_id": f"drift-{run_id}", "run_id": run_id, "dataset": ds_id, "check_id": "T1_PARTITION_DRIFT",
         "execution_type": "NORMAL", "evaluation_mode": "NORMAL", "state": state, "reason_code": reason,
         "window_end": evaluated, "evaluated_at": evaluated, "run_date": day}])
    append_rows(spark, DQ, "dq_partition_state", [
        {"dataset": ds_id, "table_name": table, "partition_value": "2026-01-15", "file_count": 1,
         "observed_at": evaluated, "run_id": run_id, "run_date": day}])


def as_of_tables(spark, name: str):
    """Curated and gold, both append_only; key 2 reached gold only after the cutoff."""
    create_table(spark, f"hop.{name}_cur", CURATED_COLUMNS, [curated_row(sub_id="1", updated=IN_WINDOW),
                                                             curated_row(sub_id="2", updated=IN_WINDOW)])
    create_table(spark, f"hop.{name}_gold", GOLD_COLUMNS, [
        gold_row(sub_id="0001", record=IN_WINDOW, loaded="2026-01-15 11:00:00:000000"),
        gold_row(sub_id="0002", record=IN_WINDOW, loaded="2026-01-16 13:00:00:000000")])
    up = append_only(curated(f"hop.{name}_cur"))
    dn = append_only(gold(f"hop.{name}_gold", dataset=f"gold_{name}", load_time=GOLD_LOAD))
    return up, dn


def proven(spark, name: str, up, dn) -> None:
    """Drift evidence for both tables: a baseline before the cutoff, unchanged after it."""
    for ds in (up, dn):
        drift(spark, f"{name}-{ds.dataset}-base", f"drift_{name}_{ds.layer}", ds.table, CUTOFF - timedelta(hours=2),
              reason="insufficient_history", state="DID_NOT_RUN")
        drift(spark, f"{name}-{ds.dataset}-after", f"drift_{name}_{ds.layer}", ds.table, CUTOFF + timedelta(days=1))


def test_an_as_of_replay_reads_append_only_data_as_it_stood_at_the_cutoff(spark, store) -> None:
    """Key 2 reached gold after the cutoff: as of the cutoff it was missing (FAILED), while a reading
    of the tables as they are now finds it delivered. The scope says what the reading rests on."""
    up, dn = as_of_tables(spark, "asof_ok")
    proven(spark, "asof_ok", up, dn)
    now = as_of_ledger(spark, dn, up, as_of=None)
    assert (now.state, now.population, now.violations) == ("PASSED", 2, 0)
    then = as_of_ledger(spark, dn, up)
    assert (then.state, then.population, then.violations) == ("FAILED", 2, 1)
    assert then.observed.startswith("due 2 (pending 0): delivered 1, missing 1")
    scope = json.loads(then.detail)
    assert scope["evaluation_mode"] == "AS_OF_REPLAY"
    assert scope["as_of"] == (f"read as the data stood at {CUTOFF.isoformat()}: gold_asof_ok, "
                              "example_curated_enrollment append_only, T1_PARTITION_DRIFT unchanged since, "
                              f"drift checked through {(CUTOFF + timedelta(days=1)).isoformat()}")
    assert "evaluation_mode" not in json.loads(now.detail)


@pytest.mark.parametrize("case", ["no_drift_after", "drift_failed", "no_baseline", "mutable", "no_load_time",
                                  "rejects", "not_capable"])
def test_an_as_of_replay_without_proof_is_did_not_run(spark, store, case: str) -> None:
    """Fail closed: whatever the as-of proof lacks, the result is DID_NOT_RUN / no_as_of_evidence,
    never a verdict."""
    name = f"asof_{case}"
    up, dn = as_of_tables(spark, name)
    check = None
    if case == "no_drift_after":
        drift(spark, f"{name}-u0", f"drift_{name}_c", up.table, CUTOFF - timedelta(hours=2))
        drift(spark, f"{name}-d0", f"drift_{name}_g", dn.table, CUTOFF - timedelta(hours=2))
        why = "no T1_PARTITION_DRIFT evaluation since the cutoff shows it unchanged"
    else:
        proven(spark, name, up, dn)
        if case == "drift_failed":
            drift(spark, f"{name}-late", f"drift_{name}_GOLD", dn.table, CUTOFF + timedelta(days=2), state="FAILED")
            why = "T1_PARTITION_DRIFT found the table changed (or its evidence invalid) since the cutoff"
        elif case == "no_baseline":
            create_table(spark, f"hop.{name}_other", CURATED_COLUMNS, [curated_row(sub_id="1", updated=IN_WINDOW)])
            up = up.model_copy(update={"table": f"hop.{name}_other"})
            why = f"hop.{name}_other: no T1_PARTITION_DRIFT evaluation recorded its partitions at or before the cutoff"
        elif case == "mutable":
            dn = dn.model_copy(update={"grain": Grain(history="mutable")})
            why = f"gold_{name} is mutable, not append_only"
        elif case == "no_load_time":
            dn = dn.model_copy(update={"load_time": None})
            why = f"gold_{name} has no load_time"
        elif case == "rejects":
            rejects = RejectsSource(table=f"hop.{name}_cur", lineage=RejectLineage(
                version=TimeColumn(column="sourcelastupdatets", timezone="UTC")))
            dn = dn.model_copy(update={"contracts": {up.dataset: EdgeContract(mapping="dedup", rejects=rejects)}})
            why = "a rejects table declares no history"
        else:
            check = KeyNulls()
            why = "T1_KEY_NULLS reads the tables as they are now"
    r = as_of_ledger(spark, dn, up, check=check)
    assert (r.state, r.reason_category, r.reason_code) == ("DID_NOT_RUN", "DATA_UNAVAILABLE", "no_as_of_evidence")
    assert why in r.detail
