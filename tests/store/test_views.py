"""The dq views read back current state from append-only rows (spec section 5)."""

from __future__ import annotations

from datetime import date

from tests.store.conftest import append_rows, ts

D = date(2026, 1, 1)


def result(**row):
    base = {"run_date": D, "dataset": "gold_member_coverage", "feed": "example_realtime", "expectation_version": 1}
    return {**base, **row}


def test_latest_result_per_dataset_and_check(spark, store) -> None:
    append_rows(spark, store, "dq_check_result", [
        result(evaluation_id="e1", run_id="r1", check_id="T1_ZERO_ROWS", state="PASSED", evaluated_at=ts(1)),
        result(evaluation_id="e2", run_id="r2", check_id="T1_ZERO_ROWS", state="FAILED", evaluated_at=ts(2)),
        # A rerun of e2 later on: the rerun is the latest.
        result(evaluation_id="e2", run_id="r3", check_id="T1_ZERO_ROWS", state="PASSED",
               execution_type="RERUN", evaluated_at=ts(3)),
        result(evaluation_id="e4", run_id="r1", check_id="T1_KEY_NULLS", state="PASSED", evaluated_at=ts(1)),
    ])
    rows = {r.check_id: r for r in spark.table(f"{store}.v_latest_result").collect()}
    assert set(rows) == {"T1_ZERO_ROWS", "T1_KEY_NULLS"}
    assert (rows["T1_ZERO_ROWS"].run_id, rows["T1_ZERO_ROWS"].state, rows["T1_ZERO_ROWS"].execution_type) == (
        "r3", "PASSED", "RERUN")
    assert rows["T1_KEY_NULLS"].state == "PASSED"


def test_latest_result_keeps_every_group_of_the_latest_evaluation(spark, store) -> None:
    rule = "one_row_per_coverage"
    append_rows(spark, store, "dq_check_result", [
        result(evaluation_id="g1", run_id="r1", check_id=rule, state="FAILED", evaluated_at=ts(1),
               group_values={"src_sys_nm": "SRC_A"}),
        result(evaluation_id="g1", run_id="r1", check_id=rule, state="PASSED", evaluated_at=ts(1),
               group_values={"src_sys_nm": "SRC_B"}),
        result(evaluation_id="g2", run_id="r2", check_id=rule, state="PASSED", evaluated_at=ts(2),
               group_values={"src_sys_nm": "SRC_A"}),
        result(evaluation_id="g2", run_id="r2", check_id=rule, state="PASSED", evaluated_at=ts(2),
               group_values={"src_sys_nm": "SRC_B"}),
    ])
    rows = spark.table(f"{store}.v_latest_result").where(f"check_id = '{rule}'").collect()
    assert sorted((r.group_values["src_sys_nm"], r.run_id, r.state) for r in rows) == [
        ("SRC_A", "r2", "PASSED"), ("SRC_B", "r2", "PASSED")]


def key_event(key_hash: str, event: str, day: int):
    return {"run_date": D, "run_id": f"r{day}", "evaluation_id": f"e{day}", "dataset": "gold_member_coverage",
            "check_id": "HOP_KEY_CURRENCY", "key_hash": key_hash, "key_value": "synthetic-key",
            "event": event, "state_detail": "STALE", "observed_at": ts(day)}


def test_open_keys(spark, store) -> None:
    append_rows(spark, store, "dq_key_event", [
        key_event("k_open", "FLAGGED", 1), key_event("k_open", "STILL_FLAGGED", 2),
        key_event("k_cleared", "FLAGGED", 1), key_event("k_cleared", "CLEARED", 2),
        key_event("k_reopened", "FLAGGED", 1), key_event("k_reopened", "CLEARED", 2),
        key_event("k_reopened", "FLAGGED", 3), key_event("k_reopened", "STILL_FLAGGED", 4),
    ])
    view = spark.table(f"{store}.v_open_keys")
    rows = {r.key_hash: r for r in view.collect()}
    assert set(rows) == {"k_open", "k_reopened"}
    assert (rows["k_open"].latest_event, rows["k_open"].first_flagged_at, rows["k_open"].last_observed_at) == (
        "STILL_FLAGGED", ts(1).replace(tzinfo=None), ts(2).replace(tzinfo=None))
    assert rows["k_reopened"].first_flagged_at == ts(3).replace(tzinfo=None)  # flagged again after CLEARED
    assert "key_value" not in view.columns  # restricted column stays in the table only


def test_file_status(spark, store) -> None:
    path = "/data/landing/example_feed/file_0001"
    append_rows(spark, store, "dq_file", [
        {"run_date": D, "path": path, "feed": "example_realtime", "first_seen_at": ts(1), "size_bytes": 10,
         "modified_at": ts(1), "observed_at": ts(1), "run_id": "r1"},
        {"run_date": D, "path": path, "feed": "example_realtime", "first_seen_at": ts(2), "size_bytes": 25,
         "modified_at": ts(2), "observed_at": ts(2), "run_id": "r2"},
        {"run_date": D, "path": path + "_b", "feed": "example_realtime", "first_seen_at": ts(2), "size_bytes": 5,
         "modified_at": ts(2), "observed_at": ts(2), "run_id": "r2"},
    ])
    rows = {r.path: r for r in spark.table(f"{store}.v_file_status").collect()}
    assert len(rows) == 2
    assert (rows[path].first_seen_at, rows[path].latest_size_bytes, rows[path].last_observed_at) == (
        ts(1).replace(tzinfo=None), 25, ts(2).replace(tzinfo=None))


def test_views_empty_after_a_run_with_no_checks(spark) -> None:
    # Done when (step 3): a run with no checks writes a dq_run row, and the views read back correctly.
    from hcsc.datalake.dre.store.local_setup import create_store
    from hcsc.datalake.dre.store.runs import finish_run, start_run

    create_store(spark, "dq_empty_run")
    run = start_run(now=ts(9))
    assert finish_run(spark, "dq_empty_run", run, 0, 0, now=ts(9, 1)) == "COMPLETED"
    assert spark.table("dq_empty_run.dq_run").count() == 1
    for view in ("v_latest_result", "v_open_keys", "v_file_status"):
        assert spark.table(f"dq_empty_run.{view}").count() == 0
