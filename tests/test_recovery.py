"""Recoverable evidence (code review, October 2026, findings 3 and 4).

Finding 3: a result stored without its key events is marked DID_NOT_RUN / evidence_incomplete,
the run is PARTIAL, and the window is held, so the next run evaluates it again: exactly one
current result, with complete key events.
Finding 4: a cause investigation that cannot be stored never changes the check; the run records
it as incomplete (the watchdog reports it) and the next run retries it.
"""

from __future__ import annotations

from hcsc.datalake.dre.checks.hop.common import key_hash
from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.replay.conftest import latest, replay_conf
from tests.replay.test_r04_to_r07_hops import CURATED_LOAD_TIME, KEY, SECRET, key_events, setup

CURATED = "datasets/example_curated_enrollment.yaml"


def final_runs(spark, replay) -> list:
    return sorted((r for r in spark.table(f"{replay.dq}.v_latest_run").collect()), key=lambda r: r.started_at)


def test_a_result_stored_without_its_key_events_is_recovered_by_the_next_run(spark, tmp_path, monkeypatch) -> None:
    from hcsc.datalake.dre import runner

    replay = setup(spark, tmp_path, "recover_events")  # R05: gold holds an older version of the key
    opened, terminated = curated_load_time(30), curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(updated=opened), curated_row(end="2026-06-30", updated=terminated)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(record=opened, loaded=gold_load_time(29))])

    def fail_after_the_result(*args, **kwargs):  # the result is stored; then the event write fails
        raise OSError("synthetic write failure")

    monkeypatch.setattr(runner, "write_key_events", fail_after_the_result)
    assert main(["run", "--conf", str(replay.conf)]) == 2
    assert key_events(spark, replay) == []
    [marker] = latest(spark, replay)[("gold_member_coverage", "HOP_KEY_CURRENCY")]
    assert (marker.state, marker.reason_category, marker.reason_code) == (
        "DID_NOT_RUN", "PLATFORM", "evidence_incomplete")
    first_window = marker.window_start

    monkeypatch.undo()  # the store is back; the next scheduled run
    assert main(["run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0
    [current] = latest(spark, replay)[("gold_member_coverage", "HOP_KEY_CURRENCY")]
    assert (current.state, current.window_start) == ("FAILED", first_window)  # the held window, again
    assert key_events(spark, replay) == [(key_hash(SECRET, KEY), "FLAGGED", "STALE")]  # complete, once
    assert [r.status for r in final_runs(spark, replay)] == ["PARTIAL", "COMPLETED"]


def test_a_cause_investigation_that_cannot_be_stored_is_tracked_and_retried(spark, tmp_path, monkeypatch,
                                                                            capsys) -> None:
    from hcsc.datalake.dre import runner
    from hcsc.datalake.dre.store import causes

    replay = replay_conf(spark, tmp_path, "recover_causes", {CURATED: [CURATED_LOAD_TIME]})  # R03
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(loaded=gold_load_time(72))])
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(sub_id=f"1234{h:02d}", updated=curated_load_time(h)) for h in range(1, 40)])

    def store_down(*args, **kwargs):
        raise OSError("synthetic cause store failure")

    monkeypatch.setattr(causes, "append_cause_results", store_down)
    assert main(["run", "--conf", str(replay.conf)]) == 0  # the checks completed
    out = capsys.readouterr().out
    assert "cause investigation incomplete (OSError); the next run retries it" in out
    [run] = final_runs(spark, replay)
    assert run.status == "COMPLETED"
    assert run.investigations_expected > 0 and run.investigations_done == 0
    [zero] = latest(spark, replay)[("gold_member_coverage", "T1_ZERO_ROWS")]
    assert zero.state == "FAILED"  # the check keeps its state
    assert spark.table(f"{replay.dq}.dq_cause_result").count() == 0
    pending = {r["check_id"] for r in runner.pending_investigations(
        spark, runner._load(str(replay.conf), "test"), run.run_date, set())}
    assert "T1_ZERO_ROWS" in pending

    monkeypatch.undo()
    assert main(["run", "--conf", str(replay.conf)]) == 0  # nothing new is due; the retry runs
    assert "gold_member_coverage T1_ZERO_ROWS: cause investigation retried" in capsys.readouterr().out
    retry = final_runs(spark, replay)[-1]
    assert retry.investigations_done == retry.investigations_expected > 0
    rows = spark.table(f"{replay.dq}.dq_cause_result").collect()
    assert {r.evaluation_id for r in rows} >= {zero.evaluation_id}
    assert runner.pending_investigations(spark, runner._load(str(replay.conf), "test"), run.run_date, set()) == []
