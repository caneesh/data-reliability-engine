"""Recoverable evidence (code review, October 2026, findings 3 and 4).

Finding 3: a result stored without its key events is marked DID_NOT_RUN / evidence_incomplete,
the run is PARTIAL, and the window is held, so the next run evaluates it again: exactly one
current result, with complete key events.
Finding 4: a cause investigation that cannot be stored never changes the check; the run records
it as incomplete (the watchdog reports it) and the next run retries it.
"""

from __future__ import annotations

from datetime import timedelta

from hcsc.datalake.dre.checks.hop.common import key_hash
from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.replay.conftest import latest, replay_conf
from tests.replay.test_r04_to_r07_hops import KEY, SECRET, key_events, setup



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

    replay = replay_conf(spark, tmp_path, "recover_causes")  # R03
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(loaded=gold_load_time(72))])
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(sub_id=f"1234{h:02d}", updated=curated_load_time(h)) for h in range(1, 40)])

    def store_down(*args, **kwargs):
        raise OSError("synthetic cause store failure")

    monkeypatch.setattr(runner, "append_cause_results", store_down)
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
        spark, runner.load_config(str(replay.conf), "test"), run.run_date, set())}
    assert "T1_ZERO_ROWS" in pending
    # HOP_LEDGER failed too (gold is missing the keys), but it has no failure type: nothing to retry
    [ledger] = latest(spark, replay)[("gold_member_coverage", "HOP_LEDGER")]
    assert ledger.state == "FAILED" and "HOP_LEDGER" not in pending

    monkeypatch.undo()
    assert main(["run", "--conf", str(replay.conf)]) == 0  # nothing new is due; the retry runs
    assert "gold_member_coverage T1_ZERO_ROWS: cause investigation retried" in capsys.readouterr().out
    retry = final_runs(spark, replay)[-1]
    assert retry.investigations_done == retry.investigations_expected > 0
    rows = spark.table(f"{replay.dq}.dq_cause_result").collect()
    assert {r.evaluation_id for r in rows} >= {zero.evaluation_id}
    assert runner.pending_investigations(spark, runner.load_config(str(replay.conf), "test"), run.run_date, set()) == []


def test_every_row_of_an_evaluation_is_marked_incomplete_together(spark, tmp_path) -> None:
    """A marker for some groups only would outrank the evaluation's other groups in v_latest_result
    (it ranks a dataset's check by evaluated_at), hiding them until the next run. So every row is
    marked, with one shared time, whether or not that result carried key events."""
    from dataclasses import replace
    from datetime import datetime, timezone

    from hcsc.datalake.dre.checks.base import did_not_run, from_counts
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.checks.hop.key_currency import KeyCurrency
    from hcsc.datalake.dre.config.loader import load
    from hcsc.datalake.dre.runner import Evaluation, Planned, incomplete_rows, result_rows
    from hcsc.datalake.dre.store import runs
    from tests.store.conftest import append_rows

    replay = replay_conf(spark, tmp_path, "recover_groups")
    config, _ = load(replay.conf)
    gold = config.datasets["gold_member_coverage"]
    planned = Planned(config.feeds["example_realtime"], gold, KeyCurrency())
    at = datetime(2026, 1, 16, 12, tzinfo=timezone.utc)
    event = Event(gold.dataset, at - timedelta(hours=1), at)
    with_events = replace(from_counts(3, 1, "1 of 3 keys missing", "every key present",
                                      group_values={"upstream": "curated_a"}), key_events=object())
    without = did_not_run("invalid_config", detail="synthetic", group_values={"upstream": "curated_b"})
    run = runs.new_run(now=at)
    evaluation = Evaluation(planned, event, [with_events, without], at, 10)
    rows = result_rows(evaluation, run, "NORMAL")
    marked = incomplete_rows(evaluation, rows, at + timedelta(minutes=1))
    assert [m["group_values"]["upstream"] for m in marked] == ["curated_a", "curated_b"]
    assert {(m["state"], m["reason_category"], m["reason_code"], m["evaluated_at"]) for m in marked} == {
        ("DID_NOT_RUN", "PLATFORM", "evidence_incomplete", at + timedelta(minutes=1))}
    append_rows(spark, replay.dq, "dq_check_result", rows + marked)
    current = [r for r in spark.table(f"{replay.dq}.v_latest_result").collect() if r.check_id == "HOP_KEY_CURRENCY"]
    assert sorted((r.group_values["upstream"], r.reason_code) for r in current) == [
        ("curated_a", "evidence_incomplete"), ("curated_b", "evidence_incomplete")]  # both groups still current


def _slow_job(spark) -> None:
    spark.sparkContext.parallelize(range(4), 2).map(lambda x: __import__("time").sleep(120) or x).count()


def test_a_slow_cause_investigation_is_cancelled_by_the_budget_and_retried(spark, tmp_path, monkeypatch) -> None:
    """The cause engine runs under the dataset's remaining compute budget: past it, the
    investigation is cancelled, counted as not done (the next run retries it) and the check
    stands; with the budget spent it is skipped the same way."""
    from datetime import datetime, timezone

    from hcsc.datalake.dre import runner
    from hcsc.datalake.dre.checks.base import from_counts
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.checks.tier1.on_time import OnTime
    from hcsc.datalake.dre.config.loader import load
    from hcsc.datalake.dre.runner import Evaluation, Investigations, Planned, check_context, result_rows
    from hcsc.datalake.dre.store import runs

    replay = replay_conf(spark, tmp_path, "recover_budget")
    config, _ = load(replay.conf)
    ds = config.datasets["example_curated_enrollment"]
    planned = Planned(config.feeds["example_realtime"], ds, OnTime())
    at = datetime(2026, 1, 16, 12, tzinfo=timezone.utc)
    event = Event(ds.dataset, at - timedelta(hours=1), at)
    failed = from_counts(1, 1, "1 of 1 slots missed", "every slot loaded")
    ctx = check_context(spark, config, planned, event)
    evaluation = Evaluation(planned, event, [failed], at, 10, ctx)
    run = runs.new_run(now=at)
    rows = result_rows(evaluation, run, "NORMAL")
    monkeypatch.setattr(runner, "explain", lambda *args, **kwargs: _slow_job(spark) or [])
    tally = Investigations()
    lines = runner.explain_failures(spark, config, evaluation, rows, run, tally, 0.02)
    assert lines == ["  cause investigation exceeded the compute budget (0.02 minutes); the next run retries it"]
    assert (tally.expected, tally.done) == (1, 0)
    lines = runner.explain_failures(spark, config, evaluation, rows, run, tally, 0)
    assert lines == ["  cause investigation skipped: the dataset's compute budget is spent; the next run retries it"]
    assert (tally.expected, tally.done) == (2, 0)
    monkeypatch.setattr(runner, "explain", lambda *args, **kwargs: [])
    assert runner.explain_failures(spark, config, evaluation, rows, run, tally, 0.5) == []
    assert (tally.expected, tally.done) == (3, 1)


def test_a_slow_key_event_write_is_cancelled_by_the_budget_and_marked_incomplete(spark, tmp_path, monkeypatch) -> None:
    from dataclasses import replace
    from datetime import datetime, timezone

    from hcsc.datalake.dre import runner
    from hcsc.datalake.dre.checks.base import from_counts
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.checks.hop.key_currency import KeyCurrency
    from hcsc.datalake.dre.config.loader import load
    from hcsc.datalake.dre.runner import Evaluation, Planned, result_rows, write_evaluation
    from hcsc.datalake.dre.store import runs

    replay = replay_conf(spark, tmp_path, "recover_write_budget")
    config, _ = load(replay.conf)
    gold = config.datasets["gold_member_coverage"]
    planned = Planned(config.feeds["example_realtime"], gold, KeyCurrency())
    at = datetime(2026, 1, 16, 12, tzinfo=timezone.utc)
    event = Event(gold.dataset, at - timedelta(hours=1), at)
    result = replace(from_counts(3, 1, "1 of 3 keys missing", "every key present",
                                 group_values={"upstream": "example_curated_enrollment"}), key_events=object())
    run = runs.new_run(now=at)
    evaluation = Evaluation(planned, event, [result], at, 10)
    rows = result_rows(evaluation, run, "NORMAL")
    monkeypatch.setattr(runner, "write_key_events", lambda *args, **kwargs: _slow_job(spark))
    stored, complete, commits = write_evaluation(spark, replay.dq, evaluation, rows, run, budget_minutes=0.02)
    assert (len(stored), complete) == (1, 0)
    assert [(c.key_event_rows, c.partition_state_rows) for c in commits] == [(0, 0)]  # the marker vouches for no evidence
    runner.append_commits(spark, replay.dq, commits, at, run.run_date)  # as the run's publisher does (ADR-07)
    current = [r for r in spark.table(f"{replay.dq}.v_latest_result").collect() if r.check_id == "HOP_KEY_CURRENCY"]
    assert [(r.state, r.reason_code) for r in current] == [("DID_NOT_RUN", "evidence_incomplete")]


def test_older_pending_investigations_are_retried_once_a_day(spark, tmp_path, monkeypatch) -> None:
    """A pending investigation from today is retried every run; older ones only in the hour from
    rule_run_at, so a failure that repeats is not retried hourly for a week."""
    from datetime import datetime, timezone

    from hcsc.datalake.dre import runner
    from hcsc.datalake.dre.config.loader import load
    from hcsc.datalake.dre.runner import Investigations, retry_investigations
    from hcsc.datalake.dre.store import runs

    replay = replay_conf(spark, tmp_path, "recover_cadence")
    config, _ = load(replay.conf)  # rule_run_at 06:00 America/Chicago = 12:00 UTC in January
    asked: list = []
    monkeypatch.setattr(runner, "pending_investigations", lambda spark_, config_, since, skip: asked.append(since) or [])
    tally = Investigations()
    off_hour = runs.new_run(now=datetime(2026, 1, 15, 15, tzinfo=timezone.utc))
    retry_investigations(spark, config, off_hour, None, tally, set())
    assert asked[-1] == off_hour.run_date  # today only
    daily_hour = runs.new_run(now=datetime(2026, 1, 15, 12, 20, tzinfo=timezone.utc))
    retry_investigations(spark, config, daily_hour, None, tally, set())
    assert asked[-1] == daily_hour.run_date - timedelta(days=config.defaults.investigation_retry_days)


def test_a_retried_hop_investigation_sees_the_keys_judged_because_they_were_open(spark, tmp_path, monkeypatch) -> None:
    """A key judged only because it was open (R05 then a later window) failed; its investigation
    could not be stored; the retry must judge the open keys too, or it would find nothing."""
    from hcsc.datalake.dre import runner

    replay = setup(spark, tmp_path, "recover_open_retry")  # R05: gold holds an older version of the key
    opened, terminated = curated_load_time(30), curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(updated=opened), curated_row(end="2026-06-30", updated=terminated)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(record=opened, loaded=gold_load_time(29))])
    assert main(["run", "--conf", str(replay.conf)]) == 0  # the key is FLAGGED (STALE) and explained
    assert key_events(spark, replay) == [(key_hash(SECRET, KEY), "FLAGGED", "STALE")]

    def store_down(*args, **kwargs):
        raise OSError("synthetic cause store failure")
    monkeypatch.setattr(runner, "append_cause_results", store_down)
    # The next window: the key is judged only because it is open; the investigation cannot be stored.
    assert main(["run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0
    assert key_events(spark, replay)[-1][1] == "STILL_FLAGGED"
    monkeypatch.undo()
    assert main(["run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0  # retries it
    rows = spark.table(f"{replay.dq}.dq_cause_result").collect()
    retried = [r for r in rows if r.run_id == final_runs(spark, replay)[-1].run_id]
    assert retried and {r.failure_ref for r in retried} == {key_hash(SECRET, KEY)}
    assert all("nothing to explain" not in (r.evidence or "") for r in retried)
