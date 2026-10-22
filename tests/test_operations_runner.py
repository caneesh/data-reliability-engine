"""dre run under ADR-08 (build step 8): the run budget in priority order, the kill switch at the start
and between checks, the queue refused before the run starts, and backpressure deferring the weekly
full sweep without losing it. Time is a fake clock (a minute per evaluated check), never a sleep."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from hcsc.datalake.dre import operations, runner
from hcsc.datalake.dre.checks.events import previous_window_end
from hcsc.datalake.dre.cli import main
from hcsc.datalake.dre.store import runs
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_row, gold_row
from tests.replay.conftest import replay_conf

UTC = timezone.utc
DEFAULTS = "defaults.yaml"


def healthy(spark, tmp_path, name: str, edits: list[tuple[str, str]] | None = None):
    replay = replay_conf(spark, tmp_path, name, {DEFAULTS: edits} if edits else None)
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()])
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row()])
    return replay


def run_feed(replay) -> int:
    return main(["run", "--conf", str(replay.conf), "--feed", "example_realtime"])


def spy(monkeypatch, minutes_per_check: float = 0.0, after: int | None = None, then=None) -> list:
    """Wrap runner.evaluate: each evaluated check advances the run budget's clock by
    minutes_per_check; after `after` evaluations, `then()` runs once. Returns the evaluations."""
    now = [0.0]
    monkeypatch.setattr(operations, "clock", lambda: now[0])
    real = runner.evaluate
    seen: list = []

    def timed(*args, **kwargs):
        now[0] += minutes_per_check * 60
        evaluation = real(*args, **kwargs)
        seen.append(evaluation)
        if after is not None and len(seen) == after:
            then()
        return evaluation

    monkeypatch.setattr(runner, "evaluate", timed)
    return seen


def latest_run(spark, replay):
    return max(spark.table(f"{replay.dq}.v_latest_run").collect(), key=lambda r: r.started_at)


def run_rows(spark, replay, run_id: str) -> list:
    return [r for r in spark.table(f"{replay.dq}.dq_check_result").collect() if r.run_id == run_id]


def committed(spark, replay) -> set[str]:
    return {r.evaluation_id for r in spark.table(f"{replay.dq}.v_committed_result").collect()}


def gold_window(rows) -> tuple:
    [window] = {(r.window_start, r.window_end) for r in rows
                if r.dataset == "gold_member_coverage" and r.check_id == "T1_KEY_DUPLICATES"}
    return window


def test_checks_past_the_run_budget_are_did_not_run_committed_and_hold_their_window(
        spark, tmp_path, monkeypatch, capsys) -> None:
    replay = healthy(spark, tmp_path, "ops_budget", [("run_budget_minutes: 40", "run_budget_minutes: 3")])
    evaluated = spy(monkeypatch, minutes_per_check=1)

    assert run_feed(replay) == 0
    # Three checks fit in the budget, and they are on-time and completeness checks (priority 0).
    assert len(evaluated) == 3
    assert [operations.priority(e.planned) for e in evaluated] == [0, 0, 0]
    run = latest_run(spark, replay)
    rows = run_rows(spark, replay, run.run_id)
    cut = [r for r in rows if r.reason_code == "run_budget_exceeded"]
    assert cut and all((r.state, r.reason_category) == ("DID_NOT_RUN", "BUDGET") for r in cut)
    assert all("run budget of 3 minutes spent after 3.0 minutes" in r.detail for r in cut)
    # Every priority-1 check was reached after the budget, and some priority-0 checks too.
    assert all(r.reason_code == "run_budget_exceeded" for r in rows if r.check_id not in operations.FIRST)
    assert any(r.check_id in operations.FIRST for r in cut)
    assert {r.evaluation_id for r in cut} <= committed(spark, replay)  # written and committed like any result
    # One state per check: the cut checks count as written, so the run is COMPLETED (exit 0).
    assert run.status == "COMPLETED" and run.checks_written == run.checks_expected
    out = capsys.readouterr().out
    assert f"run budget: 3 minutes spent; {run.checks_expected - 3} check(s) reached after it" in out
    assert "pending cause investigations and the integrity sweep: not run" in out
    # BUDGET holds the window: the next run evaluates the same span again, every check this time.
    assert previous_window_end(spark, replay.dq, "gold_member_coverage") is None
    first = gold_window(rows)
    monkeypatch.undo()
    defaults = replay.conf / DEFAULTS
    defaults.write_text(defaults.read_text(encoding="utf-8").replace("run_budget_minutes: 3", "run_budget_minutes: 40"),
                        encoding="utf-8")
    assert run_feed(replay) == 0
    second = latest_run(spark, replay)
    again = run_rows(spark, replay, second.run_id)
    assert gold_window(again)[0] == first[0]
    assert not [r for r in again if r.reason_code == "run_budget_exceeded"]


def test_the_kill_switch_at_the_start_disables_every_check_and_the_run_is_partial(
        spark, tmp_path, monkeypatch, capsys) -> None:
    switch = tmp_path / "stop_dre"
    switch.write_text("")
    replay = healthy(spark, tmp_path, "ops_kill_start", [("kill_switch_file: null", f"kill_switch_file: {switch}")])
    evaluated = spy(monkeypatch)

    assert run_feed(replay) == 2
    assert evaluated == []  # nothing is evaluated: no table of any layer is read for a check
    run = latest_run(spark, replay)
    rows = run_rows(spark, replay, run.run_id)
    assert rows and {(r.state, r.reason_category, r.reason_code) for r in rows} == {("DID_NOT_RUN", "PLATFORM", "disabled")}
    assert {r.evaluation_id for r in rows} <= committed(spark, replay)
    assert run.status == "PARTIAL" and run.checks_written == 0 and run.checks_expected == len(rows)
    assert spark.table(f"{replay.dq}.dq_key_event").count() == 0
    out = capsys.readouterr().out
    assert "kill switch: on at the start of the run (the kill_switch_file is present)" in out
    assert f"kill switch: on (the kill_switch_file is present); {len(rows)} check(s) written DID_NOT_RUN / disabled" in out
    assert str(switch) not in out
    assert "dre run: PARTIAL, 0 of" in out
    # The windows are held: once the switch is off, the next run evaluates the same span.
    first = gold_window(rows)
    switch.unlink()
    assert run_feed(replay) == 0
    second = latest_run(spark, replay)
    assert second.status == "COMPLETED"
    assert gold_window(run_rows(spark, replay, second.run_id))[0] == first[0]


def test_the_kill_switch_between_checks_disables_the_remaining_ones(spark, tmp_path, monkeypatch, capsys) -> None:
    switch = tmp_path / "stop_dre"
    replay = healthy(spark, tmp_path, "ops_kill_between", [("kill_switch_file: null", f"kill_switch_file: {switch}")])
    evaluated = spy(monkeypatch, after=2, then=lambda: switch.write_text(""))

    assert run_feed(replay) == 2
    assert len(evaluated) == 2
    run = latest_run(spark, replay)
    rows = run_rows(spark, replay, run.run_id)
    disabled = [r for r in rows if r.reason_code == "disabled"]
    assert len(disabled) == run.checks_expected - 2 and disabled
    assert {r.evaluation_id for r in rows} <= committed(spark, replay)
    assert run.status == "PARTIAL" and run.checks_written == 2
    out = capsys.readouterr().out
    assert "kill switch: on at the start" not in out
    assert f"{len(disabled)} check(s) written DID_NOT_RUN / disabled, the run ends PARTIAL" in out
    assert "pending cause investigations and the integrity sweep: not run" in out


def test_a_protected_or_foreign_queue_cannot_start_a_run(spark, tmp_path, monkeypatch, capsys) -> None:
    replay = healthy(spark, tmp_path, "ops_queue", [(
        "kill_switch_file: null",
        "kill_switch_file: null\nyarn_queue: example_dre_queue\nprotected_queues: [example_pipeline_queue]")])
    for queue, says in (("example_pipeline_queue", "is one of protected_queues"),
                        ("example_other_queue", "is not yarn_queue"), (None, "names no queue")):
        monkeypatch.setattr(operations, "submitted_queue", lambda spark_, q=queue: (q, True))
        assert run_feed(replay) == 3
        out = capsys.readouterr().out
        assert says in out and "--queue example_dre_queue" in out
        assert "example_pipeline_queue" not in out and "example_other_queue" not in out
    assert spark.table(f"{replay.dq}.dq_run").count() == 0  # refused before the run started
    monkeypatch.setattr(operations, "submitted_queue", lambda spark_: ("example_dre_queue", True))
    assert run_feed(replay) == 0


def test_backpressure_defers_the_full_sweep_and_the_next_unpressured_run_performs_it(
        spark, tmp_path, monkeypatch, capsys) -> None:
    replay = healthy(spark, tmp_path, "ops_backpressure_run")
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    boundary = now - timedelta(hours=4)  # the full_sweep_day boundary: only the first window crosses it
    monkeypatch.setattr(runner, "is_full_sweep",
                        lambda config, dataset_id, event: event.window_start < boundary <= event.window_end)
    starts = iter(now - timedelta(hours=h) for h in (3, 2, 1, 0))  # four hourly runs
    real_start = runs.start_run
    monkeypatch.setattr(runs, "start_run", lambda *a, **k: real_start(*a, **{**k, "now": next(starts)}))
    previous = [35.0]  # minutes the previous NORMAL run took (0.8 * 40 = 32)
    monkeypatch.setattr(operations, "previous_run_minutes", lambda *a, **k: previous[0])
    evaluated = spy(monkeypatch)

    def gold_sweeps() -> set[bool]:
        swept = {e.ctx.full_sweep for e in evaluated if e.planned.dataset.dataset == "gold_member_coverage"}
        evaluated.clear()
        return swept

    def deferrals() -> int:
        return len([r for r in spark.table(f"{replay.dq}.dq_integrity").collect()
                    if r.record == operations.SWEEP_DEFERRED and r.dataset == "gold_member_coverage"])

    # 1. Under backpressure: the calendar's sweep is deferred, and recorded before any check runs.
    assert run_feed(replay) == 0
    assert gold_sweeps() == {False}
    out = capsys.readouterr().out
    assert "backpressure: the previous NORMAL run took 35.0 minutes, over 0.8 of the 40-minute run budget" in out
    assert "is deferred to the next NORMAL run not under backpressure" in out
    assert deferrals() == 1
    # 2. Still under backpressure: deferred again, never dropped.
    assert run_feed(replay) == 0
    assert gold_sweeps() == {False}
    assert deferrals() == 2
    # 3. The first run not under backpressure performs the deferred sweep.
    previous[0] = 5.0
    assert run_feed(replay) == 0
    assert gold_sweeps() == {True}
    assert "swept now for a sweep deferred earlier" in capsys.readouterr().out
    assert latest_run(spark, replay).status == "COMPLETED"
    # 4. Its window was evaluated and not held: the obligation is met, the next run does not sweep.
    assert previous_window_end(spark, replay.dq, "gold_member_coverage") is not None
    assert run_feed(replay) == 0
    assert gold_sweeps() == {False}
    assert "swept now" not in capsys.readouterr().out


def test_a_compromised_baseline_s_forced_sweep_is_never_deferred(spark, tmp_path, monkeypatch) -> None:
    """Integrity's forced sweep (HOP_KEY_CURRENCY on a compromised baseline) wins over backpressure."""
    from hcsc.datalake.dre.store import integrity

    replay = healthy(spark, tmp_path, "ops_tainted")
    monkeypatch.setattr(runner, "is_full_sweep", lambda config, dataset_id, event: True)
    monkeypatch.setattr(operations, "previous_run_minutes", lambda *a, **k: 39.0)
    tainted = integrity.Compromised({("gold_member_coverage", "HOP_KEY_CURRENCY"): [("r", now_date(), "e")]})
    monkeypatch.setattr(integrity, "compromised_baselines", lambda spark_, db: tainted)
    evaluated = spy(monkeypatch)
    assert run_feed(replay) == 0
    sweeps = {e.planned.check.check_id: e.ctx.full_sweep for e in evaluated
              if e.planned.dataset.dataset == "gold_member_coverage" and e.ctx is not None}
    assert sweeps["HOP_KEY_CURRENCY"] is True
    assert sweeps["HOP_VALUE_AGREEMENT"] is False and sweeps["HOP_LEDGER"] is False  # deferred


def now_date():
    return datetime.now(UTC).date()
