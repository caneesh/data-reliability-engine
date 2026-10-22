"""operations.py (ADR-08, build step 8) on its own: priority order, the kill switch, the queue rule,
the run budget's clock, backpressure from dq_run, and recording a deferred full sweep."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from hcsc.datalake.dre import operations
from hcsc.datalake.dre.checks.base import REASONS
from hcsc.datalake.dre.checks.events import BLOCKING_CATEGORIES, Event

UTC = timezone.utc


def planned(check_id: str, n: int = 0) -> SimpleNamespace:
    return SimpleNamespace(check=SimpleNamespace(check_id=check_id), n=n)


def config(**defaults) -> SimpleNamespace:
    values = {"kill_switch_file": None, "yarn_queue": None, "protected_queues": [], "run_budget_minutes": 40,
              "backpressure_share": 0.8, "history_lookback_days": 400, **defaults}
    return SimpleNamespace(defaults=SimpleNamespace(**values))


def fake_spark(queue: str | None, master: str = "yarn") -> SimpleNamespace:
    conf = {} if queue is None else {"spark.yarn.queue": queue}
    sc = SimpleNamespace(master=master, getConf=lambda: SimpleNamespace(get=lambda key, default=None: conf.get(key, default)))
    return SimpleNamespace(sparkContext=sc)


def test_the_new_reason_codes_hold_their_window() -> None:
    assert REASONS["run_budget_exceeded"] == "BUDGET" and REASONS["disabled"] == "PLATFORM"
    assert {REASONS["run_budget_exceeded"], REASONS["disabled"]} <= set(BLOCKING_CATEGORIES)


def test_on_time_and_completeness_checks_come_first_in_a_stable_order() -> None:
    event = Event("ds", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC))
    plan = ["T1_VOLUME", "T1_ON_TIME", "HOP_KEY_CURRENCY", "T1_ZERO_ROWS", "T1_KEY_NULLS", "HOP_LEDGER",
            "T1_FILES_NOT_LOADED", "HOP_FILE_COMPLETENESS", "one_row_per_coverage", "CHAIN_COMPLETENESS", "T1_VOLUME"]
    jobs = [(planned(c, n), event) for n, c in enumerate(plan)]
    ordered = [(p.check.check_id, p.n) for p, _ in operations.order(jobs)]
    assert ordered == [("T1_ON_TIME", 1), ("T1_ZERO_ROWS", 3), ("HOP_LEDGER", 5), ("T1_FILES_NOT_LOADED", 6),
                       ("HOP_FILE_COMPLETENESS", 7), ("CHAIN_COMPLETENESS", 9),
                       ("T1_VOLUME", 0), ("HOP_KEY_CURRENCY", 2), ("T1_KEY_NULLS", 4), ("one_row_per_coverage", 8),
                       ("T1_VOLUME", 10)]
    assert [operations.priority(planned(c)) for c in ("T1_ON_TIME", "HOP_VALUE_AGREEMENT")] == [0, 1]


def test_the_kill_switch_is_the_file_and_an_unreadable_location_counts_as_on(tmp_path: Path, monkeypatch) -> None:
    switch = tmp_path / "stop_dre"
    assert operations.kill_switch(config()) is None  # not configured
    assert not operations.kill_switch_on(config(kill_switch_file=str(switch)))  # absent: off
    switch.write_text("")
    assert operations.kill_switch(config(kill_switch_file=str(switch))) == "the kill_switch_file is present"
    assert operations.kill_switch_on(config(kill_switch_file=str(switch / "below"))) is False  # not a directory: off

    def denied(path, *args, **kwargs):
        raise PermissionError(13, "synthetic: permission denied")

    monkeypatch.setattr(operations, "os", SimpleNamespace(stat=denied))
    why = operations.kill_switch(config(kill_switch_file=str(switch)))
    assert why is not None and "PermissionError" in why and "fail closed" in why
    assert str(switch) not in why  # a deployment value: the path is never printed


def test_a_protected_queue_is_refused_and_its_name_is_never_printed() -> None:
    cfg = config(yarn_queue="example_dre_queue", protected_queues=["example_pipeline_queue"])
    why = operations.queue_refusal(fake_spark("example_pipeline_queue"), cfg)
    assert why is not None and "protected_queues" in why and "example_pipeline_queue" not in why
    assert "--queue example_dre_queue" in why
    # The protected list alone, with no yarn_queue configured, still refuses.
    assert operations.queue_refusal(fake_spark("example_pipeline_queue"), config(protected_queues=["example_pipeline_queue"]))


def test_a_queue_other_than_yarn_queue_is_refused() -> None:
    cfg = config(yarn_queue="example_dre_queue")
    assert operations.queue_refusal(fake_spark("example_dre_queue"), cfg) is None
    other = operations.queue_refusal(fake_spark("example_other_queue"), cfg)
    assert other is not None and "is not yarn_queue" in other and "example_other_queue" not in other
    assert "names no queue" in operations.queue_refusal(fake_spark(None, "yarn"), cfg)
    # Off YARN (local mode) there is no queue to compete in.
    assert operations.queue_refusal(fake_spark(None, "local[2]"), cfg) is None
    assert operations.queue_refusal(fake_spark("anything"), config()) is None  # nothing configured


def test_the_halt_cuts_checks_once_the_run_budget_is_spent(monkeypatch) -> None:
    now = [0.0]
    monkeypatch.setattr(operations, "clock", lambda: now[0])
    halt = operations.Halt(config(), 2.0)
    assert halt.check() is None
    now[0] = 119.0
    assert halt.check() is None
    now[0] = 120.0  # elapsed >= budget: not run
    cut = halt.check()
    assert (cut.state, cut.reason_category, cut.reason_code) == ("DID_NOT_RUN", "BUDGET", "run_budget_exceeded")
    assert "run budget of 2 minutes spent after 2.0 minutes" in cut.detail
    assert halt.cut == 1 and halt.spent()
    assert operations.Halt(config(), None).check() is None  # no run budget (RERUN, REPLAY)
    assert "1 check(s) reached after it" in "\n".join(halt.lines())


def test_the_kill_switch_stays_on_for_the_rest_of_the_run_once_seen(tmp_path: Path) -> None:
    switch = tmp_path / "stop_dre"
    halt = operations.Halt(config(kill_switch_file=str(switch)), None)
    assert halt.check() is None
    switch.write_text("")
    disabled = halt.check()
    assert (disabled.state, disabled.reason_category, disabled.reason_code) == ("DID_NOT_RUN", "PLATFORM", "disabled")
    switch.unlink()
    assert halt.check().reason_code == "disabled"  # removing the file mid-run does not resume it
    assert halt.disabled == 2 and "2 check(s) written DID_NOT_RUN / disabled" in halt.lines()[0]


def test_check_context_defers_the_calendar_sweep_but_never_a_forced_one() -> None:
    from hcsc.datalake.dre.config.loader import load
    from hcsc.datalake.dre.runner import check_context, plan
    from tests.conftest import REPO_ROOT

    cfg, _ = load(REPO_ROOT / "conf")  # full_sweep_day SUNDAY, America/Chicago
    [p] = [p for p in plan(cfg) if (p.dataset.dataset, p.check.check_id) == ("gold_member_coverage", "HOP_KEY_CURRENCY")]
    base = datetime(2026, 1, 18, 6, tzinfo=UTC)  # Sunday 00:00 in Chicago
    sweep = Event("gold_member_coverage", base - timedelta(hours=1), base + timedelta(hours=1))
    assert check_context(None, cfg, p, sweep).full_sweep
    assert not check_context(None, cfg, p, sweep, defer_sweep=True).full_sweep
    assert check_context(None, cfg, p, sweep, force_full_sweep=True, defer_sweep=True).full_sweep  # integrity
    later = Event("gold_member_coverage", base + timedelta(hours=1), base + timedelta(hours=2))
    assert check_context(None, cfg, p, later, force_full_sweep=True).full_sweep  # an owed sweep, forced


# --- with the store ---------------------------------------------------------------------------


def _conf(spark, tmp_path, name):
    from hcsc.datalake.dre.config.validate import validate_conf
    from tests.replay.conftest import replay_conf

    replay = replay_conf(spark, tmp_path, name)
    cfg, errors, _ = validate_conf(replay.conf)
    assert errors == []
    return replay, cfg


def test_backpressure_reads_the_previous_finished_normal_run(spark, tmp_path) -> None:
    from hcsc.datalake.dre.store import runs

    replay, cfg = _conf(spark, tmp_path, "ops_backpressure")
    now = datetime.now(UTC)
    assert operations.previous_run_minutes(spark, replay.dq, now, now.date() - timedelta(days=2)) is None
    assert not operations.defer_full_sweep(spark, replay.dq, cfg, now)

    def finished(started_hours_ago: float, minutes: float, kind: str = "NORMAL", failed: bool = False):
        started = now - timedelta(hours=started_hours_ago)
        run = runs.start_run(spark, replay.dq, now=started, execution_type=kind)
        runs.finish_run(spark, replay.dq, run, 1, 1, failed=failed, now=started + timedelta(minutes=minutes))

    finished(3, 35)  # 35 > 0.8 * 40
    assert operations.defer_full_sweep(spark, replay.dq, cfg, now)
    finished(2, 10)  # the latest finished NORMAL run decides
    assert not operations.defer_full_sweep(spark, replay.dq, cfg, now)
    finished(1.5, 50, kind="RERUN")  # a RERUN is not the hourly run
    runs.start_run(spark, replay.dq, now=now - timedelta(hours=1))  # still STARTED: not finished
    assert operations.previous_run_minutes(spark, replay.dq, now, now.date() - timedelta(days=2)) == pytest.approx(10)
    finished(0.5, 33, failed=True)  # a FAILED run took the time too
    assert operations.backpressure(spark, replay.dq, cfg, now) == pytest.approx(33)
    assert not operations.defer_full_sweep(spark, replay.dq, cfg, now - timedelta(minutes=40))  # before it started


def test_a_deferral_that_cannot_be_recorded_is_not_made(spark, tmp_path, monkeypatch) -> None:
    from hcsc.datalake.dre.runner import plan
    from hcsc.datalake.dre.store import integrity, runs

    replay, cfg = _conf(spark, tmp_path, "ops_unrecorded")
    run = runs.new_run(now=datetime.now(UTC))
    [p] = [p for p in plan(cfg) if (p.dataset.dataset, p.check.check_id) == ("gold_member_coverage", "HOP_LEDGER")]
    event = Event("gold_member_coverage", run.started_at - timedelta(hours=2), run.started_at - timedelta(hours=1))
    monkeypatch.setattr(operations, "previous_run_minutes", lambda *a, **k: 39.0)
    since = run.run_date - timedelta(days=1)

    def broken(*args, **kwargs):
        raise OSError("synthetic store failure")

    monkeypatch.setattr(integrity, "append_records", broken)
    sweeps = operations.plan_sweeps(spark, cfg, run, [(p, event)], lambda d, e: True, since)
    assert sweeps.deferred == frozenset()  # unrecorded, it would be lost: swept now instead
    assert "is NOT deferred" in sweeps.lines[-1] and "OSError" in sweeps.lines[-1]
    monkeypatch.undo()
    monkeypatch.setattr(operations, "previous_run_minutes", lambda *a, **k: 39.0)
    sweeps = operations.plan_sweeps(spark, cfg, run, [(p, event)], lambda d, e: True, since)
    assert sweeps.deferred == frozenset({"gold_member_coverage"})
    [row] = spark.table(f"{replay.dq}.dq_integrity").collect()
    assert (row.record, row.dataset, row.subject_run_id, row.run_id) == (
        operations.SWEEP_DEFERRED, "gold_member_coverage", run.run_id, run.run_id)
    assert "over 0.8 of the 40-minute run budget" in row.detail
