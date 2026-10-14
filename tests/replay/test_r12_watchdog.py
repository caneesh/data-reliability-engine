"""R12: the main run never happens, and the watchdog alerts (build step 10). Also the other
alerts (a run left STARTED, a run that wrote fewer checks than expected) and the healthy case.
Runs are written relative to now, into the expected hour the watchdog judges."""

from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

from hcsc.datalake.dre.cli import main
from hcsc.datalake.dre.store import runs
from hcsc.datalake.dre.watchdog import RunRow, expected_hour, problems
from tests.conftest import PACKAGE_ROOT
from tests.replay.conftest import replay_conf

UTC = timezone.utc


def hour_now() -> datetime:
    return expected_hour(datetime.now(UTC), 30)


def watchdog(replay, capsys) -> tuple[int, str]:
    capsys.readouterr()
    code = main(["watchdog", "--conf", str(replay.conf)])
    return code, capsys.readouterr().out


def test_r12_the_main_run_never_happens_and_the_watchdog_alerts(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "r12")
    # The last run was three hours ago; the expected hour has none.
    earlier = runs.start_run(spark, replay.dq, now=hour_now() - timedelta(hours=3) + timedelta(minutes=1))
    runs.finish_run(spark, replay.dq, earlier, 10, 10, now=earlier.started_at + timedelta(minutes=5))
    code, out = watchdog(replay, capsys)
    assert code == 1
    hour = hour_now()
    assert f"dre watchdog: no dre run started in {hour:%Y-%m-%d %H:%M}-{hour + timedelta(hours=1):%H:%M} UTC" in out
    assert "email for watchdog: DRE dev watchdog: 1 problem(s) (not sent" in out


def test_watchdog_is_healthy_when_the_expected_run_completed(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "r12_ok")
    run = runs.start_run(spark, replay.dq, now=hour_now() + timedelta(minutes=2))
    runs.finish_run(spark, replay.dq, run, 36, 36, now=run.started_at + timedelta(minutes=4))
    code, out = watchdog(replay, capsys)
    assert code == 0 and "dre watchdog: healthy" in out


def test_watchdog_alerts_on_a_run_left_started_or_incomplete(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "r12_bad")
    # Started on the hour: the watchdog judges this hour only once the grace (30 minutes) has
    # passed since the hour began, so the run is always past its grace, whatever the clock says.
    stuck = runs.start_run(spark, replay.dq, now=hour_now())
    partial = runs.start_run(spark, replay.dq, now=hour_now() + timedelta(minutes=3))
    runs.finish_run(spark, replay.dq, partial, 36, 30, now=partial.started_at + timedelta(minutes=4))
    code, out = watchdog(replay, capsys)
    assert code == 1
    assert f"run {stuck.run_id} " in out and "is still STARTED after" in out
    assert f"run {partial.run_id} " in out and "ended PARTIAL: 30 of 36 checks written" in out
    assert "2 problem(s)" in out


def test_watchdog_alerts_when_the_store_cannot_be_read(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "r12_nostore")
    spark.sql(f"DROP TABLE IF EXISTS {replay.dq}.dq_run")  # test setup only: the store is gone
    code, out = watchdog(replay, capsys)
    assert code == 1 and f"cannot read {replay.dq}.dq_run" in out


def test_a_run_still_within_its_grace_is_not_an_alert() -> None:
    now = datetime(2026, 1, 16, 12, 40, tzinfo=UTC)  # judges 12:00-13:00, grace 30 minutes
    late = RunRow("r1", datetime(2026, 1, 16, 12, 25, tzinfo=UTC), "STARTED")
    assert problems([late], now, 30) == []
    assert problems([late], now + timedelta(minutes=20), 30) == [
        "run r1 (started 12:25 UTC) is still STARTED after 35 minutes"]
    failed = [RunRow("r2", datetime(2026, 1, 16, 12, 1, tzinfo=UTC), "STARTED"),
              RunRow("r2", datetime(2026, 1, 16, 12, 1, tzinfo=UTC), "FAILED", 5, 5,
                     datetime(2026, 1, 16, 12, 3, tzinfo=UTC))]
    assert problems(failed, now, 30) == ["run r2 (started 12:01 UTC) ended FAILED: 5 of 5 checks written"]


def test_the_watchdog_does_not_depend_on_the_main_run_s_code() -> None:
    """Spec section 8: no dependency beyond the store schema (and config and the notification sinks)."""
    tree = ast.parse(Path(PACKAGE_ROOT / "watchdog.py").read_text(encoding="utf-8"))
    imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    imported |= {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    engine = {m for m in imported if m and m.startswith("hcsc.datalake.dre")}
    assert engine == {"hcsc.datalake.dre.store.names", "hcsc.datalake.dre.config.loader",
                      "hcsc.datalake.dre.notify.sinks"}
    sinks = ast.parse(Path(PACKAGE_ROOT / "notify" / "sinks.py").read_text(encoding="utf-8"))
    deps = {n.module for n in ast.walk(sinks) if isinstance(n, ast.ImportFrom) and n.module
            and n.module.startswith("hcsc.datalake.dre") and n.module != "hcsc.datalake.dre.notify.email"}
    assert deps <= {"hcsc.datalake.dre.config.models", "hcsc.datalake.dre.registry", "hcsc.datalake.dre.notify.digest"}


def test_watchdog_owner_gets_the_alerts(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "r12_owner", {"defaults.yaml": [
        ("watchdog_owner: null", "watchdog_owner: provider-data")]})
    code, out = watchdog(replay, capsys)
    assert code == 1 and "email for watchdog" in out
    from hcsc.datalake.dre.config.validate import validate_conf

    (replay.conf / "defaults.yaml").write_text((replay.conf / "defaults.yaml").read_text(encoding="utf-8").replace(
        "watchdog_owner: provider-data", "watchdog_owner: nobody"), encoding="utf-8")
    _, errors, _ = validate_conf(replay.conf)
    assert [e.field for e in errors] == ["watchdog_owner"]


def test_a_manual_rerun_is_not_the_scheduled_run(spark, tmp_path, capsys) -> None:
    """Item 7: dq_run records the execution type; a RERUN in the expected hour does not stand in
    for the missing NORMAL run."""
    replay = replay_conf(spark, tmp_path, "r12_rerun")
    rerun = runs.start_run(spark, replay.dq, now=hour_now() + timedelta(minutes=2), execution_type="RERUN")
    runs.finish_run(spark, replay.dq, rerun, 5, 5, now=rerun.started_at + timedelta(minutes=4))
    code, out = watchdog(replay, capsys)
    assert code == 1 and "no dre run started in" in out


def test_incomplete_cause_investigations_are_reported_apart_from_the_checks() -> None:
    """Finding 4 (code review, October 2026): a run whose checks all completed but whose cause
    investigations did not is still COMPLETED; the watchdog reports the investigations."""
    now = datetime(2026, 1, 16, 12, 40, tzinfo=UTC)
    started = datetime(2026, 1, 16, 12, 1, tzinfo=UTC)
    rows = [RunRow("r3", started, "STARTED"),
            RunRow("r3", started, "COMPLETED", 5, 5, started + timedelta(minutes=2), 3, 1)]
    assert problems(rows, now, 30) == [
        "run r3 (started 12:01 UTC): 2 of 3 cause investigations incomplete (the next run retries them)"]
    done = [rows[0], RunRow("r3", started, "COMPLETED", 5, 5, started + timedelta(minutes=2), 3, 3)]
    assert problems(done, now, 30) == []


def test_the_watchdog_s_own_failures_have_their_own_exit_code(spark, tmp_path, capsys, monkeypatch) -> None:
    """0 healthy, 1 alert, 3 the watchdog could not run: a scheduler keyed on exit codes can tell
    an alert from a broken watchdog. Its store read is bounded by run_date."""
    from hcsc.datalake.dre import watchdog as wd

    replay = replay_conf(spark, tmp_path, "r12_exit")
    (replay.conf / "defaults.yaml").write_text("dq_database: [not a name\n", encoding="utf-8")
    code, out = watchdog(replay, capsys)
    assert code == 3 and "cannot read" in out
    replay = replay_conf(spark, tmp_path / "again", "r12_exit_exc")
    monkeypatch.setattr(wd, "_utc", lambda value: 1 / 0)  # before the store read, whose errors are an alert
    code, out = watchdog(replay, capsys)
    assert code == 3 and "dre watchdog: cannot run (ZeroDivisionError)" in out
    monkeypatch.undo()
    seen: list[str] = []
    real_sql = spark.sql
    monkeypatch.setattr(spark, "sql", lambda text: seen.append(text) or real_sql(text))
    wd.read_runs(spark, replay.dq, datetime(2026, 1, 15, 11, tzinfo=UTC))
    assert "run_date >= DATE'2026-01-15'" in seen[-1]
