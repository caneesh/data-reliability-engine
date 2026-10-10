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
    stuck = runs.start_run(spark, replay.dq, now=hour_now() + timedelta(minutes=1))
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
    """Spec section 8: no dependency beyond the store schema (and config and mail)."""
    tree = ast.parse(Path(PACKAGE_ROOT / "watchdog.py").read_text(encoding="utf-8"))
    imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    imported |= {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    engine = {m for m in imported if m and m.startswith("hcsc.datalake.dre")}
    assert engine == {"hcsc.datalake.dre.store.names", "hcsc.datalake.dre.config.loader",
                      "hcsc.datalake.dre.notify.email"}


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
