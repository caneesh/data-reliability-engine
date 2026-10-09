"""dq_run bookkeeping: a STARTED row at the start, a final row at the end.

Step 3 "done when": a run with no checks writes its dq_run rows.
"""

from __future__ import annotations

import subprocess
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from hcsc.datalake.dre import __version__
from hcsc.datalake.dre.store.runs import config_commit, finish_run, new_run, run_status, start_run
from tests.store.conftest import ts


def run_rows(spark, store, run_id):
    return sorted(spark.table(f"{store}.dq_run").where(f"run_id = '{run_id}'").collect(),
                  key=lambda r: r.status != "STARTED")


def test_run_with_no_checks_writes_started_then_final_row(spark, store) -> None:
    run = start_run(spark, store, now=datetime(2026, 1, 5, 23, 50, tzinfo=timezone.utc))
    started = run_rows(spark, store, run.run_id)
    assert [(r.status, r.ended_at, r.checks_expected, r.checks_written) for r in started] == [
        ("STARTED", None, None, None)]

    status = finish_run(spark, store, run, checks_expected=0, checks_written=0,
                        now=datetime(2026, 1, 6, 0, 5, tzinfo=timezone.utc))
    assert status == "COMPLETED"
    first, final = run_rows(spark, store, run.run_id)
    assert (first.status, final.status) == ("STARTED", "COMPLETED")
    assert (final.checks_expected, final.checks_written) == (0, 0)
    assert final.engine_version == __version__
    assert final.started_at < final.ended_at
    # Both rows sit in the partition of the UTC date the run started, though it ended the next day.
    assert first.run_date == final.run_date == date(2026, 1, 5)


def test_latest_run_view_gives_each_runs_state(spark, store) -> None:
    finished = start_run(spark, store, now=ts(7))
    unfinished = start_run(spark, store, now=ts(7, 4))
    partial = start_run(spark, store, now=ts(7, 6))
    finish_run(spark, store, finished, 3, 3, now=ts(7, 1))
    finish_run(spark, store, partial, 3, 2, now=ts(7, 7))
    latest = {r.run_id: r.status for r in spark.table(f"{store}.v_latest_run").collect()}
    assert latest[finished.run_id] == "COMPLETED"
    assert latest[unfinished.run_id] == "STARTED"  # the watchdog reports runs left here
    assert latest[partial.run_id] == "PARTIAL"
    assert spark.table(f"{store}.v_latest_run").where(f"run_id = '{finished.run_id}'").count() == 1


def test_each_run_appends(spark, store) -> None:
    before = spark.table(f"{store}.dq_run").count()
    run = start_run(spark, store, now=ts(8))
    finish_run(spark, store, run, 1, 1, failed=True, now=ts(8, 1))
    assert spark.table(f"{store}.dq_run").count() == before + 2
    assert [r.status for r in run_rows(spark, store, run.run_id)] == ["STARTED", "FAILED"]


@pytest.mark.parametrize(
    ("expected", "written", "failed", "status"),
    [(0, 0, False, "COMPLETED"), (5, 5, False, "COMPLETED"), (5, 4, False, "PARTIAL"), (5, 5, True, "FAILED")],
)
def test_run_status(expected: int, written: int, failed: bool, status: str) -> None:
    assert run_status(expected, written, failed) == status


def test_config_commit_from_git(tmp_path: Path) -> None:
    git = ["git", "-C", str(tmp_path), "-c", "user.email=t@example.com", "-c", "user.name=t"]
    subprocess.run([*git, "init", "-q"], check=True)
    (tmp_path / "defaults.yaml").write_text("dq_database: dq\n", encoding="utf-8")
    subprocess.run([*git, "add", "."], check=True)
    subprocess.run([*git, "commit", "-qm", "conf"], check=True)
    head = subprocess.run([*git, "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    assert config_commit(tmp_path) == head
    assert new_run(conf_dir=tmp_path).config_commit == head


def test_config_commit_outside_git_is_none(tmp_path: Path) -> None:
    assert config_commit(tmp_path) is None
