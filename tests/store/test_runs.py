"""dq_run bookkeeping. Step 3 "done when": a run with no checks writes a dq_run row."""

from __future__ import annotations

import subprocess
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from hcsc.datalake.dre import __version__
from hcsc.datalake.dre.store.runs import config_commit, finish_run, run_status, start_run
from tests.store.conftest import ts


def test_run_with_no_checks_writes_one_dq_run_row(spark, store) -> None:
    run = start_run(now=datetime(2026, 1, 5, 23, 50, tzinfo=timezone.utc))
    status = finish_run(spark, store, run, checks_expected=0, checks_written=0,
                        now=datetime(2026, 1, 6, 0, 5, tzinfo=timezone.utc))
    assert status == "COMPLETED"
    rows = spark.table(f"{store}.dq_run").where(f"run_id = '{run.run_id}'").collect()
    assert len(rows) == 1
    row = rows[0]
    assert (row.status, row.checks_expected, row.checks_written) == ("COMPLETED", 0, 0)
    assert row.engine_version == __version__
    assert row.started_at < row.ended_at
    assert row.run_date == date(2026, 1, 5)  # UTC date the run started


def test_each_run_appends(spark, store) -> None:
    before = spark.table(f"{store}.dq_run").count()
    first, second = start_run(now=ts(7)), start_run(now=ts(7, 4))
    finish_run(spark, store, first, 3, 3, now=ts(7, 1))
    finish_run(spark, store, second, 3, 2, now=ts(7, 5))
    rows = {r.run_id: r.status for r in spark.table(f"{store}.dq_run").collect()}
    assert spark.table(f"{store}.dq_run").count() == before + 2
    assert (rows[first.run_id], rows[second.run_id]) == ("COMPLETED", "PARTIAL")
    assert first.run_id != second.run_id


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
    assert start_run(conf_dir=tmp_path).config_commit == head


def test_config_commit_outside_git_is_none(tmp_path: Path) -> None:
    assert config_commit(tmp_path) is None
