"""dq_run bookkeeping (spec sections 4 and 5).

A run appends a STARTED row when it starts and a final row (COMPLETED,
PARTIAL or FAILED) when it ends; its state is its latest row (view
v_latest_run). A run that dies before finishing stays STARTED, which the
watchdog reports. Both rows carry run_date = the UTC date the run started.
"""

from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from hcsc.datalake.dre import __version__
from hcsc.datalake.dre.checks.times import as_utc
from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

STARTED, COMPLETED, PARTIAL, FAILED = "STARTED", "COMPLETED", "PARTIAL", "FAILED"

_SCHEMA = (
    "run_id STRING, started_at TIMESTAMP, ended_at TIMESTAMP, engine_version STRING, "
    "config_commit STRING, status STRING, checks_expected INT, checks_written INT, "
    "investigations_expected INT, investigations_done INT, execution_type STRING, run_date DATE"
)


@dataclass(frozen=True)
class Run:
    run_id: str
    started_at: datetime
    engine_version: str
    config_commit: str | None
    execution_type: str = "NORMAL"  # NORMAL | RERUN | REPLAY

    @property
    def run_date(self) -> date:
        return self.started_at.astimezone(timezone.utc).date()


def new_run(conf_dir: Path | str | None = None, now: datetime | None = None, execution_type: str = "NORMAL") -> Run:
    """A new run: UUID, UTC start time, engine version, the config's git commit and execution type."""
    return Run(
        run_id=str(uuid.uuid4()),
        started_at=now or datetime.now(timezone.utc),
        engine_version=__version__,
        config_commit=config_commit(conf_dir) if conf_dir is not None else None,
        execution_type=execution_type,
    )


def config_commit(conf_dir: Path | str) -> str | None:
    """HEAD commit of the git repository holding conf_dir, or None if there is none."""
    try:
        out = subprocess.run(
            ["git", "-C", str(conf_dir), "rev-parse", "HEAD"],
            capture_output=True, text=True, check=True, timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() or None


def run_status(checks_expected: int, checks_written: int, failed: bool = False) -> str:
    if failed:
        return FAILED
    return COMPLETED if checks_written >= checks_expected else PARTIAL


def start_run(
    spark: SparkSession, dq_database: str, conf_dir: Path | str | None = None, now: datetime | None = None,
    execution_type: str = "NORMAL",
) -> Run:
    """Create a run and append its STARTED row."""
    run = new_run(conf_dir, now, execution_type)
    _append(spark, dq_database, run, None, STARTED, None, None)
    return run


def in_progress(spark: SparkSession, dq_database: str, now: datetime,
                grace_minutes: int) -> tuple[str, datetime] | None:
    """The NORMAL run whose latest row is still STARTED and that started within the last
    grace_minutes, as (run_id, started_at); None when there is none. An older STARTED run died
    (the watchdog reports it) and does not block the next run."""
    validate_dq_database(dq_database)
    since = as_utc(now) - timedelta(minutes=grace_minutes)
    rows = spark.sql(
        f"SELECT run_id, started_at FROM {dq_database}.v_latest_run "
        f"WHERE status = '{STARTED}' AND execution_type = 'NORMAL' "
        f"AND started_at >= TIMESTAMP '{since:%Y-%m-%d %H:%M:%S.%f}' ORDER BY started_at DESC LIMIT 1"
    ).collect()
    return None if not rows else (rows[0].run_id, as_utc(rows[0].started_at))


def finish_run(
    spark: SparkSession,
    dq_database: str,
    run: Run,
    checks_expected: int,
    checks_written: int,
    failed: bool = False,
    now: datetime | None = None,
    investigations: tuple[int, int] = (0, 0),
) -> str:
    """Append the run's final row; return its status. The status is about checks only; cause
    investigations (expected, done) are recorded beside it, so an investigation that could not
    finish never relabels a check, and the watchdog reports it."""
    status = run_status(checks_expected, checks_written, failed)
    _append(spark, dq_database, run, now or datetime.now(timezone.utc), status, checks_expected, checks_written,
            investigations)
    return status


def _append(
    spark: SparkSession, dq_database: str, run: Run, ended_at: datetime | None, status: str,
    checks_expected: int | None, checks_written: int | None,
    investigations: tuple[int, int] | None = None,
) -> None:
    validate_dq_database(dq_database)
    expected, done = investigations if investigations is not None else (None, None)
    row = (run.run_id, run.started_at, ended_at, run.engine_version, run.config_commit, status,
           checks_expected, checks_written, expected, done, run.execution_type, run.run_date)
    writer.append(spark.createDataFrame([row], _SCHEMA), dq_database, "dq_run")
