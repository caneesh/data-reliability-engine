"""dq_run bookkeeping (spec sections 4 and 5).

A run's single dq_run row is appended when the run ends, so the table stays
append-only. A run that dies before finishing leaves no row, which is what the
watchdog alerts on (spec section 8).
"""

from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from hcsc.datalake.dre import __version__
from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

COMPLETED, PARTIAL, FAILED = "COMPLETED", "PARTIAL", "FAILED"


@dataclass(frozen=True)
class Run:
    run_id: str
    started_at: datetime
    engine_version: str
    config_commit: str | None


def start_run(conf_dir: Path | str | None = None, now: datetime | None = None) -> Run:
    """A new run: UUID, UTC start time, engine version and the config's git commit."""
    return Run(
        run_id=str(uuid.uuid4()),
        started_at=now or datetime.now(timezone.utc),
        engine_version=__version__,
        config_commit=config_commit(conf_dir) if conf_dir is not None else None,
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


def finish_run(
    spark: SparkSession,
    dq_database: str,
    run: Run,
    checks_expected: int,
    checks_written: int,
    failed: bool = False,
    now: datetime | None = None,
) -> str:
    """Append the run's dq_run row; return its status. run_date is the UTC date it started."""
    validate_dq_database(dq_database)
    ended_at = now or datetime.now(timezone.utc)
    status = run_status(checks_expected, checks_written, failed)
    row = (
        run.run_id,
        run.started_at,
        ended_at,
        run.engine_version,
        run.config_commit,
        status,
        checks_expected,
        checks_written,
        run.started_at.astimezone(timezone.utc).date(),
    )
    schema = (
        "run_id STRING, started_at TIMESTAMP, ended_at TIMESTAMP, engine_version STRING, "
        "config_commit STRING, status STRING, checks_expected INT, checks_written INT, run_date DATE"
    )
    writer.append(spark.createDataFrame([row], schema), dq_database, "dq_run")
    return status
