"""`dre watchdog` (spec section 8): did the last expected hourly run happen, and finish whole?

A separate entry point, scheduled apart from `dre run` (a different scheduler folder or host).
It depends on nothing of the main run's code beyond the store schema: it reads dq_run, the
configuration (for the store's name, the recipients and the mail relay) and sends mail. It
writes nothing.

The expected run is the hour H = (now - watchdog_grace_minutes) floored to the hour. Alerts:
- no dq_run row started in [H, H + 1 hour);
- a run started then whose latest row is still STARTED more than the grace after it started;
- a run started then that ended FAILED, or wrote fewer checks than it expected.
Alerts go to watchdog_owner's recipients, or every address in recipients when it is not set.
Exit 0 healthy, 1 alert raised (sent, or printed when no mail relay is configured).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

STARTED = "STARTED"
HOUR = timedelta(hours=1)


@dataclass(frozen=True)
class RunRow:
    run_id: str
    started_at: datetime  # UTC
    status: str
    checks_expected: int | None = None
    checks_written: int | None = None
    ended_at: datetime | None = None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def expected_hour(now: datetime, grace_minutes: int) -> datetime:
    return (_utc(now) - timedelta(minutes=grace_minutes)).replace(minute=0, second=0, microsecond=0)


def problems(rows: list[RunRow], now: datetime, grace_minutes: int) -> list[str]:
    """What is wrong with the expected hour's runs; empty when healthy."""
    now = _utc(now)
    hour = expected_hour(now, grace_minutes)
    grace = timedelta(minutes=grace_minutes)
    label = f"{hour:%Y-%m-%d %H:%M}-{hour + HOUR:%H:%M} UTC"
    runs: dict[str, list[RunRow]] = {}
    for row in rows:
        if hour <= _utc(row.started_at) < hour + HOUR:
            runs.setdefault(row.run_id, []).append(row)
    if not runs:
        return [f"no dre run started in {label}"]
    found = []
    for run_id, run_rows in sorted(runs.items(), key=lambda kv: min(_utc(r.started_at) for r in kv[1])):
        started = min(_utc(r.started_at) for r in run_rows)
        final = [r for r in run_rows if r.status != STARTED]
        if not final:
            if now - started > grace:
                minutes = int((now - started).total_seconds() // 60)
                found.append(f"run {run_id} (started {started:%H:%M} UTC) is still STARTED after {minutes} minutes")
            continue
        last = max(final, key=lambda r: _utc(r.ended_at) if r.ended_at else started)
        expected, written = last.checks_expected or 0, last.checks_written or 0
        if last.status == "FAILED" or written < expected:
            found.append(f"run {run_id} (started {started:%H:%M} UTC) ended {last.status}: "
                         f"{written} of {expected} checks written")
    return found


def read_runs(spark: SparkSession, dq_database: str, since: datetime) -> list[RunRow]:
    dq_database = validate_dq_database(dq_database)
    stamp = _utc(since).strftime("%Y-%m-%d %H:%M:%S")
    return [RunRow(r.run_id, _utc(r.started_at), r.status, r.checks_expected, r.checks_written,
                   _utc(r.ended_at) if r.ended_at else None)
            for r in spark.sql(
                f"SELECT run_id, started_at, ended_at, status, checks_expected, checks_written "
                f"FROM {dq_database}.dq_run WHERE started_at >= TIMESTAMP '{stamp}'").collect()]


def watch(spark: SparkSession, conf: str, now: datetime | None = None) -> int:
    from pathlib import Path

    from hcsc.datalake.dre.config.loader import load
    from hcsc.datalake.dre.notify.email import send_text

    config, errors = load(Path(conf))
    defaults = config.defaults
    if defaults is None:
        for error in errors:
            print(error)
        print(f"dre watchdog: cannot read {conf}/defaults.yaml")
        return 1
    now = _utc(now or datetime.now(timezone.utc))
    grace = defaults.watchdog_grace_minutes
    try:
        found = problems(read_runs(spark, defaults.dq_database, expected_hour(now, grace) - HOUR), now, grace)
    except Exception as exc:
        found = [f"cannot read {defaults.dq_database}.dq_run ({type(exc).__name__})"]
    if not found:
        print(f"dre watchdog: healthy (the run for {expected_hour(now, grace):%Y-%m-%d %H:00} UTC completed)")
        return 0
    subject = f"DRE {defaults.environment} watchdog: {len(found)} problem(s)"
    body = "\n".join([subject, "", *[f"- {p}" for p in found], ""])
    for line in found:
        print(f"dre watchdog: {line}")
    if defaults.watchdog_owner is not None:
        recipients = list(defaults.recipients.get(defaults.watchdog_owner, []))
    else:
        recipients = sorted({address for addresses in defaults.recipients.values() for address in addresses})
    print(send_text("watchdog", subject, body, recipients, defaults.email))
    return 1
