"""`dre watchdog` (spec section 8): did the last expected hourly run happen, and finish whole?

A separate entry point, scheduled apart from `dre run` (a different scheduler folder or host).
It depends on nothing of the main run's code beyond the store schema: it reads dq_run, the
configuration (for the store's name, the recipients and the mail relay) and sends mail. It
writes nothing.

The expected run is the hour H = (now - watchdog_grace_minutes) floored to the hour. Alerts:
- no NORMAL dq_run row started in [H, H + 1 hour) (a manual RERUN or REPLAY is not the scheduled run);
- a run started then whose latest row is still STARTED more than the grace after it started;
- a run started then that ended FAILED, or wrote fewer checks than it expected;
- a run started then whose cause investigations did not all finish (tracked apart from its
  checks, so the checks keep their states; the next run retries the investigations);
- the store's integrity is not verified: no integrity sweep completed in the last 26 hours, once
  the store is older than that (checkpoint 2 corrections, R1: integrity-check freshness);
- integrity incidents (an evidence mismatch or a commit conflict) recorded in that hour.
Alerts go to watchdog_owner's recipients, or every address in recipients when it is not set.
Exit 0 healthy, 1 alert raised (sent, or printed when no mail relay is configured).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

STARTED = "STARTED"
HOUR = timedelta(hours=1)
STALE_AFTER = timedelta(hours=26)  # the integrity sweep runs daily


@dataclass(frozen=True)
class RunRow:
    run_id: str
    started_at: datetime  # UTC
    status: str
    checks_expected: int | None = None
    checks_written: int | None = None
    ended_at: datetime | None = None
    investigations_expected: int | None = None
    investigations_done: int | None = None


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
        wanted, done = last.investigations_expected or 0, last.investigations_done or 0
        if done < wanted:
            found.append(f"run {run_id} (started {started:%H:%M} UTC): {wanted - done} of {wanted} cause "
                         f"investigations incomplete (the next run retries them)")
    return found


EXIT_HEALTHY, EXIT_ALERT, EXIT_CANNOT_RUN = 0, 1, 3


def read_runs(spark: SparkSession, dq_database: str, since: datetime) -> list[RunRow]:
    dq_database = validate_dq_database(dq_database)
    stamp = _utc(since).strftime("%Y-%m-%d %H:%M:%S")
    day = _utc(since).date().isoformat()
    return [RunRow(r.run_id, _utc(r.started_at), r.status, r.checks_expected, r.checks_written,
                   _utc(r.ended_at) if r.ended_at else None, r.investigations_expected, r.investigations_done)
            for r in spark.sql(
                f"SELECT run_id, started_at, ended_at, status, checks_expected, checks_written, "
                f"investigations_expected, investigations_done "
                f"FROM {dq_database}.dq_run WHERE run_date >= DATE'{day}' AND started_at >= TIMESTAMP '{stamp}' "
                f"AND coalesce(execution_type, 'NORMAL') = 'NORMAL'").collect()]


def integrity_problems(spark: SparkSession, dq_database: str, now: datetime, grace_minutes: int) -> list[str]:
    """Integrity-check freshness and the expected hour's integrity incidents (dq_integrity)."""
    dq_database = validate_dq_database(dq_database)
    now = _utc(now)
    hour = expected_hour(now, grace_minutes)
    stamp = lambda t: _utc(t).strftime("%Y-%m-%d %H:%M:%S")  # noqa: E731
    row = spark.sql(
        f"SELECT MAX(CASE WHEN record = 'SWEEP' AND status = 'COMPLETED' THEN recorded_at END) AS verified, "
        f"SUM(CASE WHEN record IN ('EVIDENCE_MISMATCH', 'COMMIT_CONFLICT') AND run_date >= DATE'{hour.date().isoformat()}' "
        f"  AND recorded_at >= TIMESTAMP '{stamp(hour)}' AND recorded_at < TIMESTAMP '{stamp(hour + HOUR)}' "
        f"  THEN 1 ELSE 0 END) AS incidents FROM {dq_database}.dq_integrity").collect()[0]
    found = []
    oldest = spark.sql(f"SELECT MIN(started_at) AS t FROM {dq_database}.dq_run").collect()[0].t
    if oldest is not None and now - _utc(oldest) > STALE_AFTER:
        if row.verified is None:
            found.append("store integrity not verified: no integrity sweep has completed")
        elif now - _utc(row.verified) > STALE_AFTER:
            found.append(f"store integrity verification is stale: the last integrity sweep completed "
                         f"{_utc(row.verified):%Y-%m-%d %H:%M} UTC")
    if row.incidents:
        found.append(f"{int(row.incidents)} integrity incident(s) recorded in {hour:%Y-%m-%d %H:%M}-"
                     f"{hour + HOUR:%H:%M} UTC (dq_integrity: evidence changed or lost after publication, "
                     "or a conflicting commit)")
    return found


def watch(spark: SparkSession, conf: str, now: datetime | None = None) -> int:
    """0 healthy, 1 alert sent (or printed), 3 the watchdog itself could not run (configuration
    unreadable, or an error outside the store read), so a scheduler can tell the two apart."""
    try:
        return _watch(spark, conf, now)
    except Exception as exc:
        print(f"dre watchdog: cannot run ({type(exc).__name__})")
        return EXIT_CANNOT_RUN


def _watch(spark: SparkSession, conf: str, now: datetime | None) -> int:
    from pathlib import Path

    from hcsc.datalake.dre.config.loader import load
    from hcsc.datalake.dre.notify.sinks import Message, deliver

    config, errors = load(Path(conf))
    defaults = config.defaults
    if defaults is None:
        for error in errors:
            print(error)
        print(f"dre watchdog: cannot read {conf}/defaults.yaml")
        return EXIT_CANNOT_RUN
    now = _utc(now or datetime.now(timezone.utc))
    grace = defaults.watchdog_grace_minutes
    try:
        found = problems(read_runs(spark, defaults.dq_database, expected_hour(now, grace) - HOUR), now, grace)
    except Exception as exc:
        found = [f"cannot read {defaults.dq_database}.dq_run ({type(exc).__name__})"]
    try:
        found += integrity_problems(spark, defaults.dq_database, now, grace)
    except Exception as exc:
        found.append(f"cannot read {defaults.dq_database}.dq_integrity ({type(exc).__name__})")
    if not found:
        print(f"dre watchdog: healthy (the run for {expected_hour(now, grace):%Y-%m-%d %H:00} UTC completed)")
        return EXIT_HEALTHY
    subject = f"DRE {defaults.environment} watchdog: {len(found)} problem(s)"
    body = "\n".join([subject, "", *[f"- {p}" for p in found], ""])
    for line in found:
        print(f"dre watchdog: {line}")
    if defaults.watchdog_owner is not None:
        recipients = list(defaults.recipients.get(defaults.watchdog_owner, []))
    else:
        recipients = sorted({address for addresses in defaults.recipients.values() for address in addresses})
    for line in deliver([Message("watchdog", subject, body, recipients)], defaults):
        print(line)
    return EXIT_ALERT
