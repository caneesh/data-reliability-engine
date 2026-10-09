"""`dre run` and `dre dry-run`: preconditions, checks and results (spec section 8).

Build step 4 runs the checks and appends their results; causes (step 8) and
the email digest (step 9) come later. A check never stops the run: run_check
turns any failure into DID_NOT_RUN, and a failed write leaves the run PARTIAL.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.base import (
    Check, CheckContext, CheckResult, did_not_run, error_detail, from_exception, run_check,
)
from hcsc.datalake.dre.checks.events import Event, evaluation_id, previous_window_end, window_for
from hcsc.datalake.dre.checks.registry import checks_for
from hcsc.datalake.dre.config.loader import Config, ConfigError
from hcsc.datalake.dre.config.validate import validate_conf
from hcsc.datalake.dre.store import runs
from hcsc.datalake.dre.store.results import append_check_results

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.config.models import Dataset, Feed

log = logging.getLogger(__name__)

EXECUTION_TYPES = ("NORMAL", "RERUN", "REPLAY")
EXIT_COMPLETED, EXIT_PARTIAL, EXIT_CANNOT_START = 0, 2, 3


@dataclass(frozen=True)
class Planned:
    feed: Feed | None  # None for a table-wide dataset
    dataset: Dataset
    check: Check

    @property
    def expectation_version(self) -> int:
        return self.feed.expectation_version if self.feed else (self.dataset.expectation_version or 1)


@dataclass(frozen=True)
class Evaluation:
    planned: Planned
    event: Event
    results: list[CheckResult]
    evaluated_at: datetime
    duration_ms: int


def plan(config: Config, feed_id: str | None = None) -> list[Planned]:
    """Every (feed, dataset, check) to evaluate: feed datasets in config order, then, when no
    feed is named, table-wide datasets (those no feed lists)."""
    feeds = [config.feeds[feed_id]] if feed_id else list(config.feeds.values())
    work = [
        Planned(feed, config.datasets[ds_id], check)
        for feed in feeds
        for ds_id in feed.datasets
        for check in checks_for(config.datasets[ds_id], feed.pattern)
    ]
    if feed_id is None:
        listed = {ds for feed in config.feeds.values() for ds in feed.datasets}
        work += [
            Planned(None, ds, check)
            for ds_id, ds in config.datasets.items() if ds_id not in listed
            for check in checks_for(ds, None)
        ]
    return once_per_table(work, "T1_SCHEMA_DRIFT")


def once_per_table(work: list[Planned], check_id: str) -> list[Planned]:
    """Keep one evaluation of check_id per physical table: the table-wide dataset's if there is
    one, otherwise the first planned."""
    keep: dict[str, Planned] = {}
    for p in work:
        if p.check.check_id == check_id:
            current = keep.get(p.dataset.table)
            if current is None or (p.feed is None and current.feed is not None):
                keep[p.dataset.table] = p
    return [p for p in work if p.check.check_id != check_id or keep[p.dataset.table] is p]


def events_for(spark: SparkSession, config: Config, work: list[Planned], run_start: datetime,
               read_store: bool = True) -> dict[str, Event]:
    """One event per dataset, fixed before any result of this run is written (otherwise a
    dataset's later checks would see its earlier checks' window as the previous one).
    read_store=False (dry-run): if the store cannot be read, window as on a first run."""
    events: dict[str, Event] = {}
    for planned in work:
        ds = planned.dataset
        if ds.dataset in events:
            continue
        try:
            previous = previous_window_end(spark, config.defaults.dq_database, ds.dataset)
        except Exception:
            if read_store:
                raise
            previous = None
        events[ds.dataset] = window_for(ds, run_start, previous, config.dataset_settings(ds.dataset),
                                        config.defaults.timezone)
    return events


def refresh_landing(spark: SparkSession, config: Config, work: list[Planned], run: runs.Run | None,
                    now: datetime) -> dict[str, CheckResult]:
    """List the landing roots of every file-pattern feed in the plan and, on a real run (run given),
    register new and changed files in dq_file. Returns feed id -> DID_NOT_RUN result for feeds whose
    landing could not be listed or registered."""
    from hcsc.datalake.dre.sources.hdfs import LandingError, list_landing
    from hcsc.datalake.dre.store.files import register_files

    feeds = {p.feed.feed: p.feed for p in work if p.feed is not None and p.feed.landing is not None}
    problems: dict[str, CheckResult] = {}
    for feed_id, feed in feeds.items():
        try:
            files = list_landing(spark, feed.landing.roots, feed.landing.file_name_pattern)
        except LandingError as exc:
            problems[feed_id] = did_not_run(exc.code, detail=exc.detail)
            continue
        if run is None:
            print(f"{feed_id}: {len(files)} landed files listed (dry-run: not registered)")
            continue
        try:
            added = register_files(spark, config.defaults.dq_database, feed_id, files, run, now)
            print(f"{feed_id}: {len(files)} landed files listed, {added} new or changed")
        except Exception as exc:
            log.error("could not register landed files for %s: %s", feed_id, error_detail(exc))
            problems[feed_id] = from_exception(exc)
    return problems


def evaluate(spark: SparkSession, config: Config, planned: Planned, event: Event,
             landing_problems: dict[str, CheckResult] | None = None) -> Evaluation:
    settings = config.dataset_settings(planned.dataset.dataset)
    landing_problem = (landing_problems or {}).get(planned.feed.feed) if planned.feed else None
    ctx = CheckContext(spark, planned.dataset, planned.feed, settings, config.defaults.timezone,
                       config.defaults.dq_database, landing_problem)
    results, duration_ms = run_check(planned.check, ctx, event)
    return Evaluation(planned, event, results, datetime.now(timezone.utc), duration_ms)


def result_rows(evaluation: Evaluation, run: runs.Run, execution_type: str) -> list[dict[str, Any]]:
    p = evaluation.planned
    event = evaluation.event
    return [
        {
            "evaluation_id": evaluation_id(event.event_id, p.check.check_id, p.expectation_version,
                                           run.engine_version, r.group_values),
            "run_id": run.run_id, "event_id": event.event_id,
            "window_start": event.window_start, "window_end": event.window_end,
            "execution_type": execution_type, "feed": p.feed.feed if p.feed else None,
            "dataset": p.dataset.dataset,
            "check_id": p.check.check_id, "expectation_version": p.expectation_version,
            "state": r.state, "reason_category": r.reason_category, "reason_code": r.reason_code,
            "population": r.population, "violations": r.violations, "observed": r.observed,
            "expected": r.expected, "group_values": r.group_values, "severity": None,
            "evaluated_at": evaluation.evaluated_at, "duration_ms": evaluation.duration_ms,
            "detail": r.detail, "run_date": run.run_date,
        }
        for r in evaluation.results
    ]


def describe(evaluation: Evaluation) -> list[str]:
    """Counts, check ids and table names only (hard rule 6)."""
    p = evaluation.planned
    lines = []
    for r in evaluation.results:
        groups = ",".join(f"{k}={v}" for k, v in sorted(r.group_values.items()))
        what = (f"{r.reason_category}/{r.reason_code}" if r.reason_code
                else f"population {r.population}, violations {r.violations}")
        lines.append(f"{p.dataset.dataset} {p.check.check_id}{' [' + groups + ']' if groups else ''}: "
                     f"{r.state} ({what})")
    return lines


def _load(conf: str, command: str) -> Config | None:
    config, errors, _ = validate_conf(Path(conf))
    if errors or config.defaults is None:
        print(f"dre {command}: {conf} has {len(errors)} configuration error(s); run dre validate --conf {conf}")
        return None
    return config


def run(spark: SparkSession, conf: str, feed_id: str | None = None, execution_type: str = "NORMAL") -> int:
    from hcsc.datalake.dre.store.install import missing_objects

    config = _load(conf, "run")
    if config is None:
        return EXIT_CANNOT_START
    if feed_id is not None and feed_id not in config.feeds:
        print(f"dre run: unknown feed {feed_id!r}")
        return EXIT_CANNOT_START
    db = config.defaults.dq_database
    missing = missing_objects(spark, db)
    if missing:
        print(f"dre run: the dq store is not installed in {db} (missing: {', '.join(missing)}). "
              "Run dre install --apply once the platform team has created the database.")
        return EXIT_CANNOT_START

    run_record = runs.start_run(spark, db, conf_dir=conf)
    work = plan(config, feed_id)
    written = 0
    try:
        events = events_for(spark, config, work, run_record.started_at)
        landing = refresh_landing(spark, config, work, run_record, run_record.started_at)
        for planned in work:
            evaluation = evaluate(spark, config, planned, events[planned.dataset.dataset], landing)
            try:
                append_check_results(spark, db, result_rows(evaluation, run_record, execution_type))
                written += 1
            except Exception as exc:  # the run continues; the missing write makes it PARTIAL
                log.error("could not write %s for %s: %s", planned.check.check_id, planned.dataset.dataset,
                          error_detail(exc))
            for line in describe(evaluation):
                print(line)
    except Exception as exc:
        log.error("run %s failed: %s", run_record.run_id, error_detail(exc))
        runs.finish_run(spark, db, run_record, len(work), written, failed=True)
        return EXIT_PARTIAL
    status = runs.finish_run(spark, db, run_record, len(work), written)
    print(f"dre run: {status}, {written} of {len(work)} checks written (run {run_record.run_id})")
    return EXIT_COMPLETED if status == runs.COMPLETED else EXIT_PARTIAL


def dry_run(spark: SparkSession, conf: str, feed_id: str) -> int:
    """Preconditions and checks for one feed, printed; nothing is written. Exit 0 unless it cannot start."""
    from hcsc.datalake.dre.config.fragments import check_fragments

    config = _load(conf, "dry-run")
    if config is None:
        return EXIT_CANNOT_START
    if feed_id not in config.feeds:
        print(f"dre dry-run: unknown feed {feed_id!r}")
        return EXIT_CANNOT_START
    problems: list[ConfigError] = check_fragments(spark, config, feed_id)
    for problem in problems:
        print(problem)
    now = datetime.now(timezone.utc)
    work = plan(config, feed_id)
    events = events_for(spark, config, work, now, read_store=False)
    landing = refresh_landing(spark, config, work, None, now)
    for planned in work:
        for line in describe(evaluate(spark, config, planned, events[planned.dataset.dataset], landing)):
            print(line)
    print(f"dre dry-run: {len(work)} checks evaluated, {len(problems)} SQL fragment problem(s), nothing written")
    return 0
