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

from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, error_detail, run_check
from hcsc.datalake.dre.checks.events import Event, evaluation_id, previous_window_end
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
    feed: Feed
    dataset: Dataset
    check: Check


@dataclass(frozen=True)
class Evaluation:
    planned: Planned
    event: Event
    results: list[CheckResult]
    evaluated_at: datetime
    duration_ms: int


def plan(config: Config, feed_id: str | None = None) -> list[Planned]:
    """Every (feed, dataset, check) to evaluate, in config order."""
    feeds = [config.feeds[feed_id]] if feed_id else list(config.feeds.values())
    return [
        Planned(feed, config.datasets[ds_id], check)
        for feed in feeds
        for ds_id in feed.datasets
        for check in checks_for(config.datasets[ds_id], feed.pattern)
    ]


def evaluate(spark: SparkSession, config: Config, planned: Planned, window_end: datetime,
             window_start: datetime | None) -> Evaluation:
    event = Event(planned.dataset.dataset, window_start, window_end)
    ctx = CheckContext(spark, planned.dataset, planned.feed, config.dataset_settings(planned.dataset.dataset))
    results, duration_ms = run_check(planned.check, ctx, event)
    return Evaluation(planned, event, results, datetime.now(timezone.utc), duration_ms)


def result_rows(evaluation: Evaluation, run: runs.Run, execution_type: str) -> list[dict[str, Any]]:
    p = evaluation.planned
    eval_id = evaluation_id(evaluation.event.event_id, p.check.check_id, p.feed.expectation_version,
                            run.engine_version)
    return [
        {
            "evaluation_id": eval_id, "run_id": run.run_id, "event_id": evaluation.event.event_id,
            "execution_type": execution_type, "feed": p.feed.feed, "dataset": p.dataset.dataset,
            "check_id": p.check.check_id, "expectation_version": p.feed.expectation_version,
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
        for planned in work:
            window_start = previous_window_end(spark, db, planned.dataset.dataset)
            evaluation = evaluate(spark, config, planned, run_record.started_at, window_start)
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
    db = config.defaults.dq_database
    problems: list[ConfigError] = check_fragments(spark, config, feed_id)
    for problem in problems:
        print(problem)
    now = datetime.now(timezone.utc)
    work = plan(config, feed_id)
    for planned in work:
        try:
            window_start = previous_window_end(spark, db, planned.dataset.dataset)
        except Exception:
            window_start = None
        for line in describe(evaluate(spark, config, planned, now, window_start)):
            print(line)
    print(f"dre dry-run: {len(work)} checks evaluated, {len(problems)} SQL fragment problem(s), nothing written")
    return 0
