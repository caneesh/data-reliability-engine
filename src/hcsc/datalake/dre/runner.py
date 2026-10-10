"""`dre run` and `dre dry-run`: preconditions, checks and results (spec section 8).

It runs the checks, appends their results, explains failures (causes) and
emails one digest per owner. A check never stops the run: run_check
turns any failure into DID_NOT_RUN, and a failed write leaves the run PARTIAL.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.base import (
    Check, CheckContext, CheckResult, did_not_run, error_detail, from_exception, run_check,
)
from hcsc.datalake.dre.checks.events import (
    GAP_CHECK_ID, Event, WindowHistory, evaluation_id, release_hold, window_for, window_history,
)
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
    ctx: CheckContext | None = None


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
    work += plan_rules(config, feed_id)
    return once_per_table(work, "T1_SCHEMA_DRIFT")


def is_rule(planned: Planned) -> bool:
    from hcsc.datalake.dre.checks.rules.rule_check import RuleCheck

    return isinstance(planned.check, RuleCheck)


def rules_due(spark: SparkSession, config: Config, planned: list[Planned], run_start: datetime) -> list[Planned]:
    """Gold rules due by their frequency (checks/rules/schedule.py), whether or not their feed is due."""
    from hcsc.datalake.dre.checks.rules.schedule import is_due, last_completed

    rules = [p for p in planned if is_rule(p)]
    if not rules:
        return []
    done = last_completed(spark, config.defaults.dq_database)
    d = config.defaults
    return [p for p in rules
            if is_due(p.check.rule.frequency, run_start, d.rule_run_at, d.timezone,
                      d.rule_weekly_day or config.dataset_settings(p.dataset.dataset).full_sweep_day,
                      done.get((p.dataset.dataset, p.check.check_id)))]


def plan_rules(config: Config, feed_id: str | None = None) -> list[Planned]:
    """One check per gold rule that is not retired, on its dataset (with the dataset's feed, if any)."""
    from hcsc.datalake.dre.checks.rules.rule_check import RuleCheck

    planned = []
    for rule in config.rules.values():
        if rule.status == "retired" or rule.dataset not in config.datasets:
            continue
        feed = config.feed_of(rule.dataset)
        if feed_id is not None and (feed is None or feed.feed != feed_id):
            continue
        parent_id = rule.params.get("parent_dataset") if rule.template == "child_within_parent" else None
        check = RuleCheck(rule, config.datasets.get(parent_id) if parent_id else None)
        planned.append(Planned(feed, config.datasets[rule.dataset], check))
    return planned


def read_key_secret(config: Config) -> bytes | None:
    """The HMAC secret for key_hash, or None when hmac_secret_file is not set. Raises OSError if set
    but unreadable or empty: a run must not quietly lose its key events."""
    path = config.defaults.hmac_secret_file
    if path is None:
        return None
    secret = Path(path).read_bytes().strip()
    if not secret:
        raise OSError("hmac_secret_file is empty")
    return secret


def is_full_sweep(config: Config, dataset_id: str, run_start: datetime) -> bool:
    """Today (in the default time zone) is the dataset's full_sweep_day."""
    from zoneinfo import ZoneInfo

    day = run_start.astimezone(ZoneInfo(config.defaults.timezone)).strftime("%A").upper()
    return day == config.dataset_settings(dataset_id).full_sweep_day


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
               read_store: bool = True,
               gaps: dict[str, tuple[Event, tuple[str, ...]]] | None = None) -> dict[str, Event]:
    """One event per dataset, fixed before any result of this run is written (otherwise a
    dataset's later checks would see its earlier checks' window as the previous one).
    A window held longer than max_window_hours is released; the given-up span goes in `gaps`.
    read_store=False (dry-run): if the store cannot be read, window as on a first run."""
    events: dict[str, Event] = {}
    for planned in work:
        ds = planned.dataset
        if ds.dataset in events:
            continue
        rule_ids = tuple(r.rule for r in config.rules.values() if r.dataset == ds.dataset)
        try:
            history = window_history(spark, config.defaults.dq_database, ds.dataset, rule_ids)
        except Exception:
            if read_store:
                raise
            history = WindowHistory()
        settings = config.dataset_settings(ds.dataset)
        delay = planned.feed.check_delay_minutes if planned.feed else 0
        end = window_for(ds, run_start, None, settings, config.defaults.timezone, delay).window_end
        previous, gap = release_hold(history, ds.dataset, end, settings.max_window_hours)
        events[ds.dataset] = window_for(ds, run_start, previous, settings, config.defaults.timezone, delay)
        if gap is not None and gaps is not None:
            gaps[ds.dataset] = (gap, history.held_by)
    return events


def gap_row(planned: Planned, gap: Event, held_by: tuple[str, ...], run: runs.Run, execution_type: str,
            now: datetime) -> dict[str, Any]:
    """The WINDOW_GAP result for a held span given up: DID_NOT_RUN / window_gap over that span."""
    hours = (gap.window_end - gap.window_start).total_seconds() / 3600
    return {
        "evaluation_id": evaluation_id(gap.event_id, GAP_CHECK_ID, planned.expectation_version, run.engine_version),
        "run_id": run.run_id, "event_id": gap.event_id,
        "window_start": gap.window_start, "window_end": gap.window_end, "execution_type": execution_type,
        "feed": planned.feed.feed if planned.feed else None, "dataset": planned.dataset.dataset,
        "check_id": GAP_CHECK_ID, "expectation_version": planned.expectation_version,
        "state": "DID_NOT_RUN", "reason_category": "DATA_UNAVAILABLE", "reason_code": "window_gap",
        "observed": f"{hours:.1f} hours never checked", "expected": "every window checked",
        "group_values": {}, "evaluated_at": now, "duration_ms": 0,
        "detail": json.dumps({"unchecked_hours": round(hours, 2), "held_by": list(held_by)}),
        "run_date": run.run_date,
    }


def due_feeds(config: Config, work: list[Planned], events: dict[str, Event], changed_files: dict[str, int],
              forced: set[str]) -> set[str]:
    """Feeds with something due (spec section 8): a cadence slot's deadline (slot + sla_hours)
    fell in one of the feed's dataset windows, i.e. passed since its last evaluation, or new or
    changed landed files were registered this run. Feeds in `forced` are always due."""
    from hcsc.datalake.dre.checks.cadence import UnsupportedCalendar, slots

    due = set(forced)
    for p in work:
        feed = p.feed
        if feed is None or feed.feed in due:
            continue
        if changed_files.get(feed.feed, 0) > 0:
            due.add(feed.feed)
            continue
        event = events[p.dataset.dataset]
        sla = timedelta(hours=config.dataset_settings(p.dataset.dataset).sla_hours)
        try:
            if slots(feed.cadence, event.window_start - sla, event.window_end - sla):
                due.add(feed.feed)
        except UnsupportedCalendar:
            due.add(feed.feed)  # let the checks report invalid_config
    return due


def only_due(work: list[Planned], due: set[str]) -> list[Planned]:
    """Due feeds' work, plus table-wide datasets on a table that a due feed's dataset shares, or
    on a table no feed dataset covers (those run every time)."""
    feed_tables: dict[str, set[str]] = {}
    for p in work:
        if p.feed is not None:
            feed_tables.setdefault(p.dataset.table, set()).add(p.feed.feed)
    kept = []
    for p in work:
        if p.feed is not None:
            if p.feed.feed in due:
                kept.append(p)
        elif not feed_tables.get(p.dataset.table) or feed_tables[p.dataset.table] & due:
            kept.append(p)
    return kept


def refresh_landing(spark: SparkSession, config: Config, work: list[Planned], run: runs.Run | None,
                    now: datetime, changed: dict[str, int] | None = None) -> dict[str, CheckResult]:
    """List the landing roots of every file-pattern feed in the plan and, on a real run (run given),
    register new and changed files in dq_file, counting them per feed in `changed`. Returns feed
    id -> DID_NOT_RUN result for feeds whose landing could not be listed or registered."""
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
            if changed is not None:
                changed[feed_id] = added
            print(f"{feed_id}: {len(files)} landed files listed, {added} new or changed")
        except Exception as exc:
            log.error("could not register landed files for %s: %s", feed_id, error_detail(exc))
            problems[feed_id] = from_exception(exc)
    return problems


def evaluate(spark: SparkSession, config: Config, planned: Planned, event: Event,
             landing_problems: dict[str, CheckResult] | None = None, key_secret: bytes | None = None,
             run_start: datetime | None = None, budget_minutes: float | None = None) -> Evaluation:
    ds = planned.dataset
    settings = config.dataset_settings(ds.dataset)
    landing_problem = (landing_problems or {}).get(planned.feed.feed) if planned.feed else None
    ctx = CheckContext(
        spark, ds, planned.feed, settings, config.defaults.timezone, config.defaults.dq_database, landing_problem,
        upstreams={u: config.datasets[u] for u in ds.key_map if u in config.datasets},
        key_secret=key_secret,
        full_sweep=is_full_sweep(config, ds.dataset, run_start or event.window_end),
        budget_minutes=budget_minutes, scan_mb_per_minute=config.defaults.scan_mb_per_minute,
    )
    results, duration_ms = run_check(planned.check, ctx, event)
    return Evaluation(planned, event, results, datetime.now(timezone.utc), duration_ms, ctx)


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
            "expected": r.expected, "group_values": r.group_values, "severity": p.check.severity,
            "evaluated_at": evaluation.evaluated_at, "duration_ms": evaluation.duration_ms,
            "detail": r.detail, "run_date": run.run_date,
        }
        for r in evaluation.results
    ]


def write_key_events(db: str, evaluation: Evaluation, rows: list[dict[str, Any]], run: runs.Run) -> None:
    """Append each result's key events (hop checks) with the result's evaluation id."""
    from hcsc.datalake.dre.store.key_events import append_key_events

    for result, row in zip(evaluation.results, rows):
        if result.key_events is not None:
            append_key_events(result.key_events, db, run.run_id, row["evaluation_id"], row["dataset"],
                              row["check_id"], row["evaluated_at"], run.run_date)


def explain_failures(spark: SparkSession, config: Config, evaluation: Evaluation, rows: list[dict[str, Any]],
                     run: runs.Run) -> list[str]:
    """Run the cause engine for each FAILED result, append its dq_cause_result rows, and return
    one line per resolved cause (codes and counts only, hard rule 6)."""
    from collections import Counter

    from hcsc.datalake.dre.causes.base import CauseContext
    from hcsc.datalake.dre.causes.engine import explain, failure_type, summarise
    from hcsc.datalake.dre.store.causes import append_cause_results

    p = evaluation.planned
    ft = failure_type(p.feed.pattern if p.feed else None, p.check.check_id)
    if ft is None or evaluation.ctx is None:
        return []
    lines = []
    for result, row in zip(evaluation.results, rows):
        if result.state != "FAILED":
            continue
        upstream = result.group_values.get("upstream") if p.check.check_id.startswith("HOP_") else None
        cx = CauseContext(evaluation.ctx, evaluation.event, config, p.check.check_id, upstream)
        try:
            cause_rows = explain(cx, result, row["evaluation_id"], run.run_id, run.run_date,
                                 datetime.now(timezone.utc))
            append_cause_results(spark, config.defaults.dq_database, cause_rows)
        except Exception as exc:  # causes never stop the run
            log.error("could not explain %s for %s: %s", p.check.check_id, p.dataset.dataset, error_detail(exc))
            continue
        counts = Counter(line.text() for line in summarise(cause_rows, ft).values())
        lines += [f"  {text} [{n} failing]" for text, n in sorted(counts.items())]
    return lines


def email_digests(spark: SparkSession, config: Config, run: runs.Run) -> list[str]:
    """Build this run's digests from the store and send them; never stops the run."""
    from hcsc.datalake.dre.notify.digest import digests_for_run
    from hcsc.datalake.dre.notify.email import send

    from hcsc.datalake.dre.notify.digest import to_send

    try:
        digests = digests_for_run(spark, config, run)
        kept = to_send(digests, config.defaults, run.started_at)
        skipped = [f"email for {d.owner}: {d.subject} (not sent: email.all_clear_digest is "
                   f"{config.defaults.email.all_clear_digest})" for d in digests if d not in kept]
        return send(kept, config.defaults.email) + skipped
    except Exception as exc:
        log.error("could not build the digests for run %s: %s", run.run_id, error_detail(exc))
        return [f"email: not sent ({type(exc).__name__})"]


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
    """One scheduled (hourly) run: only feeds with something due are evaluated; --feed forces one."""
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

    try:
        key_secret = read_key_secret(config)
    except OSError as exc:
        print(f"dre run: cannot read hmac_secret_file ({type(exc).__name__}); key events need it")
        return EXIT_CANNOT_START
    run_record = runs.start_run(spark, db, conf_dir=conf)
    planned_all = plan(config, feed_id)
    work: list[Planned] = []
    written = expected = 0
    try:
        gaps: dict[str, tuple[Event, tuple[str, ...]]] = {}
        events = events_for(spark, config, planned_all, run_record.started_at, gaps=gaps)
        changed: dict[str, int] = {}
        landing = refresh_landing(spark, config, planned_all, run_record, run_record.started_at, changed)
        checks = [p for p in planned_all if not is_rule(p)]
        due = due_feeds(config, checks, events, changed, forced={feed_id} if feed_id else set())
        work = only_due(checks, due)
        for skipped in sorted({p.feed.feed for p in checks if p.feed is not None} - due):
            print(f"{skipped}: nothing due (no slot deadline passed, no new files); not evaluated")
        gap_for = {p.dataset.dataset: p for p in work if p.dataset.dataset in gaps}
        work += rules_due(spark, config, planned_all, run_record.started_at)
        expected = len(work) + len(gap_for)
        for ds_id, planned in gap_for.items():
            gap, held_by = gaps[ds_id]
            append_check_results(spark, db, [gap_row(planned, gap, held_by, run_record, execution_type,
                                                     datetime.now(timezone.utc))])
            written += 1
            print(f"{ds_id} {GAP_CHECK_ID}: DID_NOT_RUN (DATA_UNAVAILABLE/window_gap)")
        spent: dict[str, float] = {}  # compute_budget_scope dataset: minutes used per dataset this run
        for planned in work:
            ds_id = planned.dataset.dataset
            budget = None
            if config.defaults.compute_budget_scope == "dataset":
                budget = config.dataset_settings(ds_id).compute_budget_minutes - spent.get(ds_id, 0.0)
            evaluation = evaluate(spark, config, planned, events[ds_id], landing, key_secret,
                                  run_record.started_at, budget)
            spent[ds_id] = spent.get(ds_id, 0.0) + evaluation.duration_ms / 60000
            try:
                rows = result_rows(evaluation, run_record, execution_type)
                append_check_results(spark, db, rows)
                written += 1
                write_key_events(db, evaluation, rows, run_record)
            except Exception as exc:  # the run continues; the missing write makes it PARTIAL
                log.error("could not write %s for %s: %s", planned.check.check_id, planned.dataset.dataset,
                          error_detail(exc))
                rows = []
            for line in describe(evaluation) + explain_failures(spark, config, evaluation, rows, run_record):
                print(line)
    except Exception as exc:
        log.error("run %s failed: %s", run_record.run_id, error_detail(exc))
        runs.finish_run(spark, db, run_record, expected, written, failed=True)
        return EXIT_PARTIAL
    for line in email_digests(spark, config, run_record):
        print(line)
    status = runs.finish_run(spark, db, run_record, expected, written)
    print(f"dre run: {status}, {written} of {expected} checks written (run {run_record.run_id})")
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
