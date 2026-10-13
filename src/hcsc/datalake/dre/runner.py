"""`dre run` and `dre dry-run`: preconditions, checks and results (spec section 8).

It runs the checks, appends their results, explains failures (causes) and
emails one digest per owner. A check never stops the run: run_check
turns any failure into DID_NOT_RUN, and a failed write leaves the run PARTIAL.

Execution types (spec section 4):
- NORMAL (scheduled): each dataset's next eligible window; due feeds only; gold rules by their
  frequency; registers landed files and keeps the open-key state.
- RERUN: one named historical window [start, end) for every dataset planned.
- REPLAY: every window of a historical range [start, end): the recorded NORMAL windows in it,
  exactly, and each span none of them covers.
RERUN and REPLAY are idempotent: a result identical to the stored result of the same evaluation
is not written again, and they neither read nor change the open-key state (no key events) or
the dq_file registry. They evaluate the windowed checks only (gold rules and T1_SCHEMA_DRIFT
read the table as it is now, not a window), never move or hold a NORMAL window, and send no email.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.causes.base import CauseContext
from hcsc.datalake.dre.causes.engine import explain, failure_type, summarise
from hcsc.datalake.dre.checks.base import (
    Check, CheckContext, CheckResult, did_not_run, error_detail, from_exception, run_check,
)
from hcsc.datalake.dre.checks.cadence import slots, UnsupportedCalendar
from hcsc.datalake.dre.checks.derived import base_datasets
from hcsc.datalake.dre.checks.events import (
    evaluation_id, Event, fixed_window, GAP_CHECK_ID, recorded_windows, release_hold, replay_windows, window_for,
    window_histories, WindowHistory,
)
from hcsc.datalake.dre.checks.hop.common import hop_label
from hcsc.datalake.dre.checks.patterns import checks_for
from hcsc.datalake.dre.checks.rules.rule_check import RuleCheck
from hcsc.datalake.dre.checks.rules.schedule import is_due, last_completed
from hcsc.datalake.dre.checks.rules.template import TEMPLATES
from hcsc.datalake.dre.checks.times import as_utc
from hcsc.datalake.dre.config.fragments import check_fragments
from hcsc.datalake.dre.config.loader import Config, ConfigError
from hcsc.datalake.dre.config.models import Dataset, Feed
from hcsc.datalake.dre.config.validate import validate_conf
from hcsc.datalake.dre.notify.digest import digests_for_run, GROUPS_SQL, to_send
from hcsc.datalake.dre.notify.sinks import deliver, from_digests
from hcsc.datalake.dre.sources.hdfs import LandingError, list_landing
from hcsc.datalake.dre.store import runs
from hcsc.datalake.dre.store.causes import append_cause_results
from hcsc.datalake.dre.store.files import register_files
from hcsc.datalake.dre.store.install import missing_objects
from hcsc.datalake.dre.store.key_events import append_key_events
from hcsc.datalake.dre.store.results import append_check_results

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


log = logging.getLogger(__name__)

SCHEMA_DRIFT = "T1_SCHEMA_DRIFT"  # runs once per physical table (spec section 6)
EXECUTION_TYPES = ("NORMAL", "RERUN", "REPLAY")
_HEX = re.compile(r"^[0-9a-f]{64}$")
# The columns that make two results of one evaluation the same outcome (idempotent reruns).
OUTCOME = ("state", "reason_category", "reason_code", "population", "violations", "observed", "expected",
           "detail", "severity")

EXIT_COMPLETED, EXIT_PARTIAL, EXIT_CANNOT_START = 0, 2, 3


@dataclass(frozen=True)
class Selection:
    """What a run evaluates: NORMAL, the next eligible windows; RERUN, the historical window
    [start, end); REPLAY, every window of the historical range [start, end)."""

    execution_type: str = "NORMAL"
    start: datetime | None = None
    end: datetime | None = None

    def problem(self, now: datetime) -> str | None:
        """Why this selection cannot run, or None."""
        kind = self.execution_type
        if kind not in EXECUTION_TYPES:
            return f"unknown execution type {kind!r}"
        if kind == "NORMAL":
            if self.start is not None or self.end is not None:
                return "a NORMAL run takes no window: it evaluates each dataset's next eligible window"
            return None
        flags = "--window-start and --window-end" if kind == "RERUN" else "--from and --to"
        if self.start is None or self.end is None:
            return f"a {kind} run needs {flags} (UTC)"
        if self.start >= self.end:
            return f"{flags}: the start must be before the end"
        if self.end > now:
            return f"{flags}: a {kind} names a historical span; it must end by now"
        return None


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


def plan(config: Config, feed_id: str | None = None, per_table: bool = True) -> list[Planned]:
    """Every (feed, dataset, check) to evaluate: feed datasets in config order, then, when no
    feed is named, table-wide datasets (those no feed lists). per_table=False keeps every
    T1_SCHEMA_DRIFT, for a caller that drops feeds that are not due before choosing one per table."""
    feeds = [config.feeds[feed_id]] if feed_id else list(config.feeds.values())
    work = [
        Planned(feed, config.datasets[ds_id], check)
        for feed in feeds
        for ds_id in feed.datasets
        for check in checks_for(config.datasets[ds_id], feed.pattern, feed)
    ]
    if feed_id is None:
        listed = {ds for feed in config.feeds.values() for ds in feed.datasets}
        work += [
            Planned(None, ds, check)
            for ds_id, ds in config.datasets.items() if ds_id not in listed and ds.derived is None
            for check in checks_for(ds, None)  # derived datasets are read as upstreams, never checked
        ]
    work += plan_rules(config, feed_id)
    return once_per_table(work, SCHEMA_DRIFT) if per_table else work


def is_rule(planned: Planned) -> bool:
    return isinstance(planned.check, RuleCheck)


def rules_due(spark: SparkSession, config: Config, planned: list[Planned], run_start: datetime) -> list[Planned]:
    """Gold rules due by their frequency (checks/rules/schedule.py), whether or not their feed is due."""

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

    planned = []
    for rule in config.rules.values():
        if rule.status == "retired" or rule.dataset not in config.datasets:
            continue
        feed = config.feed_of(rule.dataset)
        if feed_id is not None and (feed is None or feed.feed != feed_id):
            continue
        spec = TEMPLATES[rule.template]
        named = spec.related(spec.params_model.model_validate(rule.params)).values()
        related = {d: config.datasets[d] for d in named if d in config.datasets}
        planned.append(Planned(feed, config.datasets[rule.dataset], RuleCheck(rule, related)))
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


def is_full_sweep(config: Config, dataset_id: str, event: Event) -> bool:
    """The event is the dataset's first window ending on its full_sweep_day (in the default time
    zone): the window ends that day and starts before it. So the sweep runs once that day, not
    in every hourly run, and again only if that window is held and evaluated again."""
    from zoneinfo import ZoneInfo

    tz = ZoneInfo(config.defaults.timezone)
    end = event.window_end.astimezone(tz)
    day_start = end.replace(hour=0, minute=0, second=0, microsecond=0)
    return (end.strftime("%A").upper() == config.dataset_settings(dataset_id).full_sweep_day
            and event.window_start.astimezone(tz) < day_start)


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
    datasets = list(dict.fromkeys(p.dataset.dataset for p in work))
    try:
        histories = window_histories(spark, config.defaults.dq_database, datasets)
    except Exception:
        if read_store:
            raise
        histories = {}
    for planned in work:
        ds = planned.dataset
        if ds.dataset in events:
            continue
        history = histories.get(ds.dataset, WindowHistory())
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
                    now: datetime, changed: dict[str, int] | None = None,
                    why_not: str = "dry-run") -> dict[str, CheckResult]:
    """List the landing roots of every file-pattern feed in the plan and, on a real run (run given),
    register new and changed files in dq_file, counting them per feed in `changed`. Returns feed
    id -> DID_NOT_RUN result for feeds whose landing could not be listed or registered."""

    feeds = {p.feed.feed: p.feed for p in work if p.feed is not None and p.feed.landing is not None}
    problems: dict[str, CheckResult] = {}
    for feed_id, feed in feeds.items():
        try:
            files = list_landing(spark, feed.landing.roots, feed.landing.file_name_pattern)
        except LandingError as exc:
            problems[feed_id] = did_not_run(exc.code, detail=exc.detail)
            continue
        if run is None:
            print(f"{feed_id}: {len(files)} landed files listed ({why_not}: not registered)")
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


def upstreams_of(config: Config, ds: Dataset) -> dict[str, Dataset]:
    """The datasets a check on `ds` reads: each key_map upstream and, when one is derived, the
    datasets it is derived from."""

    found = {u: config.datasets[u] for u in ds.key_map if u in config.datasets}
    for up in list(found.values()):
        found.update(base_datasets(up, config.datasets))
    return found


def check_context(spark: SparkSession, config: Config, planned: Planned, event: Event,
                  landing_problems: dict[str, CheckResult] | None = None, key_secret: bytes | None = None,
                  budget_minutes: float | None = None, record_key_events: bool = True) -> CheckContext:
    ds = planned.dataset
    landing_problem = (landing_problems or {}).get(planned.feed.feed) if planned.feed else None
    return CheckContext(
        spark, ds, planned.feed, config.dataset_settings(ds.dataset), config.defaults.timezone,
        config.defaults.dq_database, landing_problem,
        upstreams=upstreams_of(config, ds),
        key_secret=key_secret,
        full_sweep=is_full_sweep(config, ds.dataset, event),
        budget_minutes=budget_minutes, scan_mb_per_minute=config.defaults.scan_mb_per_minute,
        record_key_events=record_key_events,
    )


def evaluate(spark: SparkSession, config: Config, planned: Planned, event: Event,
             landing_problems: dict[str, CheckResult] | None = None, key_secret: bytes | None = None,
             budget_minutes: float | None = None, record_key_events: bool = True) -> Evaluation:
    ctx = check_context(spark, config, planned, event, landing_problems, key_secret, budget_minutes,
                        record_key_events)
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


def incomplete_rows(evaluation: Evaluation, rows: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
    """For results stored without their key events: a DID_NOT_RUN / PLATFORM / evidence_incomplete row
    under the same evaluation_id, newer than the result, so the current state says the evidence is
    incomplete and the window is held and evaluated again (spec section 4)."""
    marked = []
    for result, row in zip(evaluation.results, rows):
        if result.key_events is None:
            continue
        marked.append({**row, "state": "DID_NOT_RUN", "reason_category": "PLATFORM",
                       "reason_code": "evidence_incomplete", "population": None, "violations": None,
                       "observed": f"{row['state']} result stored; its key events were not",
                       "expected": "the result and its key events stored together", "evaluated_at": now,
                       "detail": "the window is held and evaluated again"})
    return marked


def write_key_events(db: str, evaluation: Evaluation, rows: list[dict[str, Any]], run: runs.Run) -> None:
    """Append each result's key events (hop checks) with the result's evaluation id."""

    for result, row in zip(evaluation.results, rows):
        if result.key_events is not None:
            hop = hop_label(result.group_values.get("upstream") or "", row["dataset"])
            append_key_events(result.key_events, db, run.run_id, row["evaluation_id"], row["dataset"],
                              row["check_id"], hop, row["evaluated_at"], run.run_date)


@dataclass
class Investigations:
    """A run's cause investigations, tracked apart from its checks: one per FAILED result with a
    cause pattern; done once its dq_cause_result rows are stored."""

    expected: int = 0
    done: int = 0


def explain_failures(spark: SparkSession, config: Config, evaluation: Evaluation, rows: list[dict[str, Any]],
                     run: runs.Run, tally: Investigations | None = None) -> list[str]:
    """Run the cause engine for each FAILED result, append its dq_cause_result rows, and return
    one line per resolved cause (codes and counts only, hard rule 6). A failed investigation
    never stops the run or changes the check: it is counted in `tally` as not done, reported by
    the watchdog and retried by the next NORMAL run (retry_investigations)."""
    from collections import Counter


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
        if tally is not None:
            tally.expected += 1
        try:
            cause_rows = explain(cx, result, row["evaluation_id"], run.run_id, run.run_date,
                                 datetime.now(timezone.utc))
            append_cause_results(spark, config.defaults.dq_database, cause_rows)
        except Exception as exc:  # causes never stop the run; the next run retries
            log.error("could not explain %s for %s: %s", p.check.check_id, p.dataset.dataset, error_detail(exc))
            lines.append(f"  cause investigation incomplete ({type(exc).__name__}); the next run retries it")
            continue
        if tally is not None:
            tally.done += 1
        counts = Counter(line.text() for line in summarise(cause_rows, ft).values())
        lines += [f"  {text} [{n} failing]" for text, n in sorted(counts.items())]
    return lines


def pending_investigations(spark: SparkSession, config: Config, since: date,
                           skip: set[str]) -> list[dict[str, Any]]:
    """FAILED NORMAL results since `since` that are still their check's (and group's) latest result
    and have no dq_cause_result rows: investigations a run could not finish. Both scans are bounded
    by run_date."""

    db = config.defaults.dq_database
    day = f"DATE'{since.isoformat()}'"
    rows = spark.sql(
        f"SELECT r.* FROM (SELECT d.*, ROW_NUMBER() OVER (PARTITION BY dataset, check_id, {GROUPS_SQL} "
        f"  ORDER BY window_end DESC, evaluated_at DESC, run_id DESC) AS rn "
        f"  FROM {db}.dq_check_result d WHERE run_date >= {day} AND execution_type = 'NORMAL') r "
        f"LEFT ANTI JOIN (SELECT DISTINCT evaluation_id FROM {db}.dq_cause_result WHERE run_date >= {day}) c "
        f"  ON c.evaluation_id = r.evaluation_id "
        f"WHERE r.rn = 1 AND r.state = 'FAILED'").collect()
    return [r.asDict() for r in rows if r.evaluation_id not in skip]


def retry_investigations(spark: SparkSession, config: Config, run: runs.Run, key_secret: bytes | None,
                         tally: Investigations, skip: set[str]) -> list[str]:
    """Explain again, against the data as it is now, the FAILED results whose investigation a run
    could not finish (pending_investigations). Each counts in this run's tally."""

    since = run.run_date - timedelta(days=config.defaults.investigation_retry_days)
    by_check = {(p.dataset.dataset, p.check.check_id, p.feed.feed if p.feed else None): p
                for p in plan(config, per_table=False)}
    lines = []
    for row in pending_investigations(spark, config, since, skip):
        planned = by_check.get((row["dataset"], row["check_id"], row["feed"]))
        if planned is None or failure_type(planned.feed.pattern if planned.feed else None,
                                           planned.check.check_id) is None:
            continue
        event = Event(row["dataset"], as_utc(row["window_start"]), as_utc(row["window_end"]))
        result = CheckResult("FAILED", row["population"], row["violations"], row["observed"], row["expected"],
                             group_values=dict(row["group_values"] or {}), detail=row["detail"])
        ctx = check_context(spark, config, planned, event, key_secret=key_secret, record_key_events=False)
        evaluation = Evaluation(planned, event, [result], datetime.now(timezone.utc), 0, ctx)
        explained = explain_failures(spark, config, evaluation, [row], run, tally)
        lines += [f"{row['dataset']} {row['check_id']}: cause investigation retried"] + explained
    return lines


def drop_judged_views(spark: SparkSession, evaluation: Evaluation) -> None:
    """Release the hop judgements HOP_KEY_CURRENCY kept for the cause engine."""
    for result in evaluation.results:
        if result.judged_view is not None:
            try:
                spark.catalog.dropTempView(result.judged_view)
            except Exception as exc:  # only memory is at stake
                log.warning("could not drop a temp view: %s", error_detail(exc))


def email_digests(spark: SparkSession, config: Config, run: runs.Run) -> list[str]:
    """Build this run's digests from the store and send them; never stops the run."""

    try:
        digests = digests_for_run(spark, config, run)
        kept = to_send(digests, config.defaults, run.started_at)
        skipped = [f"email for {d.owner}: {d.subject} (not sent: email.all_clear_digest is "
                   f"{config.defaults.email.all_clear_digest})" for d in digests if d not in kept]
        return deliver(from_digests(kept), config.defaults) + skipped
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


def load_config(conf: str, command: str) -> Config | None:
    """The configuration a command runs on, or None after telling the operator to run dre validate."""
    config, errors, _ = validate_conf(Path(conf))
    if errors or config.defaults is None:
        print(f"dre {command}: {conf} has {len(errors)} configuration error(s); run dre validate --conf {conf}")
        return None
    return config


Job = tuple[Planned, Event]
GapRow = tuple[Planned, Event, tuple[str, ...]]


def normal_jobs(spark: SparkSession, config: Config, planned_all: list[Planned], run_record: runs.Run,
                feed_id: str | None) -> tuple[list[Job], dict[str, CheckResult], list[GapRow]]:
    """NORMAL: each dataset's next eligible window, due feeds only, rules by frequency, and the
    WINDOW_GAP rows of holds given up."""
    gaps: dict[str, tuple[Event, tuple[str, ...]]] = {}
    events = events_for(spark, config, planned_all, run_record.started_at, gaps=gaps)
    changed: dict[str, int] = {}
    landing = refresh_landing(spark, config, planned_all, run_record, run_record.started_at, changed)
    checks = [p for p in planned_all if not is_rule(p)]
    due = due_feeds(config, checks, events, changed, forced={feed_id} if feed_id else set())
    work = once_per_table(only_due(checks, due), SCHEMA_DRIFT)  # a due dataset checks each table
    for skipped in sorted({p.feed.feed for p in checks if p.feed is not None} - due):
        print(f"{skipped}: nothing due (no slot deadline passed, no new files); not evaluated")
    gap_for = {p.dataset.dataset: p for p in work if p.dataset.dataset in gaps}
    gap_rows = [(p, *gaps[ds_id]) for ds_id, p in gap_for.items()]
    work += rules_due(spark, config, planned_all, run_record.started_at)
    return [(p, events[p.dataset.dataset]) for p in work], landing, gap_rows


def historical_jobs(spark: SparkSession, config: Config, planned_all: list[Planned], selection: Selection,
                    feed_id: str | None, now: datetime) -> tuple[list[Job], dict[str, CheckResult]]:
    """RERUN and REPLAY: the windowed checks over the named window, or every window of the range,
    per dataset; a feed is evaluated in a window only if something was due in it (or --feed)."""
    # Gold rules and T1_SCHEMA_DRIFT read the table as it is now, not a window: nothing to re-evaluate.
    checks = [p for p in planned_all if not is_rule(p) and p.check.check_id != SCHEMA_DRIFT]
    landing = refresh_landing(spark, config, checks, None, now, why_not=selection.execution_type)
    tz = config.defaults.timezone
    datasets = {p.dataset.dataset: p.dataset for p in checks}
    if selection.execution_type == "RERUN":
        windows = {d: [fixed_window(ds, selection.start, selection.end, tz)] for d, ds in datasets.items()}
    else:
        recorded = recorded_windows(spark, config.defaults.dq_database, list(datasets), selection.start, selection.end)
        windows = {d: replay_windows(ds, selection.start, selection.end, recorded[d], tz) for d, ds in datasets.items()}
    forced = {feed_id} if feed_id else set()
    jobs: list[Job] = []
    for ds_id, ds_windows in windows.items():
        mine = [p for p in checks if p.dataset.dataset == ds_id]
        for event in ds_windows:
            due = due_feeds(config, mine, {ds_id: event}, {}, forced)
            kept = [p for p in mine if p.feed is None or p.feed.feed in due]
            if not kept:
                print(f"{ds_id} [{event.window_start:%Y-%m-%d %H:%M}, {event.window_end:%Y-%m-%d %H:%M}): "
                      "nothing was due; not evaluated")
            jobs += [(p, event) for p in kept]
    return jobs, landing


def stored_outcomes(spark: SparkSession, db: str, event_ids: list[str]) -> dict[str, tuple[Any, ...]]:
    """evaluation_id -> the OUTCOME of its latest stored result row, for the given events."""
    ids = sorted(set(event_ids))
    if not ids:
        return {}
    if not all(_HEX.match(e) for e in ids):
        raise ValueError("unexpected event id")
    columns = ", ".join(OUTCOME)
    in_list = ", ".join(f"'{e}'" for e in ids)
    rows = spark.sql(
        f"SELECT evaluation_id, {columns} FROM (SELECT r.*, ROW_NUMBER() OVER (PARTITION BY evaluation_id "
        f"ORDER BY evaluated_at DESC, run_id DESC) AS rn FROM {db}.dq_check_result r "
        f"WHERE event_id IN ({in_list})) latest WHERE rn = 1").collect()
    return {r.evaluation_id: tuple(r[c] for c in OUTCOME) for r in rows}


def unchanged(rows: list[dict[str, Any]], stored: dict[str, tuple[Any, ...]]) -> bool:
    """Every row of the evaluation is already stored with the same outcome."""
    return bool(rows) and all(stored.get(r["evaluation_id"]) == tuple(r[c] for c in OUTCOME) for r in rows)


def _preflight(spark: SparkSession, conf: str, feed_id: str | None,
               selection: Selection) -> tuple[Config, bytes | None] | int:
    """Everything a run needs before it starts, or the exit code that says why it cannot: valid
    configuration, a known feed, a historical window that is historical, the store installed, the
    HMAC secret readable."""
    config = load_config(conf, "run")
    if config is None:
        return EXIT_CANNOT_START
    if feed_id is not None and feed_id not in config.feeds:
        print(f"dre run: unknown feed {feed_id!r}")
        return EXIT_CANNOT_START
    problem = selection.problem(datetime.now(timezone.utc))
    if problem is not None:
        print(f"dre run: {problem}")
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
    return config, key_secret


def write_evaluation(spark: SparkSession, db: str, evaluation: Evaluation, rows: list[dict[str, Any]],
                     run_record: runs.Run) -> tuple[list[dict[str, Any]], int]:
    """Store an evaluation's results, then its key events. Returns (the rows that are stored, 1 if
    the check counts as written). A check counts as written only once both are stored; a missing
    write makes the run PARTIAL, and a result stored without its key events gets a newer
    evidence_incomplete row. A stored result is explained either way (the caller's job)."""
    check_id, ds_id = evaluation.planned.check.check_id, evaluation.planned.dataset.dataset
    try:
        append_check_results(spark, db, rows)
    except Exception as exc:
        log.error("could not write %s for %s: %s", check_id, ds_id, error_detail(exc))
        return [], 0
    try:
        write_key_events(db, evaluation, rows, run_record)
    except Exception as exc:
        log.error("could not write the key events of %s for %s: %s", check_id, ds_id, error_detail(exc))
        try:  # say so explicitly; the run is PARTIAL either way, which also holds the window
            append_check_results(spark, db, incomplete_rows(evaluation, rows, datetime.now(timezone.utc)))
        except Exception as marker_exc:
            log.error("could not mark %s incomplete: %s", check_id, error_detail(marker_exc))
        return rows, 0
    return rows, 1


def run(spark: SparkSession, conf: str, feed_id: str | None = None, selection: Selection | None = None) -> int:
    """One run. NORMAL (scheduled hourly): only feeds with something due are evaluated; --feed
    forces one. RERUN and REPLAY: historical windows (module docstring)."""

    selection = selection or Selection()
    ready = _preflight(spark, conf, feed_id, selection)
    if isinstance(ready, int):
        return ready
    config, key_secret = ready
    db = config.defaults.dq_database
    kind = selection.execution_type
    normal = kind == "NORMAL"
    run_record = runs.start_run(spark, db, conf_dir=conf, execution_type=kind)
    written = expected = 0
    investigations = Investigations()
    explained: set[str] = set()  # evaluation ids this run already investigated
    try:
        planned_all = plan(config, feed_id, per_table=False)
        gap_rows: list[GapRow] = []
        if normal:
            jobs, landing, gap_rows = normal_jobs(spark, config, planned_all, run_record, feed_id)
            stored: dict[str, tuple[Any, ...]] = {}
        else:
            print(f"note: a {kind} reads the tables as they are now, not as they were in the window; rows "
                  "changed or purged since are not seen (a best-effort re-evaluation, not a snapshot)")
            jobs, landing = historical_jobs(spark, config, planned_all, selection, feed_id, run_record.started_at)
            stored = stored_outcomes(spark, db, [event.event_id for _, event in jobs])
        expected = len(jobs) + len(gap_rows)
        for planned, gap, held_by in gap_rows:
            append_check_results(spark, db, [gap_row(planned, gap, held_by, run_record, kind,
                                                     datetime.now(timezone.utc))])
            written += 1
            print(f"{planned.dataset.dataset} {GAP_CHECK_ID}: DID_NOT_RUN (DATA_UNAVAILABLE/window_gap)")
        spent: dict[tuple[str, str], float] = {}  # compute_budget_scope dataset: minutes per dataset window
        for planned, event in jobs:
            ds_id = planned.dataset.dataset
            budget = None
            if config.defaults.compute_budget_scope == "dataset":
                budget = config.dataset_settings(ds_id).compute_budget_minutes - spent.get((ds_id, event.event_id), 0.0)
            evaluation = evaluate(spark, config, planned, event, landing, key_secret, budget,
                                  record_key_events=normal)
            spent[(ds_id, event.event_id)] = spent.get((ds_id, event.event_id), 0.0) + evaluation.duration_ms / 60000
            rows = result_rows(evaluation, run_record, kind)
            if unchanged(rows, stored):  # RERUN, REPLAY: the same outcome is stored already
                written += 1
                print(f"{ds_id} {planned.check.check_id}: unchanged since its stored result; not written again")
                drop_judged_views(spark, evaluation)
                continue
            stored_rows, complete = write_evaluation(spark, db, evaluation, rows, run_record)
            written += complete
            rows = stored_rows
            explained.update(r["evaluation_id"] for r in rows)
            for line in describe(evaluation) + explain_failures(spark, config, evaluation, rows, run_record,
                                                                investigations):
                print(line)
            drop_judged_views(spark, evaluation)
        if normal:
            for line in retry_investigations(spark, config, run_record, key_secret, investigations, explained):
                print(line)
    except Exception as exc:
        log.error("run %s failed: %s", run_record.run_id, error_detail(exc))
        runs.finish_run(spark, db, run_record, expected, written, failed=True,
                        investigations=(investigations.expected, investigations.done))
        return EXIT_PARTIAL
    if normal:
        for line in email_digests(spark, config, run_record):
            print(line)
    else:
        print(f"email: not sent for a {kind} run (digests report scheduled runs)")
    status = runs.finish_run(spark, db, run_record, expected, written,
                             investigations=(investigations.expected, investigations.done))
    incomplete = investigations.expected - investigations.done
    note = f"; {incomplete} cause investigation(s) incomplete, retried next run" if incomplete else ""
    print(f"dre run: {status}, {written} of {expected} checks written{note} ({kind} run {run_record.run_id})")
    return EXIT_COMPLETED if status == runs.COMPLETED else EXIT_PARTIAL


def dry_run(spark: SparkSession, conf: str, feed_id: str) -> int:
    """Preconditions and checks for one feed, printed; nothing is written. Exit 0 unless it cannot start."""

    config = load_config(conf, "dry-run")
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
        evaluation = evaluate(spark, config, planned, events[planned.dataset.dataset], landing)
        for line in describe(evaluation):
            print(line)
        drop_judged_views(spark, evaluation)
    print(f"dre dry-run: {len(work)} checks evaluated, {len(problems)} SQL fragment problem(s), nothing written")
    return 0
