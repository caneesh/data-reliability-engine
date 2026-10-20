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
the dq_file registry. They evaluate the windowed checks only (gold rules, T1_SCHEMA_DRIFT and
T1_PARTITION_DRIFT read the table as it is now, not a window), never move or hold a NORMAL window, and send no email.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.causes.base import CauseContext
from hcsc.datalake.dre.causes.engine import explain, failure_type, summarise
from hcsc.datalake.dre.checks.budget import BudgetExceeded, within_budget
from hcsc.datalake.dre.checks.base import (
    Check, CheckContext, CheckResult, did_not_run, error_detail, from_exception, release_checkpoint, run_check,
    utc_literal,
)
from hcsc.datalake.dre.checks.cadence import slots, UnsupportedCalendar
from hcsc.datalake.dre.checks.derived import base_datasets
from hcsc.datalake.dre.checks.events import (
    evaluation_id, Event, fixed_window, GAP_CHECK_ID, recorded_windows, release_hold, replay_windows, window_for,
    window_histories, WindowHistory,
)
from hcsc.datalake.dre.checks.hop.common import hop_label
from hcsc.datalake.dre.checks.hop.key_currency import judged_view
from hcsc.datalake.dre.checks.hop.ledger import ledger_view
from hcsc.datalake.dre.checks.patterns import checks_for
from hcsc.datalake.dre.checks.rules.rule_check import RuleCheck
from hcsc.datalake.dre.checks.rules.schedule import due_moment, is_due, last_completed
from hcsc.datalake.dre.checks.rules.template import TEMPLATES
from hcsc.datalake.dre.checks.times import as_utc
from hcsc.datalake.dre.config.fragments import check_fragments
from hcsc.datalake.dre.config.loader import Config, ConfigError
from hcsc.datalake.dre.config.models import Dataset, Feed
from hcsc.datalake.dre.config.validate import validate_conf
from hcsc.datalake.dre.notify.digest import digests_for_run, GROUPS_SQL, to_send
from hcsc.datalake.dre.notify.sinks import deliver, from_digests
from hcsc.datalake.dre.sources.hdfs import LandingError, list_landing
from hcsc.datalake.dre.store import integrity, runs
from hcsc.datalake.dre.store.causes import append_cause_results
from hcsc.datalake.dre.store.commits import CAUSE, Commit, append_commits, mismatches, stored_commits, stored_counts
from hcsc.datalake.dre.store.files import register_files
from hcsc.datalake.dre.store.install import missing_objects
from hcsc.datalake.dre.store.names import validate_identifier
from hcsc.datalake.dre.store.key_events import append_key_events
from hcsc.datalake.dre.store.results import append_check_results

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


log = logging.getLogger(__name__)

SCHEMA_DRIFT = "T1_SCHEMA_DRIFT"  # runs once per physical table (spec section 6)
PARTITION_DRIFT = "T1_PARTITION_DRIFT"  # likewise; both read the table as it is now, never a window
TABLE_CHECKS = (SCHEMA_DRIFT, PARTITION_DRIFT)
# Once per physical table too, but over a window, so RERUN and REPLAY keep it (build step 5).
SOURCE_ATTRIBUTION = "T1_SOURCE_ATTRIBUTION"
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
    per-table check (TABLE_CHECKS), for a caller that drops feeds that are not due before choosing one per table."""
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
    return once_per_table_checks(work) if per_table else work


def is_rule(planned: Planned) -> bool:
    return isinstance(planned.check, RuleCheck)


def rules_due(spark: SparkSession, config: Config, planned: list[Planned], run_start: datetime) -> list[Planned]:
    """Gold rules due by their frequency (checks/rules/schedule.py), whether or not their feed is due."""

    rules = [p for p in planned if is_rule(p)]
    if not rules:
        return []
    done = last_completed(spark, config.defaults.dq_database, history_since(config, run_start))
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


def once_per_table_checks(work: list[Planned]) -> list[Planned]:
    """once_per_table for every check that reads the table as a whole (TABLE_CHECKS, and
    T1_SOURCE_ATTRIBUTION: one result per union target, whichever sibling dataset carries it)."""
    for check_id in (*TABLE_CHECKS, SOURCE_ATTRIBUTION):
        work = once_per_table(work, check_id)
    return work


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


def history_since(config: Config, run_start: datetime) -> date:
    """The first run_date the hourly store reads look at (defaults.history_lookback_days)."""
    return (as_utc(run_start) - timedelta(days=config.defaults.history_lookback_days)).date()


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
        histories = window_histories(spark, config.defaults.dq_database, datasets, history_since(config, run_start))
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
            files = list_landing(spark, feed.landing.roots, feed.landing.file_name_pattern, feed.landing.max_files)
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
    """The datasets a check on `ds` reads: each key_map upstream, each contract's upstream (a
    join_contribution contributor has no key_map entry) and, when one is derived, the datasets it is
    derived from."""

    found = {u: config.datasets[u] for u in [*ds.key_map, *ds.contracts] if u in config.datasets}
    for up in list(found.values()):
        found.update(base_datasets(up, config.datasets))
    return found


def check_context(spark: SparkSession, config: Config, planned: Planned, event: Event,
                  landing_problems: dict[str, CheckResult] | None = None, key_secret: bytes | None = None,
                  budget_minutes: float | None = None, record_key_events: bool = True,
                  force_full_sweep: bool = False) -> CheckContext:
    ds = planned.dataset
    landing_problem = (landing_problems or {}).get(planned.feed.feed) if planned.feed else None
    return CheckContext(
        spark, ds, planned.feed, config.dataset_settings(ds.dataset), config.defaults.timezone,
        config.defaults.dq_database, landing_problem,
        upstreams=upstreams_of(config, ds), datasets=config.datasets,
        key_secret=key_secret,
        full_sweep=force_full_sweep or is_full_sweep(config, ds.dataset, event),
        budget_minutes=budget_minutes, scan_mb_per_minute=config.defaults.scan_mb_per_minute,
        record_key_events=record_key_events, history_since=history_since(config, event.window_end),
    )


def evaluate(spark: SparkSession, config: Config, planned: Planned, event: Event,
             landing_problems: dict[str, CheckResult] | None = None, key_secret: bytes | None = None,
             budget_minutes: float | None = None, record_key_events: bool = True,
             force_full_sweep: bool = False) -> Evaluation:
    ctx = check_context(spark, config, planned, event, landing_problems, key_secret, budget_minutes,
                        record_key_events, force_full_sweep)
    results, duration_ms = run_check(planned.check, ctx, event)
    return Evaluation(planned, event, results, datetime.now(timezone.utc), duration_ms, ctx)


def covers(result: CheckResult) -> bool:
    """Whether a result judged its range (checks/hop/coverage.py): PASSED, FAILED, or nothing due.
    Any other outcome (an error, missing or insufficient evidence, a budget cut) covers nothing,
    so the next window judges the range again."""
    return result.state in ("PASSED", "FAILED") or result.reason_code == "empty_population"


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
            "detail": r.detail, "qualifications": ",".join(r.qualifications) or None,
            "coverage_from": r.coverage[0] if r.coverage is not None and covers(r) else None,
            "coverage_to": r.coverage[1] if r.coverage is not None and covers(r) else None,
            "run_date": run.run_date,
        }
        for r in evaluation.results
    ]


def incomplete_rows(evaluation: Evaluation, rows: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
    """For an evaluation whose key events were not all stored: a DID_NOT_RUN / PLATFORM /
    evidence_incomplete row for every one of its results, under the same evaluation_id and one
    shared time, newer than the results, so the current state says the evidence is incomplete and
    the window is held and evaluated again (spec section 4). Every row, not only the ones that
    carried key events: v_latest_result ranks a (dataset, check) by evaluated_at, so a marker for
    some groups only would hide the evaluation's other groups until the next run. An evaluation
    that carried no key events at all had nothing to lose: no marker."""
    if all(result.key_events is None and not result.partition_state and not result.reconciliation
           for result in evaluation.results):
        return []
    marked = []
    for row in rows:
        marked.append({**row, "state": "DID_NOT_RUN", "reason_category": "PLATFORM",
                       "reason_code": "evidence_incomplete", "population": None, "violations": None,
                       "observed": f"{row['state']} result not published: the evaluation's evidence rows (key events, partition states) were not all stored",
                       "expected": "the result and its evidence rows stored together", "evaluated_at": now,
                       "detail": "the window is held and evaluated again",
                       "qualifications": None, "coverage_from": None, "coverage_to": None})
    return marked


def write_key_events(spark: SparkSession, db: str, evaluation: Evaluation, rows: list[dict[str, Any]],
                     run: runs.Run) -> dict[str, tuple[int, int, int]]:
    """Append each result's evidence rows with the run id: key events (hop checks), partition
    states (T1_PARTITION_DRIFT) and ledger lines (HOP_LEDGER). Returns evaluation id -> (key events,
    partition states, ledger lines) written."""
    from hcsc.datalake.dre.store.partitions import record_state
    from hcsc.datalake.dre.store.reconciliation import append_ledger

    counts: dict[str, tuple[int, int, int]] = {}
    for result, row in zip(evaluation.results, rows):
        events = states = ledger = 0
        if result.key_events is not None:
            hop = hop_label(result.group_values.get("upstream") or "", row["dataset"])
            events = append_key_events(result.key_events, db, run.run_id, row["evaluation_id"], row["dataset"],
                                       row["check_id"], hop, row["evaluated_at"], run.run_date)
        if result.partition_state:
            states = record_state(spark, db, row["dataset"], evaluation.planned.dataset.table or "",
                                  list(result.partition_state), run.run_id, row["evaluated_at"], run.run_date)
        if result.reconciliation:
            ledger = append_ledger(spark, db, list(result.reconciliation), run.run_id, row["evaluation_id"],
                                   row["dataset"], row["check_id"], row["evaluated_at"], run.run_date)
        counts[row["evaluation_id"]] = (events, states, ledger)
    return counts


def commits_for(rows: list[dict[str, Any]], run: runs.Run,
                evidence: dict[str, tuple[int, int, int]] | None = None) -> list[Commit]:
    """One commit per stored result row (each row is its own evaluation id), vouching for the
    evidence counts written for it (none for a marker or a row without evidence)."""
    evidence = evidence or {}
    commits = []
    for r in rows:
        events, states, ledger = evidence.get(r["evaluation_id"], (0, 0, 0))
        commits.append(Commit(run.run_id, r["evaluation_id"], r["dataset"], r["check_id"], 1, events, states,
                              reconciliation_rows=ledger))
    return commits


# Units (evaluations, investigations) whose commit rows are appended together, unless
# defaults.commit_batch says otherwise (ADR-07, amended on approval: one append per batch, not a small
# file per evaluation). The run's remaining commits are appended at its end.
COMMIT_BATCH = 25


@dataclass(frozen=True)
class Unit:
    """What is published or withheld whole: an evaluation (every row and group it stored) or a
    cause investigation. `complete` counts in checks_written, `investigated` in investigations_done;
    `after` are dq_integrity rows recorded once the unit is published (REBASELINED)."""

    commits: list[Commit]
    complete: int = 0
    investigated: int = 0
    after: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class Publisher:
    """Collects stored units' commits and appends them in batches (ADR-07). Before a batch is
    written, the rows its commits vouch for are counted in the store, and a unit whose stored rows
    differ is withheld whole. A unit counts (checks_written, investigations_done) only once its
    commits are confirmed stored, so a commit that cannot be stored makes the run PARTIAL and holds
    its windows, like any other failed write. After a failed write the run reads the batch back
    (a write can fail after its file landed), appends what is missing, reads back again, and
    publishes only CONFIRMED units: absent, conflicting (recorded as integrity incidents) or
    unreadable ones fail closed. With `retention`, each commit records how long its evidence is kept."""

    spark: SparkSession
    db: str
    run: runs.Run
    batch: int = COMMIT_BATCH
    tally: Investigations | None = None
    retention: dict[str, int] | None = None
    pending: list[Unit] = field(default_factory=list)
    published: int = 0  # evaluations counted as written
    withheld: int = 0  # units held back by verification
    unconfirmed: int = 0  # units not confirmed after a failed commit write

    def add(self, commits: list[Commit], complete: int, investigated: int = 0,
            after: list[dict[str, Any]] | None = None) -> None:
        if self.retention is not None:
            commits = [c.with_lifetime(self.run.run_date, self.retention) for c in commits]
        unit = Unit(commits, complete, investigated, after or [])
        if not commits:  # an evaluation with no result rows (a grouped check with no groups): nothing to publish
            self._count([unit])
            return
        self.pending.append(unit)
        if len(self.pending) >= self.batch:
            self.flush()

    def _count(self, units: list[Unit]) -> None:
        self.published += sum(u.complete for u in units)
        if self.tally is not None:
            self.tally.done += sum(u.investigated for u in units)
        records = [r for u in units for r in u.after]
        if records:
            try:
                integrity.append_records(self.spark, self.db, records)
            except Exception as exc:  # the next run finds the baseline still compromised and re-establishes it
                log.error("could not record %d integrity row(s): %s", len(records), error_detail(exc))

    def _verified(self, units: list[Unit]) -> list[Unit]:
        """The units whose stored rows match what their commits declare; the others are withheld."""
        try:
            counts = stored_counts(self.spark, self.db, self.run.run_id, self.run.run_date,
                                   [c for u in units for c in u.commits])
        except Exception as exc:
            log.error("could not count the rows of %d unit(s) before their commits: %s", len(units), error_detail(exc))
            print(f"integrity: {len(units)} unit(s) withheld: their stored rows could not be counted "
                  f"({type(exc).__name__}); their windows are evaluated again")
            self.withheld += len(units)
            return []
        kept = []
        for unit in units:
            problems = [p for c in unit.commits for p in mismatches(c, counts)]
            if not problems:
                kept.append(unit)
                continue
            first = unit.commits[0]
            log.error("integrity: %s %s (%s) withheld: %s", first.dataset, first.check_id, first.kind, "; ".join(problems))
            print(f"integrity: {first.dataset} {first.check_id} ({first.kind}) withheld: its stored rows differ "
                  f"from what its commits declare ({'; '.join(problems)}); it is evaluated again")
            self.withheld += 1
        return kept

    def _readback(self) -> dict[tuple[str, str, str], set[tuple]] | None:
        try:
            return stored_commits(self.spark, self.db, self.run.run_id, self.run.run_date)
        except Exception as exc:
            log.error("could not read the commit batch back: %s", error_detail(exc))
            return None

    def _repaired(self, units: list[Unit], commits: list[Commit]) -> list[Unit]:
        """After a failed write: read back, append what is missing once, read back again, and return
        the CONFIRMED units only (every commit stored with exactly its own contents)."""
        stored = self._readback()
        if stored is not None:
            missing = [c for c in commits if c.identity not in stored]
            if missing:
                try:
                    append_commits(self.spark, self.db, missing, datetime.now(timezone.utc), self.run.run_date)
                except Exception as exc:  # the readback below says what is stored
                    log.error("could not append the missing commit rows: %s", error_detail(exc))
                stored = self._readback()
        if stored is None:
            print(f"commit write failed and could not be read back: {len(units)} unit(s) not confirmed "
                  "(uncertain, not published); their windows are evaluated again")
            self.unconfirmed += len(units)
            return []
        day = self.run.run_date
        confirmed, absent, conflicting = [], 0, []
        for unit in units:
            states = [("absent" if c.identity not in stored
                       else "confirmed" if stored[c.identity] == {c.contents(day)} else "conflicting")
                      for c in unit.commits]
            if all(s == "confirmed" for s in states):
                confirmed.append(unit)
            elif "conflicting" in states:
                conflicting += [c for c, s in zip(unit.commits, states) if s == "conflicting"]
            else:
                absent += 1
        if conflicting:
            try:
                integrity.append_records(self.spark, self.db,
                                         [integrity.conflict_record(self.run, c, stored[c.identity]) for c in conflicting])
            except Exception as exc:  # the views mark a conflicting identity INVALID by themselves
                log.error("could not record %d commit conflict(s): %s", len(conflicting), error_detail(exc))
        not_confirmed = len(units) - len(confirmed)
        self.unconfirmed += not_confirmed
        print(f"commit write failed; after reading back: {len(confirmed)} unit(s) confirmed, {absent} absent, "
              f"{len(conflicting)} conflicting commit(s) (recorded as integrity incidents)"
              + (f"; {not_confirmed} unit(s) not published, their windows are evaluated again" if not_confirmed else ""))
        return confirmed

    def flush(self) -> None:
        if not self.pending:
            return
        units, self.pending = self._verified(self.pending), []
        commits = [c for u in units for c in u.commits]
        if not commits:
            return
        try:
            append_commits(self.spark, self.db, commits, datetime.now(timezone.utc), self.run.run_date)
        except Exception as exc:
            log.error("could not write %d commit rows: %s", len(commits), error_detail(exc))
            units = self._repaired(units, commits)
        self._count(units)


@dataclass
class Investigations:
    """A run's cause investigations, tracked apart from its checks: one per FAILED result with a
    cause pattern; done once its dq_cause_result rows are stored."""

    expected: int = 0
    done: int = 0


def explain_failures(spark: SparkSession, config: Config, evaluation: Evaluation, rows: list[dict[str, Any]],
                     run: runs.Run, tally: Investigations | None = None,
                     budget_minutes: float | None = None, publisher: Publisher | None = None) -> list[str]:
    """Run the cause engine for each stored FAILED result row, append its dq_cause_result rows, and
    return one line per resolved cause (codes and counts only, hard rule 6). The rows are published
    by a cause commit naming the result's run (ADR-07): through `publisher`, which counts the
    investigation as done once the commit is stored, or, without one, appended at once. A failed
    investigation never stops the run or changes the check: it is counted in `tally` as not done,
    reported by the watchdog and retried by the next NORMAL run (retry_investigations). With a
    budget (the dataset's compute budget, less what its checks spent), an investigation that runs
    past it is cancelled and counted the same way; a spent budget skips it. An evidence_incomplete
    marker stored in place of a FAILED result is not investigated: no FAILED result was published."""
    from collections import Counter

    p = evaluation.planned
    ft = failure_type(p.feed.pattern if p.feed else None, p.check.check_id)
    if ft is None or evaluation.ctx is None:
        return []
    lines = []
    db = config.defaults.dq_database
    for result, row in zip(evaluation.results, rows):
        if result.state != "FAILED" or row["state"] != "FAILED":
            continue
        upstream = result.group_values.get("upstream") if p.check.check_id.startswith("HOP_") else None
        cx = CauseContext(evaluation.ctx, evaluation.event, config, p.check.check_id, upstream)
        if tally is not None:
            tally.expected += 1
        if budget_minutes is not None and budget_minutes <= 0:
            lines.append("  cause investigation skipped: the dataset's compute budget is spent; the next run retries it")
            continue

        def investigate() -> list[dict[str, Any]]:
            found = explain(cx, result, row["evaluation_id"], run.run_id, run.run_date, datetime.now(timezone.utc))
            append_cause_results(spark, db, found)
            return found
        try:
            cause_rows = within_budget(spark, budget_minutes, investigate) if budget_minutes is not None else investigate()
        except BudgetExceeded as exc:
            lines.append(f"  cause investigation exceeded the compute budget ({exc.minutes:g} minutes); "
                         "the next run retries it")
            continue
        except Exception as exc:  # causes never stop the run; the next run retries
            log.error("could not explain %s for %s: %s", p.check.check_id, p.dataset.dataset, error_detail(exc))
            lines.append(f"  cause investigation incomplete ({type(exc).__name__}); the next run retries it")
            continue
        # The commit is written here, in the run's thread, never inside the cancellable investigation:
        # an investigation cancelled by its budget keeps running in the background and may still store
        # rows, but it can never publish them.
        if publisher is not None:  # done once the cause commit is stored
            publisher.add(cause_commits(cause_rows, row, run), 0, investigated=1)
        else:
            try:
                append_commits(spark, db, cause_commits(cause_rows, row, run), datetime.now(timezone.utc), run.run_date)
            except Exception as exc:
                log.error("could not publish the causes of %s for %s: %s", p.check.check_id, p.dataset.dataset,
                          error_detail(exc))
                lines.append(f"  cause investigation incomplete ({type(exc).__name__}); the next run retries it")
                continue
            if tally is not None:
                tally.done += 1
        counts = Counter(line.text() for line in summarise(cause_rows, ft).values())
        lines += [f"  {text} [{n} failing]" for text, n in sorted(counts.items())]
    return lines


def cause_commits(cause_rows: list[dict[str, Any]], row: dict[str, Any], run: runs.Run) -> list[Commit]:
    """The commit that publishes one investigation's cause rows, naming the committed result they
    explain by its run, run_date and evaluation id (this run's, or the original run's for a retry).
    None without cause rows."""
    if not cause_rows:
        return []
    return [Commit(run.run_id, row["evaluation_id"], row["dataset"], row["check_id"], 0, kind=CAUSE,
                   result_run_id=row["run_id"], result_run_date=row["run_date"], cause_rows=len(cause_rows))]


def pending_investigations(spark: SparkSession, config: Config, since: date,
                           skip: set[str]) -> list[dict[str, Any]]:
    """FAILED NORMAL results since `since` that are still their check's (and group's) latest result
    and have no committed cause naming that result (its run and evaluation id): investigations a
    run could not finish. A check with no failure type in its pattern (HOP_LEDGER, for one) is never
    investigated, so its results are not pending. The result and cause scans are bounded by run_date."""

    db = config.defaults.dq_database
    day = f"DATE'{since.isoformat()}'"
    rows = spark.sql(
        f"SELECT r.* FROM (SELECT d.*, ROW_NUMBER() OVER (PARTITION BY dataset, check_id, {GROUPS_SQL} "
        f"  ORDER BY window_end DESC, evaluated_at DESC, run_id DESC) AS rn "
        f"  FROM {db}.v_committed_result d WHERE run_date >= {day} AND execution_type = 'NORMAL') r "
        f"LEFT ANTI JOIN (SELECT DISTINCT evaluation_id, result_run_id FROM {db}.v_committed_cause "
        f"  WHERE run_date >= {day}) c "
        f"  ON c.evaluation_id = r.evaluation_id AND c.result_run_id = r.run_id "
        f"WHERE r.rn = 1 AND r.state = 'FAILED'").collect()
    investigated = {(p.dataset.dataset, p.check.check_id, p.feed.feed if p.feed else None)
                    for p in plan(config, per_table=False)
                    if failure_type(p.feed.pattern if p.feed else None, p.check.check_id) is not None}
    return [r.asDict() for r in rows
            if r.evaluation_id not in skip and (r.dataset, r.check_id, r.feed) in investigated]


def retry_investigations(spark: SparkSession, config: Config, run: runs.Run, key_secret: bytes | None,
                         tally: Investigations, skip: set[str], publisher: Publisher | None = None) -> list[str]:
    """Explain again, against the data as it is now, the FAILED results whose investigation a run
    could not finish (pending_investigations). Each counts in this run's tally."""

    # Today's pending investigations every run (a store that was down for an hour); older ones once
    # a day, in the hour from rule_run_at, so a failure that repeats is not retried hourly for a week.
    d = config.defaults
    since = run.run_date - timedelta(days=d.investigation_retry_days)
    daily = due_moment("daily", run.started_at, d.rule_run_at, d.timezone, "MONDAY")
    retry_older = daily is not None and daily <= as_utc(run.started_at) < daily + timedelta(hours=1)
    if not retry_older:
        since = run.run_date
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
        ctx = replace(ctx, judge_open_keys=True)  # the failing judgement included open keys: list them again
        evaluation = Evaluation(planned, event, [result], datetime.now(timezone.utc), 0, ctx)
        explained = explain_failures(spark, config, evaluation, [row], run, tally,
                                     config.dataset_settings(row["dataset"]).compute_budget_minutes, publisher)
        lines += [f"{row['dataset']} {row['check_id']}: cause investigation retried"] + explained
    return lines


def drop_judged_views(spark: SparkSession, evaluation: Evaluation) -> None:
    """Release the hop judgements HOP_KEY_CURRENCY kept for the cause engine, and HOP_LEDGER's
    assessed records: the checkpointed frames' blocks and the temp views. Every hop's view is
    dropped, named from the key_map, so a hop cancelled by the budget after creating its view (its
    result carries no view) is released too."""
    views = {r.judged_view for r in evaluation.results if r.judged_view is not None}
    named = {"HOP_KEY_CURRENCY": judged_view, "HOP_LEDGER": ledger_view}.get(evaluation.planned.check.check_id)
    if named is not None:
        views |= {named(evaluation.planned.dataset.dataset, up_id) for up_id in evaluation.planned.dataset.key_map}
    for result in evaluation.results:
        for frame in result.checkpoints:
            release_checkpoint(frame)
    for view in sorted(views):
        try:
            spark.catalog.dropTempView(view)
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


CONFIG_GROUPS = ("upstream",)  # group keys whose values are configuration ids, not data


def describe(evaluation: Evaluation) -> list[str]:
    """Counts, check ids and table names only (hard rule 6): a group_by value is data, so a grouped
    result is named by its column and its place in the evaluation, never by the value (the value
    is in the dq store and in the email). The hop's upstream is a dataset id and is printed."""
    p = evaluation.planned
    lines = []
    for n, r in enumerate(evaluation.results, 1):
        groups = ",".join(f"{k}={v}" if k in CONFIG_GROUPS else f"{k} #{n}" for k, v in sorted(r.group_values.items()))
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
    work = once_per_table_checks(only_due(checks, due))  # a due dataset checks each table
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
    # Gold rules and the per-table checks read the table as it is now, not a window: nothing to re-evaluate.
    checks = [p for p in planned_all if not is_rule(p) and p.check.check_id not in TABLE_CHECKS]
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


def without_pass(evaluation: Evaluation) -> Evaluation:
    """T1_PARTITION_DRIFT on a compromised baseline (R1): a PASSED result cannot be trusted (a
    partition first recorded by the lost evidence now looks new), so it becomes DID_NOT_RUN /
    PLATFORM / evidence_invalid, which holds the window. FAILED stands: the drift it found against
    the older baseline is real. The partition states are still stored, and become the baseline."""
    from hcsc.datalake.dre.checks.base import DID_NOT_RUN, PASSED

    return replace(evaluation, results=[
        replace(r, state=DID_NOT_RUN, reason_category="PLATFORM", reason_code="evidence_invalid",
                population=None, violations=None,
                observed="not judged: the baseline partition states were found compromised after publication",
                expected="a baseline whose evidence is intact")
        if r.state == PASSED else r for r in evaluation.results])


def rebaselines(run: runs.Run, evaluation: Evaluation, tainted: list[tuple[str, date, str]] | None,
                complete: int) -> list[dict[str, Any]]:
    """The REBASELINED rows to record once this evaluation is published: it re-established the
    baseline of the INVALID units in `tainted` if it was stored whole and either judged every key
    (HOP_KEY_CURRENCY, a full sweep, every group PASSED or FAILED) or stored new partition states
    (T1_PARTITION_DRIFT)."""
    if not tainted or not complete:
        return []
    check_id, results = evaluation.planned.check.check_id, evaluation.results
    if check_id == "HOP_KEY_CURRENCY":
        established = (evaluation.ctx is not None and evaluation.ctx.full_sweep
                       and all(r.state in ("PASSED", "FAILED") for r in results))
    else:
        established = any(r.partition_state for r in results)
    return integrity.rebaseline_records(run, evaluation.planned.dataset.dataset, check_id, tainted) if established else []


def record_unbalanced(spark: SparkSession, db: str, run: runs.Run, rows: list[dict[str, Any]]) -> None:
    """Record a LEDGER_UNBALANCED incident for each stored result whose ledger did not balance (L5),
    when it is stored: an engine defect is an observation, kept whether or not its unit is published.
    Never stops the run."""
    found = integrity.unbalanced_records(run, rows)
    if not found:
        return
    try:
        integrity.append_records(spark, db, found)
    except Exception as exc:
        log.warning("could not record an unbalanced ledger: %s", error_detail(exc))


def integrity_sweep(spark: SparkSession, config: Config, run: runs.Run) -> list[str]:
    """The integrity sweep, when due (R1): FULL (every commit whose evidence is still kept) at the
    first run at or after rule_run_at on integrity_full_sweep_day, otherwise DAILY (the last
    integrity_sweep_days days) at the first run at or after rule_run_at each day; a failed sweep is
    due again at the next run. Never stops the run: a failure is recorded as a FAILED sweep."""
    from hcsc.datalake.dre.store.commits import add_months

    d = config.defaults
    db = d.dq_database
    try:
        latest = {r.scope: as_utc(r.at) for r in spark.sql(
            f"SELECT scope, MAX(recorded_at) AS at FROM {db}.dq_integrity "
            f"WHERE record = '{integrity.SWEEP}' AND status = '{integrity.COMPLETED}' GROUP BY scope").collect()}
    except Exception as exc:
        log.error("could not read the integrity sweeps: %s", error_detail(exc))
        return [f"integrity sweep: not run ({type(exc).__name__}); the next run tries again"]
    done_since = lambda moment: any(at >= moment for at in latest.values())  # noqa: E731
    today = run.run_date
    scope = None
    if d.integrity_full_sweep_day is not None:
        weekly = due_moment("weekly", run.started_at, d.rule_run_at, d.timezone, d.integrity_full_sweep_day)
        if weekly is not None and latest.get(integrity.FULL, datetime.min.replace(tzinfo=timezone.utc)) < weekly:
            scope, start = integrity.FULL, add_months(today, -max(d.retention_months.values()))
    if scope is None:
        daily = due_moment("daily", run.started_at, d.rule_run_at, d.timezone, "MONDAY")
        if daily is None or done_since(daily):
            return []
        scope, start = integrity.DAILY, today - timedelta(days=d.integrity_sweep_days)
    try:
        outcome = integrity.sweep(spark, db, run, scope, start, today, today)
    except Exception as exc:
        log.error("integrity sweep failed: %s", error_detail(exc))
        return [f"integrity sweep ({scope.lower()}): FAILED ({type(exc).__name__}); what it covers is UNKNOWN "
                "until a sweep completes; the next run tries again"]
    return [f"integrity sweep ({scope.lower()}, run_date {start.isoformat()} to {today.isoformat()}): "
            f"{outcome.mismatches} new evidence mismatch(es), {outcome.conflicts} new commit conflict(s)"]


def stored_outcomes(spark: SparkSession, db: str, jobs: list[Job]) -> dict[str, tuple[Any, ...]]:
    """evaluation_id -> the OUTCOME of its latest stored result row, for the jobs' events. Read by
    dataset and the span the events cover (one predicate however many events), then kept for the
    jobs' event ids in Python. A latest row whose evidence is INVALID is not "stored": a RERUN of its
    window writes and publishes the evaluation again, which is how such a result is repaired."""
    if not jobs:
        return {}
    ids = {event.event_id for _, event in jobs}
    if not all(_HEX.match(e) for e in ids):
        raise ValueError("unexpected event id")
    datasets = ", ".join(f"'{validate_identifier(d, 'dataset id')}'" for d in sorted({p.dataset.dataset for p, _ in jobs}))
    lo = utc_literal(min(event.window_start for _, event in jobs))
    hi = utc_literal(max(event.window_end for _, event in jobs))
    columns = ", ".join(OUTCOME)
    rows = spark.sql(
        f"SELECT evaluation_id, event_id, {columns} FROM (SELECT r.*, ROW_NUMBER() OVER (PARTITION BY evaluation_id "
        f"ORDER BY evaluated_at DESC, run_id DESC) AS rn FROM {db}.v_committed_result r "
        f"WHERE dataset IN ({datasets}) AND window_start >= {lo} AND window_end <= {hi}) latest "
        f"WHERE rn = 1 AND evidence_state <> 'INVALID'").collect()
    return {r.evaluation_id: tuple(r[c] for c in OUTCOME) for r in rows if r.event_id in ids}


def unchanged(rows: list[dict[str, Any]], stored: dict[str, tuple[Any, ...]]) -> bool:
    """Every row of the evaluation is already stored with the same outcome."""
    return bool(rows) and all(stored.get(r["evaluation_id"]) == tuple(r[c] for c in OUTCOME) for r in rows)


def _preflight(spark: SparkSession, conf: str, feed_id: str | None,
               selection: Selection) -> tuple[Config, bytes | None] | int:
    """Everything a run needs before it starts, or the exit code that says why it cannot: valid
    configuration, a known feed, a historical window that is historical, the store installed, no
    other NORMAL run in progress, the HMAC secret readable."""
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
    if selection.execution_type == "NORMAL":
        grace = config.defaults.watchdog_grace_minutes
        busy = runs.in_progress(spark, db, datetime.now(timezone.utc), grace)
        if busy is not None:  # two NORMAL runs would evaluate the same windows and store everything twice
            run_id, started = busy
            print(f"dre run: a NORMAL run is in progress (run {run_id}, started {started:%H:%M} UTC); "
                  f"a run still STARTED after {grace} minutes counts as dead and no longer blocks")
            return EXIT_CANNOT_START
    try:
        key_secret = read_key_secret(config)
    except OSError as exc:
        print(f"dre run: cannot read hmac_secret_file ({type(exc).__name__}); key events need it")
        return EXIT_CANNOT_START
    return config, key_secret


def write_evaluation(spark: SparkSession, db: str, evaluation: Evaluation, rows: list[dict[str, Any]],
                     run_record: runs.Run, budget_minutes: float | None = None
                     ) -> tuple[list[dict[str, Any]], int, list[Commit]]:
    """Store an evaluation's evidence (key events, partition states), then its results (ADR-07).
    Returns (the rows stored, 1 if the check counts as written once committed, the commits that
    publish them). Nothing is visible until the caller stores the commits. If the evidence cannot
    be stored, the results are not written; an evidence_incomplete marker is, and its commits vouch
    for no evidence, so the partial evidence stays invisible; the check does not count as written,
    which makes the run PARTIAL and holds the window. If the results cannot be stored, the marker
    is tried the same way. The evidence write runs under budget_minutes (WRITE_BUDGET_MINUTES in a
    run): one that runs past it takes the marker path like any failed write."""
    check_id, ds_id = evaluation.planned.check.check_id, evaluation.planned.dataset.dataset

    def marked(what: str, exc: Exception) -> tuple[list[dict[str, Any]], int, list[Commit]]:
        log.error("could not write the %s of %s for %s: %s", what, check_id, ds_id, error_detail(exc))
        marker = incomplete_rows(evaluation, rows, datetime.now(timezone.utc))
        try:  # say so explicitly; the run is PARTIAL either way, which also holds the window
            append_check_results(spark, db, marker)
        except Exception as marker_exc:
            log.error("could not mark %s incomplete: %s", check_id, error_detail(marker_exc))
            return [], 0, []
        return marker, 0, commits_for(marker, run_record)

    carries = any(r.key_events is not None or r.partition_state or r.reconciliation for r in evaluation.results)
    try:
        if not carries:  # nothing to store before the results
            evidence: dict[str, tuple[int, int, int]] = {}
        elif budget_minutes is not None:
            evidence = within_budget(spark, budget_minutes,
                                     lambda: write_key_events(spark, db, evaluation, rows, run_record))
        else:
            evidence = write_key_events(spark, db, evaluation, rows, run_record)
    except Exception as exc:
        return marked("evidence rows", exc)
    try:
        append_check_results(spark, db, rows)
    except Exception as exc:
        return marked("results", exc)
    return rows, 1, commits_for(rows, run_record, evidence)


# How long a key-event write may take before it counts as failed (evidence_incomplete, the window
# held): a fixed bound, apart from the compute budget, which is about reading production tables.
WRITE_BUDGET_MINUTES = 15.0


def investigation_budget(config: Config, dataset_id: str, spent_minutes: float) -> float:
    """What a cause investigation may spend: the dataset's compute budget, less what its checks
    in this run already spent when compute_budget_scope is dataset."""
    budget = config.dataset_settings(dataset_id).compute_budget_minutes
    if config.defaults.compute_budget_scope == "dataset":
        return budget - spent_minutes
    return budget


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
    print(f"dre run: started {run_record.run_id} ({kind} run, {run_record.started_at:%Y-%m-%d %H:%M} UTC)")
    written = expected = 0
    investigations = Investigations()
    publisher = Publisher(spark, db, run_record, config.defaults.commit_batch, investigations,
                          dict(config.defaults.retention_months))
    # Checks whose baseline rests on evidence found INVALID after it was published (R1): the hop's
    # open keys are judged by a full sweep instead, and partition drift cannot pass, until a later
    # evaluation re-establishes the baseline (REBASELINED). Unreadable: every such check counts.
    compromised = integrity.compromised_baselines(spark, db)
    if compromised.unknown:
        print("integrity: compromised baselines could not be read; hop checks run as full sweeps and "
              "partition drift cannot pass this run")
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
            stored = stored_outcomes(spark, db, jobs)
        expected = len(jobs) + len(gap_rows)
        for planned, gap, held_by in gap_rows:
            gap_rows_stored = [gap_row(planned, gap, held_by, run_record, kind, datetime.now(timezone.utc))]
            append_check_results(spark, db, gap_rows_stored)
            publisher.add(commits_for(gap_rows_stored, run_record), 1)
            print(f"{planned.dataset.dataset} {GAP_CHECK_ID}: DID_NOT_RUN (DATA_UNAVAILABLE/window_gap)")
        spent: dict[tuple[str, str], float] = {}  # compute_budget_scope dataset: minutes per dataset window
        for planned, event in jobs:
            ds_id = planned.dataset.dataset
            budget = None
            if config.defaults.compute_budget_scope == "dataset":
                budget = config.dataset_settings(ds_id).compute_budget_minutes - spent.get((ds_id, event.event_id), 0.0)
            check_id = planned.check.check_id
            tainted = compromised.of(ds_id, check_id)
            evaluation = evaluate(spark, config, planned, event, landing, key_secret, budget,
                                  record_key_events=normal,
                                  force_full_sweep=tainted is not None and check_id == "HOP_KEY_CURRENCY")
            if tainted is not None and check_id == "T1_PARTITION_DRIFT":
                evaluation = without_pass(evaluation)
            spent[(ds_id, event.event_id)] = spent.get((ds_id, event.event_id), 0.0) + evaluation.duration_ms / 60000
            remaining = investigation_budget(config, ds_id, spent.get((ds_id, event.event_id), 0.0))
            rows = result_rows(evaluation, run_record, kind)
            if unchanged(rows, stored):  # RERUN, REPLAY: the same outcome is stored already
                written += 1
                print(f"{ds_id} {planned.check.check_id}: unchanged since its stored result; not written again")
                drop_judged_views(spark, evaluation)
                continue
            stored_rows, complete, commits = write_evaluation(spark, db, evaluation, rows, run_record,
                                                              WRITE_BUDGET_MINUTES)
            publisher.add(commits, complete, after=rebaselines(run_record, evaluation, tainted, complete))
            record_unbalanced(spark, db, run_record, stored_rows)
            rows = stored_rows
            explained.update(r["evaluation_id"] for r in rows)
            for line in describe(evaluation) + explain_failures(spark, config, evaluation, rows, run_record,
                                                                investigations, remaining, publisher):
                print(line)
            drop_judged_views(spark, evaluation)
        publisher.flush()  # the run's remaining commits, before anything reads this run's results
        if normal:
            try:
                for line in retry_investigations(spark, config, run_record, key_secret, investigations, explained,
                                                 publisher):
                    print(line)
            except Exception as exc:  # every check is written by now: the retries are not worth a FAILED run
                log.error("could not retry pending investigations: %s", error_detail(exc))
                print(f"pending cause investigations not retried ({type(exc).__name__}); the next run retries them")
            publisher.flush()  # the retried investigations' commits, before the digest reads this run's causes
            for line in integrity_sweep(spark, config, run_record):
                print(line)
    except Exception as exc:
        log.error("run %s failed: %s", run_record.run_id, error_detail(exc))
        publisher.flush()  # what was stored completely before the failure is published; nothing else is
        written += publisher.published
        print(f"dre run: FAILED, {written} of {expected} checks written ({kind} run {run_record.run_id}): "
              f"{error_detail(exc)}")
        try:
            runs.finish_run(spark, db, run_record, expected, written, failed=True,
                            investigations=(investigations.expected, investigations.done))
        except Exception as final_exc:  # the run stays STARTED, which the watchdog reports
            log.error("could not write the final row of run %s: %s", run_record.run_id, error_detail(final_exc))
        return EXIT_PARTIAL
    written += publisher.published
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
