"""Operational isolation of a run (ADR-08, build step 8): order, run budget, kill switch, queue, backpressure.

- **Priority.** A NORMAL run evaluates the on-time and completeness checks first (priority 0:
  T1_ON_TIME, T1_ZERO_ROWS, T1_FILES_NOT_LOADED, HOP_FILE_COMPLETENESS, HOP_LEDGER, CHAIN_COMPLETENESS),
  then every other check (priority 1), keeping the plan's order within a class.
- **Run budget.** A NORMAL run's checks share `run_budget_minutes`, counted from the run's start on a
  monotonic clock. A check reached once it is spent is not run: its result is DID_NOT_RUN / BUDGET /
  run_budget_exceeded, written and committed like any result, so the window is held and evaluated by
  the next run (a check already running keeps its own compute budget).
- **Kill switch.** `kill_switch_file` (a deployment value; null: none), read at the start and before
  every check. Once seen, every remaining check is DID_NOT_RUN / PLATFORM / disabled for the rest of
  the run, which ends PARTIAL. A location that cannot be read counts as on (fail closed), and the run
  says so. No pipeline is touched either way.
- **Queue.** A run refuses to start when its YARN queue (`spark.yarn.queue`) is one of
  `protected_queues`, or when `yarn_queue` is set and the run is in another queue (or names none on
  YARN). Queue names come from configuration and the Spark conf only.
- **Backpressure.** When the previous finished NORMAL run took more than `backpressure_share` of the run
  budget, the weekly full sweep is deferred. A deferred sweep is never lost: before evaluating, the
  run records one dq_integrity row (SWEEP_DEFERRED) per dataset whose sweep it defers, and a dataset's
  sweep is due at every NORMAL window that starts at or before the end of the latest window whose
  sweep was deferred (that run's results for the dataset). The first run not under backpressure that
  evaluates the dataset performs it; once its window is evaluated and not held, the next window starts
  after the deferred one and the obligation is met (a held window starts earlier and sweeps again).
  If the deferral cannot be recorded, or the records cannot be read, the sweep is not deferred.
  Sweeps forced because a baseline was found compromised (integrity) are never deferred.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Callable

from hcsc.datalake.dre.checks.base import CheckResult, did_not_run, error_detail, utc_literal
from hcsc.datalake.dre.checks.times import as_utc
from hcsc.datalake.dre.store import integrity
from hcsc.datalake.dre.store.names import validate_dq_database, validate_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.loader import Config
    from hcsc.datalake.dre.store.runs import Run

log = logging.getLogger(__name__)

# Priority 0 (ADR-08: on-time and completeness first); every other check is priority 1.
FIRST = ("T1_ON_TIME", "T1_ZERO_ROWS", "T1_FILES_NOT_LOADED", "HOP_FILE_COMPLETENESS", "HOP_LEDGER",
         "CHAIN_COMPLETENESS")
SWEEP_DEFERRED = "SWEEP_DEFERRED"  # dq_integrity record: a run deferred this dataset's full sweep

clock: Callable[[], float] = time.monotonic  # seconds; the run budget's clock (a test replaces it)


def priority(planned: Any) -> int:
    """0 for the on-time and completeness checks, 1 for every other check."""
    return 0 if planned.check.check_id in FIRST else 1


def order(jobs: list[tuple[Any, Event]]) -> list[tuple[Any, Event]]:
    """(planned, event) jobs by priority; a stable sort, so the plan's order holds within a class."""
    return sorted(jobs, key=lambda job: priority(job[0]))


def kill_switch(config: Config) -> str | None:
    """Why the kill switch is on, or None when it is off or not configured. The file present: on. Its
    location cannot be read: on too (fail closed). Never names the path (a deployment value)."""
    path = config.defaults.kill_switch_file
    if path is None:
        return None
    try:
        os.stat(path)
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as exc:
        return f"the kill_switch_file location cannot be read ({type(exc).__name__}), so it counts as on (fail closed)"
    return "the kill_switch_file is present"


def kill_switch_on(config: Config) -> bool:
    return kill_switch(config) is not None


def submitted_queue(spark: SparkSession) -> tuple[str | None, bool]:
    """(the application's spark.yarn.queue, or None when not set; whether it runs on YARN)."""
    sc = spark.sparkContext
    return sc.getConf().get("spark.yarn.queue", None), str(sc.master).startswith("yarn")


def queue_refusal(spark: SparkSession, config: Config) -> str | None:
    """Why this application may not run in its YARN queue (ADR-08), or None. Only configured names are
    printed: the queue DRE should use (yarn_queue), never a protected one."""
    d = config.defaults
    queue, on_yarn = submitted_queue(spark)
    use = f"submit it with --queue {d.yarn_queue}" if d.yarn_queue else "submit it in DRE's own queue"
    if queue is not None and queue in d.protected_queues:
        return ("the application's YARN queue (spark.yarn.queue) is one of protected_queues, which belong to the "
                f"production pipelines: DRE never runs there (ADR-08); {use}")
    if d.yarn_queue is not None and queue != d.yarn_queue and (queue is not None or on_yarn):
        what = "names no queue" if queue is None else "is not yarn_queue"
        return f"the application's YARN queue (spark.yarn.queue) {what} (ADR-08); {use}"
    return None


@dataclass
class Halt:
    """What stops a run's checks: the kill switch (every run) and the run budget (budget_minutes;
    None: no run budget). check() is called before each check."""

    config: Config
    budget_minutes: float | None
    started: float = field(default_factory=lambda: clock())
    switched: str | None = None  # why the kill switch is on, once seen; it stays on for the run
    disabled: int = 0  # checks written DID_NOT_RUN / disabled
    cut: int = 0  # checks written DID_NOT_RUN / run_budget_exceeded

    def elapsed_minutes(self) -> float:
        return (clock() - self.started) / 60

    def switch(self) -> str | None:
        """Read the kill switch (until it has been seen on); why it is on, or None."""
        if self.switched is None:
            self.switched = kill_switch(self.config)
        return self.switched

    def spent(self) -> bool:
        return self.budget_minutes is not None and self.elapsed_minutes() >= self.budget_minutes

    def check(self) -> CheckResult | None:
        """The result to write instead of running the next check, or None to run it."""
        if self.switch() is not None:
            self.disabled += 1
            return did_not_run("disabled", detail=f"kill switch on: {self.switched}; not run, the window is held")
        if self.spent():
            self.cut += 1
            return did_not_run("run_budget_exceeded", detail=(
                f"run budget of {self.budget_minutes:g} minutes spent after {self.elapsed_minutes():.1f} minutes; "
                "not run, the window is held and evaluated next run"))
        return None

    def lines(self) -> list[str]:
        """What the run's output says about them (counts only)."""
        out = []
        if self.disabled:
            out.append(f"kill switch: on ({self.switched}); {self.disabled} check(s) written DID_NOT_RUN / "
                       "disabled, the run ends PARTIAL and their windows are held")
        if self.cut:
            out.append(f"run budget: {self.budget_minutes:g} minutes spent; {self.cut} check(s) reached after it "
                       "written DID_NOT_RUN / run_budget_exceeded, their windows are held and evaluated next run")
        return out


def previous_run_minutes(spark: SparkSession, db: str, now: datetime, since: date) -> float | None:
    """How long the latest NORMAL run that started before `now` and finished (COMPLETED, PARTIAL or
    FAILED) took, in minutes; None when there is none since `since` (run_date)."""
    validate_dq_database(db)
    rows = spark.sql(
        f"SELECT started_at, ended_at FROM {db}.v_latest_run "
        f"WHERE execution_type = 'NORMAL' AND status <> 'STARTED' AND ended_at IS NOT NULL "
        f"AND started_at < {utc_literal(now)} AND run_date >= DATE'{since.isoformat()}' "
        f"ORDER BY started_at DESC LIMIT 1").collect()
    if not rows:
        return None
    return (as_utc(rows[0].ended_at) - as_utc(rows[0].started_at)).total_seconds() / 60


def backpressure(spark: SparkSession, db: str, config: Config, now: datetime, since: date | None = None) -> float | None:
    """The previous NORMAL run's minutes when they exceed backpressure_share of the run budget, else None."""
    d = config.defaults
    since = since or (as_utc(now) - timedelta(days=d.history_lookback_days)).date()
    minutes = previous_run_minutes(spark, db, now, since)
    return minutes if minutes is not None and minutes > d.backpressure_share * d.run_budget_minutes else None


def defer_full_sweep(spark: SparkSession, db: str, config: Config, now: datetime) -> bool:
    """The previous NORMAL run used more than backpressure_share of the run budget."""
    return backpressure(spark, db, config, now) is not None


def deferred_ends(spark: SparkSession, db: str, datasets: list[str], since: date) -> dict[str, datetime]:
    """Dataset -> the end of the latest window whose full sweep a run deferred: SWEEP_DEFERRED records
    since `since`, joined to the deferring run's NORMAL results for the dataset."""
    validate_dq_database(db)
    if not datasets:
        return {}
    names = ", ".join(f"'{validate_identifier(d, 'dataset id')}'" for d in sorted(set(datasets)))
    day = f"DATE'{since.isoformat()}'"
    rows = spark.sql(
        f"SELECT i.dataset, MAX(r.window_end) AS deferred_end FROM {db}.dq_integrity i "
        f"JOIN {db}.dq_check_result r ON r.run_id = i.subject_run_id AND r.run_date = i.subject_run_date "
        f"  AND r.dataset = i.dataset "
        f"WHERE i.record = '{SWEEP_DEFERRED}' AND i.dataset IN ({names}) AND i.run_date >= {day} "
        f"  AND r.run_date >= {day} AND r.execution_type = 'NORMAL' AND r.severity IS NULL "
        f"GROUP BY i.dataset").collect()
    return {r.dataset: as_utc(r.deferred_end) for r in rows if r.deferred_end is not None}


@dataclass(frozen=True)
class Sweeps:
    """A NORMAL run's full sweeps beyond the calendar's: `forced`, datasets whose deferred sweep this
    run performs; `deferred`, datasets whose due sweep this run defers. `lines`: what the run says."""

    forced: frozenset[str] = frozenset()
    deferred: frozenset[str] = frozenset()
    lines: tuple[str, ...] = ()


def plan_sweeps(spark: SparkSession, config: Config, run: Run, jobs: list[tuple[Any, Event]],
                is_sweep: Callable[[str, Event], bool], since: date) -> Sweeps:
    """Which datasets this NORMAL run sweeps beyond the calendar (`forced`) or defers (`deferred`):
    a sweep is due on the calendar (is_sweep) or because an earlier one was deferred (deferred_ends);
    under backpressure every due sweep is deferred, once its deferral is recorded."""
    db = config.defaults.dq_database
    events = {p.dataset.dataset: e for p, e in jobs}
    if not events:
        return Sweeps()
    try:
        owed_until = deferred_ends(spark, db, list(events), since)
        owed = {d for d, e in events.items() if d in owed_until and as_utc(e.window_start) <= owed_until[d]}
        unread = False
    except Exception as exc:  # cannot tell which sweeps are owed: every dataset counts (fail closed)
        log.error("could not read the deferred full sweeps: %s", error_detail(exc))
        owed, unread = set(events), True
    due = owed | {d for d, e in events.items() if is_sweep(d, e)}
    note = (["full sweep: the deferred sweeps could not be read; every dataset in this run counts as owed one"]
            if unread else [])
    try:
        pressure = backpressure(spark, db, config, run.started_at, since)
    except Exception as exc:  # the sweep is evidence; deferring it is only a saving
        log.error("could not read the previous run's duration: %s", error_detail(exc))
        note.append(f"backpressure: the previous run's duration could not be read ({type(exc).__name__}); "
                    "no full sweep is deferred")
        pressure = None
    if pressure is None:
        lines = note + ([f"full sweep: {len(owed)} dataset(s) swept now for a sweep deferred earlier"] if owed else [])
        return Sweeps(forced=frozenset(owed), lines=tuple(lines))
    d = config.defaults
    why = (f"the previous NORMAL run took {pressure:.1f} minutes, over {d.backpressure_share:g} of the "
           f"{d.run_budget_minutes:g}-minute run budget")
    if not due:
        return Sweeps(lines=tuple(note + [f"backpressure: {why}; no full sweep is due in this run"]))
    records = [integrity.record(run, SWEEP_DEFERRED, subject_run_id=run.run_id, subject_run_date=run.run_date,
                                dataset=ds, detail=why) for ds in sorted(due)]
    try:
        integrity.append_records(spark, db, records)
    except Exception as exc:  # unrecorded, a deferral could be lost: sweep now instead
        log.error("could not record %d deferred full sweep(s): %s", len(records), error_detail(exc))
        return Sweeps(forced=frozenset(owed), lines=tuple(note + [
            f"backpressure: {why}; the full sweep of {len(due)} dataset(s) is NOT deferred: the deferral could not "
            f"be recorded ({type(exc).__name__}) and a deferred sweep must never be lost"]))
    return Sweeps(deferred=frozenset(due), lines=tuple(note + [
        f"backpressure: {why}; the full sweep of {len(due)} dataset(s) is deferred to the next NORMAL run "
        "not under backpressure"]))
