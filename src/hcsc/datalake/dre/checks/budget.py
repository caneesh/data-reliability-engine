"""Compute budget for one check (spec section 8).

A check gets its dataset's compute_budget_minutes. It runs in its own Spark job group; if it
has not finished when the budget runs out, the group is cancelled (running Spark jobs are
interrupted) and the check is DID_NOT_RUN / budget_exceeded. The table's estimated scan size,
from Spark's table statistics when the metastore has them, goes in the result's detail.
Python work inside a cancelled check cannot be interrupted; it is left to end on its own and
its result is ignored.

The concurrency limit (ADR-08; final review O6). Every unit of DRE work that issues Spark jobs (a
check, a cause investigation, an evidence write) runs through within_budget, in its own job group,
and GATE admits at most `max_concurrent_jobs` live units. A unit cancelled by its budget stays live,
and counts, until its thread has really stopped; while it lingers, its job group is cancelled again
every RECANCEL_SECONDS (Spark's cancelJobGroup stops only the jobs active at that moment, so a job
the unit starts afterwards would otherwise run). A unit that cannot be admitted within
`cancel_grace_seconds` is refused (ConcurrencyLimit: DID_NOT_RUN / BUDGET / concurrency_limit),
never run alongside. One Spark action may itself run more than one job (a broadcast); the limit is
on DRE's units, the YARN queue's share bounds the cluster.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Spark's spark.sql.defaultSizeInBytes: what the plan reports when it has no statistics.
_UNKNOWN_SIZE = 2**63 - 1


DEFAULT_GRACE_SECONDS = 60.0
RECANCEL_SECONDS = 0.5


class BudgetExceeded(Exception):
    def __init__(self, minutes: float) -> None:
        super().__init__(f"compute budget of {minutes:g} minutes exceeded")
        self.minutes = minutes


class ConcurrencyLimit(BudgetExceeded):
    """A unit refused because max_concurrent_jobs units were still live after the grace."""

    def __init__(self, live: int, limit: int, grace: float) -> None:
        Exception.__init__(self, f"concurrency limit of {limit} unit(s) reached: {live} unit(s) still running "
                                 f"after {grace:g}s of waiting (a cancelled unit counts until it stops); not run")
        self.minutes = 0.0


@dataclass
class _Unit:
    thread: threading.Thread
    group: str
    cancelled: bool = False


class Gate:
    """The live units of this process (one run), and the most that were ever live at once."""

    def __init__(self, max_units: int = 1, grace_seconds: float = DEFAULT_GRACE_SECONDS) -> None:
        self.max_units, self.grace_seconds, self.peak = max_units, grace_seconds, 0
        self._units: list[_Unit] = []
        self._lock = threading.Lock()
        self._reaper: threading.Thread | None = None
        self._spark: Any = None

    def _alive(self) -> list[_Unit]:
        with self._lock:
            self._units = [u for u in self._units if u.thread.is_alive()]
            return list(self._units)

    def live(self) -> int:
        return len(self._alive())

    def _recancel(self) -> None:
        for unit in self._alive():
            if unit.cancelled and self._spark is not None:
                self._spark.sparkContext.cancelJobGroup(unit.group)

    def admit(self, spark: SparkSession) -> None:
        """Return once fewer than max_units units are live, cancelling lingering units' jobs again
        while waiting; raise ConcurrencyLimit when the grace runs out first."""
        self._spark = spark
        deadline = time.monotonic() + self.grace_seconds
        while True:
            alive = self._alive()
            if len(alive) < self.max_units:
                return
            self._recancel()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ConcurrencyLimit(len(alive), self.max_units, self.grace_seconds)
            alive[0].thread.join(min(RECANCEL_SECONDS, remaining))

    def started(self, unit: _Unit) -> None:
        with self._lock:
            self._units.append(unit)
            self.peak = max(self.peak, sum(1 for u in self._units if u.thread.is_alive()))

    def cancel(self, spark: SparkSession, unit: _Unit) -> None:
        """Cancel the unit's jobs now, and keep cancelling any it starts until it stops."""
        self._spark = spark
        unit.cancelled = True
        spark.sparkContext.cancelJobGroup(unit.group)
        with self._lock:
            if self._reaper is not None and self._reaper.is_alive():
                return
            self._reaper = threading.Thread(target=self._reap, daemon=True, name="dre-reaper")
            self._reaper.start()

    def _reap(self) -> None:
        while any(u.cancelled for u in self._alive()):
            self._recancel()
            time.sleep(RECANCEL_SECONDS)

    def drain(self, seconds: float) -> int:
        """Wait up to `seconds` for the live units to stop (re-cancelling lingering ones); the number left."""
        deadline = time.monotonic() + seconds
        while (alive := self._alive()) and time.monotonic() < deadline:
            self._recancel()
            alive[0].thread.join(min(RECANCEL_SECONDS, max(deadline - time.monotonic(), 0)))
        return self.live()


GATE = Gate()


def configure(max_units: int, grace_seconds: float) -> None:
    """Set the limit for this run (defaults.max_concurrent_jobs, cancel_grace_seconds) and reset the peak."""
    GATE.max_units, GATE.grace_seconds, GATE.peak = max_units, grace_seconds, 0


def estimated_scan_bytes(spark: SparkSession, table: str) -> int | None:
    """The table's size from Spark's statistics, or None when there are none."""
    try:
        size = int(str(spark.table(table)._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()))
    except Exception:
        return None
    return None if size >= _UNKNOWN_SIZE else size


def within_budget(spark: SparkSession, minutes: float, work: Callable[[], Any]) -> Any:
    """work() in its own Spark job group, once GATE admits it; raises BudgetExceeded (after cancelling
    the group) when it runs past the budget, ConcurrencyLimit when it is not admitted, or whatever
    work() raised."""
    from pyspark import InheritableThread

    GATE.admit(spark)  # raises ConcurrencyLimit: never more than max_concurrent_jobs live units
    group = f"dre-check-{uuid.uuid4().hex}"
    box: dict[str, Any] = {}

    def target() -> None:
        spark.sparkContext.setJobGroup(group, "dre check", interruptOnCancel=True)
        try:
            box["value"] = work()
        except BaseException as exc:  # handed back to the caller's thread
            box["error"] = exc

    worker = InheritableThread(target=target, daemon=True)
    unit = _Unit(worker, group)
    worker.start()
    GATE.started(unit)
    worker.join(minutes * 60)
    if worker.is_alive():
        GATE.cancel(spark, unit)  # it stays live, and counted, until its thread stops
        raise BudgetExceeded(minutes)
    if "error" in box:
        raise box["error"]
    return box["value"]
