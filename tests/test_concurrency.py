"""The run's concurrency limit (ADR-08, "at most N concurrent Spark jobs per run"; final review O6).

Every unit of DRE work that issues Spark jobs (a check, a cause investigation, an evidence write)
runs through checks/budget.py within_budget, in its own Spark job group. At most
`max_concurrent_jobs` units are live at once, and a unit cancelled by its budget stays live, and
counts, until its thread has really stopped: while it lingers its job group is cancelled again, so a
job it starts after the first cancellation is killed too. A unit that cannot start within
`cancel_grace_seconds` is refused (DID_NOT_RUN / BUDGET / concurrency_limit, which holds the window),
never run alongside.
"""

from __future__ import annotations

import threading
import time

import pytest

from hcsc.datalake.dre.checks import budget
from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.budget import BudgetExceeded, ConcurrencyLimit, within_budget


@pytest.fixture(autouse=True)
def gate():
    budget.configure(max_units=1, grace_seconds=2.0)
    yield
    budget.GATE.drain(30)  # leave no lingering unit to the next test
    budget.configure(max_units=1, grace_seconds=budget.DEFAULT_GRACE_SECONDS)


def lingering(seconds: float, started: threading.Event | None = None):
    """Python work that ignores cancellation (it issues no Spark job that could be interrupted)."""
    def work():
        if started is not None:
            started.set()
        time.sleep(seconds)
        return "late"
    return work


def test_a_cancelled_unit_still_counts_until_it_stops_and_the_next_waits_for_it(spark) -> None:
    """N = 1: the first unit runs past its budget and is cancelled, but its thread lives on for about
    a second. The next unit does not start alongside it: it waits, inside the grace, and then runs.
    Never more than one live unit."""
    with pytest.raises(BudgetExceeded):
        within_budget(spark, 0.5 / 60, lingering(1.5))
    assert budget.GATE.live() == 1  # cancelled, not stopped: still counted
    started = time.monotonic()
    assert within_budget(spark, 1.0, lambda: "next") == "next"
    assert time.monotonic() - started >= 0.5  # it waited for the lingering unit to stop
    assert budget.GATE.peak == 1


def test_a_unit_that_cannot_start_within_the_grace_is_refused_never_run_alongside(spark) -> None:
    with pytest.raises(BudgetExceeded):
        within_budget(spark, 0.2 / 60, lingering(10))
    ran = []
    with pytest.raises(ConcurrencyLimit) as refused:
        within_budget(spark, 1.0, lambda: ran.append(1))
    assert ran == [] and budget.GATE.peak == 1
    assert "1 unit(s) still running" in str(refused.value)


def test_a_cancelled_unit_s_later_spark_jobs_are_cancelled_too(spark) -> None:
    """A cancelled unit whose code swallows the cancellation and starts another Spark job: the gate
    cancels its group again while it lingers, so that job is killed and the unit stops; the next unit
    then runs. Spark's cancelJobGroup alone stops only the jobs active at that moment."""
    from pyspark.sql.functions import udf

    budget.configure(max_units=1, grace_seconds=30.0)  # long enough for the re-cancellations to stop it
    slow = udf(lambda x: time.sleep(4) or x, "long")
    attempts = []

    def stubborn():
        for _ in range(3):
            try:
                spark.range(1, numPartitions=1).select(slow("id")).collect()
                attempts.append("finished")
            except Exception:
                attempts.append("cancelled")  # swallowed, and tries again
        return "done"

    with pytest.raises(BudgetExceeded):
        within_budget(spark, 1.0 / 60, stubborn)
    assert within_budget(spark, 1.0, lambda: "next") == "next"
    assert "finished" not in attempts  # every job the lingering unit started was cancelled
    assert budget.GATE.peak == 1


def test_a_check_refused_by_the_limit_is_did_not_run_and_holds_its_window(spark) -> None:
    """Through run_check: the refusal is a result like any other, DID_NOT_RUN / BUDGET /
    concurrency_limit; BUDGET holds the window (spec section 4)."""
    from hcsc.datalake.dre.checks.events import BLOCKING_CATEGORIES
    from tests.checks.test_hop import EVENT, curated

    with pytest.raises(BudgetExceeded):
        within_budget(spark, 0.2 / 60, lingering(10))

    class Probe:
        check_id, params_model, severity = "T1_PROBE", None, None

        def required_columns(self, dataset):
            return []

        def evaluate(self, ctx, event):
            raise AssertionError("must not run while the limit is reached")

    from tests.fixtures.settings import SETTINGS

    ctx = CheckContext(spark, curated("hop.none"), None, SETTINGS, "UTC")
    [r], _ = run_check(Probe(), ctx, EVENT)
    assert (r.state, r.reason_category, r.reason_code) == ("DID_NOT_RUN", "BUDGET", "concurrency_limit")
    assert r.reason_category in BLOCKING_CATEGORIES


def test_two_units_may_overlap_only_when_the_limit_allows_it(spark) -> None:
    """N = 2: a lingering unit and a new one may be live together, never three."""
    budget.configure(max_units=2, grace_seconds=1.0)
    with pytest.raises(BudgetExceeded):
        within_budget(spark, 0.2 / 60, lingering(6))
    assert within_budget(spark, 1.0, lambda: "alongside") == "alongside"
    with pytest.raises(BudgetExceeded):
        within_budget(spark, 0.2 / 60, lingering(6))
    with pytest.raises(ConcurrencyLimit):
        within_budget(spark, 1.0, lambda: "third")
    assert budget.GATE.peak == 2
