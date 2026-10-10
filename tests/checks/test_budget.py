"""Compute budget (spec section 8, build step 10): a deliberately slow check ends DID_NOT_RUN /
budget_exceeded, its Spark jobs are cancelled, and the run goes on."""

from __future__ import annotations

import time
from datetime import datetime, timezone

from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, from_counts, run_check
from hcsc.datalake.dre.checks.budget import estimated_scan_bytes
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.config.models import Dataset
from tests.fixtures.layers import GOLD_COLUMNS, create_table, gold_row
from tests.fixtures.settings import SETTINGS

UTC = timezone.utc
EVENT = Event("gold", datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 1, 16, 12, tzinfo=UTC))
BUDGET = SETTINGS.model_copy(update={"compute_budget_minutes": 0.05})  # 3 seconds


class Slow(Check):
    """A Spark job whose every task sleeps far past the budget."""
    check_id = "SLOW"

    def required_columns(self, dataset: Dataset) -> list[str]:
        return []

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        n = ctx.spark.sparkContext.parallelize(range(4), 2).map(lambda x: time.sleep(120) or x).count()
        return [from_counts(n, 0, "done", "done")]


class Quick(Slow):
    check_id = "QUICK"

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        n = ctx.spark.sql("SELECT COUNT(*) AS n FROM budget.gold").collect()[0].n
        return [from_counts(n, 0, "rows", "rows")]


def ctx(spark, settings=BUDGET) -> CheckContext:
    create_table(spark, "budget.gold", GOLD_COLUMNS, [gold_row()])
    ds = Dataset(dataset="gold", table="budget.gold", layer="GOLD", key=["sub_id"])
    return CheckContext(spark, ds, None, settings, "UTC")


def test_a_slow_check_is_cancelled_as_budget_exceeded(spark) -> None:
    started = time.monotonic()
    [result], duration_ms = run_check(Slow(), ctx(spark), EVENT)
    elapsed = time.monotonic() - started
    assert (result.state, result.reason_category, result.reason_code) == ("DID_NOT_RUN", "BUDGET", "budget_exceeded")
    assert result.detail.startswith("compute budget of 0.05 minutes exceeded")
    assert 3 <= elapsed < 60 and duration_ms >= 3000  # stopped at the budget, not after the tasks' 120 s
    # The check's Spark jobs were cancelled, not left running.
    deadline = time.monotonic() + 30
    while spark.sparkContext.statusTracker().getActiveJobsIds() and time.monotonic() < deadline:
        time.sleep(0.5)
    assert spark.sparkContext.statusTracker().getActiveJobsIds() == []


def test_a_check_within_its_budget_runs_normally(spark) -> None:
    [result], _ = run_check(Quick(), ctx(spark), EVENT)
    assert (result.state, result.population) == ("PASSED", 1)
    # A failure inside the budget is still a DID_NOT_RUN with its own reason.
    class Broken(Quick):
        def evaluate(self, ctx, event):
            return ctx.spark.sql("SELECT nope FROM budget.gold").collect()
    [result], _ = run_check(Broken(), ctx(spark), EVENT)
    assert (result.state, result.reason_category) == ("DID_NOT_RUN", "CONFIGURATION")


def test_scan_size_estimate(spark) -> None:
    ctx(spark)
    spark.sql("ANALYZE TABLE budget.gold COMPUTE STATISTICS")
    assert estimated_scan_bytes(spark, "budget.gold") > 0
    assert estimated_scan_bytes(spark, "budget.no_such_table") is None
