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


def test_a_spent_dataset_budget_or_a_too_large_scan_is_not_run(spark) -> None:
    """compute_budget_scope: dataset (no minutes left) and scan_mb_per_minute (refused up front)."""
    context = ctx(spark, SETTINGS)
    spent = CheckContext(context.spark, context.dataset, None, SETTINGS, "UTC", budget_minutes=0)
    [result], duration_ms = run_check(Quick(), spent, EVENT)
    assert (result.reason_code, duration_ms) == ("budget_exceeded", 0)
    assert "spent" in result.detail
    spark.sql("ANALYZE TABLE budget.gold COMPUTE STATISTICS")
    slow_cluster = CheckContext(context.spark, context.dataset, None, BUDGET, "UTC", scan_mb_per_minute=1e-9)
    [result], _ = run_check(Quick(), slow_cluster, EVENT)
    assert result.reason_code == "budget_exceeded" and "not run" in result.detail
    fast_cluster = CheckContext(context.spark, context.dataset, None, BUDGET, "UTC", scan_mb_per_minute=1e6)
    [result], _ = run_check(Quick(), fast_cluster, EVENT)
    assert result.state == "PASSED"


def test_dataset_budget_scope_is_shared_by_a_dataset_s_checks_in_a_run(spark, tmp_path) -> None:
    from hcsc.datalake.dre.cli import main
    from tests.replay.conftest import latest, replay_conf

    # A budget of a few milliseconds per dataset: the first check spends it, the rest are not run.
    replay = replay_conf(spark, tmp_path, "budget_scope", {"defaults.yaml": [
        ("compute_budget_scope: check", "compute_budget_scope: dataset"),
        ("compute_budget_minutes: 8", "compute_budget_minutes: 0.0001")],
        "datasets/gold_member_coverage.yaml": [("compute_budget_minutes: 8", "compute_budget_minutes: 0.0001")]})
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()])
    assert main(["run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0
    gold = [row for (ds, _), rows in latest(spark, replay).items() if ds == "gold_member_coverage" for row in rows]
    assert {r.reason_code for r in gold} == {"budget_exceeded"}
    assert any("spent" in (r.detail or "") for r in gold)
