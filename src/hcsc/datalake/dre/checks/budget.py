"""Compute budget for one check (spec section 8).

A check gets its dataset's compute_budget_minutes. It runs in its own Spark job group; if it
has not finished when the budget runs out, the group is cancelled (running Spark jobs are
interrupted) and the check is DID_NOT_RUN / budget_exceeded. The table's estimated scan size,
from Spark's table statistics when the metastore has them, goes in the result's detail.
Python work inside a cancelled check cannot be interrupted; it is left to end on its own and
its result is ignored.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Spark's spark.sql.defaultSizeInBytes: what the plan reports when it has no statistics.
_UNKNOWN_SIZE = 2**63 - 1


class BudgetExceeded(Exception):
    def __init__(self, minutes: float) -> None:
        super().__init__(f"compute budget of {minutes:g} minutes exceeded")
        self.minutes = minutes


def estimated_scan_bytes(spark: SparkSession, table: str) -> int | None:
    """The table's size from Spark's statistics, or None when there are none."""
    try:
        size = int(str(spark.table(table)._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()))
    except Exception:
        return None
    return None if size >= _UNKNOWN_SIZE else size


def within_budget(spark: SparkSession, minutes: float, work: Callable[[], Any]) -> Any:
    """work() in its own Spark job group; raises BudgetExceeded (after cancelling the group) when it
    runs past the budget, or whatever work() raised."""
    from pyspark import InheritableThread

    group = f"dre-check-{uuid.uuid4().hex}"
    box: dict[str, Any] = {}

    def target() -> None:
        spark.sparkContext.setJobGroup(group, "dre check", interruptOnCancel=True)
        try:
            box["value"] = work()
        except BaseException as exc:  # handed back to the caller's thread
            box["error"] = exc

    worker = InheritableThread(target=target, daemon=True)
    worker.start()
    worker.join(minutes * 60)
    if worker.is_alive():
        spark.sparkContext.cancelJobGroup(group)
        raise BudgetExceeded(minutes)
    if "error" in box:
        raise box["error"]
    return box["value"]
