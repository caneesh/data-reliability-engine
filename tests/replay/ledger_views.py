"""Reading a hop's ledger off the committed store views, for the proof-scenario replays (R14 onwards).

Every helper reads what `dre run` committed: `v_latest_result` and `v_committed_result` for the
result, `v_committed_reconciliation` for its ledger lines (counts, codes and labels only). Nothing
here reads the engine's internals.
"""

from __future__ import annotations

import json
from typing import Any

from tests.replay.conftest import Replay

GOLD = "gold_member_coverage"
UP = "example_curated_enrollment"
# The ledger's dispositions (spec section 6, HOP_LEDGER; reconciliation model section 3): the lines
# whose records add up to the due population. pending, untimed, orphan, overlap and guard sit beside it.
DISPOSITIONS = ("unassessable", "deleted", "deleted_not_applied", "excluded", "excluded_but_present", "rejected",
                "superseded", "delivered", "divergent", "stale", "missing")


def ledger_results(spark, replay: Replay, dataset: str = GOLD, upstream: str = UP) -> list[Any]:
    """Every committed HOP_LEDGER result of the hop, oldest first (v_committed_result)."""
    rows = spark.table(f"{replay.dq}.v_committed_result").where(
        f"dataset = '{dataset}' AND check_id = 'HOP_LEDGER'").collect()
    rows = [r for r in rows if r.group_values == {"upstream": upstream}]
    return sorted(rows, key=lambda r: (r.evaluated_at, r.run_id))


def current_ledger(spark, replay: Replay, dataset: str = GOLD, upstream: str = UP) -> Any:
    """The hop's current HOP_LEDGER result (v_latest_result), with `healthy`."""
    rows = spark.table(f"{replay.dq}.v_latest_result").where(
        f"dataset = '{dataset}' AND check_id = 'HOP_LEDGER'").collect()
    [row] = [r for r in rows if r.group_values == {"upstream": upstream}]
    return row


def lines(spark, replay: Replay, evaluation_id: str) -> dict[tuple[str, str | None], tuple[int, str]]:
    """(line, label) -> (records, class), from v_committed_reconciliation."""
    out: dict[tuple[str, str | None], tuple[int, str]] = {}
    for x in spark.table(f"{replay.dq}.v_committed_reconciliation").where(
            f"evaluation_id = '{evaluation_id}'").collect():
        assert (x.line, x.label) not in out, (x.line, x.label)
        out[(x.line, x.label)] = (x.records, x["class"])
    return out


def nonzero(ledger: dict[tuple[str, str | None], tuple[int, str]]) -> dict[tuple[str, str | None], int]:
    """The ledger lines with records, without their class."""
    return {k: n for k, (n, _) in ledger.items() if n}


def assert_balanced(ledger: dict[tuple[str, str | None], tuple[int, str]], population: int) -> None:
    """The identity, read off the store: the dispositions add up to the due population."""
    buckets = sum(n for (line, _), (n, _) in ledger.items() if line in DISPOSITIONS)
    assert ledger[("due", None)][0] == buckets == population


def scope(result: Any) -> dict:
    return json.loads(result.detail)
