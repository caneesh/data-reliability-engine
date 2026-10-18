"""dq_reconciliation: HOP_LEDGER's lines are evidence of their result (build step 3; ADR-07).

They are written before the result, counted before the commit, visible only once committed
(v_committed_reconciliation), hidden while their unit is INVALID, and listed by the integrity view
when they are lost. Through `dre run`, a hop's ledger is published with its result and the lines
balance against its population.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

from hcsc.datalake.dre.cli import main
from hcsc.datalake.dre.store.commits import Commit, append_commits
from hcsc.datalake.dre.store.reconciliation import append_ledger
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row
from tests.replay.conftest import latest
from tests.store.conftest import append_rows, store  # noqa: F401  (the store fixture)

UTC = timezone.utc
AT = datetime(2026, 1, 1, tzinfo=UTC)


def ledger_lines(n: int) -> list[dict]:
    names = ["due", "delivered", "missing", "pending"]
    return [{"upstream": "up", "line": names[i % len(names)], "class": "ok", "records": i,
             "evidence_level": "observed", "scope": "{}"} for i in range(n)]


def evaluation(spark, db: str, run_id: str, ev: str, day: date, lines: int) -> int:
    """A HOP_LEDGER result with its ledger lines, stored as a run stores them (uncommitted)."""
    append_rows(spark, db, "dq_check_result", [{"evaluation_id": ev, "run_id": run_id, "dataset": "gold",
                                                "check_id": "HOP_LEDGER", "state": "PASSED", "run_date": day}],
                commit=False)
    return append_ledger(spark, db, ledger_lines(lines), run_id, ev, "gold", "HOP_LEDGER", AT, day)


def shown(spark, db: str, ev: str) -> int:
    return spark.table(f"{db}.v_committed_reconciliation").where(f"evaluation_id = '{ev}'").count()


def test_ledger_lines_are_visible_only_under_a_commit_that_vouches_for_them(spark, store) -> None:
    day = date(2026, 3, 1)
    assert evaluation(spark, store, "r1", "never", day, 3) == 3
    assert shown(spark, store, "never") == 0  # no commit: invisible
    evaluation(spark, store, "r1", "zero", day, 2)
    append_commits(spark, store, [Commit("r1", "zero", "gold", "HOP_LEDGER", 1)], AT, day)
    assert shown(spark, store, "zero") == 0  # a commit that declares none keeps them invisible
    evaluation(spark, store, "r1", "yes", day, 4)
    append_commits(spark, store, [Commit("r1", "yes", "gold", "HOP_LEDGER", 1, reconciliation_rows=4)], AT, day)
    assert shown(spark, store, "yes") == 4


def test_a_unit_whose_ledger_lines_differ_from_its_commit_is_withheld(spark, store) -> None:
    from hcsc.datalake.dre.runner import Publisher
    from hcsc.datalake.dre.store import runs

    run = runs.new_run(now=datetime(2026, 3, 2, 12, tzinfo=UTC))
    evaluation(spark, store, run.run_id, "short", run.run_date, 2)
    evaluation(spark, store, run.run_id, "whole", run.run_date, 3)
    publisher = Publisher(spark, store, run, batch=10)
    publisher.add([Commit(run.run_id, "short", "gold", "HOP_LEDGER", 1, reconciliation_rows=3)], 1)  # one line lost
    publisher.add([Commit(run.run_id, "whole", "gold", "HOP_LEDGER", 1, reconciliation_rows=3)], 1)
    publisher.flush()
    assert (publisher.published, publisher.withheld) == (1, 1)
    committed = {r.evaluation_id for r in spark.table(f"{store}.v_committed_result").where(f"run_id = '{run.run_id}'")
                 .collect()}
    assert committed == {"whole"} and shown(spark, store, "short") == 0 and shown(spark, store, "whole") == 3


def test_lost_ledger_lines_are_listed_by_the_integrity_view_and_hidden_once_invalid(spark, store) -> None:
    """A commit whose ledger lines were lost is listed by v_commit_integrity; a sweep records lines
    changed after publication as EVIDENCE_MISMATCH (counts only), and the unit's lines are hidden
    from then on, so no report rests on them."""
    from hcsc.datalake.dre.store import integrity, runs

    run = runs.new_run(now=datetime(2026, 3, 3, 12, tzinfo=UTC))
    day = run.run_date
    append_rows(spark, store, "dq_check_result", [{"evaluation_id": "lost", "run_id": "r3", "dataset": "gold",
                                                   "check_id": "HOP_LEDGER", "state": "PASSED", "run_date": day}],
                commit=False)
    append_commits(spark, store, [Commit("r3", "lost", "gold", "HOP_LEDGER", 1, reconciliation_rows=5)], AT, day)
    rows = spark.table(f"{store}.v_commit_integrity").where(f"run_date = DATE'{day.isoformat()}'").collect()
    assert [(r.evaluation_id, r.reconciliation_rows, r.stored_reconciliation_rows) for r in rows] == [("lost", 5, 0)]
    evaluation(spark, store, "r4", "bad", day, 2)
    append_commits(spark, store, [Commit("r4", "bad", "gold", "HOP_LEDGER", 1, reconciliation_rows=2)], AT, day)
    assert shown(spark, store, "bad") == 2
    append_ledger(spark, store, ledger_lines(1), "r4", "bad", "gold", "HOP_LEDGER", AT, day)  # changed afterwards
    assert integrity.sweep(spark, store, run, integrity.DAILY, day, day, day).mismatches == 2
    details = {r.evaluation_id: r.detail for r in spark.table(f"{store}.dq_integrity")
               .where("record = 'EVIDENCE_MISMATCH'").collect()}
    assert details == {"lost": "ledger rows: 5 declared, 0 stored", "bad": "ledger rows: 2 declared, 3 stored"}
    assert shown(spark, store, "bad") == 0


def test_a_run_publishes_each_hop_ledger_with_its_result(spark, tmp_path) -> None:
    """Through dre run: one HOP_LEDGER result for the hop, its lines committed with it, the identity
    readable off the store (the dispositions add up to the due population), the scope on every line,
    and nothing in the integrity view."""
    from tests.replay.test_r04_to_r07_hops import setup

    replay = setup(spark, tmp_path, "ledger_run")
    loaded = curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(sub_id="123401", updated=loaded),
                                                          curated_row(sub_id="123402", updated=loaded)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [])
    assert main(["run", "--conf", str(replay.conf)]) == 0
    [result] = latest(spark, replay)[("gold_member_coverage", "HOP_LEDGER")]
    assert (result.state, result.population, result.violations) == ("FAILED", 2, 2)
    assert result.observed.startswith("due 2 (pending 0): missing 2")
    lines = spark.table(f"{replay.dq}.v_committed_reconciliation").where(
        f"evaluation_id = '{result.evaluation_id}'").collect()
    [commit] = spark.table(f"{replay.dq}.dq_commit").where(f"evaluation_id = '{result.evaluation_id}'").collect()
    assert commit.reconciliation_rows == len(lines) > 0
    by_line = {}
    for x in lines:
        by_line[x.line] = by_line.get(x.line, 0) + (x.records or 0)
    buckets = sum(n for line, n in by_line.items()
                  if line not in ("due", "pending", "untimed", "orphan", "overlap", "guard"))
    assert by_line["due"] == buckets == 2 and by_line["missing"] == 2
    assert {x.scope for x in lines} == {result.detail}
    scope = json.loads(result.detail)
    # the sample declares the hop a union_partition attributed by gold's feed_filter (decision D5)
    assert (scope["mapping"], scope["claim"], scope["orphans"]) == ("union_partition", "contract-proven", "checked")
    assert spark.table(f"{replay.dq}.v_commit_integrity").count() == 0
