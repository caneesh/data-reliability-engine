"""A small, controlled local benchmark of HOP_LEDGER (examination only; synthetic data; NOT a load test).

Run from the repository root (local-mode Spark, 2 cores, 2 shuffle partitions):
    PYSPARK_PYTHON=$PWD/.venv/bin/python .venv/bin/python -m pytest -q -s -p no:cacheprovider \
        docs/reviews/reconciliation-examination/adversarial/bench_ledger.py
It times one ledger evaluation at a few sizes and counts the physical plan's operators that cost at
scale (table scans, exchanges, sort-merge joins, window sorts). Numbers are for this machine only.
"""

from __future__ import annotations

import re
import time

import pytest

from hcsc.datalake.dre.checks.base import render_sql
from hcsc.datalake.dre.checks.hop.ledger import HopLedger, hop_of, ledger_params
from tests.checks.test_hop import EVENT, IN_WINDOW, tables

from test_ledger_adversarial import UP, declared, ledger
from tests.checks.test_hop_ledger import LOADED_HERE
from tests.fixtures.layers import curated_row, gold_row
from tests.fixtures.settings import SETTINGS


@pytest.mark.parametrize("n", [10_000, 100_000, 300_000])
def test_bench_ledger(spark, n: int) -> None:
    cur = [curated_row(sub_id=str(i), updated=IN_WINDOW) for i in range(1, n + 1)]
    gold = [gold_row(sub_id=f"{i:09d}", record=IN_WINDOW, loaded=LOADED_HERE) for i in range(1, n + 1)]
    up, dn = tables(spark, f"bench_{n}", cur, gold)
    dn = declared(dn)
    started = time.monotonic()
    r = ledger(spark, dn, up)
    elapsed = time.monotonic() - started
    from hcsc.datalake.dre.checks.base import CheckContext

    ctx = CheckContext(spark, dn, None, SETTINGS, "UTC", None, upstreams={UP: up}, datasets={dn.dataset: dn, UP: up})
    params = ledger_params(ctx, EVENT, hop_of(ctx, UP), up)
    plan = spark.sql("EXPLAIN FORMATTED " + render_sql("hop_ledger.sql.j2", p=params, mode="assessed", source=None)).collect()[0][0]
    ops = {name: len(re.findall(rf"\(\d+\) {name}\b", plan)) for name in
           ("Scan", "Exchange", "SortMergeJoin", "BroadcastHashJoin", "Window", "HashAggregate", "Sort")}
    print(f"\nBENCH n={n}: state={r.state} due={r.population} elapsed={elapsed:.1f}s ops={ops}")
    assert r.state == "PASSED" and r.population == n and HopLedger
