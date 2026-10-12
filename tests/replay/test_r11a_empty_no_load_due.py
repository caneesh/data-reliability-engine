"""R11a: empty tables, no load due in the window. Every check is DID_NOT_RUN, never PASSED.

T1_ON_TIME and T1_ZERO_ROWS count loads due, not rows: none due is empty_population.
Row-based checks find no rows: empty_population. T1_SCHEMA_DRIFT has no baseline yet:
insufficient_history. (Decided in the step 4 review; spec section 9.)
"""

from __future__ import annotations

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, gold_row
from tests.replay.conftest import REALTIME_DATASETS, latest, replay_conf

FEED = "feeds/example_realtime.yaml"
CURATED = "datasets/example_curated_enrollment.yaml"
# Synthetic test config: a cadence whose only date is long past, so no load is due now,
# and a load time on curated so its load-time checks evaluate.
NO_LOAD_DUE = (
    "  kind: times\n  times: [",
    "  kind: calendar_dates\n  dates: [2020-01-01]\n  times: [",
)
CURATED_LOAD_TIME = ("load_time: null ", "load_time: { column: sourcelastupdatets, granularity: minute } ")


def test_r11a_no_load_due_everything_did_not_run(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "r11a", {
        FEED: [NO_LOAD_DUE],
        CURATED: [("key_unique: false", "key_unique: true"), CURATED_LOAD_TIME],
    })
    create_table(spark, replay.gold, GOLD_COLUMNS)
    create_table(spark, replay.curated, CURATED_COLUMNS)

    # A scheduled run does not evaluate a feed's checks with nothing due; gold rules run on their
    # own (daily) schedule, and on empty tables they too are DID_NOT_RUN ...
    assert main(["run", "--conf", str(replay.conf)]) == 0
    first = [r for r in spark.table(f"{replay.dq}.dq_check_result").collect() if r.feed == "example_realtime"]
    # (the table-wide dataset's rule coverage_end_date_format also ran; it has no feed)
    assert sorted(r.check_id for r in first) == ["end_not_before_start", "older_coverage_still_open",
                                                 "one_open_row_per_coverage", "one_row_per_coverage"]
    assert {(r.state, r.reason_code) for r in first} == {("DID_NOT_RUN", "empty_population")}
    # ... so the scenario forces it, as an operator would with --feed.
    assert main(["run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0
    rows = [row for (ds, _), group in latest(spark, replay).items() if ds in REALTIME_DATASETS for row in group]
    # gold: 6 Tier 1 + HOP_KEY_CURRENCY + HOP_VALUE_AGREEMENT + 4 rules = 12; curated 6; the table-wide
    # dataset's rule from the first run (rules run on their own schedule) 1. --feed runs no table-wide checks.
    assert len(rows) == 19
    for row in rows:
        assert row.state == "DID_NOT_RUN", row
        expected = "insufficient_history" if row.check_id == "T1_SCHEMA_DRIFT" else "empty_population"
        assert row.reason_code == expected, row
    assert not [r for r in spark.table(f"{replay.dq}.dq_check_result").collect() if r.state == "PASSED"]


def test_r11a_rows_outside_the_feed_filter_are_an_empty_population(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "r11a_filter", {FEED: [NO_LOAD_DUE]})
    # Rows exist, but none belong to this feed (feed_filter src_sys_nm = 'SRC_A').
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(source="SRC_B")])
    create_table(spark, replay.curated, CURATED_COLUMNS)

    assert main(["run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0
    [gold] = latest(spark, replay)[("gold_member_coverage", "T1_KEY_DUPLICATES")]
    assert (gold.state, gold.reason_code) == ("DID_NOT_RUN", "empty_population")
