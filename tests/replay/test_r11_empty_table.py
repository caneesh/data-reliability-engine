"""R11: empty table. Checks are DID_NOT_RUN / empty_population, never PASSED (spec section 9)."""

from __future__ import annotations

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, gold_row
from tests.replay.conftest import latest, replay_conf

CURATED = "datasets/example_curated_enrollment.yaml"


def test_r11_empty_tables_are_empty_population(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "r11", {CURATED: [("key_unique: false", "key_unique: true")]})
    create_table(spark, replay.gold, GOLD_COLUMNS)  # empty; gold is grouped by src_sys_nm
    create_table(spark, replay.curated, CURATED_COLUMNS)  # empty; ungrouped

    assert main(["run", "--conf", str(replay.conf)]) == 0
    results = latest(spark, replay)
    rows = [row for group in results.values() for row in group]
    assert {(r.dataset, r.check_id) for r in rows} == {
        ("gold_member_coverage", "T1_KEY_DUPLICATES"), ("example_curated_enrollment", "T1_KEY_DUPLICATES")}
    for row in rows:
        assert (row.state, row.reason_category, row.reason_code) == (
            "DID_NOT_RUN", "DATA_UNAVAILABLE", "empty_population"), row
    assert not [r for r in spark.table(f"{replay.dq}.dq_check_result").collect() if r.state == "PASSED"]


def test_r11_rows_outside_the_feed_filter_are_an_empty_population(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "r11b")
    # Rows exist, but none belong to this feed (feed_filter src_sys_nm = 'SRC_A').
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(source="SRC_B")])
    create_table(spark, replay.curated, CURATED_COLUMNS)

    assert main(["run", "--conf", str(replay.conf)]) == 0
    [gold] = latest(spark, replay)[("gold_member_coverage", "T1_KEY_DUPLICATES")]
    assert (gold.state, gold.reason_code) == ("DID_NOT_RUN", "empty_population")
