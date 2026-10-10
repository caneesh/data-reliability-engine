"""R10 at check level (build step 7): an older coverage stays open while a newer one exists.
The sample rule older_coverage_still_open (superseded_still_open, grouped by source) FAILS.
"""

from __future__ import annotations

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, gold_load_time, gold_row
from tests.replay.conftest import latest, replay_conf


def test_r10_older_coverage_still_open_is_rule_failed_grouped_by_source(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "r10")
    loaded = gold_load_time(2)
    create_table(spark, replay.gold, GOLD_COLUMNS, [
        gold_row(eff="2026-01-01", loaded=loaded),                          # older, still open
        gold_row(eff="2026-03-01", loaded=loaded),                          # newer, open
        gold_row(sub_id="000123402", eff="2026-01-01", end="2026-02-28", loaded=loaded),  # closed properly
        gold_row(sub_id="000123402", eff="2026-03-01", loaded=loaded),
    ])
    create_table(spark, replay.curated, CURATED_COLUMNS)

    assert main(["run", "--conf", str(replay.conf)]) == 0
    results = latest(spark, replay)
    [rule] = results[("gold_member_coverage", "older_coverage_still_open")]
    assert (rule.state, rule.population, rule.violations) == ("FAILED", 3, 1)
    assert rule.group_values == {"src_sys_nm": "SRC_A"}
    assert rule.severity == "medium"
    assert rule.observed == "1 of 3 open rows have a later row in their group"
    # The other gold rules ran over the same rows and pass: one open row and one row per coverage.
    for rule_id in ("one_open_row_per_coverage", "one_row_per_coverage", "end_not_before_start"):
        [other] = results[("gold_member_coverage", rule_id)]
        assert (other.state, other.violations) == ("PASSED", 0), rule_id
    [fmt] = results[("gold_member_coverage_all", "coverage_end_date_format")]
    assert (fmt.state, fmt.severity) == ("PASSED", "low")
