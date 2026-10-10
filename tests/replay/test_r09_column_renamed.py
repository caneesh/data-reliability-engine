"""R09: a configured column is renamed in the table.

Checks using it are DID_NOT_RUN / column_missing; the run continues (spec section 9).
"""

from __future__ import annotations

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_row, gold_row
from tests.replay.conftest import REALTIME_DATASETS, latest, replay_conf

CURATED = "datasets/example_curated_enrollment.yaml"


def test_r09_renamed_column_is_column_missing_and_run_continues(spark, tmp_path, capsys) -> None:
    # Curated is made key-unique here so a second check runs alongside the broken one.
    replay = replay_conf(spark, tmp_path, "r09", {CURATED: [("key_unique: false", "key_unique: true")]})
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()], rename={"mem_nbr": "member_nbr"})
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(), curated_row(sub_id="123402")])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    results = latest(spark, replay)

    [gold] = results[("gold_member_coverage", "T1_KEY_DUPLICATES")]
    assert (gold.state, gold.reason_category, gold.reason_code) == ("DID_NOT_RUN", "CONFIGURATION", "column_missing")
    assert "mem_nbr" in gold.detail
    assert gold.population is None

    # Every check that reads the renamed key column is column_missing ...
    [gold_nulls] = results[("gold_member_coverage", "T1_KEY_NULLS")]
    assert (gold_nulls.state, gold_nulls.reason_code) == ("DID_NOT_RUN", "column_missing")
    # ... while gold's checks that do not read it still evaluate.
    [on_time] = results[("gold_member_coverage", "T1_ON_TIME")]
    assert on_time.reason_code != "column_missing"

    # The table-wide dataset over the same gold table reads the same column.
    [table_wide] = results[("gold_member_coverage_all", "T1_KEY_DUPLICATES")]
    assert (table_wide.state, table_wide.reason_code) == ("DID_NOT_RUN", "column_missing")

    [curated] = results[("example_curated_enrollment", "T1_KEY_DUPLICATES")]
    assert (curated.state, curated.population, curated.violations) == ("PASSED", 2, 0)

    [run] = spark.table(f"{replay.dq}.v_latest_run").collect()
    assert run.status == "COMPLETED" and run.checks_written == run.checks_expected
    # This feed: gold 6 + curated 6 + table-wide 3, less one: schema drift runs once per physical table.
    assert len([k for k in results if k[0] in REALTIME_DATASETS]) == 14
    assert "dre run: COMPLETED" in capsys.readouterr().out
