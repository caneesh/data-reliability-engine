"""R02, R03 and R08 at check level (build step 5). Causes come in step 8."""

from __future__ import annotations

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.replay.conftest import latest, replay_conf

CURATED = "datasets/example_curated_enrollment.yaml"
CURATED_LOAD_TIME = ("load_time: null ", "load_time: { column: sourcelastupdatets, granularity: minute } ")


def test_r02_no_loads_for_longer_than_the_sla_is_on_time_failed(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "r02")
    # The last gold load was three days ago: every slot due in the window missed its SLA.
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(loaded=gold_load_time(72))])
    create_table(spark, replay.curated, CURATED_COLUMNS)

    assert main(["run", "--conf", str(replay.conf)]) == 0
    [on_time] = latest(spark, replay)[("gold_member_coverage", "T1_ON_TIME")]
    assert on_time.state == "FAILED"
    assert on_time.population > 0 and on_time.violations == on_time.population
    assert "had no load within the SLA" in on_time.observed


def test_r03_gold_wrote_zero_rows_while_upstream_had_rows_is_zero_rows_failed(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "r03", {CURATED: [CURATED_LOAD_TIME]})
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(loaded=gold_load_time(72))])  # nothing recent
    # Upstream loaded every hour through the window, so every load due upstream wrote rows.
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(sub_id=f"1234{h:02d}", updated=curated_load_time(h)) for h in range(1, 40)])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    results = latest(spark, replay)
    [gold] = results[("gold_member_coverage", "T1_ZERO_ROWS")]
    assert gold.state == "FAILED"
    assert gold.population > 0 and gold.violations == gold.population  # every load due wrote nothing
    assert gold.observed.endswith("due loads wrote fewer rows than the minimum")
    [upstream] = results[("example_curated_enrollment", "T1_ZERO_ROWS")]
    assert upstream.state == "PASSED"  # upstream did load: the gap is at gold


def test_r08_duplicate_rows_per_key_in_gold_is_key_duplicates_failed(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "r08")
    recent = gold_load_time(2)
    rows = [gold_row(loaded=recent), gold_row(loaded=recent),                       # the same key twice
            gold_row(sub_id="123401", loaded=recent),                               # and again, unpadded
            gold_row(sub_id="000123402", loaded=recent)]
    create_table(spark, replay.gold, GOLD_COLUMNS, rows)
    create_table(spark, replay.curated, CURATED_COLUMNS)

    assert main(["run", "--conf", str(replay.conf)]) == 0
    [dups] = latest(spark, replay)[("gold_member_coverage", "T1_KEY_DUPLICATES")]
    assert (dups.state, dups.population, dups.violations) == ("FAILED", 2, 1)
    assert dups.group_values == {"src_sys_nm": "SRC_A"}
