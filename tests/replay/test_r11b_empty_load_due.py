"""R11b: empty tables with a load due. T1_ON_TIME and T1_ZERO_ROWS are FAILED (a load was
due and nothing arrived); row-based checks are DID_NOT_RUN / empty_population; nothing
PASSES. (Decided in the step 4 review; spec section 9.)
"""

from __future__ import annotations

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table
from tests.replay.conftest import REALTIME_DATASETS, latest, replay_conf

CURATED = "datasets/example_curated_enrollment.yaml"
CURATED_LOAD_TIME = ("load_time: null ", "load_time: { column: sourcelastupdatets, granularity: minute } ")
PRESENCE = {"T1_ON_TIME", "T1_ZERO_ROWS"}


def test_r11b_load_due_presence_checks_fail_row_checks_did_not_run(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "r11b", {
        CURATED: [("key_unique: false", "key_unique: true"), CURATED_LOAD_TIME],
    })
    create_table(spark, replay.gold, GOLD_COLUMNS)
    create_table(spark, replay.curated, CURATED_COLUMNS)

    assert main(["run", "--conf", str(replay.conf)]) == 0  # FAILED checks still complete the run
    rows = [row for (ds, _), group in latest(spark, replay).items() if ds in REALTIME_DATASETS for row in group]
    assert len(rows) == 21  # gold: 6 Tier 1 + HOP_KEY_CURRENCY + HOP_VALUE_AGREEMENT + 4 rules = 12; curated 6; table-wide 4; drift once per table
    for row in rows:
        assert row.state != "PASSED", row
        if row.check_id in PRESENCE:
            assert row.state == "FAILED", row
            assert row.population > 0 and row.violations > 0, row
        elif row.check_id == "T1_SCHEMA_DRIFT":
            assert (row.state, row.reason_code) == ("DID_NOT_RUN", "insufficient_history"), row
        else:
            assert (row.state, row.reason_code) == ("DID_NOT_RUN", "empty_population"), row
    failed = {(r.dataset, r.check_id) for r in rows if r.state == "FAILED"}
    assert failed == {(ds, c) for ds in ("gold_member_coverage", "example_curated_enrollment") for c in PRESENCE}
