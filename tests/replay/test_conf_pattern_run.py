"""A feed on a pattern defined by configuration alone runs end to end through `dre run`: the
pattern's checks, its cause list, and nothing from the packaged pattern it replaced."""

from __future__ import annotations

from hcsc.datalake.dre.checks.registry import use_conf_patterns
from hcsc.datalake.dre.cli import main
from tests.config.test_conf_patterns import RELOAD, drop_probes
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table
from tests.replay.conftest import latest, replay_conf


def test_a_feed_on_a_conf_pattern_runs_end_to_end(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "cpat", {"feeds/example_realtime.yaml": [("pattern: FILE_CYCLIC",
                                                                                     "pattern: TABLE_RELOAD")]})
    (replay.conf / "patterns").mkdir()
    (replay.conf / "patterns" / "table_reload.yaml").write_text(RELOAD, encoding="utf-8")
    drop_probes(replay.conf)
    create_table(spark, replay.gold, GOLD_COLUMNS, [])
    create_table(spark, replay.curated, CURATED_COLUMNS, [])
    try:
        assert main(["validate", "--conf", str(replay.conf)]) == 0
        assert f"from {replay.conf}/patterns ['TABLE_RELOAD']" in capsys.readouterr().out
        assert main(["run", "--conf", str(replay.conf)]) == 0
        results = latest(spark, replay)
        gold = {check for (ds, check) in results if ds == "gold_member_coverage"}
        # the conf pattern's checks (T1_KEY_DUPLICATES needs key_unique, which the sample sets) and the gold rules;
        # nothing the packaged FILE_CYCLIC pattern would have run (hops, key nulls, files)
        assert {"T1_ON_TIME", "T1_ZERO_ROWS", "T1_KEY_DUPLICATES", "T1_FRESHNESS"} <= gold
        assert not {"HOP_KEY_CURRENCY", "HOP_VALUE_AGREEMENT", "T1_KEY_NULLS", "T1_FILES_NOT_LOADED"} & gold
        [fresh] = results[("gold_member_coverage", "T1_FRESHNESS")]
        assert fresh.state == "DID_NOT_RUN" and fresh.reason_code == "empty_population"  # the tables are empty here
        run = spark.table(f"{replay.dq}.v_latest_run").collect()[0]
        assert run.status == "COMPLETED" and run.checks_written == run.checks_expected
    finally:
        use_conf_patterns(None)
