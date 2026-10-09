"""dre run writes results and run rows; dre dry-run writes nothing and checks SQL fragments."""

from __future__ import annotations

from datetime import timedelta, timezone

from hcsc.datalake.dre.checks.events import Event, previous_window_end
from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_row, gold_row
from tests.replay.conftest import replay_conf

GOLD = "datasets/gold_member_coverage.yaml"


def counts(spark, dq: str) -> tuple[int, int]:
    return spark.table(f"{dq}.dq_check_result").count(), spark.table(f"{dq}.dq_run").count()


def test_run_writes_results_and_windows_follow_on(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "runner_a")
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(), gold_row()])  # one duplicated key
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row()])

    assert main(["run", "--conf", str(replay.conf)]) == 0  # FAILED checks still exit 0
    out = capsys.readouterr().out
    assert "gold_member_coverage T1_KEY_DUPLICATES [src_sys_nm=SRC_A]: FAILED (population 1, violations 1)" in out
    rows = {r.dataset: r for r in spark.table(f"{replay.dq}.dq_check_result").collect()}
    first = rows["gold_member_coverage"]
    assert (first.feed, first.expectation_version, first.execution_type) == ("example_realtime", 1, "NORMAL")
    assert first.group_values == {"src_sys_nm": "SRC_A"}
    table_wide = rows["gold_member_coverage_all"]
    assert (table_wide.feed, table_wide.expectation_version) == (None, 1)

    # First window: ends settle_minutes (15) before the run, truncated to gold's minute
    # granularity, and starts initial_lookback_hours (24) earlier.
    [run1] = spark.table(f"{replay.dq}.v_latest_run").collect()
    end = (run1.started_at - timedelta(minutes=15)).replace(second=0, microsecond=0)
    assert (first.window_start, first.window_end) == (end - timedelta(hours=24), end)
    assert first.event_id == Event("gold_member_coverage", first.window_start, first.window_end).event_id

    # The next run's window starts where the first one ended.
    assert previous_window_end(spark, replay.dq, "gold_member_coverage") == end.replace(tzinfo=timezone.utc)
    assert main(["run", "--conf", str(replay.conf)]) == 0
    second = [r for r in spark.table(f"{replay.dq}.dq_check_result").collect()
              if r.run_id != run1.run_id and r.dataset == "gold_member_coverage"]
    assert [r.window_start for r in second] == [end]
    assert second[0].event_id != first.event_id


def test_replay_runs_do_not_move_the_window(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "runner_f")
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()])
    assert main(["run", "--conf", str(replay.conf), "--execution-type", "REPLAY"]) == 0
    rows = spark.table(f"{replay.dq}.dq_check_result").collect()
    assert {r.execution_type for r in rows} == {"REPLAY"}
    assert previous_window_end(spark, replay.dq, "gold_member_coverage") is None


def test_run_one_feed_and_unknown_feed(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "runner_b")
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()])
    assert main(["run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0
    assert main(["run", "--conf", str(replay.conf), "--feed", "nope"]) == 3
    assert "unknown feed 'nope'" in capsys.readouterr().out


def test_dry_run_prints_results_and_writes_nothing(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "runner_c")
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()])
    before = counts(spark, replay.dq)
    assert main(["dry-run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0
    out = capsys.readouterr().out
    assert "gold_member_coverage T1_KEY_DUPLICATES [src_sys_nm=SRC_A]: PASSED" in out
    assert "nothing written" in out
    assert counts(spark, replay.dq) == before


def test_dry_run_reports_sql_fragments_that_do_not_resolve(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "runner_d", {GOLD: [("src_sys_nm = 'SRC_A'", "src_system = 'SRC_A'")]})
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()])
    assert main(["dry-run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0
    out = capsys.readouterr().out
    gold_file = replay.conf / GOLD
    line = next(i for i, text in enumerate(gold_file.read_text().splitlines(), 1) if "feed_filter:" in text)
    assert f"{gold_file}:{line}: feed_filter: feed_filter does not parse or resolve" in out
    assert "UNRESOLVED_COLUMN" in out and "Fix:" in out
    # The check itself reports the same problem as a configuration DID_NOT_RUN.
    assert "T1_KEY_DUPLICATES: DID_NOT_RUN (CONFIGURATION/column_missing)" in out


def test_fragment_checks_cover_open_when_and_filter_rule_syntax(spark, tmp_path) -> None:
    from hcsc.datalake.dre.config.fragments import check_fragments
    from hcsc.datalake.dre.config.validate import validate_conf

    replay = replay_conf(spark, tmp_path, "runner_e", {
        "rules/one_open_row_per_coverage.yaml": [("mbr_mbrshp_covrg_end_dt = '9999-12-31'", "no_such_col = 1")],
        "feeds/example_realtime.yaml": [("  filter_rules: null", "  filter_rules:\n    - id: drop_tests\n"
                                         "      condition: \"src = 'X' AND (\"\n      code_ref: example.sql:1\n"
                                         "      owner: membership-gold\n      expected_daily_volume: 0")],
    })
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()])
    config, errors, _ = validate_conf(replay.conf)
    assert errors == []
    problems = {p.field: p for p in check_fragments(spark, config, "example_realtime")}
    assert set(problems) == {"params.open_when", "cause_inputs.filter_rules[0].condition"}
    assert problems["params.open_when"].file.endswith("one_open_row_per_coverage.yaml")
    assert "ParseException" in problems["cause_inputs.filter_rules[0].condition"].problem
