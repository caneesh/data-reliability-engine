"""dre run writes results and run rows; dre dry-run writes nothing and checks SQL fragments."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from hcsc.datalake.dre.checks.events import Event, previous_window_end
from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_row, gold_row
from tests.replay.conftest import replay_conf
from tests.store.conftest import append_rows

GOLD = "datasets/gold_member_coverage.yaml"


def counts(spark, dq: str) -> tuple[int, int]:
    return spark.table(f"{dq}.dq_check_result").count(), spark.table(f"{dq}.dq_run").count()


def test_run_writes_results_and_windows_follow_on(spark, tmp_path, capsys) -> None:
    # Curated has no load time in the sample config, so gold's hop checks are DID_NOT_RUN /
    # invalid_config. A CONFIGURATION reason never holds a window (spec section 4): gold's moves on.
    replay = replay_conf(spark, tmp_path, "runner_a")
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(), gold_row()])  # one duplicated key
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row()])

    assert main(["run", "--conf", str(replay.conf)]) == 0  # FAILED checks still exit 0
    out = capsys.readouterr().out
    assert "gold_member_coverage T1_KEY_DUPLICATES [src_sys_nm=SRC_A]: FAILED (population 1, violations 1)" in out
    rows = {r.dataset: r for r in spark.table(f"{replay.dq}.dq_check_result").collect()
            if r.check_id == "T1_KEY_DUPLICATES"}
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

    # Every check on a dataset in one run shares the same event (the window is fixed before any write).
    gold_run1 = [r for r in spark.table(f"{replay.dq}.dq_check_result").collect() if r.dataset == "gold_member_coverage"]
    # 12 checks (6 Tier 1, 2 hop, 4 rules); schema drift for this table runs under the table-wide dataset.
    assert len(gold_run1) == 11
    assert {(r.event_id, r.window_start, r.window_end) for r in gold_run1} == {
        (first.event_id, first.window_start, first.window_end)}

    # The next run's window starts where the first one ended.
    assert previous_window_end(spark, replay.dq, "gold_member_coverage") == end.replace(tzinfo=timezone.utc)
    # Seconds later nothing is due, so force the feed to see the next window.
    assert main(["run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0
    second = [r for r in spark.table(f"{replay.dq}.dq_check_result").collect()
              if r.run_id != run1.run_id and r.dataset == "gold_member_coverage" and r.check_id == "T1_KEY_DUPLICATES"]
    assert [r.window_start for r in second] == [end]
    assert second[0].event_id != first.event_id
    # The CONFIGURATION problem is reported again on every run.
    hops = [(r.check_id, r.reason_code) for r in spark.table(f"{replay.dq}.dq_check_result").collect()
            if r.run_id != run1.run_id and r.dataset == "gold_member_coverage" and r.check_id.startswith("HOP_")]
    assert sorted(hops) == [("HOP_KEY_CURRENCY", "invalid_config"), ("HOP_VALUE_AGREEMENT", "invalid_config")]


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
                                         "      table: curated_db.enrollment\n"
                                         "      condition: \"src = 'X' AND (\"\n      code_ref: example.sql:1")],
    })
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()])
    config, errors, _ = validate_conf(replay.conf)
    assert errors == []
    problems = {p.field: p for p in check_fragments(spark, config, "example_realtime")}
    assert set(problems) == {"params.open_when", "probes.filter_rules[0].condition"}
    assert problems["params.open_when"].file.endswith("one_open_row_per_coverage.yaml")
    assert "ParseException" in problems["probes.filter_rules[0].condition"].problem


def test_schema_drift_runs_once_per_physical_table(spark, tmp_path) -> None:
    from hcsc.datalake.dre.config.validate import validate_conf
    from hcsc.datalake.dre.runner import plan

    replay = replay_conf(spark, tmp_path, "runner_g")
    config, errors, _ = validate_conf(replay.conf)
    assert errors == []
    drift = [(p.dataset.dataset, p.dataset.table) for p in plan(config) if p.check.check_id == "T1_SCHEMA_DRIFT"]
    # Two physical tables; gold's drift runs under the table-wide dataset, not gold_member_coverage.
    assert sorted(drift) == [("example_curated_enrollment", replay.curated), ("gold_member_coverage_all", replay.gold),
                             ("provider_directory", replay.provider_gold),
                             ("provider_roster_raw", replay.provider_raw)]
    # With one feed named, the table-wide dataset is not planned, so the feed's dataset runs it.
    one_feed = [p.dataset.dataset for p in plan(config, "example_realtime") if p.check.check_id == "T1_SCHEMA_DRIFT"]
    assert sorted(one_feed) == ["example_curated_enrollment", "gold_member_coverage"]


def test_run_refuses_to_start_with_an_unreadable_or_empty_secret(spark, tmp_path, capsys) -> None:
    """Key events must not be lost quietly: a configured but unusable secret stops the run (exit 3)
    before any row is written, and the message names the setting, never the secret."""
    for name, content in (("runner_secret_missing", None), ("runner_secret_empty", b"  \n")):
        secret = tmp_path / name / "hmac.secret"
        secret.parent.mkdir()
        if content is not None:
            secret.write_bytes(content)
        replay = replay_conf(spark, tmp_path / name, name, {"defaults.yaml": [
            ("hmac_secret_file: null", f"hmac_secret_file: {secret}")]})
        assert main(["run", "--conf", str(replay.conf)]) == 3
        assert "cannot read hmac_secret_file" in capsys.readouterr().out
        assert counts(spark, replay.dq) == (0, 0)


def test_rules_are_planned_unless_retired(tmp_path) -> None:
    import shutil

    from hcsc.datalake.dre.config.loader import load
    from hcsc.datalake.dre.runner import plan_rules
    from tests.conftest import REPO_ROOT

    conf = tmp_path / "conf"
    shutil.copytree(REPO_ROOT / "conf", conf)
    rule = conf / "rules" / "one_row_per_coverage.yaml"
    rule.write_text(rule.read_text(encoding="utf-8").replace("status: proposed", "status: retired"), encoding="utf-8")
    config, _ = load(conf)
    planned = {(p.dataset.dataset, p.check.check_id, p.feed.feed if p.feed else None) for p in plan_rules(config)}
    assert ("gold_member_coverage", "older_coverage_still_open", "example_realtime") in planned
    assert ("gold_member_coverage_all", "coverage_end_date_format", None) in planned       # table-wide: no feed
    assert ("provider_directory", "provider_specialty_present", "provider_directory_merge") in planned
    assert not any(check == "one_row_per_coverage" for _, check, _ in planned)
    only = {p.check.check_id for p in plan_rules(config, "provider_directory_merge")}
    assert only == {"provider_specialty_present"}


def _held(dq_dataset: str, event: str, start: datetime, end: datetime, state: str = "PASSED",
          category: str | None = None, code: str | None = None) -> dict:
    return {"evaluation_id": f"{event}-{state}", "run_id": f"earlier-{event}", "event_id": event,
            "window_start": start, "window_end": end, "execution_type": "NORMAL", "feed": "example_realtime",
            "dataset": dq_dataset, "check_id": "T1_KEY_DUPLICATES", "state": state, "reason_category": category,
            "reason_code": code, "run_date": start.date()}


def _held_history(spark, dq: str, now: datetime, first_held_hours_ago: float) -> tuple[datetime, datetime]:
    """A good window ending 100 hours ago, then two windows held by a PLATFORM problem, the
    first ending first_held_hours_ago and the last 5 hours ago. Returns (good end, last held end)."""
    good_end, first_held_end, last_held_end = (now - timedelta(hours=h) for h in (100, first_held_hours_ago, 5))
    append_rows(spark, dq, "dq_check_result", [
        _held("gold_member_coverage", "good", good_end - timedelta(hours=1), good_end),
        _held("gold_member_coverage", "held1", good_end, first_held_end, "DID_NOT_RUN", "PLATFORM", "query_failed"),
        _held("gold_member_coverage", "held2", good_end, last_held_end, "DID_NOT_RUN", "PLATFORM", "query_failed"),
    ])
    return good_end, last_held_end


def _gold_windows(spark, dq: str) -> set[tuple]:
    return {(r.window_start, r.window_end) for r in spark.table(f"{dq}.dq_check_result").collect()
            if r.dataset == "gold_member_coverage" and r.check_id == "T1_KEY_DUPLICATES" and not r.run_id.startswith("earlier")}


def test_a_platform_hold_keeps_the_window_until_max_window_hours(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "runner_hold")
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()])
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0, tzinfo=None)
    good_end, _ = _held_history(spark, replay.dq, now, first_held_hours_ago=60)  # held for 60 hours: < 72

    assert main(["run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0
    [(start, _)] = _gold_windows(spark, replay.dq)
    assert start == good_end  # the held span is evaluated again
    assert not [r for r in spark.table(f"{replay.dq}.dq_check_result").collect() if r.check_id == "WINDOW_GAP"]


def test_a_hold_past_max_window_hours_is_released_with_a_window_gap(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "runner_gap")
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()])
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0, tzinfo=None)
    good_end, last_held_end = _held_history(spark, replay.dq, now, first_held_hours_ago=90)  # 90 hours: > 72

    assert main(["run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0
    [gap] = [r for r in spark.table(f"{replay.dq}.dq_check_result").collect() if r.check_id == "WINDOW_GAP"]
    assert (gap.state, gap.reason_category, gap.reason_code) == ("DID_NOT_RUN", "DATA_UNAVAILABLE", "window_gap")
    assert (gap.dataset, gap.feed, gap.window_start, gap.window_end) == (
        "gold_member_coverage", "example_realtime", good_end, last_held_end)
    assert json.loads(gap.detail) == {"unchecked_hours": 95.0, "held_by": ["query_failed"]}
    # The checks evaluate from where the last held window ended; the gap is not evaluated again.
    [(start, _)] = _gold_windows(spark, replay.dq)
    assert start == last_held_end
    assert previous_window_end(spark, replay.dq, "gold_member_coverage") > last_held_end.replace(tzinfo=timezone.utc)
    [run] = spark.table(f"{replay.dq}.v_latest_run").collect()
    assert run.status == "COMPLETED" and run.checks_written == run.checks_expected


def test_rules_run_by_frequency_not_every_run(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "runner_rules", {
        "rules/one_row_per_coverage.yaml": [("severity: high", "severity: high\nfrequency: every_run")]})
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()])

    def rule_runs() -> dict[str, int]:
        out: dict[str, int] = {}
        for r in spark.table(f"{replay.dq}.dq_check_result").collect():
            if r.check_id in ("one_row_per_coverage", "end_not_before_start"):
                out[r.check_id] = out.get(r.check_id, 0) + 1
        return out

    assert main(["run", "--conf", str(replay.conf)]) == 0
    assert rule_runs() == {"one_row_per_coverage": 1, "end_not_before_start": 1}
    # An hour later (forced here): the every_run rule runs again, the daily one is not due.
    assert main(["run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0
    assert rule_runs() == {"one_row_per_coverage": 2, "end_not_before_start": 1}


def test_full_sweep_day_is_counted_in_the_default_timezone() -> None:
    from hcsc.datalake.dre.config.loader import load
    from hcsc.datalake.dre.runner import is_full_sweep
    from tests.conftest import REPO_ROOT

    config, _ = load(REPO_ROOT / "conf")  # full_sweep_day SUNDAY, timezone America/Chicago
    assert is_full_sweep(config, "gold_member_coverage", datetime(2026, 1, 18, 12, tzinfo=timezone.utc))      # Sunday
    assert not is_full_sweep(config, "gold_member_coverage", datetime(2026, 1, 17, 12, tzinfo=timezone.utc))  # Saturday
    # Sunday 03:00 UTC is still Saturday evening in Chicago.
    assert not is_full_sweep(config, "gold_member_coverage", datetime(2026, 1, 18, 3, tzinfo=timezone.utc))
