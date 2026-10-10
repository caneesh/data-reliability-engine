"""R01: a landed file has no raw rows; later files did load. T1_FILES_NOT_LOADED FAILED
(check level; the cause, PASSED_OVER or SKIPPED_BEHIND_CURSOR, comes in step 8).

Also the check's PASS and DID_NOT_RUN paths, through dre run.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from hcsc.datalake.dre.cli import main
from hcsc.datalake.dre.sources.hdfs import list_landing
from hcsc.datalake.dre.store.files import register_files
from hcsc.datalake.dre.store.runs import new_run
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, RAW_COLUMNS, create_table, raw_dataset_yaml, raw_row,
)
from tests.replay.conftest import latest, replay_conf

FEED = "feeds/example_realtime.yaml"
CHECK = ("example_raw_enrollment", "T1_FILES_NOT_LOADED")


def setup(spark, tmp_path: Path, name: str, landing: Path):
    replay = replay_conf(spark, tmp_path, name, {FEED: [
        ("roots: [/data/landing/example_feed]", f"roots: [{landing}]"),
        ("datasets: [", "datasets: [example_raw_enrollment, "),
    ]})
    (replay.conf / "datasets" / "example_raw_enrollment.yaml").write_text(
        raw_dataset_yaml(f"{name}_raw.enrollment"), encoding="utf-8")
    create_table(spark, replay.gold, GOLD_COLUMNS)
    create_table(spark, replay.curated, CURATED_COLUMNS)
    return replay


def land(landing: Path, *names: str) -> None:
    landing.mkdir(parents=True, exist_ok=True)
    for name in names:
        (landing / name).write_bytes(b"SEQ synthetic")


def seen_earlier(spark, replay, landing: Path, hours_ago: float) -> None:
    """Register what is landed now as first seen hours_ago (as an earlier run would have)."""
    when = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    register_files(spark, replay.dq, "example_realtime", list_landing(spark, [str(landing)]), new_run(now=when), when)


def test_r01_landed_file_without_raw_rows_while_later_files_loaded(spark, tmp_path) -> None:
    landing = tmp_path / "landing"
    replay = setup(spark, tmp_path, "r01", landing)
    land(landing, "rt_0001.seq", "rt_0002.seq", "rt_0003.seq")
    seen_earlier(spark, replay, landing, hours_ago=48)
    land(landing, "rt_0004.seq")  # arrives now: not yet past its SLA, not judged
    # rt_0001 never reached raw; the later rt_0002 and rt_0003 did. The batch feed's file shares the table.
    create_table(spark, "r01_raw.enrollment", RAW_COLUMNS,
                 [raw_row("rt_0002.seq"), raw_row("rt_0003.seq", sub_id="123402"), raw_row("batch_0001.dat")])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    [result] = latest(spark, replay)[CHECK]
    assert (result.state, result.population, result.violations) == ("FAILED", 3, 1)
    detail = json.loads(result.detail)
    assert [Path(p).name for p in detail["not_loaded"]] == ["rt_0001.seq"]
    # The run registered the new file; the three earlier ones kept their first-seen time.
    files = {Path(r.path).name: r for r in spark.table(f"{replay.dq}.v_file_status").collect()}
    assert sorted(files) == ["rt_0001.seq", "rt_0002.seq", "rt_0003.seq", "rt_0004.seq"]
    assert files["rt_0001.seq"].first_seen_at < files["rt_0004.seq"].first_seen_at


def test_files_not_loaded_passes_when_every_file_reached_raw(spark, tmp_path) -> None:
    landing = tmp_path / "landing"
    replay = setup(spark, tmp_path, "r01_pass", landing)
    land(landing, "rt_0001.seq", "rt_0002.seq")
    seen_earlier(spark, replay, landing, hours_ago=48)
    create_table(spark, "r01_pass_raw.enrollment", RAW_COLUMNS, [raw_row("rt_0001.seq"), raw_row("rt_0002.seq")])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    [result] = latest(spark, replay)[CHECK]
    assert (result.state, result.population, result.violations) == ("PASSED", 2, 0)


def test_files_not_loaded_did_not_run_when_no_file_is_old_enough(spark, tmp_path) -> None:
    landing = tmp_path / "landing"
    replay = setup(spark, tmp_path, "r01_new", landing)
    land(landing, "rt_0001.seq")  # first seen by this run: within its SLA
    create_table(spark, "r01_new_raw.enrollment", RAW_COLUMNS)

    assert main(["run", "--conf", str(replay.conf)]) == 0
    [result] = latest(spark, replay)[CHECK]
    assert (result.state, result.reason_code) == ("DID_NOT_RUN", "empty_population")


def test_files_not_loaded_did_not_run_when_landing_is_unreadable(spark, tmp_path, capsys) -> None:
    replay = setup(spark, tmp_path, "r01_gone", tmp_path / "missing_landing")
    create_table(spark, "r01_gone_raw.enrollment", RAW_COLUMNS)

    assert main(["run", "--conf", str(replay.conf)]) == 0  # the run continues
    [result] = latest(spark, replay)[CHECK]
    assert (result.state, result.reason_category, result.reason_code) == (
        "DID_NOT_RUN", "DATA_UNAVAILABLE", "landing_unreadable")
    assert "missing_landing" not in capsys.readouterr().out  # paths stay in the dq store, not the console


def test_files_not_loaded_not_planned_without_a_raw_dataset(spark, tmp_path) -> None:
    from hcsc.datalake.dre.config.validate import validate_conf
    from hcsc.datalake.dre.runner import plan

    replay = replay_conf(spark, tmp_path, "r01_noraw")
    config, errors, _ = validate_conf(replay.conf)
    assert errors == []
    planned = [p for p in plan(config) if p.check.check_id == "T1_FILES_NOT_LOADED"]
    # Only the second synthetic feed has a raw dataset with a file_name_column.
    assert [(p.feed.feed, p.dataset.dataset) for p in planned] == [("provider_roster_monthly", "provider_roster_raw")]
