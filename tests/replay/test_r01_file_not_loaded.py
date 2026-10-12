"""R01: a landed file has no raw rows; later files did load. T1_FILES_NOT_LOADED FAILED, cause
PASSED_OVER (not proven) without probe inputs; SKIPPED_BEHIND_CURSOR when the partition cursor
file is configured and names a later partition than the file's folder.

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
from tests.fixtures.landing import land_sequence_files
from tests.replay.conftest import causes, latest, replay_conf

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


def land(spark, landing: Path, *names: str) -> None:
    """Real (synthetic) sequence files; a name may include a partition folder."""
    for name in names:
        land_sequence_files(spark, (landing / name).parent, Path(name).name)


def seen_earlier(spark, replay, landing: Path, hours_ago: float) -> None:
    """Register what is landed now as first seen hours_ago (as an earlier run would have)."""
    when = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    register_files(spark, replay.dq, "example_realtime", list_landing(spark, [str(landing)]), new_run(now=when), when)


def test_r01_landed_file_without_raw_rows_while_later_files_loaded(spark, tmp_path) -> None:
    landing = tmp_path / "landing"
    replay = setup(spark, tmp_path, "r01", landing)
    land(spark, landing, "rt_0001.seq", "rt_0002.seq", "rt_0003.seq")
    seen_earlier(spark, replay, landing, hours_ago=48)
    land(spark, landing, "rt_0004.seq")  # arrives now: not yet past its SLA, not judged
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
    # Cause: no probe inputs are configured in the sample feed, so not proven.
    [(ref, line)] = causes(spark, replay, *CHECK, "FILE_CYCLIC").items()
    assert Path(ref).name == "rt_0001.seq"
    assert (line.code, line.proven) == ("PASSED_OVER", False)
    assert line.ruled_out == ("PIPELINE_STALLED", "INCOMPLETE_AT_LOAD", "UNREADABLE")  # later files loaded
    assert line.not_ready == ("RAW_LOAD_HELD", "SKIPPED_BEHIND_CURSOR", "LOAD_ERROR")


def test_r01_with_the_partition_cursor_configured_is_skipped_behind_cursor(spark, tmp_path) -> None:
    landing = tmp_path / "landing"
    replay = setup(spark, tmp_path, "r01_cursor", landing)
    cursor = tmp_path / "ctl" / "cursor.prm"
    cursor.parent.mkdir()
    cursor.write_text("last_partition=20261002\n", encoding="utf-8")
    feed = replay.conf / FEED
    feed.write_text(feed.read_text(encoding="utf-8").replace(
        "partition_cursor: { path: null, extract_regex: null, format: null, compare_to: partition }",
        f"partition_cursor: {{ path: {cursor}, extract_regex: '(\\d{{8}})', format: yyyyMMdd, compare_to: partition }}",
    ), encoding="utf-8")
    # The loader moved on to 2026-10-02's folder; 2026-10-01's file was left behind.
    land(spark, landing, "20261001/rt_0001.seq", "20261002/rt_0002.seq", "20261002/rt_0003.seq")
    seen_earlier(spark, replay, landing, hours_ago=48)
    create_table(spark, "r01_cursor_raw.enrollment", RAW_COLUMNS,
                 [raw_row("rt_0002.seq"), raw_row("rt_0003.seq", sub_id="123402")])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    [result] = latest(spark, replay)[CHECK]
    assert (result.state, result.violations) == ("FAILED", 1)
    [line] = causes(spark, replay, *CHECK, "FILE_CYCLIC").values()
    assert (line.code, line.proven) == ("SKIPPED_BEHIND_CURSOR", True)


def test_files_not_loaded_passes_when_every_file_reached_raw(spark, tmp_path) -> None:
    landing = tmp_path / "landing"
    replay = setup(spark, tmp_path, "r01_pass", landing)
    land(spark, landing, "rt_0001.seq", "rt_0002.seq")
    seen_earlier(spark, replay, landing, hours_ago=48)
    create_table(spark, "r01_pass_raw.enrollment", RAW_COLUMNS, [raw_row("rt_0001.seq"), raw_row("rt_0002.seq")])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    [result] = latest(spark, replay)[CHECK]
    assert (result.state, result.population, result.violations) == ("PASSED", 2, 0)


def test_files_not_loaded_did_not_run_when_no_file_is_old_enough(spark, tmp_path) -> None:
    landing = tmp_path / "landing"
    replay = setup(spark, tmp_path, "r01_new", landing)
    land(spark, landing, "rt_0001.seq")  # first seen by this run: within its SLA
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
