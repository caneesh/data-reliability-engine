"""R02, R03 and R08: the check and its cause (R08 needs none)."""

from __future__ import annotations

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.fixtures.landing import land_sequence_files
from tests.fixtures.layers import RAW_COLUMNS, raw_dataset_yaml, raw_row
from tests.replay.conftest import causes, latest, replay_conf

FEED = "feeds/example_realtime.yaml"

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


def _stalled(spark, tmp_path, name: str, hold_marker: bool):
    """Gold has not loaded for three days; files keep landing but none reached raw since."""
    landing = tmp_path / "landing"
    edits = [("roots: [/data/landing/example_feed]", f"roots: [{landing}]"),
             ("datasets: [", "datasets: [example_raw_enrollment, ")]
    if hold_marker:
        marker = tmp_path / "ctl" / "hold.flag"
        marker.parent.mkdir()
        marker.write_text("", encoding="utf-8")
        edits.append(("load_hold_marker: { path: null }", f"load_hold_marker: {{ path: {marker} }}"))
    replay = replay_conf(spark, tmp_path, name, {FEED: edits})
    (replay.conf / "datasets" / "example_raw_enrollment.yaml").write_text(
        raw_dataset_yaml(f"{name}_raw.enrollment"), encoding="utf-8")
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(loaded=gold_load_time(72))])
    create_table(spark, replay.curated, CURATED_COLUMNS)
    create_table(spark, f"{name}_raw.enrollment", RAW_COLUMNS, [raw_row("rt_0001.seq")])  # an old file only
    land_sequence_files(spark, landing, "rt_0002.seq", "rt_0003.seq")  # first seen by this run
    assert main(["run", "--conf", str(replay.conf)]) == 0
    [on_time] = latest(spark, replay)[("gold_member_coverage", "T1_ON_TIME")]
    assert on_time.state == "FAILED"
    return {line for line in causes(spark, replay, "gold_member_coverage", "T1_ON_TIME", "FILE_CYCLIC").values()}


def test_r02_no_loads_while_files_keep_landing_is_pipeline_stalled(spark, tmp_path) -> None:
    lines = _stalled(spark, tmp_path, "r02_stalled", hold_marker=False)
    assert {(line.code, line.proven) for line in lines} == {("PIPELINE_STALLED", True)}


def test_r02_with_the_load_hold_marker_present_is_raw_load_held(spark, tmp_path) -> None:
    lines = _stalled(spark, tmp_path, "r02_held", hold_marker=True)
    assert {(line.code, line.proven) for line in lines} == {("RAW_LOAD_HELD", True)}


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
    # Cause: upstream had rows, and no cursor is configured: not proven.
    lines = set(causes(spark, replay, "gold_member_coverage", "T1_ZERO_ROWS", "FILE_CYCLIC").values())
    assert {(line.code, line.proven, line.ruled_out, line.not_ready) for line in lines} == {
        ("EMPTY_LOAD", False, ("NO_UPSTREAM_DATA",), ("WRONG_PARTITION",))}


def test_r03_rows_went_to_the_cursor_s_later_partition_is_wrong_partition(spark, tmp_path) -> None:
    from datetime import datetime, timedelta, timezone
    from zoneinfo import ZoneInfo

    cursor = tmp_path / "ctl" / "cursor.prm"
    cursor.parent.mkdir()
    # The handoff file names tomorrow's partition, so today's loads went there.
    tomorrow = (datetime.now(timezone.utc).astimezone(ZoneInfo("America/Chicago")) + timedelta(days=1))
    cursor.write_text(f"partition={tomorrow:%Y%m%d}\n", encoding="utf-8")
    replay = replay_conf(spark, tmp_path, "r03_cursor", {CURATED: [CURATED_LOAD_TIME], FEED: [(
        "partition_cursor: { path: null, extract_regex: null, format: null, compare_to: partition }",
        f"partition_cursor: {{ path: {cursor}, extract_regex: '(\\d{{8}})', format: yyyyMMdd, compare_to: partition }}",
    )]})
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(loaded=gold_load_time(72))])
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(sub_id=f"1234{h:02d}", updated=curated_load_time(h)) for h in range(1, 40)])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    [gold] = latest(spark, replay)[("gold_member_coverage", "T1_ZERO_ROWS")]
    assert gold.state == "FAILED"
    lines = set(causes(spark, replay, "gold_member_coverage", "T1_ZERO_ROWS", "FILE_CYCLIC").values())
    assert {(line.code, line.proven) for line in lines} == {("WRONG_PARTITION", True)}


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
