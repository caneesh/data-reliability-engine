"""Acceptance: a synthetic three-layer pipeline, raw -> curated -> gold, through `dre run`.

Architecture review (October 2026), acceptance criterion 2: missing data is found at the
earliest layer where it is observable, across three stages. With, in one run:

- key 1: reached every layer;
- key 2: lost between raw and curated: flagged at raw->curated only (gold never expected it);
- key 3: lost between curated and gold: flagged at curated->gold;
- key 4: a test member that a business rule keeps out of gold (hop_exclusions): not flagged,
  but counted as excluded;
- key 5: arrived late (loaded upstream within sla_hours): not judged yet, counted as pending.

`dre trace` then names the first gap of each lost key.
"""

from __future__ import annotations

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, RAW_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time,
    gold_row, raw_dataset_yaml, raw_row,
)
from tests.replay.conftest import latest
from tests.replay.test_r04_to_r07_hops import CURATED, setup

GOLD = "datasets/gold_member_coverage.yaml"
KEYS = {1: "123401", 2: "123402", 3: "123403", 4: "900004", 5: "123405"}


def build(spark, tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    replay = setup(spark, tmp_path, "accept3", {
        "feeds/example_realtime.yaml": [("roots: [/data/landing/example_feed]", f"roots: [{landing}]"),
                                        ("datasets: [", "datasets: [example_raw_enrollment, ")],
        CURATED: [("upstream: []", "upstream: [example_raw_enrollment]"),
                  ("file_name_column: null", "file_name_column: src_file_nm"),
                  ("key_unique: false", "key_unique: false\nkey_map:\n  example_raw_enrollment:\n"
                   "    subscriberidnumber: subscriberidnumber\n    membernumber: membernumber\n"
                   "    effectivedate: effectivedate\n    qualifiedhealthplanid: qualifiedhealthplanid")],
        GOLD: [("winner_rule:", "hop_exclusions:\n  example_curated_enrollment: \"subscriberidnumber LIKE '9%'\"\n"
                                "winner_rule:")],
    })
    raw_yaml = raw_dataset_yaml("accept3_raw.enrollment").replace(
        "key: [subscriberidnumber, membernumber, effectivedate]",
        "key: [subscriberidnumber, membernumber, effectivedate, qualifiedhealthplanid]",
    ) + ("key_normalise: { effectivedate: { parse_date: MM/dd/yyyy } }\n"
         "record_time: { column: msg_ts }\nload_time: { column: msg_ts, granularity: minute }\n")
    (replay.conf / "datasets" / "example_raw_enrollment.yaml").write_text(raw_yaml, encoding="utf-8")

    on_time, late = curated_load_time(12), curated_load_time(2)  # late: younger than sla_hours (8)
    raw_columns = [*RAW_COLUMNS, ("qualifiedhealthplanid", "STRING"), ("msg_ts", "TIMESTAMP")]
    create_table(spark, "accept3_raw.enrollment", raw_columns, [
        {**raw_row("rt_0001.seq", sub_id=KEYS[n]), "qualifiedhealthplanid": "AGR-A",
         "msg_ts": late if n == 5 else on_time} for n in KEYS])
    curated_keys = [1, 3, 4, 5]  # key 2 never reached curated
    create_table(spark, replay.curated, [*CURATED_COLUMNS, ("src_file_nm", "STRING")], [
        {**curated_row(sub_id=KEYS[n], updated=late if n == 5 else on_time), "src_file_nm": "rt_0001.seq"}
        for n in curated_keys])
    # Gold has key 1 only: key 3 was lost, key 4 is excluded by contract, key 5 is not due yet.
    create_table(spark, replay.gold, GOLD_COLUMNS,
                 [gold_row(sub_id="000" + KEYS[1], record=on_time, loaded=gold_load_time(10))])
    return replay


def test_missing_data_is_found_at_the_earliest_layer_across_three_stages(spark, tmp_path, capsys) -> None:
    replay = build(spark, tmp_path)
    assert main(["run", "--conf", str(replay.conf)]) == 0
    results = latest(spark, replay)

    [raw_to_curated] = results[("example_curated_enrollment", "HOP_KEY_CURRENCY")]
    assert raw_to_curated.group_values == {"upstream": "example_raw_enrollment"}
    assert (raw_to_curated.state, raw_to_curated.population, raw_to_curated.violations) == ("FAILED", 4, 1)
    assert raw_to_curated.observed == "1 missing and 0 stale of 4 keys; 1 key pending (within sla_hours)"

    [curated_to_gold] = results[("gold_member_coverage", "HOP_KEY_CURRENCY")]
    assert curated_to_gold.group_values == {"upstream": "example_curated_enrollment"}
    # Keys 1 and 3 judged; key 2 is not in curated, so gold never expected it.
    assert (curated_to_gold.state, curated_to_gold.population, curated_to_gold.violations) == ("FAILED", 2, 1)
    assert curated_to_gold.observed == ("1 missing and 0 stale of 2 keys; "
                                        "1 key excluded by hop_exclusions, 1 key pending (within sla_hours)")

    # The earliest observable gap, key by key.
    capsys.readouterr()
    for n, gap in ((2, "example_raw_enrollment -> example_curated_enrollment"),
                   (3, "example_curated_enrollment -> gold_member_coverage")):
        assert main(["trace", "--conf", str(replay.conf), "--dataset", "gold_member_coverage",
                     "--key", f"000{KEYS[n]},01,2026-01-01,AGR-A"]) == 0
        lines = capsys.readouterr().out.splitlines()
        assert len(lines) == 5  # raw, curated, gold, the first gap, its cause line
        assert lines[3].startswith(f"first gap: {gap}:") and lines[3].endswith("(MISSING)")
