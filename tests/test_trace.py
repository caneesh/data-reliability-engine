"""dre trace beyond R05 (tests/replay): a missing key, no gap, no secret, and bad input."""

from __future__ import annotations

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_row
from tests.replay.conftest import replay_conf

CURATED_LOAD_TIME = ("load_time: null ", "load_time: { column: sourcelastupdatets, granularity: minute } ")
KEY = ["--dataset", "gold_member_coverage", "--key", "000123401,01,2026-01-01,AGR-A"]


def trace(replay, capsys, key=KEY) -> tuple[int, list[str]]:
    capsys.readouterr()
    code = main(["trace", "--conf", str(replay.conf), *key])
    return code, capsys.readouterr().out.splitlines()


def test_a_key_missing_downstream_and_no_secret(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "trace_missing",
                         {"datasets/example_curated_enrollment.yaml": [CURATED_LOAD_TIME]})
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(updated=curated_load_time(12))])
    create_table(spark, replay.gold, GOLD_COLUMNS)
    code, lines = trace(replay, capsys)
    assert code == 0
    assert lines[1] == f"gold_member_coverage ({replay.gold}): absent"
    assert lines[2:] == [
        "first gap: example_curated_enrollment -> gold_member_coverage: gold_member_coverage does not have the key "
        "(MISSING)",
        "cause: not available (hmac_secret_file is not set, so causes are not kept per key)"]


def test_no_gap_when_every_layer_has_the_latest_version(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "trace_current")
    at = curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(updated=at)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(record=at)])
    code, lines = trace(replay, capsys)
    assert code == 0 and lines[-1] == "no gap: every layer that has the key holds its latest version"
    assert len(lines) == 3  # curated, gold, the verdict


def test_bad_input(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "trace_bad")
    code, lines = trace(replay, capsys, ["--dataset", "gold_member_coverage", "--key", "000123401,01"])
    assert code == 3 and "4-part key" in lines[0]
    code, lines = trace(replay, capsys, ["--dataset", "nope", "--key", "x"])
    assert code == 3 and lines == ["dre trace: unknown dataset 'nope'"]
