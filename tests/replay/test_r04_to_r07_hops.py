"""R04 to R07 at check level (build step 7): hop checks through dre run. Causes come in step 8.

Each scenario confirms curated's load time (spec section 11 leaves it open in the sample
config) and gives the run an HMAC secret, so key events are written.
"""

from __future__ import annotations

import json
from pathlib import Path

from hcsc.datalake.dre.checks.hop.common import key_hash
from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, RAW_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time,
    gold_row, raw_dataset_yaml, raw_row,
)
from tests.replay.conftest import latest, replay_conf

CURATED = "datasets/example_curated_enrollment.yaml"
CURATED_LOAD_TIME = ("load_time: null ", "load_time: { column: sourcelastupdatets, granularity: minute } ")
SECRET = b"synthetic-replay-secret"
KEY = "123401|01|2026-01-01|AGR-A"  # curated_row()'s key, and gold_row()'s once sub_id is normalised


def setup(spark, tmp_path: Path, name: str, extra: dict | None = None):
    secret = tmp_path / "hmac.secret"
    secret.write_bytes(SECRET + b"\n")
    edits = {"defaults.yaml": [("hmac_secret_file: null", f"hmac_secret_file: {secret}")],
             CURATED: [CURATED_LOAD_TIME]}
    for rel, pairs in (extra or {}).items():
        edits[rel] = edits.get(rel, []) + pairs
    return replay_conf(spark, tmp_path, name, edits)


def hop(spark, replay, check: str):
    [result] = latest(spark, replay)[("gold_member_coverage", check)]
    assert result.group_values == {"upstream": "example_curated_enrollment"}
    return result


def key_events(spark, replay) -> list[tuple[str, str, str]]:
    return sorted((r.key_hash, r.event, r.state_detail)
                  for r in spark.table(f"{replay.dq}.dq_key_event").collect())


def test_r04_a_raw_message_missing_from_curated_is_file_completeness_failed(spark, tmp_path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    replay = setup(spark, tmp_path, "r04", {
        "feeds/example_realtime.yaml": [("roots: [/data/landing/example_feed]", f"roots: [{landing}]"),
                                        ("datasets: [", "datasets: [example_raw_enrollment, ")],
        CURATED: [("upstream: []", "upstream: [example_raw_enrollment]"),
                  ("file_name_column: null", "file_name_column: src_file_nm"),
                  ("key_unique: false", "key_unique: false\nkey_map:\n  example_raw_enrollment:\n"
                   "    subscriberidnumber: subscriberidnumber\n    membernumber: membernumber\n"
                   "    effectivedate: effectivedate\n    qualifiedhealthplanid: qualifiedhealthplanid")],
    })
    # A synthetic raw dataset (test-only columns): dates MM/dd/yyyy, read as ISO dates to match curated.
    raw_yaml = raw_dataset_yaml("r04_raw.enrollment").replace(
        "key: [subscriberidnumber, membernumber, effectivedate]",
        "key: [subscriberidnumber, membernumber, effectivedate, qualifiedhealthplanid]",
    ) + ("key_normalise: { effectivedate: { parse_date: MM/dd/yyyy } }\n"
         "record_time: { column: msg_ts }\nload_time: { column: msg_ts, granularity: minute }\n")
    (replay.conf / "datasets" / "example_raw_enrollment.yaml").write_text(raw_yaml, encoding="utf-8")

    loaded = curated_load_time(12)
    raw_columns = [*RAW_COLUMNS, ("qualifiedhealthplanid", "STRING"), ("msg_ts", "TIMESTAMP")]
    create_table(spark, "r04_raw.enrollment", raw_columns, [
        {**raw_row("rt_0001.seq", sub_id=s), "qualifiedhealthplanid": "AGR-A", "msg_ts": loaded}
        for s in ("123401", "123402", "123403")
    ] + [{**raw_row("batch_0001.dat", sub_id="9"), "qualifiedhealthplanid": "AGR-A", "msg_ts": loaded}])  # filtered
    # Two of rt_0001's three messages reached curated; the file's other rows loaded.
    create_table(spark, replay.curated, [*CURATED_COLUMNS, ("src_file_nm", "STRING")], [
        {**curated_row(sub_id=s, updated=loaded), "src_file_nm": "rt_0001.seq"} for s in ("123401", "123402")])
    create_table(spark, replay.gold, GOLD_COLUMNS)

    assert main(["run", "--conf", str(replay.conf)]) == 0
    [result] = latest(spark, replay)[("example_curated_enrollment", "HOP_FILE_COMPLETENESS")]
    assert (result.state, result.population, result.violations) == ("FAILED", 1, 1)
    assert result.group_values == {"upstream": "example_raw_enrollment"}
    assert json.loads(result.detail) == {"short_files": [
        {"file": "rt_0001.seq", "upstream_rows": 3, "rows_here": 2}]}
    # The same hop, key by key: the missing message's key is MISSING in curated.
    [currency] = latest(spark, replay)[("example_curated_enrollment", "HOP_KEY_CURRENCY")]
    assert (currency.state, currency.population, currency.violations) == ("FAILED", 3, 1)


def test_r05_curated_latest_is_a_termination_gold_older_is_key_currency_stale(spark, tmp_path) -> None:
    replay = setup(spark, tmp_path, "r05")
    opened, terminated = curated_load_time(30), curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(updated=opened), curated_row(end="2026-06-30", updated=terminated)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(record=opened, loaded=gold_load_time(29))])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    currency = hop(spark, replay, "HOP_KEY_CURRENCY")
    assert (currency.state, currency.population, currency.violations) == ("FAILED", 1, 1)
    assert currency.observed == "0 missing and 1 stale of 1 keys"
    assert key_events(spark, replay) == [(key_hash(SECRET, KEY), "FLAGGED", "STALE")]
    [open_key] = spark.table(f"{replay.dq}.v_open_keys").collect()
    assert (open_key.dataset, open_key.check_id) == ("gold_member_coverage", "HOP_KEY_CURRENCY")
    # A stale key is not CURRENT, so value agreement has nothing to compare.
    assert hop(spark, replay, "HOP_VALUE_AGREEMENT").reason_code == "empty_population"


def test_r06_tied_open_and_terminated_rows_is_value_agreement_failed(spark, tmp_path) -> None:
    replay = setup(spark, tmp_path, "r06")
    tied = curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(updated=tied), curated_row(end="2026-06-30", updated=tied)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(record=tied, loaded=gold_load_time(11))])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    agreement = hop(spark, replay, "HOP_VALUE_AGREEMENT")
    assert (agreement.state, agreement.population, agreement.violations) == ("FAILED", 1, 1)
    currency = hop(spark, replay, "HOP_KEY_CURRENCY")
    assert (currency.state, currency.violations) == ("PASSED", 0)  # gold has the latest record time
    assert key_events(spark, replay) == []


def test_r07_older_version_loaded_after_a_newer_one_reached_curated_is_stale(spark, tmp_path) -> None:
    replay = setup(spark, tmp_path, "r07")
    older, newer = curated_load_time(30), curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(updated=older), curated_row(end="2026-06-30", updated=newer)])
    # Gold wrote the older version two hours ago, after the newer one reached curated.
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(record=older, loaded=gold_load_time(2))])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    currency = hop(spark, replay, "HOP_KEY_CURRENCY")
    assert (currency.state, currency.population, currency.violations) == ("FAILED", 1, 1)
    assert currency.observed == "0 missing and 1 stale of 1 keys"
    assert key_events(spark, replay) == [(key_hash(SECRET, KEY), "FLAGGED", "STALE")]


def test_hops_pass_when_gold_is_current_and_agrees(spark, tmp_path) -> None:
    replay = setup(spark, tmp_path, "r05_pass")
    latest_version = curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(updated=curated_load_time(30)), curated_row(end="2026-06-30", updated=latest_version)])
    create_table(spark, replay.gold, GOLD_COLUMNS,
                 [gold_row(end="2026-06-30", record=latest_version, loaded=gold_load_time(11))])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    assert hop(spark, replay, "HOP_KEY_CURRENCY").state == "PASSED"
    assert hop(spark, replay, "HOP_VALUE_AGREEMENT").state == "PASSED"
    assert key_events(spark, replay) == []
