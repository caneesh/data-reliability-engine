"""Replay scenarios on the second synthetic feed, which looks nothing like the first: a monthly
CSV (FILE_PERIODIC, flat folder, no partitions, UTC, single-column key) and the TABLE_MERGE gold
table built from it. Applicable scenarios: R01, R02, R03, R08, R09, R11a, R11b, and the hop checks
(R05's missing key and R06's disagreement) with a rule, at check level.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from hcsc.datalake.dre.checks.cadence import slots
from hcsc.datalake.dre.cli import main
from hcsc.datalake.dre.config.loader import load
from hcsc.datalake.dre.sources.hdfs import list_landing
from hcsc.datalake.dre.store.files import register_files
from hcsc.datalake.dre.store.runs import new_run
from tests.fixtures.layers import (
    PROVIDER_GOLD_COLUMNS, PROVIDER_RAW_COLUMNS, create_table, provider_gold_row, provider_raw_row,
)
from tests.replay.conftest import causes, latest, replay_conf

UTC = timezone.utc
RAW_FEED, GOLD_FEED = "provider_roster_monthly", "provider_directory_merge"
RAW_FILE, GOLD_FILE = "feeds/provider_roster_monthly.yaml", "feeds/provider_directory_merge.yaml"
RAW, GOLD = "provider_roster_raw", "provider_directory"


def judged_slot(conf: Path, feed_id: str) -> datetime:
    """The latest monthly slot whose deadline (slot + SLA) a run now has passed."""
    config, _ = load(conf)
    feed = config.feeds[feed_id]
    end = datetime.now(UTC) - timedelta(minutes=15)
    sla = timedelta(hours=feed.sla_hours)
    return slots(feed.cadence, end - timedelta(days=40) - sla, end - sla)[-1]


def setup(spark, tmp_path: Path, name: str, edits=None, landing: Path | None = None):
    landing = landing or tmp_path / "landing"
    landing.mkdir(parents=True, exist_ok=True)
    edits = {rel: list(pairs) for rel, pairs in (edits or {}).items()}
    edits.setdefault(RAW_FILE, []).append(("roots: [/data/landing/provider_roster]", f"roots: [{landing}]"))
    return replay_conf(spark, tmp_path, name, edits), landing


def run_feed(replay, feed_id: str) -> int:
    return main(["run", "--conf", str(replay.conf), "--feed", feed_id])


def test_r01_monthly_csv_not_loaded_while_a_later_file_loaded(spark, tmp_path) -> None:
    replay, landing = setup(spark, tmp_path, "p_r01")
    for name in ("roster_2026_06.csv", "roster_2026_07.csv", "notes.txt"):
        (landing / name).write_text("provider_id,provider_name\n", encoding="utf-8")
    when = datetime.now(UTC) - timedelta(days=60)
    register_files(spark, replay.dq, RAW_FEED, list_landing(spark, [str(landing)], "*.csv"), new_run(now=when), when)
    loaded = datetime.now(UTC) - timedelta(days=30)
    create_table(spark, replay.provider_raw, PROVIDER_RAW_COLUMNS,
                 [provider_raw_row("P001", loaded, "roster_2026_07.csv")])  # June's file never loaded

    assert run_feed(replay, RAW_FEED) == 0
    [result] = latest(spark, replay)[(RAW, "T1_FILES_NOT_LOADED")]
    assert (result.state, result.population, result.violations) == ("FAILED", 2, 1)
    assert [Path(p).name for p in json.loads(result.detail)["not_loaded"]] == ["roster_2026_06.csv"]
    # Cause: July's file loaded, the CSV header reads, its size never changed, and the job log
    # configured for this feed is not there: not proven.
    [line] = causes(spark, replay, RAW, "T1_FILES_NOT_LOADED", "FILE_PERIODIC").values()
    assert (line.code, line.proven) == ("PASSED_OVER", False)
    assert line.ruled_out == ("PIPELINE_STALLED", "INCOMPLETE_AT_LOAD", "UNREADABLE", "LOAD_ERROR")
    assert line.not_ready == ("RAW_LOAD_HELD", "SKIPPED_BEHIND_CURSOR")


def test_r01_monthly_csv_with_an_error_in_the_job_log_is_load_error(spark, tmp_path) -> None:
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "loader_20260701.log").write_text(
        "INFO starting roster load\nERROR rejected roster_2026_06.csv: bad header\nINFO done\n", encoding="utf-8")
    (logs / "loader_20260801.log").write_text("ERROR something else entirely\n", encoding="utf-8")
    replay, landing = setup(spark, tmp_path, "p_r01_log", {RAW_FILE: [
        ("path_glob: /data/logs/provider_roster/*.log", f"path_glob: {logs}/*.log")]})
    for name in ("roster_2026_06.csv", "roster_2026_07.csv"):
        (landing / name).write_text("provider_id,provider_name\n", encoding="utf-8")
    when = datetime.now(UTC) - timedelta(days=60)
    register_files(spark, replay.dq, RAW_FEED, list_landing(spark, [str(landing)], "*.csv"), new_run(now=when), when)
    create_table(spark, replay.provider_raw, PROVIDER_RAW_COLUMNS,
                 [provider_raw_row("P001", datetime.now(UTC) - timedelta(days=30), "roster_2026_07.csv")])

    assert run_feed(replay, RAW_FEED) == 0
    [line] = causes(spark, replay, RAW, "T1_FILES_NOT_LOADED", "FILE_PERIODIC").values()
    assert (line.code, line.proven) == ("LOAD_ERROR", True)


def test_r02_monthly_file_late_is_on_time_failed(spark, tmp_path) -> None:
    replay, _ = setup(spark, tmp_path, "p_r02")
    slot = judged_slot(replay.conf, RAW_FEED)
    create_table(spark, replay.provider_raw, PROVIDER_RAW_COLUMNS,
                 [provider_raw_row("P001", slot - timedelta(days=20))])  # last month's file only

    assert run_feed(replay, RAW_FEED) == 0
    [on_time] = latest(spark, replay)[(RAW, "T1_ON_TIME")]
    assert on_time.state == "FAILED" and on_time.violations == on_time.population >= 1
    # Cause: nothing landed since the slot, so nothing shows a stalled pipeline: not proven.
    lines = set(causes(spark, replay, RAW, "T1_ON_TIME", "FILE_PERIODIC").values())
    assert {(line.code, line.proven) for line in lines} == {("PASSED_OVER", False)}
    assert all("PIPELINE_STALLED" in line.ruled_out for line in lines)


def test_r02_monthly_file_landed_but_never_loaded_is_pipeline_stalled(spark, tmp_path) -> None:
    replay, landing = setup(spark, tmp_path, "p_r02_stalled")
    slot = judged_slot(replay.conf, RAW_FEED)
    (landing / "roster_latest.csv").write_text("provider_id,provider_name\n", encoding="utf-8")  # seen this run
    create_table(spark, replay.provider_raw, PROVIDER_RAW_COLUMNS,
                 [provider_raw_row("P001", slot - timedelta(days=20), "roster_previous.csv")])

    assert run_feed(replay, RAW_FEED) == 0
    lines = set(causes(spark, replay, RAW, "T1_ON_TIME", "FILE_PERIODIC").values())
    assert {(line.code, line.proven) for line in lines} == {("PIPELINE_STALLED", True)}


def test_r03_merge_wrote_nothing_while_the_roster_loaded(spark, tmp_path) -> None:
    replay, _ = setup(spark, tmp_path, "p_r03")
    raw_slot, gold_slot = judged_slot(replay.conf, RAW_FEED), judged_slot(replay.conf, GOLD_FEED)
    create_table(spark, replay.provider_raw, PROVIDER_RAW_COLUMNS,
                 [provider_raw_row(f"P{i:03d}", raw_slot + timedelta(hours=1)) for i in range(5)])
    create_table(spark, replay.provider_gold, PROVIDER_GOLD_COLUMNS,
                 [provider_gold_row("P001", gold_slot - timedelta(days=20))])  # nothing merged this month

    assert run_feed(replay, RAW_FEED) == 0
    assert run_feed(replay, GOLD_FEED) == 0
    results = latest(spark, replay)
    [gold] = results[(GOLD, "T1_ZERO_ROWS")]
    assert gold.state == "FAILED" and gold.violations == gold.population >= 1
    [raw] = results[(RAW, "T1_ZERO_ROWS")]
    assert raw.state == "PASSED"  # the roster did load: the gap is at the merge
    # Cause: the roster had rows in the window; no partition cursor (flat folder): not proven.
    lines = set(causes(spark, replay, GOLD, "T1_ZERO_ROWS", "TABLE_MERGE").values())
    assert {(line.code, line.proven, line.ruled_out, line.not_ready) for line in lines} == {
        ("EMPTY_LOAD", False, ("NO_UPSTREAM_DATA",), ("WRONG_PARTITION",))}


def test_r08_duplicate_provider_in_gold(spark, tmp_path) -> None:
    replay, _ = setup(spark, tmp_path, "p_r08")
    merged = judged_slot(replay.conf, GOLD_FEED) + timedelta(hours=1)
    create_table(spark, replay.provider_gold, PROVIDER_GOLD_COLUMNS,
                 [provider_gold_row("P001", merged), provider_gold_row("P001", merged), provider_gold_row("P002", merged)])

    assert run_feed(replay, GOLD_FEED) == 0
    [dups] = latest(spark, replay)[(GOLD, "T1_KEY_DUPLICATES")]
    assert (dups.state, dups.population, dups.violations, dups.group_values) == ("FAILED", 2, 1, {})


def test_r09_renamed_key_column(spark, tmp_path) -> None:
    replay, _ = setup(spark, tmp_path, "p_r09")
    merged = judged_slot(replay.conf, GOLD_FEED) + timedelta(hours=1)
    renamed = [("provider_key" if n == "provider_id" else n, t) for n, t in PROVIDER_GOLD_COLUMNS]
    create_table(spark, replay.provider_gold, renamed, [])
    spark.createDataFrame([("P001", "Provider P001", "general", merged.replace(tzinfo=None) - timedelta(days=2),
                            merged.replace(tzinfo=None))], ", ".join(f"{n} {t}" for n, t in renamed)
                          ).write.insertInto(replay.provider_gold)

    assert run_feed(replay, GOLD_FEED) == 0
    results = latest(spark, replay)
    for check in ("T1_KEY_DUPLICATES", "T1_KEY_NULLS"):
        [r] = results[(GOLD, check)]
        assert (r.state, r.reason_code) == ("DID_NOT_RUN", "column_missing"), check
    [on_time] = results[(GOLD, "T1_ON_TIME")]
    assert on_time.state == "PASSED"  # checks that do not read the key still run


def test_r11a_no_load_due_everything_did_not_run(spark, tmp_path) -> None:
    replay, _ = setup(spark, tmp_path, "p_r11a", {RAW_FILE: [
        ("  kind: monthly\n  days_of_month: [1]", "  kind: calendar_dates\n  dates: [2020-01-01]")]})
    create_table(spark, replay.provider_raw, PROVIDER_RAW_COLUMNS)

    assert run_feed(replay, RAW_FEED) == 0
    rows = [r for (ds, _), group in latest(spark, replay).items() if ds == RAW for r in group]
    assert len(rows) == 7
    for r in rows:
        expected = "insufficient_history" if r.check_id == "T1_SCHEMA_DRIFT" else "empty_population"
        assert (r.state, r.reason_code) == ("DID_NOT_RUN", expected), r


def test_r11b_load_due_presence_checks_fail_row_checks_did_not_run(spark, tmp_path) -> None:
    replay, _ = setup(spark, tmp_path, "p_r11b")
    create_table(spark, replay.provider_raw, PROVIDER_RAW_COLUMNS)

    assert run_feed(replay, RAW_FEED) == 0
    rows = [r for (ds, _), group in latest(spark, replay).items() if ds == RAW for r in group]
    assert len(rows) == 7 and not [r for r in rows if r.state == "PASSED"]
    by_check = {r.check_id: r for r in rows}
    for check in ("T1_ON_TIME", "T1_ZERO_ROWS"):
        assert by_check[check].state == "FAILED", check
    for check in ("T1_VOLUME", "T1_KEY_NULLS", "T1_KEY_DUPLICATES", "T1_FILES_NOT_LOADED"):
        assert (by_check[check].state, by_check[check].reason_code) == ("DID_NOT_RUN", "empty_population"), check


def test_hops_on_the_merge_table_flag_missing_and_disagreeing_providers(spark, tmp_path) -> None:
    """Hop checks and a rule on the second feed: raw roster to the merged directory."""
    secret = tmp_path / "hmac.secret"
    secret.write_bytes(b"synthetic-provider-secret")
    replay, _ = setup(spark, tmp_path, "p_hop", {"defaults.yaml": [("hmac_secret_file: null", f"hmac_secret_file: {secret}")]})
    loaded = datetime.now(UTC) - timedelta(hours=30)  # past the feed's 24-hour SLA, so judged
    raw = [provider_raw_row(p, loaded) for p in ("P001", "P002", "P003")]
    create_table(spark, replay.provider_raw, PROVIDER_RAW_COLUMNS, raw)
    merged = loaded + timedelta(hours=1)
    gold = [{**provider_gold_row(p, merged), "roster_effective_ts": r["roster_effective_ts"]}
            for p, r in zip(("P001", "P002"), raw)]            # P003 never merged
    gold[1]["specialty"] = "cardiology"                         # P002 disagrees with the roster
    create_table(spark, replay.provider_gold, PROVIDER_GOLD_COLUMNS, gold)

    assert run_feed(replay, GOLD_FEED) == 0
    results = latest(spark, replay)
    [currency] = results[(GOLD, "HOP_KEY_CURRENCY")]
    assert (currency.state, currency.population, currency.violations) == ("FAILED", 3, 1)
    assert currency.observed == "1 missing and 0 stale of 3 keys"
    [agreement] = results[(GOLD, "HOP_VALUE_AGREEMENT")]
    assert (agreement.state, agreement.population, agreement.violations) == ("FAILED", 2, 1)
    assert agreement.group_values == {"upstream": RAW}
    [specialty] = results[(GOLD, "provider_specialty_present")]
    assert (specialty.state, specialty.population, specialty.violations) == ("PASSED", 2, 0)
    # Causes: the merge ran after the roster loaded, so not NOT_RUN; nothing else confirms.
    for check in ("HOP_KEY_CURRENCY", "HOP_VALUE_AGREEMENT"):
        [line] = causes(spark, replay, GOLD, check, "TABLE_MERGE").values()
        assert (line.code, line.proven) == ("MERGE_NOT_APPLIED", False), check
        assert line.ruled_out[0] == "NOT_RUN" and "TIE_RESOLVED_BY_RULE" in line.not_ready  # no winner_rule
