"""Cause checks one by one (spec section 7): each generic probe and builtin can be CONFIRMED,
RULED_OUT and NOT_READY, and a probe that raises is ERROR. The replays (tests/replay) cover the
causes each scenario expects; these cover the other paths.

Window [2026-01-15 12:00, 2026-01-16 12:00) UTC, SLA 8 hours, as in tests/checks/test_hop.py.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from hcsc.datalake.dre.causes import landing, merge
from hcsc.datalake.dre.causes.base import CauseContext, Failure
from hcsc.datalake.dre.causes.engine import CauseLine, explain, failure_type, resolve, run_entry
from hcsc.datalake.dre.causes.probes import (
    file_exists, file_value_compare, log_contains, size_changed, table_contains,
)
from hcsc.datalake.dre.checks.base import CheckContext, CheckResult
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.hop.common import key_hash
from hcsc.datalake.dre.checks.registry import CauseEntry
from hcsc.datalake.dre.config.models import Cadence, Dataset, Feed, Landing, TimeColumn
from hcsc.datalake.dre.config.probes import FileExists, FileValueCompare, LogContains, SizeChanged, TableContains
from hcsc.datalake.dre.store.local_setup import create_store
from tests.checks.test_hop import curated, gold
from tests.fixtures.landing import land_sequence_files
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_row, gold_row
from tests.fixtures.settings import SETTINGS
from tests.store.conftest import append_rows

UTC = timezone.utc
DQ = "dq_causes"
EVENT = Event("gold", datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 1, 16, 12, tzinfo=UTC))
IN_WINDOW = datetime(2026, 1, 15, 10, 0)
SECRET = b"synthetic-cause-secret"
KEY = "123401|01|2026-01-01|AGR-A"
GOLD_LOAD = TimeColumn(column="gld_lcts", format="yyyy-MM-dd HH:mm:ss:SSSSSS", granularity="minute", timezone="UTC")


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


def feed(landing_root: str = "/data/landing/example_feed", file_format: str = "sequence", **probes) -> Feed:
    return Feed(feed="example_realtime", expectation_version=1, pattern="FILE_CYCLIC", owner="membership-gold",
                landing=Landing(roots=[landing_root], file_format=file_format),
                cadence=Cadence(kind="times", times=["00:30"], timezone="UTC"),
                datasets=["example_raw_enrollment", "gold"], probes=probes)


def context(spark, ds: Dataset, check: str, the_feed: Feed | None = None, upstream: Dataset | None = None,
            datasets: dict | None = None, secret: bytes | None = None) -> CauseContext:
    ctx = CheckContext(spark, ds, the_feed, SETTINGS, "UTC", DQ,
                       upstreams={upstream.dataset: upstream} if upstream else {}, key_secret=secret)
    return CauseContext(ctx, EVENT, SimpleNamespace(datasets=datasets or {}), check,
                        upstream.dataset if upstream else None)


# --- generic probes ---

def test_file_exists(spark, tmp_path) -> None:
    marker = tmp_path / "hold.flag"
    cx = context(spark, gold("causes.unused"), "T1_ON_TIME")
    assert file_exists(cx, FileExists(path=str(marker)), Failure("x")).state == "RULED_OUT"
    marker.write_text("", encoding="utf-8")
    assert file_exists(cx, FileExists(path=str(marker)), Failure("x")).state == "CONFIRMED"


def _cursor(tmp_path: Path, value: str) -> str:
    path = tmp_path / f"cursor_{value}.prm"
    path.write_text(f"# synthetic handoff\nlast_partition={value}\n", encoding="utf-8")
    return str(path)


def test_file_value_compare_against_the_failing_file_s_folder(spark, tmp_path) -> None:
    cx = context(spark, gold("causes.unused"), "T1_FILES_NOT_LOADED")
    failing = Failure("f", files=("file:/data/landing/example_feed/20261001/rt_0001.seq",))
    params = lambda value: FileValueCompare(path=_cursor(tmp_path, value), extract_regex=r"(\d{8})",  # noqa: E731
                                            format="yyyyMMdd", compare_to="partition")
    later = file_value_compare(cx, params("20261002"), failing)
    assert (later.state, later.evidence["value"], later.evidence["partitions"]) == ("CONFIRMED", "20261002", ["20261001"])
    assert file_value_compare(cx, params("20261001"), failing).state == "RULED_OUT"  # same partition
    assert file_value_compare(cx, params("20260930"), failing).state == "RULED_OUT"  # behind it
    # Parsed, not compared as strings: 2026-9-30 written without padding is still before 2026-10-01.
    unpadded = FileValueCompare(path=_cursor(tmp_path, "2026-9-30"), extract_regex=r"(\d{4}-\d{1,2}-\d{1,2})",
                                format="yyyy-M-d", compare_to="partition")
    padded = Failure("f", files=("file:/landing/2026-10-1/rt_0001.seq",))
    assert file_value_compare(cx, unpadded, padded).state == "RULED_OUT"


def test_file_value_compare_against_the_window(spark, tmp_path) -> None:
    cx = context(spark, gold("causes.unused"), "T1_ZERO_ROWS")
    params = lambda value: FileValueCompare(path=_cursor(tmp_path, value), extract_regex=r"(\d{8})",  # noqa: E731
                                            format="yyyyMMdd", compare_to="window")
    assert file_value_compare(cx, params("20260116"), Failure("s")).state == "RULED_OUT"   # 16th 00:00, inside
    assert file_value_compare(cx, params("20260117"), Failure("s")).state == "CONFIRMED"   # after the window


def test_file_value_compare_errors_when_the_value_cannot_be_read(spark, tmp_path) -> None:
    ds = gold("causes.unused")
    entry = CauseEntry("SKIPPED_BEHIND_CURSOR", "file_value_compare", "partition_cursor")
    failing = Failure("f", files=("file:/landing/20261001/rt_0001.seq",))
    no_value = feed(partition_cursor={"path": _cursor(tmp_path, "none"), "extract_regex": r"(\d{8})",
                                      "format": "yyyyMMdd", "compare_to": "partition"})
    outcome = run_entry(context(spark, ds, "T1_FILES_NOT_LOADED", no_value), entry, failing)
    assert (outcome.state, outcome.evidence) == ("ERROR", {"error": "ValueError"})
    missing = feed(partition_cursor={"path": str(tmp_path / "absent.prm"), "extract_regex": r"(\d{8})",
                                     "format": "yyyyMMdd", "compare_to": "partition"})
    assert run_entry(context(spark, ds, "T1_FILES_NOT_LOADED", missing), entry, failing).state == "ERROR"
    unset = feed(partition_cursor={"path": None, "extract_regex": None, "format": None, "compare_to": "partition"})
    outcome = run_entry(context(spark, ds, "T1_FILES_NOT_LOADED", unset), entry, failing)
    assert (outcome.state, outcome.evidence) == ("NOT_READY", {"reason": "probes.partition_cursor is not configured"})


def test_log_contains(spark, tmp_path) -> None:
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "load_1.log").write_text("INFO loaded rt_0002.seq\nERROR rt_0001.seq: header\n", encoding="utf-8")
    cx = context(spark, gold("causes.unused"), "T1_FILES_NOT_LOADED")
    params = LogContains(path_glob=f"{logs}/*.log", pattern="ERROR")
    hit = log_contains(cx, params, Failure("f", files=("file:/landing/rt_0001.seq",)))
    assert hit.state == "CONFIRMED" and list(hit.evidence["logs"].values()) == [1]
    assert "header" not in json.dumps(hit.evidence)  # log lines never go into evidence
    assert log_contains(cx, params, Failure("f", files=("file:/landing/rt_0002.seq",))).state == "RULED_OUT"  # INFO
    nothing = LogContains(path_glob=f"{tmp_path}/none/*.log", pattern="ERROR")
    assert log_contains(cx, nothing, Failure("f", files=("file:/landing/rt_0001.seq",))).evidence == {"logs": 0}


def test_table_contains_by_file_and_by_key(spark) -> None:
    up = Dataset(dataset="example_curated_enrollment", table="causes.tc_curated", layer="CURATED",
                 key=["subscriberidnumber", "membernumber", "effectivedate", "qualifiedhealthplanid"],
                 file_name_column="src_file_nm")
    create_table(spark, "causes.tc_rejects", [*CURATED_COLUMNS, ("src_file_nm", "STRING"), ("reason", "STRING")], [
        {**curated_row(), "src_file_nm": "/landing/rt_0001.seq", "reason": "bad plan"},
        {**curated_row(sub_id="9"), "src_file_nm": "rt_0002.seq", "reason": None}])
    params = TableContains(table="causes.tc_rejects", condition="reason IS NOT NULL", id="rejects")
    cx = context(spark, gold("causes.unused"), "HOP_FILE_COMPLETENESS", upstream=up)
    assert table_contains(cx, params, Failure("rt_0001.seq", file_name="rt_0001.seq")).state == "CONFIRMED"
    assert table_contains(cx, params, Failure("rt_0002.seq", file_name="rt_0002.seq")).state == "RULED_OUT"
    cx = context(spark, gold("causes.unused"), "HOP_KEY_CURRENCY", upstream=up, secret=SECRET)
    hit = Failure(key_hash(SECRET, KEY), key={"key_state": "MISSING"})
    miss = Failure(key_hash(SECRET, "9|01|2026-01-01|AGR-A"), key={"key_state": "MISSING"})
    assert table_contains(cx, params, hit).evidence == {"table": "causes.tc_rejects", "id": "rejects"}
    assert table_contains(cx, params, hit).state == "CONFIRMED"
    assert table_contains(cx, params, miss).state == "RULED_OUT"


def test_size_changed(spark) -> None:
    append_rows(spark, DQ, "dq_file", [
        {"path": "file:/landing/rt_0001.seq", "feed": "example_realtime", "first_seen_at": IN_WINDOW, "size_bytes": 10,
         "run_id": "r1", "run_date": IN_WINDOW.date()},
        {"path": "file:/landing/rt_0001.seq", "feed": "example_realtime", "first_seen_at": IN_WINDOW, "size_bytes": 20,
         "run_id": "r2", "run_date": IN_WINDOW.date()},
        {"path": "file:/landing/rt_0002.seq", "feed": "example_realtime", "first_seen_at": IN_WINDOW, "size_bytes": 10,
         "run_id": "r1", "run_date": IN_WINDOW.date()}])
    cx = context(spark, gold("causes.unused"), "T1_FILES_NOT_LOADED", feed())
    assert size_changed(cx, SizeChanged(), Failure("a", files=("file:/landing/rt_0001.seq",))).state == "CONFIRMED"
    assert size_changed(cx, SizeChanged(), Failure("b", files=("file:/landing/rt_0002.seq",))).state == "RULED_OUT"
    assert size_changed(cx, SizeChanged(), Failure("c", files=None)).state == "NOT_READY"  # slot, no raw dataset


# --- landing builtins ---

def test_unreadable(spark, tmp_path) -> None:
    good = land_sequence_files(spark, tmp_path, "rt_0001.seq")[0]
    bad = tmp_path / "rt_0002.seq"
    bad.write_bytes(b"not a sequence file")
    empty = tmp_path / "rt_0003.seq"
    empty.write_bytes(b"")
    cx = context(spark, gold("causes.unused"), "T1_FILES_NOT_LOADED", feed())
    assert landing.unreadable(cx, Failure("g", files=(str(good),))).state == "RULED_OUT"
    hit = landing.unreadable(cx, Failure("b", files=(str(bad),)))
    assert hit.state == "CONFIRMED" and hit.evidence["unreadable"][0]["file"] == str(bad)
    assert landing.unreadable(cx, Failure("e", files=(str(empty),))).state == "CONFIRMED"
    assert landing.unreadable(cx, Failure("s", files=None)).state == "NOT_READY"
    assert landing.unreadable(cx, Failure("s", files=())).state == "RULED_OUT"
    csv = tmp_path / "roster.csv"
    csv.write_text("provider_id,provider_name\nP1,Provider P1\n", encoding="utf-8")
    cx = context(spark, gold("causes.unused"), "T1_FILES_NOT_LOADED", feed(file_format="csv"))
    assert landing.unreadable(cx, Failure("c", files=(str(csv),))).state == "RULED_OUT"


def test_pipeline_stalled_needs_the_raw_dataset(spark) -> None:
    cx = context(spark, gold("causes.unused"), "T1_ON_TIME", feed())
    assert landing.pipeline_stalled(cx, Failure("s", slot=EVENT.window_start)).state == "NOT_READY"


# --- hop and load builtins ---

def _hop(spark, name: str, curated_rows: list, gold_rows: list, **gold_extra):
    create_table(spark, f"causes.{name}_cur", CURATED_COLUMNS, curated_rows)
    create_table(spark, f"causes.{name}_gold", GOLD_COLUMNS, gold_rows)
    return curated(f"causes.{name}_cur"), gold(f"causes.{name}_gold", dataset=f"gold_{name}", **gold_extra)


def _key_failures(spark, cx: CauseContext, mode: str = "currency") -> list[Failure]:
    from hcsc.datalake.dre.causes.failures import failures_for

    result = CheckResult("FAILED", population=1, violations=1)
    cx.check_id = "HOP_KEY_CURRENCY" if mode == "currency" else "HOP_VALUE_AGREEMENT"
    return failures_for(cx, result)


def test_key_mismatch(spark) -> None:
    # Gold has the key under another agreement id: it matches once covrg_agrmt_id is left out.
    up, dn = _hop(spark, "km", [curated_row(updated=IN_WINDOW)], [gold_row(agreement="AGR-Z", record=IN_WINDOW)],
                  mismatch_probe_drop=["covrg_agrmt_id"])
    cx = context(spark, dn, "HOP_KEY_CURRENCY", upstream=up, secret=SECRET)
    [failure] = _key_failures(spark, cx)
    assert failure.key["key_state"] == "MISSING" and failure.ref == key_hash(SECRET, KEY)
    assert merge.key_mismatch(cx, failure).evidence == {"matches_without": "covrg_agrmt_id"}
    other = Dataset(**{**dn.model_dump(), "mismatch_probe_drop": ["mem_nbr"]})
    cx = context(spark, other, "HOP_KEY_CURRENCY", upstream=up, secret=SECRET)
    [failure] = _key_failures(spark, cx)
    assert merge.key_mismatch(cx, failure).state == "RULED_OUT"
    cx = context(spark, gold(dn.table, dataset=dn.dataset), "HOP_KEY_CURRENCY", upstream=up, secret=SECRET)
    assert merge.key_mismatch(cx, _key_failures(spark, cx)[0]).state == "NOT_READY"


def test_older_version_written_later_and_not_run_need_a_load_time_here(spark) -> None:
    up, dn = _hop(spark, "ov", [curated_row(updated=IN_WINDOW)], [gold_row(record=datetime(2026, 1, 9))])
    cx = context(spark, dn, "HOP_KEY_CURRENCY", upstream=up, secret=SECRET)  # gold without load_time
    [failure] = _key_failures(spark, cx)
    assert failure.key["key_state"] == "STALE"
    assert merge.older_version_written_later(cx, failure).state == "NOT_READY"
    assert merge.not_run(cx, failure).state == "NOT_READY"


def test_file_skipped_and_invalid_key(spark) -> None:
    columns = [("subscriberidnumber", "STRING"), ("membernumber", "STRING"), ("effectivedate", "STRING"),
               ("qualifiedhealthplanid", "STRING"), ("sourcelastupdatets", "TIMESTAMP"), ("src_file_nm", "STRING")]
    row = lambda sub, f, at, eff="01/01/2026": {"subscriberidnumber": sub, "membernumber": "01",  # noqa: E731
                                                "effectivedate": eff, "qualifiedhealthplanid": "AGR-A",
                                                "sourcelastupdatets": at, "src_file_nm": f}
    create_table(spark, "causes.fs_raw", columns, [
        row("1", "rt_0001.seq", datetime(2026, 1, 15, 8)),                  # skipped entirely
        row("2", "rt_0002.seq", datetime(2026, 1, 15, 9)),                  # loaded
        row("3", "rt_0003.seq", datetime(2026, 1, 15, 9), eff="13/45/2026"),  # unparseable date
        row("4", "rt_0003.seq", datetime(2026, 1, 15, 9))])
    create_table(spark, "causes.fs_cur", columns, [
        {**row("2", "rt_0002.seq", datetime(2026, 1, 15, 10)), "effectivedate": "2026-01-01"},
        {**row("4", "rt_0003.seq", datetime(2026, 1, 15, 10)), "effectivedate": "2026-01-01"}])
    ts = TimeColumn(column="sourcelastupdatets", granularity="minute", timezone="UTC")
    raw = Dataset(dataset="example_raw_enrollment", table="causes.fs_raw", layer="RAW",
                  key=["subscriberidnumber", "membernumber", "effectivedate", "qualifiedhealthplanid"],
                  key_normalise={"effectivedate": {"parse_date": "MM/dd/yyyy"}}, file_name_column="src_file_nm",
                  record_time=ts, load_time=ts)
    cur = Dataset(dataset="example_curated_enrollment", table="causes.fs_cur", layer="CURATED", key=raw.key,
                  upstream=["example_raw_enrollment"], file_name_column="src_file_nm", record_time=ts, load_time=ts,
                  key_map={"example_raw_enrollment": {c: c for c in raw.key}})
    cx = context(spark, cur, "HOP_FILE_COMPLETENESS", upstream=raw)
    skipped, partial = Failure("rt_0001.seq", file_name="rt_0001.seq"), Failure("rt_0003.seq", file_name="rt_0003.seq")
    assert merge.file_skipped(cx, skipped).evidence == {"later_files_loaded": 2}
    assert merge.file_skipped(cx, partial).state == "RULED_OUT"  # one of its rows is here
    assert merge.invalid_key(cx, partial).evidence == {"invalid_key_rows": 1, "upstream_rows": 2}
    assert merge.invalid_key(cx, skipped).state == "RULED_OUT"
    assert merge.not_run(cx, skipped).state == "RULED_OUT"  # curated loaded after 08:00


def test_no_upstream_data(spark) -> None:
    up, dn = _hop(spark, "nu", [curated_row(updated=datetime(2026, 1, 10))], [])
    up = curated(up.table)
    with_load = gold(dn.table, dataset=dn.dataset, load_time=GOLD_LOAD)
    cx = context(spark, with_load, "T1_ZERO_ROWS", datasets={up.dataset: up})
    assert merge.no_upstream_data(cx, Failure("s", slot=EVENT.window_start)).state == "CONFIRMED"  # nothing in window
    create_table(spark, up.table, CURATED_COLUMNS, [curated_row(updated=datetime(2026, 1, 15, 13))])
    cx = context(spark, with_load, "T1_ZERO_ROWS", datasets={up.dataset: up})
    assert merge.no_upstream_data(cx, Failure("s", slot=EVENT.window_start)).state == "RULED_OUT"
    alone = gold(dn.table, dataset=dn.dataset, upstream=[], key_map={}, owned_columns={})
    assert merge.no_upstream_data(context(spark, alone, "T1_ZERO_ROWS"), Failure("s")).state == "NOT_READY"


# --- the engine ---

def test_any_parameter_set_confirming_confirms(spark, tmp_path) -> None:
    (tmp_path / "second.flag").write_text("", encoding="utf-8")
    the_feed = feed(load_hold_marker=[{"path": str(tmp_path / "first.flag")}, {"path": str(tmp_path / "second.flag")}])
    entry = CauseEntry("RAW_LOAD_HELD", "file_exists", "load_hold_marker")
    cx = context(spark, gold("causes.unused"), "T1_ON_TIME", the_feed)
    assert run_entry(cx, entry, Failure("s")).state == "CONFIRMED"
    one = feed(load_hold_marker=[{"path": str(tmp_path / "first.flag")}, {"path": None}])
    assert run_entry(context(spark, gold("causes.unused"), "T1_ON_TIME", one), entry, Failure("s")).state == "RULED_OUT"


def test_key_causes_without_a_secret_are_not_ready(spark) -> None:
    up, dn = _hop(spark, "ns", [curated_row(updated=IN_WINDOW)], [], load_time=GOLD_LOAD)
    cx = context(spark, dn, "HOP_KEY_CURRENCY", feed(), upstream=up)
    result = CheckResult("FAILED", population=1, violations=1, group_values={"upstream": up.dataset})
    rows = explain(cx, result, "eval-1", "run-1", EVENT.window_end.date(), EVENT.window_end)
    assert len(rows) == 6 and {r["failure_ref"] for r in rows} == {None}
    assert {(r["state"], json.loads(r["evidence"])["reason"]) for r in rows} == {
        ("NOT_READY", "hmac_secret_file is not set, so failing keys cannot be referenced")}


def test_explain_writes_every_cause_check_in_order(spark) -> None:
    up, dn = _hop(spark, "ex", [curated_row(updated=IN_WINDOW)], [gold_row(record=datetime(2026, 1, 9),
                  loaded="2026-01-15 13:00:00:000000")], load_time=GOLD_LOAD)
    cx = context(spark, dn, "HOP_KEY_CURRENCY", feed(), upstream=up, secret=SECRET)
    result = CheckResult("FAILED", population=1, violations=1, group_values={"upstream": up.dataset})
    rows = explain(cx, result, "eval-1", "run-1", EVENT.window_end.date(), EVENT.window_end)
    assert [(r["order_no"], r["cause_code"], r["state"]) for r in rows] == [
        (1, "NOT_RUN", "RULED_OUT"), (2, "KEY_MISMATCH", "NOT_READY"), (3, "TIE_RESOLVED_BY_RULE", "NOT_READY"),
        (4, "OLDER_VERSION_WRITTEN_LATER", "CONFIRMED"), (5, "FILTERED", "NOT_READY"), (6, "REJECTED", "NOT_READY")]
    assert {r["hop"] for r in rows} == {"example_curated_enrollment->gold_ex"}
    assert {r["failure_ref"] for r in rows} == {key_hash(SECRET, KEY)}
    assert KEY not in json.dumps(rows, default=str)  # key values never leave memory
    ft = failure_type("FILE_CYCLIC", "HOP_KEY_CURRENCY")
    assert resolve(rows, ft) == CauseLine("OLDER_VERSION_WRITTEN_LATER", True)
    # Not FAILED, or no causes for the check: nothing to explain.
    assert explain(cx, CheckResult("PASSED", 1, 0), "e", "r", EVENT.window_end.date(), EVENT.window_end) == []
    cx.check_id = "T1_KEY_DUPLICATES"
    assert explain(cx, result, "e", "r", EVENT.window_end.date(), EVENT.window_end) == []


def test_a_failure_that_cannot_be_listed_is_error_for_every_cause(spark) -> None:
    # A detail that cannot be read: every cause check of the failure type is ERROR, none is lost.
    cx = context(spark, gold("causes.no_such_table"), "HOP_FILE_COMPLETENESS", feed(), upstream=curated("causes.x"))
    rows = explain(cx, CheckResult("FAILED", 1, 1, detail="{not json"), "e", "r", EVENT.window_end.date(),
                   EVENT.window_end)
    assert [(r["cause_code"], r["state"], r["failure_ref"]) for r in rows] == [
        (code, "ERROR", None) for code in ("NOT_RUN", "FILE_SKIPPED", "INVALID_KEY", "FILTERED", "REJECTED")]
    # A hop that cannot be evaluated (the upstream has no load_time): the keys cannot be listed.
    cx = context(spark, gold("causes.no_such_table"), "HOP_KEY_CURRENCY", feed(),
                 upstream=curated("causes.x", load_time=None), secret=SECRET)
    rows = explain(cx, CheckResult("FAILED", 1, 1), "e", "r", EVENT.window_end.date(), EVENT.window_end)
    assert {r["state"] for r in rows} == {"ERROR"} and len(rows) == 6


def test_cause_lines() -> None:
    assert CauseLine("FILE_SKIPPED", True).text() == "cause: FILE_SKIPPED (confirmed)"
    line = CauseLine("DROPPED", False, ("NOT_RUN", "INVALID_KEY"), ("FILTERED",))
    assert line.text() == "cause not proven (DROPPED): ruled out NOT_RUN, INVALID_KEY; not ready FILTERED"
    assert CauseLine("EMPTY_LOAD", False).text() == "cause not proven (EMPTY_LOAD)"


def test_file_value_compare_can_confirm_on_any_different_partition(spark, tmp_path) -> None:
    """confirm_when: different (configurable; the spec's default is later)."""
    cx = context(spark, gold("causes.unused"), "T1_FILES_NOT_LOADED")
    failing = Failure("f", files=("file:/data/landing/example_feed/20261001/rt_0001.seq",))
    params = lambda value, when: FileValueCompare(path=_cursor(tmp_path, value), extract_regex=r"(\d{8})",  # noqa: E731
                                                  format="yyyyMMdd", compare_to="partition", confirm_when=when)
    assert file_value_compare(cx, params("20260930", "later"), failing).state == "RULED_OUT"
    assert file_value_compare(cx, params("20260930", "different"), failing).state == "CONFIRMED"
    assert file_value_compare(cx, params("20261001", "different"), failing).state == "RULED_OUT"
