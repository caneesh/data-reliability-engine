"""HOP_KEY_CURRENCY, HOP_VALUE_AGREEMENT and HOP_FILE_COMPLETENESS can PASS, FAIL and be DID_NOT_RUN.

Fixed window [2026-01-15 12:00, 2026-01-16 12:00) UTC with an 8-hour SLA: upstream keys and
files loaded in [2026-01-15 04:00, 2026-01-16 04:00) are judged. All times are UTC.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import date, datetime, timezone

import pytest

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.hop.common import hop_label, key_hash, register_key_hash
from hcsc.datalake.dre.checks.hop.file_completeness import FileCompleteness
from hcsc.datalake.dre.checks.hop.key_currency import KeyCurrency
from hcsc.datalake.dre.checks.hop.value_agreement import ValueAgreement
from hcsc.datalake.dre.config.models import Dataset, TimeColumn
from hcsc.datalake.dre.store.key_events import append_key_events
from hcsc.datalake.dre.store.local_setup import create_store
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_row, gold_row
from tests.fixtures.settings import SETTINGS

UTC = timezone.utc
DQ = "dq_hop"
EVENT = Event("gold", datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 1, 16, 12, tzinfo=UTC))
IN_WINDOW = datetime(2026, 1, 15, 10, 0)   # loaded upstream inside the judged range
EARLIER = datetime(2026, 1, 15, 9, 0)
BEFORE = datetime(2026, 1, 10, 10, 0)     # loaded before the judged range
SECRET = b"synthetic-test-secret"
KEY = "123401|01|2026-01-01|AGR-A"        # the canonical key of curated_row() and gold_row()


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


def curated(table: str, **extra) -> Dataset:
    fields = {"dataset": "example_curated_enrollment", "table": table, "layer": "CURATED",
              "key": ["subscriberidnumber", "membernumber", "effectivedate", "qualifiedhealthplanid"],
              "record_time": TimeColumn(column="sourcelastupdatets", timezone="UTC"),
              "load_time": TimeColumn(column="sourcelastupdatets", granularity="minute", timezone="UTC"), **extra}
    return Dataset(**fields)


def gold(table: str, dataset: str = "gold", **extra) -> Dataset:
    fields = {"dataset": dataset, "table": table, "layer": "GOLD", "upstream": ["example_curated_enrollment"],
              "key": ["sub_id", "mem_nbr", "mbr_mbrshp_covrg_eff_dt", "covrg_agrmt_id"],
              "key_normalise": {"sub_id": "strip_leading_zeros"},
              "record_time": TimeColumn(column="src_lcts", timezone="UTC"),
              "key_map": {"example_curated_enrollment": {
                  "sub_id": "subscriberidnumber", "mem_nbr": "membernumber",
                  "mbr_mbrshp_covrg_eff_dt": "effectivedate", "covrg_agrmt_id": "qualifiedhealthplanid"}},
              "owned_columns": {"enddate": "mbr_mbrshp_covrg_end_dt"}, **extra}
    return Dataset(**fields)


def run(spark, check, ds: Dataset, up: Dataset | None, event: Event = EVENT, secret: bytes | None = None,
        full_sweep: bool = False):
    upstreams = {up.dataset: up} if up is not None else {}
    ctx = CheckContext(spark, ds, None, SETTINGS, "UTC", DQ, upstreams=upstreams, key_secret=secret,
                       full_sweep=full_sweep)
    results, _ = run_check(check, ctx, event)
    return results


def tables(spark, name: str, curated_rows: list, gold_rows: list) -> tuple[Dataset, Dataset]:
    create_table(spark, f"hop.{name}_cur", CURATED_COLUMNS, curated_rows)
    create_table(spark, f"hop.{name}_gold", GOLD_COLUMNS, gold_rows)
    return curated(f"hop.{name}_cur"), gold(f"hop.{name}_gold", dataset=f"gold_{name}")


def events_of(result) -> list[tuple[str, str, str, str | None]]:
    return sorted((r.key_hash, r.event, r.state_detail, r.key_value) for r in result.key_events.collect())


def record(spark, result, ds: Dataset, run_id: str, observed: datetime) -> None:
    hop = hop_label(result.group_values["upstream"], ds.dataset)
    append_key_events(result.key_events, DQ, run_id, f"eval-{run_id}-{hop}", ds.dataset, "HOP_KEY_CURRENCY",
                      hop, observed, observed.date())


# --- HOP_KEY_CURRENCY ---

def test_key_currency_passes_when_gold_has_the_latest_version(spark) -> None:
    # Gold's sub_id is zero-padded; the join applies key_normalise.
    up, dn = tables(spark, "kc_ok", [curated_row(updated=IN_WINDOW)], [gold_row(record=IN_WINDOW)])
    [r] = run(spark, KeyCurrency(), dn, up)
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)
    assert r.group_values == {"upstream": "example_curated_enrollment"}


def test_key_currency_fails_on_missing_and_stale_keys(spark) -> None:
    up, dn = tables(spark, "kc_bad", [
        curated_row(sub_id="1", updated=IN_WINDOW),                 # missing in gold
        curated_row(sub_id="2", updated=IN_WINDOW),                 # stale in gold
        curated_row(sub_id="3", updated=BEFORE),                    # missing, but not judged in this window
    ], [gold_row(sub_id="0002", record=EARLIER)])
    [r] = run(spark, KeyCurrency(), dn, up)
    assert (r.state, r.population, r.violations) == ("FAILED", 2, 2)
    assert r.observed == "1 missing and 1 stale of 2 keys"
    assert r.detail == "key events not recorded: hmac_secret_file is not set"
    assert r.key_events is None


def test_key_currency_did_not_run_paths(spark) -> None:
    up, dn = tables(spark, "kc_dnr", [curated_row(updated=BEFORE)], [])
    [r] = run(spark, KeyCurrency(), dn, None)
    assert (r.state, r.reason_category, r.reason_code) == ("DID_NOT_RUN", "CONFIGURATION", "invalid_config")
    [r] = run(spark, KeyCurrency(), dn, curated(up.table, load_time=None))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "invalid_config")
    [r] = run(spark, KeyCurrency(), gold(dn.table, record_time=None), up)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "invalid_config")
    # Nothing loaded upstream in the judged range and no open keys: no population.
    [r] = run(spark, KeyCurrency(), dn, up)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


def test_key_currency_takes_the_latest_upstream_version(spark) -> None:
    # Two curated versions; gold holds the older one.
    up, dn = tables(spark, "kc_latest", [curated_row(updated=EARLIER), curated_row(updated=IN_WINDOW)],
                    [gold_row(record=EARLIER)])
    [r] = run(spark, KeyCurrency(), dn, up)
    assert (r.state, r.observed) == ("FAILED", "0 missing and 1 stale of 1 keys")


GOLD_LOAD = TimeColumn(column="gld_lcts", format="yyyy-MM-dd HH:mm:ss:SSSSSS", granularity="minute", timezone="UTC")


def test_a_key_that_regresses_here_without_an_upstream_change_is_judged_again(spark) -> None:
    # Curated last changed long ago; gold was overwritten in the window with an older version.
    up, dn = tables(spark, "kc_regress", [curated_row(updated=BEFORE)],
                    [gold_row(record=datetime(2026, 1, 9), loaded="2026-01-15 13:00:00:000000")])
    [r] = run(spark, KeyCurrency(), gold(dn.table, dataset=dn.dataset, load_time=GOLD_LOAD), up)
    assert (r.state, r.population, r.violations) == ("FAILED", 1, 1)
    assert r.observed == "0 missing and 1 stale of 1 keys"
    # Loaded here before the window: not judged again (it was judged when it happened).
    create_table(spark, dn.table, GOLD_COLUMNS,
                 [gold_row(record=datetime(2026, 1, 9), loaded="2026-01-12 13:00:00:000000")])
    [r] = run(spark, KeyCurrency(), gold(dn.table, dataset=dn.dataset, load_time=GOLD_LOAD), up)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


def test_a_reload_here_does_not_judge_a_key_before_its_upstream_deadline(spark) -> None:
    # Upstream changed 1 hour before the window end (inside the 8-hour SLA): the key is not judged yet,
    # even though gold reloaded it in the window.
    recent = datetime(2026, 1, 16, 11, 0)
    up, dn = tables(spark, "kc_early", [curated_row(updated=recent)],
                    [gold_row(record=datetime(2026, 1, 9), loaded="2026-01-16 11:30:00:000000")])
    [r] = run(spark, KeyCurrency(), gold(dn.table, dataset=dn.dataset, load_time=GOLD_LOAD), up)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")


def test_key_events_flagged_still_flagged_cleared(spark) -> None:
    up, dn = tables(spark, "kc_events", [curated_row(updated=IN_WINDOW)], [])
    expected_hash = hmac.new(SECRET, KEY.encode(), hashlib.sha256).hexdigest()

    [r] = run(spark, KeyCurrency(), dn, up, secret=SECRET)
    assert r.state == "FAILED"
    assert events_of(r) == [(expected_hash, "FLAGGED", "MISSING", KEY)]
    record(spark, r, dn, "run-1", datetime(2026, 1, 16, 12, tzinfo=UTC))

    # A later window: the key is past its deadline but still open, so it is judged again.
    later = Event("gold", datetime(2026, 1, 16, 12, tzinfo=UTC), datetime(2026, 1, 16, 13, tzinfo=UTC))
    [r] = run(spark, KeyCurrency(), dn, up, event=later, secret=SECRET)
    assert (r.state, r.population, r.violations) == ("FAILED", 1, 1)
    assert events_of(r) == [(expected_hash, "STILL_FLAGGED", "MISSING", KEY)]
    record(spark, r, dn, "run-2", datetime(2026, 1, 16, 13, tzinfo=UTC))

    # Gold catches up: the open key is CURRENT and cleared.
    create_table(spark, dn.table, GOLD_COLUMNS, [gold_row(record=IN_WINDOW)])
    last = Event("gold", datetime(2026, 1, 16, 13, tzinfo=UTC), datetime(2026, 1, 16, 14, tzinfo=UTC))
    [r] = run(spark, KeyCurrency(), dn, up, event=last, secret=SECRET)
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)
    assert events_of(r) == [(expected_hash, "CLEARED", "CURRENT", KEY)]
    record(spark, r, dn, "run-3", datetime(2026, 1, 16, 14, tzinfo=UTC))
    assert spark.sql(f"SELECT * FROM {DQ}.v_open_keys WHERE dataset = '{dn.dataset}'").count() == 0


def two_upstreams(spark, name: str, rows_a: list, rows_b: list, gold_rows: list):
    """gold with two upstreams, curated_a and curated_b, mapped the same way."""
    create_table(spark, f"hop.{name}_a", CURATED_COLUMNS, rows_a)
    create_table(spark, f"hop.{name}_b", CURATED_COLUMNS, rows_b)
    create_table(spark, f"hop.{name}_gold", GOLD_COLUMNS, gold_rows)
    a = curated(f"hop.{name}_a", dataset="curated_a")
    b = curated(f"hop.{name}_b", dataset="curated_b")
    mapping = gold("hop.unused").key_map["example_curated_enrollment"]
    dn = gold(f"hop.{name}_gold", dataset=f"gold_{name}", upstream=["curated_a", "curated_b"],
              key_map={"curated_a": mapping, "curated_b": mapping})
    return a, b, dn


def run_hops(spark, check, ds: Dataset, upstreams: list[Dataset], event: Event = EVENT, secret: bytes | None = None):
    ctx = CheckContext(spark, ds, None, SETTINGS, "UTC", DQ, upstreams={u.dataset: u for u in upstreams},
                       key_secret=secret)
    results, _ = run_check(check, ctx, event)
    return {r.group_values["upstream"]: r for r in results}


def test_open_keys_are_kept_per_hop(spark) -> None:
    """Review finding 2: a key STALE against curated_a and CURRENT against curated_b stays open at
    curated_a->gold; curated_b's hop does not clear it."""
    a, b, dn = two_upstreams(spark, "kc_two", [curated_row(updated=IN_WINDOW)], [curated_row(updated=EARLIER)],
                             [gold_row(record=EARLIER)])
    results = run_hops(spark, KeyCurrency(), dn, [a, b], secret=SECRET)
    assert {u: r.state for u, r in results.items()} == {"curated_a": "FAILED", "curated_b": "PASSED"}
    for r in results.values():
        record(spark, r, dn, "two-1", datetime(2026, 1, 16, 12, tzinfo=UTC))
    later = Event("gold", datetime(2026, 1, 16, 12, tzinfo=UTC), datetime(2026, 1, 16, 13, tzinfo=UTC))
    results = run_hops(spark, KeyCurrency(), dn, [a, b], event=later, secret=SECRET)
    assert events_of(results["curated_a"]) == [(key_hash(SECRET, KEY), "STILL_FLAGGED", "STALE", KEY)]
    assert (results["curated_b"].state, results["curated_b"].reason_code) == ("DID_NOT_RUN", "empty_population")
    assert events_of(results["curated_b"]) == []  # never open at curated_b->gold, so nothing to clear
    open_hops = [r.hop for r in spark.sql(f"SELECT hop FROM {DQ}.v_open_keys WHERE dataset = '{dn.dataset}'").collect()]
    assert open_hops == ["curated_a->gold_kc_two"]


@pytest.mark.parametrize("check", [KeyCurrency(), ValueAgreement()], ids=lambda c: c.check_id)  # FileCompleteness: same per_hop
def test_one_broken_hop_does_not_hide_the_others(spark, check) -> None:
    """Review finding 14: curated_b's table is gone; curated_a's result still stands."""
    a, b, dn = two_upstreams(spark, f"hop_broken_{check.check_id.lower()}", [curated_row(sub_id="1", updated=IN_WINDOW)],
                             [curated_row(updated=IN_WINDOW)], [gold_row(record=EARLIER)])
    b = curated("hop.no_such_table", dataset="curated_b")
    results = run_hops(spark, check, dn, [a, b])
    assert results["curated_a"].state in ("FAILED", "DID_NOT_RUN")
    assert results["curated_a"].reason_code != "table_missing"
    assert (results["curated_b"].state, results["curated_b"].reason_code) == ("DID_NOT_RUN", "table_missing")


def test_full_sweep_clears_keys_no_longer_upstream(spark) -> None:
    up, dn = tables(spark, "kc_sweep", [curated_row(updated=IN_WINDOW)], [])
    [r] = run(spark, KeyCurrency(), dn, up, secret=SECRET)
    record(spark, r, dn, "sweep-1", datetime(2026, 1, 16, 12, tzinfo=UTC))
    # The key leaves curated. An ordinary window does not judge it (it has no upstream row) ...
    create_table(spark, up.table, CURATED_COLUMNS, [curated_row(sub_id="9", updated=BEFORE)])
    later = Event("gold", datetime(2026, 1, 16, 12, tzinfo=UTC), datetime(2026, 1, 16, 13, tzinfo=UTC))
    [r] = run(spark, KeyCurrency(), dn, up, event=later, secret=SECRET)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")
    # ... the full sweep reads every key: the old key is cleared, the earlier missing key is judged.
    [r] = run(spark, KeyCurrency(), dn, up, event=later, secret=SECRET, full_sweep=True)
    assert (r.state, r.population, r.violations) == ("FAILED", 1, 1)
    gone = key_hash(SECRET, KEY)
    new = key_hash(SECRET, "9|01|2026-01-01|AGR-A")
    assert events_of(r) == sorted([(gone, "CLEARED", "no longer upstream", None),
                                   (new, "FLAGGED", "MISSING", "9|01|2026-01-01|AGR-A")])


def test_sql_key_hash_matches_python_hmac(spark) -> None:
    register_key_hash(spark, SECRET)
    [row] = spark.sql(f"SELECT dre_key_hash('{KEY}') AS h, dre_key_hash(CAST(NULL AS STRING)) AS n").collect()
    assert row.h == hmac.new(SECRET, KEY.encode(), hashlib.sha256).hexdigest() == key_hash(SECRET, KEY)
    assert row.n is None
    # The secret is not in the SQL text or the plan.
    plan = spark.sql(f"SELECT dre_key_hash('{KEY}')")._jdf.queryExecution().toString()
    assert SECRET.decode() not in plan


# --- HOP_VALUE_AGREEMENT ---

def test_value_agreement_passes_when_owned_columns_match(spark) -> None:
    up, dn = tables(spark, "va_ok", [curated_row(updated=IN_WINDOW, end="2026-06-30")],
                    [gold_row(record=IN_WINDOW, end="2026-06-30")])
    [r] = run(spark, ValueAgreement(), dn, up)
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)


def test_value_agreement_is_null_safe(spark) -> None:
    up, dn = tables(spark, "va_null", [curated_row(updated=IN_WINDOW, end=None)],
                    [gold_row(record=IN_WINDOW, end=None)])
    [r] = run(spark, ValueAgreement(), dn, up)
    assert (r.state, r.violations) == ("PASSED", 0)


def test_value_agreement_fails_on_a_mismatch(spark) -> None:
    up, dn = tables(spark, "va_bad", [curated_row(sub_id="1", updated=IN_WINDOW, end="2026-06-30"),
                                      curated_row(sub_id="2", updated=IN_WINDOW)],
                    [gold_row(sub_id="0001", record=IN_WINDOW), gold_row(sub_id="0002", record=IN_WINDOW)])
    [r] = run(spark, ValueAgreement(), dn, up)
    assert (r.state, r.population, r.violations) == ("FAILED", 2, 1)
    assert r.observed == "1 of 2 current keys disagree on enddate->mbr_mbrshp_covrg_end_dt"


def test_value_agreement_did_not_run_paths(spark) -> None:
    # The only judged key is missing in gold: no CURRENT keys to compare.
    up, dn = tables(spark, "va_dnr", [curated_row(updated=IN_WINDOW)], [])
    [r] = run(spark, ValueAgreement(), dn, up)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")
    [r] = run(spark, ValueAgreement(), dn, None)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "invalid_config")
    assert not ValueAgreement().applies_to(gold(dn.table, owned_columns={}), "FILE_CYCLIC")


# --- HOP_FILE_COMPLETENESS ---

FILE_UP = [("id", "STRING"), ("ts", "TIMESTAMP"), ("src_file_nm", "STRING")]
FILE_DN = [("id", "STRING"), ("ts", "TIMESTAMP"), ("file_nm", "STRING")]


def file_tables(spark, name: str, up_rows: list[tuple], dn_rows: list[tuple]) -> tuple[Dataset, Dataset]:
    create_table(spark, f"hop.{name}_up", FILE_UP, [dict(zip(("id", "ts", "src_file_nm"), r)) for r in up_rows])
    create_table(spark, f"hop.{name}_dn", FILE_DN, [dict(zip(("id", "ts", "file_nm"), r)) for r in dn_rows])
    ts = TimeColumn(column="ts", granularity="minute", timezone="UTC")
    up = Dataset(dataset="file_up", table=f"hop.{name}_up", layer="RAW", key=["id"], record_time=ts, load_time=ts,
                 file_name_column="src_file_nm")
    dn = Dataset(dataset=f"file_dn_{name}", table=f"hop.{name}_dn", layer="CURATED", upstream=["file_up"],
                 key=["id"], record_time=ts, file_name_column="file_nm", key_map={"file_up": {"id": "id"}})
    return up, dn


def test_file_completeness_passes_when_every_row_arrived(spark) -> None:
    # Paths upstream, base names here: compared by base name.
    up, dn = file_tables(spark, "fc_ok",
                         [("a", IN_WINDOW, "/data/landing/example_feed/rt_0001.seq"),
                          ("b", IN_WINDOW, "/data/landing/example_feed/rt_0001.seq")],
                         [("a", IN_WINDOW, "rt_0001.seq"), ("b", IN_WINDOW, "rt_0001.seq")])
    [r] = run(spark, FileCompleteness(), dn, up)
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)


def test_file_completeness_fails_on_a_short_or_missing_file(spark) -> None:
    up, dn = file_tables(spark, "fc_bad",
                         [("a", IN_WINDOW, "rt_0001.seq"), ("b", IN_WINDOW, "rt_0001.seq"),
                          ("c", IN_WINDOW, "rt_0001.seq"), ("d", IN_WINDOW, "rt_0002.seq"),
                          ("e", IN_WINDOW, "rt_0003.seq")],
                         [("a", IN_WINDOW, "rt_0001.seq"), ("b", IN_WINDOW, "rt_0001.seq"),
                          ("d", IN_WINDOW, "rt_0002.seq")])
    [r] = run(spark, FileCompleteness(), dn, up)
    assert (r.state, r.population, r.violations) == ("FAILED", 3, 2)
    assert json.loads(r.detail) == {"short_files": [
        {"file": "rt_0001.seq", "upstream_rows": 3, "rows_here": 2},
        {"file": "rt_0003.seq", "upstream_rows": 1, "rows_here": 0}]}


def test_file_completeness_did_not_run_paths(spark) -> None:
    up, dn = file_tables(spark, "fc_dnr", [("a", BEFORE, "rt_0001.seq")], [])
    [r] = run(spark, FileCompleteness(), dn, up)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")
    [r] = run(spark, FileCompleteness(), dn, Dataset(**{**up.model_dump(), "file_name_column": None}))
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "invalid_config")
    # The full sweep judges every file past its deadline, including the old one.
    [r] = run(spark, FileCompleteness(), dn, up, full_sweep=True)
    assert (r.state, r.population, r.violations) == ("FAILED", 1, 1)


def test_hop_checks_apply_only_with_a_key_map(spark) -> None:
    plain = curated("hop.none")
    assert not KeyCurrency().applies_to(plain, "FILE_CYCLIC")
    assert not ValueAgreement().applies_to(plain, "FILE_CYCLIC")
    assert not FileCompleteness().applies_to(plain, "FILE_CYCLIC")
    assert not FileCompleteness().applies_to(gold("hop.none"), "FILE_CYCLIC")  # no file_name_column


def test_key_events_keep_key_value_in_the_store_only(spark) -> None:
    """key_value lands in dq_key_event (restricted) and nowhere in the result."""
    up, dn = tables(spark, "kc_store", [curated_row(updated=IN_WINDOW)], [])
    [r] = run(spark, KeyCurrency(), dn, up, secret=SECRET)
    record(spark, r, dn, "store-1", datetime(2026, 1, 16, 12, tzinfo=UTC))
    [row] = spark.sql(f"SELECT key_value, run_date FROM {DQ}.dq_key_event WHERE dataset = '{dn.dataset}'").collect()
    assert (row.key_value, row.run_date) == (KEY, date(2026, 1, 16))
    assert KEY not in f"{r.observed} {r.expected} {r.detail}"


# --- latest-time ties and null record times (code review, October 2026) ---

def test_value_agreement_compares_the_tied_latest_rows_as_sets(spark) -> None:
    """Finding 1: two identical multi-row populations tied at the latest time pass; one changed row fails."""
    tied_up = [curated_row(updated=IN_WINDOW, end="2026-06-30"), curated_row(updated=IN_WINDOW, end="2026-12-31")]
    up, dn = tables(spark, "va_tie_ok", tied_up, [gold_row(record=IN_WINDOW, end="2026-06-30"),
                                                  gold_row(record=IN_WINDOW, end="2026-12-31")])
    [r] = run(spark, ValueAgreement(), dn, up)
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)
    up, dn = tables(spark, "va_tie_bad", tied_up, [gold_row(record=IN_WINDOW, end="2026-06-30"),
                                                   gold_row(record=IN_WINDOW, end="2027-01-31")])
    [r] = run(spark, ValueAgreement(), dn, up)
    assert (r.state, r.population, r.violations) == ("FAILED", 1, 1)


RECORD_COLUMNS = [*CURATED_COLUMNS, ("record_ts", "TIMESTAMP")]


def test_a_key_whose_upstream_record_times_are_all_null_is_unassessable_not_hidden(spark) -> None:
    """Finding 2: the key stays in the judgement with an explicit state, never silently passed."""
    rows = [{**curated_row(sub_id="1", updated=IN_WINDOW), "record_ts": None},             # no record time
            {**curated_row(sub_id="2", updated=IN_WINDOW), "record_ts": IN_WINDOW}]
    create_table(spark, "hop.kc_nullrec_cur", RECORD_COLUMNS, rows)
    create_table(spark, "hop.kc_nullrec_gold", GOLD_COLUMNS,
                 [gold_row(sub_id="0001", record=IN_WINDOW), gold_row(sub_id="0002", record=IN_WINDOW)])
    up = curated("hop.kc_nullrec_cur", record_time=TimeColumn(column="record_ts", timezone="UTC"))
    dn = gold("hop.kc_nullrec_gold", dataset="gold_kc_nullrec")
    [r] = run(spark, KeyCurrency(), dn, up, secret=SECRET)
    assert (r.state, r.population, r.violations) == ("FAILED", 2, 1)
    assert r.observed == "0 missing, 0 stale and 1 unassessable (no record time) of 2 keys"
    assert events_of(r) == [(key_hash(SECRET, "1|01|2026-01-01|AGR-A"), "FLAGGED", "UNASSESSABLE",
                             "1|01|2026-01-01|AGR-A")]


def test_a_synthetic_pipeline_with_two_upstreams_exclusions_late_data_and_a_missing_key(spark) -> None:
    """Code review (October 2026), P1: one gold dataset fed by two upstreams.
    curated_a: key 1 reached gold; key 2 is a test member a business rule excludes (not data loss);
    key 3 never reached gold (MISSING); key 4 arrived late upstream, so it is not yet due (not judged).
    curated_b: key 1 only, same version as gold. Each hop is judged on its own."""
    late = datetime(2026, 1, 16, 9, 0)  # loaded upstream after the window's deadline range
    a, b, dn = two_upstreams(
        spark, "pipeline",
        [curated_row(sub_id="1", updated=IN_WINDOW), curated_row(sub_id="9002", updated=IN_WINDOW),
         curated_row(sub_id="3", updated=IN_WINDOW), curated_row(sub_id="4", updated=late)],
        [curated_row(sub_id="1", updated=IN_WINDOW)],
        [gold_row(sub_id="0001", record=IN_WINDOW)])
    excluded = gold(dn.table, dataset=dn.dataset, upstream=["curated_a", "curated_b"], key_map=dn.key_map,
                    hop_exclusions={"curated_a": "subscriberidnumber LIKE '9%'"})
    results = run_hops(spark, KeyCurrency(), excluded, [a, b], secret=SECRET)
    assert (results["curated_a"].state, results["curated_a"].population, results["curated_a"].violations) == (
        "FAILED", 2, 1)  # keys 1 and 3 judged; key 2 excluded; key 4 not yet due
    assert results["curated_a"].observed == (
        "1 missing and 0 stale of 2 keys; 1 key excluded by hop_exclusions, 1 key pending (within sla_hours)")
    assert events_of(results["curated_a"]) == [(key_hash(SECRET, "3|01|2026-01-01|AGR-A"), "FLAGGED", "MISSING",
                                                "3|01|2026-01-01|AGR-A")]
    assert (results["curated_b"].state, results["curated_b"].population) == ("PASSED", 1)
    # Without the exclusion, the test member would be reported as lost.
    plain = run_hops(spark, KeyCurrency(), dn, [a, b])
    assert (plain["curated_a"].population, plain["curated_a"].violations) == (3, 2)


def test_file_completeness_reports_what_it_excluded_and_what_is_pending(spark) -> None:
    """Exclusions are counted, not hidden: a file whose only missing row is a test record passes,
    and the result says one row was excluded and one file is not due yet."""
    late = datetime(2026, 1, 16, 9, 0)  # first loaded after the deadline range: pending
    up, dn = file_tables(spark, "fc_excl",
                         [("a", IN_WINDOW, "rt_0001.seq"), ("b", IN_WINDOW, "rt_0001.seq"),
                          ("x1", IN_WINDOW, "rt_0001.seq"), ("c", late, "rt_0002.seq")],
                         [("a", IN_WINDOW, "rt_0001.seq"), ("b", IN_WINDOW, "rt_0001.seq")])
    with_exclusion = Dataset(**{**dn.model_dump(), "hop_exclusions": {"file_up": "id LIKE 'x%'"}})
    [r] = run(spark, FileCompleteness(), with_exclusion, up)
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)
    assert r.observed == ("0 of 1 upstream files have fewer rows here; "
                          "1 row excluded by hop_exclusions, 1 file pending (within sla_hours)")
    [plain] = run(spark, FileCompleteness(), dn, up)
    assert (plain.state, plain.violations) == ("FAILED", 1)  # without the contract, the test row looks lost
