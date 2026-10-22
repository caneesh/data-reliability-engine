"""Release N independent review: adversarial checks of the highest-risk compatibility paths.

Each test states what it probes. Assertions record the behaviour observed at b72b45a; a test named
`..._defect_...` reproduces a finding of docs/reviews/release-n/independent-review.md.
"""

from __future__ import annotations

import importlib.util
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.hop.common import hop_label, key_hash
from hcsc.datalake.dre.checks.hop.key_currency import KeyCurrency
from hcsc.datalake.dre.store import writer
from hcsc.datalake.dre.store.local_setup import create_store
from tests.checks.test_hop import BEFORE, EVENT, KEY, SECRET, curated, gold
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_row
from tests.fixtures.settings import SETTINGS
from tests.store.conftest import append_rows

UTC = timezone.utc
DQ = "dq_release_n_review"
BASELINE = Path(os.environ.get("BASELINE", "/nonexistent"))
D = date(2026, 1, 15)


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    spark.sql(f"DROP DATABASE IF EXISTS {DQ} CASCADE")
    create_store(spark, DQ)
    yield
    spark.sql(f"DROP DATABASE IF EXISTS {DQ} CASCADE")


def ev(h, event, at, run, enc=None, detail="MISSING", pred=None, hop="u->d"):
    return {"run_date": at.date(), "run_id": run, "evaluation_id": f"e-{run}-{h}", "dataset": "d",
            "check_id": "HOP_KEY_CURRENCY", "hop": hop, "key_hash": h, "key_value": "synthetic", "event": event,
            "state_detail": detail, "observed_at": at, "key_encoding": enc, "predecessor_hash": pred}


def baseline_view(spark) -> str:
    """The pre-release-N v_open_keys, created beside the new one from the baseline's store/ddl.sql."""
    if not BASELINE.exists():
        pytest.skip("BASELINE worktree not set")
    text = (BASELINE / "src/hcsc/datalake/dre/store/ddl.sql").read_text(encoding="utf-8")
    sql = text[text.index("CREATE OR REPLACE VIEW {{ dq_database }}.v_open_keys AS"):]
    sql = sql[:sql.index(";")].replace("{{ dq_database }}.v_open_keys", f"{DQ}.v_open_keys_baseline")
    spark.sql(sql.replace("{{ dq_database }}", DQ))
    return f"{DQ}.v_open_keys_baseline"


# --- 7, 8: the views ---

def test_null_and_explicit_encoding_1_are_one_identity(spark) -> None:
    """Legacy decoding: an event stored before migration 2 (NULL) and a release N event (1) of the same hash
    are one identity, one spell, first flagged at the legacy event."""
    t = datetime(2026, 1, 15, 1, tzinfo=UTC)
    append_rows(spark, DQ, "dq_key_event", [ev("a1", "FLAGGED", t, "a-r1"),
                                           ev("a1", "STILL_FLAGGED", t + timedelta(hours=1), "a-r2", enc=1)])
    rows = spark.sql(f"SELECT * FROM {DQ}.v_open_keys WHERE key_hash = 'a1'").collect()
    assert len(rows) == 1 and rows[0].key_encoding == 1 and rows[0].first_flagged_at == t.replace(tzinfo=None)


def test_defect_same_instant_cleared_and_reflagged_drops_an_open_key_from_v_open_keys(spark) -> None:
    """A key CLEARED and FLAGGED again at the same observed_at by two runs: the latest event (run_id order) is
    FLAGGED, so the key is open. The baseline view lists it (first_flagged_at NULL); release N's view drops
    it, because its spell requires observed_at > last CLEARED and the open row is INNER JOINed to the spell."""
    old = baseline_view(spark)
    t = datetime(2026, 1, 15, 2, tzinfo=UTC)
    append_rows(spark, DQ, "dq_key_event", [ev("b1", "FLAGGED", t - timedelta(hours=1), "b-r0"),
                                           ev("b1", "CLEARED", t, "b-r1"), ev("b1", "FLAGGED", t, "b-r2")])
    before = spark.sql(f"SELECT key_hash, latest_event, first_flagged_at FROM {old} WHERE key_hash = 'b1'").collect()
    after = spark.sql(f"SELECT key_hash FROM {DQ}.v_open_keys WHERE key_hash = 'b1'").collect()
    assert [(r.latest_event, r.first_flagged_at) for r in before] == [("FLAGGED", None)]
    assert after == []  # the finding: an open key missing from the view


def test_the_baseline_and_release_n_views_agree_on_ordinary_histories(spark) -> None:
    old = baseline_view(spark)
    t = datetime(2026, 1, 16, 1, tzinfo=UTC)
    rows = []
    for i in range(30):
        h = f"c{i}"
        rows += [ev(h, "FLAGGED", t, f"c-r1-{i}")]
        if i % 3 == 0:
            rows += [ev(h, "CLEARED", t + timedelta(hours=1), f"c-r2-{i}")]
        if i % 5 == 0:
            rows += [ev(h, "FLAGGED", t + timedelta(hours=2), f"c-r3-{i}")]
        if i % 2 == 0:
            rows += [ev(h, "STILL_FLAGGED", t + timedelta(hours=3), f"c-r4-{i}")]
    append_rows(spark, DQ, "dq_key_event", rows)
    cols = "key_hash, latest_event, state_detail, first_flagged_at, last_observed_at, run_id, evaluation_id"
    a = sorted(map(tuple, spark.sql(f"SELECT {cols} FROM {old} WHERE key_hash LIKE 'c%'").collect()))
    b = sorted(map(tuple, spark.sql(f"SELECT {cols} FROM {DQ}.v_open_keys WHERE key_hash LIKE 'c%'").collect()))
    assert a == b and len(a) > 0


# --- 4: writer; 10: rollback below N ---

def test_the_release_n_minus_1_writer_refuses_a_migrated_dq_key_event(spark) -> None:
    """Executed, not only documented: the baseline's store/writer.py (loaded from the worktree) raises on a
    frame without migration 2's columns."""
    if not BASELINE.exists():
        pytest.skip("BASELINE worktree not set")
    spec = importlib.util.spec_from_file_location("baseline_writer", BASELINE / "src/hcsc/datalake/dre/store/writer.py")
    old = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(old)
    cols = [c for c in spark.table(f"{DQ}.dq_key_event").columns if c not in ("key_encoding", "predecessor_hash")]
    schema = spark.table(f"{DQ}.dq_key_event").select(*cols).schema
    row = {c: None for c in cols}
    row.update({"run_id": "old", "key_hash": "x", "event": "FLAGGED", "run_date": D})
    frame = spark.createDataFrame([tuple(row[c] for c in cols)], schema)
    with pytest.raises(ValueError, match="missing"):
        old.append(frame, DQ, "dq_key_event")
    writer.append(frame, DQ, "dq_key_event")  # release N tolerates it


def test_writer_tolerance_also_accepts_a_check_result_without_evaluation_mode(spark) -> None:
    """Observation (latent): the tolerance covers migration 1's column too, so a result frame that forgot
    evaluation_mode is now written with NULL, which v_latest_result reads from execution_type: an
    AS_OF_REPLAY written that way would read as CURRENT_RECHECK. No current writer omits it (the full suite)."""
    cols = [c for c in spark.table(f"{DQ}.dq_check_result").columns if c != "evaluation_mode"]
    schema = spark.table(f"{DQ}.dq_check_result").select(*cols).schema
    row = {c: None for c in cols}
    row.update({"run_id": "asof", "evaluation_id": "asof", "dataset": "d", "check_id": "HOP_LEDGER", "state": "PASSED",
                "execution_type": "AS_OF_REPLAY", "run_date": D})
    writer.append(spark.createDataFrame([tuple(row[c] for c in cols)], schema), DQ, "dq_check_result")
    [r] = spark.sql(f"SELECT evaluation_mode FROM {DQ}.dq_check_result WHERE run_id = 'asof'").collect()
    assert r.evaluation_mode is None


# --- 6, 10: readers, rollback ---

def tables(spark, name):
    create_table(spark, f"hop.{name}_cur", CURATED_COLUMNS, [curated_row(updated=BEFORE)])
    create_table(spark, f"hop.{name}_gold", GOLD_COLUMNS, [])
    return curated(f"hop.{name}_cur"), gold(f"hop.{name}_gold", dataset=f"gold_{name}")


def kc(spark, dn, up, **ctx):
    c = CheckContext(spark, dn, None, SETTINGS, "UTC", DQ, upstreams={up.dataset: up}, key_secret=SECRET, **ctx)
    [r], _ = run_check(KeyCurrency(), c, EVENT)
    return r


def seed(spark, dn, up, rows):
    hop = hop_label(up.dataset, dn.dataset)
    append_rows(spark, DQ, "dq_key_event", [
        {**ev(h, e, at, f"s-{dn.dataset}-{i}", enc=enc, detail=d, hop=hop), "dataset": dn.dataset, "key_value": v}
        for i, (h, v, e, d, enc, at) in enumerate(rows)])


def test_rollback_sequence_n1_continuation_then_release_n(spark) -> None:
    """The N+1 -> N reader contract on a store an N+1 engine wrote (fixtures stand in for N+1): legacy L
    flagged under encoding 1; N+1 REKEYED it and continued it as S (encoding 2, predecessor L). Release N
    must continue L (spell and age intact), close S with REKEYED, and judge the hop in full."""
    up, dn = tables(spark, "rv_rollback")
    t0, t1 = datetime(2026, 1, 10, 12, tzinfo=UTC), datetime(2026, 1, 14, 12, tzinfo=UTC)
    L = key_hash(SECRET, KEY)
    seed(spark, dn, up, [(L, KEY, "FLAGGED", "MISSING", None, t0),
                         (L, KEY, "REKEYED", "key encoding 2", 1, t1)])
    append_rows(spark, DQ, "dq_key_event", [{**ev("S-lpb", "STILL_FLAGGED", t1, "n1-run", enc=2, pred=L,
                                                  hop=hop_label(up.dataset, dn.dataset)),
                                             "dataset": dn.dataset}])
    r = kc(spark, dn, up)
    got = sorted((x.key_hash, x.event, x.state_detail, x.key_encoding) for x in r.key_events.collect())
    assert got == sorted([(L, "STILL_FLAGGED", "MISSING", 1), ("S-lpb", "REKEYED", "key encoding 1 (rollback)", 2)])
    assert "of another key encoding open at this hop" in r.observed


def test_p2_a_retried_investigation_during_a_rollback_judges_the_whole_hop(spark) -> None:
    """A retried cause investigation (record_key_events False, judge_open_keys True, runner.py:795) also sees
    the other-encoding identity and judges every key: its failure list can exceed the original evaluation's.
    Causes only; no result or key event is written by a retry."""
    up, dn = tables(spark, "rv_retry")
    seed(spark, dn, up, [("S2-lpb", "synthetic", "FLAGGED", "MISSING", 2, datetime(2026, 1, 14, 12, tzinfo=UTC))])
    plain = kc(spark, dn, up, record_key_events=False)
    retry = kc(spark, dn, up, record_key_events=False, judge_open_keys=True)
    assert plain.reason_code == "empty_population"          # an ordinary recheck: nothing judged
    assert (retry.state, retry.population) == ("FAILED", 1)  # the retry judged the old key too
    assert retry.key_events is None


def test_nonnumeric_or_negative_encodings_cannot_be_stored() -> None:
    """key_encoding is INT: only integers reach the readers; 0 or negative values are handled as foreign."""
    assert re.search(r"key_encoding INT", (Path(__file__).resolve().parents[4] / "src/hcsc/datalake/dre/store/ddl.sql")
                     .read_text(encoding="utf-8"))
