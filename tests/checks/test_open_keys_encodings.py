"""Gate A remediation, release N: HOP_KEY_CURRENCY reads open keys by key encoding (spec section 6,
"Key events").

A release N engine computes key encoding 1. It reads the open identities of encoding 1 (a stored row
without the column is encoding 1), taking REKEYED as no event, so a spell that an N+1 engine continued
under encoding 2 is open again for it, with its age. An identity of another encoding (encoding 2,
written by N+1 before a rollback; or any encoding it does not know) is never matched to an encoding-1
key, even when the hash text is the same. When such an identity is open at the hop, the hop is judged
as on the full sweep, and the identity is closed with REKEYED "key encoding 1 (rollback)" in its own
encoding, so no discrepancy is lost. Each evaluation writes at most one event per identity.
"""

from __future__ import annotations

from datetime import datetime, timezone

from hcsc.datalake.dre.checks.hop.common import hop_label, key_hash
from hcsc.datalake.dre.checks.hop.key_currency import KeyCurrency
from hcsc.datalake.dre.checks.events import Event
from tests.checks.test_hop import BEFORE, DQ, IN_WINDOW, KEY, SECRET, record, run, tables
from tests.fixtures.layers import curated_row, gold_row
from tests.store.conftest import append_rows

import pytest

from hcsc.datalake.dre.store.local_setup import create_store

UTC = timezone.utc
SEEDED = datetime(2026, 1, 14, 12, tzinfo=UTC)
REKEYED_AT = datetime(2026, 1, 15, 1, tzinfo=UTC)


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


def seed(spark, dn, up, rows: list[tuple]) -> None:
    """(key_hash, key_value, event, state_detail, key_encoding, observed_at) as committed events."""
    hop = hop_label(up.dataset, dn.dataset)
    append_rows(spark, DQ, "dq_key_event", [
        {"run_date": at.date(), "run_id": f"seed-{dn.dataset}-{i}", "evaluation_id": f"seed-{dn.dataset}-{i}",
         "dataset": dn.dataset, "check_id": "HOP_KEY_CURRENCY", "hop": hop, "key_hash": h, "key_value": v,
         "event": e, "state_detail": d, "observed_at": at, "key_encoding": enc}
        for i, (h, v, e, d, enc, at) in enumerate(rows)])


def events(result) -> list[tuple]:
    return sorted((r.key_hash, r.event, r.state_detail, r.asDict().get("key_encoding", 1))
                  for r in result.key_events.collect())


def test_a_legacy_open_key_is_read_as_encoding_1_and_written_with_it(spark) -> None:
    up, dn = tables(spark, "enc_legacy", [curated_row(updated=BEFORE)], [])
    h = key_hash(SECRET, KEY)
    seed(spark, dn, up, [(h, KEY, "FLAGGED", "MISSING", None, SEEDED)])  # stored before migration 2
    [r] = run(spark, KeyCurrency(), dn, up, secret=SECRET)
    assert (r.state, r.population) == ("FAILED", 1)
    assert events(r) == [(h, "STILL_FLAGGED", "MISSING", 1)]
    record(spark, r, dn, "enc-legacy-1", datetime(2026, 1, 16, 12, tzinfo=UTC))
    [stored] = spark.sql(f"SELECT key_encoding FROM {DQ}.dq_key_event WHERE run_id = 'enc-legacy-1'").collect()
    assert stored.key_encoding == 1


def test_rekeyed_is_no_event_for_the_open_key_read(spark) -> None:
    """After a rollback from N+1: the legacy key's latest event is REKEYED; release N continues its spell."""
    up, dn = tables(spark, "enc_rekeyed", [curated_row(updated=BEFORE)], [])
    h = key_hash(SECRET, KEY)
    seed(spark, dn, up, [(h, KEY, "FLAGGED", "MISSING", None, SEEDED),
                         (h, KEY, "REKEYED", "key encoding 2", 1, REKEYED_AT)])
    [r] = run(spark, KeyCurrency(), dn, up, secret=SECRET)
    assert (r.state, r.population) == ("FAILED", 1)
    assert events(r) == [(h, "STILL_FLAGGED", "MISSING", 1)]
    record(spark, r, dn, "enc-rekeyed-1", datetime(2026, 1, 16, 12, tzinfo=UTC))
    [open_key] = spark.sql(f"SELECT * FROM {DQ}.v_open_keys WHERE dataset = '{dn.dataset}'").collect()
    assert open_key.first_flagged_at == SEEDED.replace(tzinfo=None)  # REKEYED did not end the spell


def test_an_open_encoding_2_identity_is_judged_by_a_full_sweep_and_closed_with_rekeyed(spark) -> None:
    """The key was loaded before the judged range: an ordinary window does not judge it. An encoding-2
    identity open at the hop makes this evaluation judge every key, as the full sweep does."""
    up, dn = tables(spark, "enc_foreign", [curated_row(updated=BEFORE)], [])
    [r] = run(spark, KeyCurrency(), dn, up, secret=SECRET)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")  # without a foreign identity
    seed(spark, dn, up, [("lpb-hash-1", "synthetic-lpb-key", "FLAGGED", "MISSING", 2, SEEDED)])
    [r] = run(spark, KeyCurrency(), dn, up, secret=SECRET)
    assert (r.state, r.population, r.violations) == ("FAILED", 1, 1)
    assert events(r) == sorted([(key_hash(SECRET, KEY), "FLAGGED", "MISSING", 1),
                                ("lpb-hash-1", "REKEYED", "key encoding 1 (rollback)", 2)])
    record(spark, r, dn, "enc-foreign-1", datetime(2026, 1, 16, 12, tzinfo=UTC))
    # closed: the next evaluation sees no foreign identity open, and judges as usual
    later = Event("gold", datetime(2026, 1, 16, 12, tzinfo=UTC), datetime(2026, 1, 16, 13, tzinfo=UTC))
    [r] = run(spark, KeyCurrency(), dn, up, event=later, secret=SECRET)
    assert events(r) == [(key_hash(SECRET, KEY), "STILL_FLAGGED", "MISSING", 1)]
    assert "lpb-hash-1" not in {row.key_hash for row in spark.table(f"{DQ}.v_open_keys").collect()}


def test_the_same_hash_text_in_another_encoding_is_not_the_encoding_1_key(spark) -> None:
    """Hashes of different encodings are never interchangeable: an encoding-2 event whose hash text equals
    this key's encoding-1 hash does not make the key open, and the two identities get one event each."""
    up, dn = tables(spark, "enc_same_text", [curated_row(updated=IN_WINDOW)], [])
    h = key_hash(SECRET, KEY)
    seed(spark, dn, up, [(h, "synthetic-lpb-key", "FLAGGED", "MISSING", 2, SEEDED)])
    [r] = run(spark, KeyCurrency(), dn, up, secret=SECRET)
    assert events(r) == sorted([(h, "FLAGGED", "MISSING", 1),       # newly flagged: not open as encoding 1
                                (h, "REKEYED", "key encoding 1 (rollback)", 2)])
    assert "key open from earlier runs" not in r.observed


def test_an_unknown_encoding_is_handled_as_foreign(spark) -> None:
    """Fail conservatively: an encoding this engine does not know is never read as its own, and makes the
    hop judge every key."""
    up, dn = tables(spark, "enc_unknown", [curated_row(updated=BEFORE)], [])
    seed(spark, dn, up, [("enc3-hash", "synthetic", "FLAGGED", "MISSING", 3, SEEDED)])
    [r] = run(spark, KeyCurrency(), dn, up, secret=SECRET)
    assert (r.state, r.population) == ("FAILED", 1)
    assert ("enc3-hash", "REKEYED", "key encoding 1 (rollback)", 3) in events(r)


def test_an_uncommitted_encoding_2_event_is_invisible(spark) -> None:
    up, dn = tables(spark, "enc_uncommitted", [curated_row(updated=BEFORE)], [])
    hop = hop_label(up.dataset, dn.dataset)
    append_rows(spark, DQ, "dq_key_event", [{
        "run_date": SEEDED.date(), "run_id": "died", "evaluation_id": "died", "dataset": dn.dataset,
        "check_id": "HOP_KEY_CURRENCY", "hop": hop, "key_hash": "lpb-hash-2", "key_value": "synthetic",
        "event": "FLAGGED", "state_detail": "MISSING", "observed_at": SEEDED, "key_encoding": 2}], commit=False)
    [r] = run(spark, KeyCurrency(), dn, up, secret=SECRET)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")  # no full sweep forced


def test_without_a_secret_nothing_is_read_or_written(spark) -> None:
    up, dn = tables(spark, "enc_nosecret", [curated_row(updated=BEFORE)], [])
    seed(spark, dn, up, [("lpb-hash-3", "synthetic", "FLAGGED", "MISSING", 2, SEEDED)])
    [r] = run(spark, KeyCurrency(), dn, up)
    assert (r.state, r.reason_code, r.key_events) == ("DID_NOT_RUN", "empty_population", None)


def test_at_most_one_event_per_identity(spark) -> None:
    up, dn = tables(spark, "enc_one_event", [curated_row(sub_id="1", updated=BEFORE),
                                             curated_row(sub_id="2", updated=IN_WINDOW)], [gold_row(record=BEFORE)])
    seed(spark, dn, up, [("lpb-hash-4", "synthetic", "FLAGGED", "MISSING", 2, SEEDED),
                         (key_hash(SECRET, "1|01|2026-01-01|AGR-A"), "1|01|2026-01-01|AGR-A", "FLAGGED", "MISSING",
                          None, SEEDED)])
    [r] = run(spark, KeyCurrency(), dn, up, secret=SECRET)
    pairs = [(row.key_encoding, row.key_hash) for row in r.key_events.collect()]
    assert len(pairs) == len(set(pairs)) == 3
