"""Gate A remediation, release N: CHAIN_COMPLETENESS carries PENDING records by key encoding (spec
section 6, "Key events": CHAIN_COMPLETENESS's PENDING records follow the same rules).

The carry reads PENDING records of this engine's key encoding, REKEYED taken as no event. A PENDING
record of another encoding (a later engine's, after a rollback) cannot be read by this engine's key;
when one is open, the chain's first edge is judged in full, as on the full sweep, and the record is
closed with REKEYED in its own encoding, so no record it stood for is forgotten (CA-02).
"""

from __future__ import annotations

from datetime import datetime, timezone

from tests.checks.test_chain import (
    CHAIN_DQ, EDGE2, EVENT, LABEL, NEXT, RAW, carry_store, chain_at, events_of, key_of, pending_window, publish,
    scope,
)
from tests.store.conftest import append_rows

UTC = timezone.utc
SEEDED = datetime(2026, 1, 16, 12, tzinfo=UTC)

__all__ = ["carry_store"]  # the fixture, re-exported for pytest


def seed(spark, rows: list[tuple]) -> None:
    """(key_hash, key_value, event, state_detail, key_encoding) as committed CHAIN_COMPLETENESS events."""
    append_rows(spark, CHAIN_DQ, "dq_key_event", [
        {"run_date": SEEDED.date(), "run_id": f"seed-{i}", "evaluation_id": f"seed-{i}", "dataset": RAW,
         "check_id": "CHAIN_COMPLETENESS", "hop": LABEL, "key_hash": h, "key_value": v, "event": e,
         "state_detail": d, "observed_at": SEEDED, "key_encoding": enc}
        for i, (h, v, e, d, enc) in enumerate(rows)])


def test_an_encoding_1_pending_record_is_still_carried_after_rekeyed(spark, carry_store) -> None:
    first = chain_at(spark, pending_window(spark, "ch_enc_rekeyed", gold_holds_it=False), EVENT)
    publish(spark, first, "r1", EVENT)
    [pending] = [h for h, e, _, _ in events_of(first) if e == "PENDING"]
    later = datetime(2026, 1, 16, 13, tzinfo=UTC)
    append_rows(spark, CHAIN_DQ, "dq_key_event", [{
        "run_date": later.date(), "run_id": "n1", "evaluation_id": "n1", "dataset": RAW,
        "check_id": "CHAIN_COMPLETENESS", "hop": LABEL, "key_hash": pending, "key_value": key_of("123402"),
        "event": "REKEYED", "state_detail": "key encoding 2", "observed_at": later, "key_encoding": 1}])
    second = chain_at(spark, pending_window(spark, "ch_enc_rekeyed", gold_holds_it=True), NEXT)
    assert scope(second)["carried_in"] == 1  # REKEYED is no event for the carry
    assert [(e, v) for _, e, _, v in events_of(second)] == [("RESOLVED", key_of("123402"))]


def test_an_open_encoding_2_pending_record_makes_the_first_edge_judged_in_full_and_is_closed(spark, carry_store) -> None:
    datasets = pending_window(spark, "ch_enc_foreign", gold_holds_it=True)
    quiet = chain_at(spark, datasets, NEXT)
    assert quiet.reason_code == "empty_population"  # NEXT's range holds nothing of its own
    seed(spark, [("lpb-chain-1", "synthetic-lpb-key", "PENDING", f"pending at {EDGE2}", 2)])
    r = chain_at(spark, datasets, NEXT)
    assert (r.state, r.population) == ("PASSED", 2)  # every source record judged, as on the full sweep
    assert scope(r)["other_key_encoding_open"] == 1
    rows = [(row.key_hash, row.event, row.state_detail, row.asDict().get("key_encoding", 1))
            for row in r.key_events.collect()]
    assert ("lpb-chain-1", "REKEYED", "key encoding 1 (rollback)", 2) in rows
    assert len({(enc, h) for h, _, _, enc in rows}) == len(rows)  # one event per identity
    publish(spark, r, "r2", NEXT)
    again = chain_at(spark, datasets, NEXT)
    assert again.reason_code == "empty_population" and "other_key_encoding_open" not in scope(again)


def test_an_encoding_2_pending_record_is_never_carried_as_this_engines_key(spark, carry_store) -> None:
    datasets = pending_window(spark, "ch_enc_notcarried", gold_holds_it=True)
    seed(spark, [("lpb-chain-2", key_of("123402"), "PENDING", f"pending at {EDGE2}", 2)])
    r = chain_at(spark, datasets, NEXT)
    assert scope(r)["carried_in"] == 0
