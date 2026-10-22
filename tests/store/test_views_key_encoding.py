"""Gate A remediation, release N: the final key-event views (spec section 5).

`v_open_keys` identifies a key by its encoding and its hash, so an encoding-1 hash and an encoding-2
hash are never the same identity; REKEYED never ends an open spell and is never open itself; a
provable continuation (an event naming a predecessor that was REKEYED) keeps the predecessor's age;
an encoding-1 identity "awaiting successors" is `legacy_unresolved`. `v_key_lineage` lists the events
that name a predecessor (continuation or ambiguous; no rollback kind: INC-1 deferred). Both read
committed events only.
"""

from __future__ import annotations

from datetime import date

from tests.store.conftest import append_rows, ts

D = date(2026, 1, 15)


def ev(key_hash: str, event: str, day: int, *, enc: int | None = None, pred: str | None = None,
       detail: str = "MISSING", hop: str = "u->d", run: str | None = None) -> dict:
    tag = key_hash[0]  # one evaluation per test and day: the store fixture commits each evaluation once
    return {"run_date": D, "run_id": run or f"r{day}-{tag}", "evaluation_id": f"e{day}-{tag}", "dataset": "d",
            "check_id": "HOP_KEY_CURRENCY", "hop": hop, "key_hash": key_hash, "key_value": "synthetic-key",
            "event": event, "state_detail": detail, "observed_at": ts(day), "key_encoding": enc,
            "predecessor_hash": pred}


def open_keys(spark, store) -> dict:
    return {(r.key_encoding, r.key_hash): r for r in spark.table(f"{store}.v_open_keys").collect()}


def test_legacy_events_read_as_encoding_1_and_keep_the_existing_open_semantics(spark, store) -> None:
    append_rows(spark, store, "dq_key_event", [
        ev("a_open", "FLAGGED", 1), ev("a_open", "STILL_FLAGGED", 2),
        ev("a_cleared", "FLAGGED", 1), ev("a_cleared", "CLEARED", 2)])
    rows = open_keys(spark, store)
    assert set(rows) == {(1, "a_open")}
    r = rows[(1, "a_open")]
    assert (r.latest_event, r.category, r.predecessor_hash) == ("STILL_FLAGGED", "active", None)
    assert r.first_flagged_at == ts(1).replace(tzinfo=None)
    assert "key_value" not in spark.table(f"{store}.v_open_keys").columns


def test_rekeyed_is_not_open_and_does_not_end_a_spell(spark, store) -> None:
    append_rows(spark, store, "dq_key_event", [
        ev("b_rekeyed", "FLAGGED", 1), ev("b_rekeyed", "REKEYED", 3, detail="key encoding 2"),
        ev("b_back", "FLAGGED", 1), ev("b_back", "REKEYED", 3, detail="key encoding 2"),
        ev("b_back", "STILL_FLAGGED", 5)])            # a release N engine continued it after a rollback
    rows = open_keys(spark, store)
    assert (1, "b_rekeyed") not in rows
    assert rows[(1, "b_back")].first_flagged_at == ts(1).replace(tzinfo=None)  # REKEYED did not end the spell


def test_an_encoding_1_and_an_encoding_2_hash_are_never_one_identity(spark, store) -> None:
    """The same hash text under two encodings: two identities, each with its own state."""
    append_rows(spark, store, "dq_key_event", [
        ev("c_same", "FLAGGED", 1), ev("c_same", "CLEARED", 2),
        ev("c_same", "FLAGGED", 3, enc=2)])
    rows = open_keys(spark, store)
    assert (2, "c_same") in rows and (1, "c_same") not in rows
    assert rows[(2, "c_same")].first_flagged_at == ts(3).replace(tzinfo=None)


def test_a_provable_continuation_keeps_the_predecessors_age(spark, store) -> None:
    append_rows(spark, store, "dq_key_event", [
        ev("d_legacy", "FLAGGED", 1), ev("d_legacy", "REKEYED", 4, enc=1, detail="key encoding 2"),
        ev("d_succ", "STILL_FLAGGED", 4, enc=2, pred="d_legacy", run="r4b")])
    r = open_keys(spark, store)[(2, "d_succ")]
    assert (r.category, r.predecessor_hash, r.first_flagged_at) == ("active", "d_legacy", ts(1).replace(tzinfo=None))


def test_an_ambiguous_legacy_identity_stays_open_with_its_age_and_successors_have_their_own(spark, store) -> None:
    append_rows(spark, store, "dq_key_event", [
        ev("e_legacy", "FLAGGED", 1), ev("e_legacy", "STILL_FLAGGED", 4, enc=1, detail="awaiting successors"),
        ev("e_succ", "FLAGGED", 4, enc=2, pred="e_legacy", run="r4b")])
    rows = open_keys(spark, store)
    legacy, succ = rows[(1, "e_legacy")], rows[(2, "e_succ")]
    assert (legacy.category, legacy.first_flagged_at) == ("legacy_unresolved", ts(1).replace(tzinfo=None))
    assert (succ.category, succ.first_flagged_at) == ("active", ts(4).replace(tzinfo=None))


def test_lineage_lists_continuations_and_ambiguous_transitions_only(spark, store) -> None:
    lineage = {(r.predecessor_hash, r.successor_hash): r for r in spark.table(f"{store}.v_key_lineage").collect()}
    assert {k: (v.kind, v.predecessor_encoding, v.successor_encoding) for k, v in lineage.items()} == {
        ("d_legacy", "d_succ"): ("continuation", 1, 2), ("e_legacy", "e_succ"): ("ambiguous", 1, 2)}
    assert "key_value" not in spark.table(f"{store}.v_key_lineage").columns


def test_uncommitted_events_are_invisible_to_both_views(spark, store) -> None:
    append_rows(spark, store, "dq_key_event", [ev("f_uncommitted", "FLAGGED", 6, enc=2, pred="d_legacy")],
                commit=False)
    assert (2, "f_uncommitted") not in open_keys(spark, store)
    assert "f_uncommitted" not in {r.successor_hash for r in spark.table(f"{store}.v_key_lineage").collect()}


def test_latest_event_selection_is_deterministic_across_shuffle_partitions(spark, store) -> None:
    prior = spark.conf.get("spark.sql.shuffle.partitions")
    seen = []
    try:
        for n in ("1", "7"):
            spark.conf.set("spark.sql.shuffle.partitions", n)
            seen.append(sorted((k, r.latest_event, r.first_flagged_at, r.category)
                               for k, r in open_keys(spark, store).items()))
    finally:
        spark.conf.set("spark.sql.shuffle.partitions", prior)
    assert seen[0] == seen[1]
