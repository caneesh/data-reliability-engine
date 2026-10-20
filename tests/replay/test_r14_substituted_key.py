"""R14 (proof scenario PS1): equal counts with a substituted business key fail the edge ledger.

Reconciliation model section 10, row 1: "Edge ledger FAILED: the dropped key is `missing`, and its
substitute is an `orphan`. Counts are never the evidence." Curated holds two due keys and gold two
rows, so every count agrees, yet one key never reached gold and a key no upstream record maps to
took its place. Through `dre run`, on the sample configuration (the hop is a declared
union_partition read through gold's feed_filter, so orphans are checked; decision D5).
"""

from __future__ import annotations

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.replay.conftest import latest
from tests.replay.ledger_views import assert_balanced, current_ledger, lines, nonzero, scope
from tests.replay.test_r04_to_r07_hops import setup


def test_r14_equal_counts_with_a_substituted_key_is_a_missing_key_and_an_orphan(spark, tmp_path) -> None:
    """PS1 / model section 10 row 1: missing 1 (the dropped key) and orphans 1 (its substitute), FAILED,
    although both layers hold two rows."""
    replay = setup(spark, tmp_path, "r14")
    loaded = curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(sub_id="123401", updated=loaded),
                                                          curated_row(sub_id="123402", updated=loaded)])
    # Gold wrote 123401 and, in place of 123402, 123499: a key no curated record has.
    create_table(spark, replay.gold, GOLD_COLUMNS, [
        gold_row(sub_id="000123401", record=loaded, loaded=gold_load_time(11)),
        gold_row(sub_id="000123499", record=loaded, loaded=gold_load_time(11))])
    assert spark.table(replay.curated).count() == spark.table(replay.gold).count() == 2  # counts agree

    assert main(["run", "--conf", str(replay.conf)]) == 0
    result = current_ledger(spark, replay)
    assert (result.state, result.population, result.violations) == ("FAILED", 2, 2)
    assert result.observed == "due 2 (pending 0): delivered 1, missing 1; orphans 1; fails: 1 orphan; 1 missing"
    ledger = lines(spark, replay, result.evaluation_id)
    assert nonzero(ledger) == {("due", None): 2, ("delivered", None): 1, ("missing", None): 1, ("orphan", None): 1}
    assert ledger[("missing", None)][1] == "deficit" and ledger[("orphan", None)][1] == "breach"
    assert_balanced(ledger, 2)  # the orphan is beside the identity, not in it
    assert scope(result)["orphans"] == "checked"
    assert scope(result)["triage"] == [
        "1 row(s) here that no upstream row maps to: look for substituted keys or another writer",
        "1 missing: HOP_KEY_CURRENCY and HOP_VALUE_AGREEMENT name these keys and their causes"]
    # The release 1 key check sees the dropped key too; only the ledger sees the substitute.
    [currency] = latest(spark, replay)[("gold_member_coverage", "HOP_KEY_CURRENCY")]
    assert (currency.state, currency.observed) == ("FAILED", "1 missing and 0 stale of 2 keys")


def test_r14_the_same_counts_with_the_right_keys_pass(spark, tmp_path) -> None:
    """PS1, the PASS side: the same two rows each side, the right keys, nothing missing and no orphan.
    The sample's union attribution is unproven until build step 5, so the PASS is qualified and not
    healthy (C3-02)."""
    replay = setup(spark, tmp_path, "r14_ok")
    loaded = curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(sub_id="123401", updated=loaded),
                                                          curated_row(sub_id="123402", updated=loaded)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [
        gold_row(sub_id="000123401", record=loaded, loaded=gold_load_time(11)),
        gold_row(sub_id="000123402", record=loaded, loaded=gold_load_time(11))])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    result = current_ledger(spark, replay)
    assert (result.state, result.population, result.violations) == ("PASSED", 2, 0)
    assert result.observed == "due 2 (pending 0): delivered 2; orphans 0"
    assert (result.qualifications, result.healthy) == ("attribution_unverified", False)
    assert_balanced(lines(spark, replay, result.evaluation_id), 2)
