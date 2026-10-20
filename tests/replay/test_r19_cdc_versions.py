"""R19 (proof scenario PS6): CDC versions have one deterministic expected ledger state each.

Reconciliation model section 10, row 6: "The latest version by `grain.version` as of C. Tombstone:
`deleted` or `deleted_not_applied`. An older version arriving late: no change. A newer one out of
order: judged at its own deadline. Each case gives one deterministic ledger (example 9.1)." Section
4 ("Versions, out-of-order and late data") and section 5 (the member pipeline's void: a new version
whose end date equals its effective date, applied soft, the row kept in gold).

The sample configuration declares that deletion rule. Window W ends 15 minutes before the run (the
cutoff C) and the SLA is 8 hours, so a version loaded 12 hours ago is due, one loaded 3 hours ago is
pending, and one loaded 5 minutes ago is after C. Each case is one curated key:

| Key | Curated versions (record time, loaded) | Gold holds | Expected |
| --- | --- | --- | --- |
| 123401 | v1 (30h, 30h), v2 terminated (12h, 12h): a CDC update | v2 | delivered |
| 123407 | the same update | v1 | stale |
| 123402 | v1 open (30h, 30h), v2 void (12h, 12h) | v2, voided | deleted |
| 123403 | the same void | v1, open | deleted_not_applied (breach) |
| 123404 | v2 (20h, 20h), then v1, older, arriving late (40h, 10h): out of order | v2 | delivered (judged on v2; judged on the late v1 it would be divergent) |
| 123405 | v1 (30h, 30h), v2 (4h, 3h), inside its SLA | v1 | pending, outside the ledger (not stale) |
| 123406 | v1 (30h, 30h), v2 loaded after C (5 minutes ago) | v1 | delivered (v2 belongs to a later window) |
"""

from __future__ import annotations

from hcsc.datalake.dre.checks.hop.common import key_hash
from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.replay.conftest import latest
from tests.replay.ledger_views import assert_balanced, current_ledger, lines, nonzero, scope
from tests.replay.test_r04_to_r07_hops import SECRET, setup

VOID = "2026-01-01"         # end date = effective date (curated_row's and gold_row's effective date)
TERMINATED = "2026-06-30"


def hours(h: float):
    return curated_load_time(h)


def test_r19_cdc_update_tombstone_out_of_order_and_late_arrival_each_have_one_ledger_state(spark, tmp_path) -> None:
    """PS6 / model section 10 row 6 (example 9.1): due 6, pending 1; deleted 1, deleted_not_applied 1,
    delivered 3, stale 1; FAILED (the unapplied void is a breach, the unapplied update a deficit)."""
    replay = setup(spark, tmp_path, "r19")
    v1, v2 = hours(30), hours(12)
    create_table(spark, replay.curated, CURATED_COLUMNS, [
        # a CDC update, applied (123401) and not applied (123407)
        curated_row(sub_id="123401", updated=v1), curated_row(sub_id="123401", end=TERMINATED, updated=v2),
        curated_row(sub_id="123407", updated=v1), curated_row(sub_id="123407", end=TERMINATED, updated=v2),
        # a tombstone (soft void), applied (123402) and not applied (123403)
        curated_row(sub_id="123402", updated=v1), curated_row(sub_id="123402", end=VOID, updated=v2),
        curated_row(sub_id="123403", updated=v1), curated_row(sub_id="123403", end=VOID, updated=v2),
        # out of order: the newer version arrived first, an older one arrived late
        curated_row(sub_id="123404", end=TERMINATED, updated=hours(20)),
        curated_row(sub_id="123404", updated=hours(40), loaded=hours(10)),
        # a newer version inside its SLA: the key is pending
        curated_row(sub_id="123405", updated=v1), curated_row(sub_id="123405", end=TERMINATED, updated=hours(4),
                                                              loaded=hours(3)),
        # a newer version loaded after the cutoff: it belongs to a later window
        curated_row(sub_id="123406", updated=v1), curated_row(sub_id="123406", end=TERMINATED, updated=hours(1),
                                                              loaded=hours(5 / 60)),
    ])
    at = gold_load_time(11)
    create_table(spark, replay.gold, GOLD_COLUMNS, [
        gold_row(sub_id="000123401", end=TERMINATED, record=v2, loaded=at),
        gold_row(sub_id="000123407", record=v1, loaded=at),
        gold_row(sub_id="000123402", end=VOID, record=v2, loaded=at),
        gold_row(sub_id="000123403", record=v1, loaded=at),
        gold_row(sub_id="000123404", end=TERMINATED, record=hours(20), loaded=at),
        gold_row(sub_id="000123405", record=v1, loaded=at),
        gold_row(sub_id="000123406", record=v1, loaded=at),
    ])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    result = current_ledger(spark, replay)
    assert (result.state, result.population, result.violations) == ("FAILED", 6, 2)
    assert result.observed == ("due 6 (pending 1): deleted 1, deleted_not_applied 1, delivered 3, stale 1; orphans 0; "
                               "fails: 1 deleted_not_applied; 1 stale")
    ledger = lines(spark, replay, result.evaluation_id)
    assert nonzero(ledger) == {("due", None): 6, ("pending", None): 1, ("deleted", None): 1,
                               ("deleted_not_applied", None): 1, ("delivered", None): 3, ("stale", None): 1}
    assert ledger[("deleted", None)][1] == "authorised" and ledger[("deleted_not_applied", None)][1] == "breach"
    assert_balanced(ledger, 6)
    assert scope(result)["evidence_levels"]["deleted"] == "reconstructed"
    # Which keys: the release 1 key check flags exactly the two keys gold holds an older version of (the
    # unapplied void and the unapplied update; spec section 6, "Beside the release 1 hop checks"), by
    # HMAC. The late older version of 123404 and the pending 123405 are not flagged.
    flagged = {(r.key_hash, r.event, r.state_detail)
               for r in spark.table(f"{replay.dq}.v_committed_key_event").collect()}
    assert flagged == {(key_hash(SECRET, f"{s}|01|2026-01-01|AGR-A"), "FLAGGED", "STALE") for s in ("123403", "123407")}
    # The key check places a key by its latest load time, the row loaded after C included, so it holds
    # 123406 back as pending with 123405, where the ledger judges 123406 on its version as of C.
    [currency] = latest(spark, replay)[("gold_member_coverage", "HOP_KEY_CURRENCY")]
    assert currency.observed == "0 missing and 2 stale of 5 keys; 2 keys pending (within sla_hours)"


def test_r19_every_version_applied_passes(spark, tmp_path) -> None:
    """PS6, the PASS side: the update and the void both applied, the late older version ignored, the
    pending key outside the ledger. PASSED (qualified by the sample union's unproven attribution)."""
    replay = setup(spark, tmp_path, "r19_ok")
    v1, v2 = hours(30), hours(12)
    create_table(spark, replay.curated, CURATED_COLUMNS, [
        curated_row(sub_id="123401", updated=v1), curated_row(sub_id="123401", end=TERMINATED, updated=v2),
        curated_row(sub_id="123402", updated=v1), curated_row(sub_id="123402", end=VOID, updated=v2),
        curated_row(sub_id="123404", end=TERMINATED, updated=hours(20)),
        curated_row(sub_id="123404", updated=hours(40), loaded=hours(10)),
        curated_row(sub_id="123405", updated=v1), curated_row(sub_id="123405", end=TERMINATED, updated=hours(4),
                                                              loaded=hours(3)),
    ])
    at = gold_load_time(11)
    create_table(spark, replay.gold, GOLD_COLUMNS, [
        gold_row(sub_id="000123401", end=TERMINATED, record=v2, loaded=at),
        gold_row(sub_id="000123402", end=VOID, record=v2, loaded=at),
        gold_row(sub_id="000123404", end=TERMINATED, record=hours(20), loaded=at),
        gold_row(sub_id="000123405", record=v1, loaded=at),
    ])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    result = current_ledger(spark, replay)
    assert (result.state, result.population, result.violations) == ("PASSED", 3, 0)
    assert result.observed == "due 3 (pending 1): deleted 1, delivered 2; orphans 0"
    assert_balanced(lines(spark, replay, result.evaluation_id), 3)
    assert (result.qualifications, result.healthy) == ("attribution_unverified", False)
