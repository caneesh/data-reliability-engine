"""R23 (proof scenario PS10): an unsupported one-to-many mapping is visibly unsupported, never passed.

Reconciliation model section 10, row 10: "`fan_out`: DID_NOT_RUN / `unsupported_mapping` on every
run, listed by `dre validate` as a blind spot. Never PASSED." Section 4 (ADR-02): an edge without a
supported kind is DID_NOT_RUN / unsupported_mapping (CONFIGURATION) on every run. The sample hop is
declared `fan_out` (one curated record to several gold rows); a fan_out hop cannot be keyed, so it
has no key_map entry (spec section 3). Gold holds every expected row, so nothing but the mapping
keeps the ledger from passing.
"""

from __future__ import annotations

import json

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.replay.ledger_views import ledger_results, lines
from tests.replay.test_r04_to_r07_hops import setup

GOLD_YAML = "datasets/gold_member_coverage.yaml"
FAN_OUT = [
    ("    mapping: union_partition\n", "    mapping: fan_out\n"),
    ("key_map:\n  example_curated_enrollment:\n    sub_id: subscriberidnumber\n    mem_nbr: membernumber\n"
     "    mbr_mbrshp_covrg_eff_dt: effectivedate\n    covrg_agrmt_id: qualifiedhealthplanid\n"
     "owned_columns: { enddate: mbr_mbrshp_covrg_end_dt }\n", ""),
]


def test_r23_a_fan_out_hop_is_did_not_run_unsupported_mapping_on_every_run(spark, tmp_path, capsys) -> None:
    """PS10 / model section 10 row 10: two runs, each with one HOP_LEDGER result for the hop, each
    DID_NOT_RUN / CONFIGURATION / unsupported_mapping with its scope and no ledger lines; no PASSED
    result for the hop is ever committed; `dre validate` names the hop and its blind spot."""
    replay = setup(spark, tmp_path, "r23", {GOLD_YAML: FAN_OUT})
    loaded = curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(sub_id="123401", updated=loaded)])
    # One curated record fans out to two gold rows (two agreements): every expected row is there.
    create_table(spark, replay.gold, GOLD_COLUMNS, [
        gold_row(sub_id="000123401", agreement=a, record=loaded, loaded=gold_load_time(11))
        for a in ("AGR-A", "AGR-B")])

    assert main(["validate", "--conf", str(replay.conf)]) == 0
    out = capsys.readouterr().out
    assert "  example_curated_enrollment->gold_member_coverage: fan_out (declared)" in out
    assert ("    blind spot: fan_out cannot be judged: one upstream record to several records here "
            "(unsupported_mapping)") in out

    assert main(["run", "--conf", str(replay.conf)]) == 0
    assert main(["run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0  # a second run
    results = ledger_results(spark, replay)
    assert len({r.run_id for r in results}) == len(results) == 2  # one result per run
    for result in results:
        assert (result.state, result.reason_category, result.reason_code) == (
            "DID_NOT_RUN", "CONFIGURATION", "unsupported_mapping")
        scope = json.loads(result.detail)
        assert scope["mapping"] == "fan_out" and scope["note"].startswith("fan_out (declared)")
        assert any("unsupported_mapping" in s for s in scope["blind_spots"])
        assert lines(spark, replay, result.evaluation_id) == {}  # nothing is reconciled, so no ledger
    states = {r.state for r in spark.table(f"{replay.dq}.v_committed_result").where(
        "dataset = 'gold_member_coverage' AND check_id = 'HOP_LEDGER'").collect()}
    assert states == {"DID_NOT_RUN"}  # never PASSED
    # A hop that cannot be keyed has no key-level hop checks either: nothing passes in the ledger's place.
    hop_checks = {r.check_id for r in spark.table(f"{replay.dq}.v_committed_result").where(
        "dataset = 'gold_member_coverage' AND check_id LIKE 'HOP_%'").collect()}
    assert hop_checks == {"HOP_LEDGER"}
