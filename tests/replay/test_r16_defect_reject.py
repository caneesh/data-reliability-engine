"""R16 (proof scenario PS3): a record rejected for a defect reason never passes.

Reconciliation model section 10, row 3: "`rejected[PARSE_ERROR]` classed defect: FAILED. Never
PASSED." Section 5 and checkpoint 3 amendment 3: a reject accounts for a record only through its
lineage (the refused version), and the contract's `reject_policy` classes its reason; the default is
defect, and an unlisted reason is a defect. The rejects table is a synthetic one in the shape of
docs/evidence-contract.md (the upstream's key columns and raw values, the refused record's version,
the reason, when it was rejected), declared as `contracts.<upstream>.rejects` with `lineage.version`
(EdgeContract.rejects). Through `dre run`, on the sample configuration.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.replay.ledger_views import assert_balanced, current_ledger, lines, nonzero, scope
from tests.replay.test_r04_to_r07_hops import setup

GOLD_YAML = "datasets/gold_member_coverage.yaml"
MAPPING = "    mapping: union_partition\n"
REJECT_COLUMNS = [*CURATED_COLUMNS[:4], ("record_version", "TIMESTAMP"), ("reason_code", "STRING"),
                  ("rejected_at", "TIMESTAMP")]


def contract(rejects_table: str) -> str:
    """The hop's rejects evidence (version lineage: record_version is the refused record's
    sourcelastupdatets, written in the defaults.yaml zone as curated writes it) and its policy:
    INVALID_CODE authorised up to 60% of the due records, PARSE_ERROR a defect, anything else unlisted."""
    return (MAPPING
            + "    rejects:\n"
            f"      table: {rejects_table}\n"
            "      lineage: { version: { column: record_version } }\n"
            "    reject_policy:\n"
            "      INVALID_CODE: { disposition: authorised, max_share: 0.6 }\n"
            "      PARSE_ERROR: { disposition: defect }\n")


def reject(sub_id: str, version: datetime, reason: str) -> dict:
    rejected_at = (datetime.now(timezone.utc) - timedelta(hours=11)).replace(tzinfo=None)  # UTC, before the cutoff
    return {"subscriberidnumber": sub_id, "membernumber": "01", "effectivedate": "2026-01-01",
            "qualifiedhealthplanid": "AGR-A", "record_version": version, "reason_code": reason,
            "rejected_at": rejected_at}


def scenario(spark, tmp_path, name: str, reasons: dict[str, str]):
    """Curated: 123401 (reaches gold) and one record per entry of `reasons`, each rejected for its reason."""
    loaded = curated_load_time(12)
    table = f"{name}_ops.dq_rejects"
    replay = setup(spark, tmp_path, name, {GOLD_YAML: [(MAPPING, contract(table))]})
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(sub_id=s, updated=loaded) for s in ("123401", *reasons)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(sub_id="000123401", record=loaded,
                                                             loaded=gold_load_time(11))])
    create_table(spark, table, REJECT_COLUMNS, [reject(s, loaded, reason) for s, reason in reasons.items()])
    assert main(["run", "--conf", str(replay.conf)]) == 0
    return replay


def test_r16_a_reject_for_a_defect_reason_or_an_unlisted_one_is_failed(spark, tmp_path) -> None:
    """PS3 / model section 10 row 3: PARSE_ERROR (listed as a defect) and SCHEMA_MISMATCH (not listed:
    a defect by default) each account for their record as `rejected[...]` classed defect, and the
    ledger is FAILED, never PASSED, although no record is missing."""
    replay = scenario(spark, tmp_path, "r16", {"123402": "PARSE_ERROR", "123403": "SCHEMA_MISMATCH"})
    result = current_ledger(spark, replay)
    assert (result.state, result.population, result.violations) == ("FAILED", 3, 2)
    assert result.observed == ("due 3 (pending 0): rejected PARSE_ERROR (defect) 1, "
                               "rejected SCHEMA_MISMATCH (defect) 1, delivered 1; orphans 0; "
                               "fails: 2 rejected for a defect reason")
    ledger = lines(spark, replay, result.evaluation_id)
    assert nonzero(ledger) == {("due", None): 3, ("rejected", "PARSE_ERROR"): 1, ("rejected", "SCHEMA_MISMATCH"): 1,
                               ("delivered", None): 1}
    assert ledger[("rejected", "PARSE_ERROR")][1] == ledger[("rejected", "SCHEMA_MISMATCH")][1] == "defect"
    assert ("missing", None) not in nonzero(ledger)  # accounted for by the rejects, not lost
    assert_balanced(ledger, 3)
    assert scope(result)["triage"] == [
        "2 record(s) rejected for a defect reason (PARSE_ERROR, SCHEMA_MISMATCH): "
        "correct the data or the rule upstream"]
    assert result.healthy is False


def test_r16_an_authorised_reason_accounts_for_its_record_and_is_reported_evidence(spark, tmp_path) -> None:
    """PS3, the contrast: the same record rejected for INVALID_CODE, which the policy authorises, is
    `rejected INVALID_CODE (authorised)` and the ledger PASSED. Rejects are reported evidence, so the
    claim ceiling is `reported` and the PASS carries evidence_reported (the sample union's attribution
    is proven since build step 5): never healthy."""
    replay = scenario(spark, tmp_path, "r16_ok", {"123402": "INVALID_CODE"})
    result = current_ledger(spark, replay)
    assert (result.state, result.population, result.violations) == ("PASSED", 2, 0)
    assert result.observed == "due 2 (pending 0): rejected INVALID_CODE (authorised) 1, delivered 1; orphans 0"
    ledger = lines(spark, replay, result.evaluation_id)
    assert ledger[("rejected", "INVALID_CODE")] == (1, "authorised")
    assert_balanced(ledger, 2)
    assert scope(result)["evidence_level"] == "reported"
    assert (result.qualifications, result.healthy) == ("evidence_reported", False)
