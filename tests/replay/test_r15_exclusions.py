"""R15 (proof scenario PS2): an approved, versioned exclusion accounts for a record; an unapproved filter does not.

Reconciliation model section 10, row 2: "Approved: `excluded[rule@ver]`, PASSED if nothing else.
Unapproved: `missing`, FAILED, with the reported rule named in the cause line." Section 9.4 is the
worked example. The approved rule is a contract exclusion (contracts.<upstream>.exclusions: id,
version, condition, effective, approved_by, max_share); the unapproved one exists only in the
pipeline's filter registry (`probes.filter_rules`, reported evidence, docs/evidence-contract.md),
which never authorises an exclusion. Through `dre run`, on the sample configuration.
"""

from __future__ import annotations

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.replay.conftest import cause_rows, causes, latest
from tests.replay.ledger_views import assert_balanced, current_ledger, lines, nonzero, scope
from tests.replay.test_r04_to_r07_hops import setup

GOLD_YAML = "datasets/gold_member_coverage.yaml"
FEED_YAML = "feeds/example_realtime.yaml"
MAPPING = "    mapping: union_partition\n"
# The approved rule (synthetic change record and code reference): test agreements never reach gold.
APPROVED = (
    "    exclusions:\n"
    "      - id: drop_test_agreements\n"
    "        version: 2\n"
    "        condition: \"qualifiedhealthplanid = 'AGR-TEST'\"\n"
    "        effective: { from: 2026-01-01 }\n"
    "        approved_by: CHG-0000\n"
    "        code_ref: \"example_build.sql:12\"\n"
    "        max_share: 0.5\n"
)


def unapproved_filter(table: str) -> tuple[str, str]:
    """The pipeline's own filter registry reports a rule no contract approves (reported evidence)."""
    return ("  filter_rules: null", "  filter_rules:\n    - id: drop_inactive_agreements\n"
            f"      table: {table}\n      condition: \"qualifiedhealthplanid = 'AGR-INACTIVE'\"\n"
            "      code_ref: example_build.sql:40")


def test_r15_approved_exclusion_is_excluded_and_an_unapproved_filter_is_missing(spark, tmp_path) -> None:
    """PS2 / model section 10 row 2 (example 9.4): AGR-TEST is `excluded drop_test_agreements@2`
    (authorised); AGR-INACTIVE, dropped by a rule only the pipeline reports, is `missing` and the ledger
    FAILED. The cause line of the release 1 key check names the reported rule (the ledger itself is not
    investigated; spec section 7)."""
    loaded = curated_load_time(12)
    replay = setup(spark, tmp_path, "r15", {GOLD_YAML: [(MAPPING, MAPPING + APPROVED)]})
    feed = (replay.conf / FEED_YAML)
    old, new = unapproved_filter(replay.curated)
    feed.write_text(feed.read_text(encoding="utf-8").replace(old, new), encoding="utf-8")
    create_table(spark, replay.curated, CURATED_COLUMNS, [
        curated_row(sub_id="123401", updated=loaded),
        curated_row(sub_id="123402", agreement="AGR-TEST", updated=loaded),       # approved exclusion
        curated_row(sub_id="123403", agreement="AGR-INACTIVE", updated=loaded)])  # unapproved filter
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(sub_id="000123401", record=loaded,
                                                             loaded=gold_load_time(11))])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    result = current_ledger(spark, replay)
    assert (result.state, result.population, result.violations) == ("FAILED", 3, 1)
    assert result.observed == ("due 3 (pending 0): excluded drop_test_agreements@2 1, delivered 1, missing 1; "
                               "orphans 0; fails: 1 missing")
    ledger = lines(spark, replay, result.evaluation_id)
    assert nonzero(ledger) == {("due", None): 3, ("excluded", "drop_test_agreements@2"): 1, ("delivered", None): 1,
                               ("missing", None): 1}
    assert ledger[("excluded", "drop_test_agreements@2")][1] == "authorised"
    assert ledger[("missing", None)][1] == "deficit"
    assert_balanced(ledger, 3)
    assert scope(result)["evidence_level"] == "reconstructed"  # the exclusion is re-applied, not observed
    assert result.qualifications == "attribution_unverified" and result.healthy is False
    # The key check leaves the approved exclusion out and finds the other key MISSING; its cause is the
    # rule the pipeline reports, by id and code reference (never the key).
    [currency] = latest(spark, replay)[("gold_member_coverage", "HOP_KEY_CURRENCY")]
    assert (currency.state, currency.observed) == (
        "FAILED", "1 missing and 0 stale of 2 keys; 1 key excluded by exclusions drop_test_agreements@2")
    [line] = causes(spark, replay, "gold_member_coverage", "HOP_KEY_CURRENCY", "FILE_CYCLIC").values()
    assert (line.code, line.proven) == ("FILTERED", True)
    [filtered] = [r for r in cause_rows(spark, replay, "gold_member_coverage", "HOP_KEY_CURRENCY")
                  if r.cause_code == "FILTERED"]
    assert "drop_inactive_agreements" in filtered.evidence and "example_build.sql:40" in filtered.evidence


def test_r15_only_the_approved_exclusion_passes_qualified_by_its_unproven_attribution(spark, tmp_path) -> None:
    """PS2, the PASS side: only the approved exclusion applies, so the ledger PASSED. The sample hop is a
    union_partition whose attribution is unproven until build step 5, so the PASS carries exactly the
    qualification attribution_unverified and is not healthy (C3-02, C3-04); nothing else qualifies it."""
    loaded = curated_load_time(12)
    replay = setup(spark, tmp_path, "r15_ok", {GOLD_YAML: [(MAPPING, MAPPING + APPROVED)]})
    create_table(spark, replay.curated, CURATED_COLUMNS, [
        curated_row(sub_id="123401", updated=loaded), curated_row(sub_id="123404", updated=loaded),
        curated_row(sub_id="123402", agreement="AGR-TEST", updated=loaded)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [
        gold_row(sub_id="000123401", record=loaded, loaded=gold_load_time(11)),
        gold_row(sub_id="000123404", record=loaded, loaded=gold_load_time(11))])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    result = current_ledger(spark, replay)
    assert (result.state, result.population, result.violations) == ("PASSED", 3, 0)
    assert result.observed == "due 3 (pending 0): excluded drop_test_agreements@2 1, delivered 2; orphans 0"
    ledger = lines(spark, replay, result.evaluation_id)
    assert nonzero(ledger) == {("due", None): 3, ("excluded", "drop_test_agreements@2"): 1, ("delivered", None): 2}
    assert_balanced(ledger, 3)
    assert (result.qualifications, result.evidence_state, result.healthy) == ("attribution_unverified", "VALID", False)
    assert scope(result)["claim"] == "declared contract, qualified: attribution_unverified"


def test_r15_the_approved_exclusion_alone_under_a_merge_contract_is_healthy(spark, tmp_path) -> None:
    """PS2, the PASS side with nothing unproven: the same hop declared `merge` (one source, orphans
    checked, no union attribution to prove) is contract-proven, with no qualification, and healthy.
    The exclusion's evidence is reconstructed, which does not qualify the claim."""
    loaded = curated_load_time(12)
    replay = setup(spark, tmp_path, "r15_merge", {GOLD_YAML: [(MAPPING, "    mapping: merge\n" + APPROVED)]})
    create_table(spark, replay.curated, CURATED_COLUMNS, [
        curated_row(sub_id="123401", updated=loaded), curated_row(sub_id="123404", updated=loaded),
        curated_row(sub_id="123402", agreement="AGR-TEST", updated=loaded)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [
        gold_row(sub_id="000123401", record=loaded, loaded=gold_load_time(11)),
        gold_row(sub_id="000123404", record=loaded, loaded=gold_load_time(11))])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    result = current_ledger(spark, replay)
    assert (result.state, result.population, result.violations) == ("PASSED", 3, 0)
    assert result.observed == "due 3 (pending 0): excluded drop_test_agreements@2 1, delivered 2; orphans 0"
    assert (result.qualifications, result.evidence_state, result.healthy) == (None, "VALID", True)
    assert scope(result)["claim"] == "contract-proven"
