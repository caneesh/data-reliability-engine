"""R17 (proof scenario PS4): two sources into one gold table, without double counting.

Reconciliation model section 10, row 4: "`join_contribution`: counted once by the primary, the
contributor checked by HOP_CONTRIBUTION. Or `union_partition`, with T1_SOURCE_ATTRIBUTION proving no
overlap" (model sections 6.1, 6.2 and example 9.2). Through `dre run`, on the sample configuration
with a second source: gold_member_coverage (SRC_A, from curated) and gold_member_coverage_b (SRC_B,
from a second curated table) are union partitions of one gold table, told apart by their feed_filter.
"""

from __future__ import annotations

from pathlib import Path

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.replay.conftest import latest, replay_conf
from tests.replay.ledger_views import current_ledger, scope

GOLD_A, GOLD_B = "gold_member_coverage", "gold_member_coverage_b"
UP_A, UP_B = "example_curated_enrollment", "example_curated_enrollment_b"


def two_sources(spark, tmp_path: Path, name: str):
    """The sample with a second source on the gold table: its own curated table and gold dataset."""
    replay = replay_conf(spark, tmp_path, name, {
        "feeds/example_realtime.yaml": [("datasets: [example_curated_enrollment, gold_member_coverage]",
                                         f"datasets: [example_curated_enrollment, gold_member_coverage, {UP_B}, {GOLD_B}]")]})
    datasets = replay.conf / "datasets"
    curated_b = (datasets / "example_curated_enrollment.yaml").read_text(encoding="utf-8")
    (datasets / f"{UP_B}.yaml").write_text(
        curated_b.replace(f"dataset: {UP_A}", f"dataset: {UP_B}").replace(replay.curated, f"{replay.curated}_b"),
        encoding="utf-8")
    gold_b = (datasets / "gold_member_coverage.yaml").read_text(encoding="utf-8")
    (datasets / f"{GOLD_B}.yaml").write_text(
        gold_b.replace(f"dataset: {GOLD_A}", f"dataset: {GOLD_B}").replace("'SRC_A'", "'SRC_B'")
        .replace(f"upstream: [{UP_A}]", f"upstream: [{UP_B}]").replace(f"  {UP_A}:", f"  {UP_B}:"),
        encoding="utf-8")
    assert main(["validate", "--conf", str(replay.conf)]) == 0
    return replay


def attribution(spark, replay):
    """The one T1_SOURCE_ATTRIBUTION result of the gold table (one per target per window)."""
    found = [r for (ds, check), rows in latest(spark, replay).items() if check == "T1_SOURCE_ATTRIBUTION" for r in rows]
    assert len(found) == 1, [(r.dataset, r.state) for r in found]
    return found[0]


def test_r17_a_union_whose_filters_leave_rows_unattributed_fails_and_stays_qualified(spark, tmp_path) -> None:
    """Model example 9.2, union: rows with src_sys_nm NULL and 'SRC_C' belong to no source.
    T1_SOURCE_ATTRIBUTION is FAILED with the 2 unattributed rows; neither edge's ledger may claim its
    attribution, so each stays qualified and never healthy, and neither counts the other's rows."""
    replay = two_sources(spark, tmp_path, "r17_union_bad")
    loaded = curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(sub_id="123401", updated=loaded),
                                                          curated_row(sub_id="123402", updated=loaded)])
    create_table(spark, f"{replay.curated}_b", CURATED_COLUMNS, [curated_row(sub_id="223401", updated=loaded)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [
        gold_row(sub_id="000123401", record=loaded, loaded=gold_load_time(11)),
        gold_row(sub_id="000123402", record=loaded, loaded=gold_load_time(11)),
        gold_row(sub_id="000223401", source="SRC_B", record=loaded, loaded=gold_load_time(11)),
        gold_row(sub_id="000323401", source=None, record=loaded, loaded=gold_load_time(11)),
        gold_row(sub_id="000423401", source="SRC_C", record=loaded, loaded=gold_load_time(11))])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    proof = attribution(spark, replay)
    assert (proof.state, proof.population, proof.violations) == ("FAILED", 5, 2)
    for gold, up, due in ((GOLD_A, UP_A, 2), (GOLD_B, UP_B, 1)):
        ledger = current_ledger(spark, replay, gold, up)
        assert (ledger.state, ledger.population) == ("PASSED", due)  # its own source's records only
        assert "attribution_unverified" in ledger.qualifications.split(",") and not ledger.healthy
        assert scope(ledger)["attribution"] == "feed_filter_unverified"


def test_r17_a_union_proven_disjoint_and_complete_counts_each_row_once(spark, tmp_path) -> None:
    """PS4 PASS side: every gold row is SRC_A or SRC_B. T1_SOURCE_ATTRIBUTION PASSES, each edge's
    ledger proves the same over the table (attribution verified), and the due records of the two
    edges add up to the gold rows: nothing counted twice."""
    replay = two_sources(spark, tmp_path, "r17_union_ok")
    loaded = curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(sub_id="123401", updated=loaded),
                                                          curated_row(sub_id="123402", updated=loaded)])
    create_table(spark, f"{replay.curated}_b", CURATED_COLUMNS, [curated_row(sub_id="223401", updated=loaded)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [
        gold_row(sub_id="000123401", record=loaded, loaded=gold_load_time(11)),
        gold_row(sub_id="000123402", record=loaded, loaded=gold_load_time(11)),
        gold_row(sub_id="000223401", source="SRC_B", record=loaded, loaded=gold_load_time(11))])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    proof = attribution(spark, replay)
    assert (proof.state, proof.population, proof.violations) == ("PASSED", 3, 0)
    due = 0
    for gold, up in ((GOLD_A, UP_A), (GOLD_B, UP_B)):
        ledger = current_ledger(spark, replay, gold, up)
        assert ledger.state == "PASSED" and scope(ledger)["attribution"] == "verified"
        assert "attribution_unverified" not in (ledger.qualifications or "")
        due += ledger.population
    assert due == spark.table(replay.gold).count() == 3


def contributor(spark, tmp_path: Path, name: str, gold_rows: list[dict]):
    """The sample with a contributor: gold's covrg_agrmt_id refers to a synthetic agreement table."""
    replay = replay_conf(spark, tmp_path, name, {
        "datasets/gold_member_coverage.yaml": [
            ("upstream: [example_curated_enrollment]", "upstream: [example_curated_enrollment, example_agreement]"),
            ("compute_budget_minutes: 8",
             "  example_agreement:\n    mapping: join_contribution\n    reference: { covrg_agrmt_id: agreement_id }\n"
             "    contributes: { agreement_end: mbr_mbrshp_covrg_end_dt }\ncompute_budget_minutes: 8")]})
    (replay.conf / "datasets" / "example_agreement.yaml").write_text(
        f"dataset: example_agreement\ntable: {name}_ref.agreement\nlayer: CURATED\nowner: membership-gold\n"
        "expectation_version: 1\nkey: [agreement_id]\n"
        "record_time: { column: updated_at, timezone: UTC }\n"
        "load_time: { column: updated_at, granularity: minute, timezone: UTC }\n", encoding="utf-8")
    assert main(["validate", "--conf", str(replay.conf)]) == 0
    from datetime import datetime
    create_table(spark, f"{name}_ref.agreement", [("agreement_id", "STRING"), ("agreement_end", "STRING"),
                                                 ("updated_at", "TIMESTAMP")],
                 [{"agreement_id": "AGR-A", "agreement_end": "9999-12-31", "updated_at": datetime(2026, 1, 1)}])
    loaded = curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(sub_id=r["sub_id"].lstrip("0"), updated=loaded,
                                                                      agreement=r["covrg_agrmt_id"])
                                                          for r in gold_rows])
    create_table(spark, replay.gold, GOLD_COLUMNS, gold_rows)
    return replay


def contribution(spark, replay):
    [result] = latest(spark, replay)[(GOLD_A, "HOP_CONTRIBUTION")]
    assert result.group_values == {"upstream": "example_agreement"}
    return result


def test_r17_a_row_whose_contributor_reference_resolves_to_nothing_fails_contribution(spark, tmp_path) -> None:
    """Model example 9.2, join contribution: one gold row's agreement resolves to no contributor
    version: `unresolved` (JOIN_UNMATCHED), HOP_CONTRIBUTION FAILED with 1 violation. The row is still
    counted once, by the primary edge's ledger."""
    loaded = curated_load_time(12)
    replay = contributor(spark, tmp_path, "r17_join_bad", [
        gold_row(sub_id="000123401", record=loaded, loaded=gold_load_time(11)),
        gold_row(sub_id="000123402", agreement="AGR-Z", record=loaded, loaded=gold_load_time(11))])
    assert main(["run", "--conf", str(replay.conf)]) == 0
    result = contribution(spark, replay)
    assert (result.state, result.population, result.violations) == ("FAILED", 2, 1)
    assert "JOIN_UNMATCHED" in result.observed + (result.detail or "")
    primary = current_ledger(spark, replay)
    assert primary.population == 2  # each gold row counted once, by the primary


def test_r17_rows_whose_contributor_resolves_and_agrees_pass_contribution(spark, tmp_path) -> None:
    loaded = curated_load_time(12)
    replay = contributor(spark, tmp_path, "r17_join_ok", [
        gold_row(sub_id="000123401", record=loaded, loaded=gold_load_time(11)),
        gold_row(sub_id="000123402", record=loaded, loaded=gold_load_time(11))])
    assert main(["run", "--conf", str(replay.conf)]) == 0
    result = contribution(spark, replay)
    assert (result.state, result.population, result.violations) == ("PASSED", 2, 0)
