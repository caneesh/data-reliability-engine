"""The reconciliation contract (build step 1 of the Phase 1 design): `contracts`, `grain` and the
isolation settings validate with file, line, field and fix; hops without a contract resolve from
the release 1 fields; `dre validate` reports every hop's kind and blind spots.

Design: docs/design/target-architecture.md (ADR-01 to ADR-08), docs/design/reconciliation-model.md.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hcsc.datalake.dre.cli import main
from hcsc.datalake.dre.config.contracts import blind_spots, hop_contract, hops
from hcsc.datalake.dre.config.loader import ConfigError
from hcsc.datalake.dre.config.validate import validate_conf
from tests.config.conftest import SAMPLE_CONF, edit, line_with

GOLD = "datasets/gold_member_coverage.yaml"
DEFAULTS = "defaults.yaml"
HOP = "contracts.example_curated_enrollment"
MAPPING = "    mapping: union_partition\n"
EXCLUSION = ("    exclusions:\n"
             "      - { id: drop_test_members, version: 1, condition: \"status = 'TEST'\", approved_by: CHG-0000,"
             " max_share: 0.05 }\n")


def _find(errors: list[ConfigError], file: Path, field: str) -> ConfigError:
    matching = [e for e in errors if e.file == str(file) and e.field == field]
    assert matching, f"no issue for {file}:{field}; got:\n" + "\n".join(map(str, errors))
    return matching[0]


def test_the_sample_contract_resolves() -> None:
    config, errors, _ = validate_conf(SAMPLE_CONF)
    assert errors == []
    gold = config.datasets["gold_member_coverage"]
    hop = hop_contract(config.datasets, gold, "example_curated_enrollment")
    assert (hop.declared, hop.contract.mapping, hop.contract.deletion.applied_as) == (True, "union_partition", "soft")
    assert gold.grain.history == "mutable"
    assert config.datasets["example_curated_enrollment"].grain.history is None  # a platform value, not invented
    assert hop.contract.tolerance.model_dump() == {"missing": 0, "stale": 0, "divergent": 0}
    assert hop.contract.reject_policy == {}  # every reject reason is a defect unless listed (ADR-04)


def test_a_hop_without_a_contract_resolves_from_the_release_1_fields() -> None:
    """The aliases: key_map is a dedup hop (the latest upstream version per key), hop_exclusions one
    legacy exclusion used exactly as written, a derived upstream an aggregate hop."""
    config, _, _ = validate_conf(SAMPLE_CONF)
    provider = hop_contract(config.datasets, config.datasets["provider_directory"], "provider_roster_raw")
    assert (provider.declared, provider.contract.mapping, provider.legacy_exclusion) == (False, "dedup", None)
    legacy = config.datasets["gold_member_coverage"].model_copy(
        update={"contracts": {}, "hop_exclusions": {"example_curated_enrollment": "status = 'TEST'"}})
    hop = hop_contract(config.datasets, legacy, "example_curated_enrollment")
    assert (hop.contract.mapping, hop.legacy_exclusion, hop.exclusions) == ("dedup", "status = 'TEST'", [])
    assert any("no id, version, approval or volume guard" in s for s in blind_spots(config, hop))
    derived = config.datasets["provider_roster_raw"].model_copy(
        update={"table": None, "derived": {"from": "provider_directory", "group_by": {"provider_id": "provider_id"}}})
    inferred = hop_contract({**config.datasets, "provider_roster_raw": derived}, config.datasets["provider_directory"],
                            "provider_roster_raw")
    assert (inferred.declared, inferred.contract.mapping) == (False, "aggregate")
    assert any("aggregation is not reconciled by contract until a contract declares" in s
               for s in blind_spots(config, inferred))  # D6 condition 3
    assert [h.label for h in hops(config)] == ["example_curated_enrollment->gold_member_coverage",
                                               "provider_roster_raw->provider_directory"]


def test_validate_prints_each_hop_its_kind_and_its_blind_spots(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["validate", "--conf", str(SAMPLE_CONF)]) == 0
    out = capsys.readouterr().out
    assert "contracts: 2 hop(s)" in out
    assert "  example_curated_enrollment->gold_member_coverage: union_partition (declared)" in out
    assert "  provider_roster_raw->provider_directory: dedup (inferred from key_map)" in out
    assert "blind spot: attribution (each row here owned by exactly one source) is not proven until build step 5" in out
    assert "blind spot: as-of replay unavailable: gold_member_coverage is mutable" in out
    assert "blind spot: deletions not modelled" in out  # the provider hop declares none
    # D6 conditions 1 and 2 (2026-10-17): an inferred kind is labelled, and says it proves nothing by contract
    assert "blind spot: kind dedup is inferred from key_map, not declared: its results can never claim " \
        "contract-proven completeness" in out
    assert out.count("is inferred from key_map") == 1  # the declared gold hop does not carry it


# (id, file, old, new, field, line needle, problem contains, fix contains)
CASES = [
    ("unknown-mapping", GOLD, MAPPING, "    mapping: one_to_many\n", f"{HOP}.mapping", "mapping: one_to_many",
     "not allowed", "union_partition"),
    ("tolerance-on-unknown", GOLD, MAPPING, MAPPING + "    tolerance: { missing: 2, unassessable: 1 }\n",
     f"{HOP}.tolerance", "tolerance:", "cannot cover ['unassessable']", "always fail"),
    ("tolerance-negative", GOLD, MAPPING, MAPPING + "    tolerance: { missing: -1 }\n", f"{HOP}.tolerance.missing",
     "tolerance:", "greater than", ">= 0"),
    ("soft-without-soft-when", GOLD,
     "      soft_when: \"to_date(mbr_mbrshp_covrg_end_dt, 'yyyy-MM-dd') = to_date(mbr_mbrshp_covrg_eff_dt, 'yyyy-MM-dd')\"\n",
     "", f"{HOP}.deletion", "deletion:", "soft_when goes with applied_as: soft", "soft_when"),
    ("contract-not-upstream", GOLD, "contracts:                        # the hop's reconciliation contract "
     "(docs/design/reconciliation-model.md)\n  example_curated_enrollment:",
     "contracts:\n  nowhere:", "contracts.nowhere", "  nowhere:", "not in upstream", "add nowhere to upstream"),
    ("union-needs-attribution", GOLD, "feed_filter: \"src_sys_nm = 'SRC_A'\"\n", "", f"{HOP}.mapping",
     "mapping: union_partition", "attribution, which is the dataset's feed_filter", "set feed_filter"),
    ("aggregate-needs-derived", GOLD, MAPPING, "    mapping: aggregate\n", f"{HOP}.mapping", "mapping: aggregate",
     "needs a derived upstream", "derived dataset"),
    ("fan-out-through-key-map", GOLD, MAPPING, "    mapping: fan_out\n", f"{HOP}.mapping", "mapping: fan_out",
     "cannot be judged through key_map", "remove key_map.example_curated_enrollment"),
    ("reference-without-join", GOLD, MAPPING, MAPPING + "    reference: { sub_id: subscriberidnumber }\n",
     f"{HOP}.reference", "reference:", "join_contribution only", "remove it"),
    ("precedence-without-survivorship", GOLD, MAPPING, MAPPING + "    precedence: [example_curated_enrollment]\n",
     f"{HOP}.precedence", "precedence:", "survivorship only", "remove it"),
    ("exclusion-needs-approval", GOLD, MAPPING, MAPPING + EXCLUSION.replace(" approved_by: CHG-0000,", ""),
     f"{HOP}.exclusions[0].approved_by", "- { id: drop_test_members", "missing", "add `approved_by`"),
    ("exclusion-needs-volume-guard", GOLD, MAPPING, MAPPING + EXCLUSION.replace(", max_share: 0.05", ""),
     f"{HOP}.exclusions[0].max_share", "- { id: drop_test_members", "missing", "add `max_share`"),
    ("exclusion-share-above-one", GOLD, MAPPING, MAPPING + EXCLUSION.replace("0.05", "1.5"),
     f"{HOP}.exclusions[0].max_share", "- { id: drop_test_members", "less than or equal", "<= 1"),
    ("exclusion-listed-twice", GOLD, MAPPING, MAPPING + EXCLUSION + EXCLUSION.split("\n", 1)[1].replace(
        "max_share", "code_ref: copy, max_share"), f"{HOP}.exclusions[1]", "code_ref: copy",
     "drop_test_members@1 is listed twice", "new version"),
    ("exclusion-versions-overlap", GOLD, MAPPING, MAPPING + EXCLUSION + EXCLUSION.split("\n", 1)[1].replace(
        "version: 1", "version: 2"), f"{HOP}.exclusions[1].effective", "version: 2",
     "versions 1 and 2 of exclusion 'drop_test_members' are effective at the same time", "effective.to"),
    ("exclusion-dates-backwards", GOLD, MAPPING, MAPPING + EXCLUSION.replace(
        "approved_by", "effective: { from: 2026-09-01, to: 2026-08-01 }, approved_by"),
     f"{HOP}.exclusions[0].effective", "- { id: drop_test_members", "is not after effective.from", "set `to` after"),
    ("authorised-reject-needs-guard", GOLD, MAPPING, MAPPING + "    reject_policy: { INVALID_CODE: { disposition: authorised } }\n",
     f"{HOP}.reject_policy.INVALID_CODE", "reject_policy:", "needs max_share", "set max_share"),
    ("rejects-need-a-key", GOLD, MAPPING, "    mapping: join_contribution\n    reference: { sub_id: subscriberidnumber }\n"
     "    rejects: { table: example_db.enrollment_rejects, lineage: { version: { column: record_version } } }\n", f"{HOP}.rejects", "rejects:", "matched by key",
     "remove rejects"),
    ("rejects-need-a-table", GOLD, MAPPING, MAPPING + "    rejects: { condition: \"stage = 'x'\", lineage: { version: { column: record_version } } }\n",
     f"{HOP}.rejects.table", "rejects:", "missing", "add `table`"),
    # checkpoint 3, C3-01: a reject must name the record version it refused, and a rule by id and version
    ("rejects-need-lineage", GOLD, MAPPING, MAPPING + "    rejects: { table: example_db.enrollment_rejects }\n",
     f"{HOP}.rejects.lineage", "rejects:", "missing", "add `lineage`"),
    ("lineage-is-one-of", GOLD, MAPPING, MAPPING + "    rejects: { table: example_db.enrollment_rejects, lineage: "
     "{ version: { column: record_version }, batch: { column: batch_id, upstream_column: batch_id } } }\n",
     f"{HOP}.rejects.lineage", "rejects:", "exactly one of version and batch", "set version"),
    ("rule-needs-its-version", GOLD, MAPPING, MAPPING + "    rejects: { table: example_db.enrollment_rejects, lineage: { version: { column: record_version } }, "
     "rule_id_column: rule_id }\n", f"{HOP}.rejects", "rejects:", "by its id and its version",
     "rule_version_column"),
    ("policy-rules-need-rule-columns", GOLD, MAPPING, MAPPING + "    rejects: { table: example_db.enrollment_rejects, lineage: { version: { column: record_version } } }\n"
     "    reject_policy: { INVALID_CODE: { disposition: authorised, max_share: 0.02, rules: [code_check@2] } }\n",
     f"{HOP}.reject_policy.INVALID_CODE.rules", "reject_policy:", "the rejects name no rule", "rule_id_column"),
    ("policy-rule-format", GOLD, MAPPING, MAPPING +
     "    reject_policy: { INVALID_CODE: { disposition: authorised, max_share: 0.02, rules: [code_check] } }\n",
     f"{HOP}.reject_policy.INVALID_CODE.rules[0]", "reject_policy:", "is not a rule id@version", "<id>@<version>"),
    ("defect-takes-no-rules", GOLD, MAPPING, MAPPING +
     "    reject_policy: { PARSE_ERROR: { disposition: defect, rules: [parser@1] } }\n",
     f"{HOP}.reject_policy.PARSE_ERROR", "reject_policy:", "rules approve rejects of an authorised reason",
     "remove rules"),
    ("bad-history", GOLD, "  history: mutable", "  history: sometimes", "grain.history", "history: sometimes",
     "not allowed", "append_only"),
    ("queue-is-protected", DEFAULTS, "hmac_secret_file: null", "hmac_secret_file: null\nyarn_queue: shared_q\n"
     "protected_queues: [shared_q]", "yarn_queue", "yarn_queue:", "one of protected_queues", "DRE's own queue"),
]


@pytest.mark.parametrize(("case", "rel", "old", "new", "field", "needle", "problem", "fix"), CASES, ids=[c[0] for c in CASES])
def test_contract_errors_name_file_line_field_and_fix(
    conf: Path, case: str, rel: str, old: str, new: str, field: str, needle: str, problem: str, fix: str
) -> None:
    edit(conf, rel, old, new)
    _, errors, _ = validate_conf(conf)
    error = _find(errors, conf / rel, field)
    assert error.line == line_with(conf, rel, needle), str(error)
    assert problem in error.problem, str(error)
    assert fix in error.fix, str(error)


def test_exclusions_given_twice_are_an_error_and_hop_exclusions_alone_a_warning(conf: Path) -> None:
    edit(conf, GOLD, "winner_rule:", "hop_exclusions: { example_curated_enrollment: \"status = 'TEST'\" }\nwinner_rule:")
    _, errors, warnings = validate_conf(conf)
    assert errors == []
    warning = _find(warnings, conf / GOLD, "hop_exclusions.example_curated_enrollment")
    assert "no id, version, approval or volume guard" in warning.problem
    assert "contracts.example_curated_enrollment.exclusions" in warning.fix
    edit(conf, GOLD, MAPPING, MAPPING + EXCLUSION)
    _, errors, _ = validate_conf(conf)
    error = _find(errors, conf / GOLD, "hop_exclusions.example_curated_enrollment")
    assert "set twice" in error.problem and "remove this entry" in error.fix


def test_kinds_judged_later_or_never_are_warnings_never_silent(conf: Path) -> None:
    """A join_contribution hop is not keyed, needs a reference covering the contributor's key, and is a
    blind spot until build step 5. A fan_out hop is a blind spot for good (unsupported_mapping)."""
    edit(conf, GOLD, MAPPING, "    mapping: join_contribution\n    reference: { sub_id: subscriberidnumber }\n")
    _, errors, warnings = validate_conf(conf)
    fields = {e.field: e.problem for e in errors}
    assert "cannot be judged through key_map" in fields[f"{HOP}.mapping"]
    assert "must map to all of example_curated_enrollment's key" in fields[f"{HOP}.reference"]
    assert "not judged until build step 5" in _find(warnings, conf / GOLD, f"{HOP}.mapping").problem

    edit(conf, GOLD, "    mapping: join_contribution\n    reference: { sub_id: subscriberidnumber }\n",
         "    mapping: fan_out\n")
    # fan_out cannot be keyed: the hop has no key_map entry, and so no owned_columns
    edit(conf, GOLD, "key_map:\n  example_curated_enrollment:\n    sub_id: subscriberidnumber\n    mem_nbr: membernumber\n"
         "    mbr_mbrshp_covrg_eff_dt: effectivedate\n    covrg_agrmt_id: qualifiedhealthplanid\n"
         "owned_columns: { enddate: mbr_mbrshp_covrg_end_dt }\n", "")
    config, errors, warnings = validate_conf(conf)
    assert [e for e in errors if e.field.startswith("contracts")] == []
    assert "unsupported_mapping" in _find(warnings, conf / GOLD, f"{HOP}.mapping").problem
    [hop] = [h for h in hops(config) if h.dataset == "gold_member_coverage"]
    assert any("cannot be judged" in s and "unsupported_mapping" in s for s in blind_spots(config, hop))


def test_survivorship_hops_share_one_precedence(conf: Path) -> None:
    edit(conf, GOLD, MAPPING, "    mapping: survivorship\n    precedence: [some_other_source]\n")
    _, errors, _ = validate_conf(conf)
    error = _find(errors, conf / GOLD, f"{HOP}.precedence")
    assert "must list 'example_curated_enrollment'" in error.problem


def test_a_derived_dataset_cannot_carry_contracts(conf: Path) -> None:
    (conf / "datasets/monthly_counts.yaml").write_text(
        "dataset: monthly_counts\nlayer: CURATED\nkey: [plan]\n"
        "derived:\n  from: example_curated_enrollment\n  group_by: { plan: \"example_curated_enrollment.qualifiedhealthplanid\" }\n"
        "contracts:\n  example_curated_enrollment: { mapping: aggregate }\n", encoding="utf-8")
    _, errors, _ = validate_conf(conf)
    error = _find(errors, conf / "datasets/monthly_counts.yaml", "key_map")
    assert "cannot be a downstream" in error.problem and "contracts" in error.problem


def test_a_reject_policy_without_rejects_evidence_is_a_warning(conf: Path) -> None:
    """Build step 3: the policy classes reasons of rejects the ledger reads from the declared `rejects`
    table; without one no record can be rejected, which validate says."""
    policy = "    reject_policy: { PARSE_ERROR: { disposition: defect } }\n"
    edit(conf, GOLD, MAPPING, MAPPING + policy)
    config, errors, warnings = validate_conf(conf)
    assert errors == []
    warning = _find(warnings, conf / GOLD, f"{HOP}.reject_policy")
    assert "counts as missing" in warning.problem and "set rejects" in warning.fix
    [hop] = [h for h in hops(config) if h.dataset == "gold_member_coverage"]
    assert "no rejects evidence: a record the pipeline rejected counts as missing (the reject policy cannot be " \
        "applied)" in blind_spots(config, hop)
    edit(conf, GOLD, policy, policy + "    rejects: { table: example_db.enrollment_rejects, condition: \"stage = 'x'\", "
         "lineage: { version: { column: record_version } } }\n")
    config, errors, warnings = validate_conf(conf)
    assert errors == [] and not [w for w in warnings if w.field == f"{HOP}.reject_policy"]
    [hop] = [h for h in hops(config) if h.dataset == "gold_member_coverage"]
    assert hop.contract.rejects.table == "example_db.enrollment_rejects"
    assert hop.contract.rejects.time_column.column == "rejected_at"
    assert not any("no rejects evidence" in s for s in blind_spots(config, hop))
