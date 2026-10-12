"""Derived datasets in config: defaults, the table-or-derived rule, and validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from hcsc.datalake.dre.config.models import Dataset
from hcsc.datalake.dre.config.validate import validate_conf
from tests.config.conftest import edit

DERIVED = {"from": "curated_claims", "group_by": {"memberid": "curated_claims.memberid"}}


def test_a_derived_dataset_gets_the_engine_s_times_and_no_table() -> None:
    ds = Dataset(dataset="by_member", layer="CURATED", key=["memberid"], derived=DERIVED)
    assert ds.table is None and ds.derived.from_ == "curated_claims" and ds.derived.sources == ["curated_claims"]
    assert (ds.record_time.column, ds.record_time.timezone) == ("record_ts", "UTC")
    assert (ds.load_time.column, ds.load_time.granularity, ds.load_time.timezone) == ("load_ts", "minute", "UTC")


@pytest.mark.parametrize("fields", [{}, {"table": "db.t", "derived": DERIVED}])
def test_a_dataset_is_a_table_or_derived(fields) -> None:
    with pytest.raises(ValueError, match="either a table or derived"):
        Dataset(dataset="x", layer="GOLD", key=["k"], **fields)


CLAIMS = """dataset: curated_claims
table: curated_db.claims
layer: CURATED
owner: membership-gold           # no feed lists it in this test, so it is table-wide
expectation_version: 1
key: [memberid, servicedate]
record_time: { column: sourcelastupdatets }
load_time: { column: sourcelastupdatets, granularity: minute }
"""
BY_MONTH = """dataset: claims_by_member_month
layer: CURATED
key: [memberid, service_month]
derived:
  from: curated_claims
  group_by:
    memberid: curated_claims.memberid
    service_month: "substr(curated_claims.servicedate, 1, 7)"
  columns:
    claim_count: "count(*)"
"""
GOLD = """dataset: gold_claims_by_month
table: gold_db.claims_by_month
layer: GOLD
owner: membership-gold
expectation_version: 1
upstream: [claims_by_member_month]
key: [mbr_id, svc_month]
record_time: { column: src_lcts }
load_time: { column: gld_lcts, granularity: minute }
key_map:
  claims_by_member_month: { mbr_id: memberid, svc_month: service_month }
owned_columns: { claim_count: clm_cnt }
"""


def _with_derived(conf: Path) -> None:
    (conf / "datasets" / "curated_claims.yaml").write_text(CLAIMS, encoding="utf-8")
    (conf / "datasets" / "claims_by_member_month.yaml").write_text(BY_MONTH, encoding="utf-8")
    (conf / "datasets" / "gold_claims_by_month.yaml").write_text(GOLD, encoding="utf-8")


def test_a_derived_upstream_validates(conf: Path) -> None:
    _with_derived(conf)
    _, errors, warnings = validate_conf(conf)
    assert errors == [], "\n".join(map(str, errors))
    assert not [w for w in warnings if "claims" in str(w)]


CASES = [
    ("feed-lists-derived", "feeds/example_realtime.yaml", "datasets: [", "datasets: [claims_by_member_month, ",
     "derived", "lists this derived dataset", "remove it from the feed"),
    ("unknown-from", "datasets/claims_by_member_month.yaml", "from: curated_claims", "from: nowhere",
     "derived.from", "unknown dataset", "add datasets/nowhere.yaml"),
    ("key-not-a-group", "datasets/claims_by_member_month.yaml", "key: [memberid, service_month]",
     "key: [memberid, plan]", "key[1]", "not a group_by output",
     "group_by outputs only"),
    ("own-time", "datasets/claims_by_member_month.yaml", "derived:",
     "record_time: { column: sourcelastupdatets }\nderived:", "record_time", "engine's record_ts",
     "leave record_time out"),
    ("no-feed-filter", "datasets/claims_by_member_month.yaml", "derived:", "feed_filter: \"x = 1\"\nderived:",
     "feed_filter", "has no feed_filter", "derived.where"),
    ("not-a-downstream", "datasets/claims_by_member_month.yaml", "derived:",
     "upstream: [curated_claims]\nderived:", "key_map", "cannot be a downstream", "upstream of the dataset"),
    ("from-needs-load-time", "datasets/curated_claims.yaml",
     "load_time: { column: sourcelastupdatets, granularity: minute }\n", "", "load_time",
     "derives record_ts and load_ts from it", "add load_time"),
]


@pytest.mark.parametrize(("case", "rel", "old", "new", "field", "problem", "fix"), CASES, ids=[c[0] for c in CASES])
def test_derived_validation_errors(conf: Path, case, rel, old, new, field, problem, fix) -> None:
    _with_derived(conf)
    edit(conf, rel, old, new)
    _, errors, _ = validate_conf(conf)
    matching = [e for e in errors if e.field == field and problem in e.problem]
    assert matching, "\n".join(map(str, errors))
    assert fix in matching[0].fix
    assert matching[0].line >= 1


def test_an_unused_derived_dataset_is_a_warning(conf: Path) -> None:
    _with_derived(conf)
    (conf / "datasets" / "gold_claims_by_month.yaml").unlink()
    _, errors, warnings = validate_conf(conf)
    assert errors == []
    assert any("no dataset names this derived dataset as an upstream" in w.problem for w in warnings)
