"""HOP_CONTRIBUTION (build step 5): a join_contribution contributor's reference resolves to the version
current at each row's load time, and what it contributes agrees (reconciliation model section 6.2).

Shown able to FAIL (unresolved, divergent), to PASS and to be DID_NOT_RUN, with model example 9.2's
join case as a test. Window [2026-01-15 12:00, 2026-01-16 12:00) UTC as in test_hop.py; all times
UTC. A synthetic coverage table here takes provider_nm from a provider contributor (its
provider_name) by provider_id.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.hop.contribution import HopContribution
from hcsc.datalake.dre.checks.hop.ledger import HopLedger
from hcsc.datalake.dre.config.models import Dataset, EdgeContract, TimeColumn
from hcsc.datalake.dre.store.local_setup import create_store
from tests.checks.test_hop import DQ, EVENT
from tests.fixtures.layers import create_table
from tests.fixtures.settings import SETTINGS

PROVIDER = "example_provider"
HERE_COLUMNS = [("member_id", "STRING"), ("provider_id", "STRING"), ("provider_nm", "STRING"),
                ("src_lcts", "TIMESTAMP"), ("loaded_at", "TIMESTAMP")]
PROVIDER_COLUMNS = [("provider_id", "STRING"), ("provider_name", "STRING"), ("updated_at", "TIMESTAMP"),
                    ("loaded_at", "TIMESTAMP")]
IN_WINDOW = datetime(2026, 1, 15, 14, 0)
LATER = datetime(2026, 1, 15, 18, 0)
BEFORE = datetime(2026, 1, 14, 10, 0)    # before the window
JOIN = EdgeContract(mapping="join_contribution", reference={"provider_id": "provider_id"},
                    contributes={"provider_name": "provider_nm"})


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


def here_row(member: str, provider: str | None, name: str | None, loaded: datetime | None = IN_WINDOW) -> dict:
    return {"member_id": member, "provider_id": provider, "provider_nm": name, "src_lcts": BEFORE, "loaded_at": loaded}


def provider_row(provider: str, name: str, loaded: datetime = BEFORE) -> dict:
    return {"provider_id": provider, "provider_name": name, "updated_at": loaded, "loaded_at": loaded}


def hop(spark, name: str, here_rows: list[dict], provider_rows: list[dict], contract: EdgeContract = JOIN,
        **here_fields) -> tuple[Dataset, Dataset]:
    create_table(spark, f"hop.{name}_here", HERE_COLUMNS, here_rows)
    create_table(spark, f"hop.{name}_provider", PROVIDER_COLUMNS, provider_rows)
    utc = {"timezone": "UTC"}
    provider = Dataset(dataset=PROVIDER, table=f"hop.{name}_provider", layer="CURATED", key=["provider_id"],
                       key_normalise={"provider_id": "strip_leading_zeros"},
                       record_time=TimeColumn(column="updated_at", **utc), load_time=TimeColumn(column="loaded_at", **utc))
    here = Dataset(**{"dataset": f"gold_{name}", "table": f"hop.{name}_here", "layer": "GOLD", "key": ["member_id"],
                      "upstream": ["example_enrollment", PROVIDER], "record_time": TimeColumn(column="src_lcts", **utc),
                      "load_time": TimeColumn(column="loaded_at", **utc), "contracts": {PROVIDER: contract},
                      **here_fields})
    return here, provider


def contribution(spark, here: Dataset, provider: Dataset | None, full_sweep: bool = False):
    ctx = CheckContext(spark, here, None, SETTINGS, "UTC", DQ, full_sweep=full_sweep,
                       upstreams={PROVIDER: provider} if provider is not None else {})
    [r], _ = run_check(HopContribution(), ctx, EVENT)
    assert r.group_values == {"upstream": PROVIDER}
    return r


def lines(result) -> dict[tuple[str, str | None], int]:
    return {(x["line"], x.get("label")): x["records"] for x in result.reconciliation}


def test_model_example_9_2_one_unresolved_reference_fails(spark) -> None:
    """Model 9.2: 50 coverage rows; 49 resolve to a current provider version and agree on the name; one
    row's provider_id resolves to nothing: unresolved, cause JOIN_UNMATCHED. FAILED, 1 violation."""
    rows = [here_row(f"M{i:02d}", f"P{i:02d}", f"Provider {i:02d}") for i in range(49)]
    rows.append(here_row("M49", "P99", "Provider 99"))
    here, provider = hop(spark, "contrib_model", rows, [provider_row(f"P{i:02d}", f"Provider {i:02d}") for i in range(49)])
    r = contribution(spark, here, provider)
    assert (r.state, r.population, r.violations) == ("FAILED", 50, 1)
    assert r.observed == "due 50: resolved 49, unresolved 1 (cause JOIN_UNMATCHED: no version 1), divergent 0"
    got = lines(r)
    assert (got[("due", None)], got[("resolved", None)], got[("unresolved", "no version")], got[("divergent", None)]) \
        == (50, 49, 1, 0)
    assert all(set(x) >= {"upstream", "line", "class", "records", "evidence_level", "scope"} for x in r.reconciliation)
    assert all(x["scope"] == r.detail for x in r.reconciliation)
    scope = json.loads(r.detail)
    assert (scope["edge"], scope["mapping"], scope["cause"]) == (f"{PROVIDER}->gold_contrib_model", "join_contribution",
                                                                 "JOIN_UNMATCHED")
    assert "completeness" in scope["claim"]  # never a completeness claim: the primary edge counts the row


def test_every_reference_resolving_and_agreeing_passes(spark) -> None:
    """key_normalise applies on both sides: the provider holds 0042, the row references 42."""
    here, provider = hop(spark, "contrib_ok", [here_row("M1", "42", "Provider 42"), here_row("M2", "7", "Provider 7")],
                         [provider_row("0042", "Provider 42"), provider_row("7", "Provider 7")])
    r = contribution(spark, here, provider)
    assert (r.state, r.population, r.violations) == ("PASSED", 2, 0)
    assert r.observed == "due 2: resolved 2, unresolved 0, divergent 0"
    assert "cause" not in json.loads(r.detail)


def test_the_version_current_at_the_rows_load_time_is_the_one_compared(spark) -> None:
    """P1 is renamed at 16:00. A row loaded at 14:00 with the old name agrees (the new version was not
    loaded yet); a row loaded at 18:00 with the old name is divergent. P2's only version arrives at 20:00,
    after the row that references it: unresolved. A row with no provider_id references nothing:
    unresolved. A row loaded before the window is not in this window's population; one with no load
    time is untimed, beside it."""
    renamed = datetime(2026, 1, 15, 16, 0)
    here, provider = hop(spark, "contrib_time", [
        here_row("M1", "P1", "Old name"), here_row("M2", "P1", "Old name", loaded=LATER),
        here_row("M3", "P2", "Provider 2"), here_row("M4", None, None),
        here_row("M5", "P9", "Provider 9", loaded=BEFORE), here_row("M6", "P1", "Old name", loaded=None),
    ], [provider_row("P1", "Old name"), provider_row("P1", "New name", loaded=renamed),
        provider_row("P2", "Provider 2", loaded=datetime(2026, 1, 15, 20, 0))])
    r = contribution(spark, here, provider)
    assert (r.state, r.population, r.violations) == ("FAILED", 4, 3)
    assert r.observed == ("due 4 (untimed 1): resolved 1, unresolved 2 (cause JOIN_UNMATCHED: no reference 1, "
                          "no version 1), divergent 1")
    got = lines(r)
    assert (got[("divergent", None)], got[("untimed", None)]) == (1, 1)
    sweep = contribution(spark, here, provider, full_sweep=True)  # every row loaded before the window end
    assert (sweep.population, sweep.violations) == (5, 4)  # P9 does not exist: unresolved too
    assert json.loads(sweep.detail)["full_sweep"] is True


def test_a_contributed_value_that_disagrees_is_divergent(spark) -> None:
    here, provider = hop(spark, "contrib_div", [here_row("M1", "P1", "Wrong name"), here_row("M2", "P2", None)],
                         [provider_row("P1", "Right name"), provider_row("P2", "Provider 2")])
    r = contribution(spark, here, provider)
    assert (r.state, r.violations) == ("FAILED", 2)
    assert r.observed == "due 2: resolved 0, unresolved 0, divergent 2"
    assert "cause" not in json.loads(r.detail)  # JOIN_UNMATCHED names unresolved rows only
    # without contributes only the reference's resolution is checked
    plain = JOIN.model_copy(update={"contributes": {}})
    here, provider = hop(spark, "contrib_plain", [here_row("M1", "P1", "Wrong name")], [provider_row("P1", "Right")],
                         contract=plain)
    r = contribution(spark, here, provider)
    assert (r.state, r.observed) == ("PASSED", "due 1: resolved 1, unresolved 0, divergent 0")
    assert any("no contributed value is compared" in s for s in json.loads(r.detail)["blind_spots"])


def test_nothing_loaded_in_the_window_is_did_not_run_and_still_records_its_lines(spark) -> None:
    here, provider = hop(spark, "contrib_empty", [here_row("M1", "P1", "P", loaded=BEFORE)], [provider_row("P1", "P")])
    r = contribution(spark, here, provider)
    assert (r.state, r.reason_category, r.reason_code) == ("DID_NOT_RUN", "DATA_UNAVAILABLE", "empty_population")
    assert lines(r)[("due", None)] == 0


def test_missing_configuration_is_did_not_run(spark) -> None:
    here, provider = hop(spark, "contrib_dnr", [here_row("M1", "P1", "P")], [provider_row("P1", "P")])
    no_reference = here.model_copy(update={"contracts": {PROVIDER: EdgeContract(mapping="join_contribution")}})
    for ds, up, code in ((no_reference, provider, "invalid_config"), (here, None, "invalid_config"),
                         (here.model_copy(update={"load_time": None}), provider, "invalid_config"),
                         (here, provider.model_copy(update={"load_time": None}), "invalid_config"),
                         (here, provider.model_copy(update={"table": "hop.contrib_no_such_table"}), "table_missing")):
        r = contribution(spark, ds, up)
        assert (r.state, r.reason_code) == ("DID_NOT_RUN", code), (ds.load_time, up)
    gone = here.model_copy(update={"contracts": {PROVIDER: JOIN.model_copy(update={
        "contributes": {"provider_name": "no_such_column"}})}})
    # a missing column fails the check's preconditions: one result for the check, as for every check
    ctx = CheckContext(spark, gone, None, SETTINGS, "UTC", DQ, upstreams={PROVIDER: provider})
    [missing], _ = run_check(HopContribution(), ctx, EVENT)
    assert (missing.state, missing.reason_code) == ("DID_NOT_RUN", "column_missing")


def test_an_exception_inside_the_check_is_did_not_run(spark, monkeypatch) -> None:
    from hcsc.datalake.dre.checks.hop import contribution as module

    here, provider = hop(spark, "contrib_boom", [here_row("M1", "P1", "P")], [provider_row("P1", "P")])

    def boom(*_args, **_kwargs):
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr(module, "counted", boom)
    r = contribution(spark, here, provider)
    assert r.state == "DID_NOT_RUN" and r.reason_code is not None and "synthetic" not in (r.detail or "")


def test_the_contributor_never_counts_toward_completeness(spark) -> None:
    """HOP_LEDGER leaves the contributor hop unsupported: the row is counted once, by its primary edge."""
    here, provider = hop(spark, "contrib_ledger", [here_row("M1", "P1", "P")], [provider_row("P1", "P")])
    ctx = CheckContext(spark, here, None, SETTINGS, "UTC", DQ, upstreams={PROVIDER: provider})
    [r], _ = run_check(HopLedger(), ctx, EVENT)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "unsupported_mapping")
    assert "HOP_CONTRIBUTION" in json.loads(r.detail)["note"]
    assert HopContribution().applies_to(here, "TABLE_MERGE")
    assert not HopContribution().applies_to(here.model_copy(update={"contracts": {}}), "TABLE_MERGE")


def test_no_key_value_leaves_the_check(spark) -> None:
    here, provider = hop(spark, "contrib_private", [here_row("M555123", "P555123", "Name 555123")], [])
    r = contribution(spark, here, provider)
    assert r.state == "FAILED"
    assert "555123" not in json.dumps([r.observed, r.detail, list(r.reconciliation)], default=str)
