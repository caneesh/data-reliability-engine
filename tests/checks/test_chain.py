"""CHAIN_COMPLETENESS (build step 6): every due source record followed edge by edge to the terminal.

A synthetic three-layer chain, raw -> curated -> gold (tests/fixtures/layers.py). Window
[2026-01-15 12:00, 2026-01-16 12:00) UTC. curated's sla_hours is 4 and gold's 8, so the chain's SLA is
12 hours and a raw record is due for the chain when its version was loaded in [2026-01-15 00:00,
2026-01-16 00:00). Raw dates are MM/DD/YYYY and normalised; gold's sub_id is zero-padded and normalised.
Each outcome is shown able to FAIL, to PASS and to be DID_NOT_RUN.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from hcsc.datalake.dre.checks.base import CheckContext, did_not_run, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.hop.chain import ChainCompleteness, paths
from hcsc.datalake.dre.checks.hop.common import key_hash
from hcsc.datalake.dre.checks.patterns import checks_for, pattern_check_ids
from hcsc.datalake.dre.config.loader import Config
from hcsc.datalake.dre.config.models import Dataset, Defaults, EdgeContract, Effective, Exclusion, TimeColumn
from hcsc.datalake.dre.runner import covers
from hcsc.datalake.dre.store.local_setup import create_store
from hcsc.datalake.dre.store.names import DQ_TABLES
from tests.checks.test_hop import GOLD_LOAD, curated, gold
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, RAW_COLUMNS, create_table, curated_row, gold_row, raw_row
from tests.fixtures.settings import SETTINGS

UTC = timezone.utc
DQ = "dq_chain"
RAW, CUR, GOLD = "example_raw_enrollment", "example_curated_enrollment", "gold_member_coverage"
LABEL = f"{RAW}->{GOLD}"
EDGE1, EDGE2 = f"{RAW}->{CUR}", f"{CUR}->{GOLD}"
EVENT = Event(RAW, datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 1, 16, 12, tzinfo=UTC))
LOADED = datetime(2026, 1, 15, 6, 0)       # raw load in the chain's range: deadline 18:00, judged
CURATED_AT = datetime(2026, 1, 15, 7, 0)   # curated's load: its gold deadline (15:00) passed long before the cutoff
GOLD_AT = datetime(2026, 1, 15, 8, 0)
LATE_RAW = datetime(2026, 1, 16, 2, 0)     # raw load whose chain deadline (14:00 on the 16th) is after the cutoff
LATE_CURATED = datetime(2026, 1, 16, 5, 0)  # curated's load + 8h is after the cutoff: pending at curated -> gold
SECRET = b"synthetic-test-secret"
RAW_TABLE_COLUMNS = [*RAW_COLUMNS, ("qualifiedhealthplanid", "STRING"), ("msg_ts", "TIMESTAMP")]
IDENTITY = {c: c for c in ("subscriberidnumber", "membernumber", "effectivedate", "qualifiedhealthplanid")}


def key_of(sub_id: str) -> str:
    """The canonical key the chain follows: raw's columns in curated's key order, normalised."""
    return f"{sub_id}|01|2026-01-01|AGR-A"


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


def raw(table: str, **extra) -> Dataset:
    fields = {"dataset": RAW, "table": table, "layer": "RAW", "key": list(IDENTITY),
              "key_normalise": {"effectivedate": {"parse_date": "MM/dd/yyyy"}},
              "record_time": TimeColumn(column="msg_ts", timezone="UTC"),
              "load_time": TimeColumn(column="msg_ts", granularity="minute", timezone="UTC"),
              "check_params": {"CHAIN_COMPLETENESS": {"terminal": GOLD}}, **extra}
    return Dataset(**fields)


def layers(spark, name: str, raw_rows: list, curated_rows: list, gold_rows: list,
           cur_extra: dict | None = None, gold_extra: dict | None = None) -> dict[str, Dataset]:
    create_table(spark, f"chain.{name}_raw", RAW_TABLE_COLUMNS, raw_rows)
    create_table(spark, f"chain.{name}_cur", CURATED_COLUMNS, curated_rows)
    create_table(spark, f"chain.{name}_gold", GOLD_COLUMNS, gold_rows)
    return {RAW: raw(f"chain.{name}_raw"),
            CUR: curated(f"chain.{name}_cur", **{"upstream": [RAW], "key_map": {RAW: IDENTITY}, "sla_hours": 4,
                                                  **(cur_extra or {})}),
            GOLD: gold(f"chain.{name}_gold", dataset=GOLD, **(gold_extra or {}))}


def raw_record(sub_id: str, loaded: datetime = LOADED) -> dict:
    return {**raw_row("rt_0001.seq", sub_id=sub_id), "qualifiedhealthplanid": "AGR-A", "msg_ts": loaded}


def config(datasets: dict[str, Dataset]) -> Config:
    defaults = Defaults(dq_database=DQ, environment="test", timezone="UTC",
                        retention_months={t: 84 for t in DQ_TABLES}, **SETTINGS.model_dump())
    return Config(conf_dir=Path("."), defaults=defaults, datasets=datasets)


def chain_of(spark, datasets: dict[str, Dataset], secret: bytes | None = None, full_sweep: bool = False):
    ctx = CheckContext(spark, datasets[RAW], None, config(datasets).dataset_settings(RAW), "UTC", DQ,
                       key_secret=secret, full_sweep=full_sweep, config=config(datasets))
    [r], _ = run_check(ChainCompleteness(), ctx, EVENT)
    return r


def scope(result) -> dict:
    return json.loads(result.detail)


def lines(result) -> dict[tuple[str, str | None], int]:
    found: dict[tuple[str, str | None], int] = {}
    for x in result.reconciliation:
        found[(x["line"], x.get("label"))] = found.get((x["line"], x.get("label")), 0) + (x["records"] or 0)
    return found


def outcomes(result) -> dict[str, int]:
    return {k: v for k, v in scope(result)["outcomes"].items() if k in ("due", "reached", "accounted", "lost",
                                                                         "unobservable", "pending")}


def events_of(result) -> list[tuple[str, str, str, str]]:
    return sorted((r.key_hash, r.event, r.state_detail, r.key_value) for r in result.key_events.collect())


def through(sub_id: str, cur_at: datetime = CURATED_AT, gold_at: datetime = GOLD_AT,
            raw_at: datetime = LOADED) -> tuple[dict, dict, dict]:
    """A record that reached every layer."""
    return (raw_record(sub_id, raw_at), curated_row(sub_id=sub_id, updated=cur_at),
            gold_row(sub_id="000" + sub_id, record=gold_at))


def build(spark, name: str, reached: list[str], raw_only: list[str] = (), to_curated: list[str] = (),
          **extra) -> dict[str, Dataset]:
    rows = [through(s) for s in reached]
    return layers(spark, name,
                  [r for r, _, _ in rows] + [raw_record(s) for s in [*raw_only, *to_curated]],
                  [c for _, c, _ in rows] + [curated_row(sub_id=s, updated=CURATED_AT) for s in to_curated],
                  [g for _, _, g in rows], **extra)


# --- outcomes ---

def test_every_due_record_reaching_the_terminal_passes(spark) -> None:
    datasets = build(spark, "ch_ok", reached=["123401", "123402", "123403"])
    r = chain_of(spark, datasets, secret=SECRET)
    assert (r.state, r.population, r.violations) == ("PASSED", 3, 0)
    assert r.group_values == {"chain": LABEL}
    assert r.observed == "due 3 (pending 0): reached 3, accounted 0, lost 0, unobservable 0"
    s = scope(r)
    assert s["path"] == [RAW, CUR, GOLD]
    assert [(e["edge"], e["mapping"], e["contract"], e["sla_hours"]) for e in s["edges"]] == [
        (EDGE1, "dedup", "inferred from key_map", 4), (EDGE2, "dedup", "inferred from key_map", 8)]
    assert s["chain_sla_hours"] == 12 and s["cutoff"] == EVENT.window_end.isoformat()
    assert s["deadline_range"] == ["2026-01-15T00:00:00+00:00", "2026-01-16T00:00:00+00:00"]
    assert s["first_observed_gap"] is None and s["proven_segment"] == [EDGE1, EDGE2]
    assert r.coverage == (datetime(2026, 1, 15, tzinfo=UTC), datetime(2026, 1, 16, tzinfo=UTC))
    got = lines(r)
    assert got[("due", None)] == 3 and got[("reached", EDGE2)] == 3
    assert got[("accounted", None)] == got[("lost", None)] == got[("unobservable", None)] == 0
    assert all(x["upstream"] == LABEL and x["scope"] == r.detail for x in r.reconciliation)
    # every edge's qualifications come with it: an inferred contract never makes a healthy chain PASS
    assert "inferred_contract" in r.qualifications and s["claim"] == "inferred contract"
    assert r.key_events is None  # nothing lost: no exception keys


def test_a_declared_chain_with_nothing_unproven_is_contract_proven(spark) -> None:
    datasets = build(spark, "ch_proven", reached=["123401"],
                     cur_extra={"contracts": {RAW: EdgeContract(mapping="identity")}},
                     gold_extra={"contracts": {CUR: EdgeContract(mapping="identity")}, "load_time": GOLD_LOAD})
    r = chain_of(spark, datasets)
    assert r.state == "PASSED"
    assert (r.qualifications, scope(r)["claim"]) == ((), "contract-proven")


def test_a_record_lost_at_the_second_edge_fails_and_names_the_first_observed_gap(spark) -> None:
    datasets = build(spark, "ch_lost2", reached=["123401", "123402"], to_curated=["123403"])
    r = chain_of(spark, datasets, secret=SECRET)
    assert (r.state, r.population, r.violations) == ("FAILED", 3, 1)
    assert r.observed == ("due 3 (pending 0): reached 2, accounted 0, lost 1, unobservable 0; "
                          f"first observed gap {EDGE2}")
    s = scope(r)
    assert s["first_observed_gap"]["edge"] == EDGE2 and "not necessarily" in s["first_observed_gap"]["note"]
    o = outcomes(r)
    assert o["reached"] + o["accounted"] + o["lost"] + o["unobservable"] == o["due"] == 3
    assert lines(r)[("lost", EDGE2)] == 1
    assert [x["class"] for x in r.reconciliation if x["line"] == "lost"] == ["deficit"]
    assert s["by_edge"][EDGE2] == {"lost missing": 1}
    assert s["triage"]
    # exception keys only: the lost record, hashed with the secret; the reached ones are not events
    assert events_of(r) == [(key_hash(SECRET, key_of("123403")), "LOST", f"missing at {EDGE2}", key_of("123403"))]


def test_the_first_observed_gap_is_the_first_edge_that_lost_a_record(spark) -> None:
    datasets = build(spark, "ch_lost12", reached=["123401"], raw_only=["123402"], to_curated=["123403"])
    r = chain_of(spark, datasets)
    assert (r.state, r.population, r.violations) == ("FAILED", 3, 2)
    assert scope(r)["first_observed_gap"]["edge"] == EDGE1
    assert lines(r)[("lost", EDGE1)] == 1 and lines(r)[("lost", EDGE2)] == 1
    assert scope(r)["key_events"] == "not recorded: hmac_secret_file is not set"
    assert r.key_events is None


def test_a_record_an_approved_exclusion_keeps_out_is_accounted_and_passes(spark) -> None:
    exclusion = Exclusion(id="drop_test_members", version=2, condition="subscriberidnumber LIKE '9%'",
                          effective=Effective(**{"from": date(2026, 1, 1)}), approved_by="CHG-0000", max_share=0.5)
    datasets = build(spark, "ch_excl", reached=["123401", "123402"], to_curated=["900004"],
                     gold_extra={"contracts": {CUR: EdgeContract(mapping="dedup", exclusions=[exclusion])}})
    r = chain_of(spark, datasets, secret=SECRET)
    assert (r.state, r.population, r.violations) == ("PASSED", 3, 0)
    assert r.observed == "due 3 (pending 0): reached 2, accounted 1, lost 0, unobservable 0"
    assert lines(r)[("accounted", EDGE2)] == 1
    [accounted] = [x for x in r.reconciliation if x["line"] == "accounted"]
    assert (accounted["class"], accounted["evidence_level"]) == ("authorised", "reconstructed")
    assert scope(r)["by_edge"][EDGE2] == {"accounted excluded drop_test_members@2": 1}
    assert scope(r)["evidence_level"] == "reconstructed"
    assert r.key_events is None  # an accounted record is not an exception


def test_an_edge_the_ledger_does_not_judge_makes_a_partial_chain(spark) -> None:
    datasets = build(spark, "ch_partial", reached=["123401", "123402"],
                     gold_extra={"contracts": {CUR: EdgeContract(mapping="aggregate")}})
    r = chain_of(spark, datasets, secret=SECRET)
    assert (r.state, r.reason_category, r.reason_code) == ("DID_NOT_RUN", "DATA_UNAVAILABLE", "partial_chain")
    s = scope(r)
    assert s["unobservable"]["edge"] == EDGE2 and "aggregate" in s["unobservable"]["why"]
    assert s["unobservable"]["proven_segment"] == f"{RAW} -> {CUR}" and s["proven_segment"] == [EDGE1]
    assert lines(r)[("unobservable", EDGE2)] == 2
    [line] = [x for x in r.reconciliation if x["line"] == "unobservable"]
    assert line["evidence_level"] == "unavailable"
    assert r.coverage is not None and covers(r)  # a partial chain judged what it can see: it covers its range
    assert {e[1] for e in events_of(r)} == {"UNOBSERVABLE"}
    assert {e[2] for e in events_of(r)} == {f"unobservable at {EDGE2}"}


def test_a_loss_on_the_observable_segment_still_fails_a_partial_chain(spark) -> None:
    datasets = build(spark, "ch_partial_lost", reached=["123401"], raw_only=["123402"],
                     gold_extra={"contracts": {CUR: EdgeContract(mapping="fan_out")}})
    r = chain_of(spark, datasets)
    assert (r.state, r.population, r.violations) == ("FAILED", 2, 1)
    assert scope(r)["first_observed_gap"]["edge"] == EDGE1 and outcomes(r)["unobservable"] == 1


def test_a_record_pending_at_an_edge_is_pending_for_the_chain(spark) -> None:
    rows = [through("123401"), through("123402", cur_at=LATE_CURATED, gold_at=LATE_CURATED)]
    datasets = layers(spark, "ch_pend", [r for r, _, _ in rows] + [raw_record("123405", LATE_RAW)],
                      [c for _, c, _ in rows], [rows[0][2]])  # 123402 is not in gold yet: pending, not lost
    r = chain_of(spark, datasets)
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)
    assert r.observed == "due 1 (pending 2): reached 1, accounted 0, lost 0, unobservable 0"
    assert scope(r)["outcomes"]["pending_at_edges"] == {EDGE2: 1}
    got = lines(r)
    assert got[("pending", None)] == 1 and got[("pending", EDGE2)] == 1 and got[("due", None)] == 1


def test_nothing_due_is_empty_population_and_still_covers(spark) -> None:
    datasets = layers(spark, "ch_empty", [raw_record("123405", LATE_RAW)], [], [])
    r = chain_of(spark, datasets)
    assert (r.state, r.reason_category, r.reason_code) == ("DID_NOT_RUN", "DATA_UNAVAILABLE", "empty_population")
    assert lines(r)[("pending", None)] == 1 and lines(r)[("due", None)] == 0
    assert r.coverage is not None


# --- the path ---

def test_no_path_an_ambiguous_path_or_an_unknown_terminal_is_invalid_config(spark) -> None:
    datasets = build(spark, "ch_paths", reached=["123401"])
    alone = Dataset(dataset="example_other", table="chain.ch_paths_gold", layer="GOLD", key=["sub_id"])
    for terminal, why in ((alone.dataset, "no key_map path"), ("unknown_ds", "not a configured dataset"),
                          (RAW, "is the chain's source")):
        ds = {**datasets, alone.dataset: alone,
              RAW: datasets[RAW].model_copy(update={"check_params": {"CHAIN_COMPLETENESS": {"terminal": terminal}}})}
        r = chain_of(spark, ds)
        assert (r.state, r.reason_category, r.reason_code) == ("DID_NOT_RUN", "CONFIGURATION", "invalid_config")
        assert why in r.detail
    # a second curated layer feeding gold: two paths, so no chain
    twin = datasets[CUR].model_copy(update={"dataset": "example_curated_twin"})
    two = {**datasets, twin.dataset: twin,
           GOLD: datasets[GOLD].model_copy(update={"key_map": {**datasets[GOLD].key_map,
                                                                twin.dataset: datasets[GOLD].key_map[CUR]}})}
    assert len(paths(two, RAW, GOLD)) == 2
    r = chain_of(spark, two)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "invalid_config")
    assert "more than one key_map path" in r.detail and r.group_values == {"chain": LABEL}


def test_a_missing_layer_table_is_did_not_run_naming_the_edge(spark) -> None:
    datasets = build(spark, "ch_gone", reached=["123401"])
    spark.sql("DROP TABLE IF EXISTS chain.ch_gone_gold")
    r = chain_of(spark, datasets)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "table_missing")
    assert r.detail.startswith(f"edge {EDGE2}:")


# --- where it runs ---

def test_it_is_listed_where_hop_ledger_is_and_runs_only_where_configured() -> None:
    for pattern in ("FILE_CYCLIC", "FILE_PERIODIC", "TABLE_MERGE"):
        assert "CHAIN_COMPLETENESS" in pattern_check_ids(pattern)
    configured = raw("chain.any_raw")
    plain = configured.model_copy(update={"check_params": {}})
    assert "CHAIN_COMPLETENESS" in {c.check_id for c in checks_for(configured, "FILE_CYCLIC")}
    assert "CHAIN_COMPLETENESS" not in {c.check_id for c in checks_for(plain, "FILE_CYCLIC")}


def test_partial_chain_never_holds_a_window_and_covers() -> None:
    r = did_not_run("partial_chain")
    assert r.reason_category == "DATA_UNAVAILABLE" and covers(r)


def test_one_record_mapped_to_several_keys_downstream_is_unobservable(spark) -> None:
    """Gold keys on the agreement, which curated holds outside its key: the chain maps each record
    forward through curated's rows, so one agreement reaches gold and two cannot be followed."""
    short_key = [c for c in IDENTITY if c != "qualifiedhealthplanid"]
    datasets = layers(
        spark, "ch_fanout", [raw_record("123401"), raw_record("123402")],
        [curated_row(sub_id="123401", updated=CURATED_AT),
         curated_row(sub_id="123402", updated=CURATED_AT), curated_row(sub_id="123402", agreement="AGR-B",
                                                                         updated=CURATED_AT)],
        [gold_row(sub_id="000123401", record=GOLD_AT)],
        cur_extra={"key": short_key, "key_map": {RAW: {c: c for c in short_key}}})
    r = chain_of(spark, datasets, secret=SECRET)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "partial_chain")
    assert (outcomes(r)["reached"], outcomes(r)["unobservable"]) == (1, 1)
    assert lines(r)[("unobservable", EDGE2)] == 1
    assert events_of(r) == [(key_hash(SECRET, "123402|01|2026-01-01"), "UNOBSERVABLE", f"unobservable at {EDGE2}",
                             "123402|01|2026-01-01")]


def test_the_chain_range_continues_from_the_chain_coverage(spark) -> None:
    """The range starts where the chain's latest committed covering result of an earlier window ended,
    read under its own group (`chain`), never a hop's."""
    from tests.store.conftest import append_rows

    early = datetime(2026, 1, 15, 2, 0)  # in the window's own range, but covered by the earlier window
    rows = [through("123401", raw_at=early), through("123402")]
    datasets = layers(spark, "ch_cov", [r for r, _, _ in rows], [c for _, c, _ in rows], [g for _, _, g in rows])
    earlier = Event(RAW, datetime(2026, 1, 14, 12, tzinfo=UTC), datetime(2026, 1, 15, 12, tzinfo=UTC))
    append_rows(spark, DQ, "dq_check_result", [{
        "evaluation_id": "e-chain-cov", "run_id": "r-earlier", "event_id": "x",
        "window_start": earlier.window_start, "window_end": earlier.window_end, "execution_type": "NORMAL",
        "dataset": RAW, "check_id": "CHAIN_COMPLETENESS", "state": "PASSED", "group_values": {"chain": LABEL},
        "evaluated_at": earlier.window_end, "coverage_from": datetime(2026, 1, 14, 0, tzinfo=UTC),
        "coverage_to": datetime(2026, 1, 15, 3, tzinfo=UTC), "run_date": earlier.window_end.date()}])
    r = chain_of(spark, datasets)
    assert (r.state, r.population) == ("PASSED", 1)
    assert r.coverage == (datetime(2026, 1, 15, 3, tzinfo=UTC), datetime(2026, 1, 16, tzinfo=UTC))


# --- final review P1-03: a record pending at a later edge is judged once it is due ---

NEXT = Event(RAW, datetime(2026, 1, 16, 12, tzinfo=UTC), datetime(2026, 1, 17, 12, tzinfo=UTC))
AFTER = Event(RAW, datetime(2026, 1, 17, 12, tzinfo=UTC), datetime(2026, 1, 18, 12, tzinfo=UTC))
CHAIN_DQ = "dq_chain_carry"


def chain_at(spark, datasets: dict[str, Dataset], event: Event, secret: bytes | None = SECRET,
             history_since: date | None = None, key_events_since: date | None = None, full_sweep: bool = False,
             coverage_since: date | None = None):
    """The chain evaluated as a NORMAL run would, reading its own committed history in CHAIN_DQ;
    history_since and key_events_since as the runner sets them (the lookback, dq_key_event's retention)."""
    cfg = config(datasets)
    cfg = dataclasses.replace(cfg, defaults=cfg.defaults.model_copy(update={"dq_database": CHAIN_DQ}))
    retention = {"key_events_since": key_events_since} if key_events_since is not None else {}
    ctx = CheckContext(spark, datasets[RAW], None, cfg.dataset_settings(RAW), "UTC", CHAIN_DQ,
                       key_secret=secret, config=cfg, history_since=history_since, full_sweep=full_sweep,
                       coverage_since=coverage_since, **retention)
    [r], _ = run_check(ChainCompleteness(), ctx, event)
    return r


def publish(spark, result, run_id: str, event: Event, kept_until: date | None = None) -> None:
    """Commit the chain's result and its key events as the runner does (ADR-07)."""
    from hcsc.datalake.dre.store.commits import Commit, append_commits
    from hcsc.datalake.dre.store.key_events import append_key_events
    from hcsc.datalake.dre.runner import covers
    from tests.store.conftest import append_rows

    ev, day = f"e-chain-{run_id}", event.window_end.date()
    append_rows(spark, CHAIN_DQ, "dq_check_result", [{
        "evaluation_id": ev, "run_id": run_id, "event_id": "x", "window_start": event.window_start,
        "window_end": event.window_end, "execution_type": "NORMAL", "dataset": RAW, "check_id": "CHAIN_COMPLETENESS",
        "state": result.state, "reason_code": result.reason_code, "group_values": {"chain": LABEL},
        "evaluated_at": event.window_end, "coverage_from": result.coverage[0] if covers(result) else None,
        "coverage_to": result.coverage[1] if covers(result) else None, "run_date": day}], commit=False)
    n = 0 if result.key_events is None else append_key_events(
        result.key_events, CHAIN_DQ, run_id, ev, RAW, "CHAIN_COMPLETENESS", LABEL, event.window_end, day)
    append_commits(spark, CHAIN_DQ, [Commit(run_id, ev, RAW, "CHAIN_COMPLETENESS", 1, key_event_rows=n,
                                            evidence_kept_until=kept_until)], event.window_end, day)


@pytest.fixture()
def carry_store(spark):
    spark.sql(f"DROP DATABASE IF EXISTS {CHAIN_DQ} CASCADE")
    create_store(spark, CHAIN_DQ)


def pending_window(spark, name: str, gold_holds_it: bool) -> dict[str, Dataset]:
    """123402 reached curated late (01-16 05:00), so it is pending at curated -> gold in EVENT; its
    raw load (01-15 06:00) puts its chain deadline inside EVENT's range only, never NEXT's."""
    rows = [through("123401"), through("123402", cur_at=LATE_CURATED, gold_at=LATE_CURATED)]
    return layers(spark, name, [r for r, _, _ in rows], [c for _, c, _ in rows],
                  [g for _, _, g in rows] if gold_holds_it else [rows[0][2]])


def test_a_record_pending_at_a_later_edge_is_judged_when_it_becomes_due(spark, carry_store) -> None:
    """P1-03: in EVENT the record is pending at curated -> gold and recorded PENDING; NEXT's range has
    moved past its chain deadline, yet NEXT judges it (it reached gold: reached) and records it
    RESOLVED; a third window carries nothing. It is counted once: pending in EVENT, due in NEXT."""
    first = chain_at(spark, pending_window(spark, "ch_carry", gold_holds_it=False), EVENT)
    assert (first.state, first.population) == ("PASSED", 1) and scope(first)["outcomes"]["pending_at_edges"] == {EDGE2: 1}
    assert [(e, d, v) for _, e, d, v in events_of(first)] == [("PENDING", f"pending at {EDGE2}", key_of("123402"))]
    publish(spark, first, "r1", EVENT)

    second = chain_at(spark, pending_window(spark, "ch_carry", gold_holds_it=True), NEXT)
    assert (second.state, second.population, second.violations) == ("PASSED", 1, 0)
    assert outcomes(second) == {"due": 1, "reached": 1, "accounted": 0, "lost": 0, "unobservable": 0, "pending": 0}
    assert scope(second)["carried_in"] == 1
    assert [(e, v) for _, e, _, v in events_of(second)] == [("RESOLVED", key_of("123402"))]
    publish(spark, second, "r2", NEXT)

    third = chain_at(spark, pending_window(spark, "ch_carry", gold_holds_it=True), AFTER)
    assert third.reason_code == "empty_population" and scope(third)["carried_in"] == 0  # never judged twice


def test_a_carried_record_that_never_arrives_is_lost_never_a_false_pass(spark, carry_store) -> None:
    """P1-03: the carried record is due at curated -> gold in NEXT (loaded 01-16 05:00 + 8h, before
    NEXT's cutoff) and gold still lacks it: lost, FAILED, first observed gap at that edge."""
    first = chain_at(spark, pending_window(spark, "ch_carry_lost", gold_holds_it=False), EVENT)
    publish(spark, first, "r1", EVENT)
    second = chain_at(spark, pending_window(spark, "ch_carry_lost", gold_holds_it=False), NEXT)
    assert (second.state, second.population, second.violations) == ("FAILED", 1, 1)
    assert scope(second)["first_observed_gap"]["edge"] == EDGE2
    assert [(e, v) for _, e, _, v in events_of(second)] == [("LOST", key_of("123402"))]


def test_without_key_events_a_pending_record_is_not_carried_and_the_scope_says_so(spark, carry_store) -> None:
    """P1-03, fail visible: with no HMAC secret nothing is recorded, so nothing can be carried; the
    scope names the gap instead of passing over it."""
    first = chain_at(spark, pending_window(spark, "ch_carry_nosecret", gold_holds_it=False), EVENT, secret=None)
    assert first.key_events is None
    assert any("not carried" in s for s in scope(first)["blind_spots"])


# --- final conditional approval, CA-02: a long-pending record is never silently forgotten ---
# The chain carries a record pending at a later edge through its PENDING key event, which every
# carrying evaluation that still finds it pending records again. The hourly read of those events looks
# back to history_since; when the chain's own coverage did not continue from inside that lookback (it
# was not evaluated for longer), the read reaches back to dq_key_event's retention instead (as the
# coverage read does, P1-02). FAR is 60 days after EVENT; its lookback (30 days) does not reach EVENT.

FAR = Event(RAW, datetime(2026, 3, 16, 12, tzinfo=UTC), datetime(2026, 3, 17, 12, tzinfo=UTC))
FAR_NEXT = Event(RAW, datetime(2026, 3, 17, 12, tzinfo=UTC), datetime(2026, 3, 18, 12, tzinfo=UTC))
FAR_LOOKBACK = date(2026, 2, 14)


def pending_published(spark, name: str, **kwargs) -> None:
    """EVENT: 123402 is pending at curated -> gold and recorded PENDING (run_date 2026-01-16)."""
    first = chain_at(spark, pending_window(spark, name, gold_holds_it=False), EVENT)
    assert [e for _, e, _, _ in events_of(first)] == ["PENDING"]
    publish(spark, first, "r-pending", EVENT, **kwargs)


def test_a_pending_record_younger_than_the_lookback_is_carried(spark, carry_store) -> None:
    """CA-02, 1: the PENDING event is inside the lookback: carried and judged (P1-03, with a lookback)."""
    pending_published(spark, "ch_young")
    r = chain_at(spark, pending_window(spark, "ch_young", gold_holds_it=True), NEXT, history_since=date(2026, 1, 1))
    assert scope(r)["carried_in"] == 1 and outcomes(r)["reached"] == 1


def test_a_pending_record_older_than_the_lookback_is_carried_and_reaches_gold(spark, carry_store) -> None:
    """CA-02, 2 and 3: the chain was not evaluated for 60 days, so the PENDING event is older than the
    lookback. The chain's coverage continues from before the lookback, so the carry read reaches back
    too: the record is judged (it reached gold), RESOLVED, and counted once; later windows carry nothing."""
    pending_published(spark, "ch_old_reached")
    datasets = pending_window(spark, "ch_old_reached", gold_holds_it=True)
    r = chain_at(spark, datasets, FAR, history_since=FAR_LOOKBACK)
    assert scope(r)["carried_in"] == 1
    assert (r.state, r.population, r.violations) == ("PASSED", 1, 0)
    assert outcomes(r) == {"due": 1, "reached": 1, "accounted": 0, "lost": 0, "unobservable": 0, "pending": 0}
    assert [(e, v) for _, e, _, v in events_of(r)] == [("RESOLVED", key_of("123402"))]
    publish(spark, r, "r-far", FAR)

    again = chain_at(spark, datasets, FAR_NEXT, history_since=FAR_LOOKBACK)
    assert again.reason_code == "empty_population" and scope(again)["carried_in"] == 0  # never judged twice


def test_a_long_pending_record_that_never_reaches_gold_is_lost_at_the_first_failing_edge(spark, carry_store) -> None:
    """CA-02, 4 and 6: gold never receives it. Once due at curated -> gold it is lost there: FAILED,
    the first observed gap is that edge (not claimed as the origin), one LOST event; repeated later
    evaluations neither carry it nor record a second terminal outcome."""
    pending_published(spark, "ch_old_lost")
    datasets = pending_window(spark, "ch_old_lost", gold_holds_it=False)
    r = chain_at(spark, datasets, FAR, history_since=FAR_LOOKBACK)
    assert (r.state, r.population, r.violations) == ("FAILED", 1, 1)
    gap = scope(r)["first_observed_gap"]
    assert gap["edge"] == EDGE2 and "not necessarily where the loss began" in gap["note"]
    assert [(e, v) for _, e, _, v in events_of(r)] == [("LOST", key_of("123402"))]
    publish(spark, r, "r-far", FAR)
    for i, event in enumerate((FAR_NEXT, Event(RAW, FAR_NEXT.window_end, FAR_NEXT.window_end.replace(day=19)))):
        later = chain_at(spark, datasets, event, history_since=FAR_LOOKBACK)
        assert later.reason_code == "empty_population" and scope(later)["carried_in"] == 0
        assert later.key_events is None  # nothing to record: no second LOST
        publish(spark, later, f"r-later-{i}", event)


def test_a_pending_record_whose_evidence_has_expired_is_still_judged_from_the_layers(spark, carry_store) -> None:
    """CA-02, 5: the PENDING event's commit is past evidence_kept_until (EXPIRED). It only names the
    key to judge again; the outcome comes from this evaluation's own reading of the layers."""
    pending_published(spark, "ch_expired", kept_until=date(2026, 1, 17))
    assert spark.sql(f"SELECT evidence_state FROM {CHAIN_DQ}.v_commit_state WHERE run_id = 'r-pending'"
                     ).collect()[0].evidence_state == "EXPIRED"
    r = chain_at(spark, pending_window(spark, "ch_expired", gold_holds_it=False), FAR, history_since=FAR_LOOKBACK)
    assert scope(r)["carried_in"] == 1 and (r.state, r.violations) == ("FAILED", 1)


def test_a_pending_record_past_dq_key_event_retention_is_not_carried_says_so_and_the_full_sweep_judges_it(
        spark, carry_store) -> None:
    """CA-02, 5: past dq_key_event's retention (key_events_since) the PENDING event is gone by the
    approved retention policy, so it cannot be carried; the scope names that horizon, and the full
    sweep, which reads the layers rather than the store, still judges the record."""
    pending_published(spark, "ch_retained")
    datasets = pending_window(spark, "ch_retained", gold_holds_it=False)
    r = chain_at(spark, datasets, FAR, history_since=FAR_LOOKBACK, key_events_since=date(2026, 2, 1))
    assert scope(r)["carried_in"] == 0
    assert any("2026-02-01" in s and "retention" in s for s in scope(r)["blind_spots"])
    sweep = chain_at(spark, datasets, FAR, history_since=FAR_LOOKBACK, key_events_since=date(2026, 2, 1),
                     full_sweep=True)
    assert (sweep.state, sweep.violations) == ("FAILED", 1) and scope(sweep)["first_observed_gap"]["edge"] == EDGE2


def test_a_pending_record_recorded_by_invalid_evidence_is_judged_again_not_trusted(spark, carry_store) -> None:
    """CA-02, 5: EVENT's evaluation is found INVALID. Its PENDING event is not trusted and its coverage
    covers nothing, so NEXT judges EVENT's range again from the layers: the record is found there."""
    from tests.store.conftest import append_rows

    pending_published(spark, "ch_invalid")
    append_rows(spark, CHAIN_DQ, "dq_integrity", [{
        "run_id": "sweep", "record": "EVIDENCE_MISMATCH", "subject_run_id": "r-pending",
        "subject_run_date": EVENT.window_end.date(), "evaluation_id": "e-chain-r-pending",
        "commit_kind": "result", "recorded_at": NEXT.window_start, "run_date": NEXT.window_start.date()}])
    r = chain_at(spark, pending_window(spark, "ch_invalid", gold_holds_it=True), NEXT, history_since=date(2026, 1, 1))
    assert scope(r)["carried_in"] == 0
    assert (r.state, r.population, r.violations) == ("PASSED", 2, 0)  # EVENT's two records, judged again
    assert outcomes(r)["reached"] == 2


def test_a_chain_without_retained_coverage_is_a_qualified_pass_until_the_full_sweep(spark, carry_store) -> None:
    """CA-01 for the chain: its range has the hop checks' rule. The store is older than the retention
    (a partition at or before coverage_since) and the chain has no coverage retained, so its PASS is
    qualified (coverage_gap), never healthy; the full sweep is not. In a store that has lost nothing,
    the same first evaluation is not."""
    from tests.store.conftest import append_rows

    datasets = build(spark, "ch_gap", reached=["123401"])
    young = chain_at(spark, datasets, EVENT, history_since=date(2026, 1, 1), coverage_since=date(2025, 7, 1))
    assert "coverage_gap" not in young.qualifications
    assert scope(young)["coverage_basis"].startswith("first evaluation")
    append_rows(spark, CHAIN_DQ, "dq_check_result", [{  # another check's result, before the retention
        "evaluation_id": "e-old", "run_id": "r-old", "event_id": "x", "window_start": EVENT.window_start,
        "window_end": EVENT.window_end, "execution_type": "NORMAL", "dataset": CUR, "check_id": "T1_ZERO_ROWS",
        "state": "PASSED", "evaluated_at": EVENT.window_end, "run_date": date(2025, 6, 1)}])
    first = chain_at(spark, datasets, EVENT, history_since=date(2026, 1, 1), coverage_since=date(2025, 7, 1))
    assert first.state == "PASSED" and "coverage_gap" in first.qualifications
    assert "coverage_gap" in scope(first)["qualifications"]
    assert scope(first)["coverage_basis"].startswith("unresolved coverage gap")
    sweep = chain_at(spark, datasets, EVENT, history_since=date(2026, 1, 1), coverage_since=date(2025, 7, 1),
                     full_sweep=True)
    assert sweep.state == "PASSED" and "coverage_gap" not in sweep.qualifications
