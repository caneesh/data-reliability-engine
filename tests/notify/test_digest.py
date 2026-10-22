"""The email digest (spec section 8): a golden-file test of the rendered text, built from a
synthetic dq store holding two earlier runs and the current one; plus the rules around it.

Set UPDATE_GOLDEN=1 to rewrite the golden files after an intended change (then review the diff).
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

from hcsc.datalake.dre.config.loader import load
from hcsc.datalake.dre.notify.digest import digests_for_run
from hcsc.datalake.dre.store.local_setup import create_store
from hcsc.datalake.dre.store.runs import Run
from tests.conftest import REPO_ROOT
from tests.store.conftest import append_rows

UTC = timezone.utc
DQ = "dq_digest"
GOLDEN = Path(__file__).parent / "golden"
OLDER, PREVIOUS, CURRENT = (f"00000000-0000-0000-0000-00000000000{i}" for i in (1, 2, 3))
AT = {OLDER: datetime(2026, 1, 14, 12, 5), PREVIOUS: datetime(2026, 1, 15, 12, 5), CURRENT: datetime(2026, 1, 16, 12, 5)}
SRC_A = {"src_sys_nm": "SRC_A"}
UPSTREAM = {"upstream": "example_curated_enrollment"}
KEY_VALUE = "123401|01|2026-01-01|AGR-A"  # synthetic; must never reach a digest
KEY_HASH = "a" * 64
SLOTS = ["2026-01-16T06:30:00+00:00", "2026-01-16T10:00:00+00:00"]
LEDGER_SCOPE = json.dumps({  # HOP_LEDGER's detail: the result's scope (build step 3), shortened
    "edge": "example_curated_enrollment->gold_member_coverage", "mapping": "union_partition", "contract": "declared",
    "claim": "declared contract, qualified: attribution_unverified", "evidence_level": "observed",
    "cutoff": "2026-01-16T12:05:00+00:00", "full_sweep": False,
    "values": {"verified": [{"upstream": "enddate", "here": "mbr_mbrshp_covrg_end_dt"}],
               "unverified": ["covrg_agrmt_desc", "src_sys_nm"], "coverage": "partial"},
    "blind_spots": ["attribution (each row here owned by exactly one source) is not proven until build step 5",
                    "as-of replay unavailable: gold_member_coverage is mutable"],
    "triage": ["1 stale: HOP_KEY_CURRENCY and HOP_VALUE_AGREEMENT name these keys and their causes"]})
PROVIDER_SCOPE = json.dumps({
    "edge": "provider_roster_raw->provider_directory", "mapping": "dedup", "contract": "inferred from key_map",
    "claim": "inferred contract", "evidence_level": "observed", "cutoff": "2026-01-16T12:05:00+00:00",
    "full_sweep": False, "values": {"verified": [{"upstream": "specialty", "here": "specialty"}], "unverified": [],
                                    "coverage": "complete"},
    "blind_spots": ["kind dedup is inferred from key_map, not declared: its results can never claim "
                    "contract-proven completeness"]})


def result(run: str, dataset: str, check: str, state: str, groups: dict | None = None, *, feed="example_realtime",
           violations: int | None = None, observed: str | None = None, expected: str | None = None,
           reason: tuple[str, str] | None = None, detail: str | None = None, qualifications: str | None = None) -> dict:
    group_text = ",".join(f"{k}={v}" for k, v in sorted((groups or {}).items()))
    return {"evaluation_id": f"{run[-1]}-{dataset}-{check}-{group_text}", "run_id": run, "event_id": f"e-{run[-1]}",
            "window_start": AT[run], "window_end": AT[run], "execution_type": "NORMAL", "feed": feed,
            "dataset": dataset, "check_id": check, "expectation_version": 1, "state": state,
            "reason_category": reason[0] if reason else None, "reason_code": reason[1] if reason else None,
            "population": None if state == "DID_NOT_RUN" else 10, "violations": violations,
            "observed": observed, "expected": expected, "group_values": groups or {},
            "evaluated_at": AT[run], "detail": detail, "qualifications": qualifications, "run_date": AT[run].date()}


def cause(evaluation: str, ref: str, order: int, code: str, state: str) -> dict:
    return {"run_id": CURRENT, "evaluation_id": evaluation, "failure_ref": ref, "hop": "h", "cause_check_id": "builtin",
            "order_no": order, "state": state, "cause_code": code, "evidence": "{}", "evaluated_at": AT[CURRENT],
            "run_date": AT[CURRENT].date()}


@pytest.fixture(scope="module")
def config(tmp_path_factory):
    conf = tmp_path_factory.mktemp("digest") / "conf"
    shutil.copytree(REPO_ROOT / "conf", conf)
    rule = conf / "rules" / "one_row_per_coverage.yaml"
    rule.write_text(rule.read_text(encoding="utf-8").replace("status: proposed", "status: approved"), encoding="utf-8")
    feed = conf / "feeds" / "example_realtime.yaml"
    feed.write_text(feed.read_text(encoding="utf-8").replace("email_sample_keys: false", "email_sample_keys: true"),
                    encoding="utf-8")
    loaded, errors = load(conf)
    assert not [e for e in errors if e.level == "error"]
    return loaded


@pytest.fixture(scope="module")
def digests(spark, config):
    create_store(spark, DQ)
    gold, cur = "gold_member_coverage", "example_curated_enrollment"
    keys_obs = ("1 of 3 keys have more than one row", "every key at most once")
    results = [
        # two runs ago
        result(OLDER, gold, "T1_KEY_NULLS", "FAILED", SRC_A, violations=2),
        result(OLDER, gold, "one_row_per_coverage", "PASSED", SRC_A, violations=0),
        # the previous run
        result(PREVIOUS, gold, "T1_KEY_DUPLICATES", "PASSED", SRC_A, violations=0),
        result(PREVIOUS, gold, "T1_KEY_DUPLICATES", "PASSED", {"src_sys_nm": "SRC_B"}, violations=0),  # gone now
        result(PREVIOUS, gold, "T1_KEY_NULLS", "FAILED", SRC_A, violations=2),
        result(PREVIOUS, gold, "T1_ON_TIME", "PASSED"),
        result(PREVIOUS, gold, "T1_ZERO_ROWS", "PASSED"),
        result(PREVIOUS, gold, "HOP_KEY_CURRENCY", "PASSED", UPSTREAM, violations=0),
        result(PREVIOUS, gold, "T1_VOLUME", "PASSED", {"slot": "00:30", "slot_time": "2026-01-15 00:30"}),
        result(PREVIOUS, gold, "T1_VOLUME", "PASSED", {"slot": "04:00", "slot_time": "2026-01-15 04:00"}),
        result(PREVIOUS, gold, "one_row_per_coverage", "FAILED", SRC_A, violations=1),
        result(PREVIOUS, gold, "one_open_row_per_coverage", "PASSED", SRC_A, violations=0),
        result(PREVIOUS, cur, "T1_KEY_DUPLICATES", "FAILED", violations=4),
        # this run
        result(CURRENT, gold, "T1_KEY_DUPLICATES", "FAILED", SRC_A, violations=1, observed=keys_obs[0],
               expected=keys_obs[1]),
        result(CURRENT, gold, "T1_KEY_NULLS", "FAILED", SRC_A, violations=3),
        result(CURRENT, gold, "T1_ON_TIME", "FAILED", violations=2, observed="2 of 2 due loads had no load within the SLA",
               expected="a load within 8 hours of each cadence slot", detail=json.dumps({"slots": SLOTS})),
        result(CURRENT, gold, "T1_ZERO_ROWS", "FAILED", violations=2,
               observed="2 of 2 due loads wrote fewer rows than the minimum", expected="at least 1 rows per load",
               detail=json.dumps({"slots": SLOTS})),
        result(CURRENT, gold, "HOP_KEY_CURRENCY", "FAILED", UPSTREAM, violations=1,
               observed="0 missing and 1 stale of 1 keys", expected="every upstream key present at its latest version"),
        result(CURRENT, gold, "HOP_LEDGER", "FAILED", UPSTREAM, violations=1,
               observed="due 3 (pending 0): delivered 2, stale 1; orphans 0; fails: 1 stale",
               expected="every due record in exactly one authorised disposition", detail=LEDGER_SCOPE,
               qualifications="attribution_unverified"),
        result(CURRENT, gold, "T1_VOLUME", "PASSED", {"slot": "00:30", "slot_time": "2026-01-16 00:30"}),
        result(CURRENT, gold, "WINDOW_GAP", "DID_NOT_RUN", reason=("DATA_UNAVAILABLE", "window_gap")),
        result(CURRENT, gold, "one_row_per_coverage", "FAILED", SRC_A, violations=3),
        result(CURRENT, gold, "one_open_row_per_coverage", "FAILED", SRC_A, violations=2),
        result(CURRENT, gold, "end_not_before_start", "PASSED", SRC_A, violations=0),
        result(CURRENT, cur, "T1_KEY_DUPLICATES", "PASSED", violations=0),
        result(CURRENT, cur, "T1_ON_TIME", "DID_NOT_RUN", reason=("CONFIGURATION", "invalid_config")),
        result(CURRENT, cur, "HOP_FILE_COMPLETENESS", "FAILED", {"upstream": "example_raw_enrollment"}, violations=1,
               observed="1 of 4 upstream files have fewer rows here", expected="every upstream row of each file present here"),
        result(CURRENT, "provider_directory", "T1_KEY_NULLS", "PASSED", feed="provider_directory_merge", violations=0),
        result(CURRENT, "provider_directory", "HOP_LEDGER", "PASSED", {"upstream": "provider_roster_raw"},
               feed="provider_directory_merge", violations=0, observed="due 3 (pending 0): delivered 3",
               detail=PROVIDER_SCOPE, qualifications="inferred_contract"),
    ]
    on_time = f"3-{gold}-T1_ON_TIME-"
    currency = f"3-{gold}-HOP_KEY_CURRENCY-upstream=example_curated_enrollment"
    # The hop's result is committed with its key events below: one commit vouches for both, as a run writes it.
    append_rows(spark, DQ, "dq_check_result", [r for r in results if r["evaluation_id"] != currency])
    append_rows(spark, DQ, "dq_check_result", [r for r in results if r["evaluation_id"] == currency], commit=False)
    files = f"3-{cur}-HOP_FILE_COMPLETENESS-upstream=example_raw_enrollment"
    append_rows(spark, DQ, "dq_cause_result", [
        *[cause(on_time, slot, 2, "PIPELINE_STALLED", "CONFIRMED") for slot in SLOTS],
        cause(currency, KEY_HASH, 1, "NOT_RUN", "CONFIRMED"),
        cause(files, "rt_0001.seq", 1, "NOT_RUN", "RULED_OUT"), cause(files, "rt_0001.seq", 2, "FILE_SKIPPED", "RULED_OUT"),
        cause(files, "rt_0001.seq", 3, "INVALID_KEY", "RULED_OUT"), cause(files, "rt_0001.seq", 4, "FILTERED", "NOT_READY"),
        cause(files, "rt_0001.seq", 5, "REJECTED", "NOT_READY"),
    ])
    append_rows(spark, DQ, "dq_key_event", [{
        "run_id": CURRENT, "evaluation_id": currency, "dataset": gold, "check_id": "HOP_KEY_CURRENCY",
        "key_hash": KEY_HASH, "key_value": KEY_VALUE, "event": "FLAGGED", "state_detail": "STALE",
        "observed_at": AT[CURRENT], "run_date": AT[CURRENT].date()}])
    dq_config = config.model_copy() if hasattr(config, "model_copy") else config
    dq_config.defaults = config.defaults.model_copy(update={"dq_database": DQ})
    return {d.owner: d for d in digests_for_run(spark, dq_config, Run(CURRENT, AT[CURRENT].replace(tzinfo=UTC),
                                                                       "0.1.0", None))}


@pytest.mark.parametrize("owner", ["membership-gold", "provider-data"])
def test_digest_matches_the_golden_file(digests, owner: str) -> None:
    text = digests[owner].text()
    golden = GOLDEN / f"{owner}.txt"
    if os.environ.get("UPDATE_GOLDEN"):
        golden.write_text(text, encoding="utf-8")
    assert text == golden.read_text(encoding="utf-8")


def test_subject_counts_new_failures_and_checks_that_did_not_run(digests) -> None:
    # New failures: KEY_DUPLICATES, ON_TIME (ZERO_ROWS shown with it), HOP_KEY_CURRENCY, HOP_LEDGER,
    # HOP_FILE_COMPLETENESS.
    assert digests["membership-gold"].subject == "DRE dev: 5 new failures, 2 checks did not run"
    # provider-data's only failure-free result set includes a qualified HOP_LEDGER pass: not all clear (P1-01)
    assert digests["provider-data"].subject == "DRE dev: 0 new failures, 0 checks did not run, 1 passed with qualifications"
    assert digests["membership-gold"].recipients == ["dre-alerts@example.com"]


def test_the_digest_never_carries_a_key_value(digests) -> None:
    """Spec section 9, guard rule 7, on the rendered text: key_hash only because the feed sets
    email_sample_keys; key_value never, nor any part of it."""
    text = digests["membership-gold"].text()
    assert KEY_HASH in text
    for part in (KEY_VALUE, "123401", "AGR-A"):
        assert part not in text


def test_proposed_rules_are_report_only_and_never_counted(digests) -> None:
    d = digests["membership-gold"]
    assert {i.check_id for i in d.report_only} == {"one_open_row_per_coverage", "end_not_before_start"}
    assert "one_open_row_per_coverage" not in {i.check_id for i in d.changed + d.still_failing + d.did_not_run}


def test_key_samples_are_bounded_in_sql_and_read_only_when_wanted(spark, config) -> None:
    """At most SAMPLE_KEYS hashes per evaluation reach the driver, and none at all unless some
    dataset sets email_sample_keys (the config fixture's feed does)."""
    from hcsc.datalake.dre.notify.digest import SAMPLE_KEYS, read_run

    run_id = "00000000-0000-0000-0000-000000000009"
    at = datetime(2026, 1, 17, 12, 5)
    create_store(spark, DQ)
    append_rows(spark, DQ, "dq_check_result", [{**result(CURRENT, "gold_member_coverage", "HOP_KEY_CURRENCY", "FAILED",
                                                           UPSTREAM, violations=20), "run_id": run_id,
                                                 "evaluation_id": "many-keys", "evaluated_at": at, "run_date": at.date()}],
                commit=False)  # committed with its key events
    append_rows(spark, DQ, "dq_key_event", [
        {"run_id": run_id, "evaluation_id": "many-keys", "dataset": "gold_member_coverage", "check_id": "HOP_KEY_CURRENCY",
         "hop": "h", "key_hash": f"{n:064x}", "key_value": KEY_VALUE, "event": "FLAGGED", "state_detail": "MISSING",
         "observed_at": at, "run_date": at.date()} for n in range(20)])
    data = read_run(spark, DQ, run_id, at.date(), with_samples=True)
    assert data["samples"] == {"many-keys": [f"{n:064x}" for n in range(SAMPLE_KEYS)]}
    assert read_run(spark, DQ, run_id, at.date(), with_samples=False)["samples"] == {}


def test_a_reconciliation_result_prints_its_scope_triage_and_blind_spots(digests) -> None:
    """Model section 1 and checkpoint 3 (C3-03, C3-04, L13): the digest prints the scope beside the state,
    with how many columns the values were verified for, the blind spots and, for a FAILED ledger,
    its triage lines. Results whose detail is not a scope get none."""
    from hcsc.datalake.dre.notify.digest import scope_line

    [ledger] = [i for i in digests["membership-gold"].changed if i.check_id == "HOP_LEDGER"]
    assert ledger.scope == ("scope: example_curated_enrollment->gold_member_coverage union_partition (declared); "
                            "claim declared contract, qualified: attribution_unverified; evidence observed; "
                            "values verified for 1 of 3 columns; cutoff 2026-01-16T12:05:00+00:00")
    text = digests["membership-gold"].text()
    assert ledger.scope in text
    assert "    triage: 1 stale: HOP_KEY_CURRENCY and HOP_VALUE_AGREEMENT name these keys and their causes" in text
    assert "    blind spots: attribution (each row here owned by exactly one source) is not proven" in text
    assert scope_line(json.dumps({"slots": SLOTS})) is None and scope_line("not json") is None and scope_line(None) is None


def test_a_qualified_pass_is_listed_and_never_counted_as_healthy(digests) -> None:
    """C3-03, C3-04: a PASS that rests on something unproven (here an inferred contract) is listed under
    "Passed with qualifications" with its qualifications and blind spots, and is not in the passing count."""
    d = digests["provider-data"]
    [qualified] = d.qualified
    assert (qualified.check_id, qualified.qualifications) == ("HOP_LEDGER", ["inferred_contract"])
    assert d.passing == 1  # T1_KEY_NULLS only
    text = d.text()
    assert "Passed with qualifications (not counted as healthy)\n- provider_directory HOP_LEDGER " \
           "[upstream=provider_roster_raw]: inferred_contract" in text
    assert "blind spots: kind dedup is inferred from key_map" in text


# --- final review P1-01: an all-clear digest means every result is healthy ---

def digest_of(*, qualified: int = 0, failed: int = 0, passing: int = 1):
    from hcsc.datalake.dre.notify.digest import Digest, Item

    d = Digest("membership-gold", ["dre-alerts@example.com"], "dev", "run-1", datetime(2026, 1, 16, 12, 5, tzinfo=UTC), passing=passing)
    d.qualified = [Item("gold_member_coverage", "HOP_LEDGER", f"upstream=up{n}", "PASSED",
                        qualifications=["attribution_unverified"]) for n in range(qualified)]
    d.changed = [Item("gold_member_coverage", "T1_ZERO_ROWS", f"slot={n}", "FAILED", prev_state="PASSED")
                 for n in range(failed)]
    return d


@pytest.mark.parametrize("mode", ["every_run", "daily", "never"])
def test_a_digest_whose_passes_are_all_qualified_is_never_all_clear(config, mode: str) -> None:
    """P1-01, qualified only: nothing failed and nothing was skipped, but the only pass rests on an
    unproven premise. The subject says so, and the digest is sent whatever all_clear_digest says,
    because it is not all clear."""
    from hcsc.datalake.dre.notify.digest import to_send

    d = digest_of(qualified=1, passing=0)
    assert not d.all_clear
    assert d.subject == "DRE dev: 0 new failures, 0 checks did not run, 1 passed with qualifications"
    assert "all clear" not in d.text()
    defaults = config.defaults.model_copy(update={"email": config.defaults.email.model_copy(
        update={"all_clear_digest": mode})})
    assert to_send([d], defaults, datetime(2026, 1, 16, 12, 5, tzinfo=UTC)) == [d]


def test_a_digest_with_failures_and_qualified_passes_counts_both() -> None:
    """P1-01, mixed: failures and qualified passes are both in the subject; neither hides the other."""
    d = digest_of(qualified=2, failed=1)
    assert not d.all_clear
    assert d.subject == "DRE dev: 1 new failures, 0 checks did not run, 2 passed with qualifications"


@pytest.mark.parametrize("mode", ["every_run", "daily", "never"])
def test_only_a_fully_healthy_digest_is_all_clear(config, mode: str) -> None:
    """P1-01, fully healthy: no failure, nothing skipped, no unhealthy evidence, no qualified pass.
    Only this digest is all clear, and only this one is subject to all_clear_digest."""
    from hcsc.datalake.dre.notify.digest import to_send

    d = digest_of(passing=3)
    assert d.all_clear and d.subject == "DRE dev: all clear"
    defaults = config.defaults.model_copy(update={"email": config.defaults.email.model_copy(
        update={"all_clear_digest": mode})})
    sent = to_send([d], defaults, datetime(2026, 1, 16, 3, 0, tzinfo=timezone.utc))  # not the daily hour
    assert sent == ([d] if mode == "every_run" else [])


def test_a_coverage_gap_is_reported_in_the_digest_and_is_never_all_clear() -> None:
    """KI-18 (ratified) and CA-01: a PASS qualified coverage_gap, the only result of the run, is listed
    under "Passed with qualifications" by name and counted in the subject; the digest is not all clear."""
    from hcsc.datalake.dre.notify.digest import Item

    d = digest_of(passing=0)
    d.qualified = [Item("gold_member_coverage", "HOP_LEDGER", "upstream=up0", "PASSED", qualifications=["coverage_gap"])]
    assert not d.all_clear
    assert d.subject == "DRE dev: 0 new failures, 0 checks did not run, 1 passed with qualifications"
    assert ("Passed with qualifications (not counted as healthy)\n- gold_member_coverage HOP_LEDGER "
            "[upstream=up0]: coverage_gap") in d.text()
