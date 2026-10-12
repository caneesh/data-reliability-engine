"""Derived datasets (spec section 3, release 2): an aggregate or joined upstream.

A synthetic claims pipeline: curated claims (one row per claim) feed a gold table with one row
per (member, service month) holding the claim count and the paid total. The derived dataset
`claims_by_member_month` is that grain, so the ordinary hop checks judge the gold table: a
month MISSING, a measure that disagrees, a month still receiving claims (pending), and, with a
joined members dataset, a member the lookup could not match (JOIN_UNMATCHED).
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from hcsc.datalake.dre.causes import merge
from hcsc.datalake.dre.causes.base import CauseContext
from hcsc.datalake.dre.causes.failures import failures_for
from hcsc.datalake.dre.checks.base import CheckContext, CheckResult, run_check
from hcsc.datalake.dre.checks.derived import relation
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.hop.key_currency import KeyCurrency
from hcsc.datalake.dre.checks.hop.value_agreement import ValueAgreement
from hcsc.datalake.dre.config.models import Dataset, TimeColumn
from hcsc.datalake.dre.store.local_setup import create_store
from tests.fixtures.layers import create_table
from tests.fixtures.settings import SETTINGS

UTC = timezone.utc
DQ = "dq_derived"
EVENT = Event("gold", datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 1, 16, 12, tzinfo=UTC))
IN_WINDOW = datetime(2026, 1, 15, 10, 0)  # loaded upstream inside the judged range (sla 8h)
LATE = datetime(2026, 1, 16, 9, 0)        # loaded upstream after the judged range: pending
SECRET = b"synthetic-derived-secret"

CLAIM_COLUMNS = [("memberid", "STRING"), ("servicedate", "STRING"), ("paidamount", "DECIMAL(18,2)"),
                 ("status", "STRING"), ("sourcelastupdatets", "TIMESTAMP")]
MEMBER_COLUMNS = [("memberid", "STRING"), ("plan", "STRING")]
GOLD_COLUMNS = [("mbr_id", "STRING"), ("svc_month", "STRING"), ("clm_cnt", "BIGINT"), ("paid_amt", "DECIMAL(18,2)"),
                ("plan_cd", "STRING"), ("src_lcts", "TIMESTAMP"), ("gld_lcts", "TIMESTAMP")]
TS = TimeColumn(column="sourcelastupdatets", granularity="minute", timezone="UTC")


@pytest.fixture(scope="module", autouse=True)
def store(spark):
    create_store(spark, DQ)


def claim(member: str, date: str, paid: str, at: datetime = IN_WINDOW, status: str = "PAID") -> dict:
    return {"memberid": member, "servicedate": date, "paidamount": Decimal(paid), "status": status,
            "sourcelastupdatets": at}


def gold_month(member: str, month: str, count: int, paid: str, plan: str | None = "GOLD",
               record: datetime = IN_WINDOW) -> dict:
    return {"mbr_id": member, "svc_month": month, "clm_cnt": count, "paid_amt": Decimal(paid), "plan_cd": plan,
            "src_lcts": record, "gld_lcts": datetime(2026, 1, 15, 11, 0)}


def pipeline(spark, name: str, claims: list[dict], gold: list[dict], members: list[dict] | None = None,
             join_type: str = "left") -> tuple[dict[str, Dataset], Dataset]:
    """datasets (claims, members if any, the derived one) and the gold dataset that compares against it."""
    create_table(spark, f"derived.{name}_claims", CLAIM_COLUMNS, claims)
    create_table(spark, f"derived.{name}_gold", GOLD_COLUMNS, gold)
    claims_ds = Dataset(dataset="curated_claims", table=f"derived.{name}_claims", layer="CURATED",
                        key=["memberid", "servicedate"], record_time=TS, load_time=TS,
                        feed_filter="status <> 'VOID'")
    datasets = {"curated_claims": claims_ds}
    joins, columns = [], {"claim_count": "count(*)", "paid_total": "sum(curated_claims.paidamount)"}
    if members is not None:
        create_table(spark, f"derived.{name}_members", MEMBER_COLUMNS, members)
        datasets["curated_members"] = Dataset(dataset="curated_members", table=f"derived.{name}_members",
                                              layer="CURATED", key=["memberid"])
        joins = [{"dataset": "curated_members", "match": {"memberid": "memberid"}, "type": join_type}]
        columns["plan"] = "max(curated_members.plan)"
    datasets["claims_by_member_month"] = Dataset(
        dataset="claims_by_member_month", layer="CURATED", key=["memberid", "service_month"],
        derived={"from": "curated_claims", "joins": joins,
                 "group_by": {"memberid": "curated_claims.memberid",
                              "service_month": "substr(curated_claims.servicedate, 1, 7)"},
                 "columns": columns})
    owned = {"claim_count": "clm_cnt", "paid_total": "paid_amt", **({"plan": "plan_cd"} if members else {})}
    gold_ds = Dataset(dataset=f"gold_{name}", table=f"derived.{name}_gold", layer="GOLD",
                      upstream=["claims_by_member_month"], key=["mbr_id", "svc_month"],
                      record_time=TimeColumn(column="src_lcts", timezone="UTC"),
                      load_time=TimeColumn(column="gld_lcts", granularity="minute", timezone="UTC"),
                      key_map={"claims_by_member_month": {"mbr_id": "memberid", "svc_month": "service_month"}},
                      owned_columns=owned)
    return datasets, gold_ds


def run(spark, check, gold: Dataset, datasets: dict[str, Dataset], secret: bytes | None = None,
        event: Event = EVENT) -> CheckResult:
    ctx = CheckContext(spark, gold, None, SETTINGS, "UTC", DQ, upstreams=datasets, key_secret=secret)
    [result], _ = run_check(check, ctx, event)
    assert result.group_values == {"upstream": "claims_by_member_month"}
    return result


def test_the_relation_is_the_declared_grain(spark) -> None:
    datasets, _ = pipeline(spark, "rel", [claim("m1", "2026-01-03", "10.00"), claim("m1", "2026-01-20", "5.50"),
                                          claim("m1", "2026-01-21", "1.00", status="VOID"),  # feed_filter
                                          claim("m2", "2026-01-04", "7.25")], [])
    rows = spark.sql(f"SELECT * FROM {relation(datasets['claims_by_member_month'], datasets, 'UTC')} "
                     "ORDER BY memberid").collect()
    assert [(r.memberid, r.service_month, r.claim_count, str(r.paid_total), list(r.dre_unmatched)) for r in rows] == [
        ("m1", "2026-01", 2, "15.50", []), ("m2", "2026-01", 1, "7.25", [])]
    assert rows[0].record_ts == IN_WINDOW and rows[0].load_ts == IN_WINDOW


def test_an_aggregate_upstream_is_judged_at_its_grain(spark) -> None:
    datasets, gold = pipeline(spark, "agg", [
        claim("m1", "2026-01-03", "10.00"), claim("m1", "2026-01-20", "5.50"),   # m1: 2 claims, 15.50
        claim("m2", "2026-01-04", "7.25"),                                       # m2: missing in gold
        claim("m3", "2026-01-05", "3.00"),                                       # m3: paid total disagrees
        claim("m4", "2026-01-06", "2.00", at=LATE),                              # m4: still arriving
    ], [gold_month("m1", "2026-01", 2, "15.50"), gold_month("m3", "2026-01", 1, "30.00")])
    currency = run(spark, KeyCurrency(), gold, datasets)
    assert (currency.state, currency.population, currency.violations) == ("FAILED", 3, 1)
    assert currency.observed == "1 missing and 0 stale of 3 keys; 1 key pending (within sla_hours)"
    agreement = run(spark, ValueAgreement(), gold, datasets)
    assert (agreement.state, agreement.population, agreement.violations) == ("FAILED", 2, 1)
    assert agreement.observed.startswith("1 of 2 current keys disagree on claim_count->clm_cnt, paid_total->paid_amt")


def test_an_aggregate_upstream_passes_when_gold_agrees(spark) -> None:
    datasets, gold = pipeline(spark, "agg_ok", [claim("m1", "2026-01-03", "10.00"), claim("m1", "2026-01-20", "5.50")],
                              [gold_month("m1", "2026-01", 2, "15.50")])
    assert run(spark, KeyCurrency(), gold, datasets).state == "PASSED"
    assert run(spark, ValueAgreement(), gold, datasets).state == "PASSED"
    nothing_due = Event("gold", datetime(2026, 1, 10, tzinfo=UTC), datetime(2026, 1, 11, tzinfo=UTC))
    assert run(spark, KeyCurrency(), gold, datasets, event=nothing_due).reason_code == "empty_population"


def test_a_group_still_receiving_rows_is_pending_until_it_settles(spark) -> None:
    """The group's load time is its latest row's, so a month with a claim younger than sla_hours is
    not judged yet, even though its earlier claims are past their deadline."""
    datasets, gold = pipeline(spark, "settle",
                              [claim("m1", "2026-01-03", "10.00"), claim("m1", "2026-01-20", "5.50", at=LATE)],
                              [gold_month("m1", "2026-01", 1, "10.00")])  # gold has the first claim only: fine, for now
    result = run(spark, KeyCurrency(), gold, datasets)
    assert (result.state, result.reason_code) == ("DID_NOT_RUN", "empty_population")
    assert result.detail == "no key judged; 1 key pending (within sla_hours)"


def _missing_key_failure(spark, gold: Dataset, datasets: dict[str, Dataset]) -> tuple[CauseContext, object]:
    ctx = CheckContext(spark, gold, None, SETTINGS, "UTC", DQ, upstreams=datasets, key_secret=SECRET)
    from types import SimpleNamespace

    cx = CauseContext(ctx, EVENT, SimpleNamespace(datasets=datasets), "HOP_KEY_CURRENCY", "claims_by_member_month")
    failures = failures_for(cx, CheckResult("FAILED", population=1, violations=1))
    [failure] = [f for f in failures if f.key["key_state"] == "MISSING"]
    return cx, failure


def test_a_left_join_that_did_not_match_explains_a_missing_key(spark) -> None:
    datasets, gold = pipeline(spark, "left", [claim("m1", "2026-01-03", "10.00"), claim("m9", "2026-01-04", "1.00")],
                              [gold_month("m1", "2026-01", 1, "10.00")], members=[{"memberid": "m1", "plan": "GOLD"}])
    currency = run(spark, KeyCurrency(), gold, datasets, secret=SECRET)
    assert (currency.state, currency.population, currency.violations) == ("FAILED", 2, 1)  # m9 expected, missing
    cx, failure = _missing_key_failure(spark, gold, datasets)
    assert failure.ref == hmac.new(SECRET, b"m9|2026-01", hashlib.sha256).hexdigest()
    assert merge.join_unmatched(cx, failure).evidence == {"unmatched": ["curated_members"]}
    assert run(spark, ValueAgreement(), gold, datasets).state == "PASSED"  # m1's plan came through the join


def test_an_inner_join_makes_unmatched_rows_not_expected(spark) -> None:
    datasets, gold = pipeline(spark, "inner", [claim("m1", "2026-01-03", "10.00"), claim("m9", "2026-01-04", "1.00")],
                              [gold_month("m1", "2026-01", 1, "10.00")], members=[{"memberid": "m1", "plan": "GOLD"}],
                              join_type="inner")
    currency = run(spark, KeyCurrency(), gold, datasets)
    assert (currency.state, currency.population, currency.violations) == ("PASSED", 1, 0)


def test_join_unmatched_is_not_ready_without_a_derived_upstream_with_joins(spark) -> None:
    datasets, gold = pipeline(spark, "nojoin", [claim("m2", "2026-01-04", "7.25")], [])
    cx, failure = _missing_key_failure(spark, gold, datasets)
    assert merge.join_unmatched(cx, failure).state == "NOT_READY"
