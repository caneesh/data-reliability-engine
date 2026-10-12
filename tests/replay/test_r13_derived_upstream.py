"""R13: a gold table at a coarser grain than its upstream, through `dre run` and `dre trace`.

Curated claims (one row per claim) and members feed gold claims-by-member-month. The derived
dataset `claims_by_member_month` declares the grain. In one run: m1 agrees; m2 has no member
row, so the gold load dropped it (MISSING, cause JOIN_UNMATCHED); m3's claim count disagrees.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from hcsc.datalake.dre.cli import main
from tests.checks.test_derived import CLAIM_COLUMNS, GOLD_COLUMNS, MEMBER_COLUMNS
from tests.fixtures.layers import create_table
from tests.replay.conftest import causes, latest, replay_conf

UTC = timezone.utc
FEED = """feed: claims_monthly
expectation_version: 1
pattern: TABLE_MERGE
owner: membership-gold
cadence: { kind: times, times: ["01:00"], timezone: UTC, calendar: EVERYDAY }
datasets: [curated_claims, curated_members, gold_claims_by_month]
"""
CLAIMS = """dataset: curated_claims
table: {claims}
layer: CURATED
key: [memberid, servicedate]
record_time: {{ column: sourcelastupdatets, timezone: UTC }}
load_time: {{ column: sourcelastupdatets, granularity: minute, timezone: UTC }}
feed_filter: "status <> 'VOID'"
"""
MEMBERS = """dataset: curated_members
table: {members}
layer: CURATED
key: [memberid]
"""
BY_MONTH = """dataset: claims_by_member_month
layer: CURATED
key: [memberid, service_month]
derived:
  from: curated_claims
  joins:
    - {{ dataset: curated_members, match: {{ memberid: memberid }}, type: left }}
  group_by:
    memberid: curated_claims.memberid
    service_month: "substr(curated_claims.servicedate, 1, 7)"
  columns:
    claim_count: "count(*)"
    paid_total: "sum(curated_claims.paidamount)"
    plan: "max(curated_members.plan)"
"""
GOLD = """dataset: gold_claims_by_month
table: {gold}
layer: GOLD
upstream: [claims_by_member_month]
key: [mbr_id, svc_month]
record_time: {{ column: src_lcts, timezone: UTC }}
load_time: {{ column: gld_lcts, granularity: minute, timezone: UTC }}
key_map:
  claims_by_member_month: {{ mbr_id: memberid, svc_month: service_month }}
owned_columns: {{ claim_count: clm_cnt, paid_total: paid_amt, plan: plan_cd }}
"""


def test_r13_an_aggregate_gold_is_judged_at_its_declared_grain(spark, tmp_path, capsys) -> None:
    secret = tmp_path / "hmac.secret"
    secret.write_bytes(b"synthetic-r13-secret\n")
    replay = replay_conf(spark, tmp_path, "r13", {"defaults.yaml": [("hmac_secret_file: null",
                                                                     f"hmac_secret_file: {secret}")]})
    tables = {"claims": "r13_curated.claims", "members": "r13_curated.members", "gold": "r13_gold.claims_by_month"}
    (replay.conf / "feeds" / "claims_monthly.yaml").write_text(FEED, encoding="utf-8")
    for name, text in (("curated_claims", CLAIMS), ("curated_members", MEMBERS),
                       ("claims_by_member_month", BY_MONTH), ("gold_claims_by_month", GOLD)):
        (replay.conf / "datasets" / f"{name}.yaml").write_text(text.format(**tables), encoding="utf-8")

    loaded = (datetime.now(UTC) - timedelta(hours=12)).replace(second=0, microsecond=0, tzinfo=None)
    gold_loaded = loaded + timedelta(hours=2)
    claim = lambda m, d, paid: {"memberid": m, "servicedate": d, "paidamount": Decimal(paid), "status": "PAID",  # noqa: E731
                                "sourcelastupdatets": loaded}
    create_table(spark, tables["claims"], CLAIM_COLUMNS, [
        claim("m1", "2026-01-03", "10.00"), claim("m1", "2026-01-20", "5.50"),
        claim("m2", "2026-01-04", "7.25"), claim("m3", "2026-01-05", "3.00"), claim("m3", "2026-01-06", "4.00")])
    create_table(spark, tables["members"], MEMBER_COLUMNS, [{"memberid": "m1", "plan": "GOLD"},
                                                             {"memberid": "m3", "plan": "SILVER"}])
    gold = lambda m, n, paid, plan: {"mbr_id": m, "svc_month": "2026-01", "clm_cnt": n, "paid_amt": Decimal(paid),  # noqa: E731
                                     "plan_cd": plan, "src_lcts": loaded, "gld_lcts": gold_loaded}
    create_table(spark, tables["gold"], GOLD_COLUMNS, [gold("m1", 2, "15.50", "GOLD"), gold("m3", 1, "7.00", "SILVER")])

    assert main(["run", "--conf", str(replay.conf)]) == 0
    results = latest(spark, replay)
    assert ("claims_by_member_month", "T1_KEY_NULLS") not in results  # derived datasets are never checked
    [currency] = results[("gold_claims_by_month", "HOP_KEY_CURRENCY")]
    assert (currency.state, currency.population, currency.violations) == ("FAILED", 3, 1)
    [agreement] = results[("gold_claims_by_month", "HOP_VALUE_AGREEMENT")]
    assert (agreement.state, agreement.population, agreement.violations) == ("FAILED", 2, 1)
    lines = causes(spark, replay, "gold_claims_by_month", "HOP_KEY_CURRENCY", "TABLE_MERGE")
    assert {(line.code, line.proven) for line in lines.values()} == {("JOIN_UNMATCHED", True)}

    capsys.readouterr()
    assert main(["trace", "--conf", str(replay.conf), "--dataset", "gold_claims_by_month", "--key", "m2,2026-01"]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0].startswith("claims_by_member_month (derived): present, 1 row;")
    assert out[1] == f"gold_claims_by_month ({tables['gold']}): absent"
    assert out[2] == ("first gap: claims_by_member_month -> gold_claims_by_month: gold_claims_by_month does not have "
                      "the key (MISSING)")
    assert out[3] == "cause: JOIN_UNMATCHED (confirmed)"
