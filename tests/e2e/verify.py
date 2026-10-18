"""Check an end-to-end `dre run`: one COMPLETED run, results for every synthetic feed and the
table-wide dataset, no PLATFORM failures (which would mean an engine error), and the second
feeds' healthy hops (roster to directory; curated to gold) PASSED, HOP_LEDGER included, with each
ledger committed and balanced (its dispositions add up to its due population). Exit 1 if not."""

from __future__ import annotations

import sys

from hcsc.datalake.dre.session import get_spark

EXPECTED_FEEDS = {"example_realtime", "provider_roster_monthly", "provider_directory_merge"}


def main() -> int:
    spark = get_spark("dre-e2e-verify")
    runs = spark.table("dq.v_latest_run").collect()
    results = spark.table("dq.dq_check_result").collect()
    problems = []
    if [r.status for r in runs] != ["COMPLETED"]:
        problems.append(f"expected one COMPLETED run, got {[r.status for r in runs]}")
    feeds = {r.feed for r in results if r.feed}
    if feeds != EXPECTED_FEEDS:
        problems.append(f"results for feeds {sorted(feeds)}, expected {sorted(EXPECTED_FEEDS)}")
    if not [r for r in results if r.feed is None]:
        problems.append("no results for the table-wide dataset")
    platform = [(r.dataset, r.check_id, r.reason_code) for r in results if r.reason_category == "PLATFORM"]
    if platform:
        problems.append(f"PLATFORM failures: {platform}")
    for dataset in ("provider_directory", "gold_member_coverage"):
        hops = {r.check_id: r.state for r in results if r.dataset == dataset and r.check_id.startswith("HOP_")}
        if hops != {"HOP_KEY_CURRENCY": "PASSED", "HOP_VALUE_AGREEMENT": "PASSED", "HOP_LEDGER": "PASSED"}:
            problems.append(f"{dataset} hops {hops}, expected all three PASSED")
    population = ("due", "pending", "untimed", "orphan", "overlap", "guard")
    for ledger in [r for r in results if r.check_id == "HOP_LEDGER" and r.state == "PASSED"]:
        lines = spark.table("dq.v_committed_reconciliation").filter(
            f"evaluation_id = '{ledger.evaluation_id}'").collect()
        due = sum(x.records for x in lines if x.line == "due")
        buckets = sum(x.records or 0 for x in lines if x.line not in population)
        if not lines or due != buckets or due != ledger.population:
            problems.append(f"{ledger.dataset} HOP_LEDGER: {len(lines)} committed lines, due {due}, "
                            f"in buckets {buckets}, population {ledger.population}")
    drift = [r for r in results if r.dataset == "provider_directory" and r.check_id == "T1_PARTITION_DRIFT"]
    if [(r.state, r.reason_code) for r in drift] != [("DID_NOT_RUN", "insufficient_history")]:
        problems.append(f"provider_directory T1_PARTITION_DRIFT {[(r.state, r.reason_code) for r in drift]}, "
                        "expected the first run to record the baseline")
    recorded = spark.table("dq.v_latest_partition_state").filter("dataset = 'provider_directory'").count()
    if recorded == 0:
        problems.append("no partition state recorded for provider_directory")
    states: dict[str, int] = {}
    for r in results:
        states[r.state] = states.get(r.state, 0) + 1
    print(f"e2e: {len(results)} results {states}; run {[r.status for r in runs]}")
    for problem in problems:
        print(f"e2e: {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
