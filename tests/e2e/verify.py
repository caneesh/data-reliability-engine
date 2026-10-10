"""Check an end-to-end `dre run`: one COMPLETED run, results for every synthetic feed and the
table-wide dataset, and no PLATFORM failures (which would mean an engine error). Exit 1 if not."""

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
    states: dict[str, int] = {}
    for r in results:
        states[r.state] = states.get(r.state, 0) + 1
    print(f"e2e: {len(results)} results {states}; run {[r.status for r in runs]}")
    for problem in problems:
        print(f"e2e: {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
