"""Measure a finished `dre run` from its store (ADR-08, build step 8), the same way locally and on a
cluster: the run's elapsed time from dq_run, against run_budget_minutes and the backpressure
threshold, and each check's duration_ms statistics from dq_check_result (one duration per
evaluation: the group rows of a grouped check share theirs).

    python scripts/measure_run.py --conf conf [--run-id RUN_ID]
    spark-submit --py-files dre-pyfiles-<version>.zip scripts/measure_run.py --conf conf

Without --run-id it measures the latest finished NORMAL run. Read-only: it runs SELECTs against the
configured dq database and prints times, counts and check ids only.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_RUN_ID = re.compile(r"^[0-9a-f-]{36}$")


def main(argv: list[str]) -> int:
    from hcsc.datalake.dre.checks.times import as_utc
    from hcsc.datalake.dre.config.validate import validate_conf
    from hcsc.datalake.dre.session import get_spark

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--conf", required=True)
    parser.add_argument("--run-id")
    args = parser.parse_args(argv)
    config, errors, _ = validate_conf(Path(args.conf))
    if errors or config.defaults is None:
        print(f"measure: {args.conf} has configuration errors; run dre validate --conf {args.conf}")
        return 3
    if args.run_id is not None and not _RUN_ID.match(args.run_id):
        print("measure: --run-id is not a run id")
        return 3
    d = config.defaults
    db = d.dq_database
    spark = get_spark("dre-measure")
    which = (f"run_id = '{args.run_id}'" if args.run_id
             else "execution_type = 'NORMAL' AND status <> 'STARTED'")
    runs = spark.sql(f"SELECT * FROM {db}.v_latest_run WHERE {which} ORDER BY started_at DESC LIMIT 1").collect()
    if not runs:
        print("measure: no such finished run in the store")
        return 1
    run = runs[0]
    elapsed = ((as_utc(run.ended_at) - as_utc(run.started_at)).total_seconds() / 60) if run.ended_at else None
    print(f"run {run.run_id} ({run.execution_type}, {run.status}): {run.checks_written} of {run.checks_expected} "
          f"checks written, started {as_utc(run.started_at):%Y-%m-%d %H:%M:%S} UTC")
    if elapsed is None:
        print("elapsed: unknown (the run has no final row)")
    else:
        print(f"elapsed: {elapsed:.2f} minutes = {elapsed / d.run_budget_minutes:.1%} of the {d.run_budget_minutes:g}-minute "
              f"run budget (backpressure above {d.backpressure_share:.0%})")
    day = f"DATE'{run.run_date.isoformat()}'"
    evaluations = (f"SELECT DISTINCT dataset, check_id, event_id, evaluated_at, duration_ms, reason_code "
                   f"FROM {db}.dq_check_result WHERE run_id = '{run.run_id}' AND run_date = {day}")
    rows = spark.sql(
        f"SELECT check_id, COUNT(*) AS evaluations, SUM(duration_ms) AS total_ms, "
        f"percentile_approx(duration_ms, 0.5) AS p50_ms, percentile_approx(duration_ms, 0.95) AS p95_ms, "
        f"MAX(duration_ms) AS max_ms, SUM(CASE WHEN reason_code IN ('run_budget_exceeded', 'disabled') "
        f"THEN 1 ELSE 0 END) AS not_run "
        f"FROM ({evaluations}) e GROUP BY check_id ORDER BY total_ms DESC, check_id").collect()
    print(f"{'check_id':<28} {'evals':>5} {'total_s':>9} {'p50_ms':>8} {'p95_ms':>8} {'max_ms':>8} {'not_run':>7}")
    total = 0
    for r in rows:
        total += r.total_ms or 0
        print(f"{r.check_id:<28} {r.evaluations:>5} {(r.total_ms or 0) / 1000:>9.1f} {r.p50_ms or 0:>8} "
              f"{r.p95_ms or 0:>8} {r.max_ms or 0:>8} {r.not_run:>7}")
    print(f"checks: {sum(r.evaluations for r in rows)} evaluations, {total / 60000:.2f} minutes in checks"
          + (f"; {elapsed - total / 60000:.2f} minutes outside them (planning, landing listing, writes, "
             "commits, causes, digest)" if elapsed is not None else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
