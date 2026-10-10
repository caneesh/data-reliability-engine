"""Build synthetic tables, landing folders and a conf copy for an end-to-end `dre run`.

Run from the work directory: Spark keeps its local metastore (metastore_db) and
warehouse there, so later `dre install` and `dre run` processes see the same tables.

    cd e2e && python ../tests/e2e/build_synthetic.py
"""

from __future__ import annotations

import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from hcsc.datalake.dre.session import get_spark  # noqa: E402
from hcsc.datalake.dre.store.local_setup import create_database  # noqa: E402
from tests.fixtures.layers import (  # noqa: E402
    CURATED_COLUMNS, GOLD_COLUMNS, PROVIDER_GOLD_COLUMNS, PROVIDER_RAW_COLUMNS, create_table, curated_load_time,
    curated_row, gold_load_time, gold_row, provider_gold_row, provider_raw_row,
)

UTC = timezone.utc


def main() -> None:
    work = Path.cwd()
    now = datetime.now(UTC)

    conf = work / "conf"
    if conf.exists():
        shutil.rmtree(conf)
    shutil.copytree(REPO / "conf", conf)
    landing_rt, landing_roster = work / "landing" / "example_feed", work / "landing" / "provider_roster"
    for folder in (landing_rt, landing_roster, work / "logs"):
        folder.mkdir(parents=True, exist_ok=True)
    for path in conf.rglob("*.yaml"):
        text = path.read_text(encoding="utf-8")
        text = text.replace("/data/landing/example_feed", str(landing_rt))
        text = text.replace("/data/landing/provider_roster", str(landing_roster))
        text = text.replace("/data/logs/provider_roster/*.log", str(work / "logs" / "*.log"))
        path.write_text(text, encoding="utf-8")

    for hours in (30, 20, 10, 2):
        (landing_rt / f"rt_{hours:04d}.seq").write_bytes(b"SEQ synthetic")
    (landing_roster / "roster_latest.csv").write_text("provider_id,provider_name\nP001,Provider P001\n", encoding="utf-8")

    spark = get_spark("dre-e2e-build")
    create_database(spark, "dq")  # the platform team's part on the cluster
    # First feed: gold loads every two hours for two days; curated likewise.
    create_table(spark, "gold_db.member_coverage", GOLD_COLUMNS,
                 [gold_row(sub_id=f"0001234{h:02d}", loaded=gold_load_time(h, now)) for h in range(1, 48, 2)])
    create_table(spark, "curated_db.enrollment", CURATED_COLUMNS,
                 [curated_row(sub_id=f"1234{h:02d}", updated=curated_load_time(h, now)) for h in range(1, 48, 2)])
    # Second feed: the roster loaded daily for the last 40 days; the merge likewise.
    days = range(1, 40)
    create_table(spark, "landing_db.provider_roster", PROVIDER_RAW_COLUMNS,
                 [provider_raw_row(f"P{d:03d}", now - timedelta(days=d), "roster_latest.csv") for d in days])
    create_table(spark, "gold_db.provider_directory", PROVIDER_GOLD_COLUMNS,
                 [provider_gold_row(f"P{d:03d}", now - timedelta(days=d) + timedelta(hours=12)) for d in days])
    spark.stop()
    print(f"e2e: synthetic tables, landing and conf ready in {work}")


if __name__ == "__main__":
    main()
