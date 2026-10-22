"""Gate A remediation, release N (R2-D14): two engine versions never write key events concurrently.

Only NORMAL runs write key events, and `dre run` refuses a NORMAL run while another NORMAL run is
STARTED within `watchdog_grace_minutes`, whatever that run's engine version: so a release N and a
release N+1 engine cannot interleave encodings on one store. This pins that the guard ignores the
engine version.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from hcsc.datalake.dre.store import runs
from hcsc.datalake.dre.store.local_setup import create_store
from tests.store.conftest import append_rows

UTC = timezone.utc
NOW = datetime(2026, 1, 16, 12, tzinfo=UTC)


@pytest.fixture(scope="module")
def store(spark) -> str:
    create_store(spark, "dq_run_overlap_versions")
    return "dq_run_overlap_versions"


def started(spark, store, run_id: str, engine_version: str, minutes_ago: int) -> None:
    at = NOW - timedelta(minutes=minutes_ago)
    append_rows(spark, store, "dq_run", [{
        "run_id": run_id, "started_at": at, "engine_version": engine_version, "status": "STARTED",
        "execution_type": "NORMAL", "run_date": at.date()}])


def test_a_started_run_of_another_engine_version_blocks_a_normal_run(spark, store) -> None:
    started(spark, store, "other-version-run", "release-n-plus-1-synthetic", 10)
    assert runs.in_progress(spark, store, NOW, 60) == ("other-version-run", NOW - timedelta(minutes=10))


def test_an_older_started_run_of_another_version_is_dead_and_does_not_block(spark, store) -> None:
    assert runs.in_progress(spark, store, NOW + timedelta(minutes=120), 60) is None
