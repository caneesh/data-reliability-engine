"""Commit rows and committed-only views (Phase 1 design, ADR-07; build step 2).

Step 2's acceptance condition (checkpoint 1 ratification, 2026-10-17): failure-injection and
recovery tests proving that no incomplete evaluation becomes visible as a committed result. Each
test breaks the run at one write boundary and then checks two things:

- nothing of the interrupted evaluations is visible, in any view a reader uses;
- the next run publishes the held window once, completely.

The boundaries are:

1. the process dies after the evidence and before the results;
2. it dies after the results and before the commit;
3. the commit cannot be stored;
4. the evidence is partly stored and then fails.

Unit tests cover the views directly: an unpublished result is not "stored" for a RERUN, an
unpublished partition state is not a baseline, and a cause left by a run that never committed is
not shown.

A process that dies is a BaseException raised from inside a write: it escapes the run's error
handling exactly as a killed process stops it, so the run's STARTED row is never finished.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from hcsc.datalake.dre.checks.hop.common import key_hash
from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.replay.conftest import latest
from tests.replay.test_r04_to_r07_hops import KEY, SECRET, key_events, setup
from tests.store.conftest import append_rows, store  # noqa: F401  (the store fixture: a fresh dq database)

UTC = timezone.utc
HOP = ("gold_member_coverage", "HOP_KEY_CURRENCY")


class ProcessKilled(BaseException):
    """Stands for the process dying mid-write: not an Exception, so nothing in the run catches it."""


def stale_gold(spark, tmp_path, name: str):
    """R05: gold holds an older version of the key, so the hop FAILS with one STALE key event."""
    replay = setup(spark, tmp_path, name)
    opened, terminated = curated_load_time(30), curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(updated=opened), curated_row(end="2026-06-30", updated=terminated)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(record=opened, loaded=gold_load_time(29))])
    return replay


def visible(spark, replay) -> dict[str, int]:
    """What every committed view shows: if an interrupted run published nothing, all are 0."""
    db = replay.dq
    return {view: spark.table(f"{db}.{view}").count()
            for view in ("v_committed_result", "v_latest_result", "v_committed_key_event", "v_open_keys",
                         "v_committed_cause", "v_committed_partition_state", "v_latest_partition_state")}


def nothing_visible(spark, replay) -> None:
    assert visible(spark, replay) == dict.fromkeys(visible(spark, replay), 0)


def recover(spark, replay, monkeypatch) -> list:
    """The next scheduled run, once the store is back: the dead run's STARTED row no longer blocks
    (a run still STARTED after watchdog_grace_minutes counts as dead), so the test skips the wait."""
    from hcsc.datalake.dre.store import runs

    monkeypatch.undo()
    monkeypatch.setattr(runs, "in_progress", lambda *args, **kwargs: None)
    assert main(["run", "--conf", str(replay.conf)]) == 0
    [current] = latest(spark, replay)[HOP]
    return current


def statuses(spark, replay) -> list[str]:
    rows = sorted(spark.table(f"{replay.dq}.v_latest_run").collect(), key=lambda r: r.started_at)
    return [r.status for r in rows]


def test_a_run_that_dies_after_the_evidence_publishes_nothing(spark, tmp_path, monkeypatch) -> None:
    """Boundary 1: the hop's key events are stored, then the process dies writing its results."""
    from hcsc.datalake.dre import runner

    replay = stale_gold(spark, tmp_path, "commit_dies_evidence")
    real = runner.append_check_results

    def dies_on_the_hop(spark_, db, rows):
        if rows and (rows[0]["dataset"], rows[0]["check_id"]) == HOP:
            raise ProcessKilled
        return real(spark_, db, rows)

    monkeypatch.setattr(runner, "append_check_results", dies_on_the_hop)
    with pytest.raises(ProcessKilled):
        main(["run", "--conf", str(replay.conf)])
    assert key_events(spark, replay) == [(key_hash(SECRET, KEY), "FLAGGED", "STALE")]  # stored, not published
    assert spark.table(f"{replay.dq}.dq_check_result").count() > 0  # earlier checks' results, not published
    assert spark.table(f"{replay.dq}.dq_commit").count() == 0
    nothing_visible(spark, replay)
    assert statuses(spark, replay) == ["STARTED"]

    current = recover(spark, replay, monkeypatch)
    assert (current.state, current.violations) == ("FAILED", 1)
    open_keys = spark.table(f"{replay.dq}.v_open_keys").collect()
    assert [(k.key_hash, k.latest_event) for k in open_keys] == [(key_hash(SECRET, KEY), "FLAGGED")]  # once
    assert statuses(spark, replay) == ["STARTED", "COMPLETED"]


def test_a_run_that_dies_before_the_commit_publishes_nothing(spark, tmp_path, monkeypatch) -> None:
    """Boundary 2: every evidence and result row is stored, then the process dies writing the commits."""
    from hcsc.datalake.dre import runner

    replay = stale_gold(spark, tmp_path, "commit_dies_commit")

    def dies(*args, **kwargs):
        raise ProcessKilled

    monkeypatch.setattr(runner, "append_commits", dies)
    with pytest.raises(ProcessKilled):
        main(["run", "--conf", str(replay.conf)])
    raw_hop = [r for r in spark.table(f"{replay.dq}.dq_check_result").collect() if (r.dataset, r.check_id) == HOP]
    assert [r.state for r in raw_hop] == ["FAILED"]  # the result is in the table...
    nothing_visible(spark, replay)  # ...and in no view
    first_window = raw_hop[0].window_start

    current = recover(spark, replay, monkeypatch)
    assert (current.state, current.window_start) == ("FAILED", first_window)  # the same window, published once
    assert len(latest(spark, replay)[HOP]) == 1


def test_a_commit_that_cannot_be_stored_leaves_the_run_partial_and_nothing_visible(spark, tmp_path,
                                                                                  monkeypatch) -> None:
    """Boundary 3: the commit write fails (the store refuses it). The run ends PARTIAL with no check
    written, the window is held, and the next run publishes it."""
    from hcsc.datalake.dre import runner

    replay = stale_gold(spark, tmp_path, "commit_refused")

    def refused(*args, **kwargs):
        raise OSError("synthetic commit failure")

    monkeypatch.setattr(runner, "append_commits", refused)
    assert main(["run", "--conf", str(replay.conf)]) == 2
    nothing_visible(spark, replay)
    [run] = spark.table(f"{replay.dq}.v_latest_run").collect()
    assert (run.status, run.checks_written) == ("PARTIAL", 0)

    current = recover(spark, replay, monkeypatch)
    assert current.state == "FAILED"
    assert statuses(spark, replay) == ["PARTIAL", "COMPLETED"]


def test_partly_stored_evidence_is_never_published_and_the_marker_says_why(spark, tmp_path, monkeypatch) -> None:
    """Boundary 4: the hop's key events are stored, then the evidence write fails. The results are not
    written; an evidence_incomplete marker is, and committed, but its commit vouches for no evidence,
    so the stored key events stay invisible."""
    from hcsc.datalake.dre import runner

    replay = stale_gold(spark, tmp_path, "commit_partial_evidence")
    real = runner.append_key_events

    def stores_then_fails(*args, **kwargs):
        real(*args, **kwargs)
        raise OSError("synthetic failure after the key events were written")

    monkeypatch.setattr(runner, "append_key_events", stores_then_fails)
    assert main(["run", "--conf", str(replay.conf)]) == 2
    assert key_events(spark, replay) == [(key_hash(SECRET, KEY), "FLAGGED", "STALE")]  # in the table
    assert spark.table(f"{replay.dq}.v_committed_key_event").count() == 0  # never published
    assert spark.table(f"{replay.dq}.v_open_keys").count() == 0
    [marker] = latest(spark, replay)[HOP]
    assert (marker.state, marker.reason_category, marker.reason_code) == ("DID_NOT_RUN", "PLATFORM", "evidence_incomplete")
    assert "not published" in marker.observed
    raw_hop = [r.state for r in spark.table(f"{replay.dq}.dq_check_result").collect() if (r.dataset, r.check_id) == HOP]
    assert raw_hop == ["DID_NOT_RUN"]  # the FAILED result itself was never written

    current = recover(spark, replay, monkeypatch)
    assert current.state == "FAILED"
    assert [(k.key_hash, k.latest_event) for k in spark.table(f"{replay.dq}.v_open_keys").collect()] == [
        (key_hash(SECRET, KEY), "FLAGGED")]


def test_an_unpublished_result_is_not_stored_for_a_rerun(spark, store) -> None:
    """A RERUN skips an evaluation whose outcome is already stored; an unpublished row does not count,
    so the RERUN writes and publishes it rather than leaving it invisible for good."""
    from types import SimpleNamespace

    from hcsc.datalake.dre.runner import stored_outcomes

    start, end = datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 1, 16, 12, tzinfo=UTC)
    row = {"evaluation_id": "eval-x", "run_id": "run-x", "event_id": "a" * 64, "window_start": start,
           "window_end": end, "execution_type": "NORMAL", "dataset": "gold_member_coverage",
           "check_id": "T1_ON_TIME", "state": "PASSED", "population": 1, "violations": 0,
           "evaluated_at": end, "run_date": date(2026, 1, 16)}
    job = (SimpleNamespace(dataset=SimpleNamespace(dataset="gold_member_coverage")),
           SimpleNamespace(event_id="a" * 64, window_start=start, window_end=end))
    append_rows(spark, store, "dq_check_result", [row], commit=False)
    assert stored_outcomes(spark, store, [job]) == {}
    append_rows(spark, store, "dq_check_result", [row])  # published
    assert set(stored_outcomes(spark, store, [job])) == {"eval-x"}


def test_an_unpublished_partition_state_is_not_a_baseline(spark, store) -> None:
    from hcsc.datalake.dre.store.partitions import latest_state

    state = {"dataset": "provider_directory", "table_name": "gold_db.provider_directory", "partition_value": "d=1",
             "file_count": 1, "total_bytes": 10, "row_count": 5, "run_id": "run-p", "run_date": date(2026, 1, 16)}
    append_rows(spark, store, "dq_partition_state", [state], commit=False)
    assert latest_state(spark, store, "provider_directory") == {}
    append_rows(spark, store, "dq_partition_state", [state])
    assert set(latest_state(spark, store, "provider_directory")) == {"d=1"}


def test_a_cause_from_a_run_that_never_committed_stays_hidden(spark, store) -> None:
    """Run r1 investigated evaluation e1 but never committed it; run r2 later committed e1 (the same
    window again). r1's cause rows stay out; a retry by r3 after r2's commit is shown."""
    result = {"evaluation_id": "e1", "dataset": "gold_member_coverage", "check_id": "T1_ZERO_ROWS",
              "state": "FAILED", "run_date": date(2026, 1, 1)}
    append_rows(spark, store, "dq_check_result", [{**result, "run_id": "r1"}], commit=False)
    append_rows(spark, store, "dq_check_result", [{**result, "run_id": "r2"}])  # committed 2026-01-01 00:00 UTC
    cause = {"evaluation_id": "e1", "cause_code": "NO_UPSTREAM_DATA", "state": "CONFIRMED", "run_date": date(2026, 1, 1)}
    append_rows(spark, store, "dq_cause_result", [
        {**cause, "run_id": "r1", "evaluated_at": datetime(2025, 12, 31, 23, tzinfo=UTC)},  # before r2's commit
        {**cause, "run_id": "r3", "evaluated_at": datetime(2026, 1, 1, 6, tzinfo=UTC)},     # a retry after it
    ])
    shown = sorted(r.run_id for r in spark.table(f"{store}.v_committed_cause").where("evaluation_id = 'e1'").collect())
    assert shown == ["r3"]


def test_an_evaluation_with_no_result_rows_counts_as_written_and_commits_are_batched(spark, store) -> None:
    """The publisher counts an evaluation as written only once its commits are stored, appends them in
    batches, and counts an evaluation with no result rows (nothing to publish) at once."""
    from hcsc.datalake.dre.runner import Publisher
    from hcsc.datalake.dre.store import runs
    from hcsc.datalake.dre.store.commits import Commit

    run = runs.new_run(now=datetime(2026, 1, 16, 12, tzinfo=UTC))
    publisher = Publisher(spark, store, run, batch=2)
    publisher.add([], 1)
    assert publisher.published == 1
    publisher.add([Commit(run.run_id, "b1", "d", "c", 1)], 1)
    assert (publisher.published, spark.table(f"{store}.dq_commit").where(f"run_id = '{run.run_id}'").count()) == (1, 0)
    publisher.add([Commit(run.run_id, "b2", "d", "c", 1)], 0)  # a marker: published, not written
    assert (publisher.published, spark.table(f"{store}.dq_commit").where(f"run_id = '{run.run_id}'").count()) == (2, 2)
