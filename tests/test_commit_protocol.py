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
unpublished partition state is not a baseline, and a cause attaches to a result by identity, never
by time.

The checkpoint 2 review (2026-10-17) added: a partly stored cause set, a marker over partly stored
results, verification of the stored rows against the commits (a unit withheld whole, a batch that
cannot be counted), a commit write that fails after storing none, some or all of its rows, the
integrity audit view, and the REPLAY window list from committed results.

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


def test_a_cause_attaches_to_a_result_by_identity_never_by_time(spark, store) -> None:
    """Checkpoint 2 review (CP2-03, E3 revised): a cause row is visible only with a cause commit of its
    own run and evaluation id, naming a result that has a result commit. Run r1 investigated e1 but
    never committed; run r2 committed e1 (the same window again). r1's causes stay out even though
    their timestamp is after r2's commit (a host whose clock runs ahead), which the timing rule of the
    first build let through; r3's retry, committed and naming r2, is shown with result_run_id r2."""
    day = date(2026, 2, 1)
    result = {"evaluation_id": "e1", "dataset": "gold_member_coverage", "check_id": "T1_ZERO_ROWS",
              "state": "FAILED", "run_date": day}
    append_rows(spark, store, "dq_check_result", [{**result, "run_id": "r1"}], commit=False)
    append_rows(spark, store, "dq_check_result", [{**result, "run_id": "r2"}])  # committed 2026-01-01 00:00 UTC
    cause = {"evaluation_id": "e1", "cause_code": "NO_UPSTREAM_DATA", "state": "CONFIRMED", "run_date": day,
             "evaluated_at": datetime(2026, 1, 2, tzinfo=UTC)}  # after r2's commit, for every row
    append_rows(spark, store, "dq_cause_result", [{**cause, "run_id": "r1"}], commit=False)  # r1 never committed
    append_rows(spark, store, "dq_cause_result", [{**cause, "run_id": "r3", "result_run_id": "r2"}])
    append_rows(spark, store, "dq_cause_result", [{**cause, "run_id": "r4", "result_run_id": "r1"}])  # names an unpublished result
    shown = sorted((r.run_id, r.result_run_id)
                   for r in spark.table(f"{store}.v_committed_cause").where("evaluation_id = 'e1'").collect())
    assert shown == [("r3", "r2")]


def test_a_partly_stored_cause_set_is_never_published_and_the_next_run_retries_it(spark, tmp_path,
                                                                                 monkeypatch) -> None:
    """An investigation stores some cause rows, then its write fails. The FAILED result is published,
    its partial causes are not (no cause commit), the investigation is not done, and the next run's
    retry publishes a complete cause set naming the first run's result."""
    from hcsc.datalake.dre import runner

    replay = stale_gold(spark, tmp_path, "commit_partial_causes")
    real = runner.append_cause_results

    def stores_one_then_fails(spark_, db, rows):
        real(spark_, db, rows[:1])
        raise OSError("synthetic failure after one cause row was written")

    monkeypatch.setattr(runner, "append_cause_results", stores_one_then_fails)
    assert main(["run", "--conf", str(replay.conf)]) == 0  # an unfinished investigation never fails the run
    [failed] = latest(spark, replay)[HOP]
    assert failed.state == "FAILED"
    raw = spark.table(f"{replay.dq}.dq_cause_result").where(f"evaluation_id = '{failed.evaluation_id}'")
    assert raw.count() == 1  # in the table (every investigation of the run stored one row, then failed)...
    assert spark.table(f"{replay.dq}.v_committed_cause").count() == 0  # ...and in no view
    [first] = spark.table(f"{replay.dq}.v_latest_run").collect()
    assert first.investigations_done < first.investigations_expected

    monkeypatch.undo()
    monkeypatch.setattr(runner.runs, "in_progress", lambda *args, **kwargs: None)
    assert main(["run", "--conf", str(replay.conf)]) == 0
    shown = spark.table(f"{replay.dq}.v_committed_cause").where(f"evaluation_id = '{failed.evaluation_id}'").collect()
    assert shown and {r.result_run_id for r in shown} == {failed.run_id}  # explains the first run's result...
    assert failed.run_id not in {r.run_id for r in shown}  # ...with the retry's own rows, not the partial set


def test_a_marker_over_partly_stored_results_is_withheld(spark, tmp_path, monkeypatch) -> None:
    """The hop's result write stores its row, then fails, so the run writes the evidence_incomplete
    marker under the same evaluation id. Two rows are stored where the marker's commit declares one:
    verification withholds it, so neither the FAILED row nor the marker becomes visible as committed
    (the first build published both). The next run publishes the window."""
    from hcsc.datalake.dre import runner

    replay = stale_gold(spark, tmp_path, "commit_marker_over_partial")
    real = runner.append_check_results

    def stores_then_fails_on_the_hop(spark_, db, rows):
        real(spark_, db, rows)
        if rows and (rows[0]["dataset"], rows[0]["check_id"]) == HOP and rows[0]["state"] == "FAILED":
            raise OSError("synthetic failure after the result was written")

    monkeypatch.setattr(runner, "append_check_results", stores_then_fails_on_the_hop)
    assert main(["run", "--conf", str(replay.conf)]) == 2
    raw_hop = sorted(r.state for r in spark.table(f"{replay.dq}.dq_check_result").collect() if (r.dataset, r.check_id) == HOP)
    assert raw_hop == ["DID_NOT_RUN", "FAILED"]
    assert HOP not in latest(spark, replay)
    assert spark.table(f"{replay.dq}.v_committed_key_event").count() == 0

    current = recover(spark, replay, monkeypatch)
    assert (current.state, current.violations) == ("FAILED", 1)


def stored_results(spark, store, run, ids: list[str]) -> None:
    """The result rows a unit's commits vouch for, stored as the run stores them (uncommitted)."""
    append_rows(spark, store, "dq_check_result", [
        {"evaluation_id": i, "run_id": run.run_id, "dataset": "d", "check_id": "c", "state": "PASSED",
         "run_date": run.run_date} for i in ids], commit=False)


def committed(spark, store, run) -> list[str]:
    return sorted(r.evaluation_id for r in spark.table(f"{store}.v_committed_result").where(f"run_id = '{run.run_id}'").collect())


def test_an_evaluation_with_no_result_rows_counts_as_written_and_commits_are_batched(spark, store) -> None:
    """The publisher counts an evaluation as written only once its commits are stored, appends them in
    batches, and counts an evaluation with no result rows (nothing to publish) at once."""
    from hcsc.datalake.dre.runner import Publisher
    from hcsc.datalake.dre.store import runs
    from hcsc.datalake.dre.store.commits import Commit

    run = runs.new_run(now=datetime(2026, 1, 16, 12, tzinfo=UTC))
    stored_results(spark, store, run, ["b1", "b2"])
    publisher = Publisher(spark, store, run, batch=2)
    publisher.add([], 1)
    assert publisher.published == 1
    publisher.add([Commit(run.run_id, "b1", "d", "c", 1)], 1)
    assert (publisher.published, spark.table(f"{store}.dq_commit").where(f"run_id = '{run.run_id}'").count()) == (1, 0)
    publisher.add([Commit(run.run_id, "b2", "d", "c", 1)], 0)  # a marker: published, not written
    assert (publisher.published, spark.table(f"{store}.dq_commit").where(f"run_id = '{run.run_id}'").count()) == (2, 2)


def test_a_unit_whose_stored_rows_differ_from_its_commits_is_withheld_whole(spark, store) -> None:
    """Checkpoint 2 review (CP2-01): before a batch is written, the rows its commits vouch for are
    counted. A grouped evaluation missing one group's result row, and an evaluation declaring two key
    events with one stored, are withheld whole (none of their commits written, not counted); the
    matching evaluation in the same batch is published."""
    from hcsc.datalake.dre.runner import Publisher
    from hcsc.datalake.dre.store import runs
    from hcsc.datalake.dre.store.commits import Commit

    run = runs.new_run(now=datetime(2026, 1, 17, 12, tzinfo=UTC))
    stored_results(spark, store, run, ["ok", "g1", "ev"])  # group g2's row is missing
    append_rows(spark, store, "dq_key_event", [{"run_id": run.run_id, "evaluation_id": "ev", "key_hash": "h1",
                                                "run_date": run.run_date}], commit=False)
    publisher = Publisher(spark, store, run, batch=10)
    publisher.add([Commit(run.run_id, "ok", "d", "c", 1)], 1)
    publisher.add([Commit(run.run_id, "g1", "d", "c", 1), Commit(run.run_id, "g2", "d", "c", 1)], 1)
    publisher.add([Commit(run.run_id, "ev", "d", "c", 1, key_event_rows=2)], 1)
    publisher.flush()
    assert committed(spark, store, run) == ["ok"]
    assert (publisher.published, publisher.withheld) == (1, 2)
    assert spark.table(f"{store}.dq_commit").where(f"run_id = '{run.run_id}'").count() == 1


def test_a_batch_whose_rows_cannot_be_counted_is_withheld(spark, store, monkeypatch) -> None:
    from hcsc.datalake.dre import runner
    from hcsc.datalake.dre.runner import Publisher
    from hcsc.datalake.dre.store import runs
    from hcsc.datalake.dre.store.commits import Commit

    run = runs.new_run(now=datetime(2026, 1, 18, 12, tzinfo=UTC))
    stored_results(spark, store, run, ["x1"])

    def unreadable(*args, **kwargs):
        raise OSError("synthetic: the store cannot be read")

    monkeypatch.setattr(runner, "stored_counts", unreadable)
    publisher = Publisher(spark, store, run)
    publisher.add([Commit(run.run_id, "x1", "d", "c", 1)], 1)
    publisher.flush()
    assert (committed(spark, store, run), publisher.published, publisher.withheld) == ([], 0, 1)


@pytest.mark.parametrize("stored_before_failure", ["none", "some", "all"])
def test_a_failed_commit_write_is_read_back_and_completed(spark, store, monkeypatch, stored_before_failure) -> None:
    """Checkpoint 2 review (CP2-02): the commit write raises after storing none, some or all of the
    batch's rows. The run reads back what was stored and appends the missing rows once more, so every
    unit is published and counted exactly once, whatever the write left."""
    from hcsc.datalake.dre import runner
    from hcsc.datalake.dre.runner import Publisher
    from hcsc.datalake.dre.store import runs
    from hcsc.datalake.dre.store.commits import Commit

    day = {"none": 19, "some": 20, "all": 21}[stored_before_failure]
    run = runs.new_run(now=datetime(2026, 1, day, 12, tzinfo=UTC))
    ids = ["a1", "a2", "a3"]
    stored_results(spark, store, run, ids)
    real = runner.append_commits
    calls: list[int] = []

    def fails_once(spark_, db, commits, at, run_date):
        calls.append(len(commits))
        if len(calls) == 1:
            keep = {"none": 0, "some": 2, "all": len(commits)}[stored_before_failure]
            real(spark_, db, commits[:keep], at, run_date)
            raise OSError("synthetic failure after part of the batch was stored")
        return real(spark_, db, commits, at, run_date)

    monkeypatch.setattr(runner, "append_commits", fails_once)
    publisher = Publisher(spark, store, run)
    publisher.add([Commit(run.run_id, "a1", "d", "c", 1), Commit(run.run_id, "a2", "d", "c", 1)], 1)  # two groups
    publisher.add([Commit(run.run_id, "a3", "d", "c", 1)], 1)
    publisher.flush()
    assert committed(spark, store, run) == ids
    assert publisher.published == 2
    assert calls == {"none": [3, 3], "some": [3, 1], "all": [3]}[stored_before_failure]


def test_a_unit_left_split_by_a_failed_repair_does_not_count(spark, store, monkeypatch) -> None:
    """The write stores part of the batch and raises, then the repair write fails too. Only units whose
    commits are all stored count. (A batch is written as one file, so a real write cannot leave a
    subset; this is the worst case the repair has to report.)"""
    from hcsc.datalake.dre import runner
    from hcsc.datalake.dre.runner import Publisher
    from hcsc.datalake.dre.store import runs
    from hcsc.datalake.dre.store.commits import Commit

    run = runs.new_run(now=datetime(2026, 1, 22, 12, tzinfo=UTC))
    stored_results(spark, store, run, ["s1", "s2", "s3"])
    real = runner.append_commits
    calls: list[int] = []

    def stores_two_then_always_fails(spark_, db, commits, at, run_date):
        calls.append(len(commits))
        if len(calls) == 1:
            real(spark_, db, commits[:2], at, run_date)
        raise OSError("synthetic commit failure")

    monkeypatch.setattr(runner, "append_commits", stores_two_then_always_fails)
    publisher = Publisher(spark, store, run)
    publisher.add([Commit(run.run_id, "s1", "d", "c", 1)], 1)
    publisher.add([Commit(run.run_id, "s2", "d", "c", 1), Commit(run.run_id, "s3", "d", "c", 1)], 1)
    publisher.flush()
    assert publisher.published == 1  # s1's unit; the split unit (s2 stored, s3 not) does not count


def test_the_integrity_view_lists_commits_whose_rows_were_lost(spark, store) -> None:
    """v_commit_integrity (audit): a commit whose stored rows now differ from its declaration is listed,
    here partition states gone after publication (as when their retention, shorter than the results',
    has dropped them); a commit that still matches is not."""
    from hcsc.datalake.dre.store.commits import Commit, append_commits

    day = date(2026, 1, 23)
    append_rows(spark, store, "dq_check_result", [
        {"evaluation_id": "pd", "run_id": "rp", "dataset": "provider_directory", "check_id": "T1_PARTITION_DRIFT",
         "state": "PASSED", "run_date": day},
        {"evaluation_id": "fine", "run_id": "rp", "dataset": "d", "check_id": "c", "state": "PASSED", "run_date": day}],
        commit=False)
    append_commits(spark, store, [Commit("rp", "pd", "provider_directory", "T1_PARTITION_DRIFT", 1, partition_state_rows=3),
                                  Commit("rp", "fine", "d", "c", 1)], datetime(2026, 1, 23, tzinfo=UTC), day)
    rows = spark.table(f"{store}.v_commit_integrity").where(f"run_date = DATE'{day.isoformat()}'").collect()
    assert [(r.evaluation_id, r.partition_state_rows, r.stored_partition_state_rows) for r in rows] == [("pd", 3, 0)]


def test_the_replay_window_list_has_committed_windows_only(spark, store) -> None:
    """E1 condition: the window history reads the raw table (it must see uncommitted attempts to hold
    their windows), but the REPLAY window list reads committed results only: an attempt that was never
    committed is not one of the dataset's windows."""
    from hcsc.datalake.dre.checks.events import recorded_windows

    base = {"dataset": "gold_member_coverage", "check_id": "T1_ON_TIME", "execution_type": "NORMAL",
            "state": "PASSED", "run_date": date(2026, 1, 24)}
    kept = (datetime(2026, 1, 23, 12, tzinfo=UTC), datetime(2026, 1, 23, 13, tzinfo=UTC))
    attempt = (datetime(2026, 1, 23, 13, tzinfo=UTC), datetime(2026, 1, 23, 14, tzinfo=UTC))
    append_rows(spark, store, "dq_check_result", [{**base, "evaluation_id": "w1", "run_id": "rw1",
                                                   "window_start": kept[0], "window_end": kept[1]}])
    append_rows(spark, store, "dq_check_result", [{**base, "evaluation_id": "w2", "run_id": "rw2",
                                                   "window_start": attempt[0], "window_end": attempt[1]}], commit=False)
    found = recorded_windows(spark, store, ["gold_member_coverage"], datetime(2026, 1, 23, tzinfo=UTC),
                             datetime(2026, 1, 24, tzinfo=UTC))
    assert found == {"gold_member_coverage": [kept]}
