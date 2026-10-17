"""Checkpoint 2 corrections R1 to R3 (the user's ratification, 2026-10-17), one or more tests per row
of the failure-injection matrix F01 to F10:

- F01 evidence changes between verification and commit, and late (cancelled) threads;
- F02 evidence missing after commit;
- F03 a commit append partly succeeds, then raises (also tests/test_commit_protocol.py);
- F04 the repair append succeeds, then the readback fails;
- F05 a retry under the same deterministic evaluation id;
- F06 two publishers race on the same evaluation;
- F07 a cause on another date than its result;
- F08 orphan, conflicting and collision cause commits;
- F09 commit metadata outlives its evidence's retention;
- F10 the earlier checkpoint 2 suite (tests/test_commit_protocol.py), unchanged in intent.

Plus: evidence states (VALID, INVALID, EXPIRED, UNKNOWN) and the one definition of healthy,
retention deadlines and investigation holds, compromised baselines (the hop's forced full sweep,
partition drift that cannot pass), NORMAL/RERUN/REPLAY precedence, the digest and the watchdog.
The assertion that runs through all of them: compromised or unverified evidence never yields a
healthy status.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone

import pytest

from tests.store.conftest import append_rows, store  # noqa: F401  (the store fixture: a fresh dq database)

UTC = timezone.utc
RETENTION = {"dq_run": 84, "dq_check_result": 84, "dq_cause_result": 84, "dq_key_event": 84, "dq_file": 13,
             "dq_partition_state": 13, "dq_commit": 84, "dq_integrity": 84}


def new_run(day: int, hour: int = 12):
    from hcsc.datalake.dre.store import runs

    return runs.new_run(now=datetime(2026, 2, day, hour, tzinfo=UTC))


def result_row(run, evaluation_id: str, state: str = "PASSED", check_id: str = "HOP_KEY_CURRENCY",
               dataset: str = "gold_member_coverage", **extra) -> dict:
    return {"evaluation_id": evaluation_id, "run_id": run.run_id, "dataset": dataset, "check_id": check_id,
            "state": state, "execution_type": "NORMAL", "window_start": datetime(2026, 2, 1, tzinfo=UTC),
            "window_end": datetime(2026, 2, 2, tzinfo=UTC), "evaluated_at": run.started_at, "run_date": run.run_date,
            **extra}


def key_event_rows(run, evaluation_id: str, n: int, event: str = "CLEARED") -> list[dict]:
    return [{"run_id": run.run_id, "evaluation_id": evaluation_id, "dataset": "gold_member_coverage",
             "check_id": "HOP_KEY_CURRENCY", "hop": "example_curated_enrollment->gold_member_coverage",
             "key_hash": f"{evaluation_id}-{i}", "key_value": "synthetic-key", "event": event,
             "state_detail": "CURRENT", "observed_at": run.started_at, "run_date": run.run_date} for i in range(n)]


def publish(spark, db, run, evaluation_id: str, key_events: int = 0, state: str = "PASSED", **extra):
    """Store an evaluation's rows as a run does (uncommitted), then publish it through the publisher."""
    from hcsc.datalake.dre.runner import Publisher
    from hcsc.datalake.dre.store.commits import Commit

    append_rows(spark, db, "dq_check_result", [result_row(run, evaluation_id, state, **extra)], commit=False)
    if key_events:
        append_rows(spark, db, "dq_key_event", key_event_rows(run, evaluation_id, key_events), commit=False)
    publisher = Publisher(spark, db, run, retention=RETENTION)
    publisher.add([Commit(run.run_id, evaluation_id, "gold_member_coverage", extra.get("check_id", "HOP_KEY_CURRENCY"),
                          1, key_event_rows=key_events)], 1)
    publisher.flush()
    return publisher


def latest(spark, db, evaluation_id: str):
    rows = spark.table(f"{db}.v_committed_result").where(f"evaluation_id = '{evaluation_id}'").collect()
    assert len(rows) == 1
    return rows[0]


def healthy(spark, db, run, evaluation_id: str) -> bool:
    """v_latest_result's definition, for one committed row: PASSED with VALID evidence."""
    row = spark.sql(f"SELECT state = 'PASSED' AND evidence_state = 'VALID' AS h FROM {db}.v_committed_result "
                    f"WHERE run_id = '{run.run_id}' AND evaluation_id = '{evaluation_id}'").collect()[0]
    return row.h


def sweep(spark, db, run):
    from hcsc.datalake.dre.store import integrity

    return integrity.sweep(spark, db, run, integrity.DAILY, run.run_date - timedelta(days=35), run.run_date,
                           run.run_date)


def records(spark, db, kind: str) -> list:
    return spark.table(f"{db}.dq_integrity").where(f"record = '{kind}'").collect()


# --- R1: evidence consistency ---


def test_f01_evidence_changed_after_verification_is_invalid_and_never_healthy(spark, store, monkeypatch) -> None:
    """F01: a writer outside DRE adds a key event to a unit between its verification and its commit.
    Publication cannot see it (the commit follows the count), so the sweep finds it: the unit becomes
    INVALID, its PASSED result stays visible but is not healthy, and its key events are no longer
    shown, so no open-key baseline is built on them."""
    from hcsc.datalake.dre import runner

    run = new_run(1)
    real = runner.append_commits

    def outside_writer_then_commit(spark_, db, commits, at, run_date):
        append_rows(spark_, db, "dq_key_event", [{**key_event_rows(run, "f01", 1)[0], "key_hash": "added-outside-dre"}],
                    commit=False)
        return real(spark_, db, commits, at, run_date)

    monkeypatch.setattr(runner, "append_commits", outside_writer_then_commit)
    publisher = publish(spark, store, run, "f01", key_events=2)
    assert publisher.published == 1  # verified: 2 key events stored, 2 declared
    monkeypatch.undo()
    assert (latest(spark, store, "f01").evidence_state, healthy(spark, store, run, "f01")) == ("VALID", True)
    outcome = sweep(spark, store, run)
    assert outcome.mismatches >= 1
    [found] = [r for r in records(spark, store, "EVIDENCE_MISMATCH") if r.evaluation_id == "f01"]
    assert found.detail == "key events: 2 declared, 3 stored"
    assert latest(spark, store, "f01").state == "PASSED"  # the published result is preserved...
    assert (latest(spark, store, "f01").evidence_state, healthy(spark, store, run, "f01")) == ("INVALID", False)
    assert spark.table(f"{store}.v_committed_key_event").where("evaluation_id = 'f01'").count() == 0  # ...its evidence is not


def test_f01_a_cancelled_investigation_that_finishes_late_never_publishes(spark, tmp_path, monkeypatch) -> None:
    """F01, late thread: an investigation cancelled by its budget keeps running and stores its cause
    rows afterwards. The cause commit is written only by the run's thread after a successful
    investigation, so the late rows stay unpublished (before the correction, the publisher-less path
    committed inside the cancellable thread)."""
    from hcsc.datalake.dre import runner
    from hcsc.datalake.dre.checks.base import from_counts
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.checks.tier1.on_time import OnTime
    from hcsc.datalake.dre.config.loader import load
    from hcsc.datalake.dre.runner import Evaluation, Investigations, Planned, check_context, result_rows
    from hcsc.datalake.dre.store import runs
    from tests.replay.conftest import replay_conf

    replay = replay_conf(spark, tmp_path, "late_investigation")
    config, _ = load(replay.conf)
    ds = config.datasets["example_curated_enrollment"]
    planned = Planned(config.feeds["example_realtime"], ds, OnTime())
    at = datetime(2026, 2, 3, 12, tzinfo=UTC)
    event = Event(ds.dataset, at - timedelta(hours=1), at)
    evaluation = Evaluation(planned, event, [from_counts(1, 1, "1 of 1 slots missed", "every slot loaded")], at, 10,
                            check_context(spark, config, planned, event))
    run = runs.new_run(now=at)
    rows = result_rows(evaluation, run, "NORMAL")

    def slow_explain(cx, result, evaluation_id, run_id, run_date, now):
        time.sleep(3)
        return [{"run_id": run_id, "evaluation_id": evaluation_id, "failure_ref": "x", "order_no": 1,
                 "state": "CONFIRMED", "cause_code": "NOT_RUN", "evaluated_at": now, "run_date": run_date}]

    monkeypatch.setattr(runner, "explain", slow_explain)
    tally = Investigations()
    lines = runner.explain_failures(spark, config, evaluation, rows, run, tally, 0.01)
    assert lines == ["  cause investigation exceeded the compute budget (0.01 minutes); the next run retries it"]
    time.sleep(6)  # the cancelled investigation finishes in the background and stores its row
    assert spark.table(f"{replay.dq}.dq_cause_result").count() == 1
    assert spark.table(f"{replay.dq}.dq_commit").where("kind = 'cause'").count() == 0
    assert spark.table(f"{replay.dq}.v_committed_cause").count() == 0
    assert (tally.expected, tally.done) == (1, 0)


def test_f01_a_cancelled_evidence_write_that_lands_late_stays_invisible(spark, tmp_path, monkeypatch) -> None:
    """F01, late thread: the key-event write runs past its bound, the marker is written and published
    with no evidence vouched for, and the write then lands in the background. Its rows stay invisible."""
    from dataclasses import replace

    from hcsc.datalake.dre import runner
    from hcsc.datalake.dre.checks.base import from_counts
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.checks.hop.key_currency import KeyCurrency
    from hcsc.datalake.dre.config.loader import load
    from hcsc.datalake.dre.runner import Evaluation, Planned, Publisher, result_rows, write_evaluation
    from hcsc.datalake.dre.store import runs
    from tests.replay.conftest import replay_conf

    replay = replay_conf(spark, tmp_path, "late_evidence")
    config, _ = load(replay.conf)
    gold = config.datasets["gold_member_coverage"]
    planned = Planned(config.feeds["example_realtime"], gold, KeyCurrency())
    at = datetime(2026, 2, 4, 12, tzinfo=UTC)
    event = Event(gold.dataset, at - timedelta(hours=1), at)
    result = replace(from_counts(3, 1, "1 of 3 keys missing", "every key present",
                                 group_values={"upstream": "example_curated_enrollment"}), key_events=object())
    run = runs.new_run(now=at)
    evaluation = Evaluation(planned, event, [result], at, 10)
    rows = result_rows(evaluation, run, "NORMAL")

    def slow_write(spark_, db, evaluation_, rows_, run_):
        time.sleep(3)
        append_rows(spark_, db, "dq_key_event", key_event_rows(run_, rows_[0]["evaluation_id"], 1, "FLAGGED"),
                    commit=False)
        return {rows_[0]["evaluation_id"]: (1, 0)}

    monkeypatch.setattr(runner, "write_key_events", slow_write)
    stored, complete, commits = write_evaluation(spark, replay.dq, evaluation, rows, run, budget_minutes=0.01)
    publisher = Publisher(spark, replay.dq, run, retention=RETENTION)
    publisher.add(commits, complete)
    publisher.flush()
    time.sleep(6)  # the late write lands
    assert spark.table(f"{replay.dq}.dq_key_event").count() == 1
    assert spark.table(f"{replay.dq}.v_committed_key_event").count() == 0
    [current] = [r for r in spark.table(f"{replay.dq}.v_latest_result").collect() if r.check_id == "HOP_KEY_CURRENCY"]
    assert (current.reason_code, current.healthy) == ("evidence_incomplete", False)


def test_f02_evidence_lost_after_commit_keeps_the_result_and_marks_it_invalid(spark, store) -> None:
    """F02: a unit's key events disappear after publication (files removed outside DRE). The sweep
    records the loss; the result stays visible, INVALID, and is not healthy."""
    run = new_run(5)
    publish(spark, store, run, "f02", key_events=3, state="FAILED")
    spark.sql(f"ALTER TABLE {store}.dq_key_event DROP PARTITION (run_date = DATE'{run.run_date.isoformat()}')")
    sweep(spark, store, run)
    [found] = [r for r in records(spark, store, "EVIDENCE_MISMATCH") if r.evaluation_id == "f02"]
    assert found.detail == "key events: 3 declared, 0 stored"
    row = latest(spark, store, "f02")
    assert (row.state, row.evidence_state) == ("FAILED", "INVALID")  # a published FAILED never disappears


def test_f09_commit_metadata_outliving_its_evidence_is_expired_not_lost(spark, store) -> None:
    """F09: a commit's evidence is past evidence_kept_until (retention dropped it, as documented). It is
    EXPIRED, not INVALID: the sweep records nothing, and the result is still not healthy."""
    from hcsc.datalake.dre.store.commits import Commit, append_commits

    run = new_run(6)
    append_rows(spark, store, "dq_check_result", [result_row(run, "f09", check_id="T1_PARTITION_DRIFT",
                                                             dataset="provider_directory")], commit=False)
    append_commits(spark, store, [Commit(run.run_id, "f09", "provider_directory", "T1_PARTITION_DRIFT", 1,
                                         partition_state_rows=4, evidence_kept_until=date(2026, 1, 31))],
                   run.started_at, run.run_date)  # the partition states are gone
    sweep(spark, store, run)
    assert not [r for r in records(spark, store, "EVIDENCE_MISMATCH") if r.evaluation_id == "f09"]
    assert (latest(spark, store, "f09").evidence_state, healthy(spark, store, run, "f09")) == ("EXPIRED", False)
    [audit] = spark.table(f"{store}.v_commit_integrity").where("evaluation_id = 'f09'").collect()
    assert (audit.partition_state_rows, audit.stored_partition_state_rows, audit.evidence_state) == (4, 0, "EXPIRED")


def test_a_failed_sweep_is_unknown_not_lost_until_a_sweep_completes(spark, store) -> None:
    """A transient verification failure (the sweep cannot read the store) is recorded as a FAILED
    sweep: what it covers is UNKNOWN, not INVALID, and not healthy. A later completed sweep makes it
    VALID again."""
    from hcsc.datalake.dre.store import integrity

    run = new_run(7)
    publish(spark, store, run, "unk")

    class Unreadable:
        def __init__(self, real):
            self.real = real

        def sql(self, text):
            if "v_commit_integrity" in text:
                raise OSError("synthetic: the store cannot be read")
            return self.real.sql(text)

        def __getattr__(self, name):
            return getattr(self.real, name)

    with pytest.raises(OSError):
        integrity.sweep(Unreadable(spark), store, run, integrity.DAILY, run.run_date, run.run_date, run.run_date)
    assert [r.status for r in records(spark, store, "SWEEP") if r.run_id == run.run_id] == ["FAILED"]
    assert (latest(spark, store, "unk").evidence_state, healthy(spark, store, run, "unk")) == ("UNKNOWN", False)
    time.sleep(1)  # the next sweep is recorded later
    sweep(spark, store, run)
    assert (latest(spark, store, "unk").evidence_state, healthy(spark, store, run, "unk")) == ("VALID", True)


def test_retention_keeps_promised_and_held_partitions(spark, store) -> None:
    """Retention deadlines and investigation holds: an expired partition is kept while a commit in it
    promised its rows longer (even after retention_months was shortened), or while a hold covers it."""
    from hcsc.datalake.dre.config.models import RetentionHold
    from hcsc.datalake.dre.store.commits import Commit, append_commits
    from hcsc.datalake.dre.store.retention import droppable

    old, promised, held = date(2023, 1, 10), date(2023, 1, 11), date(2023, 1, 12)
    for day, kept_until in ((old, date(2024, 2, 10)), (promised, date(2027, 1, 1)), (held, date(2024, 2, 12))):
        append_rows(spark, store, "dq_partition_state", [{"dataset": "provider_directory", "partition_value": "p",
                                                         "run_id": f"ret-{day.day}", "run_date": day}], commit=False)
        append_commits(spark, store, [Commit(f"ret-{day.day}", f"ret-{day.day}", "provider_directory",
                                             "T1_PARTITION_DRIFT", 1, partition_state_rows=1,
                                             evidence_kept_until=kept_until)], datetime(2023, 1, 1, tzinfo=UTC), day)
    hold = RetentionHold.model_validate({"from": held, "to": held, "reason": "incident review"})
    dates, kept = droppable(spark, store, "dq_partition_state", 13, date(2026, 2, 8), [hold])
    assert old in dates and promised not in dates and held not in dates
    assert kept[promised] == "evidence promised until 2027-01-01"
    assert kept[held] == "investigation hold (incident review)"
    ended = RetentionHold.model_validate({"from": held, "to": held, "reason": "done", "until": date(2025, 1, 1)})
    assert held in droppable(spark, store, "dq_partition_state", 13, date(2026, 2, 8), [ended])[0]


def test_a_commit_records_how_long_its_evidence_is_kept(spark, store) -> None:
    """The lifetime is run_date plus the shortest retention of the tables holding what it vouches for:
    13 months with partition states, 84 for results, key events and causes."""
    from hcsc.datalake.dre.store.commits import CAUSE, Commit

    day = date(2026, 2, 9)
    assert Commit("r", "e", "d", "c", 1).with_lifetime(day, RETENTION).evidence_kept_until == date(2033, 2, 9)
    assert Commit("r", "e", "d", "c", 1, partition_state_rows=2).with_lifetime(day, RETENTION).evidence_kept_until == \
        date(2027, 3, 9)
    assert Commit("r", "e", "d", "c", 0, kind=CAUSE, cause_rows=1).with_lifetime(
        date(2026, 1, 31), {**RETENTION, "dq_cause_result": 1}).evidence_kept_until == date(2026, 2, 28)


# --- compromised baselines: no check passes on compromised evidence ---


def test_compromised_baselines_until_rebaselined(spark, store) -> None:
    """An INVALID unit that carried key events compromises its hop until a later evaluation records
    REBASELINED for it; an unreadable store compromises every baseline check (fail closed)."""
    from hcsc.datalake.dre.store import integrity

    run = new_run(10)
    publish(spark, store, run, "cmp", key_events=1)
    integrity.append_records(spark, store, [integrity.record(
        run, integrity.MISMATCH, subject_run_id=run.run_id, subject_run_date=run.run_date, evaluation_id="cmp",
        commit_kind="result", dataset="gold_member_coverage", check_id="HOP_KEY_CURRENCY", detail="synthetic")])
    unit = (run.run_id, run.run_date, "cmp")
    found = integrity.compromised_baselines(spark, store)
    assert unit in found.of("gold_member_coverage", "HOP_KEY_CURRENCY")  # with the INVALID units of F01 and F02
    assert found.of("gold_member_coverage", "T1_ON_TIME") is None
    integrity.append_records(spark, store, integrity.rebaseline_records(
        new_run(11), "gold_member_coverage", "HOP_KEY_CURRENCY", [unit]))
    assert unit not in (integrity.compromised_baselines(spark, store).of("gold_member_coverage", "HOP_KEY_CURRENCY") or [])
    assert integrity.compromised_baselines(spark, "dq_does_not_exist").of("any", "T1_PARTITION_DRIFT") == []


def test_partition_drift_cannot_pass_on_a_compromised_baseline() -> None:
    """A PASSED T1_PARTITION_DRIFT on a compromised baseline is DID_NOT_RUN / PLATFORM /
    evidence_invalid (it holds the window); a FAILED stands; the partition states are kept, and
    once published they re-establish the baseline."""
    from types import SimpleNamespace

    from hcsc.datalake.dre.checks.base import from_counts
    from hcsc.datalake.dre.runner import Evaluation, rebaselines, without_pass

    passed = from_counts(5, 0, "0 of 5 partitions changed", "no partition changed")
    failed = from_counts(5, 1, "1 of 5 partitions changed", "no partition changed")
    planned = SimpleNamespace(check=SimpleNamespace(check_id="T1_PARTITION_DRIFT"),
                              dataset=SimpleNamespace(dataset="provider_directory"))
    from dataclasses import replace

    evaluation = Evaluation(planned, None, [replace(passed, partition_state=("s",)), failed], None, 0)
    out = without_pass(evaluation).results
    assert [(r.state, r.reason_category, r.reason_code) for r in out] == [
        ("DID_NOT_RUN", "PLATFORM", "evidence_invalid"), ("FAILED", None, None)]
    assert out[0].partition_state == ("s",)
    run = new_run(12)
    unit = [("r-old", date(2026, 2, 1), "e-old")]
    assert [r["record"] for r in rebaselines(run, without_pass(evaluation), unit, 1)] == ["REBASELINED"]
    assert rebaselines(run, without_pass(evaluation), unit, 0) == []  # not stored whole: not re-established


def test_a_compromised_hop_runs_as_a_full_sweep_and_is_rebaselined(spark, tmp_path, monkeypatch) -> None:
    """HOP_KEY_CURRENCY on a compromised baseline judges every key (a full sweep), so its result does
    not rest on the lost open keys; once it is published whole, REBASELINED is recorded."""
    from hcsc.datalake.dre import runner
    from hcsc.datalake.dre.cli import main
    from hcsc.datalake.dre.store import integrity
    from tests.test_commit_protocol import stale_gold

    replay = stale_gold(spark, tmp_path, "compromised_hop")
    unit = ("0" * 8 + "-0000-0000-0000-000000000001", date(2026, 1, 1), "lost-unit")
    monkeypatch.setattr(runner.integrity, "compromised_baselines", lambda spark_, db: integrity.Compromised(
        {("gold_member_coverage", "HOP_KEY_CURRENCY"): [unit]}))
    sweeps: dict[str, bool] = {}
    real = runner.evaluate

    def spy(spark_, config, planned, event, *args, **kwargs):
        evaluation = real(spark_, config, planned, event, *args, **kwargs)
        sweeps[f"{planned.dataset.dataset} {planned.check.check_id}"] = evaluation.ctx.full_sweep
        return evaluation

    monkeypatch.setattr(runner, "evaluate", spy)
    monkeypatch.setattr(runner, "is_full_sweep", lambda *args: False)  # not the weekly full sweep, whatever the day
    assert main(["run", "--conf", str(replay.conf)]) == 0
    assert sweeps["gold_member_coverage HOP_KEY_CURRENCY"] is True
    assert sweeps["gold_member_coverage HOP_VALUE_AGREEMENT"] is False  # only the check that reads open keys
    rebased = records(spark, replay.dq, "REBASELINED")
    assert [(r.subject_run_id, r.evaluation_id, r.check_id) for r in rebased] == [
        (unit[0], "lost-unit", "HOP_KEY_CURRENCY")]


# --- R2: commit repair ---


def two_units(spark, db, run, prefix: str):
    """Two units of the run, stored uncommitted: one evaluation with two groups, and one with one."""
    from hcsc.datalake.dre.runner import Publisher
    from hcsc.datalake.dre.store.commits import Commit

    ids = [f"{prefix}{n}" for n in (1, 2, 3)]
    append_rows(spark, db, "dq_check_result", [result_row(run, i) for i in ids], commit=False)
    publisher = Publisher(spark, db, run, retention=RETENTION)
    publisher.add([Commit(run.run_id, ids[0], "d", "c", 1), Commit(run.run_id, ids[1], "d", "c", 1)], 1)
    publisher.add([Commit(run.run_id, ids[2], "d", "c", 1)], 1)
    return publisher


def test_f04_a_repair_whose_readback_fails_is_uncertain_and_not_published(spark, store, monkeypatch) -> None:
    """F04: the write fails storing nothing, the repair append succeeds, then the readback fails. The
    outcome is uncertain: nothing is counted (fail closed), even though the commits did land."""
    from hcsc.datalake.dre import runner

    run = new_run(13)
    real_append, real_read = runner.append_commits, runner.stored_commits
    appends, reads = [], []

    def fails_first(*args):
        appends.append(1)
        if len(appends) == 1:
            raise OSError("synthetic commit failure, nothing stored")
        return real_append(*args)

    def second_read_fails(*args):
        reads.append(1)
        if len(reads) == 2:
            raise OSError("synthetic: the readback cannot be done")
        return real_read(*args)

    monkeypatch.setattr(runner, "append_commits", fails_first)
    monkeypatch.setattr(runner, "stored_commits", second_read_fails)
    publisher = two_units(spark, store, run, "f04-")
    publisher.flush()
    assert (publisher.published, publisher.unconfirmed) == (0, 2)
    assert spark.table(f"{store}.dq_commit").where(f"run_id = '{run.run_id}'").count() == 3  # stored, not asserted


def test_identical_duplicate_commits_are_confirmed(spark, store, monkeypatch) -> None:
    """The write stores every row twice, then raises: identical duplicates confirm the unit."""
    from hcsc.datalake.dre import runner

    run = new_run(14)
    real = runner.append_commits
    calls = []

    def twice_then_raise(spark_, db, commits, at, run_date):
        calls.append(len(commits))
        real(spark_, db, commits, at, run_date)
        real(spark_, db, commits, at, run_date)
        raise OSError("synthetic failure after the batch was stored twice")

    monkeypatch.setattr(runner, "append_commits", twice_then_raise)
    publisher = two_units(spark, store, run, "dup-")
    publisher.flush()
    assert (publisher.published, publisher.unconfirmed, calls) == (2, 0, [3])
    assert {r.evidence_state for r in spark.table(f"{store}.v_committed_result")
            .where(f"run_id = '{run.run_id}'").collect()} == {"VALID"}


def test_a_conflicting_commit_fails_closed_and_is_recorded(spark, store, monkeypatch) -> None:
    """The store holds one identity with other contents than the run's commit (a conflict). That unit
    is not published, the conflict is recorded as an integrity incident, and the identity is INVALID
    in every view; the other unit is confirmed."""
    from dataclasses import replace

    from hcsc.datalake.dre import runner

    run = new_run(15)
    real = runner.append_commits

    def conflicting_then_raise(spark_, db, commits, at, run_date):
        real(spark_, db, [replace(c, key_event_rows=7) if c.evaluation_id == "cf-3" else c for c in commits], at, run_date)
        raise OSError("synthetic failure")

    monkeypatch.setattr(runner, "append_commits", conflicting_then_raise)
    publisher = two_units(spark, store, run, "cf-")
    publisher.flush()
    assert (publisher.published, publisher.unconfirmed) == (1, 1)
    [conflict] = [r for r in records(spark, store, "COMMIT_CONFLICT") if r.subject_run_id == run.run_id]
    assert conflict.evaluation_id == "cf-3"
    monkeypatch.undo()
    from hcsc.datalake.dre.store.commits import Commit

    real(spark, store, [Commit(run.run_id, "cf-3", "d", "c", 1).with_lifetime(run.run_date, RETENTION)],
         run.started_at, run.run_date)  # even if the run's own row lands later, the identity stays INVALID
    assert latest(spark, store, "cf-3").evidence_state == "INVALID"
    assert healthy(spark, store, run, "cf-3") is False


def test_f05_f06_one_current_result_when_runs_publish_the_same_evaluation(spark, store) -> None:
    """F05 and F06: a retry, or a second publisher racing the first, publishes the same deterministic
    evaluation id under its own run. Both commits stand; v_latest_result gives exactly one current
    result: the later evaluation, then the higher run id on a tie."""
    first, second = new_run(16, 10), new_run(16, 11)
    for run, state in ((first, "FAILED"), (second, "PASSED")):
        publish(spark, store, run, "same-id", check_id="T1_KEY_NULLS", state=state,
                evaluated_at=datetime(2026, 2, 16, 12, tzinfo=UTC))
    rows = spark.table(f"{store}.v_latest_result").where("check_id = 'T1_KEY_NULLS'").collect()
    assert [(r.run_id, r.state) for r in rows] == [(max(first.run_id, second.run_id),
                                                    "FAILED" if first.run_id > second.run_id else "PASSED")]


def test_normal_rerun_and_replay_precedence(spark, store) -> None:
    """A RERUN of the current window replaces it; a REPLAY or RERUN of an older window never becomes
    the current result, however late it is evaluated."""
    def at(day, hour):
        return datetime(2026, 2, day, hour, tzinfo=UTC)

    cases = [("w1", "NORMAL", at(1, 0), at(2, 0), at(2, 1), "PASSED"),
             ("w2", "NORMAL", at(2, 0), at(3, 0), at(3, 1), "FAILED"),
             ("w1-replay", "REPLAY", at(1, 0), at(2, 0), at(20, 1), "FAILED"),
             ("w2-rerun", "RERUN", at(2, 0), at(3, 0), at(19, 1), "PASSED")]
    for i, (eid, kind, start, end, evaluated, state) in enumerate(cases):
        run = new_run(17, i)
        publish(spark, store, run, eid, check_id="T1_VOLUME", state=state, execution_type=kind,
                window_start=start, window_end=end, evaluated_at=evaluated)
    [current] = spark.table(f"{store}.v_latest_result").where("check_id = 'T1_VOLUME'").collect()
    assert (current.evaluation_id, current.execution_type, current.healthy) == ("w2-rerun", "RERUN", True)


# --- R3: cause association ---


def test_f07_f08_causes_attach_by_run_run_date_and_evaluation_id(spark, store) -> None:
    """F07: a cause written on date B for a result committed on date A is shown. F08: a cause naming a
    result that was never committed, one naming the same run and evaluation id on another date (a
    collision), and one naming an unrelated result are not; nor is an INVALID cause set."""
    from hcsc.datalake.dre.store import integrity

    day_a, day_b = date(2026, 2, 18), date(2026, 2, 19)
    base = {"dataset": "gold_member_coverage", "check_id": "T1_ZERO_ROWS", "state": "FAILED"}
    append_rows(spark, store, "dq_check_result", [{**base, "evaluation_id": "c1", "run_id": "ra", "run_date": day_a},
                                                  {**base, "evaluation_id": "c2", "run_id": "ra", "run_date": day_a}])
    append_rows(spark, store, "dq_check_result", [{**base, "evaluation_id": "c3", "run_id": "rx", "run_date": day_a}],
                commit=False)
    cause = {"cause_code": "NO_UPSTREAM_DATA", "state": "CONFIRMED", "run_date": day_b}
    append_rows(spark, store, "dq_cause_result", [
        {**cause, "run_id": "rb1", "evaluation_id": "c1", "result_run_id": "ra", "result_run_date": day_a},  # F07
        {**cause, "run_id": "rb2", "evaluation_id": "c1", "result_run_id": "ra", "result_run_date": day_b},  # collision
        {**cause, "run_id": "rb3", "evaluation_id": "c3", "result_run_id": "rx", "result_run_date": day_a},  # orphan
        {**cause, "run_id": "rb4", "evaluation_id": "c9", "result_run_id": "ra", "result_run_date": day_a},  # unrelated
        {**cause, "run_id": "rb5", "evaluation_id": "c2", "result_run_id": "ra", "result_run_date": day_a},  # INVALID below
    ])
    integrity.append_records(spark, store, [integrity.record(
        new_run(19), integrity.MISMATCH, subject_run_id="rb5", subject_run_date=day_b, evaluation_id="c2",
        commit_kind="cause", detail="synthetic")])
    shown = sorted((r.run_id, r.evaluation_id, r.result_run_id, str(r.result_run_date))
                   for r in spark.table(f"{store}.v_committed_cause").where(f"run_date = DATE'{day_b}'").collect())
    assert shown == [("rb1", "c1", "ra", "2026-02-18")]


# --- reporting: never healthy, and integrity freshness ---


def test_the_digest_never_counts_unverified_evidence_as_healthy(config_for_digest) -> None:
    """A current result whose evidence is INVALID or UNKNOWN is listed apart, not counted as passing,
    and keeps the digest from being all clear; the integrity line is carried."""
    from hcsc.datalake.dre.notify.digest import build_digests
    from hcsc.datalake.dre.store.runs import Run

    run = Run("00000000-0000-0000-0000-00000000000a", datetime(2026, 2, 20, 12, tzinfo=UTC), "0.1.0", None)
    row = {"evaluation_id": "e", "run_id": run.run_id, "dataset": "gold_member_coverage", "check_id": "T1_ZERO_ROWS",
           "groups": "", "state": "PASSED", "feed": "example_realtime", "evidence_state": "INVALID",
           "reason_category": None, "reason_code": None, "violations": 0}
    other = {**row, "evaluation_id": "o", "check_id": "T1_VOLUME", "run_id": "earlier", "evidence_state": "UNKNOWN"}
    [digest] = build_digests(config_for_digest, run, {"current": [row], "history": {}, "previous_groups": {},
                                                      "causes": [], "samples": {}, "not_healthy": [other],
                                                      "integrity": "store integrity: last verified ..."})
    assert digest.passing == 0 and not digest.all_clear
    assert [(i.check_id, i.reason) for i in digest.evidence] == [("T1_ZERO_ROWS", "INVALID"), ("T1_VOLUME", "UNKNOWN")]
    text = digest.text()
    assert "Evidence not verified (not counted as healthy)" in text and "store integrity: last verified" in text


@pytest.fixture(scope="module")
def config_for_digest():
    from pathlib import Path

    from hcsc.datalake.dre.config.loader import load

    loaded, _ = load(Path(__file__).resolve().parents[1] / "conf")
    return loaded


def test_the_watchdog_reports_integrity_freshness_and_incidents(spark) -> None:
    from hcsc.datalake.dre.store import integrity
    from hcsc.datalake.dre.store.local_setup import create_store
    from hcsc.datalake.dre.watchdog import integrity_problems

    store = "dq_watchdog_integrity"
    create_store(spark, store)
    now = datetime(2026, 2, 21, 12, 40, tzinfo=UTC)
    append_rows(spark, store, "dq_run", [{"run_id": "old-run", "started_at": now - timedelta(days=3),
                                          "status": "COMPLETED", "run_date": date(2026, 2, 18)}])
    run = new_run(21, 12)
    # an incident recorded in the judged hour (12:00-13:00: 12:40 less the 30-minute grace, floored)
    integrity.append_records(spark, store, [{**integrity.record(run, integrity.MISMATCH, evaluation_id="w"),
                                             "recorded_at": datetime(2026, 2, 21, 12, 15, tzinfo=UTC)}])
    found = integrity_problems(spark, store, now, 30)
    assert "1 integrity incident(s) recorded in 2026-02-21 12:00-13:00 UTC (dq_integrity: evidence changed or lost " \
           "after publication, or a conflicting commit)" in found
    assert "store integrity not verified: no integrity sweep has completed" in found
    integrity.append_records(spark, store, [{**integrity.record(run, integrity.SWEEP, status=integrity.COMPLETED,
                                                                scope=integrity.DAILY),
                                             "recorded_at": now - timedelta(hours=1)}])
    assert not [p for p in integrity_problems(spark, store, now, 30) if "stale" in p or "not verified" in p]
