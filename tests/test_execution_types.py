"""NORMAL, RERUN and REPLAY (review item 7): NORMAL takes each dataset's next eligible window;
RERUN names one historical window; REPLAY covers a historical range. RERUN and REPLAY are
idempotent: an identical result is not written again, and they never read or change the
open-key state, the dq_file registry or a NORMAL window."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from hcsc.datalake.dre.checks.events import Event, previous_window_end, replay_windows
from hcsc.datalake.dre.cli import main
from hcsc.datalake.dre.config.models import Dataset, TimeColumn
from hcsc.datalake.dre.runner import Selection
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.replay.conftest import latest, replay_conf
from tests.replay.test_r04_to_r07_hops import key_events, setup

UTC = timezone.utc
NOW = datetime(2026, 1, 20, 12, tzinfo=UTC)


def at(hour: int) -> datetime:
    return datetime(2026, 1, 15, hour, tzinfo=UTC)


def test_each_execution_type_names_its_own_window() -> None:
    assert Selection().problem(NOW) is None
    assert "takes no window" in Selection("NORMAL", at(1), at(2)).problem(NOW)
    assert Selection("RERUN", at(1), at(2)).problem(NOW) is None
    assert "needs --window-start and --window-end" in Selection("RERUN", at(1), None).problem(NOW)
    assert "needs --from and --to" in Selection("REPLAY").problem(NOW)
    assert "start must be before the end" in Selection("REPLAY", at(2), at(1)).problem(NOW)
    assert "historical" in Selection("RERUN", NOW, NOW + timedelta(hours=1)).problem(NOW)


def test_a_replay_keeps_recorded_windows_and_fills_the_spans_between() -> None:
    ds = Dataset(dataset="gold", table="gold_db.member_coverage", layer="GOLD", key=["sub_id"],
                 load_time=TimeColumn(column="gld_lcts", granularity="minute", timezone="UTC"))
    recorded = [(at(2), at(3)), (at(2), at(4)),   # held at [2, 3), evaluated again as [2, 4): replayed once
                (at(4), at(5)), (at(7), at(8))]
    windows = [(e.window_start.hour, e.window_end.hour) for e in replay_windows(ds, at(1), at(9), recorded, "UTC")]
    assert windows == [(1, 2), (2, 4), (4, 5), (5, 7), (7, 8), (8, 9)]
    # A recorded window keeps its bounds, so its event (and evaluation) ids are the NORMAL run's.
    assert replay_windows(ds, at(2), at(3), [(at(2), at(3))], "UTC") == [Event("gold", at(2), at(3))]


def rows(spark, replay) -> list:
    return spark.table(f"{replay.dq}.dq_check_result").collect()


def run_types(spark, replay) -> dict[str, str]:
    return {r.execution_type: r.status for r in spark.table(f"{replay.dq}.v_latest_run").collect()}


def test_a_rerun_re_evaluates_a_historical_window_idempotently(spark, tmp_path) -> None:
    # R05: gold holds an older version of the key. (A RERUN names one window for every dataset, and
    # the named bounds match a dataset's NORMAL window only at that dataset's granularity: the
    # sample gives curated and gold the same, minute.)
    replay = setup(spark, tmp_path, "rerun")
    opened, terminated = curated_load_time(30), curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(updated=opened), curated_row(end="2026-06-30", updated=terminated)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(record=opened, loaded=gold_load_time(29))])
    assert main(["run", "--conf", str(replay.conf)]) == 0
    [normal] = latest(spark, replay)[("gold_member_coverage", "HOP_KEY_CURRENCY")]
    assert normal.state == "FAILED"
    flagged = key_events(spark, replay)
    files = spark.table(f"{replay.dq}.dq_file").count()
    window_end = previous_window_end(spark, replay.dq, "gold_member_coverage")
    rerun = ["run", "--conf", str(replay.conf), "--execution-type", "RERUN",
             "--window-start", normal.window_start.isoformat(), "--window-end", normal.window_end.isoformat()]

    # Nothing changed since: the same window gives the same evaluations and outcomes, so nothing new.
    before = len(rows(spark, replay))
    assert main(rerun) == 0
    assert len(rows(spark, replay)) == before
    assert main(rerun) == 0  # and again
    assert len(rows(spark, replay)) == before
    assert run_types(spark, replay) == {"NORMAL": "COMPLETED", "RERUN": "COMPLETED"}

    # Gold catches up; a rerun of the window records the new outcome under the same evaluation.
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(record=terminated, end="2026-06-30",
                                                             loaded=gold_load_time(29))])
    assert main(rerun) == 0
    currency = [r for r in rows(spark, replay)
                if r.check_id == "HOP_KEY_CURRENCY" and r.dataset == "gold_member_coverage"]
    assert sorted((r.execution_type, r.state) for r in currency) == [("NORMAL", "FAILED"), ("RERUN", "PASSED")]
    assert len({r.evaluation_id for r in currency}) == 1
    [now] = latest(spark, replay)[("gold_member_coverage", "HOP_KEY_CURRENCY")]
    assert (now.execution_type, now.state) == ("RERUN", "PASSED")  # the current window, evaluated again
    after = len(rows(spark, replay))
    assert main(rerun) == 0
    assert len(rows(spark, replay)) == after  # idempotent again

    # The open-key state, the file registry and the NORMAL window are untouched.
    assert key_events(spark, replay) == flagged
    assert spark.table(f"{replay.dq}.v_open_keys").count() == 1
    assert spark.table(f"{replay.dq}.dq_file").count() == files
    assert previous_window_end(spark, replay.dq, "gold_member_coverage") == window_end


def test_a_rerun_of_an_older_window_does_not_become_the_current_result(spark, tmp_path) -> None:
    replay = replay_conf(spark, tmp_path, "rerun_old")
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(), gold_row()])  # a duplicated key
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row()])
    assert main(["run", "--conf", str(replay.conf)]) == 0
    [normal] = latest(spark, replay)[("gold_member_coverage", "T1_KEY_DUPLICATES")]
    older = ["run", "--conf", str(replay.conf), "--execution-type", "RERUN",
             "--window-start", (normal.window_start - timedelta(days=2)).isoformat(),
             "--window-end", (normal.window_start - timedelta(days=1)).isoformat()]
    assert main(older) == 0
    reruns = [r for r in rows(spark, replay) if r.execution_type == "RERUN"]
    assert reruns and all(r.window_end <= normal.window_start for r in reruns)
    [current] = latest(spark, replay)[("gold_member_coverage", "T1_KEY_DUPLICATES")]
    assert current.evaluation_id == normal.evaluation_id


def test_a_replay_covers_a_historical_range_idempotently(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "replay_range")
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(), gold_row()])
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row()])
    assert main(["run", "--conf", str(replay.conf)]) == 0
    [normal] = latest(spark, replay)[("gold_member_coverage", "T1_KEY_DUPLICATES")]
    window_end = previous_window_end(spark, replay.dq, "gold_member_coverage")
    start = normal.window_start - timedelta(hours=6)
    command = ["run", "--conf", str(replay.conf), "--execution-type", "REPLAY",
               "--from", start.isoformat(), "--to", normal.window_end.isoformat()]
    assert main(command) == 0
    replayed = {(r.window_start, r.window_end) for r in rows(spark, replay)
                if r.execution_type == "REPLAY" and r.dataset == "gold_member_coverage"}
    # The span before the NORMAL window is new; the NORMAL window itself gave the same outcomes.
    assert (start, normal.window_start) in replayed
    assert "unchanged since its stored result; not written again" in capsys.readouterr().out
    count = len(rows(spark, replay))
    assert main(command) == 0
    assert len(rows(spark, replay)) == count  # a second replay of the range writes nothing
    assert previous_window_end(spark, replay.dq, "gold_member_coverage") == window_end
    assert main(["run", "--conf", str(replay.conf), "--execution-type", "REPLAY"]) == 3  # needs the range
    assert main(["run", "--conf", str(replay.conf), "--execution-type", "RERUN",
                 "--from", start.isoformat(), "--to", normal.window_end.isoformat()]) == 3


def test_stored_outcomes_are_read_by_dataset_and_span(spark) -> None:
    """RERUN and REPLAY compare with the stored outcomes read by dataset and the span the events
    cover (one predicate however many events), kept for the jobs' event ids only."""
    from datetime import datetime, timezone

    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.checks.tier1.key_nulls import KeyNulls
    from hcsc.datalake.dre.config.models import Dataset
    from hcsc.datalake.dre.runner import Planned, stored_outcomes
    from hcsc.datalake.dre.store.local_setup import create_store
    from tests.store.conftest import append_rows

    create_store(spark, "dq_outcomes")
    at = datetime(2026, 1, 15, 12, tzinfo=timezone.utc)
    inside = Event("gold", at, at + timedelta(hours=1))
    other = Event("other", at, at + timedelta(hours=1))
    outside = Event("gold", at + timedelta(days=2), at + timedelta(days=2, hours=1))
    append_rows(spark, "dq_outcomes", "dq_check_result", [
        {"evaluation_id": "in", "run_id": "r1", "event_id": inside.event_id, "dataset": "gold", "check_id": "T1_KEY_NULLS",
         "window_start": inside.window_start, "window_end": inside.window_end, "state": "PASSED", "evaluated_at": at,
         "run_date": at.date()},
        {"evaluation_id": "in", "run_id": "r2", "event_id": inside.event_id, "dataset": "gold", "check_id": "T1_KEY_NULLS",
         "window_start": inside.window_start, "window_end": inside.window_end, "state": "FAILED", "evaluated_at": at + timedelta(hours=2),
         "run_date": at.date()},
        {"evaluation_id": "other", "run_id": "r1", "event_id": other.event_id, "dataset": "other", "check_id": "T1_KEY_NULLS",
         "window_start": other.window_start, "window_end": other.window_end, "state": "PASSED", "evaluated_at": at,
         "run_date": at.date()},
        {"evaluation_id": "out", "run_id": "r1", "event_id": outside.event_id, "dataset": "gold", "check_id": "T1_KEY_NULLS",
         "window_start": outside.window_start, "window_end": outside.window_end, "state": "PASSED", "evaluated_at": at,
         "run_date": at.date()}])
    gold = Dataset(dataset="gold", table="x.gold", layer="GOLD", key=["k"])
    jobs = [(Planned(None, gold, KeyNulls()), inside)]
    found = stored_outcomes(spark, "dq_outcomes", jobs)
    assert set(found) == {"in"} and found["in"][0] == "FAILED"  # the latest row; other datasets and spans left out
    assert stored_outcomes(spark, "dq_outcomes", []) == {}
