"""dre trace beyond R05 (tests/replay): a missing key, no gap, no secret, and bad input."""

from __future__ import annotations

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_row
from tests.replay.conftest import replay_conf

KEY = ["--dataset", "gold_member_coverage", "--key", "000123401,01,2026-01-01,AGR-A"]


def trace(replay, capsys, key=KEY) -> tuple[int, list[str]]:
    capsys.readouterr()
    code = main(["trace", "--conf", str(replay.conf), *key])
    return code, capsys.readouterr().out.splitlines()


def test_a_key_missing_downstream_and_no_secret(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "trace_missing")
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(updated=curated_load_time(12))])
    create_table(spark, replay.gold, GOLD_COLUMNS)
    code, lines = trace(replay, capsys)
    assert code == 0
    assert lines[1] == f"gold_member_coverage ({replay.gold}): absent"
    assert lines[2:] == [
        "first gap: example_curated_enrollment -> gold_member_coverage: gold_member_coverage does not have the key "
        "(MISSING)",
        "cause: not available (hmac_secret_file is not set, so causes are not kept per key)"]


def test_no_gap_when_every_layer_has_the_latest_version(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "trace_current")
    at = curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(updated=at)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(record=at)])
    code, lines = trace(replay, capsys)
    assert code == 0 and lines[-1] == "no gap: every layer that has the key holds its latest version"
    assert len(lines) == 3  # curated, gold, the verdict


def test_bad_input(spark, tmp_path, capsys) -> None:
    replay = replay_conf(spark, tmp_path, "trace_bad")
    code, lines = trace(replay, capsys, ["--dataset", "gold_member_coverage", "--key", "000123401,01"])
    assert code == 3 and "4-part key" in lines[0]
    code, lines = trace(replay, capsys, ["--dataset", "nope", "--key", "x"])
    assert code == 3 and lines == ["dre trace: unknown dataset 'nope'"]


def test_the_cause_line_is_the_gap_hop_s_own(spark, tmp_path, capsys) -> None:
    """Review finding 15: a cause stored for the same key at another hop is not this hop's cause."""
    from datetime import date, datetime

    from hcsc.datalake.dre.checks.hop.common import key_hash
    from tests.store.conftest import append_rows

    secret = tmp_path / "hmac.secret"
    secret.write_bytes(b"synthetic-trace-secret\n")
    replay = replay_conf(spark, tmp_path, "trace_hop", {
        "defaults.yaml": [("hmac_secret_file: null", f"hmac_secret_file: {secret}")]})
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(updated=curated_load_time(12))])
    create_table(spark, replay.gold, GOLD_COLUMNS)
    ref = key_hash(b"synthetic-trace-secret", "123401|01|2026-01-01|AGR-A")
    day = date(2026, 1, 15)
    append_rows(spark, replay.dq, "dq_check_result", [{
        "evaluation_id": "e1", "run_id": "r1", "dataset": "gold_member_coverage", "check_id": "HOP_KEY_CURRENCY",
        "state": "FAILED", "evaluated_at": datetime(2026, 1, 15, 12), "run_date": day}])
    append_rows(spark, replay.dq, "dq_cause_result", [{
        "run_id": "r1", "evaluation_id": "e1", "failure_ref": ref, "hop": "another_upstream->gold_member_coverage",
        "cause_check_id": "builtin", "order_no": 1, "state": "CONFIRMED", "cause_code": "NOT_RUN",
        "evidence": "{}", "evaluated_at": datetime(2026, 1, 15, 12), "run_date": day}])
    code, lines = trace(replay, capsys)
    assert code == 0
    assert lines[-1] == "cause: not evaluated (no failed HOP_KEY_CURRENCY for this key at this hop in the dq store)"


def test_a_retried_investigation_s_cause_is_shown(spark, tmp_path, capsys) -> None:
    """Checkpoint 2 review (CP2-03): a retry investigates a result another run committed and stores its
    causes under its own run id. The trace finds them through the cause commit's result_run_id; it
    used to require the cause's run to be the result's, so a retried cause was never shown."""
    from datetime import date, datetime

    from hcsc.datalake.dre.checks.hop.common import key_hash
    from tests.store.conftest import append_rows

    secret = tmp_path / "hmac.secret"
    secret.write_bytes(b"synthetic-trace-secret\n")
    replay = replay_conf(spark, tmp_path, "trace_retried", {
        "defaults.yaml": [("hmac_secret_file: null", f"hmac_secret_file: {secret}")]})
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(updated=curated_load_time(12))])
    create_table(spark, replay.gold, GOLD_COLUMNS)
    ref = key_hash(b"synthetic-trace-secret", "123401|01|2026-01-01|AGR-A")
    append_rows(spark, replay.dq, "dq_check_result", [{
        "evaluation_id": "e1", "run_id": "r1", "dataset": "gold_member_coverage", "check_id": "HOP_KEY_CURRENCY",
        "state": "FAILED", "evaluated_at": datetime(2026, 1, 15, 12), "run_date": date(2026, 1, 15)}])
    append_rows(spark, replay.dq, "dq_cause_result", [{  # the retry, a day later, under its own run id
        "run_id": "r2", "result_run_id": "r1", "evaluation_id": "e1", "failure_ref": ref,
        "hop": "example_curated_enrollment->gold_member_coverage", "cause_check_id": "builtin", "order_no": 1,
        "state": "CONFIRMED", "cause_code": "NOT_RUN", "evidence": "{}", "evaluated_at": datetime(2026, 1, 16, 12),
        "run_date": date(2026, 1, 16)}])
    code, lines = trace(replay, capsys)
    assert code == 0
    assert lines[-1] == "cause: NOT_RUN (confirmed)"


def test_the_key_can_come_from_a_file_or_stdin_and_never_enters_sql_text(spark, tmp_path, capsys, monkeypatch) -> None:
    """--key-file keeps the key off the command line; either way the key is data in a one-row temp
    view, so no SQL the trace runs carries it as a literal."""
    import io

    replay = replay_conf(spark, tmp_path, "trace_key_file")
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row()])
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row()])
    key_file = tmp_path / "key.txt"
    key_file.write_text("\n000123401,01,2026-01-01,AGR-A\n", encoding="utf-8")
    seen: list[str] = []
    real_sql = spark.sql
    monkeypatch.setattr(spark, "sql", lambda text: seen.append(text) or real_sql(text))
    code, lines = trace(replay, capsys, ["--dataset", "gold_member_coverage", "--key-file", str(key_file)])
    assert code == 0 and lines[-1].startswith("no gap")
    assert not [s for s in seen if "123401" in s or "AGR-A" in s]  # literals nowhere in the SQL text
    monkeypatch.setattr("sys.stdin", io.StringIO("000123401,01,2026-01-01,AGR-A\n"))
    code, lines = trace(replay, capsys, ["--dataset", "gold_member_coverage", "--key-file", "-"])
    assert code == 0 and lines[-1].startswith("no gap")
    code, lines = trace(replay, capsys, ["--dataset", "gold_member_coverage"])
    assert code == 3 and "--key-file" in lines[-1]
    code, lines = trace(replay, capsys, ["--dataset", "gold_member_coverage", "--key-file", str(tmp_path / "missing.txt")])
    assert code == 3 and "cannot read the key file" in lines[-1]
