"""Gate A remediation, release N: `dre trace` and key encodings (spec section 8, "Trace").

This engine computes key encoding 1. When the gap hop also holds committed key events of another
encoding (written by a later engine), the cause line is labelled with its encoding and a line per
other encoding says such keys are present but not searchable by this engine's key. With one encoding
at the hop the output is unchanged (test_trace.py and R05 assert it exactly).
"""

from __future__ import annotations

from datetime import date, datetime

from hcsc.datalake.dre.checks.hop.common import key_hash
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row
from tests.replay.conftest import replay_conf
from tests.store.conftest import append_rows
from tests.test_trace import trace

SECRET = b"synthetic-trace-secret"
HOP = "example_curated_enrollment->gold_member_coverage"


def setup(spark, tmp_path, name: str):
    secret = tmp_path / "hmac.secret"
    secret.write_bytes(SECRET + b"\n")
    replay = replay_conf(spark, tmp_path, name, {"defaults.yaml": [("hmac_secret_file: null",
                                                                    f"hmac_secret_file: {secret}")]})
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(updated=curated_load_time(12))])
    create_table(spark, replay.gold, GOLD_COLUMNS)
    ref = key_hash(SECRET, "123401|01|2026-01-01|AGR-A")
    append_rows(spark, replay.dq, "dq_check_result", [{
        "evaluation_id": "e1", "run_id": "r1", "dataset": "gold_member_coverage", "check_id": "HOP_KEY_CURRENCY",
        "state": "FAILED", "evaluated_at": datetime(2026, 1, 15, 12), "run_date": date(2026, 1, 15)}], commit=False)
    append_rows(spark, replay.dq, "dq_key_event", [{
        "run_id": "r1", "evaluation_id": "e1", "dataset": "gold_member_coverage", "check_id": "HOP_KEY_CURRENCY",
        "hop": HOP, "key_hash": ref, "key_value": "123401|01|2026-01-01|AGR-A", "event": "FLAGGED",
        "state_detail": "MISSING", "observed_at": datetime(2026, 1, 15, 12), "key_encoding": 1,
        "run_date": date(2026, 1, 15)}])
    append_rows(spark, replay.dq, "dq_cause_result", [{
        "run_id": "r1", "evaluation_id": "e1", "failure_ref": ref, "hop": HOP, "cause_check_id": "builtin",
        "order_no": 1, "state": "CONFIRMED", "cause_code": "NOT_RUN", "evidence": "{}",
        "evaluated_at": datetime(2026, 1, 15, 12), "run_date": date(2026, 1, 15)}])
    return replay


def test_one_encoding_at_the_hop_leaves_the_trace_unchanged(spark, tmp_path, capsys) -> None:
    replay = setup(spark, tmp_path, "trace_enc_one")
    code, lines = trace(replay, capsys)
    assert code == 0 and lines[-1] == "cause: NOT_RUN (confirmed)"


def test_another_encoding_at_the_hop_is_labelled_and_reported_not_searchable(spark, tmp_path, capsys) -> None:
    replay = setup(spark, tmp_path, "trace_enc_two")
    append_rows(spark, replay.dq, "dq_key_event", [{
        "run_id": "r9", "evaluation_id": "e9", "dataset": "gold_member_coverage", "check_id": "HOP_KEY_CURRENCY",
        "hop": HOP, "key_hash": "lpb-trace-hash", "key_value": "synthetic-lpb-key", "event": "FLAGGED",
        "state_detail": "MISSING", "observed_at": datetime(2026, 1, 16, 12), "key_encoding": 2,
        "run_date": date(2026, 1, 16)}])
    code, lines = trace(replay, capsys)
    assert code == 0
    assert lines[-2:] == [
        "cause: NOT_RUN (confirmed) [key encoding 1]",
        "key encoding 2: 1 key with events at this hop, written by a later engine; not searchable by this engine's key"]
    assert "synthetic-lpb-key" not in "\n".join(lines)  # key_value is never printed
