"""R20 (proof scenario PS7): a historical rerun after a later load never claims a historical PASS.

Reconciliation model section 10, row 7: "CURRENT_RECHECK, labelled as such, and never a historical
PASS. AS_OF_REPLAY on a `mutable` dataset: DID_NOT_RUN / `no_as_of_evidence`" (ADR-05, model section
8). Through `dre run`: the NORMAL run finds a record missing from gold; gold is loaded later; a RERUN
of the window then passes, as a current recheck, beside the NORMAL FAILED that stays the record of
what was true then; an AS_OF_REPLAY of the window is refused, gold being mutable.
"""

from __future__ import annotations

from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import (
    CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row,
)
from tests.replay.ledger_views import current_ledger, ledger_results
from tests.replay.test_r04_to_r07_hops import setup


def test_r20_a_rerun_after_a_later_load_is_a_current_recheck_and_never_rewrites_history(spark, tmp_path, capsys) -> None:
    replay = setup(spark, tmp_path, "r20")
    loaded = curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS, [curated_row(sub_id="123401", updated=loaded),
                                                          curated_row(sub_id="123402", updated=loaded)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(sub_id="000123401", record=loaded, loaded=gold_load_time(11))])
    assert main(["run", "--conf", str(replay.conf)]) == 0
    normal = current_ledger(spark, replay)
    assert (normal.state, normal.violations, normal.evaluation_mode) == ("FAILED", 1, "NORMAL")

    # The missing record is loaded later.
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(sub_id="000123401", record=loaded, loaded=gold_load_time(11)),
                                                    gold_row(sub_id="000123402", record=loaded, loaded=gold_load_time(1))])
    window = ["--window-start", normal.window_start.isoformat(), "--window-end", normal.window_end.isoformat()]
    capsys.readouterr()
    assert main(["run", "--conf", str(replay.conf), "--execution-type", "RERUN", *window]) == 0
    out = capsys.readouterr().out
    assert "is a current recheck" in out and "[current recheck]" in out
    history = [(r.execution_type, r.evaluation_mode, r.state) for r in ledger_results(spark, replay)]
    assert history == [("NORMAL", "NORMAL", "FAILED"), ("RERUN", "CURRENT_RECHECK", "PASSED")]
    # The NORMAL result is the historical record: it is kept, and the only NORMAL result of the window.
    normals = [r for r in ledger_results(spark, replay) if r.evaluation_mode == "NORMAL"]
    assert [(r.evaluation_id, r.state) for r in normals] == [(normal.evaluation_id, "FAILED")]
    now = current_ledger(spark, replay)
    assert (now.evaluation_mode, now.state) == ("CURRENT_RECHECK", "PASSED")  # labelled wherever it is read

    # As the data stood at the window end cannot be shown for a mutable gold table.
    assert main(["run", "--conf", str(replay.conf), "--execution-type", "AS_OF_REPLAY", *window]) == 0
    # refused before the hop is read, so the result names no upstream group (like a precondition failure)
    as_of = spark.table(f"{replay.dq}.v_committed_result").where(
        "dataset = 'gold_member_coverage' AND check_id = 'HOP_LEDGER' AND evaluation_mode = 'AS_OF_REPLAY'").collect()
    assert [(r.state, r.reason_category, r.reason_code) for r in as_of] == [
        ("DID_NOT_RUN", "DATA_UNAVAILABLE", "no_as_of_evidence")]
    assert "gold_member_coverage is mutable, not append_only" in as_of[0].detail
