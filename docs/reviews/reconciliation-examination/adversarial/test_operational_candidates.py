"""Executable checks of the static review's operational candidates (examination only).

Unit level where the claim is about one function; runner level (through `dre run` on the sample
configuration, as tests/replay does) for the digest. Each test asserts the behaviour observed.
"""

from __future__ import annotations

from hcsc.datalake.dre.checks.base import from_counts
from hcsc.datalake.dre.checks.events import canonical_groups, evaluation_id
from hcsc.datalake.dre.cli import main
from tests.fixtures.layers import CURATED_COLUMNS, GOLD_COLUMNS, create_table, curated_load_time, curated_row, gold_load_time, gold_row
from tests.replay.test_r04_to_r07_hops import setup


def test_o01_null_violations_with_a_population_is_passed() -> None:
    """FINDING F-15 (false PASS through operator SQL): from_counts treats violations NULL as 0. A conf
    template whose SQL yields population 5 and violations NULL is PASSED, not DID_NOT_RUN. A negative
    population and violations above the population are accepted too."""
    assert from_counts(5, None, "x", "y").state == "PASSED"
    assert from_counts(-3, 0, "x", "y").state == "PASSED"
    assert from_counts(1, 7, "x", "y").state == "FAILED"  # 7 of 1: not caught as impossible


def test_o02_null_and_empty_group_values_share_one_evaluation_id() -> None:
    """FINDING F-16: canonical_groups writes None as '', and joins with ',' and '=' unescaped. Spark keeps
    NULL and '' as two groups, but both rows get one evaluation_id; their commits then declare 1 row each
    while 2 are stored, so the publisher withholds the evaluation (every run)."""
    assert canonical_groups({"src": None}) == canonical_groups({"src": ""})
    assert canonical_groups({"a": "1,b=2", "b": "3"}) == canonical_groups({"a": "1", "b": "2,b=3"})
    assert evaluation_id("e", "T1_KEY_NULLS", 1, "v", {"src": None}) == evaluation_id("e", "T1_KEY_NULLS", 1, "v", {"src": ""})


def test_o03_volume_with_a_tolerance_of_100_percent_or_more_passes_an_empty_slot() -> None:
    """FINDING F-17 (false PASS with population 0, breaks hard rule 4): volume_tolerance_pct is validated
    only > 0. At 100 the lower bound is median * 0 = 0, so an empty slot (row_count 0, from the LEFT JOIN
    on periods) is BETWEEN the bounds: outside 0, and Volume._result writes PASSED with population 0."""
    from types import SimpleNamespace

    from hcsc.datalake.dre.checks.tier1.volume import Volume
    from hcsc.datalake.dre.config.models import Settings

    assert Settings.model_fields["volume_tolerance_pct"].metadata  # gt=0 only (no upper bound)
    low_factor, high_factor, median = 1 - 100 / 100, 1 + 100 / 100, 1000
    outside = 0 if median * low_factor <= 0 <= median * high_factor else 1
    row = SimpleNamespace(g_1="daily", g_2="06:00", row_count=0, outside=outside, low=0.0, high=2000.0,
                          median_rows=median, loads=10)
    r = Volume._result(row)
    assert (r.state, r.population) == ("PASSED", 0)


def r05_run(spark, tmp_path, name: str, capsys, monkeypatch) -> tuple[int, str, str]:
    """`dre run` on R05's data (gold older than curated); (exit code, membership-gold subject, its text)."""
    from hcsc.datalake.dre import runner

    captured = []
    real_to_send = runner.to_send
    monkeypatch.setattr(runner, "to_send", lambda digests, *a: captured.extend(digests) or real_to_send(digests, *a))
    (tmp_path / name).mkdir()
    replay = setup(spark, tmp_path / name, name)
    opened, terminated = curated_load_time(30), curated_load_time(12)
    create_table(spark, replay.curated, CURATED_COLUMNS,
                 [curated_row(updated=opened), curated_row(end="2026-06-30", updated=terminated)])
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(record=opened, loaded=gold_load_time(29))])
    code = main(["run", "--conf", str(replay.conf)])
    capsys.readouterr()
    [d] = [d for d in captured if d.owner == "membership-gold"]
    return code, d.subject, d.text()


def test_o04_a_withheld_failure_is_absent_from_the_digest_which_does_not_say_the_run_was_partial(
        spark, tmp_path, capsys, monkeypatch) -> None:
    """FINDING F-18: R05's data: HOP_KEY_CURRENCY and HOP_LEDGER FAIL. Control run: the digest counts
    them. Then the same run with the publisher withholding those units (simulated: the stored-row count
    disagrees with the commits, as after a write that lost rows): the run ends PARTIAL (exit 2, windows
    held: correct), but the digest counts only committed results, fewer new failures, and its text never
    says that checks were withheld or that the run was PARTIAL."""
    from hcsc.datalake.dre import runner

    code, control_subject, _ = r05_run(spark, tmp_path, "x_o04_control", capsys, monkeypatch)
    assert code == 0
    real = runner.mismatches
    failing = {"HOP_KEY_CURRENCY", "HOP_LEDGER"}
    monkeypatch.setattr(runner, "mismatches",
                        lambda c, counts: ["simulated: stored rows differ"] if c.check_id in failing else real(c, counts))
    code, subject, text = r05_run(spark, tmp_path, "x_o04_withheld", capsys, monkeypatch)
    assert code == 2  # EXIT_PARTIAL
    new = lambda subj: int(subj.split(": ", 1)[1].split(" new failures")[0])  # noqa: E731
    assert new(subject) < new(control_subject)
    assert "withheld" not in text.lower() and "partial" not in text.lower()
    assert "HOP_KEY_CURRENCY" not in text and "HOP_LEDGER" not in text
