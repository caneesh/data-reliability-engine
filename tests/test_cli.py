from __future__ import annotations

import pytest

from hcsc.datalake.dre.cli import COMMANDS, main


def test_no_command_prints_help(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 0
    out = capsys.readouterr().out
    for name in COMMANDS:
        assert name in out


def test_every_command_is_built() -> None:
    """Every subcommand has its branch in main."""
    import inspect

    from hcsc.datalake.dre import cli

    source = inspect.getsource(cli.main)
    for name in COMMANDS:
        assert f'"{name}"' in source, name


def test_retention_lists_then_drops_expired_partitions(spark, tmp_path, capsys) -> None:
    """Review finding 8: `dre retention` applies retention_months; by default it only lists."""
    from datetime import date

    from tests.replay.conftest import replay_conf
    from tests.store.conftest import append_rows

    replay = replay_conf(spark, tmp_path, "retention_cmd")
    old, recent = date(2020, 1, 1), date.today()
    for day in (old, recent):
        append_rows(spark, replay.dq, "dq_file", [{"path": f"/data/landing/example_feed/f_{day}", "feed": "x",
                                                   "run_id": "r", "run_date": day}])

    def dates() -> list[date]:
        return sorted(r.run_date for r in spark.table(f"{replay.dq}.dq_file").select("run_date").distinct().collect())

    capsys.readouterr()
    assert main(["retention", "--conf", str(replay.conf)]) == 0
    out = capsys.readouterr().out
    assert f"{replay.dq}.dq_file: would drop 1 partition(s) (2020-01-01 to 2020-01-01)" in out
    assert dates() == [old, recent]  # listed only
    assert main(["retention", "--conf", str(replay.conf), "--apply"]) == 0
    assert f"{replay.dq}.dq_file: dropped 1 partition(s)" in capsys.readouterr().out
    assert dates() == [recent]
