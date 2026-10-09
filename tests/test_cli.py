from __future__ import annotations

import pytest

from hcsc.datalake.dre.cli import COMMANDS, main


def test_no_command_prints_help(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 0
    out = capsys.readouterr().out
    for name in COMMANDS:
        assert name in out


@pytest.mark.parametrize("command", sorted(set(COMMANDS) - {"validate", "install", "run"}))
def test_unbuilt_command_reports_not_built(command: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([command]) == 3
    assert "not built yet" in capsys.readouterr().err
