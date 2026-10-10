from __future__ import annotations

import pytest

from hcsc.datalake.dre.cli import COMMANDS, main


def test_no_command_prints_help(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 0
    out = capsys.readouterr().out
    for name in COMMANDS:
        assert name in out


def test_every_command_is_built() -> None:
    """Step 10 builds the last one (watchdog): no subcommand falls through to "not built yet"."""
    import inspect

    from hcsc.datalake.dre import cli

    source = inspect.getsource(cli.main)
    for name in COMMANDS:
        assert f'"{name}"' in source, name
