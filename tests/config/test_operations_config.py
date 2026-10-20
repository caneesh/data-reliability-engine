"""The isolation settings of build step 8 (ADR-08): run_budget_minutes, kill_switch_file and
backpressure_share validate with file, line, field and fix; the sample leaves the kill switch null."""

from __future__ import annotations

from pathlib import Path

import pytest

from hcsc.datalake.dre.config.loader import ConfigError
from hcsc.datalake.dre.config.validate import validate_conf
from tests.config.conftest import SAMPLE_CONF, edit, line_with

DEFAULTS = "defaults.yaml"

# (id, old, new, field, line needle, problem contains, fix contains)
CASES = [
    ("run-budget-zero", "run_budget_minutes: 40", "run_budget_minutes: 0", "run_budget_minutes",
     "run_budget_minutes", "greater than", "> 0"),
    ("run-budget-not-a-number", "run_budget_minutes: 40", "run_budget_minutes: forty", "run_budget_minutes",
     "run_budget_minutes", "number", "number"),
    ("backpressure-above-one", "backpressure_share: 0.8", "backpressure_share: 1.5", "backpressure_share",
     "backpressure_share", "less than or equal", "<= 1"),
    ("backpressure-zero", "backpressure_share: 0.8", "backpressure_share: 0", "backpressure_share",
     "backpressure_share", "greater than", "> 0"),
    ("kill-switch-relative", "kill_switch_file: null", "kill_switch_file: run/stop_dre", "kill_switch_file",
     "kill_switch_file", "not an absolute path", "absolute path"),
]


def _find(errors: list[ConfigError], file: Path, field: str) -> ConfigError:
    matching = [e for e in errors if e.file == str(file) and e.field == field]
    assert matching, f"no error for {file}:{field}; got:\n" + "\n".join(map(str, errors))
    return matching[0]


@pytest.mark.parametrize(("case", "old", "new", "field", "needle", "problem", "fix"), CASES, ids=[c[0] for c in CASES])
def test_isolation_errors_name_file_line_field_and_fix(
    conf: Path, case: str, old: str, new: str, field: str, needle: str, problem: str, fix: str
) -> None:
    edit(conf, DEFAULTS, old, new)
    _, errors, _ = validate_conf(conf)
    error = _find(errors, conf / DEFAULTS, field)
    assert error.line == line_with(conf, DEFAULTS, needle), str(error)
    assert problem in error.problem, str(error)
    assert fix in error.fix, str(error)
    assert str(error).startswith(f"{conf / DEFAULTS}:{error.line}:") and "Fix: " in str(error)


def test_the_sample_takes_the_adr_08_defaults_and_leaves_the_kill_switch_unnamed() -> None:
    config, errors, _ = validate_conf(SAMPLE_CONF)
    assert errors == []
    d = config.defaults
    assert (d.run_budget_minutes, d.kill_switch_file, d.backpressure_share) == (40, None, 0.8)


def test_an_absolute_kill_switch_path_and_a_full_share_are_valid(conf: Path) -> None:
    edit(conf, DEFAULTS, "kill_switch_file: null", "kill_switch_file: /var/run/example/stop_dre")
    edit(conf, DEFAULTS, "backpressure_share: 0.8", "backpressure_share: 1")
    edit(conf, DEFAULTS, "run_budget_minutes: 40", "run_budget_minutes: 0.5")
    config, errors, _ = validate_conf(conf)
    assert errors == []
    assert (config.defaults.kill_switch_file, config.defaults.backpressure_share) == ("/var/run/example/stop_dre", 1)


def test_settings_left_out_take_the_adr_08_defaults(conf: Path) -> None:
    for line in ("run_budget_minutes: 40", "kill_switch_file: null", "backpressure_share: 0.8"):
        text = (conf / DEFAULTS).read_text(encoding="utf-8")
        (conf / DEFAULTS).write_text("\n".join(t for t in text.splitlines() if not t.startswith(line)) + "\n",
                                     encoding="utf-8")
    config, errors, _ = validate_conf(conf)
    assert errors == []
    d = config.defaults
    assert (d.run_budget_minutes, d.kill_switch_file, d.backpressure_share) == (40, None, 0.8)
