"""Generic cause probes: pattern YAML cause lists and per-feed probe parameters (spec section 7)."""

from __future__ import annotations

from pathlib import Path

import pytest

from hcsc.datalake.dre.checks import registry
from hcsc.datalake.dre.checks.registry import PATTERNS, TABLE_WIDE, pattern_causes, probe_keys
from hcsc.datalake.dre.config.probes import PROBE_PARAMS, FileValueCompare, TableContains, is_ready
from hcsc.datalake.dre.config.validate import validate_conf
from tests.config.conftest import edit, line_with

FEED = "feeds/example_realtime.yaml"


def test_every_pattern_has_well_formed_cause_lists() -> None:
    for pattern in (*PATTERNS, TABLE_WIDE):
        for failure in pattern_causes(pattern).values():
            assert failure.order[-1].fallback
            assert failure.checks
        probe_keys(pattern)  # raises if a key is shared by two probe types
    file_causes = pattern_causes("FILE_CYCLIC")["file_not_loaded"]
    assert [e.code for e in file_causes.order][:3] == ["RAW_LOAD_HELD", "PIPELINE_STALLED", "SKIPPED_BEHIND_CURSOR"]
    assert pattern_causes(TABLE_WIDE) == {}


@pytest.mark.parametrize(("yaml_text", "message"), [
    ("causes:\n  x:\n    checks: [T1_ON_TIME]\n    order:\n      - {code: A, probe: telepathy}\n      - {code: B, fallback: true}\n",
     "unknown probe"),
    ("causes:\n  x:\n    checks: [T1_ON_TIME]\n    order:\n      - {code: A, probe: file_exists}\n      - {code: B, fallback: true}\n",
     "names no params key"),
    ("causes:\n  x:\n    checks: [T1_ON_TIME]\n    order:\n      - {code: A, probe: builtin}\n", "exactly one fallback"),
])
def test_malformed_cause_lists_are_rejected(monkeypatch, yaml_text: str, message: str) -> None:
    import yaml

    monkeypatch.setattr(registry, "_load_pattern", lambda pattern: yaml.safe_load(yaml_text))
    pattern_causes.cache_clear()
    try:
        with pytest.raises(ValueError, match=message):
            pattern_causes("FILE_CYCLIC")
    finally:
        pattern_causes.cache_clear()


def test_null_parameters_are_not_ready() -> None:
    assert not is_ready(None)
    assert not is_ready(FileValueCompare(path="/data/ctl/cursor.txt", extract_regex=r"(\d{8})", compare_to=None))
    assert is_ready(FileValueCompare(path="/data/ctl/cursor.txt", extract_regex=r"(\d{8})", compare_to="partition"))
    assert is_ready(TableContains(table="ops_db.rejects", condition="reason IS NOT NULL"))  # id, code_ref optional
    assert is_ready(PROBE_PARAMS["size_changed"]())  # needs no parameters


def test_sample_probes_validate(conf: Path) -> None:
    edit(conf, FEED, "load_hold_marker: { path: null }", "load_hold_marker: { path: /data/ctl/hold.flag }")
    edit(conf, FEED, "filter_rules: null", "filter_rules:\n    - { table: curated_db.enrollment, condition: \"x = 1\", id: drop_x }\n"
                                            "    - { table: curated_db.enrollment, condition: \"y = 2\" }")
    _, errors, _ = validate_conf(conf)
    assert errors == []


@pytest.mark.parametrize(("old", "new", "field", "needle", "problem"), [
    ("load_log: {", "load_logs: {", "probes.load_logs", "load_logs:", "has no cause that uses probe key"),
    ("compare_to: partition", "compare_to: month", "probes.partition_cursor.compare_to", "partition_cursor:", "not allowed"),
    ("extract_regex: null", "extract_regex: '(['", "probes.partition_cursor.extract_regex", "partition_cursor:", "does not compile"),
    ("rejects: { table: null", "rejects: { table: rejects_only", "probes.rejects.table", "rejects:", "database>.<table"),
    ("load_hold_marker: { path: null }", "load_hold_marker: { path: null, size: 3 }", "probes.load_hold_marker.size",
     "load_hold_marker:", "unknown field"),
])
def test_bad_probe_config_names_file_line_field_and_fix(conf: Path, old, new, field, needle, problem) -> None:
    edit(conf, FEED, old, new)
    _, errors, _ = validate_conf(conf)
    matching = [e for e in errors if e.field == field]
    assert matching, "\n".join(map(str, errors))
    assert problem in matching[0].problem and matching[0].fix
    assert matching[0].line == line_with(conf, FEED, needle)
