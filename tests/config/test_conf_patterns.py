"""Patterns from configuration (spec section 3): conf/patterns/*.yaml adds a pattern or replaces a
packaged one, each pattern declares its kind, and malformed pattern files are config errors."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from hcsc.datalake.dre.checks.registry import (
    checks_for, is_file_pattern, pattern_causes, pattern_check_ids, pattern_kind, pattern_origin, patterns,
    use_conf_patterns,
)
from hcsc.datalake.dre.config.loader import load
from hcsc.datalake.dre.config.validate import validate_conf
from tests.config.conftest import edit

FEED = "feeds/example_realtime.yaml"
RELOAD = """# A table reloaded in full on each cadence slot: no files, no hop.
pattern: TABLE_RELOAD
kind: table
checks:
  - T1_ON_TIME
  - T1_ZERO_ROWS
  - T1_VOLUME
  - T1_KEY_DUPLICATES
  - {check: T1_FRESHNESS, params: {max_age_hours: 30, time: load}}
causes:
  load_wrote_nothing:
    checks: [T1_ZERO_ROWS]
    order:
      - {code: NO_UPSTREAM_DATA, probe: builtin}
      - {code: EMPTY_LOAD, fallback: true}
"""


@pytest.fixture(autouse=True)
def packaged_only_afterwards():
    yield
    use_conf_patterns(None)


def drop_probes(conf: Path) -> None:
    """Remove the sample feed's `probes:` block: a pattern that uses none of its keys rejects them."""
    path = conf / FEED
    text = re.sub(r"^probes:.*?(?=^email_sample_keys)", "", path.read_text(encoding="utf-8"), flags=re.S | re.M)
    path.write_text(text, encoding="utf-8")


def _use_reload(conf: Path, feed_pattern: str = "TABLE_RELOAD") -> None:
    (conf / "patterns").mkdir()
    (conf / "patterns" / "table_reload.yaml").write_text(RELOAD, encoding="utf-8")
    edit(conf, FEED, "pattern: FILE_CYCLIC", f"pattern: {feed_pattern}")
    drop_probes(conf)


def test_a_pattern_from_conf_is_known_once_the_conf_is_loaded(conf: Path) -> None:
    _use_reload(conf)
    assert "TABLE_RELOAD" not in patterns()  # not until a conf directory names it
    config, errors = load(conf)
    assert errors == [], "\n".join(map(str, errors))
    assert patterns() == ("FILE_CYCLIC", "FILE_PERIODIC", "TABLE_MERGE", "TABLE_RELOAD")
    assert (pattern_origin("TABLE_RELOAD"), pattern_origin("TABLE_MERGE")) == ("conf", "packaged")
    assert pattern_kind("TABLE_RELOAD") == "table" and not is_file_pattern("TABLE_RELOAD")
    assert pattern_check_ids("TABLE_RELOAD") == ("T1_ON_TIME", "T1_ZERO_ROWS", "T1_VOLUME", "T1_KEY_DUPLICATES",
                                                 "T1_FRESHNESS")
    assert list(pattern_causes("TABLE_RELOAD")) == ["load_wrote_nothing"]
    feed = config.feeds["example_realtime"]
    gold = config.datasets["gold_member_coverage"]
    # the pattern's defaults make T1_FRESHNESS apply even without a feed or dataset entry
    assert [c.check_id for c in checks_for(gold, feed.pattern, feed)] == ["T1_ON_TIME", "T1_ZERO_ROWS", "T1_VOLUME",
                                                                          "T1_KEY_DUPLICATES", "T1_FRESHNESS"]
    _, errors, warnings = validate_conf(conf)
    assert errors == [], "\n".join(map(str, errors))
    assert not [w for w in warnings if "T1_FILES_NOT_LOADED" in w.problem]  # a table pattern needs no raw dataset


def test_a_conf_pattern_replaces_a_packaged_one_with_the_same_name(conf: Path) -> None:
    (conf / "patterns").mkdir()
    (conf / "patterns" / "table_merge.yaml").write_text("pattern: TABLE_MERGE\nkind: table\nchecks: [T1_KEY_NULLS]\n",
                                                        encoding="utf-8")
    load(conf)
    assert pattern_origin("TABLE_MERGE") == "conf" and pattern_check_ids("TABLE_MERGE") == ("T1_KEY_NULLS",)
    assert pattern_causes("TABLE_MERGE") == {}
    use_conf_patterns(None)
    assert pattern_origin("TABLE_MERGE") == "packaged" and len(pattern_check_ids("TABLE_MERGE")) > 1


def test_file_kind_from_conf_needs_landing(conf: Path) -> None:
    (conf / "patterns").mkdir()
    (conf / "patterns" / "file_daily.yaml").write_text(
        "pattern: FILE_DAILY\nkind: file\nchecks: [T1_ON_TIME, T1_FILES_NOT_LOADED]\n", encoding="utf-8")
    edit(conf, FEED, "pattern: FILE_CYCLIC", "pattern: FILE_DAILY")
    drop_probes(conf)
    _, errors, _ = validate_conf(conf)
    assert errors == [], "\n".join(map(str, errors))
    assert is_file_pattern("FILE_DAILY")
    landing = "landing:\n  roots: [/data/landing/example_feed]\n  file_format: sequence\n  file_name_pattern: \"*\"\n"
    edit(conf, FEED, landing, "")
    _, errors, _ = validate_conf(conf)
    [e] = [e for e in errors if "needs `landing`" in e.problem]
    assert e.field == "" or "landing" in e.fix


CASES = [
    ("no-kind", "pattern: X_NO_KIND\nchecks: [T1_ON_TIME]\n", "kind must be one of"),
    ("wrong-name", "pattern: OTHER\nkind: table\nchecks: [T1_ON_TIME]\n", "`pattern` must be X_WRONG_NAME"),
    ("unknown-check", "pattern: X_UNKNOWN_CHECK\nkind: table\nchecks: [T1_NOPE]\n", "unknown checks"),
    ("fallback-only", "pattern: X_FALLBACK_ONLY\nkind: table\nchecks: [T1_ZERO_ROWS]\ncauses:\n  x:\n"
     "    checks: [T1_ZERO_ROWS]\n    order:\n      - {code: EMPTY_LOAD, fallback: true}\n",
     "needs at least one cause check before the fallback"),
    ("not-a-mapping", "- just\n- a list\n", "a pattern file is a mapping"),
]


@pytest.mark.parametrize(("name", "text", "problem"), CASES, ids=[c[0] for c in CASES])
def test_malformed_conf_patterns_are_config_errors_naming_the_file(conf: Path, name, text, problem) -> None:
    (conf / "patterns").mkdir()
    path = conf / "patterns" / f"x_{name.replace('-', '_')}.yaml"
    path.write_text(text, encoding="utf-8")
    _, errors, _ = validate_conf(conf)
    [e] = [e for e in errors if e.file == str(path)]
    assert problem in e.problem and "packaged patterns" in e.fix


def test_an_unknown_pattern_lists_the_conf_patterns_too(conf: Path) -> None:
    _use_reload(conf, "TABLE_DAILY")
    _, errors, _ = validate_conf(conf)
    [e] = [e for e in errors if e.field == "pattern"]
    assert "'TABLE_DAILY' is not allowed" in e.problem and "TABLE_RELOAD" in e.fix and "FILE_CYCLIC" in e.fix
