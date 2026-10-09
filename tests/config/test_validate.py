"""dre validate: the sample config passes; each error type names file, line, field and fix.

Build step 2 "done when" (spec section 10).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from hcsc.datalake.dre.cli import main
from hcsc.datalake.dre.config.loader import ConfigError
from hcsc.datalake.dre.config.validate import validate_conf
from tests.config.conftest import SAMPLE_CONF, edit, line_with

FEED = "feeds/example_realtime.yaml"
GOLD = "datasets/gold_member_coverage.yaml"
CURATED = "datasets/example_curated_enrollment.yaml"
DEFAULTS = "defaults.yaml"
RULE = "rules/one_row_per_coverage.yaml"
LANDING = 'landing:\n  roots: [/data/landing/example_feed]\n  file_format: sequence\n  file_name_pattern: "*"\n'


def test_sample_config_is_valid() -> None:
    config, errors = validate_conf(SAMPLE_CONF)
    assert errors == []
    assert set(config.feeds) == {"example_realtime"}
    assert set(config.datasets) == {"example_curated_enrollment", "gold_member_coverage"}
    assert len(config.rules) == 4


def test_cli_validate_exit_codes(conf: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["validate", "--conf", str(conf)]) == 0
    assert "is valid" in capsys.readouterr().out
    edit(conf, FEED, "pattern: FILE_CYCLIC", "pattern: FILE_HOURLY")
    assert main(["validate", "--conf", str(conf)]) == 1
    out = capsys.readouterr().out
    assert f"{conf / FEED}:{line_with(conf, FEED, 'FILE_HOURLY')}: pattern:" in out
    assert "Fix:" in out


# (id, file, old, new, field, line needle, problem contains, fix contains)
CASES = [
    ("yaml-syntax", FEED, "owner: membership-gold", "owner: membership-gold: x", "", "membership-gold: x", "YAML syntax", "syntax"),
    ("duplicate-key", FEED, "owner: membership-gold\n", "owner: membership-gold\nowner: other\n", "owner", "owner: other", "duplicate key", "keep one"),
    ("missing-field", FEED, "owner: membership-gold\n", "", "owner", "feed: example_realtime", "missing", "add `owner`"),
    ("unknown-field", FEED, "\nsla_hours: 8", "\nsla_hour: 8", "sla_hour", "sla_hour: 8", "unknown field", "spelling"),
    ("bad-enum", FEED, "pattern: FILE_CYCLIC", "pattern: FILE_HOURLY", "pattern", "FILE_HOURLY", "not allowed", "FILE_CYCLIC"),
    ("not-a-number", FEED, "expectation_version: 1", "expectation_version: one", "expectation_version", "expectation_version", "whole number", "whole number"),
    ("below-minimum", FEED, "expectation_version: 1", "expectation_version: 0", "expectation_version", "expectation_version", "greater than", ">= 1"),
    ("null-required", FEED, "roots: [/data/landing/example_feed]", "roots: null", "landing.roots", "roots: null", "null", "spec section 11"),
    # YAML 1.1 reads unquoted 4:00 as the number 240.
    ("unquoted-time", FEED, '"04:00"', "4:00", "cadence.times[1]", "times:", "not a string", "quoted"),
    ("bad-time", FEED, '"04:00"', '"4am"', "cadence.times[1]", "times:", "not a time of day", "HH:MM"),
    ("bad-timezone", FEED, "America/Chicago", "Mars/Base", "cadence.timezone", "Mars/Base", "time zone", "IANA"),
    ("cadence-kind-mismatch", FEED, "kind: times", "kind: interval", "cadence", "cadence:", "interval_minutes", "interval_minutes"),
    ("file-pattern-needs-landing", FEED, LANDING, "", "", "feed: example_realtime", "needs `landing`", "landing"),
    ("not-a-list", GOLD, "group_by: [src_sys_nm]", "group_by: src_sys_nm", "group_by", "group_by:", "list", "YAML list"),
    ("not-a-bool", GOLD, "key_unique: true", "key_unique: maybe", "key_unique", "key_unique", "true or false", "true or false"),
    ("bad-identifier", GOLD, "key: [sub_id,", 'key: ["sub-id",', "key[0]", "key: [", "not a plain identifier", "letters"),
    ("bad-table", GOLD, "table: gold_db.member_coverage", "table: member_coverage", "table", "table:", "database>.<table", "database.table"),
    ("sql-semicolon", GOLD, "src_sys_nm = 'SRC_A'", "src_sys_nm = 'SRC_A'; DROP TABLE x", "feed_filter", "feed_filter", "';'", "single SQL expression"),
    ("bad-normaliser", GOLD, "strip_leading_zeros", "trim", "key_normalise.sub_id", "key_normalise", "not allowed", "strip_leading_zeros"),
    ("normalise-non-key", GOLD, "{ sub_id: strip", "{ src_sys_nm: strip", "key_normalise.src_sys_nm", "key_normalise", "not a key column", "key columns only"),
    ("unknown-dataset-in-feed", FEED, "datasets: [", "datasets: [missing_ds, ", "datasets[0]", "datasets: [", "unknown dataset", "datasets/missing_ds.yaml"),
    ("unknown-upstream", CURATED, "upstream: []", "upstream: [nowhere]", "upstream[0]", "upstream:", "unknown dataset", "datasets/nowhere.yaml"),
    ("upstream-cycle", CURATED, "upstream: []", "upstream: [gold_member_coverage]", "upstream", "upstream:", "cycle", "remove one upstream link"),
    ("key-map-incomplete", GOLD, "    covrg_agrmt_id: qualifiedhealthplanid\n", "", "key_map.example_curated_enrollment", "  example_curated_enrollment:", "does not cover", "map every key column"),
    ("key-map-not-upstream", GOLD, "upstream: [example_curated_enrollment]", "upstream: []", "key_map.example_curated_enrollment", "  example_curated_enrollment:", "not in upstream", "add example_curated_enrollment to upstream"),
    ("key-map-not-upstream-key", GOLD, "sub_id: subscriberidnumber", "sub_id: sourcelastupdatets", "key_map.example_curated_enrollment", "  example_curated_enrollment:", "not in example_curated_enrollment's key", "map to columns"),
    ("owned-columns-need-key-map", CURATED, "key_unique: false", "owned_columns: { enddate: x }\nkey_unique: false", "owned_columns", "owned_columns", "needs a key_map", "key_map"),
    ("missing-recipient", FEED, "owner: membership-gold", "owner: other-team", "owner", "owner: other-team", "no recipients", "recipients in defaults.yaml"),
    ("unknown-template", RULE, "template: max_rows_per_key", "template: max_rows", "template", "template:", "not allowed", "max_rows_per_key"),
    ("rule-unknown-dataset", RULE, "dataset: gold_member_coverage", "dataset: nope", "dataset", "dataset: nope", "unknown dataset", "datasets/nope.yaml"),
    ("rule-param-missing", RULE, "params:\n  max: 1\n", "params: {}\n", "params.max", "params:", "missing", "add `max`"),
    ("rule-param-invalid", RULE, "max: 1", "max: -1", "params.max", "max: -1", "greater than", ">= 0"),
    ("rule-param-unknown", RULE, "max: 1", "max: 1\n  maximum: 2", "params.maximum", "maximum: 2", "unknown field", "spelling"),
    ("dq-database-not-identifier", DEFAULTS, "dq_database: dq", "dq_database: dq.prod", "dq_database", "dq_database", "plain identifier", "plain identifier"),
    ("hmac-relative-path", DEFAULTS, "hmac_secret_file: null", "hmac_secret_file: secrets/key", "", "dq_database: dq", "absolute path", "outside the repository"),
    ("retention-missing-table", DEFAULTS, "  dq_file: 13\n", "", "", "dq_database: dq", "retention_months", "dq_file"),
    ("bad-email", DEFAULTS, "dre-alerts@example.com", "dre-alerts", "recipients.membership-gold[0]", "membership-gold:", "email", "name@domain"),
]


def _find(errors: list[ConfigError], file: Path, field: str) -> ConfigError:
    matching = [e for e in errors if e.file == str(file) and e.field == field]
    assert matching, f"no error for {file}:{field}; got:\n" + "\n".join(map(str, errors))
    return matching[0]


@pytest.mark.parametrize(("case", "rel", "old", "new", "field", "needle", "problem", "fix"), CASES, ids=[c[0] for c in CASES])
def test_error_names_file_line_field_and_fix(
    conf: Path, case: str, rel: str, old: str, new: str, field: str, needle: str, problem: str, fix: str
) -> None:
    edit(conf, rel, old, new)
    _, errors = validate_conf(conf)
    error = _find(errors, conf / rel, field)
    assert error.line == line_with(conf, rel, needle), str(error)
    assert problem in error.problem, str(error)
    assert fix in error.fix, str(error)
    assert str(error).startswith(f"{conf / rel}:{error.line}:") and "Fix: " in str(error)


def test_duplicate_id(conf: Path) -> None:
    shutil.copy(conf / GOLD, conf / "datasets/zz_copy.yaml")
    _, errors = validate_conf(conf)
    error = _find(errors, conf / "datasets/zz_copy.yaml", "dataset")
    assert error.line == line_with(conf, "datasets/zz_copy.yaml", "dataset: gold_member_coverage")
    assert "duplicate dataset id" in error.problem and "unique" in error.fix


def test_dataset_in_two_feeds(conf: Path) -> None:
    shutil.copy(conf / FEED, conf / "feeds/zz_other.yaml")
    edit(conf, "feeds/zz_other.yaml", "feed: example_realtime", "feed: zz_other")
    _, errors = validate_conf(conf)
    error = _find(errors, conf / "feeds/zz_other.yaml", "datasets[0]")
    assert error.line == line_with(conf, "feeds/zz_other.yaml", "datasets: [")
    assert "already listed by feed 'example_realtime'" in error.problem and "one feed only" in error.fix


def test_setting_not_set_at_any_level(conf: Path) -> None:
    edit(conf, DEFAULTS, "sla_hours: 8\n", "")
    edit(conf, FEED, "\nsla_hours: 8", "")
    _, errors = validate_conf(conf)
    error = _find(errors, conf / GOLD, "sla_hours")
    assert error.line == line_with(conf, GOLD, "dataset: gold_member_coverage")
    assert "not set at any level" in error.problem
    assert "defaults.yaml, feed 'example_realtime'" in error.fix


def test_file_not_a_mapping(conf: Path) -> None:
    (conf / RULE).write_text("- just\n- a list\n", encoding="utf-8")
    _, errors = validate_conf(conf)
    error = _find(errors, conf / RULE, "")
    assert error.line == 1 and "one YAML mapping" in error.problem and error.fix


def test_defaults_missing(conf: Path) -> None:
    (conf / DEFAULTS).unlink()
    _, errors = validate_conf(conf)
    error = _find(errors, conf / DEFAULTS, "")
    assert error.line == 1 and "missing" in error.problem and "create" in error.fix
