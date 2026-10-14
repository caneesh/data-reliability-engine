"""Rule templates from configuration (spec section 3): a conf/templates file becomes a template
rules can use; its SQL can only be a query (spec section 9, rule 10); errors name file and line."""

from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

from hcsc.datalake.dre.checks.base import CheckContext, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.rules.conf_template import TemplateFile, forbidden_statement
from hcsc.datalake.dre.checks.rules.rule_check import RuleCheck
from hcsc.datalake.dre.checks.rules.template import TEMPLATES
from hcsc.datalake.dre.cli import main
from hcsc.datalake.dre.config.loader import load
from hcsc.datalake.dre.config.models import Dataset, Rule
from hcsc.datalake.dre.config.validate import validate_conf
from tests.config.conftest import SAMPLE_CONF, line_with
from tests.fixtures.layers import GOLD_COLUMNS, create_table, gold_row
from tests.fixtures.settings import SETTINGS

UTC = timezone.utc
EVENT = Event("gold", datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 1, 16, 12, tzinfo=UTC))
NOT_AFTER = """template: not_after
params:
  column: column                       # bare type: required
  limit: { type: string }              # an ISO date the values must not pass
  format: { type: format, required: false }
population: rows with a value
violation: rows have a value after the limit
unit: rows
sql: |
  SELECT {% for g in group_by %}{{ g }} AS g_{{ loop.index }}, {% endfor %}
         COUNT({{ params.column }}) AS population,
         SUM(CASE WHEN {{ time(params.column, params.format) }} > {{ time(literal(params.limit), params.format) }}
                  THEN 1 ELSE 0 END) AS violations
  FROM {{ table }}
  {% if feed_filter %}WHERE {{ feed_filter }}{% endif %}
  {% if group_by %}GROUP BY {{ group_by | join(', ') }}{% endif %}
"""


RAW_EXPR = """template: raw_expr
params:
  expr: { type: string }               # rendered as it is: the scan of the template's text cannot see it
population: rows
violation: rows the expression counts
unit: rows
sql: |
  SELECT COUNT(*) AS population, {{ params.expr }} AS violations FROM {{ table }}
"""


@pytest.fixture
def conf(tmp_path: Path) -> Path:
    target = tmp_path / "conf"
    shutil.copytree(SAMPLE_CONF, target)
    (target / "templates").mkdir()
    (target / "templates" / "not_after.yaml").write_text(NOT_AFTER, encoding="utf-8")
    (target / "templates" / "raw_expr.yaml").write_text(RAW_EXPR, encoding="utf-8")
    return target


def gold(table: str, **extra) -> Dataset:
    return Dataset(dataset="gold", table=table, layer="GOLD", key=["sub_id"], key_normalise={"sub_id": "strip_leading_zeros"},
                   **extra)


def rule(params: dict, group_by=("src_sys_nm",)) -> Rule:
    return Rule(rule="eff_not_after_2026", template="not_after", dataset="gold", params=params, group_by=list(group_by),
                owner="membership-gold", status="approved", severity="medium")


def test_a_conf_template_is_known_once_the_conf_is_loaded(conf: Path) -> None:
    with pytest.raises(ValueError, match="'not_after' is not allowed"):
        rule({"column": "mbr_mbrshp_covrg_eff_dt", "limit": "2026-06-30"})
    config, errors = load(conf)
    assert errors == [], "\n".join(map(str, errors))
    assert "not_after" in TEMPLATES and sorted(config.templates) == ["not_after", "raw_expr"]
    spec = TEMPLATES["not_after"]
    p = spec.params_model.model_validate({"column": "eff", "limit": "2026-06-30"})
    assert (p.column, p.limit, p.format) == ("eff", "2026-06-30", None)
    with pytest.raises(ValueError):
        spec.params_model.model_validate({"column": "eff"})  # limit is required
    with pytest.raises(ValueError):
        spec.params_model.model_validate({"column": "not a column", "limit": "x"})
    assert spec.columns(gold("x.y"), p) == ["eff"]


def test_a_rule_on_a_conf_template_fails_passes_and_did_not_run(spark, conf: Path) -> None:
    load(conf)
    create_table(spark, "ctpl.g", GOLD_COLUMNS, [
        gold_row(eff="2026-03-01"), gold_row(sub_id="2", eff="2026-09-01"),                  # SRC_A: one after
        gold_row(sub_id="3", eff="2026-01-01", source="SRC_B"), gold_row(sub_id="4", eff=None, source="SRC_B")])
    params = {"column": "mbr_mbrshp_covrg_eff_dt", "limit": "2026-06-30", "format": "yyyy-MM-dd"}
    check = RuleCheck(rule(params))
    results, _ = run_check(check, CheckContext(spark, gold("ctpl.g"), None, SETTINGS, "UTC"), EVENT)
    by = {r.group_values["src_sys_nm"]: r for r in results}
    assert (by["SRC_A"].state, by["SRC_A"].population, by["SRC_A"].violations) == ("FAILED", 2, 1)
    assert by["SRC_A"].observed == "1 of 2 rows have a value after the limit"
    assert by["SRC_A"].expected == "0 rows breaking rule eff_not_after_2026"
    assert (by["SRC_B"].state, by["SRC_B"].population) == ("PASSED", 1)  # the null value is not counted
    assert check.required_columns(gold("ctpl.g")) == ["src_sys_nm", "mbr_mbrshp_covrg_eff_dt"]
    assert check.severity == "medium" and check.check_id == "eff_not_after_2026"
    [r] = run_check(RuleCheck(rule(params)), CheckContext(spark, gold("ctpl.g", feed_filter="1 = 0"), None, SETTINGS,
                                                        "UTC"), EVENT)[0]
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")
    [r] = run_check(RuleCheck(rule({**params, "column": "gone"})), CheckContext(spark, gold("ctpl.g"), None, SETTINGS,
                                                                                 "UTC"), EVENT)[0]
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "column_missing")


def test_a_conf_template_runs_through_dre_run_and_dry_run(spark, conf: Path, capsys, tmp_path) -> None:
    from tests.replay.conftest import latest, replay_conf

    replay = replay_conf(spark, tmp_path / "run", "ctplrun")  # the conf fixture holds tmp_path/conf
    (replay.conf / "templates").mkdir()
    (replay.conf / "templates" / "not_after.yaml").write_text(NOT_AFTER, encoding="utf-8")
    (replay.conf / "rules" / "eff_not_after.yaml").write_text(
        "rule: eff_not_after\ntemplate: not_after\ndataset: gold_member_coverage\n"
        "params: { column: mbr_mbrshp_covrg_eff_dt, limit: '2026-06-30', format: yyyy-MM-dd }\n"
        "group_by: [src_sys_nm]\nowner: membership-gold\nstatus: approved\nseverity: medium\nfrequency: every_run\n",
        encoding="utf-8")
    create_table(spark, replay.gold, GOLD_COLUMNS, [gold_row(eff="2026-03-01"), gold_row(sub_id="2", eff="2026-09-01")])
    assert main(["validate", "--conf", str(replay.conf)]) == 0
    assert main(["dry-run", "--conf", str(replay.conf), "--feed", "example_realtime"]) == 0
    out = capsys.readouterr().out
    assert "does not parse" not in out
    assert main(["run", "--conf", str(replay.conf)]) == 0
    [r] = latest(spark, replay)[("gold_member_coverage", "eff_not_after")]
    assert (r.state, r.population, r.violations, r.severity) == ("FAILED", 2, 1, "medium")


# --- the guards: configuration SQL is a query, or nothing ---

@pytest.mark.parametrize("statement", ["INSERT INTO x SELECT 1", "drop table x", "SELECT 1; DELETE FROM x",
                                       "CREATE TABLE t AS SELECT 1", "MSCK REPAIR TABLE x", "ADD JAR x.jar"])
def test_forbidden_statements_are_named(statement) -> None:
    problem = forbidden_statement(f"SELECT 1 AS population, 0 AS violations\n-- then\n{statement}")
    assert problem is not None and problem.line == 3 and "read-only" in problem.problem


def test_a_select_with_a_function_called_replace_is_fine() -> None:
    assert forbidden_statement("SELECT replace(x, 'a', 'b') AS population, 0 AS violations FROM t\n"
                               "WHERE delete_flag = 'N' AND updated_at > '2026'") is None


CASES = [
    ("write", "  SELECT {% for g in group_by %}", "  DROP TABLE {{ table }}\n  SELECT {% for g in group_by %}",
     "sql", "DROP is not allowed", "query over the tables"),
    ("jinja", "{{ time(params.column, params.format) }}", "{{ time(params.column ", "sql", "template syntax", "Jinja"),
    ("bad-type", "limit: { type: string }", "limit: { type: date }", "params.limit.type", "not allowed", "use one of"),
    ("packaged-name", "template: not_after", "template: max_rows_per_key", "template", "is already a template",
     "pick another name"),
]


@pytest.mark.parametrize(("case", "old", "new", "field", "problem", "fix"), CASES, ids=[c[0] for c in CASES])
def test_template_file_errors_name_file_line_field_and_fix(conf: Path, case, old, new, field, problem, fix) -> None:
    path = conf / "templates" / "not_after.yaml"
    text = path.read_text(encoding="utf-8")
    assert old in text
    path.write_text(text.replace(old, new), encoding="utf-8")
    _, errors, _ = validate_conf(conf)
    [e] = [e for e in errors if e.file == str(path)]
    assert e.field == field and problem in e.problem and fix in e.fix
    assert e.line >= 1
    if case == "write":
        assert e.line == line_with(conf, "templates/not_after.yaml", "DROP TABLE")
    assert "max_rows_per_key" in TEMPLATES and TEMPLATES["max_rows_per_key"].related  # the packaged one is untouched


DEFAULT_CASES = [
    ("int-default", "limit: { type: string }", "limit: { type: int, default: soon }", "params.limit.default",
     "does not fit type int"),
    ("sql-default", "limit: { type: string }", "limit: { type: sql, default: \"SELECT TRANSFORM(1) USING 'cat'\" }",
     "params.limit.default", "script transform"),
    ("unbuildable", "limit: { type: string }", "model_config: { type: int }", "params", "cannot be built"),
]


@pytest.mark.parametrize(("case", "old", "new", "field", "problem"), DEFAULT_CASES, ids=[c[0] for c in DEFAULT_CASES])
def test_param_defaults_are_validated_and_scanned_and_never_a_traceback(conf: Path, case, old, new, field, problem) -> None:
    """A9, A10: a default that does not fit its type, a sql default carrying a forbidden word, or a
    params model that cannot be built are config errors on the template file, not run-time surprises."""
    path = conf / "templates" / "not_after.yaml"
    path.write_text(path.read_text(encoding="utf-8").replace(old, new), encoding="utf-8")
    _, errors, _ = validate_conf(conf)
    [e] = [e for e in errors if e.file == str(path)]
    assert e.field == field and problem in e.problem and e.line >= 1


@pytest.mark.parametrize("sql, category, code", [
    ("DROP TABLE {{ table }}", "CONFIGURATION", "invalid_config"),  # the rendered SQL is scanned: refused by word
    ("SET spark.sql.x = 1", "PLATFORM", "query_failed"),              # not a forbidden word: the wrap makes it a parse error
])
def test_a_command_in_a_registered_template_cannot_run(spark, sql, category, code) -> None:
    """Even a template that got past validation is scanned once rendered and runs wrapped as a
    subquery, so a command is refused or is a parse error, and the table is untouched."""
    from hcsc.datalake.dre.checks.rules.conf_template import spec_from

    create_table(spark, "ctpl.keep", GOLD_COLUMNS, [gold_row()])
    sneaky = TemplateFile(template="x_sneaky", population="rows", violation="rows", unit="rows",
                          sql="SELECT 1 AS population, 0 AS violations FROM {{ table }}")
    with TEMPLATES.temporary("x_sneaky", spec_from(sneaky.model_copy(update={"sql": sql}))):
        r = Rule(rule="x_r", template="x_sneaky", dataset="gold", owner="o", status="approved", severity="low")
        [result] = run_check(RuleCheck(r), CheckContext(spark, gold("ctpl.keep"), None, SETTINGS, "UTC"), EVENT)[0]
    assert (result.state, result.reason_category, result.reason_code) == ("DID_NOT_RUN", category, code)
    assert spark.catalog.tableExists("ctpl.keep") and spark.table("ctpl.keep").count() == 1


def test_the_rendered_sql_is_scanned_too(spark, conf: Path) -> None:
    """Rule 10 (spec section 9) holds for the SQL that runs, not only the template's text: a
    parameter value that puts a forbidden word into the rendered SQL makes the rule DID_NOT_RUN /
    invalid_config, naming the word and never the value; a clean value runs as before."""
    load(conf)
    create_table(spark, "ctpl.raw", GOLD_COLUMNS, [gold_row(), gold_row(sub_id="2")])

    def raw_rule(expr: str) -> Rule:
        return Rule(rule="raw_count", template="raw_expr", dataset="gold", params={"expr": expr},
                    owner="membership-gold", status="approved", severity="low")
    ctx = CheckContext(spark, gold("ctpl.raw"), None, SETTINGS, "UTC")
    [ok] = run_check(RuleCheck(raw_rule("SUM(CASE WHEN src_sys_nm = 'SRC_A' THEN 1 ELSE 0 END)")), ctx, EVENT)[0]
    assert (ok.state, ok.population, ok.violations) == ("FAILED", 2, 2)
    [refused] = run_check(RuleCheck(raw_rule("COUNT(*) + reflect")), ctx, EVENT)[0]
    assert (refused.state, refused.reason_category, refused.reason_code) == ("DID_NOT_RUN", "CONFIGURATION", "invalid_config")
    assert "REFLECT is not allowed" in refused.detail and "COUNT(*)" not in refused.detail
