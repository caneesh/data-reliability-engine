"""Extension points (spec section 2): checks, cause probes, builtin causes, rule templates and
key normalisers register themselves where they are defined, and the engine runs them through
the same paths as the packaged ones. Each test here adds one from outside the engine."""

from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from hcsc.datalake.dre.causes.base import BUILTINS, CauseContext, Failure, Outcome, builtin, confirmed, ruled_out
from hcsc.datalake.dre.causes.engine import run_entry
from hcsc.datalake.dre.checks import patterns as pattern_files
from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, from_counts, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.keys import NORMALISERS, Normaliser, NormaliserError, key_exprs, register_normaliser
from hcsc.datalake.dre.checks.registry import CHECKS, register_check
from hcsc.datalake.dre.checks.patterns import CauseEntry, checks_for, pattern_check_ids, patterns
from hcsc.datalake.dre.checks.rules.rule_check import RuleCheck
from hcsc.datalake.dre.checks.rules.template import TEMPLATES, TemplateSpec, register_template
from hcsc.datalake.dre.config.loader import load
from hcsc.datalake.dre.config.models import Dataset, Model, Rule
from hcsc.datalake.dre.config.probes import PROBES, Probe, probe
from hcsc.datalake.dre.config.validate import validate_conf
from hcsc.datalake.dre.registry import Registry
from tests.config.conftest import SAMPLE_CONF, edit
from tests.fixtures.layers import GOLD_COLUMNS, create_table, gold_row
from tests.fixtures.settings import SETTINGS

UTC = timezone.utc
EVENT = Event("gold", datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 1, 16, 12, tzinfo=UTC))


@pytest.fixture
def conf(tmp_path: Path) -> Path:
    """A writable copy of the sample configuration."""
    target = tmp_path / "conf"
    shutil.copytree(SAMPLE_CONF, target)
    return target


def gold(table: str, **extra: Any) -> Dataset:
    return Dataset(dataset="gold", table=table, layer="GOLD", key=["sub_id", "mem_nbr"],
                   key_normalise={"sub_id": "strip_leading_zeros"}, **extra)


# --- the registries themselves ---

def test_the_built_ins_are_discovered_without_a_central_list() -> None:
    assert CHECKS.ids() == ("HOP_CONTRIBUTION", "HOP_FILE_COMPLETENESS", "HOP_KEY_CURRENCY", "HOP_LEDGER",
                            "HOP_VALUE_AGREEMENT", "T1_COLUMN_RANGE", "T1_FILES_NOT_LOADED", "T1_FRESHNESS",
                            "T1_KEY_DUPLICATES", "T1_KEY_NULLS", "T1_ON_TIME", "T1_PARTITION_DRIFT", "T1_SCHEMA_DRIFT",
                            "T1_SOURCE_ATTRIBUTION", "T1_VOLUME", "T1_ZERO_ROWS")
    assert patterns() == ("FILE_CYCLIC", "FILE_PERIODIC", "TABLE_MERGE")
    assert PROBES.ids() == ("file_exists", "file_value_compare", "log_contains", "size_changed", "table_contains")
    assert BUILTINS.ids() == ("FILE_SKIPPED", "INVALID_KEY", "JOIN_UNMATCHED", "KEY_MISMATCH", "NOT_RUN",
                              "NO_UPSTREAM_DATA", "OLDER_VERSION_WRITTEN_LATER", "PIPELINE_STALLED",
                              "TIE_RESOLVED_BY_RULE", "UNREADABLE")
    assert TEMPLATES.ids() == ("child_within_parent", "column_order", "max_open_rows_per_key", "max_rows_per_key",
                               "null_rate_max", "superseded_still_open", "value_format")
    assert NORMALISERS.ids() == ("lower", "parse_date", "regex_replace", "strip_leading_zeros", "trim", "upper")


def test_a_failing_discovery_import_is_raised_and_tried_again() -> None:
    """A registry whose built-in package fails to import raises on every use, never a partial list."""
    reg: Registry[int] = Registry("thing", ("tests.no_such_package_for_registry",))
    with pytest.raises(ModuleNotFoundError):
        reg.ids()
    with pytest.raises(ModuleNotFoundError):
        "a" in reg


def test_an_id_registered_twice_is_an_error() -> None:
    reg: Registry[int] = Registry("thing")
    reg.register("a", 1)
    with pytest.raises(ValueError, match="registered twice"):
        reg.register("a", 2)
    with pytest.raises(ValueError, match="needs an id"):
        reg.register("", 3)
    with pytest.raises(KeyError, match="unknown thing 'b'"):
        reg["b"]
    with reg.temporary("b", 2):
        assert reg["b"] == 2 and "b" in reg
    assert "b" not in reg and reg.ids() == ("a",)
    for live, item in ((CHECKS, CHECKS["T1_ON_TIME"]), (TEMPLATES, TEMPLATES["value_format"])):
        with pytest.raises(ValueError, match="registered twice"):
            live.register(live.ids()[0], item)


# --- a check from its own module ---

class RowsAtMost(Check):
    """A test check: FAILED when the table holds more than 2 rows."""

    check_id = "X_ROWS_AT_MOST"

    def required_columns(self, dataset: Dataset) -> list[str]:
        return []

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        [row] = ctx.spark.sql(f"SELECT COUNT(*) AS n FROM {ctx.dataset.table}").collect()
        return [from_counts(row.n, max(row.n - 2, 0), observed=f"{row.n} rows", expected="at most 2 rows")]


def test_a_check_registers_from_its_own_module_and_a_pattern_lists_it(spark, monkeypatch) -> None:
    with CHECKS.temporary("X_ROWS_AT_MOST", RowsAtMost()):
        monkeypatch.setattr(pattern_files, "_load_pattern", lambda pattern: yaml.safe_load(
            "pattern: FILE_CYCLIC\nchecks: [T1_ON_TIME, X_ROWS_AT_MOST]\n"))
        pattern_files.clear_pattern_caches()
        try:
            assert [c.check_id for c in checks_for(gold("x.y"), "FILE_CYCLIC")] == ["T1_ON_TIME", "X_ROWS_AT_MOST"]
        finally:
            pattern_files.clear_pattern_caches()
        check = CHECKS["X_ROWS_AT_MOST"]
        create_table(spark, "ext.three", GOLD_COLUMNS, [gold_row(sub_id=s) for s in ("1", "2", "3")])
        create_table(spark, "ext.one", GOLD_COLUMNS, [gold_row(sub_id="1")])
        create_table(spark, "ext.none", GOLD_COLUMNS, [])
        run = lambda t: run_check(check, CheckContext(spark, gold(t), None, SETTINGS, "UTC"), EVENT)[0][0]  # noqa: E731
        assert (run("ext.three").state, run("ext.three").violations) == ("FAILED", 1)
        assert run("ext.one").state == "PASSED"
        assert (run("ext.none").state, run("ext.none").reason_code) == ("DID_NOT_RUN", "empty_population")
    pattern_files.clear_pattern_caches()
    with pytest.raises(ValueError, match="unknown checks"):
        pattern_check_ids("FILE_CYCLIC")  # the pattern still names the check, which is gone again
    pattern_files.clear_pattern_caches()
    with pytest.raises(ValueError, match="registered twice"):
        register_check(CHECKS["T1_ON_TIME"])


# --- a probe and a builtin cause through the cause engine ---

class Flag(Model):
    value: str | None = None


@pytest.fixture
def cause_context(spark, conf: Path) -> CauseContext:
    config, errors = load(conf)
    assert not errors
    feed = config.feeds["example_realtime"].model_copy(update={"probes": {"flag": {"value": "yes"}, "unset": None,
                                                                          "two": [{"value": "no"}, {}]}})
    ds = config.datasets["gold_member_coverage"]
    return CauseContext(CheckContext(spark, ds, feed, SETTINGS, "UTC"), EVENT, config, "T1_ZERO_ROWS")


def test_a_probe_registers_with_its_parameters_and_runs_through_the_engine(cause_context) -> None:
    def flagged(cx: CauseContext, params: Flag, failure: Failure) -> Outcome:
        return confirmed(value=params.value) if params.value == "yes" else ruled_out(value=params.value)

    def broken(cx: CauseContext, params: Flag, failure: Failure) -> Outcome:
        raise RuntimeError("secret-looking message")

    failure = Failure(ref="2026-01-15T12:00:00+00:00", slot=EVENT.window_start)
    with PROBES.temporary("x_flag", Probe("x_flag", Flag, flagged)), PROBES.temporary("x_broken", Probe("x_broken", Flag, broken)):
        assert run_entry(cause_context, CauseEntry("X", "x_flag", "flag"), failure).state == "CONFIRMED"
        assert run_entry(cause_context, CauseEntry("X", "x_flag", "unset"), failure).state == "NOT_READY"
        assert run_entry(cause_context, CauseEntry("X", "x_flag", "missing"), failure).state == "NOT_READY"
        two = run_entry(cause_context, CauseEntry("X", "x_flag", "two"), failure)  # one ruled out, one not configured
        assert (two.state, two.evidence["reason"]) == ("NOT_READY", "1 of 2 parameter sets of probes.two are not configured")
        out = run_entry(cause_context, CauseEntry("X", "x_broken", "flag"), failure)
        assert (out.state, out.evidence) == ("ERROR", {"error": "RuntimeError"})  # the message never goes in
    assert "x_flag" not in PROBES


def test_a_probe_decorator_registers_once(cause_context) -> None:
    @probe("x_once", Flag)
    def once(cx: CauseContext, params: Flag, failure: Failure) -> Outcome:
        return confirmed()
    try:
        assert PROBES["x_once"].run is once and PROBES["x_once"].params_model is Flag
        with pytest.raises(ValueError, match="registered twice"):
            probe("x_once", Flag)(once)
    finally:
        PROBES.unregister("x_once")


def test_a_builtin_cause_registers_by_code(cause_context) -> None:
    @builtin("X_ALWAYS")
    def always(cx: CauseContext, failure: Failure) -> Outcome:
        return confirmed(slot=failure.ref)
    try:
        failure = Failure(ref="2026-01-15T12:00:00+00:00", slot=EVENT.window_start)
        out = run_entry(cause_context, CauseEntry("X_ALWAYS", "builtin", None), failure)
        assert (out.state, out.evidence) == ("CONFIRMED", {"slot": failure.ref})
        assert run_entry(cause_context, CauseEntry("X_UNKNOWN", "builtin", None), failure).state == "ERROR"
    finally:
        BUILTINS.unregister("X_ALWAYS")


def test_a_pattern_naming_an_unknown_builtin_is_rejected(monkeypatch) -> None:
    monkeypatch.setattr(pattern_files, "_load_pattern", lambda pattern: yaml.safe_load(
        "causes:\n  x:\n    checks: [T1_ON_TIME]\n    order:\n      - {code: NOT_A_CAUSE, probe: builtin}\n"
        "      - {code: B, fallback: true}\n"))
    pattern_files.clear_pattern_caches()
    try:
        with pytest.raises(ValueError, match="names no builtin cause check"):
            pattern_files.pattern_causes("FILE_CYCLIC")
    finally:
        pattern_files.clear_pattern_caches()


# --- a rule template from its own module ---

class AtMostRows(Model):
    max: int


X_TEMPLATE = TemplateSpec(
    name="x_at_most_rows",
    params_model=AtMostRows,
    sql=lambda table, feed_filter, group_by, max: (
        f"SELECT {''.join(f'{g} AS g_{i}, ' for i, g in enumerate(group_by, 1))}"
        f"COUNT(*) AS population, CASE WHEN COUNT(*) > {max} THEN COUNT(*) - {max} ELSE 0 END AS violations "
        f"FROM {table}{' WHERE ' + feed_filter if feed_filter else ''}"
        f"{' GROUP BY ' + ', '.join(group_by) if group_by else ''}"),
    columns=lambda ds, p: [],
    arguments=lambda ds, p, related: {"max": p.max},
    what=lambda p: f"rows over the {p.max} allowed",
    unit="rows",
)


def test_a_rule_template_registers_from_its_own_module(spark) -> None:
    def rule(max_rows: int) -> Rule:
        return Rule(rule="x_small_table", template="x_at_most_rows", dataset="gold", params={"max": max_rows},
                    group_by=["src_sys_nm"], owner="membership-gold", status="approved", severity="low")

    with pytest.raises(ValueError, match="'x_at_most_rows' is not allowed"):
        rule(1)  # not registered yet: a config error with the known templates in its fix
    create_table(spark, "ext.rows", GOLD_COLUMNS, [gold_row(sub_id="1"), gold_row(sub_id="2"),
                                                   gold_row(sub_id="3", source="SRC_B")])
    with TEMPLATES.temporary("x_at_most_rows", X_TEMPLATE):
        check = RuleCheck(rule(1))
        results, _ = run_check(check, CheckContext(spark, gold("ext.rows"), None, SETTINGS, "UTC"), EVENT)
        assert all(r.group_values for r in results), results
        by_group = {r.group_values["src_sys_nm"]: (r.state, r.population, r.violations, r.observed) for r in results}
        assert by_group == {"SRC_A": ("FAILED", 2, 1, "1 of 2 rows over the 1 allowed"),
                            "SRC_B": ("PASSED", 1, 0, "0 of 1 rows over the 1 allowed")}
        assert {r.expected for r in results} == {"0 rows breaking rule x_small_table"}
        [r] = run_check(RuleCheck(rule(5)), CheckContext(spark, gold("ext.rows", feed_filter="1 = 0"), None, SETTINGS,
                                                        "UTC"), EVENT)[0]
        assert (r.state, r.reason_code) == ("DID_NOT_RUN", "empty_population")
        assert check.required_columns(gold("ext.rows")) == ["src_sys_nm"]
    with pytest.raises(ValueError, match="not allowed"):
        rule(1)


def test_register_template_rejects_a_second_registration() -> None:
    with TEMPLATES.temporary("x_at_most_rows", X_TEMPLATE):
        with pytest.raises(ValueError, match="registered twice"):
            register_template(X_TEMPLATE)


def test_a_template_naming_a_missing_related_dataset_is_a_config_error(conf: Path) -> None:
    (conf / "rules" / "x_parent.yaml").write_text(
        "rule: x_parent\ntemplate: child_within_parent\ndataset: gold_member_coverage\n"
        "params: { parent_dataset: nowhere, join: { sub_id: sub_id }, child_start: a, parent_start: b, parent_end: c }\n"
        "owner: membership-gold\nstatus: approved\nseverity: low\n", encoding="utf-8")
    _, errors, _ = validate_conf(conf)
    [e] = [e for e in errors if e.field == "params.parent_dataset"]
    assert "unknown dataset 'nowhere'" in e.problem and "fix parent_dataset" in e.fix and e.line >= 1


# --- key normalisers ---

def _value(spark, ds: Dataset, column: str, literal: str) -> Any:
    [expr] = [e for c, e in zip(ds.key, key_exprs(ds)) if c == column]
    return spark.sql(f"SELECT {expr.replace(column, repr(literal))} AS v").collect()[0].v


@pytest.mark.parametrize(("normaliser", "value", "expected"), [
    ("strip_leading_zeros", "000123", "123"),
    ("trim", "  ab ", "ab"),
    ("upper", "aBc", "ABC"),
    ("lower", "aBc", "abc"),
    ({"parse_date": "MM/dd/yyyy"}, "01/02/2026", "2026-01-02"),
    ({"regex_replace": {"pattern": "-", "replacement": ""}}, "12-34-56", "123456"),
    ({"regex_replace": {"pattern": "^(\\w+)@.*$", "replacement": "$1"}}, "ann@x", "ann"),
    ({"regex_replace": {"pattern": "'", "replacement": "_"}}, "it's", "it_s"),  # a quote in the pattern is escaped
])
def test_the_packaged_normalisers(spark, normaliser, value, expected) -> None:
    ds = Dataset(dataset="d", table="x.y", layer="GOLD", key=["k"], key_normalise={"k": normaliser})
    assert _value(spark, ds, "k", value) == expected


@pytest.mark.parametrize("value", [
    "parse_date", {"parse_date": ""}, {"parse_date": "MM'dd"}, {"parse_date": "MM/dd/yyyy", "other": 1},
    "reverse", {"trim": "x"}, {"regex_replace": {"pattern": "-"}}, {"regex_replace": {"pattern": "(", "replacement": ""}}, 7,
])
def test_bad_normalisers_are_config_errors(value) -> None:
    with pytest.raises(ValueError, match="not allowed"):
        Dataset(dataset="d", table="x.y", layer="GOLD", key=["k"], key_normalise={"k": value})


def test_a_normaliser_registers_from_its_own_module(spark, conf: Path) -> None:
    with NORMALISERS.temporary("x_reverse", Normaliser("x_reverse", lambda c, a: f"reverse({c})", "x_reverse")):
        ds = Dataset(dataset="d", table="x.y", layer="GOLD", key=["k"], key_normalise={"k": "x_reverse"})
        assert _value(spark, ds, "k", "abc") == "cba"
        with pytest.raises(ValueError, match="x_reverse takes no argument"):
            Dataset(dataset="d", table="x.y", layer="GOLD", key=["k"], key_normalise={"k": {"x_reverse": 1}})
    with pytest.raises(ValueError, match="not allowed"):
        Dataset(dataset="d", table="x.y", layer="GOLD", key=["k"], key_normalise={"k": "x_reverse"})
    with pytest.raises(ValueError, match="registered twice"):
        register_normaliser(NORMALISERS["trim"])
    # the error names the file and line, and the fix lists the known forms
    edit(conf, "datasets/gold_member_coverage.yaml", "strip_leading_zeros", "x_reverse")
    _, errors, _ = validate_conf(conf)
    [e] = [e for e in errors if "key_normalise" in e.field]
    assert "'x_reverse' is not allowed" in e.problem and "{ parse_date: MM/dd/yyyy }" in e.fix and e.line >= 1


def test_normaliser_error_carries_its_fix() -> None:
    with pytest.raises(NormaliserError) as info:
        from hcsc.datalake.dre.checks.keys import validate_normaliser
        validate_normaliser({"regex_replace": {"pattern": "a", "replacement": 1}})
    assert "needs pattern and replacement" in info.value.problem and "write" in info.value.fix
