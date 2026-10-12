"""Check parameters from configuration (spec section 3, `check_params`): resolution order,
validation with file and line, DID_NOT_RUN on bad values, and a check's own failures hook."""

from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from hcsc.datalake.dre.causes.base import CauseContext, Failure
from hcsc.datalake.dre.causes.failures import failures_for
from hcsc.datalake.dre.checks import registry
from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, configured_params, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.checks.registry import CHECKS, checks_for
from hcsc.datalake.dre.config.loader import load
from hcsc.datalake.dre.config.models import Cadence, Dataset, Feed
from hcsc.datalake.dre.config.validate import validate_conf
from tests.config.conftest import SAMPLE_CONF, edit, line_with
from tests.fixtures.layers import GOLD_COLUMNS, create_table, gold_row
from tests.fixtures.settings import SETTINGS

UTC = timezone.utc
EVENT = Event("gold", datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 1, 16, 12, tzinfo=UTC))
GOLD = "datasets/gold_member_coverage.yaml"
FEED = "feeds/example_realtime.yaml"


@pytest.fixture
def conf(tmp_path: Path) -> Path:
    target = tmp_path / "conf"
    shutil.copytree(SAMPLE_CONF, target)
    return target


def gold(table: str = "x.y", **extra) -> Dataset:
    return Dataset(dataset="gold", table=table, layer="GOLD", key=["sub_id"], record_time={"column": "src_lcts"},
                   load_time={"column": "gld_lcts", "format": "yyyy-MM-dd HH:mm:ss:SSSSSS", "granularity": "minute"},
                   **extra)


def feed(**extra) -> Feed:
    return Feed(feed="f", expectation_version=1, pattern="TABLE_MERGE", owner="o", datasets=["gold"],
                cadence=Cadence(kind="interval", interval_minutes=60, timezone="UTC", calendar="EVERYDAY"), **extra)


@pytest.fixture
def pattern_defaults(monkeypatch):
    """A TABLE_MERGE pattern giving T1_FRESHNESS defaults."""
    monkeypatch.setattr(registry, "_load_pattern", lambda pattern: yaml.safe_load(
        "pattern: TABLE_MERGE\nchecks:\n  - T1_KEY_NULLS\n  - {check: T1_FRESHNESS, params: {max_age_hours: 6}}\n"
        "  - T1_COLUMN_RANGE\n"))
    registry._pattern_checks.cache_clear()
    yield
    registry._pattern_checks.cache_clear()


def test_params_resolve_pattern_then_feed_then_dataset(pattern_defaults) -> None:
    check = CHECKS["T1_FRESHNESS"]
    assert configured_params(check, gold(), None, "TABLE_MERGE") == {"max_age_hours": 6}
    assert configured_params(check, gold(), feed(check_params={"T1_FRESHNESS": {"time": "load"}}), "TABLE_MERGE") == {
        "max_age_hours": 6, "time": "load"}
    assert configured_params(check, gold(check_params={"T1_FRESHNESS": {"max_age_hours": 1}}),
                             feed(check_params={"T1_FRESHNESS": {"time": "load"}}), "TABLE_MERGE") == {
        "max_age_hours": 1, "time": "load"}
    # nothing configures T1_COLUMN_RANGE, so it does not apply; the pattern's defaults make T1_FRESHNESS apply
    assert [c.check_id for c in checks_for(gold(), "TABLE_MERGE")] == ["T1_KEY_NULLS", "T1_FRESHNESS"]
    assert configured_params(CHECKS["T1_COLUMN_RANGE"], gold(), None, "TABLE_MERGE") is None
    ds = gold(check_params={"T1_COLUMN_RANGE": {"column": "mem_nbr", "min": 1}})
    assert [c.check_id for c in checks_for(ds, "TABLE_MERGE")] == ["T1_KEY_NULLS", "T1_FRESHNESS", "T1_COLUMN_RANGE"]
    assert [c.check_id for c in checks_for(gold(), "TABLE_MERGE", feed(check_params={"T1_COLUMN_RANGE": {
        "column": "mem_nbr", "max": 9}}))][-1] == "T1_COLUMN_RANGE"


@pytest.mark.parametrize(("entry", "message"), [
    ("{check: T1_ON_TIME, params: {x: 1}}", "takes no parameters"),
    ("{check: T1_FRESHNESS, params: {max_age_hours: -1}}", "params are not valid"),
    ("{check: T1_NOPE}", "unknown checks"),
    ("T1_KEY_NULLS", "lists a check twice"),
])
def test_malformed_pattern_entries_are_rejected(monkeypatch, entry, message) -> None:
    monkeypatch.setattr(registry, "_load_pattern", lambda pattern: yaml.safe_load(
        f"pattern: TABLE_MERGE\nchecks:\n  - T1_KEY_NULLS\n  - {entry}\n"))
    registry._pattern_checks.cache_clear()
    try:
        with pytest.raises(ValueError, match=message):
            registry.pattern_check_ids("TABLE_MERGE")
    finally:
        registry._pattern_checks.cache_clear()


def _errors(conf: Path) -> tuple[list, list]:
    _, errors, warnings = validate_conf(conf)
    return errors, warnings


def test_sample_config_with_params_validates(conf: Path) -> None:
    errors, _ = _errors(conf)
    assert errors == [], "\n".join(map(str, errors))


SAMPLE_ENTRY = "  T1_FRESHNESS: { max_age_hours: 72, time: load }"  # the sample gold dataset's check_params entry
CASES = [
    ("unknown-check", GOLD, "  T1_NOPE: { max_age_hours: 1 }", "check_params.T1_NOPE",
     "unknown check 'T1_NOPE'", "T1_FRESHNESS"),
    ("no-params", GOLD, "  T1_ON_TIME: { x: 1 }", "check_params.T1_ON_TIME", "takes no parameters", "remove it"),
    ("bad-value", GOLD, "  T1_FRESHNESS: { max_age_hours: -1 }", "check_params.T1_FRESHNESS.max_age_hours",
     "greater than 0", "0"),
    ("missing-field", GOLD, "  T1_COLUMN_RANGE: { min: 1 }", "check_params.T1_COLUMN_RANGE.column",
     "`column` is not set at any level", "in the feed or this dataset"),
    ("no-bound", GOLD, "  T1_COLUMN_RANGE: { column: mem_nbr }", "check_params.T1_COLUMN_RANGE",
     "needs a bound", "set min, max or allowed"),
    ("feed-bad-value", FEED, "check_params:\n  T1_FRESHNESS: { max_age_hours: never }",
     "check_params.T1_FRESHNESS.max_age_hours", "number", "number"),
]


@pytest.mark.parametrize(("case", "rel", "block", "field", "problem", "fix"), CASES, ids=[c[0] for c in CASES])
def test_check_params_errors_name_file_line_field_and_fix(conf: Path, case, rel, block, field, problem, fix) -> None:
    path = conf / rel
    if rel == GOLD:
        edit(conf, GOLD, SAMPLE_ENTRY, block)  # replace the sample entry under the existing check_params block
    else:
        path.write_text(path.read_text(encoding="utf-8") + block + "\n", encoding="utf-8")
    errors, _ = _errors(conf)
    matching = [e for e in errors if e.field == field and problem in e.problem]
    assert matching, "\n".join(map(str, errors))
    assert fix in matching[0].fix and matching[0].file == str(path)
    assert matching[0].line >= line_with(conf, rel, "check_params")


def test_a_missing_field_set_in_the_feed_is_fine(conf: Path) -> None:
    edit(conf, GOLD, SAMPLE_ENTRY, "  T1_COLUMN_RANGE: { min: 1 }")
    (conf / FEED).write_text((conf / FEED).read_text() + "check_params:\n  T1_COLUMN_RANGE: { column: mem_nbr }\n")
    errors, _ = _errors(conf)
    assert errors == [], "\n".join(map(str, errors))
    config, _ = load(conf)
    ds, fd = config.datasets["gold_member_coverage"], config.feeds["example_realtime"]
    assert configured_params(CHECKS["T1_COLUMN_RANGE"], ds, fd, fd.pattern) == {"column": "mem_nbr", "min": 1}
    assert "T1_COLUMN_RANGE" in [c.check_id for c in checks_for(ds, fd.pattern, fd)]


def test_a_check_the_pattern_does_not_list_is_a_warning(conf: Path, monkeypatch) -> None:
    monkeypatch.setattr(registry, "_load_pattern", lambda pattern: yaml.safe_load("pattern: x\nchecks: [T1_KEY_NULLS]\n"))
    registry._pattern_checks.cache_clear()
    try:
        edit(conf, GOLD, SAMPLE_ENTRY, "  T1_FRESHNESS: { max_age_hours: 1 }")
        errors, warnings = _errors(conf)
        assert not [e for e in errors if "check_params" in e.field]
        [w] = [w for w in warnings if w.field == "check_params.T1_FRESHNESS"]
        assert "does not list T1_FRESHNESS" in w.problem
    finally:
        registry._pattern_checks.cache_clear()


def test_bad_params_at_run_time_are_did_not_run(spark) -> None:
    create_table(spark, "params.g", GOLD_COLUMNS, [gold_row()])
    ds = gold(table="params.g", check_params={"T1_FRESHNESS": {"max_age_hours": "soon"}})
    [r], _ = run_check(CHECKS["T1_FRESHNESS"], CheckContext(spark, ds, None, SETTINGS, "UTC"), EVENT)
    assert (r.state, r.reason_code) == ("DID_NOT_RUN", "invalid_config")
    assert "check_params.T1_FRESHNESS" in r.detail and "max_age_hours" in r.detail and "soon" not in r.detail


class ListsFailures(Check):
    check_id = "X_LISTS"

    def required_columns(self, dataset, params=None):
        return []

    def evaluate(self, ctx, event):
        return []

    def failures(self, cx: CauseContext, result: CheckResult) -> list[Failure] | None:
        return [Failure(ref=f"item-{i}") for i in range(150)]


def test_a_check_lists_its_own_failing_things(spark, conf: Path) -> None:
    config, _ = load(conf)
    ds = config.datasets["gold_member_coverage"]
    result = CheckResult("FAILED", population=1, violations=1)
    with CHECKS.temporary("X_LISTS", ListsFailures()):
        cx = CauseContext(CheckContext(spark, ds, None, SETTINGS, "UTC"), EVENT, config, "X_LISTS")
        refs = [f.ref for f in failures_for(cx, result)]
    assert refs[:2] == ["item-0", "item-1"] and len(refs) == 100  # capped like every other list
    cx = CauseContext(CheckContext(spark, ds, None, SETTINGS, "UTC"), EVENT, config, "T1_KEY_NULLS")
    assert failures_for(cx, result) == []  # a packaged check without causes still lists nothing
