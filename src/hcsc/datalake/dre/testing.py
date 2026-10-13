"""The testing kit for extensions (spec section 2, "Extension packages"): the contracts every
capability must meet, as functions an extension's tests call. The engine's own tests hold the
same expectations for the packaged checks, probes and templates.

Table builders stay out of this module: the engine's source may not create or drop tables
(guard rules), so an extension builds its synthetic tables itself (the engine's
tests/fixtures/layers.py is the example to copy) and passes datasets that point at them.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.base import DID_NOT_RUN, FAILED, PASSED, CheckContext, run_check
from hcsc.datalake.dre.checks.events import Event
from hcsc.datalake.dre.config.models import Settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.causes.base import CauseContext, Failure, Outcome
    from hcsc.datalake.dre.checks.base import Check, CheckResult
    from hcsc.datalake.dre.config.models import Dataset, Feed

STATES = (PASSED, FAILED, DID_NOT_RUN)
CAUSE_STATES = ("CONFIRMED", "RULED_OUT", "NOT_READY", "ERROR")


def settings(**overrides: Any) -> Settings:
    """Resolved settings with the spec's defaults, for a CheckContext built by hand."""
    values = {"sla_hours": 8, "email_sample_keys": False, "min_rows_per_load": 1, "volume_tolerance_pct": 50,
              "compute_budget_minutes": 8, "full_sweep_day": "SUNDAY", "settle_minutes": 15,
              "initial_lookback_hours": 24, "max_window_hours": 72, **overrides}
    return Settings(**values)


def event(dataset: str, start: datetime, end: datetime) -> Event:
    return Event(dataset, start.replace(tzinfo=start.tzinfo or timezone.utc), end.replace(tzinfo=end.tzinfo or timezone.utc))


def context(spark: SparkSession, dataset: Dataset, feed: Feed | None = None, **fields: Any) -> CheckContext:
    return CheckContext(spark, dataset, feed, fields.pop("settings", settings()), fields.pop("default_timezone", "UTC"),
                        **fields)


def _well_formed(result: CheckResult) -> None:
    assert result.state in STATES, f"state {result.state!r} is not one of {STATES}"
    if result.state == DID_NOT_RUN:
        assert result.reason_category and result.reason_code, "DID_NOT_RUN needs reason_category and reason_code"
    else:
        assert result.population, "PASSED and FAILED need a non-empty population (the denominator rule)"
        assert result.violations is not None and result.observed and result.expected, \
            "PASSED and FAILED carry violations, observed and expected"
    if result.state == FAILED:
        assert result.violations, "FAILED needs violations"
    if result.state == PASSED:
        assert not result.violations, "PASSED has no violations"


def check_contract(spark: SparkSession, check: Check, failing: Dataset, passing: Dataset, empty: Dataset,
                   evt: Event, feed: Feed | None = None, **fields: Any) -> dict[str, list[CheckResult]]:
    """A check can FAIL, can PASS, and is DID_NOT_RUN / empty_population on an empty population
    (never PASSED). Returns the results by scenario for further assertions."""
    found = {}
    for name, dataset, want in (("failing", failing, FAILED), ("passing", passing, PASSED), ("empty", empty, DID_NOT_RUN)):
        results, _ = run_check(check, context(spark, dataset, feed, **fields), evt)
        assert results, f"{name}: a check returns at least one result"
        for result in results:
            _well_formed(result)
        states = {r.state for r in results}
        assert want in states, f"{name}: expected a {want} result, got {sorted(states)}"
        if name == "empty":
            assert all(r.reason_code == "empty_population" for r in results if r.state == DID_NOT_RUN), \
                "an empty population is DID_NOT_RUN / empty_population"
            assert PASSED not in states, "an empty population never PASSES"
        found[name] = results
    return found


def probe_contract(cx: CauseContext, kind: str, params_key: str, failure: Failure) -> Outcome:
    """A probe is NOT_READY when its parameters are not configured, never raises out of the
    engine, returns one of the four cause states, and its evidence is JSON with no key values."""
    from hcsc.datalake.dre.causes.engine import run_entry
    from hcsc.datalake.dre.checks.patterns import CauseEntry

    unready = run_entry(cx, CauseEntry("X_CONTRACT", kind, "x_probe_contract_unconfigured"), failure)
    assert unready.state == "NOT_READY", f"an unconfigured probe is NOT_READY, not {unready.state} ({unready.evidence})"
    outcome = run_entry(cx, CauseEntry("X_CONTRACT", kind, params_key), failure)
    assert outcome.state in CAUSE_STATES, f"state {outcome.state!r} is not one of {CAUSE_STATES}"
    text = json.dumps(outcome.evidence, default=str)
    assert "key_value" not in text, "evidence never carries key values"
    if failure.key is not None and "k" in failure.key:
        assert str(failure.key["k"]) not in text, "evidence never carries the key itself"
    return outcome


def template_contract(spark: SparkSession, template: str, dataset: Dataset, params: dict[str, Any],
                      group_by: list[str] | None = None, related: dict[str, Dataset] | None = None) -> list[str]:
    """A rule template's SQL is a query that returns population and violations (and g_1.. for
    group_by) against the dataset's table. Returns the result columns."""
    from hcsc.datalake.dre.checks.rules.rule_check import RuleCheck
    from hcsc.datalake.dre.checks.rules.template import TEMPLATES
    from hcsc.datalake.dre.config.models import Rule

    spec = TEMPLATES[template]
    rule = Rule(rule="x_template_contract", template=template, dataset=dataset.dataset, params=params,
                group_by=group_by or [], owner="x", status="approved", severity="low")
    check = RuleCheck(rule, related)
    sql = spec.sql(table=dataset.table, feed_filter=dataset.feed_filter, group_by=rule.group_by,
                   **spec.arguments(dataset, check.params, check.related))
    columns = spark.sql(f"SELECT * FROM ({sql}) dre_contract LIMIT 0").columns
    assert "population" in columns and "violations" in columns, f"columns {columns} lack population or violations"
    for i in range(1, len(rule.group_by) + 1):
        assert f"g_{i}" in columns, f"group_by needs g_{i} in the result"
    return columns
