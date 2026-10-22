"""Gold rules from templates (spec sections 3 and 6).

Each rule file becomes one check (check_id = the rule id), run on the whole table after the
dataset's feed_filter, one result per the rule's group_by group. proposed and approved rules
both run and record results (the email shows proposed ones as report only); retired rules do
not run. Results carry the rule's severity. The template is a `TemplateSpec` from the template
registry (checks/rules/template.py); this class runs any of them the same way.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, did_not_run, from_counts, group_values
from hcsc.datalake.dre.checks.rules.conf_template import ForbiddenSql
from hcsc.datalake.dre.checks.rules.template import TEMPLATES

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset, Rule


class RuleCheck(Check):
    def __init__(self, rule: Rule, related: dict[str, Dataset] | None = None) -> None:
        self.rule = rule
        self.check_id = rule.rule
        self.severity = rule.severity
        self.spec = TEMPLATES[rule.template]
        self.params: Any = self.spec.params_model.model_validate(rule.params)
        self.related = related or {}  # other datasets the template names (spec.related), by id

    def required_columns(self, dataset: Dataset) -> list[str]:
        return [*self.rule.group_by, *self.spec.columns(dataset, self.params)]

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        ds, p, spec = ctx.dataset, self.params, self.spec
        missing = [d for d in spec.related(p).values() if d not in self.related]
        if missing:
            return [did_not_run("invalid_config", detail=f"dataset {missing[0]} named by rule {self.rule.rule} "
                                                        "is not configured")]
        try:
            sql = spec.sql(table=ds.table, feed_filter=ds.feed_filter, group_by=self.rule.group_by,
                           **spec.arguments(ds, p, self.related))
        except ForbiddenSql as exc:  # a parameter value put a forbidden word into the rendered SQL
            return [did_not_run("invalid_config", detail=f"rule {self.rule.rule}: {exc.problem.problem}; "
                                                        f"{exc.problem.fix}")]
        rows = ctx.spark.sql(sql).collect()
        if spec.result is not None:
            return [spec.result(row, p, group_values(row, self.rule.group_by)) for row in rows]
        return [
            from_counts(row.population, row.violations,
                        observed=f"{row.violations or 0} of {row.population} {spec.what(p)}",
                        expected=f"0 {spec.unit} breaking rule {self.rule.rule}",
                        group_values=group_values(row, self.rule.group_by))
            for row in rows
        ]
