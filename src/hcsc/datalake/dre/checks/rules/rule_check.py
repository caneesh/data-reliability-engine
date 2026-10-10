"""Gold rules from templates (spec sections 3 and 6).

Each rule file becomes one check (check_id = the rule id), run on the whole table after the
dataset's feed_filter, one result per the rule's group_by group. proposed and approved rules
both run and record results (the email shows proposed ones as report only); retired rules do
not run. Results carry the rule's severity.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.base import (
    FAILED, PASSED, Check, CheckContext, CheckResult, did_not_run, from_counts, group_values, render_sql,
)
from hcsc.datalake.dre.checks.keys import column_expr, key_exprs
from hcsc.datalake.dre.checks.times import sql_string
from hcsc.datalake.dre.config.models import TEMPLATE_PARAMS

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset, Rule


def time_expr(dataset: Dataset, column: str, fmt: str | None) -> str:
    """A column as a comparable value: parsed with fmt when given, never compared as a string."""
    expr = column_expr(dataset, column)
    return f"to_timestamp({expr}, {sql_string(fmt)})" if fmt else expr


class RuleCheck(Check):
    def __init__(self, rule: Rule, parent: Dataset | None = None) -> None:
        self.rule = rule
        self.check_id = rule.rule
        self.severity = rule.severity
        self.params: Any = TEMPLATE_PARAMS[rule.template].model_validate(rule.params)
        self.parent = parent  # child_within_parent only

    def required_columns(self, dataset: Dataset) -> list[str]:
        p, t = self.params, self.rule.template
        cols = list(self.rule.group_by)
        if t in ("max_rows_per_key", "max_open_rows_per_key"):
            cols += dataset.key
        elif t == "column_order":
            cols += [p.lower, p.upper]
        elif t == "superseded_still_open":
            cols += [*p.group_key, p.order_column]
        elif t == "child_within_parent":
            cols += [*p.join, p.child_start]
        elif t in ("value_format", "null_rate_max"):
            cols += [p.column]
        return cols

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        ds, p, t = ctx.dataset, self.params, self.rule.template
        base = {"table": ds.table, "feed_filter": ds.feed_filter, "group_by": self.rule.group_by}
        if t == "max_rows_per_key":
            sql = render_sql("rule_max_rows_per_key.sql.j2", key_expr=", ".join(key_exprs(ds)), max=p.max, **base)
            return self._counts(ctx, sql, "keys have more than {max} rows".format(max=p.max), "keys")
        if t == "max_open_rows_per_key":
            sql = render_sql("rule_max_open_rows_per_key.sql.j2", key_expr=", ".join(key_exprs(ds)),
                             open_when=p.open_when, max=p.max, **base)
            return self._counts(ctx, sql, f"keys have more than {p.max} open rows", "keys")
        if t == "column_order":
            sql = render_sql("rule_column_order.sql.j2", lower=time_expr(ds, p.lower, p.format),
                             upper=time_expr(ds, p.upper, p.format), nulls_fail=p.nulls_fail, **base)
            return self._counts(ctx, sql, f"rows have {p.upper} before {p.lower}", "rows")
        if t == "superseded_still_open":
            sql = render_sql("rule_superseded_still_open.sql.j2", open_when=p.open_when,
                             order=time_expr(ds, p.order_column, p.format),
                             group_key=", ".join(column_expr(ds, c) for c in p.group_key), **base)
            return self._counts(ctx, sql, "open rows have a later row in their group", "open rows")
        if t == "child_within_parent":
            if self.parent is None:
                return [did_not_run("invalid_config", detail=f"parent dataset {p.parent_dataset} is not configured")]
            par = self.parent
            sql = render_sql(
                "rule_child_within_parent.sql.j2", **base,
                parent_table=par.table, parent_filter=par.feed_filter,
                join=[(column_expr(ds, c), column_expr(par, pc)) for c, pc in p.join.items()],
                child_start=time_expr(ds, p.child_start, p.format),
                parent_start=time_expr(par, p.parent_start, p.format),
                parent_end=time_expr(par, p.parent_end, p.format),
            )
            return self._counts(ctx, sql, "child rows start outside their parent's window", "child rows")
        if t == "value_format":
            sql = render_sql("rule_value_format.sql.j2", column=column_expr(ds, p.column),
                             pattern=sql_string(p.pattern), **base)
            return self._counts(ctx, sql, f"values of {p.column} do not match the pattern", "values")
        if t == "null_rate_max":
            sql = render_sql("rule_null_rate_max.sql.j2", column=p.column, **base)
            return [self._null_rate(row, p.max_rate) for row in ctx.spark.sql(sql).collect()]
        raise ValueError(f"unknown template {t}")

    def _counts(self, ctx: CheckContext, sql: str, what: str, unit: str) -> list[CheckResult]:
        return [
            from_counts(row.population, row.violations,
                        observed=f"{row.violations or 0} of {row.population} {what}",
                        expected=f"0 {unit} breaking rule {self.rule.rule}",
                        group_values=group_values(row, self.rule.group_by))
            for row in ctx.spark.sql(sql).collect()
        ]

    def _null_rate(self, row: Any, max_rate: float) -> CheckResult:
        groups = group_values(row, self.rule.group_by)
        if not row.population:
            return did_not_run("empty_population", group_values=groups)
        rate = (row.nulls or 0) / row.population
        over = rate > max_rate
        return CheckResult(FAILED if over else PASSED, population=row.population, violations=1 if over else 0,
                           observed=f"{rate:.2%} null ({row.nulls or 0} of {row.population} rows)",
                           expected=f"at most {max_rate:.2%} null", group_values=groups)
