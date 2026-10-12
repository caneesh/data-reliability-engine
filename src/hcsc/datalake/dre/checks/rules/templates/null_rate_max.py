"""null_rate_max: a group whose null rate for `column` exceeds `max_rate` is a violation. The
result reports the rate, so it builds its own result rather than plain counts."""

from __future__ import annotations

from functools import partial
from typing import Any

from pydantic import Field

from hcsc.datalake.dre.checks.base import FAILED, PASSED, CheckResult, did_not_run, double, render_sql
from hcsc.datalake.dre.checks.rules.template import TemplateSpec, register_template
from hcsc.datalake.dre.config.models import Identifier, Model


class NullRateMax(Model):
    column: Identifier
    max_rate: float = Field(ge=0, le=1)


def _result(row: Any, p: NullRateMax, groups: dict[str, str | None]) -> CheckResult:
    if not row.population:
        return did_not_run("empty_population", group_values=groups)
    rate = (row.nulls or 0) / row.population
    return CheckResult(FAILED if row.violations else PASSED, population=row.population, violations=row.violations,
                       observed=f"{rate:.2%} null ({row.nulls or 0} of {row.population} rows)",
                       expected=f"at most {p.max_rate:.2%} null", group_values=groups)


register_template(TemplateSpec(
    name="null_rate_max",
    params=NullRateMax,
    sql=partial(render_sql, "rule_null_rate_max.sql.j2"),
    columns=lambda ds, p: [p.column],
    arguments=lambda ds, p, related: {"column": p.column, "max_rate": double(p.max_rate)},
    what=lambda p: f"rows have a null {p.column}",
    unit="rows",
    result=_result,
))
