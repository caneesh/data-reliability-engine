"""value_format: a non-null value not matching the regex is a violation. The stored value is
checked, not its key_normalise form: the rule is about how values are written."""

from __future__ import annotations

from functools import partial

from hcsc.datalake.dre.checks.base import render_sql
from hcsc.datalake.dre.checks.rules.template import TemplateSpec, register_template
from hcsc.datalake.dre.checks.times import sql_string
from hcsc.datalake.dre.config.models import Identifier, Model, Regex


class ValueFormat(Model):
    column: Identifier
    pattern: Regex


register_template(TemplateSpec(
    name="value_format",
    params_model=ValueFormat,
    sql=partial(render_sql, "rule_value_format.sql.j2"),
    columns=lambda ds, p: [p.column],
    arguments=lambda ds, p, related: {"column": p.column, "pattern": sql_string(p.pattern)},
    what=lambda p: f"values of {p.column} do not match the pattern",
    unit="values",
))
