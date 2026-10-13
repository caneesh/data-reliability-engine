"""column_order: a row where `upper` is before `lower` is a violation (nulls ignored unless
nulls_fail). With `format`, both columns are parsed as times first: never compared as strings."""

from __future__ import annotations

from functools import partial

from hcsc.datalake.dre.checks.base import render_sql
from hcsc.datalake.dre.checks.rules.template import TemplateSpec, register_template, time_expr
from hcsc.datalake.dre.config.models import Identifier, Model, NonEmptyStr


class ColumnOrder(Model):
    lower: Identifier
    upper: Identifier
    nulls_fail: bool = False
    format: NonEmptyStr | None = None  # parse both columns as times with this format (never compare as strings)


register_template(TemplateSpec(
    name="column_order",
    params_model=ColumnOrder,
    sql=partial(render_sql, "rule_column_order.sql.j2"),
    columns=lambda ds, p: [p.lower, p.upper],
    arguments=lambda ds, p, related: {"lower": time_expr(ds, p.lower, p.format),
                                      "upper": time_expr(ds, p.upper, p.format), "nulls_fail": p.nulls_fail},
    what=lambda p: f"rows have {p.upper} before {p.lower}",
    unit="rows",
))
