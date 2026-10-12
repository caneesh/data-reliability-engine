"""max_rows_per_key: a key with more than `max` rows is a violation."""

from __future__ import annotations

from functools import partial

from pydantic import Field

from hcsc.datalake.dre.checks.base import render_sql
from hcsc.datalake.dre.checks.keys import key_exprs
from hcsc.datalake.dre.checks.rules.template import TemplateSpec, register_template
from hcsc.datalake.dre.config.models import Model


class MaxRowsPerKey(Model):
    max: int = Field(ge=0)


register_template(TemplateSpec(
    name="max_rows_per_key",
    params=MaxRowsPerKey,
    sql=partial(render_sql, "rule_max_rows_per_key.sql.j2"),
    columns=lambda ds, p: list(ds.key),
    arguments=lambda ds, p, related: {"key_expr": ", ".join(key_exprs(ds)), "max": p.max},
    what=lambda p: f"keys have more than {p.max} rows",
    unit="keys",
))
