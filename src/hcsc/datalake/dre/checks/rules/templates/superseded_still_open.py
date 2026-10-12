"""superseded_still_open: a row matching `open_when` while a row in the same group has a later
`order_column` is a violation."""

from __future__ import annotations

from functools import partial
from typing import Annotated

from pydantic import BeforeValidator, Field

from hcsc.datalake.dre.checks.base import render_sql
from hcsc.datalake.dre.checks.keys import column_expr
from hcsc.datalake.dre.checks.rules.template import TemplateSpec, one_or_more, register_template, time_expr
from hcsc.datalake.dre.config.models import Identifier, Model, NonEmptyStr, SqlFragment


class SupersededStillOpen(Model):
    group_key: Annotated[list[Identifier], Field(min_length=1), BeforeValidator(one_or_more)]
    order_column: Identifier
    open_when: SqlFragment
    format: NonEmptyStr | None = None  # parse order_column as a time with this format


register_template(TemplateSpec(
    name="superseded_still_open",
    params=SupersededStillOpen,
    sql=partial(render_sql, "rule_superseded_still_open.sql.j2"),
    columns=lambda ds, p: [*p.group_key, p.order_column],
    arguments=lambda ds, p, related: {
        "open_when": p.open_when, "order": time_expr(ds, p.order_column, p.format),
        "group_key": ", ".join(column_expr(ds, c) for c in p.group_key)},
    what=lambda p: "open rows have a later row in their group",
    unit="open rows",
))
