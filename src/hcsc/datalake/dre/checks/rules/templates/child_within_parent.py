"""child_within_parent: a child row whose start falls outside its parent's window is a violation.
The parent is another configured dataset (`parent_dataset`); its rows are matched through `join`."""

from __future__ import annotations

from functools import partial
from typing import Annotated, Any

from pydantic import Field

from hcsc.datalake.dre.checks.base import render_sql
from hcsc.datalake.dre.checks.keys import column_expr
from hcsc.datalake.dre.checks.rules.template import TemplateSpec, register_template, time_expr
from hcsc.datalake.dre.config.models import Dataset, Identifier, Model, NonEmptyStr


class ChildWithinParent(Model):
    parent_dataset: Identifier
    join: Annotated[dict[Identifier, Identifier], Field(min_length=1)]  # child column: parent column
    child_start: Identifier
    parent_start: Identifier
    parent_end: Identifier
    format: NonEmptyStr | None = None  # parse the three time columns with this format


def _arguments(ds: Dataset, p: ChildWithinParent, related: dict[str, Dataset]) -> dict[str, Any]:
    par = related[p.parent_dataset]
    return {
        "parent_table": par.table, "parent_filter": par.feed_filter,
        "join": [(column_expr(ds, c), column_expr(par, pc)) for c, pc in p.join.items()],
        "child_start": time_expr(ds, p.child_start, p.format),
        "parent_start": time_expr(par, p.parent_start, p.format),
        "parent_end": time_expr(par, p.parent_end, p.format),
    }


register_template(TemplateSpec(
    name="child_within_parent",
    params=ChildWithinParent,
    sql=partial(render_sql, "rule_child_within_parent.sql.j2"),
    columns=lambda ds, p: [*p.join, p.child_start],
    arguments=_arguments,
    what=lambda p: "child rows start outside their parent's window",
    unit="child rows",
    related=lambda p: {"parent_dataset": p.parent_dataset},
))
