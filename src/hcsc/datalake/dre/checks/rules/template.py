"""Gold rule templates as records (spec sections 3 and 6).

A template is a `TemplateSpec`: its params model, how it renders its SQL, which columns it
reads, and the text of its result. Each packaged template registers itself from its module
under checks/rules/templates/; `RuleCheck` runs any of them the same way. The SQL must return
`population` and `violations` (and g_1.. for the rule's group_by), so every template follows
the denominator rule through `from_counts`, unless it supplies its own `result` builder.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from hcsc.datalake.dre.checks.keys import column_expr
from hcsc.datalake.dre.checks.times import sql_string
from hcsc.datalake.dre.registry import Registry

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.base import CheckResult
    from hcsc.datalake.dre.config.models import Dataset, Model


def no_related(params: Any) -> dict[str, str]:
    return {}


@dataclass(frozen=True)
class TemplateSpec:
    name: str
    params: type[Model]
    sql: Callable[..., str]  # renders the statement from the template arguments (keyword arguments)
    columns: Callable[[Dataset, Any], list[str]]  # columns the rule reads on its dataset, besides group_by
    # Template arguments beyond table, feed_filter and group_by: (dataset, params, related datasets).
    arguments: Callable[[Dataset, Any, dict[str, Dataset]], dict[str, Any]]
    what: Callable[[Any], str]  # the observed text's tail, e.g. "keys have more than 2 rows"
    unit: str  # the population's unit in the expected text, e.g. "keys"
    # Other datasets the rule reads: params field name -> dataset id (validated and passed as `related`).
    related: Callable[[Any], dict[str, str]] = no_related
    # A custom result per row (row, params, group_values) for templates that are not plain counts.
    result: Callable[[Any, Any, dict[str, str | None]], CheckResult] | None = None


TEMPLATES: Registry[TemplateSpec] = Registry("rule template", ("hcsc.datalake.dre.checks.rules.templates",))


def register_template(spec: TemplateSpec) -> TemplateSpec:
    return TEMPLATES.register(spec.name, spec)


def time_expr(dataset: Dataset, column: str, fmt: str | None) -> str:
    """A column as a comparable value: parsed with fmt when given, never compared as a string."""
    expr = column_expr(dataset, column)
    return f"to_timestamp({expr}, {sql_string(fmt)})" if fmt else expr


def one_or_more(value: Any) -> Any:
    """A params field that takes a list, where a single value may be written bare."""
    return [value] if isinstance(value, str) else value
