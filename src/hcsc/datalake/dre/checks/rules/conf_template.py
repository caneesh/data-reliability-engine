"""Gold rule templates from configuration (spec section 3, "Rule templates from configuration").

A file under conf/templates/ declares a template: its params (typed), the text of its result,
and a Jinja SQL template that returns `population` and `violations` (and g_1.. for the rule's
group_by). It becomes a TemplateSpec like a packaged one, so RuleCheck runs it the same way.

Configuration is never code, so two guards hold (spec section 9, rule 10):
- The SQL renders in a sandboxed Jinja environment with a fixed set of helpers, and always runs
  wrapped as `SELECT * FROM (...) dre_rule`, so anything but a query is a parse error.
- `dre validate` rejects a template whose text carries a write or DDL verb, TRANSFORM, or a
  Java-method call (config/sqlcheck.py), naming the file and line, before anything reaches Spark.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from jinja2 import StrictUndefined, TemplateSyntaxError
from jinja2.sandbox import SandboxedEnvironment
from pydantic import BeforeValidator, Field, create_model

from hcsc.datalake.dre.checks.base import double
from hcsc.datalake.dre.checks.keys import column_expr, key_exprs
from hcsc.datalake.dre.checks.rules.template import TEMPLATES, TemplateSpec, time_expr
from hcsc.datalake.dre.checks.times import sql_string
from hcsc.datalake.dre.config.models import Dataset, Identifier, Model, NonEmptyStr, SqlFragment, invalid
from hcsc.datalake.dre.config.sqlcheck import SqlProblem, forbidden_sql

ParamType = Literal["string", "int", "float", "bool", "column", "columns", "sql", "format"]
_TYPES: dict[str, Any] = {
    "string": str, "int": int, "float": float, "bool": bool, "column": Identifier,
    "columns": list[Identifier], "sql": SqlFragment, "format": NonEmptyStr,
}
WRAP = "dre_rule"


class ParamSpec(Model):
    type: ParamType
    required: bool = True
    default: Any = None


def _bare_type(value: Any) -> Any:
    """`column: column` is short for `column: {type: column}`."""
    return {"type": value} if isinstance(value, str) else value


class TemplateFile(Model):
    """One conf/templates/<name>.yaml file."""

    template: Identifier
    params: dict[Identifier, Annotated[ParamSpec, BeforeValidator(_bare_type)]] = {}
    population: NonEmptyStr  # what the population counts, e.g. "rows with a value"
    violation: NonEmptyStr  # the observed text's tail, e.g. "rows have a value in the future"
    unit: NonEmptyStr  # the population's unit, e.g. "rows"
    sql: NonEmptyStr

    def param_specs(self) -> dict[str, ParamSpec]:
        return dict(self.params)


def forbidden_statement(text: str) -> SqlProblem | None:
    """The first forbidden SQL word in a template's text, with its line (config/sqlcheck.py)."""
    return forbidden_sql(text)


def syntax_problem(text: str) -> SqlProblem | None:
    try:
        _ENV.parse(text)
    except TemplateSyntaxError as exc:
        return SqlProblem(exc.lineno or 1, f"template syntax: {exc.message}", "fix the Jinja expression")
    return None


# No block trimming: what an operator writes is what runs (trim_blocks would glue the line after an
# inline {% endif %} to the one before it).
_ENV = SandboxedEnvironment(undefined=StrictUndefined, autoescape=False)


def params_model(file: TemplateFile) -> type[Model]:
    fields: dict[str, Any] = {}
    for name, spec in file.param_specs().items():
        kind = _TYPES[spec.type]
        if spec.required and spec.default is None:
            fields[name] = (kind, ...)
        else:
            fields[name] = (kind | None, Field(default=spec.default))
    return create_model(f"ConfTemplate_{file.template}", __base__=Model, **fields)


def _helpers(ds: Dataset) -> dict[str, Any]:
    """What a template may call: column and time expressions (with key_normalise), literals."""
    return {
        "column": lambda name: column_expr(ds, name),
        "time": lambda name, fmt=None: time_expr(ds, name, fmt),
        "key_expr": ", ".join(key_exprs(ds)),
        "literal": sql_string,
        "number": double,
    }


def spec_from(file: TemplateFile) -> TemplateSpec:
    model = params_model(file)
    column_params = [n for n, s in file.param_specs().items() if s.type == "column"]
    columns_params = [n for n, s in file.param_specs().items() if s.type == "columns"]
    compiled = _ENV.from_string(file.sql)

    def sql(table: str, feed_filter: str | None, group_by: list[str], **args: Any) -> str:
        rendered = compiled.render(table=table, feed_filter=feed_filter, group_by=group_by, **args)
        return f"SELECT * FROM (\n{rendered}\n) {WRAP}"  # a query, or a parse error (spec section 9, rule 10)

    def columns(ds: Dataset, p: Any) -> list[str]:
        found = [getattr(p, n) for n in column_params if getattr(p, n) is not None]
        for n in columns_params:
            found += getattr(p, n) or []
        return found

    return TemplateSpec(
        name=file.template, params=model, sql=sql, columns=columns,
        arguments=lambda ds, p, related: {"params": p, **_helpers(ds)},
        what=lambda p: file.violation, unit=file.unit,
    )


_CONF_IDS: set[str] = set()


def use_conf_templates(files: list[TemplateFile]) -> list[str]:
    """Register these conf templates (replacing the previous conf set); a name a packaged template
    holds is refused and returned."""
    for old in _CONF_IDS:
        TEMPLATES.unregister(old)
    _CONF_IDS.clear()
    refused = []
    for file in files:
        if file.template in TEMPLATES:
            refused.append(file.template)
            continue
        TEMPLATES.register(file.template, spec_from(file))
        _CONF_IDS.add(file.template)
    return refused


def conf_template_ids() -> frozenset[str]:
    return frozenset(_CONF_IDS)


def check_template_file(file: TemplateFile) -> SqlProblem | None:
    """Static problems: a forbidden statement, or Jinja syntax."""
    problem = forbidden_statement(file.sql)
    if problem is not None:
        return problem
    problem = syntax_problem(file.sql)
    if problem is not None:
        return problem
    try:
        params_model(file)
    except (ValueError, TypeError) as exc:
        raise invalid(f"params cannot be built: {exc}", "check each param's type and default")
    return None
