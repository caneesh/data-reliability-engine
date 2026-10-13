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

from dataclasses import dataclass
from typing import Annotated, Any, Literal

from jinja2 import StrictUndefined, TemplateSyntaxError
from jinja2.sandbox import SandboxedEnvironment
from pydantic import BeforeValidator, Field, TypeAdapter, ValidationError, create_model

from hcsc.datalake.dre.checks.base import double
from hcsc.datalake.dre.checks.keys import column_expr, key_exprs
from hcsc.datalake.dre.checks.rules.template import TemplateSpec, time_expr
from hcsc.datalake.dre.checks.times import sql_string
from hcsc.datalake.dre.config.models import Dataset, Identifier, Model, NonEmptyStr, SqlFragment
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
        else:  # the default is validated against the declared type like any value (validate_default)
            fields[name] = (kind | None, Field(default=spec.default, validate_default=True))
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


@dataclass(frozen=True)
class TemplateProblem:
    """A static problem in a template file: where (a path into the YAML) and, for the SQL text,
    which line of it."""

    loc: tuple[str, ...]
    line: int  # 1-based line within the text at `loc` (0 when the problem is the whole value)
    problem: str
    fix: str


def check_template_file(file: TemplateFile) -> TemplateProblem | None:
    """Static problems, never raised: a forbidden statement in the SQL or in a `sql` param's default,
    Jinja syntax, a params model that cannot be built, or a default that does not fit its type."""
    for check in (forbidden_statement, syntax_problem):
        problem = check(file.sql)
        if problem is not None:
            return TemplateProblem(("sql",), problem.line, problem.problem, problem.fix)
    for name, spec in file.param_specs().items():
        if spec.type == "sql" and isinstance(spec.default, str):
            problem = forbidden_sql(spec.default)
            if problem is not None:
                return TemplateProblem(("params", name, "default"), 0, problem.problem, problem.fix)
    for name, spec in file.param_specs().items():
        if spec.default is not None:
            try:
                TypeAdapter(_TYPES[spec.type]).validate_python(spec.default)
            except ValidationError as exc:
                return TemplateProblem(("params", name, "default"), 0,
                                       f"the default does not fit type {spec.type}: {exc.errors()[0]['msg']}",
                                       "give a default of the declared type, or drop the default")
    try:
        params_model(file)
    except (ValueError, TypeError) as exc:
        return TemplateProblem(("params",), 0, f"params cannot be built: {type(exc).__name__}: {exc}",
                               "check each param's name, type and default")
    return None
