"""Key and column expressions with key_normalise applied (spec sections 3 and 6).

A normaliser is a named SQL rewrite of a column, so keys written differently in two layers
compare equal. In config it is a bare name (`strip_leading_zeros`) or `{name: argument}`
(`{parse_date: MM/dd/yyyy}`). Normalisers register themselves here (`register_normaliser`);
the packaged ones are below.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from hcsc.datalake.dre.checks.times import sql_string
from hcsc.datalake.dre.registry import Registry

if TYPE_CHECKING:
    from hcsc.datalake.dre.config.models import Dataset


class NormaliserError(ValueError):
    """A normaliser's config form is wrong: `problem` and `fix` (for the config error)."""

    def __init__(self, problem: str, fix: str) -> None:
        super().__init__(problem)
        self.problem, self.fix = problem, fix


@dataclass(frozen=True)
class Normaliser:
    name: str
    sql: Callable[[str, Any], str]  # (column expression, argument) -> SQL expression
    example: str  # how it is written in config, for error messages
    # Validates the argument (None for a bare name); raises NormaliserError. The default takes no argument.
    argument: Callable[[Any], None] = lambda arg: _no_argument(arg)


NORMALISERS: Registry[Normaliser] = Registry("normaliser")


def register_normaliser(normaliser: Normaliser) -> Normaliser:
    return NORMALISERS.register(normaliser.name, normaliser)


def _no_argument(arg: Any) -> None:
    if arg is not None:
        raise NormaliserError("takes no argument", "write the bare name")


def _format_argument(arg: Any) -> None:
    if not isinstance(arg, str) or not arg or "'" in arg:
        raise NormaliserError("needs a Spark date format", "write { parse_date: MM/dd/yyyy }")


def _regex_argument(arg: Any) -> None:
    if not isinstance(arg, dict) or set(arg) != {"pattern", "replacement"} \
            or not all(isinstance(v, str) for v in arg.values()) or not arg["pattern"]:
        raise NormaliserError("needs pattern and replacement", "write { regex_replace: { pattern: '-', replacement: '' } }")
    try:
        re.compile(arg["pattern"])
    except re.error as exc:
        raise NormaliserError(f"pattern does not compile ({exc})", "fix the regular expression") from None


register_normaliser(Normaliser("strip_leading_zeros", lambda c, a: f"regexp_replace({c}, '^0+', '')",
                               "strip_leading_zeros"))
register_normaliser(Normaliser("trim", lambda c, a: f"trim({c})", "trim"))
register_normaliser(Normaliser("upper", lambda c, a: f"upper({c})", "upper"))
register_normaliser(Normaliser("lower", lambda c, a: f"lower({c})", "lower"))
register_normaliser(Normaliser(
    "parse_date", lambda c, a: f"date_format(to_date({c}, {sql_string(a)}), 'yyyy-MM-dd')",
    "{ parse_date: MM/dd/yyyy }", _format_argument))  # a date string read as an ISO date
register_normaliser(Normaliser(
    "regex_replace", lambda c, a: f"regexp_replace({c}, {sql_string(a['pattern'])}, {sql_string(a['replacement'])})",
    "{ regex_replace: { pattern: '-', replacement: '' } }", _regex_argument))


def split(normaliser: Any) -> tuple[str, Any]:
    """(name, argument) from the config form: a bare name, or a one-key mapping."""
    if isinstance(normaliser, str):
        return normaliser, None
    if isinstance(normaliser, dict) and len(normaliser) == 1:
        [(name, arg)] = normaliser.items()
        if isinstance(name, str):
            return name, arg
    raise NormaliserError(f"{normaliser!r} is not allowed", f"use one of {', '.join(n.example for n in NORMALISERS.values())}")


def validate_normaliser(normaliser: Any) -> Any:
    """The config value, checked against the registry; raises NormaliserError."""
    name, arg = split(normaliser)
    found = NORMALISERS.get(name)
    if found is None:
        raise NormaliserError(f"{normaliser!r} is not allowed", f"use one of {', '.join(n.example for n in NORMALISERS.values())}")
    try:
        found.argument(arg)
    except NormaliserError as exc:
        raise NormaliserError(f"{normaliser!r} is not allowed: {name} {exc.problem}", exc.fix) from None
    return normaliser


def normalised(column: str, normaliser: Any) -> str:
    """SQL for one (validated identifier) column under a normaliser."""
    name, arg = split(normaliser)
    return NORMALISERS[name].sql(column, arg)


def column_expr(dataset: Dataset, column: str) -> str:
    """The column, normalised if the dataset configures a normaliser for it."""
    return normalised(column, dataset.key_normalise[column]) if column in dataset.key_normalise else column


def key_exprs(dataset: Dataset) -> list[str]:
    """One SQL expression per key column, in key order."""
    return [column_expr(dataset, c) for c in dataset.key]


# The key encoding this engine computes (spec section 6, "Hop details"). Release N computes encoding 1
# only; encoding 2 (Gate A remediation) is release N+1's. Every key event records the encoding of its
# key_value and key_hash (dq_key_event.key_encoding; NULL in rows stored before migration 2 means 1).
KEY_ENCODING = 1


def key_string(exprs: list[str]) -> str:
    """One canonical string for a (normalised) key: parts joined with '|', null written as <null>."""
    parts = [f"coalesce(CAST({e} AS STRING), '<null>')" for e in exprs]
    return f"concat_ws('|', {', '.join(parts)})"
