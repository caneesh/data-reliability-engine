"""Generic cause probes and their parameters (spec section 7).

A feed configures probes under `probes:`, a map from a config key to the probe's
parameters (or a list of parameter sets, any of which may confirm the cause). The
pattern YAML says which cause code uses which probe type and which config key. A
probe whose parameters are missing or null is NOT_READY, never RULED_OUT.

Probe types:
- file_exists{path}: CONFIRMED when the file exists.
- file_value_compare{path, extract_regex, format, compare_to}: reads a value from a
  file (first regex group), parses it with format (a Spark datetime pattern; never
  compared as a string) and compares it with the failing file's partition (the same
  regex applied to the file's folder name) or the event window.
- log_contains{path_glob, pattern}: a log file matching the glob contains a line
  matching the pattern (and the failure's reference).
- table_contains{table, condition}: the table has rows matching the condition for
  the failure's key.
- size_changed{}: the file's size changed after it was first seen (from dq_file).
- builtin: logic in the engine needing no configuration (step 8).
"""

from __future__ import annotations

import re
from typing import Annotated, Any, Literal

from pydantic import AfterValidator, Field

from hcsc.datalake.dre.config.models import Model, NonEmptyStr, SqlFragment, TableName, invalid


def _regex(value: str) -> str:
    try:
        re.compile(value)
    except re.error as exc:
        raise invalid(f"pattern {value!r} does not compile: {exc}", "fix the regular expression") from None
    return value


Regex = Annotated[str, Field(min_length=1), AfterValidator(_regex)]


class FileExists(Model):
    path: NonEmptyStr | None = None


class FileValueCompare(Model):
    path: NonEmptyStr | None = None
    extract_regex: Regex | None = None
    format: NonEmptyStr | None = None  # Spark datetime pattern for the extracted value, e.g. yyyyMMdd
    compare_to: Literal["partition", "window"] | None = None
    # partition: CONFIRMED when the file's value is later than the failure's partition (spec
    # default), or when it differs from it at all ("different").
    confirm_when: Literal["later", "different"] = "later"


class LogContains(Model):
    path_glob: NonEmptyStr | None = None
    pattern: Regex | None = None


class TableContains(Model):
    table: TableName | None = None
    condition: SqlFragment | None = None
    id: NonEmptyStr | None = None        # optional label, e.g. a filter rule's name
    code_ref: NonEmptyStr | None = None  # optional: where the rule lives (file and line, or commit)


class SizeChanged(Model):
    pass


PROBE_PARAMS: dict[str, type[Model]] = {
    "file_exists": FileExists,
    "file_value_compare": FileValueCompare,
    "log_contains": LogContains,
    "table_contains": TableContains,
    "size_changed": SizeChanged,
}
PROBE_TYPES = (*PROBE_PARAMS, "builtin")


def is_ready(params: Model | None) -> bool:
    """A probe can run only when every parameter it declares is set."""
    if params is None:
        return False
    return all(getattr(params, name) is not None for name in type(params).model_fields
               if name not in ("id", "code_ref"))


def parameter_sets(raw: Any) -> list[Any]:
    """A probe key's value as a list of parameter sets (a single mapping, a list, or null)."""
    if raw is None:
        return []
    return list(raw) if isinstance(raw, list) else [raw]
