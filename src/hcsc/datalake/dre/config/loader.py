"""Load configuration YAML, keeping line numbers, and resolve settings.

Layout of a conf directory (spec section 3): `defaults.yaml`, then one object
per file in `feeds/`, `datasets/` and `rules/`. Every problem found while
loading becomes a ConfigError naming the file, line and field, with a fix.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ValidationError

from hcsc.datalake.dre.config.models import (
    SETTING_FIELDS,
    Dataset,
    Defaults,
    Feed,
    Rule,
    Settings,
    SettingsOverride,
)

KINDS: dict[str, tuple[str, type[BaseModel]]] = {
    # directory: (id field, model)
    "feeds": ("feed", Feed),
    "datasets": ("dataset", Dataset),
    "rules": ("rule", Rule),
}
SPEC_REF = "see docs/spec.md section 3"


@dataclass(frozen=True)
class ConfigError:
    file: str
    line: int
    field: str
    problem: str
    fix: str
    level: str = "error"  # "error" fails dre validate; "warning" is reported only

    def __str__(self) -> str:
        where = f" {self.field}:" if self.field else ""
        tag = "warning: " if self.level == "warning" else ""
        return f"{self.file}:{self.line}:{where} {tag}{self.problem}. Fix: {self.fix}"


@dataclass(frozen=True)
class Source:
    """Where an object came from, for locating errors."""

    file: str
    node: yaml.Node | None


@dataclass
class Config:
    conf_dir: Path
    defaults: Defaults | None = None
    feeds: dict[str, Feed] = field(default_factory=dict)
    datasets: dict[str, Dataset] = field(default_factory=dict)
    rules: dict[str, Rule] = field(default_factory=dict)
    sources: dict[tuple[str, str], Source] = field(default_factory=dict)  # (kind, id) -> Source

    def feed_of(self, dataset_id: str) -> Feed | None:
        """The feed that lists this dataset (the first, if several do; validate reports that)."""
        return next((f for f in self.feeds.values() if dataset_id in f.datasets), None)

    def feed_settings(self, feed_id: str) -> Settings:
        """Settings for a feed: defaults.yaml, then the feed. Raises ValidationError if incomplete."""
        return resolve_settings(self.defaults, self.feeds[feed_id])

    def dataset_settings(self, dataset_id: str) -> Settings:
        """Settings for a dataset: defaults.yaml, then its feed, then the dataset."""
        return resolve_settings(self.defaults, self.feed_of(dataset_id), self.datasets[dataset_id])

    def locate(self, kind: str, obj_id: str, loc: tuple[Any, ...]) -> tuple[str, int]:
        source = self.sources[(kind, obj_id)]
        return source.file, line_of(source.node, loc)


def resolve_settings(*levels: SettingsOverride | None) -> Settings:
    """Merge settings from the highest level to the lowest; a value set lower wins."""
    merged: dict[str, Any] = {}
    for level in levels:
        if level is None:
            continue
        for name in SETTING_FIELDS:
            value = getattr(level, name)
            if value is not None:
                merged[name] = value
    return Settings.model_validate(merged)


# --- locating errors in YAML ---


def line_of(node: yaml.Node | None, loc: tuple[Any, ...]) -> int:
    """1-based line of the deepest part of loc found under node."""
    if node is None:
        return 1
    line = node.start_mark.line + 1
    current: yaml.Node = node
    for step in loc:
        if isinstance(current, yaml.MappingNode):
            match = next(((k, v) for k, v in current.value if k.value == str(step)), None)
            if match is None:
                break
            line, current = match[0].start_mark.line + 1, match[1]
        elif isinstance(current, yaml.SequenceNode) and isinstance(step, int) and step < len(current.value):
            current = current.value[step]
            line = current.start_mark.line + 1
        else:
            break
    return line


def format_loc(loc: tuple[Any, ...]) -> str:
    out = ""
    for step in loc:
        if isinstance(step, int):
            out += f"[{step}]"
        elif step == "[key]":
            continue
        else:
            out += f".{step}" if out else str(step)
    return out


def _bound(err: dict[str, Any]) -> str:
    ctx = err.get("ctx", {})
    for key, op in (("gt", ">"), ("ge", ">="), ("lt", "<"), ("le", "<=")):
        if key in ctx:
            return f"use a value {op} {ctx[key]}"
    return SPEC_REF


def describe(err: dict[str, Any]) -> tuple[str, str]:
    """(problem, fix) for one pydantic error."""
    kind, value, ctx = err["type"], err.get("input"), err.get("ctx", {})
    if kind == "dre_invalid":
        return ctx["problem"], ctx["fix"]
    if kind == "missing":
        return "required field is missing", f"add `{err['loc'][-1]}`"
    if value is None:
        return (
            "required value is null",
            "set it; if this is a production value not yet confirmed (spec section 11), "
            "it must be confirmed before this config can run",
        )
    if kind == "extra_forbidden":
        return "unknown field", "remove it or correct the spelling (docs/spec.md section 3 lists the fields)"
    if kind in ("literal_error", "enum"):
        return f"{value!r} is not allowed", f"use one of {ctx.get('expected', '')}"
    if kind.startswith("int_"):
        return f"{value!r} is not a whole number", "use a whole number"
    if kind.startswith("float_"):
        return f"{value!r} is not a number", "use a number"
    if kind.startswith("bool_"):
        return f"{value!r} is not true or false", "use true or false"
    if kind == "string_type":
        return f"{value!r} is not a string", 'use a quoted string, e.g. "04:00"'
    if kind == "list_type":
        return "expected a list", "use a YAML list, e.g. [a, b]"
    if kind in ("dict_type", "model_type", "model_attributes_type"):
        return "expected a mapping", "use key: value pairs"
    if kind == "too_short":
        return "is empty", f"add at least {ctx.get('min_length', 1)} item(s)"
    if kind in ("greater_than", "greater_than_equal", "less_than", "less_than_equal"):
        return err["msg"].lower(), _bound(err)
    if kind == "date_from_datetime_parsing" or kind.startswith("date_"):
        return f"{value!r} is not a date", "use YYYY-MM-DD"
    return err["msg"], SPEC_REF


def errors_from_validation(
    exc: ValidationError, file: str, node: yaml.Node | None, prefix: tuple[Any, ...] = ()
) -> list[ConfigError]:
    found = []
    for err in exc.errors():
        loc = prefix + tuple(err["loc"])
        problem, fix = describe(err)
        found.append(ConfigError(file, line_of(node, loc), format_loc(loc), problem, fix))
    return found


# --- reading files ---


def _duplicate_keys(node: yaml.Node, file: str) -> list[ConfigError]:
    found: list[ConfigError] = []
    if isinstance(node, yaml.MappingNode):
        seen: set[str] = set()
        for key, value in node.value:
            if key.value in seen:
                found.append(
                    ConfigError(file, key.start_mark.line + 1, str(key.value), "duplicate key", "keep one of the entries")
                )
            seen.add(key.value)
            found.extend(_duplicate_keys(value, file))
    elif isinstance(node, yaml.SequenceNode):
        for item in node.value:
            found.extend(_duplicate_keys(item, file))
    return found


def read_yaml(path: Path, display: str) -> tuple[Any, yaml.Node | None, list[ConfigError]]:
    """(data, node, errors). data is None when the file cannot be used."""
    try:
        text = path.read_text(encoding="utf-8")
        node = yaml.compose(text, Loader=yaml.SafeLoader)
        data = yaml.safe_load(text)
    except yaml.MarkedYAMLError as exc:
        mark = exc.problem_mark or exc.context_mark
        line = mark.line + 1 if mark else 1
        return None, None, [ConfigError(display, line, "", f"YAML syntax error: {exc.problem}", "fix the YAML syntax at this line")]
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        return None, None, [ConfigError(display, 1, "", f"cannot read file: {exc}", "check the file is readable UTF-8 YAML")]
    if node is None or not isinstance(data, dict):
        return None, node, [ConfigError(display, 1, "", "file must hold one YAML mapping", f"write one object as key: value pairs; {SPEC_REF}")]
    dupes = _duplicate_keys(node, display)
    return (None if dupes else data), node, dupes


def load(conf_dir: Path | str) -> tuple[Config, list[ConfigError]]:
    """Load and validate each file on its own. Cross-file checks are in validate.py."""
    conf_dir = Path(conf_dir)
    config = Config(conf_dir=conf_dir)
    errors: list[ConfigError] = []

    defaults_path = conf_dir / "defaults.yaml"
    display = str(defaults_path)
    if not defaults_path.is_file():
        errors.append(ConfigError(display, 1, "", "defaults.yaml is missing", f"create {display}; {SPEC_REF}"))
    else:
        data, node, errs = read_yaml(defaults_path, display)
        errors.extend(errs)
        if data is not None:
            try:
                config.defaults = Defaults.model_validate(data)
                config.sources[("defaults", "defaults")] = Source(display, node)
            except ValidationError as exc:
                errors.extend(errors_from_validation(exc, display, node))

    for directory, (id_field, model) in KINDS.items():
        kind = id_field
        target: dict[str, Any] = getattr(config, directory)
        paths = sorted([*(conf_dir / directory).glob("*.yaml"), *(conf_dir / directory).glob("*.yml")])
        for path in paths:
            display = str(path)
            data, node, errs = read_yaml(path, display)
            errors.extend(errs)
            if data is None:
                continue
            try:
                obj = model.model_validate(data)
            except ValidationError as exc:
                errors.extend(errors_from_validation(exc, display, node))
                continue
            obj_id = getattr(obj, id_field)
            if obj_id in target:
                first = config.sources[(kind, obj_id)].file
                errors.append(
                    ConfigError(display, line_of(node, (id_field,)), id_field,
                                f"duplicate {kind} id {obj_id!r} (first defined in {first})",
                                f"give each {kind} a unique id")
                )
                continue
            target[obj_id] = obj
            config.sources[(kind, obj_id)] = Source(display, node)
    return config, errors
