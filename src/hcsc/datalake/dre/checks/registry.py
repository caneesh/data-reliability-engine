"""Which checks run for a dataset: the pattern's list in patterns/*.yaml (spec section 2).

Checks register themselves (`register_check`) in the module that defines them; the built-in
modules under checks/tier1 and checks/hop are imported on first use. Patterns are the
packaged patterns/*.yaml files plus, once `use_conf_patterns` names a conf directory, its
patterns/*.yaml: a file with a packaged name replaces that pattern, a new name adds one. Each
pattern declares `kind: file | table` (spec section 3).
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from hcsc.datalake.dre.registry import Registry

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.base import Check
    from hcsc.datalake.dre.config.models import Dataset, Feed

CHECKS: Registry[Check] = Registry("check", ("hcsc.datalake.dre.checks.tier1", "hcsc.datalake.dre.checks.hop"))
TABLE_WIDE = "TABLE_WIDE"  # pseudo-pattern for datasets that no feed lists


def register_check(check: Check) -> Check:
    """Called by a check's module; the check's `check_id` must be unique."""
    return CHECKS.register(check.check_id, check)


KINDS = ("file", "table")
_CONF_PATTERNS: Path | None = None  # <conf>/patterns, set by the config loader


def use_conf_patterns(conf_dir: Path | str | None) -> None:
    """Read patterns from <conf_dir>/patterns as well as the packaged ones (None: packaged only)."""
    global _CONF_PATTERNS
    _CONF_PATTERNS = None if conf_dir is None else Path(conf_dir) / "patterns"
    clear_pattern_caches()


def clear_pattern_caches() -> None:
    _pattern_checks.cache_clear()
    pattern_causes.cache_clear()


def _packaged_names() -> set[str]:
    folder = files("hcsc.datalake.dre").joinpath("patterns")
    return {entry.name[:-5].upper() for entry in folder.iterdir() if entry.name.endswith(".yaml")}


def _conf_names() -> set[str]:
    if _CONF_PATTERNS is None or not _CONF_PATTERNS.is_dir():
        return set()
    return {path.name[:-5].upper() for path in _CONF_PATTERNS.glob("*.yaml")}


def patterns() -> tuple[str, ...]:
    """The feed patterns (a feed's `pattern` must be one), packaged and from conf, without TABLE_WIDE."""
    return tuple(sorted((_packaged_names() | _conf_names()) - {TABLE_WIDE}))


def pattern_origin(pattern: str) -> str:
    """Where the pattern is read from: "conf" (a conf/patterns file) or "packaged"."""
    return "conf" if pattern in _conf_names() else "packaged"


def pattern_file(pattern: str) -> str:
    """The pattern's file, for error messages."""
    if pattern_origin(pattern) == "conf":
        return str(_CONF_PATTERNS / f"{pattern.lower()}.yaml")
    return f"patterns/{pattern.lower()}.yaml"


def pattern_kind(pattern: str) -> str:
    """`file` (data lands as files, listed and registered) or `table` (data arrives in a table)."""
    kind = _load_pattern(pattern).get("kind")
    if kind not in KINDS:
        raise ValueError(f"{pattern_file(pattern)}: kind must be one of {list(KINDS)}, not {kind!r}")
    return kind


def is_file_pattern(pattern: str | None) -> bool:
    return pattern is not None and pattern != TABLE_WIDE and pattern_kind(pattern) == "file"


@cache
def _pattern_checks(pattern: str) -> tuple[tuple[str, dict[str, Any] | None], ...]:
    """(check id, the pattern's default params or None) per entry of the pattern's `checks:` list.
    An entry is a check id, or {check: id, params: {...}} for a parametrised check."""
    where = pattern_file(pattern)
    found = []
    for raw in _load_pattern(pattern).get("checks") or ():
        check_id, params = (raw, None) if isinstance(raw, str) else (raw.get("check"), raw.get("params"))
        if check_id not in CHECKS:
            raise ValueError(f"{where} lists unknown checks [{check_id!r}]")
        if params is not None:
            model = CHECKS[check_id].params_model
            if model is None:
                raise ValueError(f"{where}: {check_id} takes no parameters")
            try:
                model.model_validate(params)
            except ValueError as exc:
                raise ValueError(f"{where}: {check_id} params are not valid: {exc}") from None
        found.append((check_id, params))
    if len({c for c, _ in found}) != len(found):
        raise ValueError(f"{where} lists a check twice")
    return tuple(found)


def pattern_check_ids(pattern: str) -> tuple[str, ...]:
    return tuple(c for c, _ in _pattern_checks(pattern))


def pattern_check_params(pattern: str) -> dict[str, dict[str, Any]]:
    """check id -> the pattern's default params, for the parametrised checks it gives defaults."""
    return {c: p for c, p in _pattern_checks(pattern) if p is not None}


def checks_for(dataset: Dataset, pattern: str | None, feed: Feed | None = None) -> list[Check]:
    """The pattern's checks that apply to this dataset. Table-wide datasets (pattern None)
    get the patterns/table_wide.yaml list. A parametrised check applies only where the pattern,
    the feed or the dataset configures it."""
    from hcsc.datalake.dre.checks.base import configured_params

    pattern = pattern or TABLE_WIDE
    found = []
    for check_id in pattern_check_ids(pattern):
        check = CHECKS[check_id]
        if not check.applies_to(dataset, pattern):
            continue
        if check.params_model is not None and configured_params(check, dataset, feed, pattern) is None:
            continue
        found.append(check)
    return found


@dataclass(frozen=True)
class CauseEntry:
    code: str
    probe: str | None  # a registered probe type, or "builtin"; None for the fallback
    params: str | None  # the feed's `probes:` key with this probe's parameters
    fallback: bool = False


@dataclass(frozen=True)
class FailureType:
    name: str
    checks: tuple[str, ...]
    order: tuple[CauseEntry, ...]


def _load_pattern(pattern: str) -> dict:
    """The pattern's YAML: the conf file when there is one, else the packaged one."""
    name = f"{pattern.lower()}.yaml"
    if _CONF_PATTERNS is not None and (_CONF_PATTERNS / name).is_file():
        text = (_CONF_PATTERNS / name).read_text(encoding="utf-8")
    else:
        try:
            text = files("hcsc.datalake.dre").joinpath("patterns", name).read_text(encoding="utf-8")
        except FileNotFoundError:
            raise ValueError(f"unknown pattern {pattern!r}; known: {list(patterns())}") from None
    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{pattern_file(pattern)}: a pattern file is a mapping")
    if str(data.get("pattern", "")).upper() != pattern:
        raise ValueError(f"{pattern_file(pattern)}: `pattern` must be {pattern} (the file's name)")
    return data


@cache
def pattern_causes(pattern: str) -> dict[str, FailureType]:
    """The pattern's failure types and their ordered cause checks. Raises ValueError on a malformed list."""
    from hcsc.datalake.dre.causes.base import BUILTINS
    from hcsc.datalake.dre.config.probes import BUILTIN, PROBES

    where = pattern_file(pattern)
    found: dict[str, FailureType] = {}
    for name, body in (_load_pattern(pattern).get("causes") or {}).items():
        entries = []
        for raw in body.get("order") or []:
            entry = CauseEntry(raw["code"], raw.get("probe"), raw.get("params"), bool(raw.get("fallback")))
            if entry.fallback:
                if entry.probe or entry.params:
                    raise ValueError(f"{where}: fallback {entry.code} takes no probe")
            elif entry.probe == BUILTIN:
                if entry.code not in BUILTINS:
                    raise ValueError(f"{where}: {entry.code} names no builtin cause check")
            elif entry.probe not in PROBES:
                raise ValueError(f"{where}: {entry.code} has unknown probe {entry.probe!r}")
            elif PROBES[entry.probe].params.model_fields and not entry.params:
                raise ValueError(f"{where}: {entry.code} uses probe {entry.probe} but names no params key")
            entries.append(entry)
        if not entries or not entries[-1].fallback or sum(e.fallback for e in entries) != 1:
            raise ValueError(f"{where}: failure type {name} must end with exactly one fallback")
        if len(entries) < 2:
            raise ValueError(f"{where}: failure type {name} needs at least one cause check before the fallback")
        if len({e.code for e in entries}) != len(entries):
            raise ValueError(f"{where}: failure type {name} repeats a cause code")
        found[name] = FailureType(name, tuple(body.get("checks") or ()), tuple(entries))
    return found


def probe_keys(pattern: str) -> dict[str, str]:
    """params key -> probe type, for every probe the pattern's causes configure."""
    keys: dict[str, str] = {}
    for failure in pattern_causes(pattern).values():
        for entry in failure.order:
            if entry.params:
                if keys.setdefault(entry.params, entry.probe) != entry.probe:
                    raise ValueError(f"{pattern_file(pattern)}: params key {entry.params} is used by two probe types")
    return keys
