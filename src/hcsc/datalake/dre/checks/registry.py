"""Which checks run for a dataset: the pattern's list in patterns/*.yaml (spec section 2).

Checks register themselves (`register_check`) in the module that defines them; the built-in
modules under checks/tier1 and checks/hop are imported on first use. Patterns are the
packaged patterns/*.yaml files.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from importlib.resources import files
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


def _pattern_files() -> tuple[str, ...]:
    folder = files("hcsc.datalake.dre").joinpath("patterns")
    names = (entry.name[:-5].upper() for entry in folder.iterdir() if entry.name.endswith(".yaml"))
    return tuple(sorted(n for n in names if n != TABLE_WIDE))


PATTERNS: tuple[str, ...] = _pattern_files()  # the feed patterns (a feed's `pattern` must be one)


@cache
def _pattern_checks(pattern: str) -> tuple[tuple[str, dict[str, Any] | None], ...]:
    """(check id, the pattern's default params or None) per entry of the pattern's `checks:` list.
    An entry is a check id, or {check: id, params: {...}} for a parametrised check."""
    where = f"patterns/{pattern.lower()}.yaml"
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
    text = files("hcsc.datalake.dre").joinpath("patterns", f"{pattern.lower()}.yaml").read_text(encoding="utf-8")
    return yaml.safe_load(text) or {}


@cache
def pattern_causes(pattern: str) -> dict[str, FailureType]:
    """The pattern's failure types and their ordered cause checks. Raises ValueError on a malformed list."""
    from hcsc.datalake.dre.causes.base import BUILTINS
    from hcsc.datalake.dre.config.probes import BUILTIN, PROBES

    where = f"patterns/{pattern.lower()}.yaml"
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
                    raise ValueError(f"patterns/{pattern.lower()}.yaml: params key {entry.params} "
                                     f"is used by two probe types")
    return keys
