"""Which checks run for a dataset: the pattern's list in patterns/*.yaml (spec section 2)."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from typing import TYPE_CHECKING

import yaml

from hcsc.datalake.dre.checks.base import Check
from hcsc.datalake.dre.checks.hop.file_completeness import FileCompleteness
from hcsc.datalake.dre.checks.hop.key_currency import KeyCurrency
from hcsc.datalake.dre.checks.hop.value_agreement import ValueAgreement
from hcsc.datalake.dre.checks.tier1.files_not_loaded import FilesNotLoaded
from hcsc.datalake.dre.checks.tier1.key_duplicates import KeyDuplicates
from hcsc.datalake.dre.checks.tier1.key_nulls import KeyNulls
from hcsc.datalake.dre.checks.tier1.on_time import OnTime
from hcsc.datalake.dre.checks.tier1.schema_drift import SchemaDrift
from hcsc.datalake.dre.checks.tier1.volume import Volume
from hcsc.datalake.dre.checks.tier1.zero_rows import ZeroRows

if TYPE_CHECKING:
    from hcsc.datalake.dre.config.models import Dataset

CHECKS: dict[str, Check] = {
    check.check_id: check
    for check in (OnTime(), ZeroRows(), Volume(), FilesNotLoaded(), SchemaDrift(), KeyNulls(), KeyDuplicates(),
                  FileCompleteness(), KeyCurrency(), ValueAgreement())
}
PATTERNS = ("FILE_CYCLIC", "FILE_PERIODIC", "TABLE_MERGE")
TABLE_WIDE = "TABLE_WIDE"  # pseudo-pattern for datasets that no feed lists


@cache
def pattern_check_ids(pattern: str) -> tuple[str, ...]:
    ids = tuple(_load_pattern(pattern).get("checks") or ())
    unknown = [i for i in ids if i not in CHECKS]
    if unknown:
        raise ValueError(f"patterns/{pattern.lower()}.yaml lists unknown checks {unknown}")
    return ids


def checks_for(dataset: Dataset, pattern: str | None) -> list[Check]:
    """The pattern's checks that apply to this dataset. Table-wide datasets (pattern None)
    get the patterns/table_wide.yaml list."""
    pattern = pattern or TABLE_WIDE
    return [CHECKS[i] for i in pattern_check_ids(pattern) if CHECKS[i].applies_to(dataset, pattern)]


@dataclass(frozen=True)
class CauseEntry:
    code: str
    probe: str | None  # a probe type from config/probes.py, or "builtin"; None for the fallback
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
    from hcsc.datalake.dre.config.probes import PROBE_PARAMS, PROBE_TYPES

    where = f"patterns/{pattern.lower()}.yaml"
    found: dict[str, FailureType] = {}
    for name, body in (_load_pattern(pattern).get("causes") or {}).items():
        entries = []
        for raw in body.get("order") or []:
            entry = CauseEntry(raw["code"], raw.get("probe"), raw.get("params"), bool(raw.get("fallback")))
            if entry.fallback:
                if entry.probe or entry.params:
                    raise ValueError(f"{where}: fallback {entry.code} takes no probe")
            elif entry.probe not in PROBE_TYPES:
                raise ValueError(f"{where}: {entry.code} has unknown probe {entry.probe!r}")
            elif entry.probe in PROBE_PARAMS and PROBE_PARAMS[entry.probe].model_fields and not entry.params:
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
