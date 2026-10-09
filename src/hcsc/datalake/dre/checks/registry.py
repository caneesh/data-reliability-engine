"""Which checks run for a dataset: the pattern's list in patterns/*.yaml (spec section 2)."""

from __future__ import annotations

from functools import cache
from importlib.resources import files
from typing import TYPE_CHECKING

import yaml

from hcsc.datalake.dre.checks.base import Check
from hcsc.datalake.dre.checks.tier1.key_duplicates import KeyDuplicates

if TYPE_CHECKING:
    from hcsc.datalake.dre.config.models import Dataset

CHECKS: dict[str, Check] = {check.check_id: check for check in (KeyDuplicates(),)}
PATTERNS = ("FILE_CYCLIC", "FILE_PERIODIC", "TABLE_MERGE")
TABLE_WIDE = "TABLE_WIDE"  # pseudo-pattern for datasets that no feed lists


@cache
def pattern_check_ids(pattern: str) -> tuple[str, ...]:
    text = files("hcsc.datalake.dre").joinpath("patterns", f"{pattern.lower()}.yaml").read_text(encoding="utf-8")
    data = yaml.safe_load(text) or {}
    ids = tuple(data.get("checks") or ())
    unknown = [i for i in ids if i not in CHECKS]
    if unknown:
        raise ValueError(f"patterns/{pattern.lower()}.yaml lists unknown checks {unknown}")
    return ids


def checks_for(dataset: Dataset, pattern: str | None) -> list[Check]:
    """The pattern's checks that apply to this dataset. Table-wide datasets (pattern None)
    get the patterns/table_wide.yaml list."""
    pattern = pattern or TABLE_WIDE
    return [CHECKS[i] for i in pattern_check_ids(pattern) if CHECKS[i].applies_to(dataset, pattern)]
