"""Key expressions with key_normalise applied (spec sections 3 and 6)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hcsc.datalake.dre.config.models import Dataset

# Normaliser -> SQL expression over one (validated identifier) column.
NORMALISERS = {"strip_leading_zeros": "regexp_replace({column}, '^0+', '')"}


def key_exprs(dataset: Dataset) -> list[str]:
    """One SQL expression per key column, in key order."""
    return [
        NORMALISERS[dataset.key_normalise[c]].format(column=c) if c in dataset.key_normalise else c
        for c in dataset.key
    ]
