"""Derived datasets (spec section 3, release 2): a query over other datasets, used as an upstream.

A derived dataset has no table. Wherever a hop, a cause check or `dre trace` would read an
upstream's table, it reads the derived dataset's relation instead: a subquery the engine
builds from the dataset's `derived` contract (checks/sql/derived_relation.sql.j2). Its
columns are the contract's group_by and columns outputs, plus record_ts and load_ts (UTC),
the latest record and load time of the `from` rows in each group, and dre_unmatched, the
joined datasets that had no match for the group (for the JOIN_UNMATCHED cause).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import render_sql
from hcsc.datalake.dre.checks.times import utc_expr

if TYPE_CHECKING:
    from hcsc.datalake.dre.config.models import Dataset, TimeColumn

UNMATCHED_COLUMN = "dre_unmatched"


def _qualified(column: TimeColumn, alias: str, default_tz: str) -> str:
    """The time column's UTC expression, the column qualified with its source's alias."""
    return utc_expr(column.model_copy(update={"column": f"{alias}.{column.column}"}), default_tz)


def relation(ds: Dataset, datasets: dict[str, Dataset], default_tz: str) -> str:
    """The derived dataset's subquery (parenthesised, aliased with the dataset id). `datasets`
    must hold its `from` and joined datasets (validated config)."""
    d = ds.derived
    if d is None:
        raise ValueError(f"{ds.dataset} is not derived")
    base = datasets[d.from_]
    if base.record_time is None or base.load_time is None:
        raise ValueError(f"{d.from_} needs record_time and load_time to be derived from")
    sources = [{"alias": s, "table": datasets[s].table, "filter": datasets[s].feed_filter} for s in d.sources]
    joins = [{"alias": j.dataset, "type": j.type.upper(),
              "on": [(f"{d.from_}.{left}", f"{j.dataset}.{right}") for left, right in j.match.items()],
              "probe": f"{j.dataset}.{next(iter(j.match.values()))}"} for j in d.joins]
    return "(" + render_sql(
        "derived_relation.sql.j2", base=sources[0], joins=joins, sources=sources, where=d.where,
        group_by=list(d.group_by.items()), columns=list(d.columns.items()),
        record_expr=_qualified(base.record_time, d.from_, default_tz),
        load_expr=_qualified(base.load_time, d.from_, default_tz), unmatched=UNMATCHED_COLUMN,
    ).strip() + f") {ds.dataset}"


def source_of(ds: Dataset, datasets: dict[str, Dataset], default_tz: str) -> str:
    """What to put after FROM for this dataset: its table, or its relation when derived."""
    return ds.table if ds.table is not None else relation(ds, datasets, default_tz)


def base_datasets(ds: Dataset, datasets: dict[str, Dataset]) -> dict[str, Dataset]:
    """The datasets a derived dataset reads (empty for a table-backed one)."""
    if ds.derived is None:
        return {}
    return {s: datasets[s] for s in ds.derived.sources if s in datasets}
