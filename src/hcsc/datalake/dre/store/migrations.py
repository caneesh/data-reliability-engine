"""Versioned, additive migrations of the dq store (build step 4).

`dre install --apply` creates the tables a store lacks (store/ddl.sql), but a table that already
exists keeps the columns it was created with. A migration brings such a table to the DDL: it adds
columns, at the end of the table's data columns, where store/ddl.sql declares them too, so a
migrated table and a new one have the same columns in the same order. Nothing else is allowed: a
migration never drops, renames, retypes or recreates anything, and never rewrites a stored row
(the rows already stored read the new column as NULL, which the views interpret).

The base layout is the store at checkpoint 3 (2026-10-19). A store older than that is reported by
`dre install --check`; it is not migrated automatically.

A migration is pending while any of its columns is absent, so applying is idempotent and needs no
bookkeeping table: the store's own schema says which migrations it has. Only this module may add
columns to a dq table (the guard allows that statement here alone), and only `dre install
--apply` and the local setup call `apply_pending` (through store/install.py).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from hcsc.datalake.dre.store.names import validate_dq_database, validate_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass(frozen=True)
class Migration:
    version: int
    table: str
    columns: tuple[tuple[str, str], ...]  # (name, type), in DDL order, appended after the table's data columns
    why: str

    def __str__(self) -> str:
        cols = ", ".join(f"{c} {t}" for c, t in self.columns)
        return f"migration {self.version}: {self.table} ADD COLUMNS ({cols}) ({self.why})"


MIGRATIONS: tuple[Migration, ...] = (
    Migration(1, "dq_check_result", (("evaluation_mode", "STRING"),),
              "build step 4: NORMAL, CURRENT_RECHECK or AS_OF_REPLAY (ADR-05)"),
)


def pending(spark: SparkSession, dq_database: str) -> list[Migration]:
    """The migrations whose table exists without all their columns, in version order. A table that
    does not exist is not pending: the DDL creates it whole."""
    validate_dq_database(dq_database)
    found = []
    for m in MIGRATIONS:
        name = f"{dq_database}.{validate_identifier(m.table, 'dq table name')}"
        if not spark.catalog.tableExists(name):
            continue
        have = {c.name.lower() for c in spark.catalog.listColumns(name)}
        if any(c.lower() not in have for c, _ in m.columns):
            found.append(m)
    return found


def apply_pending(spark: SparkSession, dq_database: str) -> list[Migration]:
    """Add each pending migration's absent columns, in version order; returns what was applied.
    A migration partly applied before (some of its columns present) adds only the rest."""
    applied = []
    for m in pending(spark, dq_database):
        name = f"{dq_database}.{m.table}"
        have = {c.name.lower() for c in spark.catalog.listColumns(name)}
        absent = [(c, t) for c, t in m.columns if c.lower() not in have]
        cols = ", ".join(f"{validate_identifier(c, 'dq column name')} {t}" for c, t in absent)
        spark.sql(f"ALTER TABLE {dq_database}.{m.table} ADD COLUMNS ({cols})")
        applied.append(m)
    return applied
