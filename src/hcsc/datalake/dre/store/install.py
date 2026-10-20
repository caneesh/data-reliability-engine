"""`dre install`: print or apply the dq store DDL, or check a store against it.

The platform team creates the empty dq database and grants access; `dre
install --apply` creates the tables and views in it, and brings a store installed by an
earlier release up to date: the tables first, then the pending migrations (store/migrations.py,
additive only), then the views, which may read the migrated columns. Only this module, and the
local setup used by tests, may apply the DDL; `dre run` only checks the store exists and has no
pending migration.
"""

from __future__ import annotations

from hcsc.datalake.dre.checks.base import error_detail

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from hcsc.datalake.dre.store import migrations
from hcsc.datalake.dre.store.names import validate_dq_database
from hcsc.datalake.dre.store.schema import TableDef, expected_objects, render_ddl

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

OK, MISSING, DIFFERS, PENDING = "OK", "MISSING", "DIFFERS", "PENDING"


@dataclass(frozen=True)
class Finding:
    name: str
    status: str
    detail: str = ""

    def __str__(self) -> str:
        return f"{self.status:8} {self.name}" + (f": {self.detail}" if self.detail else "")


def _steps(spark: SparkSession, dq_database: str) -> list[tuple[str, Callable[[], object]]]:
    """(what, action) in the order they apply: create the tables, apply the pending migrations,
    replace the views."""
    statements = render_ddl(dq_database)
    tables = [s for s in statements if not s.upper().startswith("CREATE OR REPLACE VIEW")]
    views = [s for s in statements if s.upper().startswith("CREATE OR REPLACE VIEW")]
    steps: list[tuple[str, Callable[[], object]]] = [(s.splitlines()[0], lambda s=s: spark.sql(s)) for s in tables]
    steps.append(("pending migrations", lambda: migrations.apply_pending(spark, dq_database)))
    steps += [(s.splitlines()[0], lambda s=s: spark.sql(s)) for s in views]
    return steps


def apply_ddl(spark: SparkSession, dq_database: str) -> None:
    """Create the dq tables that do not exist yet, apply the pending migrations, replace the views.
    Safe to repeat."""
    for _, action in _steps(spark, dq_database):
        action()


def check_store(spark: SparkSession, dq_database: str) -> list[Finding]:
    """Compare the database's tables and views with the DDL, one finding per object."""
    validate_dq_database(dq_database)
    if not spark.catalog.databaseExists(dq_database):
        return [Finding(dq_database, MISSING, "database does not exist; the platform team creates it")]
    tables, views = expected_objects(dq_database)
    waiting = {m.table: m for m in reversed(migrations.pending(spark, dq_database))}
    findings = [Finding(f"{dq_database}.{t.name}", PENDING, f"{waiting[t.name]}; run dre install --apply")
                if t.name in waiting else _check_table(spark, dq_database, t) for t in tables.values()]
    findings += [_check_view(spark, dq_database, v.name, v.query) for v in views.values()]
    return findings


def install_command(spark: SparkSession | None, dq_database: str, mode: str) -> int:
    """`dre install --print|--apply|--check`. Exit 0 done or matching; 1 store differs or
    apply failed; 3 the database does not exist. spark may be None for --print."""
    if mode == "print":
        print(";\n\n".join(render_ddl(dq_database)) + ";")
        return 0
    assert spark is not None
    if not spark.catalog.databaseExists(dq_database):
        print(f"dre install: database {dq_database} does not exist. The platform team creates the empty "
              "database and grants access; then run dre install --apply.")
        return 3
    if mode == "apply":
        waiting = migrations.pending(spark, dq_database)
        for what, action in _steps(spark, dq_database):
            try:
                action()
            except Exception as exc:  # report and stop: the store needs a person to look
                print(f"dre install: failed at `{what}`: {error_detail(exc)}. "
                      "Run dre install --check to see what differs.")
                return 1
        for m in waiting:
            print(f"dre install: applied {m}")
        print(f"dre install: dq store tables and views are in place in {dq_database}")
        return 0
    findings = check_store(spark, dq_database)
    for finding in findings:
        print(finding)
    problems = [f for f in findings if f.status != OK]
    print(f"dre install: {len(problems)} of {len(findings)} objects missing or different in {dq_database}"
          if problems else f"dre install: {dq_database} matches the DDL")
    return 1 if problems else 0


def missing_objects(spark: SparkSession, dq_database: str) -> list[str]:
    """Names of the database, tables or views that do not exist, and of the tables a migration
    has not reached yet (`dre run` cannot write to them)."""
    return [f.name + (" (migration pending)" if f.status == PENDING else "")
            for f in check_store(spark, dq_database) if f.status in (MISSING, PENDING)]


def _check_table(spark: SparkSession, db: str, table: TableDef) -> Finding:
    name = f"{db}.{table.name}"
    if not spark.catalog.tableExists(name):
        return Finding(name, MISSING)
    columns = spark.catalog.listColumns(name)
    actual = tuple((c.name.lower(), c.dataType.replace(" ", "").lower()) for c in columns)
    partitions = tuple(c.name.lower() for c in columns if c.isPartition)
    problems = []
    if actual != table.columns:
        problems.append(f"columns {list(actual)} != expected {list(table.columns)}")
    if partitions != table.partition_columns:
        problems.append(f"partitioned by {list(partitions)}, expected {list(table.partition_columns)}")
    return Finding(name, DIFFERS, "; ".join(problems)) if problems else Finding(name, OK)


def _check_view(spark: SparkSession, db: str, view: str, query: str) -> Finding:
    name = f"{db}.{view}"
    if not spark.catalog.tableExists(name):
        return Finding(name, MISSING)
    info = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE EXTENDED {name}").collect()}
    if _normalise(info.get("View Text", "")) != _normalise(query):
        return Finding(name, DIFFERS, "view definition differs from the DDL")
    return Finding(name, OK)


def _normalise(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().rstrip(";")
