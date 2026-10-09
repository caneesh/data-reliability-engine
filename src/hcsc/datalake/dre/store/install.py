"""`dre install`: print or apply the dq store DDL, or check a store against it.

The platform team creates the empty dq database and grants access; `dre
install --apply` creates the tables and views in it. Only this module, and the
local setup used by tests, may apply the DDL; `dre run` only checks the store
exists.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from hcsc.datalake.dre.store.names import validate_dq_database
from hcsc.datalake.dre.store.schema import expected_objects, render_ddl

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

OK, MISSING, DIFFERS = "OK", "MISSING", "DIFFERS"


@dataclass(frozen=True)
class Finding:
    name: str
    status: str
    detail: str = ""

    def __str__(self) -> str:
        return f"{self.status:8} {self.name}" + (f": {self.detail}" if self.detail else "")


def apply_ddl(spark: SparkSession, dq_database: str) -> None:
    """Create the dq tables and views that do not exist yet. Safe to repeat."""
    for statement in render_ddl(dq_database):
        spark.sql(statement)


def check_store(spark: SparkSession, dq_database: str) -> list[Finding]:
    """Compare the database's tables and views with the DDL, one finding per object."""
    validate_dq_database(dq_database)
    if not spark.catalog.databaseExists(dq_database):
        return [Finding(dq_database, MISSING, "database does not exist; the platform team creates it")]
    tables, views = expected_objects(dq_database)
    findings = [_check_table(spark, dq_database, t) for t in tables.values()]
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
        for statement in render_ddl(dq_database):
            try:
                spark.sql(statement)
            except Exception as exc:  # report and stop: the store needs a person to look
                first_line = statement.splitlines()[0]
                print(f"dre install: failed at `{first_line}`: {str(exc).splitlines()[0]}. "
                      "Run dre install --check to see what differs.")
                return 1
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
    """Names of the database, tables or views that do not exist."""
    return [f.name for f in check_store(spark, dq_database) if f.status == MISSING]


def _check_table(spark: SparkSession, db: str, table) -> Finding:
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
