"""Render and apply the dq store DDL (store/ddl.sql)."""

from __future__ import annotations

from importlib.resources import files
from typing import TYPE_CHECKING

from jinja2 import Environment, StrictUndefined

from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

VIEWS = ("v_latest_result", "v_open_keys", "v_file_status")


def render_ddl(dq_database: str) -> list[str]:
    """The DDL statements for a validated dq database name, in order."""
    validate_dq_database(dq_database)
    template = files("hcsc.datalake.dre.store").joinpath("ddl.sql").read_text(encoding="utf-8")
    sql = Environment(undefined=StrictUndefined, autoescape=False).from_string(template).render(
        dq_database=dq_database
    )
    lines = [line for line in sql.splitlines() if not line.lstrip().startswith("--")]
    return [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]


def apply_ddl(spark: SparkSession, dq_database: str) -> None:
    """Create the dq tables if missing and (re)create the views. Safe to repeat."""
    for statement in render_ddl(dq_database):
        spark.sql(statement)
