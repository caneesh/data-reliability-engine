"""Render store/ddl.sql for a dq database, and describe the objects it creates."""

from __future__ import annotations

import re
from dataclasses import dataclass
from importlib.resources import files

from jinja2 import Environment, StrictUndefined

from hcsc.datalake.dre.store.names import validate_dq_database

VIEWS = ("v_latest_run", "v_latest_result", "v_open_keys", "v_file_status")

_TABLE = re.compile(
    r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+\S+\.(\w+)\s+\((.*)\)\s+PARTITIONED\s+BY\s+\((.*?)\)\s+STORED\s+AS\s+ORC",
    re.S,
)
_VIEW = re.compile(r"CREATE\s+VIEW\s+IF\s+NOT\s+EXISTS\s+\S+\.(\w+)\s+AS\s+(.*)", re.S)
_COLUMN = re.compile(r"(\w+)\s+([A-Za-z]+(?:<[^>]*>)?)")


@dataclass(frozen=True)
class TableDef:
    name: str
    columns: tuple[tuple[str, str], ...]  # (name, lower-case type), data columns then partition columns
    partition_columns: tuple[str, ...]


@dataclass(frozen=True)
class ViewDef:
    name: str
    query: str


def render_ddl(dq_database: str) -> list[str]:
    """The DDL statements for a validated dq database name, in order."""
    validate_dq_database(dq_database)
    template = files("hcsc.datalake.dre.store").joinpath("ddl.sql").read_text(encoding="utf-8")
    sql = Environment(undefined=StrictUndefined, autoescape=False).from_string(template).render(
        dq_database=dq_database
    )
    lines = [line for line in sql.splitlines() if not line.lstrip().startswith("--")]
    return [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]


def expected_objects(dq_database: str) -> tuple[dict[str, TableDef], dict[str, ViewDef]]:
    """The tables and views the DDL creates, parsed from the rendered statements."""
    tables: dict[str, TableDef] = {}
    views: dict[str, ViewDef] = {}
    for statement in render_ddl(dq_database):
        if m := _TABLE.fullmatch(statement):
            name, body, partition = m.groups()
            body = re.sub(r"--[^\n]*", "", body)
            data = [(c, t.lower()) for c, t in _COLUMN.findall(body)]
            parts = [(c, t.lower()) for c, t in _COLUMN.findall(partition)]
            tables[name] = TableDef(name, tuple(data + parts), tuple(c for c, _ in parts))
        elif m := _VIEW.fullmatch(statement):
            views[m.group(1)] = ViewDef(m.group(1), m.group(2).strip())
        else:
            raise ValueError(f"unrecognised DDL statement: {statement[:60]}")
    return tables, views
