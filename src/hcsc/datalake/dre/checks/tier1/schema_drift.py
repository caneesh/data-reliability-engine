"""T1_SCHEMA_DRIFT: the table's column names and types match the last recorded schema (spec section 6).

Population: the table's columns. The schema hash goes in `observed` (the previous
one in `expected`), and the table and columns in `detail` as JSON, with the added,
removed and retyped columns on a FAILED result. The first evaluation of a table has
nothing to compare with: DID_NOT_RUN / insufficient_history, recording the baseline.
Runs once per physical table (the runner picks one dataset per table).
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import (
    DID_NOT_RUN, FAILED, PASSED, REASONS, Check, CheckContext, CheckResult, did_not_run, render_sql,
)

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset


def table_columns(ctx: CheckContext) -> list[tuple[str, str]]:
    columns = []
    for row in ctx.spark.sql(render_sql("t1_schema_drift.sql.j2", table=ctx.dataset.table)).collect():
        if not row.col_name or row.col_name.startswith("#"):
            break
        columns.append((row.col_name.lower(), row.data_type.lower()))
    return columns


def schema_hash(columns: list[tuple[str, str]]) -> str:
    return hashlib.sha256(",".join(f"{n}:{t}" for n, t in columns).encode("utf-8")).hexdigest()


class SchemaDrift(Check):
    check_id = "T1_SCHEMA_DRIFT"

    def required_columns(self, dataset: Dataset) -> list[str]:
        return []

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        if ctx.dq_database is None:
            return [did_not_run("invalid_config", detail="T1_SCHEMA_DRIFT needs the dq store")]
        table = ctx.dataset.table
        columns = table_columns(ctx)
        current = schema_hash(columns)
        record = {"table": table, "columns": [f"{n}:{t}" for n, t in columns]}
        previous = ctx.spark.sql(render_sql(
            "t1_schema_drift_history.sql.j2", dq_database=ctx.dq_database, table=table)).collect()
        if not previous:
            return [CheckResult(DID_NOT_RUN, population=len(columns), observed=current,
                                reason_category=REASONS["insufficient_history"], reason_code="insufficient_history",
                                detail=json.dumps(record))]
        before_hash = previous[0].observed
        if before_hash == current:
            return [CheckResult(PASSED, population=len(columns), violations=0, observed=current,
                                expected=before_hash, detail=json.dumps(record))]
        before = dict(c.split(":", 1) for c in json.loads(previous[0].detail)["columns"])
        now = dict(columns)
        record["added"] = sorted(set(now) - set(before))
        record["removed"] = sorted(set(before) - set(now))
        record["retyped"] = sorted(c for c in set(now) & set(before) if now[c] != before[c])
        changed = len(record["added"]) + len(record["removed"]) + len(record["retyped"])
        return [CheckResult(FAILED, population=len(columns), violations=max(changed, 1), observed=current,
                            expected=before_hash, detail=json.dumps(record))]
