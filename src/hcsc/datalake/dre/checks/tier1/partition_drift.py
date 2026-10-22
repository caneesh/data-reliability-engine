"""T1_PARTITION_DRIFT: no partition outside the window was rewritten with a different row count
(spec section 6, release 4). A parametrised check: it runs where `check_params` configures it.

Detection is by file metadata, confirmation by a bounded count. Each run lists the table's
partitions and each partition's directory through the Hadoop FileSystem API (file count, total
bytes, latest modification time; no data read) and compares with the state the previous run
stored in dq_partition_state. A partition whose files changed, or seen for the first time, is
counted (one COUNT(*) over those partitions), so its stored count always matches its last state.
The partitions the window could touch (the pruning set, widened by one granule each side) are
expected to change; any other partition whose count moved by more than `tolerance_rows`, or that
disappeared, is a violation. Hive statistics are not used: a Spark write leaves them stale, and
the engine may not refresh them.

Population: partitions outside the window with an earlier state. The first sight of a table is
DID_NOT_RUN / insufficient_history and records every partition's state and count. The state rows
ride on the result (CheckResult.partition_state) and the runner appends them with the run id.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

from pydantic import Field

from hcsc.datalake.dre.checks.base import (
    DID_NOT_RUN, FAILED, PASSED, REASONS, Check, CheckContext, CheckResult, did_not_run, render_sql,
)
from hcsc.datalake.dre.checks.partitions import MAX_VALUES, partition_values
from hcsc.datalake.dre.checks.registry import register_check
from hcsc.datalake.dre.checks.times import sql_string
from hcsc.datalake.dre.config.models import Model
from hcsc.datalake.dre.sources.hdfs import LandingError, path_exists, summarise_directory
from hcsc.datalake.dre.store.partitions import PartitionState, latest_state

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset

LISTED = 100  # partitions named in detail, at most


class PartitionDriftParams(Model):
    tolerance_rows: int = Field(default=0, ge=0)  # a count may move this much without a violation


def parse_spec(spec: str) -> dict[str, str]:
    """`col=value/col2=value2` (as SHOW PARTITIONS prints it, values escaped as Hive escapes them)
    -> {col: value}, values unescaped."""
    pairs = (part.split("=", 1) for part in spec.split("/"))
    return {col.lower(): unquote(value) for col, value in pairs}


def table_type(spark: SparkSession, table: str) -> str:
    """MANAGED, EXTERNAL or VIEW (what the catalog says)."""
    return str(spark.catalog.getTable(table).tableType or "").upper()


def table_location(spark: SparkSession, table: str) -> str | None:
    for row in spark.sql(f"DESCRIBE FORMATTED {table}").collect():
        if (row.col_name or "").strip().lower() == "location":
            return row.data_type.strip()
    return None


def partition_location(spark: SparkSession, table: str, spec: str, table_dir: str | None) -> str:
    """The partition's directory: the Hive layout under the table's location when it exists there,
    otherwise what the metastore says (a partition added with its own LOCATION)."""
    if table_dir is not None:
        candidate = table_dir.rstrip("/") + "/" + spec
        if path_exists(spark, candidate):
            return candidate
    clause = ", ".join(f"{col} = {sql_string(value)}" for col, value in parse_spec(spec).items())
    # The partition's own Location comes first; a second Location row (the table's) follows it.
    for row in spark.sql(f"DESCRIBE FORMATTED {table} PARTITION ({clause})").collect():
        if (row.col_name or "").strip().lower() == "location":
            return row.data_type.strip()
    raise LandingError("hdfs_unavailable", f"no location for a partition of {table}")


def count_partitions(spark: SparkSession, table: str, specs: list[str], every: bool) -> dict[str, int]:
    """Rows per partition spec, in one query: every partition of the table, or only the named ones."""
    if not specs:
        return {}
    columns = list(parse_spec(specs[0]))
    clauses = [] if every else [
        " AND ".join(f"{col} = {sql_string(value)}" for col, value in parse_spec(spec).items()) for spec in specs]
    sql = render_sql("t1_partition_drift_count.sql.j2", table=table, columns=columns, clauses=clauses)
    by_values = {tuple(str(row[f"g_{i}"]) for i in range(1, len(columns) + 1)): int(row.rows)
                 for row in spark.sql(sql).collect()}
    return {spec: by_values.get(tuple(parse_spec(spec).values()), 0) for spec in specs}


class PartitionDrift(Check):
    check_id = "T1_PARTITION_DRIFT"
    params_model = PartitionDriftParams

    def required_columns(self, dataset: Dataset, params: Any = None) -> list[str]:
        return [dataset.partition_column] if dataset.partition_column else []

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        ds, spark = ctx.dataset, ctx.spark
        if ctx.dq_database is None:
            return [did_not_run("invalid_config", detail="T1_PARTITION_DRIFT needs the dq store")]
        if ds.table is None or ds.partition_column is None or ds.partition_format is None:
            return [did_not_run("invalid_config", detail=(
                f"T1_PARTITION_DRIFT needs a table with partition_column and partition_format on dataset {ds.dataset}"))]
        if table_type(spark, ds.table) == "VIEW":
            return [did_not_run("invalid_config", detail=f"{ds.table} is a view, not a partitioned table")]
        partition_columns = [c.name.lower() for c in spark.catalog.listColumns(ds.table) if c.isPartition]
        if ds.partition_column.lower() not in partition_columns:
            return [did_not_run("invalid_config", detail=(
                f"{ds.table} is not partitioned by {ds.partition_column} (partition columns: {partition_columns})"))]
        specs = [row[0] for row in spark.sql(f"SHOW PARTITIONS {ds.table}").collect()]
        if len(specs) > MAX_VALUES:
            return [did_not_run("invalid_config", detail=(
                f"{ds.table} has {len(specs)} partitions, more than the {MAX_VALUES} the check lists"))]
        try:
            table_dir = table_location(spark, ds.table)
            current = {spec: summarise_directory(spark, partition_location(spark, ds.table, spec, table_dir))
                       for spec in specs}
        except LandingError as exc:
            return [did_not_run(exc.code, detail=exc.detail)]

        previous = latest_state(spark, ctx.dq_database, ds.dataset)
        expected = set(partition_values(ds, event.window_start, event.window_end, ctx.default_timezone) or ())
        in_window = {spec: parse_spec(spec)[ds.partition_column.lower()] in expected for spec in specs}
        to_count = [spec for spec in specs
                    if spec not in previous or not previous[spec].same_files(_as_state(spec, current[spec]))]
        counts = count_partitions(spark, ds.table, to_count, every=len(to_count) == len(specs))
        now = datetime.now(timezone.utc)

        tally = {"expected": 0, "unchanged": 0, "rewritten_same": 0, "changed": 0, "dropped": 0, "new_outside": 0}
        changed: list[dict[str, Any]] = []
        same: list[str] = []
        states: list[PartitionState] = []
        tolerance = ctx.params.tolerance_rows
        for spec in specs:
            summary = current[spec]
            before = previous.get(spec)
            if spec in counts:
                count, counted_at = counts[spec], now
            else:
                count, counted_at = before.row_count, before.counted_at  # type: ignore[union-attr]
            states.append(PartitionState(spec, summary.file_count, summary.total_bytes, summary.latest_modified_at,
                                         count, counted_at))
            if in_window[spec]:
                tally["expected"] += 1
            elif before is None:
                tally["new_outside"] += 1
            elif spec not in counts:
                tally["unchanged"] += 1
            elif before.row_count is not None and abs(count - before.row_count) <= tolerance:
                tally["rewritten_same"] += 1
                same.append(spec)
            else:
                tally["changed"] += 1
                changed.append({"partition": spec, "before": before.row_count, "after": count})
        dropped = [{"partition": spec, "before": state.row_count}
                   for spec, state in sorted(previous.items())
                   if spec not in current and parse_spec(spec).get(ds.partition_column.lower()) not in expected]
        tally["dropped"] = len(dropped)

        record = {"table": ds.table, "partitions": len(specs), "changed": changed[:LISTED], "dropped": dropped[:LISTED],
                  "rewritten_same": same[:LISTED]}
        if not previous:
            return [CheckResult(DID_NOT_RUN, population=len(specs), reason_category=REASONS["insufficient_history"],
                                reason_code="insufficient_history", detail=json.dumps(record),
                                observed=f"{len(specs)} partitions recorded with their row counts; nothing to compare yet",
                                partition_state=tuple(states))]
        population = sum(1 for spec in specs if not in_window[spec] and spec in previous) + len(dropped)
        violations = len(changed) + len(dropped)
        observed = (f"{len(specs)} partitions: {tally['expected']} expected in the window, {tally['unchanged']} unchanged, "
                    f"{tally['rewritten_same']} rewritten with the same rows, {tally['changed']} changed, "
                    f"{tally['dropped']} dropped, {tally['new_outside']} new outside the window")
        expected_text = (f"no partition outside the window changes its row count by more than {tolerance} "
                         "or disappears")
        if not population:
            return [CheckResult(DID_NOT_RUN, population=0, reason_category=REASONS["empty_population"],
                                reason_code="empty_population", observed=observed, detail=json.dumps(record),
                                partition_state=tuple(states))]
        return [CheckResult(FAILED if violations else PASSED, population=population, violations=violations,
                            observed=observed, expected=expected_text, detail=json.dumps(record),
                            partition_state=tuple(states))]


def _as_state(spec: str, summary: Any) -> PartitionState:
    return PartitionState(spec, summary.file_count, summary.total_bytes, summary.latest_modified_at, None, None)


register_check(PartitionDrift())
