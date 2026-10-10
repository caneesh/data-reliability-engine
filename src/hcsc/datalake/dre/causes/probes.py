"""The generic probe types (spec section 7). Each reads evidence and knows nothing about a
source: what it reads comes from the feed's `probes:` parameters. All reads only.

- file_exists: CONFIRMED when the file exists.
- file_value_compare: reads a value from a file (first regex group, else the whole match),
  parses it with `format` and compares it with the failure's partition (the same regex on the
  folder name of each landed file of the failure, or the slot's partition) or with the window.
  CONFIRMED when the value is later than the partition, or outside the window.
- log_contains: CONFIRMED when a log file matching the glob has a line matching the pattern
  that also names one of the failure's files (base name).
- table_contains: CONFIRMED when the table has rows matching the condition for the failure's
  upstream file (base name of the upstream file_name_column) or key (the upstream's mapped key).
- size_changed: CONFIRMED when a file of the failure was seen with more than one size.

Values read from files are parsed through Spark; log lines and file contents never go into
evidence (only paths, counts and the parsed values).
"""

from __future__ import annotations

import re
from typing import Any

from hcsc.datalake.dre.causes.base import CauseContext, Failure, Outcome, confirmed, not_ready, ruled_out
from hcsc.datalake.dre.causes.landing import NO_RAW, landing_files
from hcsc.datalake.dre.checks.base import render_sql, utc_literal
from hcsc.datalake.dre.checks.hop.common import HASH_FUNCTION, register_key_hash
from hcsc.datalake.dre.checks.keys import column_expr, key_string
from hcsc.datalake.dre.checks.times import as_utc, sql_string
from hcsc.datalake.dre.sources.hdfs import glob_files, path_exists

READ_LINES = 1000  # lines of a value file read when looking for the value


def file_exists(cx: CauseContext, params: Any, failure: Failure) -> Outcome:
    return confirmed(path=params.path) if path_exists(cx.spark, params.path) else ruled_out(path=params.path)


def _parse(cx: CauseContext, value: str, fmt: str, tz: str | None = None):
    sql = render_sql("cause_parse_time.sql.j2", value=sql_string(value), fmt=sql_string(fmt),
                     tz=sql_string(tz) if tz else None, slot=None)
    return cx.spark.sql(sql).collect()[0].v


def _slot_partition(cx: CauseContext, failure: Failure, fmt: str):
    tz = (cx.feed.cadence.timezone if cx.feed and cx.feed.cadence.timezone else None) or cx.ctx.default_timezone
    sql = render_sql("cause_parse_time.sql.j2", slot=utc_literal(failure.slot), fmt=sql_string(fmt),
                     tz=sql_string(tz), value=None)
    return cx.spark.sql(sql).collect()[0].v


def _extract(pattern: str, text: str) -> str | None:
    match = re.search(pattern, text)
    if match is None:
        return None
    return match.group(1) if match.groups() else match.group(0)


def file_value_compare(cx: CauseContext, params: Any, failure: Failure) -> Outcome:
    lines = [r.value for r in cx.spark.read.text(params.path).limit(READ_LINES).collect()]
    raw = _extract(params.extract_regex, "\n".join(lines))
    if raw is None:
        raise ValueError(f"extract_regex found no value in {params.path}")
    if params.compare_to == "window":
        value = _parse(cx, raw, params.format, cx.ctx.default_timezone)
        if value is None:
            raise ValueError(f"the value in {params.path} does not match format")
        value = as_utc(value)
        evidence = {"value": raw, "window_start": cx.event.window_start.isoformat(),
                    "window_end": cx.event.window_end.isoformat()}
        outside = value < cx.event.window_start or value >= cx.event.window_end
        return confirmed(**evidence) if outside else ruled_out(**evidence)
    value = _parse(cx, raw, params.format)
    if value is None:
        raise ValueError(f"the value in {params.path} does not match format")
    partitions: list[tuple[str, Any]] = []
    if failure.files:
        for path in failure.files:
            folder = path.rstrip("/").rsplit("/", 2)[-2] if path.count("/") >= 1 else ""
            text = _extract(params.extract_regex, folder)
            parsed = _parse(cx, text, params.format) if text is not None else None
            if parsed is None:
                raise ValueError("no partition value in a failing file's folder name")
            partitions.append((text, parsed))
    elif failure.slot is not None:
        partitions.append((failure.slot.isoformat(), _slot_partition(cx, failure, params.format)))
    else:
        return ruled_out(reason="no partition to compare with")
    later = [text for text, parsed in partitions if value > parsed]
    evidence = {"value": raw, "partitions": [text for text, _ in partitions]}
    return confirmed(**evidence, behind=later) if later else ruled_out(**evidence)


def _names(failure: Failure) -> list[str]:
    if failure.files:
        return sorted({p.rstrip("/").rsplit("/", 1)[-1] for p in failure.files})
    return [failure.file_name] if failure.file_name else []


def log_contains(cx: CauseContext, params: Any, failure: Failure) -> Outcome:
    from pyspark.sql import functions as F

    names = _names(failure)
    if not names:
        return ruled_out(reason="no file to look for")
    logs = glob_files(cx.spark, params.path_glob)
    if not logs:
        return ruled_out(logs=0)
    lines = cx.spark.read.text(logs).withColumn("log", F.input_file_name())
    names_hit = None
    for name in names:
        hit = F.instr(F.col("value"), name) > 0
        names_hit = hit if names_hit is None else names_hit | hit
    found = lines.filter(F.col("value").rlike(params.pattern) & names_hit).groupBy("log").count().collect()
    if found:
        return confirmed(logs={r["log"]: r["count"] for r in found})
    return ruled_out(logs=len(logs))


def table_contains(cx: CauseContext, params: Any, failure: Failure) -> Outcome:
    up = cx.upstream()
    if up is None:
        return not_ready("table_contains needs the hop's upstream dataset")
    if failure.key is not None:
        if cx.ctx.key_secret is not None:
            register_key_hash(cx.spark, cx.ctx.key_secret)
        by, ref = "key", failure.ref
        mapping = cx.dataset.key_map[cx.upstream_id]
        column = key_string([column_expr(up, mapping[c]) for c in cx.dataset.key])
    elif failure.file_name is not None:
        if up.file_name_column is None:
            return not_ready(f"upstream {up.dataset} has no file_name_column")
        by, ref, column = "file", failure.file_name, up.file_name_column
    else:
        return ruled_out(reason="no file or key to look for")

    def query() -> set[str]:
        sql = render_sql("cause_table_contains.sql.j2", by=by, column=column, table=params.table,
                         condition=params.condition, hash_fn=HASH_FUNCTION)
        return {r.ref for r in cx.spark.sql(sql).collect()}
    found = cx.cached(f"table_contains:{by}:{params.table}:{params.condition}", query)
    evidence = {"table": params.table, **({"id": params.id} if params.id else {}),
                **({"code_ref": params.code_ref} if params.code_ref else {})}
    return confirmed(**evidence) if ref in found else ruled_out(**evidence)


def size_changed(cx: CauseContext, params: Any, failure: Failure) -> Outcome:
    if failure.files is None:
        return not_ready(NO_RAW)
    if not failure.files:
        return ruled_out(reason="no unloaded file for this failure")
    files = landing_files(cx)
    changed = [p for p in failure.files if p in files and files[p].sizes > 1]
    return confirmed(files=changed) if changed else ruled_out(files_checked=len(failure.files))


PROBES = {
    "file_exists": file_exists,
    "file_value_compare": file_value_compare,
    "log_contains": log_contains,
    "table_contains": table_contains,
    "size_changed": size_changed,
}
