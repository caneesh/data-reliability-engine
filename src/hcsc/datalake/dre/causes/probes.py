"""The generic probe types (spec section 7). Each reads evidence and knows nothing about a
source: what it reads comes from the feed's `probes:` parameters. All reads only.

- file_exists: CONFIRMED when the file exists.
- file_value_compare: reads a value from a file (first regex group, else the whole match),
  parses it with `format` and compares it with the failure's partition (the same regex on the
  folder name of each landed file of the failure, or the slot's partition) or with the window.
  CONFIRMED when the value is later than the partition (confirm_when: different: when it differs
  from it), or outside the window.
- log_contains: CONFIRMED when a log file matching the glob has a line matching the pattern
  that also names one of the failure's files (base name).
- table_contains: CONFIRMED when the table has rows matching the condition for the failure's
  upstream file (base name of the upstream file_name_column) or key (the upstream's mapped key).
- size_changed: CONFIRMED when a file of the failure was seen with more than one size.

Each probe is registered with its parameter model (`probe`, config/probes.py).
Values read from files are parsed through Spark; log lines and file contents never go into
evidence (only paths, counts and the parsed values). What a probe reads (a value file, the log
lines matching a pattern, a parsed value) is read once per evaluation and shared by its failures
through the CauseContext cache.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Literal

from hcsc.datalake.dre.causes.base import CauseContext, Failure, Outcome, confirmed, not_ready, ruled_out
from hcsc.datalake.dre.causes.landing import NO_RAW, landing_files
from hcsc.datalake.dre.checks.base import render_sql, utc_literal
from hcsc.datalake.dre.checks.hop.common import HASH_FUNCTION, register_key_hash
from hcsc.datalake.dre.checks.keys import column_expr, key_string
from hcsc.datalake.dre.checks.times import as_utc, sql_string
from hcsc.datalake.dre.config.models import Model, NonEmptyStr, Regex, SqlFragment, TableName
from hcsc.datalake.dre.config.probes import probe
from hcsc.datalake.dre.sources.hdfs import glob_files, path_exists

READ_LINES = 1000  # lines of a value file read when looking for the value


class FileExists(Model):
    path: NonEmptyStr | None = None


class FileValueCompare(Model):
    path: NonEmptyStr | None = None
    extract_regex: Regex | None = None
    format: NonEmptyStr | None = None  # Spark datetime pattern for the extracted value, e.g. yyyyMMdd
    compare_to: Literal["partition", "window"] | None = None
    # partition: CONFIRMED when the file's value is later than the failure's partition (spec
    # default), or when it differs from it at all ("different").
    confirm_when: Literal["later", "different"] = "later"


class LogContains(Model):
    path_glob: NonEmptyStr | None = None
    pattern: Regex | None = None


class TableContains(Model):
    table: TableName | None = None
    condition: SqlFragment | None = None
    id: NonEmptyStr | None = None        # optional label, e.g. a filter rule's name
    code_ref: NonEmptyStr | None = None  # optional: where the rule lives (file and line, or commit)


class SizeChanged(Model):
    pass


@probe("file_exists", FileExists)
def file_exists(cx: CauseContext, params: Any, failure: Failure) -> Outcome:
    return confirmed(path=params.path) if path_exists(cx.spark, params.path) else ruled_out(path=params.path)


def _parse(cx: CauseContext, value: str, fmt: str, tz: str | None = None) -> datetime | None:
    def query() -> datetime | None:
        sql = render_sql("cause_parse_time.sql.j2", value=sql_string(value), fmt=sql_string(fmt),
                         tz=sql_string(tz) if tz else None, slot=None)
        return cx.spark.sql(sql).collect()[0].v
    return cx.cached(f"parse:{fmt}:{tz}:{value}", query)


def _slot_partition(cx: CauseContext, failure: Failure, fmt: str) -> datetime | None:
    tz = (cx.feed.cadence.timezone if cx.feed and cx.feed.cadence.timezone else None) or cx.ctx.default_timezone
    sql = render_sql("cause_parse_time.sql.j2", slot=utc_literal(failure.slot), fmt=sql_string(fmt),
                     tz=sql_string(tz), value=None)
    return cx.spark.sql(sql).collect()[0].v


def _extract(pattern: str, text: str) -> str | None:
    match = re.search(pattern, text)
    if match is None:
        return None
    return match.group(1) if match.groups() else match.group(0)


def _file_value(cx: CauseContext, path: str, regex: str) -> str | None:
    """The value extract_regex finds in the first READ_LINES lines of the file (read once)."""
    def read() -> str | None:
        lines = [r.value for r in cx.spark.read.text(path).limit(READ_LINES).collect()]
        return _extract(regex, "\n".join(lines))
    return cx.cached(f"file_value:{path}:{regex}", read)


@probe("file_value_compare", FileValueCompare)
def file_value_compare(cx: CauseContext, params: Any, failure: Failure) -> Outcome:
    raw = _file_value(cx, params.path, params.extract_regex)
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
    if params.confirm_when == "different":
        hits = [text for text, parsed in partitions if value != parsed]
    else:
        hits = [text for text, parsed in partitions if value > parsed]
    evidence = {"value": raw, "partitions": [text for text, _ in partitions], "confirm_when": params.confirm_when}
    return confirmed(**evidence, matched=hits) if hits else ruled_out(**evidence)


def _names(failure: Failure) -> list[str]:
    if failure.files:
        return sorted({p.rstrip("/").rsplit("/", 1)[-1] for p in failure.files})
    return [failure.file_name] if failure.file_name else []


@probe("log_contains", LogContains)
def log_contains(cx: CauseContext, params: Any, failure: Failure) -> Outcome:
    from pyspark.sql import functions as F

    names = _names(failure)
    if not names:
        return ruled_out(reason="no file to look for")

    def matching() -> tuple[int, list[tuple[str, str]]]:
        """(log files, (log, line) for every line matching the pattern): read once per evaluation.
        The lines stay in memory; only log paths and counts go into evidence."""
        logs = glob_files(cx.spark, params.path_glob)
        if not logs:
            return 0, []
        lines = cx.spark.read.text(logs).withColumn("log", F.input_file_name())
        return len(logs), [(r["log"], r["value"]) for r in lines.filter(F.col("value").rlike(params.pattern))
                           .select("log", "value").collect()]
    logs, lines = cx.cached(f"log_contains:{params.path_glob}:{params.pattern}", matching)
    if not logs:
        return ruled_out(logs=0)
    found: dict[str, int] = {}
    for log, line in lines:
        if any(name in line for name in names):
            found[log] = found.get(log, 0) + 1
    if found:
        return confirmed(logs=found)
    return ruled_out(logs=logs)


@probe("table_contains", TableContains)
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


@probe("size_changed", SizeChanged)
def size_changed(cx: CauseContext, params: Any, failure: Failure) -> Outcome:
    if failure.files is None:
        return not_ready(NO_RAW)
    if not failure.files:
        return ruled_out(reason="no unloaded file for this failure")
    files = landing_files(cx)
    changed = [p for p in failure.files if p in files and files[p].sizes > 1]
    return confirmed(files=changed) if changed else ruled_out(files_checked=len(failure.files))

