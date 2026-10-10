"""Cause checks for landed files (spec section 7, "File not loaded").

Facts come from DRE's own registry (dq_file) and the feed's raw dataset: when each file was
first seen, the sizes it was seen with, and its raw rows (matched on base file name). Files
are read only to see whether their header can be read.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.causes.base import (
    CauseContext, Failure, Outcome, confirmed, not_ready, ruled_out,
)
from hcsc.datalake.dre.checks.base import error_detail, render_sql
from hcsc.datalake.dre.checks.times import as_utc

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

NO_RAW = "the feed has no raw dataset with a file_name_column"


def landing_files(cx: CauseContext) -> dict[str, Any]:
    """path -> row (path, first_seen_at, sizes, raw_rows) for every file of the feed in dq_file."""
    def query() -> dict[str, Any]:
        raw = cx.raw_dataset()
        sql = render_sql(
            "cause_landing_files.sql.j2", dq_database=cx.ctx.dq_database, feed=cx.feed.feed,
            raw_table=raw.table if raw else None, file_name_column=raw.file_name_column if raw else None,
            feed_filter=raw.feed_filter if raw else None,
        )
        return {r.path: r for r in cx.spark.sql(sql).collect()}
    return cx.cached("landing_files", query)


def unloaded_files_after(cx: CauseContext, after: datetime) -> tuple[str, ...] | None:
    """Files first seen at or after `after` with no raw rows; None without a raw dataset."""
    if cx.raw_dataset() is None:
        return None
    return tuple(sorted(p for p, r in landing_files(cx).items()
                        if r.raw_rows == 0 and as_utc(r.first_seen_at) >= as_utc(after)))


def pipeline_stalled(cx: CauseContext, failure: Failure) -> Outcome:
    """CONFIRMED when other files were first seen with or after this one (or since the missed
    slot) and none of them has raw rows either. No such file at all: RULED_OUT (nothing shows the
    pipeline stopped)."""
    if cx.raw_dataset() is None:
        return not_ready(NO_RAW)
    files = landing_files(cx)
    if failure.slot is not None:
        after = as_utc(failure.slot)
    elif failure.files and failure.files[0] in files:
        after = as_utc(files[failure.files[0]].first_seen_at)
    else:
        return ruled_out(reason="the file is not in the landing registry")
    # Files first seen with this one (in the same listing) or later; one of them loading shows the
    # pipeline ran.
    itself = () if failure.slot is not None else (failure.files or ())
    later = [r for p, r in files.items() if as_utc(r.first_seen_at) >= after and p not in itself]
    loaded = [r for r in later if r.raw_rows > 0]
    if not later:
        return ruled_out(later_files=0)
    if loaded:
        return ruled_out(later_files=len(later), later_files_loaded=len(loaded))
    return confirmed(later_files=len(later), later_files_loaded=0)


def _read_header(spark: SparkSession, path: str, file_format: str) -> bool:
    """True when the first record (or header) of the file can be read with the format's reader."""
    if file_format == "sequence":
        return bool(spark.sparkContext.sequenceFile(path).take(1))
    if file_format in ("text", "xml"):
        lines = spark.sparkContext.textFile(path).take(1)
        return bool(lines) and (file_format == "text" or lines[0].lstrip().startswith("<"))
    if file_format == "csv":
        return bool(spark.read.option("header", "true").csv(path).columns)
    return bool(spark.read.format(file_format).load(path).columns)  # parquet, orc


def _missing(exc: BaseException) -> bool:
    text = f"{type(exc).__name__} {exc}"
    return "FileNotFoundException" in text or "PATH_NOT_FOUND" in text or "does not exist" in text


def unreadable(cx: CauseContext, failure: Failure) -> Outcome:
    """CONFIRMED when a file of the failure has no readable header for the feed's file_format."""
    if cx.feed is None or cx.feed.landing is None:
        return not_ready("the feed has no landing configuration")
    if failure.files is None:
        return not_ready(NO_RAW)
    if not failure.files:
        return ruled_out(reason="no unloaded file for this failure")
    bad: list[dict[str, str]] = []
    for path in failure.files:
        try:
            if not _read_header(cx.spark, path, cx.feed.landing.file_format):
                bad.append({"file": path, "error": "no header or first record"})
        except Exception as exc:
            if _missing(exc):
                raise
            bad.append({"file": path, "error": error_detail(exc)})
    if bad:
        return confirmed(file_format=cx.feed.landing.file_format, unreadable=bad)
    return ruled_out(file_format=cx.feed.landing.file_format, files_read=len(failure.files))
