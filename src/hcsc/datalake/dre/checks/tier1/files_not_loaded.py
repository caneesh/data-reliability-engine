"""T1_FILES_NOT_LOADED: landed files with no rows in the raw dataset (spec section 6).

Applies to RAW datasets with a file_name_column in file-pattern feeds. Population:
the feed's files in the dq_file registry first seen more than sla_hours before the
window end. Violation: a file with no raw rows (after feed_filter), matched on the
file's base name. The not-loaded paths (up to 100) go in `detail`, in the dq store.
If the feed's landing roots could not be listed this run: DID_NOT_RUN with that reason.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import (
    Check, CheckContext, CheckResult, did_not_run, from_counts, render_sql, utc_literal,
)

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset

FILE_PATTERNS = ("FILE_CYCLIC", "FILE_PERIODIC")
LISTED = 100


class FilesNotLoaded(Check):
    check_id = "T1_FILES_NOT_LOADED"

    def applies_to(self, dataset: Dataset, pattern: str | None) -> bool:
        return pattern in FILE_PATTERNS and dataset.layer == "RAW" and dataset.file_name_column is not None

    def required_columns(self, dataset: Dataset) -> list[str]:
        return [dataset.file_name_column] if dataset.file_name_column else []

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        if ctx.landing_problem is not None:
            return [ctx.landing_problem]
        if ctx.feed is None or ctx.dq_database is None:
            return [did_not_run("invalid_config", detail="T1_FILES_NOT_LOADED needs a feed and the dq store")]
        ds = ctx.dataset
        params = {
            "dq_database": ctx.dq_database, "feed": ctx.feed.feed, "table": ds.table,
            "file_name_column": ds.file_name_column, "feed_filter": ds.feed_filter,
            "cutoff": utc_literal(event.window_end - timedelta(hours=ctx.settings.sla_hours)), "limit": LISTED,
        }
        row = ctx.spark.sql(render_sql("t1_files_not_loaded.sql.j2", summary=True, **params)).collect()[0]
        result = from_counts(
            row.population, row.violations,
            observed=f"{row.violations or 0} of {row.population} files first seen over "
                     f"{ctx.settings.sla_hours:g} hours ago have no raw rows",
            expected="every landed file has raw rows",
        )
        if result.state == "FAILED":
            paths = [r.path for r in ctx.spark.sql(render_sql("t1_files_not_loaded.sql.j2", summary=False, **params)).collect()]
            detail = json.dumps({"not_loaded": paths, "listed": len(paths), "total": row.violations})
            result = CheckResult(result.state, result.population, result.violations, result.observed,
                                 result.expected, group_values=result.group_values, detail=detail)
        return [result]
