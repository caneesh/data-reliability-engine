"""HOP_FILE_COMPLETENESS: every upstream file's rows arrived here (spec section 6).

Per upstream file (base name of file_name_column) judged at its deadline (first upstream
load + sla_hours in the window): FAILED when it has fewer rows here than upstream, each
side after its feed_filter. Needs file_name_column on both datasets. The short files (up to
100) go in `detail`, in the dq store.
"""

from __future__ import annotations

import json
from dataclasses import replace
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, did_not_run, from_counts, render_sql
from hcsc.datalake.dre.checks.hop.common import hop_params, not_judged, per_hop, upstream_pairs

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset

LISTED = 100


class FileCompleteness(Check):
    check_id = "HOP_FILE_COMPLETENESS"

    def applies_to(self, dataset: Dataset, pattern: str | None) -> bool:
        return bool(dataset.key_map) and dataset.file_name_column is not None

    def required_columns(self, dataset: Dataset) -> list[str]:
        return [dataset.file_name_column] if dataset.file_name_column else []

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        return [per_hop(up_id, lambda up_id=up_id, up=up: self._hop(ctx, event, up_id, up))
                for up_id, up in upstream_pairs(ctx)]

    @staticmethod
    def _hop(ctx: CheckContext, event: Event, up_id: str, up: Dataset | None) -> CheckResult:
        groups = {"upstream": up_id}
        if up is not None and up.file_name_column is None:
            return did_not_run("invalid_config", detail=f"upstream {up_id} has no file_name_column",
                               group_values=groups)
        params = hop_params(ctx, event, up_id, up)
        if isinstance(params, CheckResult):
            return params
        row = ctx.spark.sql(render_sql("hop_file_completeness.sql.j2", p=params, limit=LISTED)).collect()[0]
        left_out = not_judged(row.pending, row.excluded, "file", excluded_unit="row")
        result = from_counts(
            row.population, row.violations,
            observed=f"{row.violations or 0} of {row.population} upstream files have fewer rows here"
                     + (f"; {left_out}" if left_out else ""),
            expected="every upstream row of each file present here",
            group_values=groups,
        )
        if result.state == "DID_NOT_RUN" and left_out:
            return replace(result, detail=f"no file judged; {left_out}")
        if result.state == "FAILED":
            short = [{"file": r.f, "upstream_rows": r.up_rows, "rows_here": r.dn_rows} for r in row.short or []]
            result = replace(result, detail=json.dumps({"short_files": short}))
        return result
