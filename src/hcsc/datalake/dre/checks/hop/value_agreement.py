"""HOP_VALUE_AGREEMENT: CURRENT keys agree with upstream on owned columns (spec section 6).

For keys judged CURRENT, every upstream row at the latest record time (ties kept) must
match this dataset's latest row on each owned column (upstream column -> this column),
null-safe. Tied upstream rows that disagree with each other therefore always show up.
Population: CURRENT keys judged; violations: keys with any mismatch.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, from_counts, render_sql
from hcsc.datalake.dre.checks.hop.common import hop_params, upstream_pairs

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset


class ValueAgreement(Check):
    check_id = "HOP_VALUE_AGREEMENT"

    def applies_to(self, dataset: Dataset, pattern: str | None) -> bool:
        return bool(dataset.key_map) and bool(dataset.owned_columns)

    def required_columns(self, dataset: Dataset) -> list[str]:
        record = [dataset.record_time.column] if dataset.record_time else []
        return [*dataset.key, *record, *dataset.owned_columns.values()]

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        results = []
        for up_id, up in upstream_pairs(ctx):
            params = hop_params(ctx, event, up_id, up)
            if isinstance(params, CheckResult):
                results.append(params)
                continue
            params["hashed"] = False  # open keys belong to HOP_KEY_CURRENCY; agreement judges by deadline only
            row = ctx.spark.sql(render_sql("hop_value_agreement.sql.j2", p=params)).collect()[0]
            columns = ", ".join(f"{u}->{d}" for u, d in ctx.dataset.owned_columns.items())
            results.append(from_counts(
                row.population, row.violations,
                observed=f"{row.violations or 0} of {row.population} current keys disagree on {columns}",
                expected="owned columns equal to every upstream row at the latest version",
                group_values={"upstream": up_id},
            ))
        return results
