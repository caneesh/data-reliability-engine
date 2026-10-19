"""HOP_VALUE_AGREEMENT: CURRENT keys agree with upstream on owned columns (spec section 6).

For keys judged CURRENT, the key's latest version must be the same on both sides. The
comparison grain is the set of distinct owned-column values (upstream column -> this column)
among the rows tied at each side's latest record time: equal sets agree whatever the number
of tied rows, so identical multi-row versions pass, while tied upstream versions that this
dataset kept only one of (R06) still show up.
Population: CURRENT keys judged; violations: keys with any mismatch.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, from_counts, render_sql
from hcsc.datalake.dre.checks.hop.common import hop_params, per_hop, upstream_pairs
from hcsc.datalake.dre.checks.registry import register_check

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
        return [per_hop(up_id, lambda up_id=up_id, up=up: self._hop(ctx, event, up_id, up))
                for up_id, up in upstream_pairs(ctx)]

    @staticmethod
    def _hop(ctx: CheckContext, event: Event, up_id: str, up: Dataset | None) -> CheckResult:
        params = hop_params(ctx, event, up_id, up, "HOP_VALUE_AGREEMENT")
        if isinstance(params, CheckResult):
            return params
        # open keys belong to HOP_KEY_CURRENCY; agreement judges by deadline (and reloads) only
        params["hashed"] = params["use_open"] = False
        row = ctx.spark.sql(render_sql("hop_value_agreement.sql.j2", p=params)).collect()[0]
        columns = ", ".join(f"{u}->{d}" for u, d in ctx.dataset.owned_columns.items())
        result = from_counts(
            row.population, row.violations,
            observed=f"{row.violations or 0} of {row.population} current keys disagree on {columns}",
            expected="owned columns equal to every upstream row at the latest version",
            group_values={"upstream": up_id},
        )
        return replace(result, coverage=params["coverage"])


register_check(ValueAgreement())
