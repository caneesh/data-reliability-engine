"""T1_KEY_DUPLICATES: keys with more than one row, after key_normalise (spec section 6)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, from_counts, group_values, render_sql
from hcsc.datalake.dre.checks.keys import key_exprs

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset


class KeyDuplicates(Check):
    check_id = "T1_KEY_DUPLICATES"

    def applies_to(self, dataset: Dataset, pattern: str | None) -> bool:
        return dataset.key_unique

    def required_columns(self, dataset: Dataset) -> list[str]:
        return [*dataset.key, *dataset.group_by]

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        ds = ctx.dataset
        sql = render_sql(
            "t1_key_duplicates.sql.j2",
            table=ds.table,
            key_expr=", ".join(key_exprs(ds)),
            group_by=ds.group_by,
            feed_filter=ds.feed_filter,
        )
        return [
            from_counts(
                row.population,
                row.violations,
                observed=f"{row.violations or 0} of {row.population} keys have more than one row",
                expected="0 keys with more than one row",
                group_values=group_values(row, ds.group_by),
            )
            for row in ctx.spark.sql(sql).collect()
        ]
