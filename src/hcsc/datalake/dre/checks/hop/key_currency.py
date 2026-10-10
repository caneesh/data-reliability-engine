"""HOP_KEY_CURRENCY: each upstream key reaches this dataset at its latest version (spec section 6).

For each upstream key judged in the window, the latest upstream record time (rows tied at
it kept) against this dataset's latest record time for the mapped key: MISSING (no row
here), STALE (older here) or CURRENT. Population: keys judged; violations: MISSING or
STALE. One result per upstream (group `upstream`). Key events (FLAGGED, STILL_FLAGGED,
CLEARED) go to dq_key_event when an HMAC secret is configured.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, from_counts, render_sql
from hcsc.datalake.dre.checks.hop.common import hop_params, register_key_hash, upstream_pairs

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset


class KeyCurrency(Check):
    check_id = "HOP_KEY_CURRENCY"

    def applies_to(self, dataset: Dataset, pattern: str | None) -> bool:
        return bool(dataset.key_map)

    def required_columns(self, dataset: Dataset) -> list[str]:
        return [*dataset.key, *([dataset.record_time.column] if dataset.record_time else [])]

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        if ctx.key_secret is not None:
            register_key_hash(ctx.spark, ctx.key_secret)
        results = []
        for up_id, up in upstream_pairs(ctx):
            params = hop_params(ctx, event, up_id, up)
            if isinstance(params, CheckResult):
                results.append(params)
                continue
            row = ctx.spark.sql(render_sql("hop_key_currency.sql.j2", p=params, mode="summary")).collect()[0]
            result = from_counts(
                row.population, row.violations,
                observed=f"{row.missing or 0} missing and {row.stale or 0} stale of {row.population} keys",
                expected="every upstream key present at its latest version",
                group_values={"upstream": up_id},
            )
            if params["hashed"]:
                events = ctx.spark.sql(render_sql("hop_key_currency.sql.j2", p=params, mode="events"))
                result = replace(result, key_events=events)
            elif result.state == "FAILED":
                result = replace(result, detail="key events not recorded: hmac_secret_file is not set")
            results.append(result)
        return results
