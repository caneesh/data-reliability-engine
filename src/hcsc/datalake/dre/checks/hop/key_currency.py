"""HOP_KEY_CURRENCY: each upstream key reaches this dataset at its latest version (spec section 6).

For each upstream key judged in the window, the latest upstream record time (rows tied at
it kept) against this dataset's latest record time for the mapped key: MISSING (no row
here), STALE (older here) or CURRENT. Population: keys judged; violations: MISSING or
STALE. One result per upstream (group `upstream`). Key events (FLAGGED, STILL_FLAGGED,
CLEARED) go to dq_key_event, per hop, when an HMAC secret is configured, in NORMAL runs only:
a RERUN or REPLAY of a historical window judges the keys that window owns (at their deadline,
or reloaded in it) without reading or changing the open-key state.

With a secret, the judged keys are computed once and materialised (a local checkpoint) inside
the check, so the summary, the key events and the cause engine's failing keys all read the same
judgement, and all of the hop's Spark work runs within the check's compute budget.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, from_counts, render_sql
from hcsc.datalake.dre.checks.hop.common import hop_params, per_hop, register_key_hash, upstream_pairs

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset


def judged_view(dataset_id: str, up_id: str) -> str:
    """The session temp view holding one hop's judged keys (identifiers are validated config)."""
    return f"dre_judged__{dataset_id}__{up_id}"


class KeyCurrency(Check):
    check_id = "HOP_KEY_CURRENCY"

    def applies_to(self, dataset: Dataset, pattern: str | None) -> bool:
        return bool(dataset.key_map)

    def required_columns(self, dataset: Dataset) -> list[str]:
        return [*dataset.key, *([dataset.record_time.column] if dataset.record_time else [])]

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        if ctx.key_secret is not None:
            register_key_hash(ctx.spark, ctx.key_secret)
        return [per_hop(up_id, lambda up_id=up_id, up=up: self._hop(ctx, event, up_id, up))
                for up_id, up in upstream_pairs(ctx)]

    @staticmethod
    def _hop(ctx: CheckContext, event: Event, up_id: str, up: Dataset | None) -> CheckResult:
        params = hop_params(ctx, event, up_id, up)
        if isinstance(params, CheckResult):
            return params
        view = None
        if params["hashed"]:
            view = judged_view(ctx.dataset.dataset, up_id)
            judged = ctx.spark.sql(render_sql("hop_key_currency.sql.j2", p=params, mode="judged", source=None))
            judged.localCheckpoint().createOrReplaceTempView(view)
        row = ctx.spark.sql(render_sql("hop_key_currency.sql.j2", p=params, mode="summary", source=view)).collect()[0]
        result = from_counts(
            row.population, row.violations,
            observed=f"{row.missing or 0} missing and {row.stale or 0} stale of {row.population} keys",
            expected="every upstream key present at its latest version",
            group_values={"upstream": up_id},
        )
        if view is not None and not ctx.record_key_events:  # RERUN, REPLAY: the judgement serves causes only
            return replace(result, judged_view=view)
        if view is not None:
            events = ctx.spark.sql(render_sql("hop_key_currency.sql.j2", p=params, mode="events", source=view))
            return replace(result, key_events=events.localCheckpoint(), judged_view=view)
        if result.state == "FAILED":
            return replace(result, detail="key events not recorded: hmac_secret_file is not set")
        return result
