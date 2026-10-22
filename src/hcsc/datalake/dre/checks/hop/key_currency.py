"""HOP_KEY_CURRENCY: each upstream key reaches this dataset at its latest version (spec section 6).

For each upstream key judged in the window, the latest upstream record time (rows tied at
it kept) against this dataset's latest record time for the mapped key: MISSING (no row
here), UNASSESSABLE (no usable record time on a side, so currency cannot be judged: a
violation, never passed, and given no cause), STALE (older here) or CURRENT. Population:
keys judged; violations: MISSING, STALE or UNASSESSABLE. One result per upstream (group
`upstream`). Keys not judged are counted in `observed` with the reason: excluded by
hop_exclusions, or pending (loaded upstream within sla_hours). Key events (FLAGGED,
STILL_FLAGGED, CLEARED) go to dq_key_event, per hop, when an HMAC secret is configured, in
NORMAL runs only: a RERUN or REPLAY of a historical window judges the keys that window owns (at their deadline,
or reloaded in it) without reading or changing the open-key state.

With a secret, the judged keys are computed once and materialised (a local checkpoint) inside
the check, so the summary, the key events and the cause engine's failing keys all read the same
judgement, and all of the hop's Spark work runs within the check's compute budget.

Key encodings (Gate A remediation, release N): every event records this engine's key encoding.
The open keys read are those of this encoding, REKEYED taken as no event. An identity of another
encoding open at the hop (a later engine's, after a rollback) is never read as one of this engine's
keys: the hop is judged as on the full sweep, and the identity is closed with REKEYED in its own
encoding (spec section 6, "Key events").
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from hcsc.datalake.dre.checks.base import Check, CheckContext, CheckResult, from_counts, render_sql
from hcsc.datalake.dre.checks.hop.common import (
    exclusion_label, foreign_open, foreign_params, hop_params, not_judged, per_hop, register_key_hash, rekeyed_back,
    upstream_pairs,
)
from hcsc.datalake.dre.checks.hop.coverage import may_have_dropped
from hcsc.datalake.dre.checks.keys import KEY_ENCODING
from hcsc.datalake.dre.checks.registry import register_check

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
        params = hop_params(ctx, event, up_id, up, "HOP_KEY_CURRENCY")
        if isinstance(params, CheckResult):
            return params
        if params["use_open"]:  # identities of another key encoding open here: a rollback from a later engine
            f = foreign_params(ctx, ctx.dataset.dataset, "HOP_KEY_CURRENCY", params["hop"], params["events_since"],
                               ("FLAGGED", "STILL_FLAGGED"))
            n = foreign_open(ctx, f)
            if n:  # judged as on the full sweep, so no discrepancy they stood for is lost (spec section 6)
                params = {**params, "full_sweep": True, "foreign": f, "foreign_open": n}
        return replace(KeyCurrency._judge(ctx, up_id, up, params), coverage=params["coverage"],
                       qualifications=params["qualifications"])

    @staticmethod
    def _judge(ctx: CheckContext, up_id: str, up: Dataset | None, params: dict) -> CheckResult:
        from pyspark.sql import functions as F

        view = None
        if params["hashed"]:
            view = judged_view(ctx.dataset.dataset, up_id)
            judged = ctx.spark.sql(render_sql("hop_key_currency.sql.j2", p=params, mode="judged", source=None))
            judged = judged.localCheckpoint()
            judged.createOrReplaceTempView(view)
        row = ctx.spark.sql(render_sql("hop_key_currency.sql.j2", p=params, mode="summary", source=view)).collect()[0]
        left_out = not_judged(row.pending, row.excluded, "key", excluded_by=exclusion_label(ctx.dataset, up_id, up))
        if row.open_keys:  # how much of the judgement is open keys from earlier runs: growth shows here
            still_open = f"{row.open_keys} key{'s' if row.open_keys != 1 else ''} open from earlier runs"
            left_out = f"{still_open}, {left_out}" if left_out else still_open
        if params["use_open"] and params["events_from_retention"] and may_have_dropped(
                ctx, "dq_key_event", ctx.key_events_since):  # KI-19: what the hop can no longer see
            horizon = (f"open keys are read back to {params['events_since']}, dq_key_event's retention: a key "
                       "opened before then is judged again only by the full sweep")
            left_out = f"{left_out}; {horizon}" if left_out else horizon
        if params.get("foreign_open"):  # say why every key was judged, and what this evaluation closes
            n = params["foreign_open"]
            foreign = (f"{n} key{'s' if n != 1 else ''} of another key encoding open at this hop (written by a "
                       "later engine): judged as on the full sweep, and closed with REKEYED")
            left_out = f"{left_out}; {foreign}" if left_out else foreign
        observed = (f"{row.missing or 0} missing, {row.stale or 0} stale and {row.unassessable} unassessable "
                    f"(no record time) of {row.population} keys" if row.unassessable else
                    f"{row.missing or 0} missing and {row.stale or 0} stale of {row.population} keys")
        result = from_counts(
            row.population, row.violations,
            observed=observed + (f"; {left_out}" if left_out else ""),
            expected="every upstream key present at its latest version",
            group_values={"upstream": up_id},
        )
        if result.state == "DID_NOT_RUN" and left_out:  # nothing judged: say what was left out, and why
            result = replace(result, detail=f"no key judged; {left_out}")
        if view is not None and not ctx.record_key_events:  # RERUN, REPLAY: the judgement serves causes only
            return replace(result, judged_view=view, checkpoints=(judged,))
        if view is not None:
            events = ctx.spark.sql(render_sql("hop_key_currency.sql.j2", p=params, mode="events", source=view)
                                   ).withColumn("key_encoding", F.lit(KEY_ENCODING))
            if params.get("foreign_open"):  # one event per identity: an identity is its encoding and its hash
                events = events.unionByName(rekeyed_back(ctx, params["foreign"]))
            events = events.localCheckpoint()
            return replace(result, key_events=events, judged_view=view, checkpoints=(judged, events))
        if result.state == "FAILED":
            return replace(result, detail="key events not recorded: hmac_secret_file is not set")
        return result


register_check(KeyCurrency())
