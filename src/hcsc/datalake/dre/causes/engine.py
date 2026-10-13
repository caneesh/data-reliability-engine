"""The cause engine (spec section 7).

When a check FAILS, every cause check its pattern lists for that failure type runs, in order,
for each failing thing (causes/failures.py), and one dq_cause_result row is written per cause
check. The cause is the first CONFIRMED; with none, the pattern's fallback, "not proven".
A cause check whose parameters are missing or null is NOT_READY, never RULED_OUT; one that
raises is ERROR, and never stops the run. Probe types come from the probe registry
(config/probes.py) and builtin causes from the builtin registry (causes/base.py).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.causes.base import (
    BUILTINS, CONFIRMED, ERROR, NOT_READY, RULED_OUT, CauseContext, Failure, Outcome, error, not_ready,
)
from hcsc.datalake.dre.causes.failures import failures_for
from hcsc.datalake.dre.checks.hop.common import hop_label as hop_label_of
from hcsc.datalake.dre.checks.patterns import CauseEntry, FailureType, pattern_causes
from hcsc.datalake.dre.config.probes import BUILTIN, PROBES, is_ready, parameter_sets

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.base import CheckResult

NO_SECRET = "hmac_secret_file is not set, so failing keys cannot be referenced"
NOTHING_TO_EXPLAIN = "no failing item carries the evidence a cause check needs (for example only UNASSESSABLE keys)"


def failure_type(pattern: str | None, check_id: str) -> FailureType | None:
    if pattern is None:
        return None
    return next((ft for ft in pattern_causes(pattern).values() if check_id in ft.checks), None)


def cause_check_id(entry: CauseEntry) -> str:
    return f"{entry.probe}:{entry.params_key}" if entry.params_key else str(entry.probe)


def run_entry(cx: CauseContext, entry: CauseEntry, failure: Failure) -> Outcome:
    """One cause check for one failure. Never raises."""
    try:
        if failure.ref is None and failure.key is None and cx.check_id in ("HOP_KEY_CURRENCY", "HOP_VALUE_AGREEMENT"):
            return not_ready(NO_SECRET)
        if entry.probe == BUILTIN:
            return BUILTINS[entry.code](cx, failure)
        if entry.params_key:
            raw = (cx.feed.probes if cx.feed else {}).get(entry.params_key)
            sets = parameter_sets(raw)
        else:
            sets = [{}]
        probe = PROBES[entry.probe]
        outcomes = []
        unready = 0
        for raw_set in sets:
            params = probe.params_model.model_validate(raw_set or {})
            if is_ready(params):
                outcomes.append(probe.run(cx, params, failure))
            else:
                unready += 1
        if not outcomes:
            return not_ready(f"probes.{entry.params_key} is not configured")
        for state in (CONFIRMED, ERROR):
            hit = next((o for o in outcomes if o.state == state), None)
            if hit is not None:
                return hit
        if unready:  # a set never checked could still confirm the cause: never RULED_OUT
            return not_ready(f"{unready} of {len(sets)} parameter sets of probes.{entry.params_key} are not configured")
        return outcomes[0] if len(outcomes) == 1 else Outcome(RULED_OUT, {"parameter_sets": len(outcomes)})
    except Exception as exc:
        return error(exc)


def hop_label(cx: CauseContext, ft: FailureType) -> str:
    ds = cx.dataset.dataset
    if cx.upstream_id:
        return hop_label_of(cx.upstream_id, ds)
    if ft.name == "file_not_loaded":
        raw = cx.raw_dataset()
        return f"landing->{raw.dataset if raw else ds}"
    return f"{','.join(cx.dataset.upstream)}->{ds}" if cx.dataset.upstream else ds


def explain(cx: CauseContext, result: CheckResult, evaluation_id: str, run_id: str, run_date: date,
            now: datetime) -> list[dict[str, Any]]:
    """dq_cause_result rows for a FAILED result (none for other states or checks without causes)."""
    pattern = cx.feed.pattern if cx.feed else None
    ft = failure_type(pattern, cx.check_id)
    if ft is None or result.state != "FAILED":
        return []
    rows = []
    label = hop_label(cx, ft)
    listing_error: Outcome | None = None
    try:
        failures = failures_for(cx, result)
    except Exception as exc:  # the failing things could not be listed: every cause check is ERROR
        failures, listing_error = [Failure(ref=None)], error(exc)
    if not failures:  # nothing explainable (say, only UNASSESSABLE keys): recorded, never left silent
        failures, listing_error = [Failure(ref=None)], not_ready(NOTHING_TO_EXPLAIN)
    for failure in failures:
        for order_no, entry in enumerate(ft.order, 1):
            if entry.fallback:
                continue
            outcome = listing_error or run_entry(cx, entry, failure)
            rows.append({
                "run_id": run_id, "evaluation_id": evaluation_id, "failure_ref": failure.ref, "hop": label,
                "cause_check_id": cause_check_id(entry), "order_no": order_no, "state": outcome.state,
                "cause_code": entry.code, "evidence": json.dumps(outcome.evidence, default=str, sort_keys=True),
                "evaluated_at": now, "run_date": run_date,
            })
    return rows


@dataclass(frozen=True)
class CauseLine:
    """The resolved cause of one failure: the first CONFIRMED, or the fallback, not proven."""

    code: str
    proven: bool
    ruled_out: tuple[str, ...] = ()
    not_ready: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()

    @property
    def evidence(self) -> str:
        """How far the evidence goes: `confirmed` (a cause check proved it); `suspected` (every cause
        check ran and was ruled out, so the fallback stands by elimination); `insufficient evidence`
        (some cause check could not be evaluated, so the fallback is not even established)."""
        if self.proven:
            return "confirmed"
        return "insufficient evidence" if self.not_ready or self.errors or not self.ruled_out else "suspected"

    def text(self) -> str:
        """The spec's cause line (section 8): `cause: FILE_SKIPPED (confirmed)`, or `cause not proven
        (DROPPED, suspected): ruled out NOT_RUN, INVALID_KEY`, or `cause not proven (DROPPED,
        insufficient evidence): ruled out NOT_RUN; not ready FILTERED`, naming the fallback."""
        if self.proven:
            return f"cause: {self.code} (confirmed)"
        parts = [f"{label} {', '.join(codes)}" for label, codes in
                 (("ruled out", self.ruled_out), ("not ready", self.not_ready), ("error", self.errors)) if codes]
        return f"cause not proven ({self.code}, {self.evidence})" + (": " + "; ".join(parts) if parts else "")


def resolve(rows: list[dict[str, Any]], ft: FailureType) -> CauseLine:
    """The cause line for one failure's rows (same failure_ref)."""
    ordered = sorted(rows, key=lambda r: r["order_no"])
    hit = next((r for r in ordered if r["state"] == CONFIRMED), None)
    if hit is not None:
        return CauseLine(hit["cause_code"], True)
    fallback = next(e.code for e in ft.order if e.fallback)
    pick = lambda state: tuple(r["cause_code"] for r in ordered if r["state"] == state)  # noqa: E731
    return CauseLine(fallback, False, pick(RULED_OUT), pick(NOT_READY), pick(ERROR))


def summarise(rows: list[dict[str, Any]], ft: FailureType) -> dict[str, CauseLine]:
    """failure_ref -> its cause line."""
    by_ref: dict[Any, list[dict[str, Any]]] = {}
    for row in rows:
        by_ref.setdefault(row["failure_ref"], []).append(row)
    return {ref: resolve(group, ft) for ref, group in by_ref.items()}
