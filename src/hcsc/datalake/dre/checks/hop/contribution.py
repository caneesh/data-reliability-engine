"""HOP_CONTRIBUTION: a contributor's reference resolves, and what it contributes agrees (build step 5;
docs/design/reconciliation-model.md section 6.2; ADR-03).

A join_contribution hop is a contributor: a row here is built from a primary upstream, which owns the
population (its HOP_LEDGER counts each row once), plus columns joined from the contributor through a
reference (the contract's `reference`: this dataset's column -> the contributor's key column). One
result per contributor hop (group `upstream`). The population is the rows here loaded in the window
(every row loaded before the window end on the full sweep). Each row's reference, with both datasets'
key_normalise applied, resolves to the contributor version current at the row's load time (its latest
by record time among the contributor's rows loaded at or before it). A row with no such version, or
whose reference is empty, is `unresolved` (a deficit; its cause is JOIN_UNMATCHED); one whose
contributed column (the contract's `contributes`: the contributor's column -> this dataset's column)
disagrees with that version is `divergent`. FAILED if any row is unresolved or divergent, PASSED
otherwise; nothing loaded in the window is DID_NOT_RUN / empty_population; a missing reference, load
time or record time is DID_NOT_RUN / invalid_config. It never counts toward completeness.

The lines (due, resolved, unresolved, divergent, and untimed beside them) go to dq_reconciliation as
evidence of the result, in HOP_LEDGER's shape, with the scope on every line. Counts only.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.base import (
    FAILED, PASSED, Check, CheckContext, CheckResult, did_not_run, error_detail, render_sql, utc_literal,
)
from hcsc.datalake.dre.checks.derived import source_of
from hcsc.datalake.dre.checks.hop.common import has_a_key, hop_label, per_hop
from hcsc.datalake.dre.checks.keys import column_expr, key_string
from hcsc.datalake.dre.checks.registry import register_check
from hcsc.datalake.dre.checks.times import utc_expr

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset, EdgeContract

CHECK_ID = "HOP_CONTRIBUTION"
CAUSE = "JOIN_UNMATCHED"  # model section 6.2: the cause of an unresolved reference
LINES = (("resolved", "ok"), ("unresolved", "deficit"), ("divergent", "deficit"))


def contributors(ds: Dataset) -> list[str]:
    """The upstreams this dataset declares as join_contribution contributors, in contract order."""
    return [u for u, c in ds.contracts.items() if c.mapping == "join_contribution"]


def contribution_params(ctx: CheckContext, event: Event, up_id: str, contract: EdgeContract,
                        up: Dataset | None) -> dict[str, Any] | CheckResult:
    """Template parameters for one contributor hop, or DID_NOT_RUN when it cannot be judged."""
    ds, tz, groups = ctx.dataset, ctx.default_timezone, {"upstream": up_id}
    if up is None:
        return did_not_run("invalid_config", detail=f"contributor {up_id} is not configured", group_values=groups)
    if not contract.reference:
        return did_not_run("invalid_config", detail=f"the join_contribution hop from {up_id} has no reference",
                           group_values=groups)
    missing = [k for k in up.key if k not in contract.reference.values()]
    if missing:
        return did_not_run("invalid_config", detail=f"reference does not cover {up_id}'s key column(s) {missing}",
                           group_values=groups)
    if ds.load_time is None:
        return did_not_run("invalid_config", detail=f"{ds.dataset} has no load_time to place its rows in the window",
                           group_values=groups)
    if up.load_time is None or up.record_time is None:
        return did_not_run("invalid_config", detail=f"contributor {up_id} needs record_time and load_time to find "
                                                    "the version current at a row's load time", group_values=groups)
    try:
        up_source = source_of(up, ctx.upstreams, tz)
    except (KeyError, ValueError) as exc:
        return did_not_run("invalid_config", detail=f"contributor {up_id} cannot be read: {type(exc).__name__}",
                           group_values=groups)
    if up.table is not None:
        try:
            exists = ctx.spark.catalog.tableExists(up.table)
        except Exception as exc:
            return did_not_run("metastore_unavailable", detail=error_detail(exc), group_values=groups)
        if not exists:
            return did_not_run("table_missing", detail=f"table {up.table} not found", group_values=groups)
    here = {v: h for h, v in contract.reference.items()}  # contributor key column -> this column
    dn_cols = [here[k] for k in up.key]
    return {
        "dn_table": ds.table, "dn_filter": f"({ds.feed_filter})" if ds.feed_filter else None,
        "dn_ref": key_string([column_expr(ds, c) for c in dn_cols]), "dn_referenced": has_a_key(dn_cols),
        "dn_load": utc_expr(ds.load_time, tz),
        "up_table": up_source, "up_filter": f"({up.feed_filter})" if up.feed_filter else None,
        "up_ref": key_string([column_expr(up, k) for k in up.key]), "up_keyed": has_a_key(list(up.key)),
        "up_record": utc_expr(up.record_time, tz), "up_load": utc_expr(up.load_time, tz),
        "contributed": [{"up": u, "dn": d} for u, d in contract.contributes.items()],
        "window_start": utc_literal(event.window_start), "window_end": utc_literal(event.window_end),
        "full_sweep": ctx.full_sweep,
    }


def scope_of(ctx: CheckContext, event: Event, up_id: str, contract: EdgeContract) -> dict[str, Any]:
    """What the result's claim covers (ADR-01): the contributor edge, its reference and the values it
    compares; never completeness, which the primary edge's ledger claims."""
    ds = ctx.dataset
    text = json.dumps({"contract": contract.model_dump(mode="json", by_alias=True),
                       "key_normalise": ds.key_normalise}, sort_keys=True, default=str)
    sweep = "loaded before the window end (full sweep)" if ctx.full_sweep else "loaded in the window"
    return {
        "edge": hop_label(up_id, ds.dataset), "mapping": "join_contribution", "contract": "declared",
        "contract_hash": hashlib.sha256(text.encode("utf-8")).hexdigest()[:16],
        "claim": "contributor resolution and value agreement (completeness is the primary edge's claim)",
        "qualifications": [],
        "population": f"rows here {sweep}, each judged against the contributor version current at its load time",
        "reference": dict(contract.reference),
        "values": {"verified": [{"contributor": u, "here": d} for u, d in contract.contributes.items()],
                   "coverage": "complete" if contract.contributes else "none"},
        "window": [event.window_start.isoformat(), event.window_end.isoformat()],
        "cutoff": event.window_end.isoformat(), "full_sweep": ctx.full_sweep,
        "evidence_level": "observed", "evidence_levels": {name: "observed" for name, _ in LINES},
        "blind_spots": ["completeness is not judged here: the primary edge's ledger counts each row once"]
                       + ([] if contract.contributes else ["no contributes: no contributed value is compared"]),
    }


def counted(rows: list[Any]) -> dict[str, Any]:
    """The summary rows as lines: due, untimed, and per line its labels."""
    lines: dict[str, dict[str | None, int]] = {name: {} for name, _ in LINES}
    totals = {"due": 0, "untimed": 0}
    for r in rows:
        if r.line in totals:
            totals[r.line] += r.n or 0
        else:
            lines[r.line][r.label] = lines[r.line].get(r.label, 0) + (r.n or 0)
    judged = sum(n for labels in lines.values() for n in labels.values())
    return {"lines": lines, "totals": totals, "balanced": judged == totals["due"]}


def reconciliation_lines(up_id: str, ledger: dict[str, Any], scope: str) -> tuple[dict[str, Any], ...]:
    """dq_reconciliation rows in HOP_LEDGER's shape: the population lines, then each line (zeros too)."""
    rows = [{"upstream": up_id, "line": name, "class": "population", "records": ledger["totals"][name],
             "evidence_level": "observed", "scope": scope} for name in ("due", "untimed")]
    for name, cls in LINES:
        labels = ledger["lines"][name] or {None: 0}
        for label, n in sorted(labels.items(), key=lambda kv: (kv[0] is None, kv[0] or "")):
            rows.append({"upstream": up_id, "line": name, "label": label, "class": cls, "records": n,
                         "evidence_level": "observed", "scope": scope})
    return tuple(rows)


class HopContribution(Check):
    check_id = CHECK_ID

    def applies_to(self, dataset: Dataset, pattern: str | None) -> bool:
        return bool(contributors(dataset))

    def required_columns(self, dataset: Dataset) -> list[str]:
        columns = [dataset.load_time.column] if dataset.load_time else []
        for up_id in contributors(dataset):
            c = dataset.contracts[up_id]
            columns += [*c.reference, *c.contributes.values()]
        return list(dict.fromkeys(columns))

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        return [per_hop(up_id, lambda up_id=up_id: self._hop(ctx, event, up_id)) for up_id in contributors(ctx.dataset)]

    @staticmethod
    def _hop(ctx: CheckContext, event: Event, up_id: str) -> CheckResult:
        contract = ctx.dataset.contracts[up_id]
        groups = {"upstream": up_id}
        params = contribution_params(ctx, event, up_id, contract, ctx.upstreams.get(up_id))
        if isinstance(params, CheckResult):
            return params
        ledger = counted(ctx.spark.sql(render_sql("hop_contribution.sql.j2", p=params)).collect())
        scope = scope_of(ctx, event, up_id, contract)
        unresolved = sum(ledger["lines"]["unresolved"].values())
        divergent = sum(ledger["lines"]["divergent"].values())
        if unresolved:
            scope["cause"] = CAUSE
        scope_json = json.dumps(scope, sort_keys=True)
        lines = reconciliation_lines(up_id, ledger, scope_json)
        due, untimed = ledger["totals"]["due"], ledger["totals"]["untimed"]
        labels = ", ".join(f"{label} {n}" for label, n in sorted(ledger["lines"]["unresolved"].items(),
                                                                key=lambda kv: kv[0] or "") if n and label)
        observed = (f"due {due}" + (f" (untimed {untimed})" if untimed else "")
                    + f": resolved {sum(ledger['lines']['resolved'].values())}, unresolved {unresolved}"
                    + (f" (cause {CAUSE}: {labels})" if unresolved else "") + f", divergent {divergent}")
        if not ledger["balanced"]:
            result = did_not_run("ledger_unbalanced", detail=scope_json, group_values=groups)
            return replace(result, observed=observed, reconciliation=lines)
        if not due:
            result = did_not_run("empty_population", detail=scope_json, group_values=groups)
            return replace(result, observed=observed, reconciliation=lines)
        violations = unresolved + divergent
        return CheckResult(FAILED if violations else PASSED, population=due, violations=violations,
                           observed=observed,
                           expected="every row's reference resolves to the contributor version current at its load "
                                    "time, and every contributed column agrees with it",
                           group_values=groups, detail=scope_json, reconciliation=lines)


register_check(HopContribution())
