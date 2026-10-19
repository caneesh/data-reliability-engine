"""HOP_LEDGER: every due upstream record of a hop in exactly one disposition (Phase 1 build step 3;
docs/design/reconciliation-model.md sections 1 to 5; ADR-01, ADR-02, ADR-04, ADR-06).

One result per hop (group `upstream`). The due population is each upstream key whose version under
judgement (its latest by record time, among rows loaded before the window end) has its deadline
(that version's load time + sla_hours) in the window. Each due record lands in the first matching
disposition, in the model's precedence: unassessable, deleted, deleted_not_applied,
excluded[rule@version], excluded_but_present, rejected[reason], superseded, delivered, divergent,
stale, missing. The engine asserts the identity (every due record in exactly one bucket); when it
does not hold, the result is DID_NOT_RUN / ledger_unbalanced, an engine defect, never PASSED or
FAILED. Pending and untimed records, orphans (rows here no upstream row maps to) and overlap
diagnostics are reported beside the ledger.

FAILED when any record is unassessable, in a breach (deleted_not_applied, excluded_but_present,
orphans) or a defect reject reason; when a deficit (missing, stale, divergent) exceeds its
tolerance; or when an exclusion or an authorised reject reason exceeds its max_share of the due
records. PASSED otherwise. Only hops of kind identity, dedup, merge and union_partition are judged;
the others are DID_NOT_RUN / unsupported_mapping (an inferred aggregate included: aggregation is never
reconciled from an inferred kind, D6 condition 3).

Every result carries its scope (in `detail`, and on every dq_reconciliation row): the edge, the kind,
whether the contract is declared or inferred and its hash, the claim (contract-proven only under a
declared contract, D6 conditions 1 and 2), the population rule, the cutoff, the grain, the evidence
level of each disposition and the claim ceiling, and the blind spots (D6 condition 4). The ledger's
lines go to dq_reconciliation as evidence of the result (ADR-07). Counts, codes and labels only.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.base import (
    FAILED, PASSED, Check, CheckContext, CheckResult, did_not_run, error_detail, render_sql, utc_literal,
)
from hcsc.datalake.dre.checks.derived import source_of
from hcsc.datalake.dre.checks.hop.common import exclusion_terms, has_a_key, hop_label, per_hop
from hcsc.datalake.dre.checks.hop.coverage import coverage_of, judged_range
from hcsc.datalake.dre.checks.keys import column_expr, key_string
from hcsc.datalake.dre.checks.registry import register_check
from hcsc.datalake.dre.checks.times import utc_expr

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.contracts import Hop
    from hcsc.datalake.dre.config.models import Dataset

CHECK_ID = "HOP_LEDGER"
# The dispositions in precedence (model section 3), with class and evidence level (ADR-06).
DISPOSITIONS = (
    ("unassessable", "unknown", "observed"),
    ("deleted", "authorised", "reconstructed"),
    ("deleted_not_applied", "breach", "reconstructed"),
    ("excluded", "authorised", "reconstructed"),
    ("excluded_but_present", "breach", "reconstructed"),
    ("rejected", "authorised", "reported"),  # or defect, by the reject policy
    ("superseded", "authorised", "observed"),  # survivorship only: build step 5
    ("delivered", "ok", "observed"),
    ("divergent", "deficit", "observed"),
    ("stale", "deficit", "observed"),
    ("missing", "deficit", "observed"),
)
CLASS = {d: c for d, c, _ in DISPOSITIONS}
LEVEL = {d: lv for d, _, lv in DISPOSITIONS}
LEVELS = ("observed", "reconstructed", "reported", "inferred", "unavailable")  # strongest first
TOLERATED = ("missing", "stale", "divergent")


def ledger_view(dataset_id: str, up_id: str) -> str:
    """The session temp view holding one hop's assessed records (identifiers are validated config)."""
    return f"dre_ledger__{dataset_id}__{up_id}"


def hop_of(ctx: CheckContext, up_id: str) -> Hop:
    from hcsc.datalake.dre.config.contracts import hop_contract

    return hop_contract(dict(ctx.upstreams), ctx.dataset, up_id)


def contract_hash(hop: Hop, ds: Dataset) -> str:
    """A short, stable hash of everything that decides the hop's ledger, so a result names the exact
    contract it was judged under."""
    text = json.dumps({"contract": hop.contract.model_dump(mode="json", by_alias=True),
                       "declared": hop.declared, "legacy_exclusion": hop.legacy_exclusion,
                       "key_map": ds.key_map.get(hop.upstream), "key_normalise": ds.key_normalise,
                       "owned_columns": ds.owned_columns, "feed_filter": ds.feed_filter}, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def orphans_unchecked(ctx: CheckContext, hop: Hop) -> str | None:
    """Why orphans cannot be counted on this hop, or None when they can. An orphan is a row here
    loaded in the window that no row of this upstream maps to; that means something only when every
    row read here belongs to this hop: this dataset has one upstream, and a union_partition target
    is narrowed to its source by its feed_filter (the attribution predicate, decision D5)."""
    ds = ctx.dataset
    if ds.load_time is None:
        return f"{ds.dataset} has no load_time"
    if len({*ds.key_map, *ds.contracts}) > 1:
        return f"{ds.dataset} has several upstreams, and its rows are not attributed to each"
    if hop.contract.mapping == "union_partition" and ds.feed_filter is None:
        return f"union_partition without a feed_filter on {ds.dataset} to attribute its rows (decision D5)"
    return None


# What a claim can rest on unproven (checkpoint 3, C3-03, C3-04): each makes a PASS qualified, so it is
# stored on the result (dq_check_result.qualifications) and never counted as healthy (v_latest_result).
QUALIFICATIONS = {
    "inferred_contract": "the hop's kind is inferred from key_map, not declared",
    "legacy_exclusion": "an exclusion comes from hop_exclusions: no id, version, approval or volume guard",
    "attribution_unverified": "the rows here are not proven to belong to this source (T1_SOURCE_ATTRIBUTION, "
                              "build step 5)",
    "orphans_unchecked": "rows here that no upstream row maps to were not counted",
    "evidence_reported": "the claim rests on evidence the pipeline reported (its rejects)",
}


def attribution_of(ctx: CheckContext, hop: Hop) -> str:
    """Which rows here belong to this hop (C3-02): single_source (this dataset has one upstream),
    shared_target_unattributed (several upstreams, no attribution: completeness is judged per hop,
    orphans cannot be), feed_filter_unverified (a union_partition read through its feed_filter, decision
    D5, whose disjointness and completeness are proven from build step 5), unknown (a union_partition
    without one). `verified` comes with T1_SOURCE_ATTRIBUTION (build step 5)."""
    ds = ctx.dataset
    if hop.contract.mapping == "union_partition":
        return "feed_filter_unverified" if ds.feed_filter else "unknown"
    if len({*ds.key_map, *ds.contracts}) > 1:
        return "shared_target_unattributed"
    return "single_source"


def qualifications_of(ctx: CheckContext, hop: Hop, level: str | None) -> tuple[str, ...]:
    found = []
    if not hop.declared:
        found.append("inferred_contract")
    if hop.legacy_exclusion is not None:
        found.append("legacy_exclusion")
    if attribution_of(ctx, hop) in ("feed_filter_unverified", "unknown"):
        found.append("attribution_unverified")
    if orphans_unchecked(ctx, hop) is not None:
        found.append("orphans_unchecked")
    if level is not None and LEVELS.index(level) > LEVELS.index("reconstructed"):
        found.append("evidence_reported")
    return tuple(found)


def claim_of(hop: Hop, qualifications: tuple[str, ...]) -> str:
    """contract-proven only under a declared contract with nothing unproven (D6; C3-02)."""
    if not hop.declared:
        return "inferred contract"
    return "contract-proven" if not qualifications else f"declared contract, qualified: {', '.join(qualifications)}"


def value_coverage(ctx: CheckContext) -> dict[str, Any]:
    """Which values the claim verifies (C3-03): the owned_columns (identity transforms, compared as
    sets at the version), and every other column of this dataset that is not its key, a time, the
    partition or the file name: unverified, named, never implied checked. A completeness-only claim
    does not need them; it discloses them."""
    ds = ctx.dataset
    verified = [{"upstream": u, "here": d} for u, d in ds.owned_columns.items()]
    structural = {c.lower() for c in (*ds.key, *(t.column for t in (ds.record_time, ds.load_time) if t),
                                      ds.partition_column, ds.file_name_column) if c}
    owned = {d.lower() for d in ds.owned_columns.values()}
    try:
        columns = ctx.spark.table(ds.table).schema.fieldNames() if ds.table else []
    except Exception:  # the table is checked elsewhere; here only the disclosure is at stake
        return {"verified": verified, "unverified": None, "coverage": "unknown"}
    unverified = sorted(c for c in columns if c.lower() not in structural | owned)
    coverage = "complete" if not unverified else ("partial" if verified else "none")
    return {"verified": verified, "unverified": unverified, "coverage": coverage}


def scope_of(ctx: CheckContext, event: Event, hop: Hop, up: Dataset | None, level: str | None,
             note: str | None = None, judged: tuple[Any, Any] | None = None) -> dict[str, Any]:
    """The result's scope (ADR-01): what its claim covers, and what it does not."""
    from hcsc.datalake.dre.config.contracts import blind_spots

    ds = ctx.dataset
    datasets = {ds.dataset: ds, **({hop.upstream: up} if up is not None else {})}
    spots = blind_spots(SimpleNamespace(datasets=datasets), hop)
    unchecked = orphans_unchecked(ctx, hop)
    if unchecked is not None:
        spots.append(f"orphans not checked: {unchecked}")
    qualifications = qualifications_of(ctx, hop, level)
    values = value_coverage(ctx)
    sweep = "at or before the end of" if ctx.full_sweep else "in"
    scope = {
        "edge": hop_label(hop.upstream, ds.dataset), "mapping": hop.contract.mapping,
        "contract": "declared" if hop.declared else "inferred from key_map",
        "contract_hash": contract_hash(hop, ds), "claim": claim_of(hop, qualifications),
        "qualifications": list(qualifications), "attribution": attribution_of(ctx, hop),
        "population": (f"each upstream key's latest version (by record time, among rows loaded before the window "
                       f"end) whose deadline (its load time + {ctx.settings.sla_hours:g}h) falls {sweep} the hop's "
                       f"deadline range, which continues from its committed coverage"),
        "window": [event.window_start.isoformat(), event.window_end.isoformat()],
        "deadline_range": [judged[0].isoformat(), judged[1].isoformat()] if judged else None,
        "cutoff": event.window_end.isoformat(), "full_sweep": ctx.full_sweep,
        "grain": {"key": list(ds.key), "version": ds.record_time.column if ds.record_time else None,
                  "history": ds.grain.history},
        "evidence_level": level, "evidence_levels": LEVEL, "blind_spots": spots,
        "values": values, "unverified_columns": values["unverified"],
        "orphans": "checked" if unchecked is None else "not checked",
    }
    if note:
        scope["note"] = note
    return scope


def unsupported(ctx: CheckContext, event: Event, hop: Hop, up: Dataset | None) -> CheckResult:
    kind = hop.contract.mapping
    source = "declared" if hop.declared else "inferred from key_map"
    reasons = {
        "aggregate": "aggregation is not reconciled by the ledger yet; it is never reconciled from an inferred kind "
                     "(the group-level hop checks still run)",
        "join_contribution": "a contributor is checked by HOP_CONTRIBUTION (build step 5), not by a completeness ledger",
        "survivorship": "survivorship is judged from build step 5",
        "fan_out": "one upstream record to several records cannot be judged",
    }
    note = f"{kind} ({source}): {reasons.get(kind, 'not judged by HOP_LEDGER')}"
    scope = scope_of(ctx, event, hop, up, None, note)
    return replace(did_not_run("unsupported_mapping", detail=json.dumps(scope, sort_keys=True),
                               group_values={"upstream": hop.upstream}),
                   qualifications=tuple(scope["qualifications"]))


def ledger_params(ctx: CheckContext, event: Event, hop: Hop, up: Dataset) -> dict[str, Any] | CheckResult:
    """Template parameters for one hop's ledger, or a DID_NOT_RUN result when it cannot be judged."""
    ds, up_id, tz = ctx.dataset, hop.upstream, ctx.default_timezone
    groups = {"upstream": up_id}
    if up.load_time is None:
        return did_not_run("invalid_config", detail=f"upstream {up_id} has no load_time to place its records",
                           group_values=groups)
    if ds.record_time is None or up.record_time is None:
        return did_not_run("invalid_config", detail="HOP_LEDGER needs record_time on both datasets",
                           group_values=groups)
    try:
        up_source = source_of(up, ctx.upstreams, tz)
    except (KeyError, ValueError) as exc:
        return did_not_run("invalid_config", detail=f"upstream {up_id} cannot be read: {type(exc).__name__}",
                           group_values=groups)
    for table, code in ((up.table, "table_missing"),
                        (hop.contract.rejects.table if hop.contract.rejects else None, "insufficient_evidence")):
        if table is None:
            continue
        try:
            exists = ctx.spark.catalog.tableExists(table)
        except Exception as exc:
            return did_not_run("metastore_unavailable", detail=error_detail(exc), group_values=groups)
        if not exists:
            what = "the rejects evidence" if code == "insufficient_evidence" else "table"
            return did_not_run(code, detail=f"{what} {table} not found", group_values=groups)
    mapping = ds.key_map[up_id]
    up_cols = [mapping[c] for c in ds.key]
    start, end = judged_range(ctx, event, CHECK_ID, up_id)
    deletion = hop.contract.deletion
    rejects = hop.contract.rejects
    return {
        "up_table": up_source, "up_filter": f"({up.feed_filter})" if up.feed_filter else None,
        "up_key": key_string([column_expr(up, c) for c in up_cols]), "up_raw": key_string(up_cols),
        "up_keyed": has_a_key(up_cols), "up_record": utc_expr(up.record_time, tz), "up_load": utc_expr(up.load_time, tz),
        "dn_table": ds.table, "dn_filter": f"({ds.feed_filter})" if ds.feed_filter else None,
        "dn_key": key_string([column_expr(ds, c) for c in ds.key]), "dn_raw": key_string(list(ds.key)),
        "dn_keyed": has_a_key(list(ds.key)), "dn_record": utc_expr(ds.record_time, tz),
        "dn_load": utc_expr(ds.load_time, tz) if ds.load_time is not None else None,
        "orphans": orphans_unchecked(ctx, hop) is None,
        "owned": [{"up": column_expr(up, u), "dn": column_expr(ds, d)} for u, d in ds.owned_columns.items()],
        "exclusions": [{"label": label, "condition": condition}
                       for label, condition in exclusion_terms(ds, up_id, up, tz)],
        "tombstone": deletion.tombstone if deletion else None,
        "applied_as": deletion.applied_as if deletion else "hard",
        "soft_when": deletion.soft_when if deletion else None,
        "rejects": None if rejects is None else rejects_params(hop, up, up_cols, tz),
        "window_start": utc_literal(event.window_start), "window_end": utc_literal(event.window_end),
        "deadline_start": utc_literal(start), "deadline_end": utc_literal(end),
        "pending_from": utc_literal(max(start, end)), "coverage": coverage_of(start, end), "judged": (start, end),
        "full_sweep": ctx.full_sweep, "one_to_one": hop.contract.mapping == "identity",
    }


def rejects_params(hop: Hop, up: Dataset, up_cols: list[str], tz: str) -> dict[str, Any]:
    """The rejects evidence's template parameters (C3-01): how a reject is keyed, tied to a version
    (lineage) and named (reason, rule), and which (reason, rule) labels the policy authorises."""
    rejects, policy = hop.contract.rejects, hop.contract.reject_policy
    lineage = rejects.lineage
    rule = (f"concat(CAST({rejects.rule_id_column} AS STRING), '@', CAST({rejects.rule_version_column} AS STRING))"
            if rejects.rule_id_column else None)
    authorised = {r: p for r, p in policy.items() if p.disposition == "authorised"}
    return {
        "table": rejects.table, "filter": rejects.condition, "reason": rejects.reason_column,
        "key": key_string([column_expr(up, c) for c in up_cols]), "time": utc_expr(rejects.time_column, tz),
        "by_batch": lineage.batch is not None,
        "lineage": (f"CAST({lineage.batch.column} AS STRING)" if lineage.batch else utc_expr(lineage.version, tz)),
        "up_batch": f"CAST({lineage.batch.upstream_column} AS STRING)" if lineage.batch else None,
        "rule": rule,
        "authorised_any": sorted(r for r, p in authorised.items() if not p.rules),
        "authorised": sorted(f"{r} {rule_id}" for r, p in authorised.items() for rule_id in p.rules),
        "rules_required": sorted(r for r, p in authorised.items() if p.rules),
    }


def reason_of(label: str | None) -> str | None:
    """A rejected label is the reason code, then the rule (id@version) when the rejects name one."""
    return label.split(" ", 1)[0] if label else None


def counted(rows: list[Any]) -> dict[str, Any]:
    """The summary rows as a ledger: dispositions with their labels, the populations, orphans,
    overlaps, and whether the identity holds."""
    dispositions: dict[str, dict[str | None, int]] = {d: {} for d, _, _ in DISPOSITIONS}
    variants: dict[str | None, str] = {}  # a rejected label's class under the policy, from the SQL
    totals = {"due": 0, "pending": 0, "untimed": 0, "orphan": 0}
    overlaps: dict[str, int] = {}
    due_rows = due_keys = keyless = unassigned = 0
    for r in rows:
        if r.line == "overlap":
            overlaps[f"{r.disposition}>{r.label}"] = overlaps.get(f"{r.disposition}>{r.label}", 0) + r.n
            continue
        totals[r.line] = totals.get(r.line, 0) + r.n
        if r.line != "due":
            continue
        due_rows += r.n
        if r.disposition is None:
            unassigned += r.n
        else:
            dispositions[r.disposition][r.label] = dispositions[r.disposition].get(r.label, 0) + r.n
            if r.disposition == "rejected":
                variants[r.label] = r.variant
        if r.label == "no key":
            keyless += r.n
        else:
            due_keys += r.keys
    # The identity: each due record (distinct key, or keyless row) in exactly one bucket.
    buckets = sum(n for labels in dispositions.values() for n in labels.values())
    balanced = unassigned == 0 and buckets == due_rows == due_keys + keyless
    return {"dispositions": dispositions, "totals": totals, "overlaps": overlaps, "balanced": balanced,
            "due": due_keys + keyless, "buckets": buckets, "reject_classes": variants}


def judge(hop: Hop, ledger: dict[str, Any]) -> tuple[str, int, list[str], list[str], dict[str, str]]:
    """(state, violations, observed parts, failure reasons, reject classes) under the contract."""
    c, d, due = hop.contract, ledger["dispositions"], ledger["due"]
    total = {name: sum(labels.values()) for name, labels in d.items()}
    reject_class = {label: ledger.get("reject_classes", {}).get(label) or "defect"
                    for label in d["rejected"] if label is not None}
    defect_rejects = sum(n for reason, n in d["rejected"].items() if reject_class.get(reason) == "defect")
    orphans = ledger["totals"]["orphan"]
    failures = []
    for name in ("unassessable", "deleted_not_applied", "excluded_but_present"):
        if total[name]:
            failures.append(f"{total[name]} {name}")
    if defect_rejects:
        failures.append(f"{defect_rejects} rejected for a defect reason")
    if orphans:
        failures.append(f"{orphans} orphan{'s' if orphans != 1 else ''}")
    tolerance = c.tolerance.model_dump()
    for name in TOLERATED:
        if total[name] > tolerance[name]:
            failures.append(f"{total[name]} {name}" + (f" (tolerated {tolerance[name]})" if tolerance[name] else ""))
    guards = {e.label: e.max_share for e in c.exclusions}
    for label, n in d["excluded"].items():
        share = guards.get(label)
        if share is not None and due and n / due > share:
            failures.append(f"exclusion {label} covers {n} of {due} due ({n / due:.1%}), over its max_share {share:.1%}")
    authorised_by_reason: dict[str, int] = {}
    for label, n in d["rejected"].items():
        if reject_class.get(label) == "authorised":
            authorised_by_reason[reason_of(label)] = authorised_by_reason.get(reason_of(label), 0) + n
    for reason, n in sorted(authorised_by_reason.items()):
        policy = c.reject_policy[reason]
        if due and n / due > policy.max_share:
            failures.append(f"rejects {reason} cover {n} of {due} due ({n / due:.1%}), over max_share "
                            f"{policy.max_share:.1%}")
    violations = (total["unassessable"] + total["deleted_not_applied"] + total["excluded_but_present"] + defect_rejects
                  + total["missing"] + total["stale"] + total["divergent"] + orphans)
    parts = []
    for name, _, _ in DISPOSITIONS:
        for label, n in sorted(d[name].items(), key=lambda kv: (kv[0] is None, kv[0] or "")):
            if not n:
                continue
            if name == "rejected":
                parts.append(f"rejected {label} ({reject_class.get(label, 'defect')}) {n}")
            elif label is not None:
                parts.append(f"{name} {label} {n}")
            else:
                parts.append(f"{name} {n}")
    return (FAILED if failures else PASSED), violations, parts, failures, reject_class


def triage(ledger: dict[str, Any], reject_class: dict[str, str], failures: list[str]) -> list[str]:
    """What to look at for each failing part of a FAILED ledger (L13): generic actions, by disposition,
    with counts and rule or reason labels only. HOP_LEDGER has no cause investigation; these lines
    are its triage, printed in the digest."""
    d, lines = ledger["dispositions"], []

    def labelled(name: str, only: Any = None) -> tuple[int, str]:
        picked = {k: n for k, n in d[name].items() if n and (only is None or only(k))}
        labels = ", ".join(sorted(k for k in picked if k is not None))
        return sum(picked.values()), (f" ({labels})" if labels else "")

    n, labels = labelled("unassessable")
    if n:
        lines.append(f"{n} record(s) cannot be judged{labels}: fix the key or time values, the key normaliser, "
                     "or the rejects' lineage")
    n, _ = labelled("deleted_not_applied")
    if n:
        lines.append(f"{n} deletion(s) arrived upstream and are not applied here: check the step that applies deletes")
    n, labels = labelled("excluded_but_present")
    if n:
        lines.append(f"{n} record(s) an approved exclusion keeps out are here{labels}: the exclusion is not applied "
                     "as approved")
    n, labels = labelled("rejected", lambda k: reject_class.get(k) == "defect")
    if n:
        lines.append(f"{n} record(s) rejected for a defect reason{labels}: correct the data or the rule upstream")
    if ledger["totals"]["orphan"]:
        lines.append(f"{ledger['totals']['orphan']} row(s) here that no upstream row maps to: look for substituted "
                     "keys or another writer")
    deficits = {name: sum(d[name].values()) for name in TOLERATED if sum(d[name].values())}
    if deficits:
        lines.append(", ".join(f"{n} {name}" for name, n in deficits.items())
                     + ": HOP_KEY_CURRENCY and HOP_VALUE_AGREEMENT name these keys and their causes")
    lines += [f"{f}: check that the rule is not broader than approved" for f in failures if "max_share" in f]
    return lines


def ceiling(ledger: dict[str, Any], reject_class: dict[str, str]) -> str:
    """The claim ceiling (ADR-06): the weakest evidence among the dispositions a PASS rests on."""
    d = ledger["dispositions"]
    used = [LEVEL[name] for name in ("deleted", "excluded", "superseded", "delivered") if sum(d[name].values())]
    if any(reject_class.get(reason) == "authorised" and n for reason, n in d["rejected"].items()):
        used.append(LEVEL["rejected"])
    return max(used, key=LEVELS.index) if used else "observed"


def ledger_lines(hop: Hop, ledger: dict[str, Any], reject_class: dict[str, str], failures: list[str],
                 scope: str) -> tuple[dict[str, Any], ...]:
    """The dq_reconciliation rows of one evaluation: populations, every disposition (zeros too, so the
    identity can be read off the store), orphans, overlaps and breached volume guards."""
    up = hop.upstream
    rows = [{"upstream": up, "line": name, "class": "population", "records": ledger["totals"][name],
             "evidence_level": "observed", "scope": scope} for name in ("due", "pending", "untimed")]
    for name, cls, level in DISPOSITIONS:
        labels = ledger["dispositions"][name] or {None: 0}
        for label, n in sorted(labels.items(), key=lambda kv: (kv[0] is None, kv[0] or "")):
            # a reject reason's class comes from the policy; the zero line takes the default, defect (ADR-04)
            row_class = reject_class.get(label, "defect") if name == "rejected" else cls
            rows.append({"upstream": up, "line": name, "label": label, "class": row_class, "records": n,
                         "evidence_level": level, "scope": scope})
    if json.loads(scope)["orphans"] == "checked":
        rows.append({"upstream": up, "line": "orphan", "class": "breach", "records": ledger["totals"]["orphan"],
                     "evidence_level": "observed", "scope": scope})
    for pair, n in sorted(ledger["overlaps"].items()):
        rows.append({"upstream": up, "line": "overlap", "label": pair, "class": "diagnostic", "records": n,
                     "evidence_level": "observed", "scope": scope})
    for failure in failures:
        if "max_share" in failure:
            rows.append({"upstream": up, "line": "guard", "label": failure, "class": "guard", "records": None,
                         "evidence_level": "reconstructed", "scope": scope})
    return tuple(rows)


class HopLedger(Check):
    check_id = CHECK_ID

    def applies_to(self, dataset: Dataset, pattern: str | None) -> bool:
        return bool(dataset.key_map) or bool(dataset.contracts)

    def required_columns(self, dataset: Dataset) -> list[str]:
        record = [dataset.record_time.column] if dataset.record_time else []
        return [*dataset.key, *record, *dataset.owned_columns.values()]

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        ups = [*ctx.dataset.key_map, *(u for u in ctx.dataset.contracts if u not in ctx.dataset.key_map)]
        return [per_hop(up_id, lambda up_id=up_id: self._hop(ctx, event, up_id)) for up_id in ups]

    @staticmethod
    def _hop(ctx: CheckContext, event: Event, up_id: str) -> CheckResult:
        from hcsc.datalake.dre.config.contracts import LEDGER_KINDS

        up = ctx.upstreams.get(up_id)
        hop = hop_of(ctx, up_id)
        if hop.contract.mapping not in LEDGER_KINDS or up_id not in ctx.dataset.key_map:
            return unsupported(ctx, event, hop, up)
        if up is None:
            return did_not_run("invalid_config", detail=f"upstream {up_id} is not configured",
                               group_values={"upstream": up_id})
        params = ledger_params(ctx, event, hop, up)
        if isinstance(params, CheckResult):
            return params
        view = ledger_view(ctx.dataset.dataset, up_id)
        assessed = ctx.spark.sql(render_sql("hop_ledger.sql.j2", p=params, mode="assessed", source=None))
        assessed = assessed.localCheckpoint()
        assessed.createOrReplaceTempView(view)
        rows = ctx.spark.sql(render_sql("hop_ledger.sql.j2", p=params, mode="summary", source=view)).collect()
        ledger = counted(rows)
        state, violations, parts, failures, reject_class = judge(hop, ledger)
        level = ceiling(ledger, reject_class)
        scope = scope_of(ctx, event, hop, up, level, judged=params["judged"])
        if state == FAILED:
            scope["triage"] = triage(ledger, reject_class, failures)
        scope_json = json.dumps(scope, sort_keys=True)
        held = {"judged_view": view, "checkpoints": (assessed,), "coverage": params["coverage"],
                "qualifications": tuple(scope["qualifications"])}
        lines = ledger_lines(hop, ledger, reject_class, failures, scope_json)
        totals = ledger["totals"]
        beside = [f"pending {totals['pending']}"] + ([f"untimed {totals['untimed']}"] if totals["untimed"] else [])
        observed = f"due {ledger['due']} ({', '.join(beside)}): " + (", ".join(parts) or "nothing")
        if scope["orphans"] == "checked":
            observed += f"; orphans {totals['orphan']}"
        if ledger["overlaps"]:
            observed += f"; overlaps {sum(ledger['overlaps'].values())}"
        groups = {"upstream": up_id}
        if not ledger["balanced"]:
            result = did_not_run("ledger_unbalanced", detail=scope_json, group_values=groups)
            return replace(result, observed=f"{observed}; {ledger['buckets']} in buckets for {ledger['due']} due",
                           reconciliation=lines, **held)
        if not ledger["due"]:
            result = did_not_run("empty_population", detail=scope_json, group_values=groups)
            return replace(result, observed=observed, reconciliation=lines, **held)
        return CheckResult(state, population=ledger["due"], violations=violations,
                           observed=observed + (f"; fails: {'; '.join(failures)}" if failures else ""),
                           expected="every due record in exactly one authorised disposition, within tolerance and "
                                    "volume guards, and no orphans",
                           group_values=groups, detail=scope_json, reconciliation=lines, **held)


register_check(HopLedger())
