"""CHAIN_COMPLETENESS: every due source record followed edge by edge to the chain's terminal (build step 6;
docs/design/reconciliation-model.md section 7; ADR-01, ADR-03, ADR-06).

A parametrised check, configured on the chain's source dataset: `check_params: {CHAIN_COMPLETENESS:
{terminal: <dataset id>}}`. The path is the unique key_map path from the source to the terminal (each
next dataset's key_map names the previous one); none, or more than one, is DID_NOT_RUN / invalid_config.
One result per chain per source window (group `chain`, `<source>-><terminal>`).

**Due population.** The source records (each key's latest version as of the cutoff, as HOP_LEDGER's due
definition) whose chain deadline (that version's load time + the sum of the SLAs, sla_hours of each
downstream dataset along the path) falls in the chain's deadline range (checks/hop/coverage.py, group
`chain`). A record pending at any edge is pending for the chain, not judged.

**How.** Never from stored key events: each edge's layers are read directly, through HOP_LEDGER's own
assessed SQL (hop_ledger.sql.j2), bounded at the first edge by the chain's deadline range and at every
later edge to the keys the previous edge delivered, mapped forward through the rows of the dataset
between them. A later edge judges each carried key at its latest version as of the cutoff: due when that
version's load time + the edge's sla_hours is at or before the cutoff, pending otherwise.

**Outcomes** per source record: reached (delivered at every edge), accounted (delivered up to an edge,
then an authorised disposition there), lost (a deficit, breach, defect or unknown class at an edge),
unobservable (the path crosses an edge the ledger does not judge, a kind outside LEDGER_KINDS; or one
record maps to several keys there). Classes are read from the ledger's DISPOSITIONS, so a new
disposition is placed by its class. FAILED if any record is lost (the scope names the first edge where
a record was lost as the first observed gap, not necessarily the origin); otherwise DID_NOT_RUN /
partial_chain if any is unobservable (the scope names the edge and the segment proven); otherwise
PASSED. Nothing due: DID_NOT_RUN / empty_population.

A chain PASS is the only result that may be read as "every expected business record reached the
terminal", for that chain, window, cutoff and contract versions; it carries every edge's qualifications,
so a qualified chain PASS is never healthy. Outcome counts go to dq_reconciliation (line = the outcome,
label = the edge where the record ended); the exception keys (lost, unobservable) to dq_key_event when
an HMAC secret is set. Counts, codes and labels only: never a key outside the dq store.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.base import (
    FAILED, PASSED, Check, CheckContext, CheckResult, did_not_run, error_detail, release_checkpoint, render_sql,
    utc_literal,
)
from hcsc.datalake.dre.checks.derived import base_datasets
from hcsc.datalake.dre.checks.hop.common import HASH_FUNCTION, has_a_key, hop_label, register_key_hash
from hcsc.datalake.dre.checks.hop.coverage import GAP, coverage_of, judged
from hcsc.datalake.dre.checks.keys import column_expr, key_string
from hcsc.datalake.dre.checks.registry import register_check
from hcsc.datalake.dre.config.models import Identifier, Model

if TYPE_CHECKING:
    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.contracts import Hop
    from hcsc.datalake.dre.config.models import Dataset

CHECK_ID = "CHAIN_COMPLETENESS"
OUTCOMES = ("reached", "accounted", "lost", "unobservable")
# the class and evidence level of an outcome line with no disposition behind it
OUTCOME_CLASS = {"reached": "ok", "accounted": "authorised", "lost": "deficit", "unobservable": "unknown"}
MAX_PATHS = 2  # enough to tell a unique path from an ambiguous one


class ChainParams(Model):
    terminal: Identifier  # the dataset the chain ends at


@dataclass(frozen=True)
class Edge:
    """One edge of the path: its upstream, its downstream, the contract, and a context for the ledger."""

    up: Dataset
    dn: Dataset
    hop: Hop
    ctx: CheckContext  # the downstream's context, as HOP_LEDGER would get it
    sla_hours: float

    @property
    def label(self) -> str:
        return hop_label(self.up.dataset, self.dn.dataset)

    @property
    def judged(self) -> bool:
        from hcsc.datalake.dre.config.contracts import LEDGER_KINDS

        return self.hop.contract.mapping in LEDGER_KINDS


def chain_label(source: str, terminal: str) -> str:
    return f"{source}->{terminal}"


def paths(datasets: dict[str, Dataset], source: str, terminal: str, limit: int = MAX_PATHS) -> list[list[str]]:
    """Simple key_map paths from source to terminal (a dataset follows every dataset its key_map names),
    at most `limit` of them."""
    children: dict[str, list[str]] = {}
    for ds_id in sorted(datasets):
        for up_id in datasets[ds_id].key_map:
            children.setdefault(up_id, []).append(ds_id)
    found: list[list[str]] = []

    def walk(path: list[str]) -> None:
        for nxt in children.get(path[-1], []):
            if len(found) >= limit or nxt in path:
                continue
            if nxt == terminal:
                found.append([*path, nxt])
            else:
                walk([*path, nxt])

    walk([source])
    return found


def upstreams(datasets: dict[str, Dataset], ds: Dataset) -> dict[str, Dataset]:
    """The datasets a hop check on `ds` reads (as runner.upstreams_of): its key_map upstreams and the
    datasets a derived one is built from."""
    found = {u: datasets[u] for u in ds.key_map if u in datasets}
    for up in list(found.values()):
        found.update(base_datasets(up, datasets))
    return found


def resolve(ctx: CheckContext) -> list[Edge] | CheckResult:
    """The chain's edges, or DID_NOT_RUN / invalid_config naming why there is no unique path."""
    from hcsc.datalake.dre.config.contracts import hop_contract

    config, source, terminal = ctx.config, ctx.dataset.dataset, ctx.params.terminal
    groups = {"chain": chain_label(source, terminal)}
    if config is None:
        return did_not_run("invalid_config", detail="the configuration is not available to follow the chain",
                           group_values=groups)
    datasets = config.datasets
    if terminal == source:
        return did_not_run("invalid_config", detail=f"the terminal {terminal} is the chain's source",
                           group_values=groups)
    if terminal not in datasets:
        return did_not_run("invalid_config", detail=f"the terminal {terminal} is not a configured dataset",
                           group_values=groups)
    found = paths(datasets, source, terminal)
    if not found:
        return did_not_run("invalid_config", detail=f"no key_map path from {source} to {terminal}: each dataset "
                                                    "on a chain names the one before it in its key_map",
                           group_values=groups)
    if len(found) > 1:
        shown = "; ".join(" -> ".join(p) for p in found)
        return did_not_run("invalid_config", detail=f"more than one key_map path from {source} to {terminal} "
                                                    f"({shown}): a chain needs exactly one", group_values=groups)
    edges = []
    for up_id, dn_id in zip(found[0], found[0][1:]):
        up, dn = datasets[up_id], datasets[dn_id]
        settings = config.dataset_settings(dn_id)
        edge_ctx = replace(ctx, dataset=dn, feed=config.feed_of(dn_id), settings=settings,
                           upstreams=upstreams(datasets, dn), params=None,
                           dq_database=None)  # the edge's range is the chain's: never read the hop's coverage
        edges.append(Edge(up, dn, hop_contract(datasets, dn, up_id), edge_ctx, settings.sla_hours))
    return edges


def edge_params(ctx: CheckContext, event: Event, edge: Edge, first: bool, judged: tuple[Any, Any],
                carried: str | None, recheck: str | None = None) -> dict[str, Any] | CheckResult:
    """HOP_LEDGER's parameters for one edge, bounded for the chain: the first edge judges the source
    records whose chain deadline falls in the chain's range; a later edge judges only the carried keys,
    due when their latest version's load time + the edge's sla_hours is at or before the cutoff."""
    from hcsc.datalake.dre.checks.hop.ledger import ledger_params

    params = ledger_params(edge.ctx, event, edge.hop, edge.up)
    if isinstance(params, CheckResult):
        return params
    if edge.dn.table is not None:
        try:
            exists = ctx.spark.catalog.tableExists(edge.dn.table)
        except Exception as exc:
            return did_not_run("metastore_unavailable", detail=error_detail(exc))
        if not exists:
            return did_not_run("table_missing", detail=f"table {edge.dn.table} not found")
    if first:
        start, end = judged
        bounds = {"deadline_start": utc_literal(start), "deadline_end": utc_literal(end),
                  "pending_from": utc_literal(max(start, end)), "full_sweep": ctx.full_sweep, "recheck": recheck}
    else:
        bound = utc_literal(event.window_end - timedelta(hours=edge.sla_hours))
        bounds = {"deadline_start": bound, "deadline_end": bound, "pending_from": bound, "full_sweep": True}
    return {**params, **bounds, "orphans": False, "carried": carried}


def forward_params(previous: Edge, edge: Edge, state: str) -> dict[str, Any]:
    """How the records the previous edge delivered are keyed at this edge: through the rows of the
    dataset between them, from its own key (as the previous edge's ledger keyed it) to the columns this
    edge's key_map maps."""
    mid, mapping = previous.dn, edge.dn.key_map[previous.dn.dataset]
    next_cols = [mapping[c] for c in edge.dn.key]
    return {"state": state, "table": mid.table, "filter": f"({mid.feed_filter})" if mid.feed_filter else None,
            "prev_key": key_string([column_expr(mid, c) for c in mid.key]), "prev_keyed": has_a_key(list(mid.key)),
            "next_key": key_string([column_expr(mid, c) for c in next_cols]), "next_keyed": has_a_key(next_cols),
            "previous": mid.dataset}


def edge_entry(edge: Edge, note: str | None = None) -> dict[str, Any]:
    from hcsc.datalake.dre.checks.hop.ledger import contract_hash

    entry = {"edge": edge.label, "mapping": edge.hop.contract.mapping,
             "contract": "declared" if edge.hop.declared else "inferred from key_map",
             "contract_hash": contract_hash(edge.hop, edge.dn), "sla_hours": edge.sla_hours, "judged": edge.judged}
    if note:
        entry["note"] = note
    return entry


def unjudged_note(edge: Edge) -> str:
    source = "declared" if edge.hop.declared else "inferred from key_map"
    return (f"{edge.hop.contract.mapping} ({source}): HOP_LEDGER does not judge this kind, so a record's lineage "
            "across it is not observable")


def chain_claim(edges: list[Edge], qualifications: tuple[str, ...]) -> str:
    """As ledger.claim_of, over the whole path: contract-proven only when every edge is declared and
    nothing is unproven."""
    if any(not e.hop.declared for e in edges):
        return "inferred contract"
    return "contract-proven" if not qualifications else f"declared contract, qualified: {', '.join(qualifications)}"


def tally(rows: list[Any], edges: list[Edge]) -> dict[str, Any]:
    """The fate summary as outcome counts: per outcome, per edge, and pending per edge."""
    from hcsc.datalake.dre.checks.hop.ledger import LEVEL, LEVELS

    totals = {o: 0 for o in (*OUTCOMES, "pending", "untimed")}
    by_edge: dict[str, dict[str, int]] = {e.label: {} for e in edges}
    dispositions: dict[str, dict[str, int]] = {e.label: {} for e in edges}
    lines: dict[tuple[str, str | None, str, str], int] = {}
    edge_level = {e.label: "observed" for e in edges}
    for r in rows:
        totals[r.outcome] = totals.get(r.outcome, 0) + r.n
        by_edge.setdefault(r.edge, {})
        by_edge[r.edge][r.outcome] = by_edge[r.edge].get(r.outcome, 0) + r.n
        if r.outcome in ("accounted", "lost") and r.disposition:
            name = r.disposition + (f" {r.label}" if r.label else "")
            dispositions.setdefault(r.edge, {})
            dispositions[r.edge][f"{r.outcome} {name}"] = dispositions[r.edge].get(f"{r.outcome} {name}", 0) + r.n
        if r.outcome not in OUTCOMES:
            continue
        level = ("unavailable" if r.outcome == "unobservable"
                 else "reported" if r.disposition == "rejected" and r.cls == "authorised"
                 else LEVEL.get(r.disposition or "", "observed"))
        if r.outcome == "accounted":
            edge_level[r.edge] = max(edge_level.get(r.edge, "observed"), level, key=LEVELS.index)
        key = (r.outcome, r.edge, r.cls or OUTCOME_CLASS[r.outcome], level)
        lines[key] = lines.get(key, 0) + r.n
    return {"totals": totals, "by_edge": by_edge, "dispositions": dispositions, "lines": lines,
            "edge_level": edge_level}


def chain_lines(label: str, counts: dict[str, int], pending: dict[str | None, int], untimed: int,
                outcome_lines: dict[tuple[str, str | None, str, str], int], scope: str) -> tuple[dict[str, Any], ...]:
    """dq_reconciliation rows: the due, pending (beyond the range, and at each edge) and untimed
    populations, then every outcome by the edge where the record ended (zeros too)."""
    rows = [{"upstream": label, "line": "due", "class": "population", "records": counts["due"],
             "evidence_level": "observed", "scope": scope}]
    for edge, n in sorted(pending.items(), key=lambda kv: (kv[0] is not None, kv[0] or "")):
        if edge is None or n:
            rows.append({"upstream": label, "line": "pending", "label": edge, "class": "population", "records": n,
                         "evidence_level": "observed", "scope": scope})
    rows.append({"upstream": label, "line": "untimed", "class": "population", "records": untimed,
                 "evidence_level": "observed", "scope": scope})
    for outcome in OUTCOMES:
        found = sorted((k, n) for k, n in outcome_lines.items() if k[0] == outcome)
        if not found:
            level = "unavailable" if outcome == "unobservable" else "observed"
            rows.append({"upstream": label, "line": outcome, "label": None, "class": OUTCOME_CLASS[outcome],
                         "records": 0, "evidence_level": level, "scope": scope})
        for (_, edge, cls, level), n in found:
            rows.append({"upstream": label, "line": outcome, "label": edge, "class": cls, "records": n,
                         "evidence_level": level, "scope": scope})
    return tuple(rows)


class ChainCompleteness(Check):
    check_id = CHECK_ID
    params_model = ChainParams

    def required_columns(self, dataset: Dataset, params: Any = None) -> list[str]:
        return [*dataset.key, *([dataset.record_time.column] if dataset.record_time else [])]

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        edges = resolve(ctx)
        if isinstance(edges, CheckResult):
            return [edges]
        views: list[str] = []
        frames: list[Any] = []
        try:
            return [self._judge(ctx, event, edges, views, frames)]
        finally:
            for frame in frames:
                release_checkpoint(frame)
            for view in views:
                try:
                    ctx.spark.catalog.dropTempView(view)
                except Exception:  # only memory is at stake
                    pass

    @staticmethod
    def _judge(ctx: CheckContext, event: Event, edges: list[Edge], views: list[str], frames: list[Any]) -> CheckResult:
        from hcsc.datalake.dre.checks.hop.ledger import DISPOSITIONS, LEVELS, qualifications_of

        source, terminal = ctx.dataset.dataset, ctx.params.terminal
        label = chain_label(source, terminal)
        groups = {"chain": label}
        total_sla = sum(e.sla_hours for e in edges)
        span = judged(ctx, event, CHECK_ID, label, group="chain", sla_hours=total_sla)
        start, end = span.start, span.end
        # the edges judged: up to the first the ledger does not judge (the first edge is always read: it
        # gives the source records, unobservable at once when it is not judged)
        stop = next((i for i, e in enumerate(edges) if not e.judged), None)
        read = edges[:1] if stop == 0 else edges[:stop] if stop is not None else edges
        classes = [(d, c) for d, c, _ in DISPOSITIONS]

        def name(kind: str, i: int) -> str:
            return f"dre_chain__{source}__{i}_{kind}"

        def materialise(sql: str, view: str) -> None:
            frame = ctx.spark.sql(sql).localCheckpoint()
            frames.append(frame)
            frame.createOrReplaceTempView(view)
            views.append(view)

        # Records an earlier evaluation left pending at an edge after the first (recorded PENDING): judged
        # again now, whatever the range, since the range has moved past their chain deadline (P1-03).
        # NORMAL runs only, with key events: a recheck neither reads nor writes the chain's key state.
        # Every carrying evaluation records a record it still finds pending again, so while the chain's
        # coverage continues from inside the lookback its outstanding records' latest events are inside it
        # too; otherwise (the chain was not evaluated for longer) the read reaches back to dq_key_event's
        # retention, so a long-pending record is never forgotten (final conditional approval, CA-02).
        carrying = ctx.record_key_events and ctx.key_secret is not None and ctx.dq_database is not None
        recheck, carried_in = None, 0
        carry_since = ctx.history_since if span.in_lookback else ctx.key_events_since
        if carrying and ctx.spark.catalog.tableExists(f"{ctx.dq_database}.v_committed_key_event"):
            since = f" AND run_date >= DATE'{carry_since.isoformat()}'" if carry_since is not None else ""
            materialise(
                f"SELECT key_value AS k FROM (SELECT key_value, event, ROW_NUMBER() OVER (PARTITION BY key_hash "
                f"ORDER BY observed_at DESC, run_id DESC) AS rn FROM {ctx.dq_database}.v_committed_key_event "
                f"WHERE dataset = '{source}' AND check_id = '{CHECK_ID}' AND hop = '{label}'{since}) x "
                f"WHERE rn = 1 AND event = 'PENDING' AND key_value IS NOT NULL", name("recheck", 0))
            recheck = name("recheck", 0)
            carried_in = ctx.spark.table(recheck).count()
        state_views: list[str] = []
        for i, edge in enumerate(read):
            carried = None
            if i:
                materialise(render_sql("chain.sql.j2", mode="forward",
                                       f=forward_params(read[i - 1], edge, state_views[-1])), name("forward", i))
                materialise(render_sql("chain.sql.j2", mode="keys", source=name("forward", i)), name("keys", i))
                carried = name("keys", i)
            params = edge_params(ctx, event, edge, i == 0, (start, end), carried, recheck if i == 0 else None)
            if isinstance(params, CheckResult):
                return replace(params, detail=f"edge {edge.label}: {params.detail}", group_values=groups)
            materialise(render_sql("hop_ledger.sql.j2", p=params, mode="assessed", source=None), name("assessed", i))
            e = {"first": i == 0, "judged": edge.judged, "assessed": name("assessed", i),
                 "carried": name("forward", i) if i else None, "edge": edge.label, "classes": classes}
            materialise(render_sql("chain.sql.j2", mode="edge", e=e), name("state", i))
            state_views.append(name("state", i))
        if stop is not None and stop > 0:
            ending = {"view": state_views[-1], "edge": edges[stop].label, "outcome": "unobservable",
                      "disposition": None, "label": f"not judged: {edges[stop].hop.contract.mapping}", "cls": "unknown"}
        else:
            ending = {"view": state_views[-1], "edge": read[-1].label, "outcome": "reached",
                      "disposition": "delivered", "label": None, "cls": "ok"}
        materialise(render_sql("chain.sql.j2", mode="fate", views=state_views, ending=ending), name("fate", 0))
        rows = ctx.spark.sql(render_sql("chain.sql.j2", mode="summary", source=name("fate", 0))).collect()
        population = {r.line: r.n for r in ctx.spark.sql(
            render_sql("chain.sql.j2", mode="population", source=state_views[0])).collect()}

        t = tally(rows, edges)
        totals = t["totals"]
        first_label = edges[0].label
        pending_edges = {e: n.get("pending", 0) for e, n in t["by_edge"].items() if e != first_label}
        beyond = t["by_edge"].get(first_label, {}).get("pending", 0)  # chain deadline after the range
        due = population.get("due", 0) - sum(pending_edges.values())
        counts = {"due": due, **{o: totals[o] for o in OUTCOMES}}
        balanced = sum(totals[o] for o in OUTCOMES) == due and beyond == population.get("pending", 0)
        gap = next((e.label for e in edges if t["by_edge"].get(e.label, {}).get("lost")), None)

        qualifications: list[str] = []
        for edge in edges:
            for q in qualifications_of(edge.ctx, edge.hop, t["edge_level"].get(edge.label, "observed")):
                if q not in qualifications:
                    qualifications.append(q)
        qualifications.extend(span.qualifications)  # CA-01: a range that does not continue from trusted coverage
        used = [lv for (o, _, _, lv), n in t["lines"].items() if o in ("reached", "accounted") and n]
        level = max(used, key=LEVELS.index) if used else "observed"
        proven = [e.label for e in read if e.judged]
        horizon = (f"; PENDING events are read back to {carry_since.isoformat()} ("
                   + ("the lookback, inside which the chain's coverage continues" if span.in_lookback
                      else "dq_key_event's retention") + "): a record pending only before that is judged by the "
                   "full sweep") if carry_since is not None else ""
        spots = ["a record pending at an edge after the first is recorded PENDING and judged by the next "
                 "evaluation, once due there" + horizon if carrying else
                 "a record pending at an edge after the first is not carried to the next evaluation (no key events: "
                 "no hmac_secret_file, or a recheck): it is judged only by the full sweep"]
        unobservable = None
        if stop is not None:
            unobservable = {"edge": edges[stop].label, "why": unjudged_note(edges[stop]),
                            "proven_segment": " -> ".join([source, *(e.dn.dataset for e in edges[:stop])])
                            if stop else None}
            spots.insert(0, f"{edges[stop].label}: {unjudged_note(edges[stop])}")
        secret = ctx.key_secret is not None
        exceptions = totals["lost"] + totals["unobservable"]
        scope = {
            "edge": f"{label} (chain)", "chain": label, "mapping": "chain",
            "contract": f"{len(edges)} edge(s): " + ", ".join(f"{e.label} {e.hop.contract.mapping}" for e in edges),
            "claim": chain_claim(edges, tuple(qualifications)), "qualifications": qualifications,
            "path": [source, *(e.dn.dataset for e in edges)],
            "edges": [edge_entry(e, unjudged_note(e) if not e.judged else None) for e in edges],
            "population": (f"each source key's latest version (by record time, among rows loaded before the window "
                           f"end) whose chain deadline (its load time + {total_sla:g}h, the sum of the edge SLAs) "
                           "falls in the chain's deadline range, which continues from its committed coverage; a "
                           "record pending at any edge is pending for the chain"),
            "window": [event.window_start.isoformat(), event.window_end.isoformat()],
            "deadline_range": [start.isoformat(), end.isoformat()], "chain_sla_hours": total_sla,
            "coverage_basis": span.basis,
            "cutoff": event.window_end.isoformat(), "full_sweep": ctx.full_sweep,
            "first_observed_gap": None if gap is None else {
                "edge": gap, "lost": t["by_edge"][gap]["lost"],
                "note": "the first edge where a record was lost; not necessarily where the loss began"},
            "proven_segment": proven, "unobservable": unobservable,
            "outcomes": {**counts, "pending": beyond + sum(pending_edges.values()),
                         "pending_at_edges": {e: n for e, n in pending_edges.items() if n},
                         "untimed": totals["untimed"]},
            "by_edge": t["dispositions"], "evidence_level": level, "blind_spots": spots,
            "key_events": ("exception keys recorded (lost, unobservable)" if secret
                           else "not recorded: hmac_secret_file is not set") if exceptions else None,
            "carried_in": carried_in,
        }
        state = FAILED if totals["lost"] else PASSED
        if state == FAILED:
            scope["triage"] = [f"{t['by_edge'][gap]['lost']} record(s) first lost at {gap}: its HOP_LEDGER result and "
                               "HOP_KEY_CURRENCY name the keys and their causes; earlier edges delivered them"]
        scope_json = json.dumps(scope, sort_keys=True)
        lines = chain_lines(label, counts, {None: beyond, **pending_edges}, totals["untimed"], t["lines"], scope_json)
        pending_total = beyond + sum(pending_edges.values())
        observed = (f"due {due} (pending {pending_total}" + (f", untimed {totals['untimed']}" if totals["untimed"] else "")
                    + "): " + ", ".join(f"{o} {counts[o]}" for o in OUTCOMES)
                    + (f"; first observed gap {gap}" if gap else "")
                    + (f"; unobservable at {unobservable['edge']}" if unobservable and totals["unobservable"] else ""))
        held = {"coverage": coverage_of(start, end), "qualifications": tuple(qualifications), "reconciliation": lines,
                "observed": observed}
        if not balanced:
            return replace(did_not_run("ledger_unbalanced", detail=scope_json, group_values=groups), **held)
        events = None
        if carrying and (exceptions or sum(pending_edges.values()) or carried_in):
            register_key_hash(ctx.spark, ctx.key_secret)
            events = ctx.spark.sql(render_sql("chain.sql.j2", mode="events", source=name("fate", 0),
                                              hash_fn=HASH_FUNCTION, first=first_label,
                                              recheck=recheck)).localCheckpoint()
        carry = {"key_events": events, "checkpoints": (events,) if events is not None else ()}
        if not due:  # a window whose only records are pending at an edge still records them, to carry
            return replace(did_not_run("empty_population", detail=scope_json, group_values=groups), **held, **carry)
        if state == PASSED and totals["unobservable"]:
            return replace(did_not_run("partial_chain", detail=scope_json, group_values=groups), **held, **carry)
        return replace(CheckResult(state, population=due, violations=totals["lost"], group_values=groups,
                                   detail=scope_json,
                                   expected="every due source record reached the terminal, or was accounted for by an "
                                            "authorised disposition along the path"),
                       **held, **carry)


register_check(ChainCompleteness())
