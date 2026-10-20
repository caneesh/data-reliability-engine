"""T1_SOURCE_ATTRIBUTION: each row of a union target owned by exactly one source (build step 5;
docs/design/reconciliation-model.md section 6.1; ADR-03).

A union_partition target is one physical table that several configured datasets read, each through
its feed_filter (its attribution predicate, decision D5) and each with a union_partition contract to
its own upstream: the table's *siblings*. The attribution holds when the siblings' predicates are
disjoint and complete over every row of the table (the rows any edge reads): no row matches none of
them (unattributed, a predicate evaluating to NULL counting as no match) and no row matches more
than one (overlap). One result per physical table per window (the runner keeps one evaluation per
table, runner.once_per_table). PASSED when both counts are 0, FAILED otherwise, with the counts per
predicate (counts only, never a key or value); a table with no rows is DID_NOT_RUN /
empty_population; a sibling without a feed_filter is DID_NOT_RUN / invalid_config, naming it.

HOP_LEDGER proves the same thing in its own evaluation (`prove`, from the same SQL rendering,
`attribution_sql`), so the check and the ledger can never disagree: a union hop's attribution is
`verified` only when that proof holds.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from hcsc.datalake.dre.checks.base import (
    FAILED, PASSED, Check, CheckContext, CheckResult, did_not_run, error_detail, render_sql,
)
from hcsc.datalake.dre.checks.registry import register_check

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.checks.events import Event
    from hcsc.datalake.dre.config.models import Dataset

CHECK_ID = "T1_SOURCE_ATTRIBUTION"


def is_union(ds: Dataset) -> bool:
    """Whether the dataset reads its table as one partition of a union (a union_partition contract)."""
    return any(c.mapping == "union_partition" for c in ds.contracts.values())


def siblings(datasets: Iterable[Dataset], ds: Dataset) -> list[Dataset]:
    """Every configured dataset on ds's physical table with a union_partition contract, ds itself
    included when it has one (as given, so a context that knows no other dataset still has it), in
    dataset id order."""
    found = {d.dataset: d for d in datasets if ds.table is not None and d.table == ds.table and is_union(d)}
    if is_union(ds):
        found[ds.dataset] = ds
    return [found[k] for k in sorted(found)]


def unfiltered(sibs: list[Dataset]) -> list[str]:
    """The siblings with no feed_filter: their rows cannot be attributed (dre validate rejects this)."""
    return [d.dataset for d in sibs if not d.feed_filter]


def attribution_sql(table: str, sibs: list[Dataset]) -> str:
    """The one rendering of source_attribution.sql.j2, shared by the check and HOP_LEDGER."""
    return render_sql("source_attribution.sql.j2", p={"table": table,
                                                      "predicates": [f"({d.feed_filter})" for d in sibs]})


@dataclass(frozen=True)
class Proof:
    """The attribution of one union target as one evaluation found it. `why` says why it is not
    proven (None: verified); the counts are None when the proof could not run."""

    table: str | None
    siblings: tuple[str, ...]
    rows: int | None = None
    unattributed: int | None = None
    overlap: int | None = None
    per_predicate: tuple[dict[str, Any], ...] = ()
    why: str | None = None

    @property
    def verified(self) -> bool:
        return self.why is None

    def as_dict(self) -> dict[str, Any]:
        return {"table": self.table, "siblings": list(self.siblings), "rows": self.rows,
                "unattributed": self.unattributed, "overlap": self.overlap,
                "per_predicate": list(self.per_predicate), "verified": self.verified, "why": self.why}


def prove(spark: SparkSession, ds: Dataset, datasets: Iterable[Dataset]) -> Proof:
    """Run the attribution SQL over ds's table for its siblings. Raises what Spark raises (the check
    turns it into DID_NOT_RUN, the ledger into an unproven attribution)."""
    sibs = siblings(datasets, ds)
    names = tuple(d.dataset for d in sibs)
    missing = unfiltered(sibs)
    if missing:
        return Proof(ds.table, names, why=f"sibling(s) {missing} on {ds.table} have no feed_filter to attribute rows by")
    row = spark.sql(attribution_sql(ds.table, sibs)).collect()[0]
    per = tuple({"dataset": d.dataset, "upstreams": sorted(u for u, c in d.contracts.items()
                                                           if c.mapping == "union_partition"),
                 "rows": row[f"rows_{i}"], "overlap": row[f"overlap_{i}"]} for i, d in enumerate(sibs, 1))
    why = None
    if not row.population:
        why = f"{ds.table} has no rows: there is nothing to attribute"
    elif row.unattributed or row.overlap:
        why = (f"{row.unattributed} row(s) of {ds.table} match no sibling's feed_filter and {row.overlap} match "
               f"more than one")
    return Proof(ds.table, names, row.population, row.unattributed, row.overlap, per, why)


def ledger_proof(ctx: CheckContext) -> Proof:
    """HOP_LEDGER's proof for a union hop: never raises; a proof that cannot run is not verified."""
    try:
        return prove(ctx.spark, ctx.dataset, ctx.datasets.values())
    except Exception as exc:
        return Proof(ctx.dataset.table, tuple(d.dataset for d in siblings(ctx.datasets.values(), ctx.dataset)),
                     why=f"the attribution proof could not run: {error_detail(exc)}")


def observed_of(proof: Proof) -> str:
    """`100 rows: gold_a 60, gold_b 38; unattributed 2, overlap 0`, with a predicate's overlap where it has one."""
    parts = [f"{p['dataset']} {p['rows']}" + (f" ({p['overlap']} overlapping)" if p["overlap"] else "")
             for p in proof.per_predicate]
    return (f"{proof.rows} rows: {', '.join(parts)}; unattributed {proof.unattributed}, overlap {proof.overlap}")


class SourceAttribution(Check):
    check_id = CHECK_ID

    def applies_to(self, dataset: Dataset, pattern: str | None) -> bool:
        return is_union(dataset)

    def required_columns(self, dataset: Dataset) -> list[str]:
        return []  # the predicates are SQL fragments: a missing column is column_missing from Spark

    def evaluate(self, ctx: CheckContext, event: Event) -> list[CheckResult]:
        sibs = siblings(ctx.datasets.values(), ctx.dataset)
        missing = unfiltered(sibs)
        if missing:
            return [did_not_run("invalid_config", detail=(
                f"union_partition sibling(s) {missing} on {ctx.dataset.table} have no feed_filter: their rows cannot "
                "be attributed; set each one's feed_filter to the rows its source owns"))]
        proof = prove(ctx.spark, ctx.dataset, ctx.datasets.values())
        detail = json.dumps(proof.as_dict(), sort_keys=True)
        if not proof.rows:
            return [did_not_run("empty_population", detail=detail)]
        violations = proof.unattributed + proof.overlap
        return [CheckResult(FAILED if violations else PASSED, population=proof.rows, violations=violations,
                            observed=observed_of(proof),
                            expected="every row of the table matches exactly one sibling's feed_filter",
                            detail=detail)]


register_check(SourceAttribution())
