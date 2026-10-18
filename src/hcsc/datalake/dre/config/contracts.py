"""Each hop's reconciliation contract, resolved (Phase 1 design; docs/design/reconciliation-model.md).

A hop is (dataset, one upstream). Its contract is the dataset's `contracts` entry for the upstream
when there is one; otherwise it is read from the release 1 fields, which stay valid as aliases:
`key_map` (an identity or dedup hop), `hop_exclusions` (one legacy exclusion, unversioned and
unapproved), `winner_rule` (the version rule) and `owned_columns` (identity transforms). A hop with
no contract entry is `aggregate` when its upstream is derived and `dedup` otherwise: that is what
the hop checks judge, the latest upstream version of each key. An inferred kind is told apart
from a declared one everywhere (`Hop.declared`, the report, the blind spots), and never supports a
contract-proven claim (D6, ratified 2026-10-17 with conditions; docs/decisions.md).

`blind_spots` lists what the engine cannot yet see or prove on a hop, from configuration alone:
`dre validate` prints it, so nobody assumes coverage that is not there (ADR-06).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from hcsc.datalake.dre.config.models import EdgeContract, Exclusion

if TYPE_CHECKING:
    from hcsc.datalake.dre.config.loader import Config
    from hcsc.datalake.dre.config.models import Dataset

# Kinds the engine judges today, kinds whose checks come in a later build step, and kinds it cannot
# express (target-architecture.md section 7; reconciliation model section 4).
JUDGED = ("identity", "dedup", "merge", "union_partition", "aggregate")
LATER = {"join_contribution": 5, "survivorship": 5}
UNSUPPORTED = ("fan_out",)
# Kinds compared through key_map; join_contribution resolves a reference and fan_out cannot be keyed.
KEYED = ("identity", "dedup", "merge", "union_partition", "survivorship", "aggregate")
# HOP_LEDGER judges these kinds (build step 3); the others are DID_NOT_RUN / unsupported_mapping
# there, an inferred aggregate included (D6 condition 3).
LEDGER_KINDS = ("identity", "dedup", "merge", "union_partition")


@dataclass(frozen=True)
class Hop:
    """One hop's contract as the engine uses it."""

    upstream: str
    dataset: str
    contract: EdgeContract
    declared: bool  # from `contracts`; False: read from the release 1 fields
    legacy_exclusion: str | None  # `hop_exclusions` for the upstream, when the contract lists no exclusions

    @property
    def label(self) -> str:
        return f"{self.upstream}->{self.dataset}"

    @property
    def exclusions(self) -> list[Exclusion]:
        return self.contract.exclusions


def hop_contract(config_datasets: dict[str, Dataset], ds: Dataset, up_id: str) -> Hop:
    """The contract of the hop from `up_id` into `ds`."""
    declared = ds.contracts.get(up_id)
    if declared is not None:  # a contract without exclusions keeps a hop_exclusions entry (validate warns)
        legacy = None if declared.exclusions else ds.hop_exclusions.get(up_id)
        return Hop(up_id, ds.dataset, declared, True, legacy)
    up = config_datasets.get(up_id)
    kind = "aggregate" if up is not None and up.derived is not None else "dedup"
    return Hop(up_id, ds.dataset, EdgeContract(mapping=kind), False, ds.hop_exclusions.get(up_id))


def hops(config: Config) -> list[Hop]:
    """Every hop of the configuration, by dataset then upstream: each key_map entry, and each
    contract entry outside key_map (a contributor, or a hop that cannot be keyed)."""
    found = []
    for ds_id in sorted(config.datasets):
        ds = config.datasets[ds_id]
        for up_id in [*ds.key_map, *(u for u in ds.contracts if u not in ds.key_map)]:
            found.append(hop_contract(config.datasets, ds, up_id))
    return found


def exclusion_text(hop: Hop) -> str:
    """How a hop result names what its exclusions kept out."""
    if hop.legacy_exclusion is not None:
        return "hop_exclusions"
    return "exclusions " + ", ".join(e.label for e in hop.exclusions)


def blind_spots(config: Config, hop: Hop) -> list[str]:
    """What the engine cannot see or prove on this hop yet, in plain words."""
    c, spots = hop.contract, []
    if not hop.declared:  # D6, ratified 2026-10-17 with conditions: an inferred kind proves nothing by contract
        spots.append(f"kind {c.mapping} is inferred from key_map, not declared: its results can never claim "
                     "contract-proven completeness")
        if c.mapping == "aggregate":
            spots.append("aggregation is not reconciled by contract until a contract declares mapping: aggregate "
                         "(today's group-level hop checks still run)")
    if c.mapping in UNSUPPORTED:
        spots.append(f"{c.mapping} cannot be judged: one upstream record to several records here "
                     "(unsupported_mapping)")
    elif c.mapping in LATER:
        spots.append(f"{c.mapping} is not judged until build step {LATER[c.mapping]}")
    if c.mapping == "union_partition":
        spots.append("attribution (each row here owned by exactly one source) is not proven until build step 5")
    if c.mapping == "aggregate":
        spots.append("contribution unavailable: judged at group level only")
        spots.append("HOP_LEDGER does not reconcile aggregate hops yet: unsupported_mapping (group-level hop "
                     "checks still run)")
    if c.mapping not in ("aggregate", *UNSUPPORTED):
        if c.deletion is None:
            spots.append("deletions not modelled: a deleted record is judged like any other version")
    if hop.legacy_exclusion is not None:
        spots.append("exclusion from hop_exclusions: no id, version, approval or volume guard (ADR-04)")
    if c.mapping in LEDGER_KINDS and c.rejects is None:
        spots.append("no rejects evidence: a record the pipeline rejected counts as missing"
                     + (" (the reject policy cannot be applied)" if c.reject_policy else ""))
    if c.mapping in LEDGER_KINDS:
        spots.append("values compared: owned_columns only (identity transforms); other columns are not compared")
    for ds_id in (hop.upstream, hop.dataset):
        ds = config.datasets.get(ds_id)
        if ds is None:
            continue
        if ds.grain.history is None:
            spots.append(f"as-of replay unavailable: {ds_id} does not declare grain.history")
        elif ds.grain.history == "mutable":
            spots.append(f"as-of replay unavailable: {ds_id} is mutable")
    return spots


def contract_report(config: Config) -> list[str]:
    """`dre validate`'s account of every hop: its kind, where the kind comes from, its blind spots."""
    lines = []
    found = hops(config)
    if found:
        lines.append(f"contracts: {len(found)} hop(s)")
    for hop in found:
        source = "declared" if hop.declared else "inferred from key_map"
        lines.append(f"  {hop.label}: {hop.contract.mapping} ({source})")
        lines.extend(f"    blind spot: {spot}" for spot in blind_spots(config, hop))
    return lines
