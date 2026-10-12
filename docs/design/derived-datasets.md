# Design: derived datasets (join and aggregate hops)

Agreed 2026-10-12 and built the same day. The contract is specified in `docs/spec.md` section 3 ("Derived dataset") and section 6 ("Derived upstreams"); this page keeps the reasoning.

## Problem

A hop was key-preserving: `key_map` maps this dataset's key columns to one upstream's key columns, and the hop checks compare one upstream key to one key here. That breaks when the target is at a different grain: one gold row per (member, month) summed from many claim rows, or gold rows that exist only where a lookup matched.

## Decision

One new concept, no new checks: a **derived dataset**, a structured query over table datasets that the engine renders as a subquery wherever an upstream's table would be read. Its `key` is the comparison grain. Everything downstream (hop checks, key events, `hop_exclusions`, the cause engine, `dre trace`) works unchanged because they only ever see the upstream through `p.up_table`.

Why this shape:

- **Grain is explicit.** The derived dataset's key is the grain the review asked contracts to declare.
- **Lateness falls out.** A group's load time is the latest of its rows, so a month still receiving claims is pending until it has been quiet for `sla_hours`; half-built groups are never judged.
- **Join intent is declared.** `type: inner` means unmatched rows are legitimately excluded; `type: left` means they are expected, and the new builtin cause JOIN\_UNMATCHED confirms when the lookup had no match.
- **Structured, not free SQL.** Expressions are the same validated fragments as `feed_filter`; the engine builds the SELECT, so the identifier rules in CLAUDE.md still hold. Tables are referenced by dataset id, which keeps expressions readable and avoids ambiguity between joined columns.
- **No code per source.** The contract is YAML; the engine has one template (`checks/sql/derived_relation.sql.j2`) and one module (`checks/derived.py`).

## Choices made

| Question | Choice | Reason |
| --- | --- | --- |
| Structured contract or a free SELECT in YAML | Structured | Validatable; no way to smuggle statements; readable |
| Derived datasets as downstreams too | Upstream only | Keeps planning and settings simple; add when a real case needs count-against-count reconciliation |
| Measure tolerance | Exact, as strings | Config casts sums to the target's DECIMAL scale; add a tolerance only if a real table needs one |
| Nested derived datasets | Not allowed | One level covers aggregate and join; nesting would hide cost |
| Record and load time of a derived dataset | Fixed: `record_ts`, `load_ts` (UTC) from `from` | They define the deadline semantics; letting config override them invites wrong windows |
| Inner-join drops | Not counted as excluded (yet) | Counting them needs a second pass over the relation; left for when the digest needs the number |

## Cost

The relation aggregates the whole `from` table (and its joins) each time a hop check or cause check reads it, within the check's compute budget. This is the same full scan the key-preserving hops already make.

## Tests

- `tests/checks/test_derived.py`: the relation at its grain; a group missing, a measure disagreeing, a group pending while rows still arrive; left join unmatched (JOIN\_UNMATCHED confirmed), inner join (not expected).
- `tests/config/test_derived_config.py`: defaults, table-or-derived, each validation error.
- `tests/replay/test_r13_derived_upstream.py`: `dre run` and `dre trace` end to end.
