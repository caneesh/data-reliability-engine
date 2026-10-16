# Design: reconciliation contracts (release 5, proposed)

Status: **superseded** on 2026-10-16 by `docs/design/reconciliation-model.md` and `docs/design/target-architecture.md`, after the Phase 1 architecture assessment found this ledger inconsistent (pending and exclusions inside the identity, a tolerance that could cover unassessable keys) and the chain unbuildable from stored key events (they hold failing keys only). Kept for the record. Originally: proposed on 2026-10-16, not built. It answers the question "does a passing check establish that every expected business record reached gold?", and today the honest answer is: only for what the hop contract names. This plan makes the contract name everything, and makes the engine prove the ledger balances.

## What a PASS proves today, and what it does not

A hop (one dataset and one upstream in its `key_map`) already carries a contract: `feed_filter` on each side, `key_map` with `key_normalise`, `hop_exclusions` (rows a business rule keeps out), `winner_rule` (how versions are chosen), `owned_columns` (values that must agree), `key_unique`, and derived datasets for aggregates and joins. HOP_KEY_CURRENCY judges every upstream key past its SLA and reports what it left out (excluded, pending). So a PASS proves: every upstream key the contract expects, past its deadline, has its latest version downstream, and the owned values agree.

It does not prove four things:

1. **Every exclusion is named.** `hop_exclusions` is one SQL condition per upstream. A row kept out by a second rule the condition does not cover is counted as MISSING, which is right, but a row kept out by an over-broad condition is silently fine. Nothing says which rule excluded how many.
2. **Rejects and deletions are dispositions, not causes.** A record the job rejected, or a source deletion (a tombstone that must be absent downstream), has no place in the contract. The first shows up as MISSING with a cause line; the second shows up as MISSING, wrongly, unless an exclusion happens to cover it.
3. **The ledger is never asserted.** The hop reports counts, but no check states the identity "expected = delivered + each named disposition" and fails when a key is in no bucket or in two.
4. **The chain is not composed.** Each hop is judged alone. A record can be CURRENT at every hop and still never reach gold if one hop's exclusion and the next hop's expectation disagree. Nothing follows a source key through the chain and names where it stopped, except `dre trace` for one key at a time.

Transformations and multi-source targets have partial answers: a transformed column can be compared only through a derived dataset, and a gold table fed by several sources relies on each dataset's `feed_filter` to attribute rows, with nothing checking that the attributions are disjoint and complete.

## The contract

One block per upstream on the downstream dataset. Every field is optional; a missing field means "none", and the existing fields (`hop_exclusions`, `winner_rule`, `owned_columns`) keep working as aliases of the entries they map to, so no configuration breaks.

```yaml
reconciliation:
  example_curated_enrollment:            # the upstream, as in key_map
    expected: "status <> 'DRAFT'"        # upstream rows this hop must carry (SQL over the upstream; default: all)
    exclusions:                          # rows legitimately not carried, each named (today: hop_exclusions)
      - { id: drop_test_members, condition: "status = 'TEST'", code_ref: "gcf_build.scala:212" }
      - { id: drop_future_effective, condition: "effectivedate > current_date()", code_ref: "gcf_build.scala:240" }
    deletions: "op_code = 'D'"           # upstream rows whose absence downstream is correct (a tombstone)
    rejects: { table: ops_db.dq_rejects, condition: "reason_code IS NOT NULL" }   # today: probes.rejects
    dedup:                               # several upstream rows become one downstream row
      winner: { order_by: ["sourcelastupdatets DESC", "enddate DESC"], formats: { enddate: yyyy-MM-dd } }
      cardinality: many_to_one           # one_to_one | many_to_one | one_to_many (one_to_many: deferred)
    transforms:                          # column mappings compared by value agreement (today: owned_columns, equality only)
      - { from: enddate, to: mbr_mbrshp_covrg_end_dt }
      - { from: "coalesce(termdate, '9999-12-31')", to: mbr_mbrshp_covrg_term_dt }
    attribution: "src_sys_nm = 'SRC_A'"  # for a target fed by several sources: the target rows this hop owns (today: feed_filter)
    tolerance: { missing_keys: 0 }       # a ledger may be short by this many keys and still pass (default 0)
```

Rules:

- **Exclusions have ids and code references**, so the digest can say `excluded: drop_test_members 3 (gcf_build.scala:212)`, and a changed condition is a new id.
- **Every condition is SQL over the upstream's columns** and runs only inside the engine's queries (spec section 9, rule 10). A rule that cannot be written as SQL is not an exclusion; its rows are rejects, and the job's rejects table explains them.
- **Attribution predicates of the hops into one target must be disjoint and together cover the target.** The engine checks it (below) rather than trusting it.
- **A derived upstream keeps its grain.** The contract's `expected`, `exclusions` and `deletions` apply to the derived relation's rows; `dedup` does not apply (the relation has one row per group).

## The ledger

For each hop and window, the engine classifies every expected upstream key past its SLA into exactly one disposition, and asserts the identity:

```
expected = delivered + stale + unassessable + missing
         + excluded[id_1] + ... + excluded[id_n] + deleted + rejected + pending
```

| Disposition | Meaning | Evidence |
| --- | --- | --- |
| delivered | The key's latest version is downstream (CURRENT) and its transforms agree | HOP_KEY_CURRENCY, HOP_VALUE_AGREEMENT |
| stale | Downstream holds an older version | HOP_KEY_CURRENCY |
| unassessable | No usable record time on one side | HOP_KEY_CURRENCY |
| missing | Absent downstream and in no other bucket: the unexplained remainder | HOP_KEY_CURRENCY after the other buckets |
| excluded[id] | Matches one named exclusion; absent downstream | The exclusion's condition, re-applied to the upstream row |
| deleted | Matches `deletions`; absent downstream | The deletions condition |
| rejected | Present in the rejects table for this key; absent downstream | The rejects table, by key |
| pending | Younger than the SLA; not judged | Load time |

A key that matches an exclusion *and* is present downstream is a violation of its own kind (`excluded_but_present`): the contract says the row should not be there. A key in two exclusions is counted under the first and reported, since the ids should not overlap. The ledger PASSES when `stale + unassessable + missing + excluded_but_present ≤ tolerance` and every key landed in exactly one bucket; it FAILS otherwise, naming the buckets in `detail` with counts, and the `observed` text is the ledger line. Every ledger is appended to a new store table, `dq_reconciliation` (hop, window, disposition, count, run id), so the dispositions trend over time and an auditor can read the accounting for any window.

## The chain

A feed's datasets form a chain through their `key_map` entries. CHAIN_COMPLETENESS composes the per-hop ledgers for a window: a source key *reached gold* when it is delivered at every hop; it is *accounted for* when it is delivered up to one hop and excluded, deleted or rejected there; otherwise it is *lost*, and the first hop where it stopped is named. Keys hash the same at every hop when the mapped key columns are one to one (the HMAC is over the normalised key string), so the composition is a join of `dq_key_event` rows across hops for the window; where the grain changes (a derived upstream), the chain runs in segments and the group's disposition carries the segment. One result per chain per window: population = source keys past the chain's total SLA; violations = lost keys; `detail` = lost keys per first-stop hop and disposition, up to 100. This is the statement "all expected business records reached gold" that a single hop cannot make.

## Multi-source targets

T1_SOURCE_ATTRIBUTION runs on a target with more than one hop into it: every target row must match exactly one hop's `attribution`. Rows matching none are unattributed (nobody's ledger would count them); rows matching two are double-counted. Both are violations, listed by predicate. Each hop's ledger then reads the target through its own attribution predicate, which is what `feed_filter` on a per-source dataset does today, now checked for completeness.

## Transformations, deduplication, merges

- **Transformations.** A `transforms` entry with an expression on the upstream side is compared by HOP_VALUE_AGREEMENT as the latest version's value set, like `owned_columns` today; the expression is SQL over the upstream row, parsed by `dre dry-run`. An aggregate or a join stays a derived dataset: the contract names the derived dataset as the upstream and the ledger runs at the group grain.
- **Deduplication.** `dedup.cardinality: many_to_one` with `winner` asserts three things the ledger already carries: one downstream row per key (`key_unique`, max_rows_per_key), the winner is the version present (HOP_KEY_CURRENCY's CURRENT with ties explained by TIE_RESOLVED_BY_RULE), and no key is lost to the dedup (every upstream key is in a bucket). A dedup that drops a whole key is `missing`, never silent.
- **Merges.** A merge into gold is `cardinality: many_to_one` plus `deletions` for tombstones; the ledger proves every expected key's latest version is present and every deleted key is absent. Inserted-versus-updated stays unobservable from outside (`docs/design/outside-in-evidence.md`), and is not needed for the ledger to balance.

## What stays unexplained, and how it is reported

The engine cannot know why a job dropped a record. The ledger never absorbs that: a key in no bucket is `missing` and the hop FAILS, with the cause engine's line beside it (ruled out, not ready, suspected). If the team's audit writes a rejects table (`docs/evidence-contract.md`), those keys move to `rejected` with their reason code and the hop can PASS; without it, they stay `missing`. A PASS therefore means every expected key is delivered or sits in a bucket the contract named and the evidence confirmed.

## Steps

1. **Contract schema and validation.** The `reconciliation` block in `config/models.py`; `hop_exclusions`, `winner_rule` and `owned_columns` map into it (aliases, with a deprecation note, no behaviour change); `dre validate` checks ids are unique, conditions parse (`dre dry-run`), and attribution predicates exist on every hop into a shared target. Spec section 3.
2. **HOP_LEDGER and `dq_reconciliation`.** The ledger as a check beside HOP_KEY_CURRENCY (same candidate keys, one more pass that buckets them), the identity assertion, `excluded_but_present`, the digest line, the store table and its view. Spec sections 5, 6, 8.
3. **Transforms with expressions.** `owned_columns` accepts an upstream-side expression; HOP_VALUE_AGREEMENT renders it. Spec section 6.
4. **T1_SOURCE_ATTRIBUTION.** Disjoint and complete attribution on multi-hop targets. Spec section 6.
5. **CHAIN_COMPLETENESS.** The composed ledger per feed and window, from `dq_key_event` across hops. Spec sections 4 (chain SLA) and 6.
6. **Documentation.** `docs/architecture.md` ("Transformation-aware reconciliation" becomes the ledger), `docs/extending.md`, the feature catalogue.

Release 4's remaining steps (file record count, sequence gap, row parity, cardinality) are independent of this plan; `cardinality` here reuses release 4's HOP_CARDINALITY when it exists. Recommended order: finish release 4 steps 2 and 3 (they close in-file evidence gaps), then this plan's steps 1 and 2, which give the ledger, then the rest.

## Decide before step 1

| Decision | Proposal | Who confirms |
| --- | --- | --- |
| Aliases or migration | Keep `hop_exclusions`, `winner_rule` and `owned_columns` working as aliases; `dre validate` prints the contract form once | User |
| Where deletions are expressed in the real pipeline | An operation code on the CDC or incremental layer; until confirmed the `deletions` field stays unset and tombstones are MISSING | Developer (a new question) |
| Ledger grain for derived upstreams | Groups, with the derived relation's rows as the expected set | User |
| Tolerance semantics | `missing_keys` as an absolute count per hop per window, default 0; no percentage, because a percentage hides small losses on large loads | User |

## What this plan does not do, and why

- **No in-job hooks.** Every disposition is proven from tables, the rejects table the audit writes, and the engine's own key events.
- **No one-to-many hops.** A row exploded into several target rows needs a wider target key than `key_map` expresses; still deferred.
- **No inferred exclusions.** The engine never guesses that a missing key "was probably filtered": a rule the contract does not name is a loss.
- **No change to result states or the writer.** The ledger is a check with PASSED, FAILED and DID_NOT_RUN; `dq_reconciliation` is appended through the one writer.
