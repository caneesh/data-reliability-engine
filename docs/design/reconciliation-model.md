# Design: the reconciliation model (Phase 1, proposed)

Status: proposed on 2026-10-16 in answer to the Phase 1 foundational architecture assessment (findings P1-01 to P1-08, P1-11). Nothing here is built. It supersedes `docs/design/reconciliation-contracts.md`, whose ledger the assessment found inconsistent (P1-04). `docs/design/target-architecture.md` holds the decisions (ADR-01 to ADR-08) this model depends on. All names below are synthetic.

## 1. What a reconciliation result claims

A reconciliation result makes one claim, and only that claim:

> For **edge** E under **contract version** V, every record of the **due population** at the **target grain**, as of the **evidence cutoff** C, is in exactly one **disposition**. Every disposition is authorised by V and supported by evidence at or above the edge's **required level**.

A PASS says nothing outside that scope. In particular:

- **Not about other edges.** A PASS on one edge says nothing about another edge or about the chain (section 7).
- **Not about records outside the population.** It says nothing about records the population did not include: not loaded upstream, not due, or outside the window.
- **Not historical.** It says nothing about an earlier point in time unless the mode is AS_OF (section 8).

Every result stores its scope (ADR-01): edge, contract version hash, population definition, grain, cutoff, mode, evidence level and coverage. The digest prints the scope in one line beside the state.

## 2. Populations

For edge E = U → D (upstream U, downstream D), window W and cutoff C:

| Population | Definition |
| --- | --- |
| **Candidate** `K_cand` | U records, at E's **source grain** (section 4), whose U load time falls in W and is at or before C. A record's version under judgement is its latest version at U as of C, chosen by U's version rule. |
| **Due** `K_due` | Candidates whose deadline (U load time of the version under judgement + E's SLA) is at or before C |
| **Pending** `K_pend` | `K_cand \ K_due`: not judged in this evaluation. They are judged in the window where their deadline falls. |

`K_cand = K_due ⊎ K_pend`. Pending records are reported beside the ledger and are **not** part of it. This fixes the old equation, which put pending on the right-hand side of an identity whose left side was "expected keys past the SLA" (P1-04).

A record with no usable U load time cannot be placed in a window. It is counted as `untimed` in the candidate scan, reported, and judged by the weekly full sweep, not lost silently.

## 3. Dispositions and the identity

Every due record lands in **exactly one** disposition. The dispositions are applied in a fixed precedence (first match wins). Every rule that matched after the first is recorded as an overlap diagnostic.

| # | Disposition | Class | Rule (all evaluated on the version under judgement) | Evidence |
| --- | --- | --- | --- | --- |
| 1 | `unassessable` | unknown | The version cannot be determined on either side (no usable record time, unparseable key), or the normalised key collides with another raw key (section 4) | observed |
| 2 | `deleted` | authorised | The version is a tombstone under the contract's `deletion` rule, and D shows the delete (hard: absent; soft: the configured flag or end date) | observed + reconstructed |
| 3 | `deleted_not_applied` | breach | A tombstone, and D still shows the record as live | observed + reconstructed |
| 4 | `excluded[rule@ver]` | authorised | Matches an **approved** exclusion rule, effective at the record's U load time, and absent from D | reconstructed |
| 5 | `excluded_but_present` | breach | Matches an approved exclusion, and present in D | reconstructed + observed |
| 6 | `rejected[reason]` | authorised or defect | The rejects evidence names this key and version with a reason. The contract's reject policy classes the reason (section 5) | reported |
| 7 | `superseded[source]` | authorised | Multi-source survivorship only: another contributor owns the target record by precedence, and that contributor's edge delivered it (section 6.3) | observed |
| 8 | `delivered` | ok | D holds this version, and the mapped values agree (transforms, section 4) | observed |
| 9 | `divergent` | deficit | D holds this version, and a mapped value disagrees | observed |
| 10 | `stale` | deficit | D holds an older version | observed |
| 11 | `missing` | deficit | None of the above: the unexplained remainder | observed |

**Identity:**

```
|K_due| = unassessable + deleted + deleted_not_applied + Σ_r excluded[r] + excluded_but_present
        + Σ_reason rejected[reason] + Σ_s superseded[s] + delivered + divergent + stale + missing
```

The engine computes each disposition as a set and asserts the identity. If a record is in no bucket, or the bucket sum differs from `|K_due|`, the result is DID_NOT_RUN / `ledger_unbalanced`, an engine defect. It never becomes PASSED or FAILED.

**Outcome:**

- **DID_NOT_RUN** when any of these holds:
  - `|K_due| = 0`: `empty_population`.
  - The edge's mapping is unsupported: `unsupported_mapping` (ADR-02).
  - The evidence the contract requires is unavailable: `insufficient_evidence`, naming the missing evidence.
  - The ledger does not balance: `ledger_unbalanced`.
- **FAILED** when any of these holds:
  - Any record is in a **breach** class.
  - Any record is in a **defect** reject reason.
  - Any record is `unassessable`. Unknown is never success, and the state is kept as it is today: a violation with no cause.
  - A deficit class exceeds its tolerance.
  - An authorised class exceeds its volume guard (section 5).
- **PASSED** otherwise.

**The target side.** The identity accounts for upstream records. A downstream record that no upstream record maps to is an `orphan`: for example a substituted key, or a row written by another process. Orphans are counted over D's records loaded in W (through the attribution predicate on a multi-source target) and reported beside the ledger. They are a breach class: they fail the result, but they are not part of the identity, because they are not in `K_due`.

**Tolerance applies only to named deficits** (P1-04). `tolerance: { missing: 0, stale: 0, divergent: 0 }` is the default and the only form allowed. `unassessable`, breaches and defect rejects can never be tolerated, and `dre validate` rejects a tolerance on them. A tolerance is an absolute count per edge per window.

`violations` = deficits + breaches + defect rejects + unassessable. `observed` is the ledger line, for example: `due 7 (pending 1): delivered 2, excluded drop_test_members@2 1, deleted 1, stale 1, missing 1, rejected PARSE_ERROR (defect) 1`.

## 4. Grain, identity, versions and mappings (ADR-02)

Each dataset declares its grain:

```yaml
grain:
  history: append_only            # append_only | mutable | snapshot_partitioned (section 8)
```

As built (step 1): the grain's key is the dataset's `key`, and its version is the dataset's `record_time`, with ties ordered by a hop's `winner_rule`. Neither is repeated under `grain`. Each edge's contract sits under the dataset's `contracts`, keyed by upstream (spec section 3).

Each edge declares exactly one **mapping kind**. An edge without a supported kind is DID_NOT_RUN / `unsupported_mapping` (CONFIGURATION) on every run. It is never passed, and `dre validate` lists it as a blind spot.

| Kind | Meaning | Target grain | Lineage | Status |
| --- | --- | --- | --- | --- |
| `identity` | One U record ↔ one D record, key mapped and normalised | U key | Key equality after `key_map` and `key_normalise`. A normaliser can collapse distinct keys (`strip_leading_zeros` makes `007` and `07` one key), and nothing checks this today. The ledger detects collisions at run time: distinct raw keys on one side that share a normalised key are `unassessable` | Built (`key_map`); collision detection proposed |
| `dedup` | Many U versions → the winning version per key in D | U key | Key equality. The winner comes from `version` | Built (`winner_rule`) |
| `merge` | `dedup` into a mutable target, with tombstones | U key | Key equality; `deletion` rule | Partly built: deletions missing |
| `union_partition` | D's rows partitioned among several U by an attribution predicate | U key within the predicate | Key equality inside the predicate. T1_SOURCE_ATTRIBUTION proves the predicates are disjoint and complete | Proposed |
| `join_contribution` | D's row is built from one **primary** U, which owns the population, plus **contributor** U's joined by a reference | Primary's key | Primary: key equality. Contributor: reference resolution | Proposed |
| `survivorship` | Several U may hold the same business record; D keeps one by precedence | Shared key | Key equality plus precedence | Proposed |
| `aggregate` | Many U records → one D group, non-injective | Group key g(U) | Reconstructed: g is SQL over U's columns, so every U record's group is computed, not guessed | Built at group level (derived datasets) |
| `fan_out` | One U record → many D records | U key + discriminator | Requires `fan_out.child_key` and an expected-children rule over U | **Unsupported**: DID_NOT_RUN / `unsupported_mapping` |

**Transforms.** A mapped value is declared as `{ from: <SQL over U>, to: <D column> }`. Agreement is checked in one direction, f(U) = D. A non-injective f (truncation, a code lookup, coalesce) can never be inverted: the engine never infers U from D, and `dre dry-run` parses f. A transform that cannot be written as SQL over one U row cannot be compared. Its D column is listed under `unverified_columns`, which goes into the result's scope, so the claim does not cover it.

**Versions, out-of-order and late data.** The version under judgement is the latest by `version.order_by`, never by load time. A late-arriving older version does not change it. An out-of-order newer version does, at the run where its deadline passes. The expected D state is therefore deterministic for any cutoff: the latest version as of C, unless it is a tombstone. In that case it is the deletion state.

## 5. Exclusions, rejects and deletions (ADR-04)

**Exclusions** are approved, versioned rules. A SQL condition alone is not enough (P1-05):

```yaml
exclusions:
  - id: drop_test_members
    version: 2
    condition: "status = 'TEST'"
    effective: { from: 2026-09-01 }          # midnight in defaults.yaml timezone; `to` is exclusive; judged against the record's U load time
    approved_by: CHG-0000                    # placeholder for the change record that approved it
    code_ref: "gcf_build.scala:212"          # placeholder
    max_share: 0.05                          # volume guard: more than 5% of due records excluded is FAILED
```

- A rule outside its effective dates does not match. Changing a condition means a new version.
- `max_share` guards against an over-broad condition. Without a guard, an exclusion that swallowed half a load would be "authorised".
- The pipeline's own filter registry (`docs/evidence-contract.md`) is **reported** evidence. It never authorises an exclusion. A record absent from D because of a rule the pipeline reports but the contract does not approve stays `missing`, and the cause line names the reported rule ("filtered by unapproved rule drop_inactive, reported").

**Rejects** need lineage and a policy:

- The rejects evidence must give the source key (or its HMAC), the version or batch, the reason code, the rule id and the rule version. A reject row without key lineage cannot account for any record: the record stays `missing`, and the reject count appears only as a cause line.
- `reject_policy` classes each reason code as `authorised` or `defect`. **The default is defect**, and an unknown code is a defect. For example: `{ INVALID_CODE: { disposition: authorised, max_share: 0.02 }, PARSE_ERROR: { disposition: defect } }`.
- Authorised reasons carry a `max_share` like exclusions.
- So a reject never automatically counts as success.

**Deletions:**

```yaml
deletion:
  tombstone: "to_date(enddate, 'yyyy-MM-dd') = to_date(effectivedate, 'yyyy-MM-dd')"   # SQL over U
  applied_as: soft                                # or hard (absent in D)
  soft_when: "to_date(mbr_mbrshp_covrg_end_dt, 'yyyy-MM-dd') = to_date(mbr_mbrshp_covrg_eff_dt, 'yyyy-MM-dd')"  # SQL over D
```

Dates held as strings are parsed in the condition itself (`to_date`), so they are never compared as strings.

**How the member pipeline expresses a deletion (developer, 2026-10-16; handoff question 18).**
- **There is no delete operation.** A coverage is voided by a new version whose end date equals its effective date.
- **The record stays, at every layer.** Its key (which includes the effective date) does not change, and gold keeps the row with the voided end date. So `applied_as` is `soft`, and the expected D state is "present, with end date equal to effective date".
- **Today's engine already catches an unapplied void.** The void is a newer version, so gold still holding the old end date is STALE (HOP_KEY_CURRENCY), or a disagreement on the owned end-date column (HOP_VALUE_AGREEMENT). The model adds only the label: a void that arrived is counted as `deleted`, not lost among `delivered`, and a void not applied is `deleted_not_applied`.
- **The gold rule `end_not_before_start` lets a void pass.** It fails only when the end date is *before* the effective date.

"Usually" was the developer's word, so other void shapes may exist (for example an end date before the effective date). Until they are confirmed, `tombstone` covers equality only. Any other shape is judged as an ordinary version, and the gold rule reports an end date before the effective date.

Hard deletes (a record absent from D) are not used by this pipeline. `applied_as: hard` stays in the model for other feeds.

## 6. Multi-source targets (ADR-03)

Three shapes, never mixed on one edge.

**6.1 Union partition.** Each target row belongs to exactly one contributor by an attribution predicate on D. T1_SOURCE_ATTRIBUTION (one result per target per window) proves the predicates are disjoint and complete. Each edge's ledger then reads D only through its predicate, so nothing is counted twice.

**6.2 Join contribution.** One primary edge owns the population; only it has a completeness ledger. A contributor edge has a different check, HOP_CONTRIBUTION:

- Each D row's reference to the contributor resolves to a contributor record whose version was current at the D row's load time. A row that does not resolve is `unresolved`, a deficit; the cause is JOIN_UNMATCHED.
- The contributed columns agree with that version.

A D row is therefore counted once, by its primary, and its contributors are checked for correctness, not completeness.

**6.3 Survivorship.** Several sources may hold the same business record, and D keeps one by a declared precedence (`precedence: [SRC_A, SRC_B]`). For a key held by both:

- **SRC_A's edge** judges it normally.
- **SRC_B's edge** classes it `superseded[SRC_A]`, but only if SRC_A's edge, in the same evaluation, has it `delivered`. Otherwise SRC_B's edge judges it normally, because the survivor is missing too.

A target row is counted once: by the edge whose source won.

**Not used by the member pipeline (developer, 2026-10-16).** All sources have equal priority. A member with both group and retail coverage appears in every table associated with each coverage, as separate records whose key includes the coverage (and, in a table shared across sources, the source system). No source's record replaces another's. Every multi-source gold table is therefore a **union partition** (6.1) by source, and `superseded` never occurs. Survivorship stays in the model, unconfigured, for other feeds.

## 7. Lineage and the chain

**Record-level lineage** is the path a source record takes through the edges. Its kind decides what can be claimed:

| Edge kind | What lineage is | Level |
| --- | --- | --- |
| identity, dedup, merge, union_partition, survivorship | Key equality after the injective mapping | observed |
| join_contribution (contributor side) | Reference resolution | observed |
| aggregate | The group g(record), computed from U's columns | reconstructed for membership; contribution is **unavailable** unless D, or a table between them, carries member identity |
| fan_out | Not supported | unavailable |

**CHAIN_COMPLETENESS** (one result per chain per source window) follows each due source record along the edges to the declared terminal dataset. Each source record ends in one of four outcomes:

- **reached**: delivered at every edge.
- **accounted**: delivered up to an edge, then in an authorised disposition there.
- **lost**: in a deficit, breach or defect class at some edge. The result names that edge as the **first observed gap**, not as the origin (assessment invariant 6).
- **unobservable**: the path crosses an edge whose lineage level is below `observed`/`reconstructed` for the claim.

Outcome:

- **FAILED** if any record is lost (a loss proven on the observable segments is still a loss).
- Otherwise **DID_NOT_RUN / `partial_chain`** if any record is unobservable. `detail` names the edge and the segment that was proven.
- Otherwise **PASSED**.

A chain PASS is the only result that may be read as "every expected business record reached gold", for that chain, window, cutoff and contract version.

**Chain due population.** These are the source records whose chain deadline (the sum of the edge SLAs) has passed. A record that is pending at any edge is pending for the chain.

**How it is computed.** It is not computed from stored key events: today `dq_key_event` holds only failing keys, so a chain cannot be composed from it. The chain query reads the layers directly, one edge at a time over the due source keys (bounded by the window), and applies each edge's contract. It stores:

- the outcome counts in `dq_reconciliation`;
- **exception keys only** (lost, unobservable, breach) as key events.

`dre trace` gives one record's full path on demand.

## 8. Time (ADR-05)

| Time | Meaning |
| --- | --- |
| Record time | The version: `grain.version` |
| Load time | When the record arrived at a layer (`load_time`) |
| First seen | When DRE first saw a file (`dq_file`) |
| Deadline | U load time + the edge's SLA |
| Evidence cutoff C | The instant the evaluation reads as. NORMAL: the run's start |

These are never compared with each other, except for the deadline against C.

**Evaluation modes** (P1-03):

| Mode | Reads | May claim | Allowed when |
| --- | --- | --- | --- |
| NORMAL | Current data at C = run start | "As observed at C". The stored NORMAL results are DRE's own historical record, and they are append-only | Always |
| CURRENT_RECHECK (today's RERUN and REPLAY) | Current data now, for a past window | "This window, as the data stands now". Never a historical PASS. The digest and the CLI print "current recheck" | Always |
| AS_OF_REPLAY (new) | Only rows with load time ≤ C, on every dataset the edge reads | "As the data stood at C" | Both datasets of every edge are `history: append_only` (or `snapshot_partitioned` with a snapshot at or before C), **and** T1_PARTITION_DRIFT reports no drift on them between C and now. Otherwise DID_NOT_RUN / `no_as_of_evidence` |

A `mutable` dataset, such as a merged gold main table, can never be replayed as of a past cutoff: rows merged over are gone. For a past window, the historical evidence about such a dataset is the NORMAL result stored at the time.

## 9. Worked examples

All keys and values are synthetic. Window W, cutoff C. SLA 2 hours on every edge.

### 9.1 Raw → CDC → Gold, with an update, a tombstone, an exclusion, a reject and a late record

**Raw** (`append_only`, file f1):

| Key | Versions in raw | Note |
| --- | --- | --- |
| K1 | v1 | |
| K2 | v1, v2 | update |
| K3 | v1 | status TEST |
| K4 | v1 | |
| K5 | v1 | loaded 30 minutes before C: pending |
| K6 | v1 | unparseable date |
| K7 | v1, v2 (op_code D) | tombstone |

**Edge raw → cdc** (`dedup`, deletion `hard` at gold, not here: CDC keeps tombstones as rows). CDC holds K1 v1, K2 v2, K3 v1, K4 v1, K7 v2 (D). The rejects table names K6 v1, PARSE_ERROR.

| Disposition | Keys |
| --- | --- |
| pending (outside the ledger) | K5 |
| delivered | K1, K2@v2, K3, K4, K7@v2 |
| rejected PARSE_ERROR (defect) | K6 |

Due = 6 = 5 + 1. **FAILED** (1 violation: a defect reject).

**Edge cdc → gold** (`merge`, exclusion `drop_test_members@2`, deletion `hard`). Gold holds K1 v1 and K2 v1, and nothing else.

| Disposition | Keys |
| --- | --- |
| deleted | K7 (tombstone, absent) |
| excluded drop_test_members@2 | K3 (TEST, absent) |
| delivered | K1 |
| stale | K2 (gold has v1, CDC v2) |
| missing | K4 |

Due = 5 = 1 + 1 + 1 + 1 + 1. **FAILED** (2 violations).

**Chain raw → gold** (due source records K1 to K4, K6, K7; K5 is pending):

| Outcome | Records | First observed gap |
| --- | --- | --- |
| reached | K1 | |
| accounted | K3 (excluded at cdc → gold), K7 (deleted at cdc → gold) | |
| lost | K2 (stale), K4 (missing) | cdc → gold |
| lost | K6 (defect reject) | raw → cdc |

**FAILED**, 3 lost. Counts alone would have hidden every one of these: raw has 7 keys and gold 2, and 7 − 2 = 5 is "explained" by any story.

### 9.2 Several sources into one target

**Union partition.** `gold_db.provider_directory` is fed by SRC_A and SRC_B, with attribution `src_sys_nm = 'SRC_A'` and `src_sys_nm = 'SRC_B'`. The target holds 100 rows: 60 SRC_A, 38 SRC_B, 1 with `src_sys_nm` NULL and 1 'SRC_C'. T1_SOURCE_ATTRIBUTION is **FAILED**, with 2 unattributed rows listed by predicate. Each edge's ledger reads only its own 60 or 38 rows, so no row is counted twice, and the 2 unattributed rows are reported, not ignored.

**Join contribution.** A gold coverage row is built from enrollment (primary) and provider (contributor, by `provider_id`). The enrollment edge owns the population: 50 due, 50 delivered, **PASSED**. The provider edge runs HOP_CONTRIBUTION: 49 rows resolve to a current provider version and agree on `provider_nm`; 1 row's `provider_id` resolves to nothing, so it is `unresolved` with cause JOIN_UNMATCHED. **FAILED**, 1 violation. The row is still counted once, by the enrollment edge.

**Survivorship.** Member M is in SRC_A and SRC_B, with precedence [SRC_A, SRC_B]. Gold holds SRC_A's version.
- SRC_A's edge: M is `delivered`.
- SRC_B's edge: M is `superseded[SRC_A]`.
- If gold held SRC_B's version instead, SRC_A's edge would be `stale` or `divergent` for M, and SRC_B's would not count it as superseded.

### 9.3 Aggregation

No gold table in the member pipeline holds counts or totals (developer, 2026-10-16), so the `aggregate` kind is not configured for it today. The example stays because the assessment's proof scenario 5 requires it, and other feeds may need it.

Gold holds members per plan per month: the group g = (`qualifiedhealthplanid`, month of `effectivedate`), with measure `member_count`. The edge is `aggregate`; the expected groups and measures are computed from the curated rows by g.

| Group | Members in curated | `member_count` in gold | Disposition |
| --- | --- | --- | --- |
| (P1, 2026-09) | 120 | 120 | delivered (group level) |
| (P2, 2026-09) | 45 | 44 | divergent |
| (P3, 2026-09) | 10 | (no row) | missing |

The group ledger is **FAILED**, 2 violations.

**Contribution level.** Each member's group is reconstructed (g is SQL over curated). But gold carries only a count, so a member dropped while another is counted twice leaves `member_count` unchanged. The edge therefore declares `contribution: unavailable`, and the scope of the PASS for (P1, 2026-09) says "group level". A chain through this edge is DID_NOT_RUN / `partial_chain` unless it finds a loss. The visibility limit is named, not passed (proof scenario 5).

Contribution can be proven only if a table at member grain sits between the two (for example a gold detail table the aggregate is built from). Then two edges are reconciled: curated → detail (`identity`, record level) and detail → aggregate (`aggregate`, group level, with membership observed). The chain is then observable end to end.

### 9.4 Legitimate and illegitimate exclusions

Due at cdc → gold: 200 records.

| Record set | Contract | In gold | Disposition |
| --- | --- | --- | --- |
| 6 with status TEST, loaded after 2026-09-01 | `drop_test_members@2`, effective from 2026-09-01 | absent | excluded drop_test_members@2 (authorised) |
| 2 with status TEST, loaded before 2026-09-01 | rule not effective | absent | missing |
| 3 inactive | no approved rule; the job's filter registry reports `drop_inactive` | absent | missing; cause line "filtered by unapproved rule drop_inactive (reported)" |
| 1 with status TEST | `drop_test_members@2` | **present** | excluded_but_present (breach) |
| 188 | | present, latest version | delivered |

Due = 200 = 6 + 2 + 3 + 1 + 188. **FAILED**, 6 violations. The exclusion share is 6/200 = 3%, under `max_share` 5%. Had the condition been over-broad and caught 30 records (15%), the result would also be FAILED, by the volume guard.

### 9.5 Record-level lineage, one record

`dre trace` for K4 (by HMAC; the key itself is never printed):

| Edge | Version judged | U load time | Deadline | Disposition | Evidence |
| --- | --- | --- | --- | --- | --- |
| raw → cdc | v1 | 08:05 | 10:05 | delivered | observed |
| cdc → gold | v1 | 08:20 | 10:20 | missing | observed; causes: NOT_RUN, KEY_MISMATCH, JOIN_UNMATCHED ruled out; FILTERED and REJECTED not ready (no filter registry or rejects table configured) → MERGE_NOT_APPLIED, insufficient evidence (never "confirmed") |

For a record on the aggregate path in 9.3, the trace shows its group (P2, 2026-09), and states "membership reconstructed; contribution unavailable".

## 10. Proof scenarios and their expected results

The ten Phase 1 sign-off scenarios from the assessment, each with the expected result under this model. Each becomes a replay test (R14 onwards; R01 to R13 exist) when the model is built.

| # | Scenario | Expected result |
| --- | --- | --- |
| 1 | Equal counts, with a substituted or missing business key | Edge ledger FAILED: the dropped key is `missing`, and its substitute is an `orphan`. Counts are never the evidence |
| 2 | A filtered record under an approved rule, and one under an unapproved rule | Approved: `excluded[rule@ver]`, PASSED if nothing else. Unapproved: `missing`, FAILED, with the reported rule named in the cause line |
| 3 | A rejected record with a defect reason | `rejected[PARSE_ERROR]` classed defect: FAILED. Never PASSED |
| 4 | Two sources contributing to one gold row | `join_contribution`: counted once by the primary, the contributor checked by HOP_CONTRIBUTION. Or `union_partition`, with T1_SOURCE_ATTRIBUTION proving no overlap |
| 5 | Aggregation of many records into one group | Group ledger at group level. Contribution: reconciled through a member-grain table, or declared `unavailable`, so the chain is `partial_chain` |
| 6 | A CDC update, a tombstone, an out-of-order event, a late arrival | The latest version by `grain.version` as of C. Tombstone: `deleted` or `deleted_not_applied`. An older version arriving late: no change. A newer one out of order: judged at its own deadline. Each case gives one deterministic ledger (example 9.1) |
| 7 | Historical rerun after a later load | CURRENT_RECHECK, labelled as such, and never a historical PASS. AS_OF_REPLAY on a `mutable` dataset: DID_NOT_RUN / `no_as_of_evidence` |
| 8 | Crash between the result write and the key evidence write | No commit row, so the result is invisible in every view. The next run re-evaluates the held window (ADR-07) |
| 9 | A legitimate empty batch against a silent zero-row defect | No slot due by the cadence calendar, or the source's file record count is 0: DID_NOT_RUN / `empty_population`. A slot due with source evidence of records > 0 and zero loaded: T1_ZERO_ROWS FAILED (P1-11; release 4 step 2 supplies the file record count) |
| 10 | An unsupported one-to-many mapping | `fan_out`: DID_NOT_RUN / `unsupported_mapping` on every run, listed by `dre validate` as a blind spot. Never PASSED |

## 11. What changes from today, and what stays

**Stays the same:**
- The three result states.
- One writer exposing only `append`.
- Read-only access.
- No code per source.
- The existing checks and their configuration: `key_map`, `hop_exclusions`, `winner_rule` and `owned_columns` become the `identity` / `dedup` mapping, an unversioned exclusion, the version rule and identity transforms. `dre validate` prints the contract form.

**Changes:**
- The new disposition set and its identity assertion.
- Approval metadata on exclusions.
- Reject policies.
- Deletions.
- The `grain` block and mapping kinds.
- Scope columns on results.
- The `dq_reconciliation` table: disposition counts per edge per window, append-only.
- AS_OF_REPLAY.
- Commit rows (ADR-07).
- The new checks: HOP_LEDGER, HOP_CONTRIBUTION, T1_SOURCE_ATTRIBUTION, CHAIN_COMPLETENESS.

**Not done:**
- `fan_out` lineage stays unsupported.
- `versions: all` (an SCD2 target that must hold every version, not only the latest) is deferred.
- The engine never infers an exclusion, a reject reason or a lineage edge it was not given.

## 12. Amendments ratified at checkpoint 3 (2026-10-19)

The model is frozen; these amendments were ratified by the user at checkpoint 3 (`docs/decisions.md`, "Phase 1 build step 3" and "Checkpoint 3 reviewed"). Where they differ from the sections above, they win. The spec (section 6, HOP\_LEDGER) states the rules as built.

1. **Populations (section 2).** A record is due in the window where its deadline falls (L1), within the hop's deadline range, which continues from the coverage the hop committed: a change of the SLA neither leaves a gap nor judges a record twice, and a window whose result covered nothing is judged by the next. The cutoff is the window end (L2). On the full sweep, untimed records are due, as unassessable (L8).
2. **Unassessable (section 3, row 1)** also covers: under `identity`, more than one record at the version on either side ("not one-to-one", L4); a reject of an absent record that names no version; a reject whose reason's policy lists rules but which names none (C3-01).
3. **Rejects (section 5).** A reject accounts for a record only through lineage: the refused record's version, or its batch, matching the version under judgement; a policy entry may list the approved rules (id@version) its rejects must name (C3-01, replacing L3). The rejects table's columns are in `docs/evidence-contract.md`.
4. **The claim (section 1).** A result stores its qualifications (an inferred contract, an unapproved `hop_exclusions` entry, an unverified union attribution, orphans not checked, reported evidence). It is contract-proven only with none, and a qualified PASS is never healthy (C3-02 to C3-04, replacing L11). The scope names the verified and unverified columns; partial value coverage is disclosed and does not qualify a completeness claim (C3-03, replacing L12).
5. **Union attribution (section 6.1).** Until T1\_SOURCE\_ATTRIBUTION proves the predicates disjoint and complete (build step 5), a union's attribution is `feed_filter_unverified` (or `unknown` without a predicate) and its claim stays qualified (C3-02).
6. **Identity (section 3).** An unbalanced ledger fails closed, holds its window and is recorded as an integrity incident (L5).
7. **Overlaps (section 3).** Only the explaining rules (unassessable to rejected) overlap; delivered to missing describe what the target shows when no rule explains the record (L7).
