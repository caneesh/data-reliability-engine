# Design: target architecture after the Phase 1 assessment (proposed)

Status: **approved and frozen on 2026-10-16.** The user approved ADR-01 to ADR-08 that day, together with section 3 (result states stay at three), and every Phase 1 exit criterion is met (section 5). Nothing here is built yet; section 7 is the build order. A change to an approved decision needs a new decision entry in `docs/decisions.md`. This document answers the Phase 1 foundational architecture assessment (16 October 2026, a static review; verdict "conditional, not ready for design freeze"). It has four parts:

- a verdict on each finding, checked against the repository;
- the invariants;
- eight architecture decisions (ADR-01 to ADR-08) for approval;
- the Phase 1 exit criteria and where each stands.

`docs/design/reconciliation-model.md` is the reconciliation model, with worked examples and an expected result for each proof scenario. It supersedes `docs/design/reconciliation-contracts.md`.

**Recommended order:**

1. ~~Approve ADR-01, ADR-02 and ADR-05, which resolve the three critical findings.~~ Approved 2026-10-16.
2. ~~Approve ADR-03 and ADR-04.~~ Approved 2026-10-16.
3. ~~Build the traceability matrix (P1-12).~~ Done: `docs/design/traceability-matrix.md`.
4. Only then write code. All three done on 2026-10-16: the build can start (section 7).

Release 4 (`docs/design/outside-in-evidence.md`) step 1 is built. Steps 2 onward wait with everything else, except step 2 (file record count): the expected results for proof scenario 9 depend on it.

## 1. The findings, checked against the repository

| Finding | Verdict | What the repository shows | Resolved by |
| --- | --- | --- | --- |
| P1-01 Completeness claim exceeds the contract (critical) | **Agree** | `docs/architecture.md` states hop-level guarantees and marks acceptance criteria 3 and 4 "Met" with hop tests. No result says what population, cutoff or contract it covers. Chain completeness exists only as a proposal | ADR-01; model sections 1, 7 |
| P1-02 Identity and grain not general enough (critical) | **Agree, and one more gap** | One-to-many hops and derived downstreams are listed as open. Also, nothing checks that `key_normalise` is injective: `strip_leading_zeros` and `upper` can merge distinct keys silently (`checks/keys.py`) | ADR-02; model section 4 |
| P1-03 Temporal correctness and replay (critical) | **Agree, with one refinement** | Spec section 4: RERUN and REPLAY read today's data ("best effort, not a snapshot"). The refinement: DRE's own NORMAL results are append-only, so they *are* as-of evidence of what was observed at each run. And an append-only layer can be read as of a cutoff by its load time | ADR-05; model section 8 |
| P1-04 Ledger semantics inconsistent | **Agree** | `reconciliation-contracts.md` puts `pending` and `excluded` on the right of an identity whose left side is "expected keys past the SLA". Its tolerance covers `unassessable` | ADR-01; model sections 2, 3 |
| P1-05 Reject and exclusion trust | **Agree** | The old plan let a rejects row move a key to `rejected` "and the hop can PASS". `hop_exclusions` is an unversioned SQL condition with no approval, no effective dates and no volume guard | ADR-04; model section 5 |
| P1-06 Multi-source attribution and overlap | **Agree** | The old plan required attribution to be disjoint everywhere. That is wrong for a join, where several sources contribute to one row, and for survivorship | ADR-03; model section 6 |
| P1-07 Read-only observability limit | **Agree, with one more level** | The cause engine has evidence levels (confirmed, suspected, insufficient), but checks and dispositions have none. The pipeline's own audit artefacts (`docs/evidence-contract.md`) are a distinct kind of evidence: reported, not observed | ADR-06 |
| P1-08 Chain composition across changed grain | **Agree, and worse** | The old plan composed the chain from `dq_key_event`, but that table holds only failing keys (FLAGGED, STILL_FLAGGED, CLEARED). A chain cannot be built from it | ADR-02, ADR-03; model section 7 |
| P1-09 Non-atomic evidence persistence | **Agree, confirmed** | `v_latest_result` takes the newest result row with no regard to its run (`store/ddl.sql`). If the process dies after a result row and before its key events, the result is visible without its evidence until the next run re-evaluates the held window. The `evidence_incomplete` row is written only when the process survives | ADR-07 |
| P1-10 Isolation not proven | **Agree** | A per-check compute budget and Spark job groups exist. There is no run-level budget, no queue, no kill switch, and no load test (acceptance criterion 7 says "not yet demonstrated") | ADR-08 |
| P1-11 Silent success and zero records | **Partly addressed** | T1_ON_TIME and T1_ZERO_ROWS count expected cadence slots, honour named calendars and never read job status. Missing: source-side evidence that a load should have had records (release 4 step 2, the file record count) | Model section 10, scenario 9 |
| P1-12 Design status against test claims | **Agree** | `docs/architecture.md` marks criteria "Met" by hop-level tests while the ledger and chain are proposals | Traceability matrix (next deliverable); a scope note added to `docs/architecture.md` |

## 2. Invariants

The repository's hard constraints (CLAUDE.md 1 to 7) stand unchanged. The assessment's invariants are adopted, each with how it will be enforced:

| Invariant | Enforced by |
| --- | --- |
| No production writes, pipeline hooks, schedule dependencies or job-blocking behaviour | Hard constraints 1 and 2; guard tests (spec section 9) |
| Every PASS is scoped to a versioned contract, population, grain, evidence cutoff and coverage | The scope columns on every result (ADR-01) |
| Every candidate record is accounted for exactly once, or reported as unknown | The disposition identity, asserted by the engine. An unbalanced ledger is DID_NOT_RUN / `ledger_unbalanced` (model section 3) |
| Transformations, merges, deletes, rejects, exclusions and late arrivals have distinct semantics | Separate mapping kinds and dispositions (model sections 3 to 5) |
| An unobservable cause cannot be labelled confirmed | Claim ceilings by evidence level (ADR-06) |
| The earliest observed gap is not necessarily the originating defect | The chain names a "first observed gap", never an "origin" (model section 7) |
| Every result is reproducible against its declared evidence snapshot, or labelled a current recheck | Evaluation modes (ADR-05) |
| An incomplete evidence write never becomes a visible complete PASS | Commit rows (ADR-07) |

## 3. Result states stay at three

The assessment uses NOT_ASSESSABLE, PARTIAL_CHAIN, UNKNOWN, CURRENT_RECHECK and AS_OF_REPLAY. Hard constraint 4 (exactly PASSED, FAILED or DID_NOT_RUN) stays. Each new concept is expressed as a reason, a disposition or a mode, never as a fourth state:

| Assessment concept | Expressed as |
| --- | --- |
| NOT_ASSESSABLE (an unsupported edge) | DID_NOT_RUN / CONFIGURATION / `unsupported_mapping`, on every run, and listed by `dre validate` |
| UNKNOWN (a record) | Disposition `unassessable`: a violation that can never be tolerated |
| PARTIAL_CHAIN | CHAIN_COMPLETENESS DID_NOT_RUN / `partial_chain`, or FAILED when the observable segments prove a loss |
| Missing evidence | DID_NOT_RUN / `insufficient_evidence`, naming what is missing |
| CURRENT_RECHECK, AS_OF_REPLAY | Evaluation modes: a column on the result, not a state (ADR-05) |

The trade-off: a dashboard that counts states alone will put an unsupported edge and an empty window in the same bucket. The digest separates them by reason category, as it already does for CONFIGURATION against DATA.

## 4. Architecture decisions

### ADR-01: candidate and due populations, the disposition taxonomy, the scope of a PASS

**Status: approved by the user, 2026-10-16.**

**Decision.**

- **Populations.** Candidate, due and pending populations are defined as in model section 2, and pending is outside the identity.
- **Dispositions.** There are eleven, in a fixed precedence, in four classes: ok, authorised, deficit and breach, plus unknown (model section 3).
- **Identity.** The engine asserts it on every evaluation.
- **Tolerance.** It applies only to `missing`, `stale` and `divergent`, as absolute counts.
- **Scope.** Every result stores its scope: edge, contract version hash, population definition, grain, cutoff, mode, evidence level and coverage. A PASS claims nothing outside that scope.
- **Chain claim.** No chain-level claim is made anywhere (digest, docs, acceptance table) until CHAIN_COMPLETENESS is built and tested.

**Alternatives.**

- A percentage tolerance: rejected, because a percentage hides small losses on large loads.
- Pending inside the ledger: rejected (P1-04).

### ADR-02: grain, identity, version and cardinality

**Status: approved by the user, 2026-10-16.**

**Decision.**

- **Grain.** Every dataset declares `grain` (key, version rule, history class).
- **Mapping kind.** Every edge declares exactly one of `identity`, `dedup`, `merge`, `union_partition`, `join_contribution`, `survivorship`, `aggregate` or `fan_out` (model section 4).
- **Unsupported kinds.** An edge without a supported kind is DID_NOT_RUN / `unsupported_mapping`. `fan_out` stays unsupported.
- **Transforms.** They are one-directional SQL over U, so a non-injective transform is never inverted.
- **Key collisions.** Collisions caused by normalisers are detected at run time and are `unassessable`.
- **Today's fields.** `key_map`, `winner_rule` and `owned_columns` become aliases of `identity` / `dedup` and identity transforms.

**Alternatives.** Inferring cardinality from data was rejected: the engine must not guess a contract.

### ADR-03: union ownership, joins, aggregation and fan-out lineage

**Status: approved by the user, 2026-10-16.**

**Decision.** There are three multi-source shapes, never mixed on one edge (model section 6):

- **Union partition.** Exhaustive, disjoint attribution, proven by T1_SOURCE_ATTRIBUTION.
- **Join contribution.** One primary owns the population. Contributors are checked by HOP_CONTRIBUTION for resolution and value agreement, not for completeness.
- **Survivorship.** Declared precedence; the losing source's record is `superseded` only if the winner's edge delivered it.

**Lineage.**

- **Aggregation.** Membership is *reconstructed* (the group function is SQL over U). Contribution is *unavailable* unless a member-grain table exists between U and the aggregate, and the scope says so.
- **The chain.** It follows records edge by edge, reading the layers directly over the due source keys, not from stored key events. A loss proven on observable segments is FAILED. An unobservable segment without a loss is `partial_chain`.

### ADR-04: approved exclusions, reject policies, deletions

**Status: approved by the user, 2026-10-16.**

**Decision.**

- **Exclusions.** Each carries an id, a version, effective dates, an approval reference, a code reference and a `max_share` volume guard.
- **Rejects.** They count only with key lineage, and only under a `reject_policy` whose default is defect.
- **Deletions.** A declared tombstone rule and how it is applied (hard or soft). `deleted_not_applied` is a breach.
- **Reported evidence.** The pipeline's own filter registry never authorises an exclusion; it can only name a cause.

All of this is in model section 5.

**Answered (2026-10-16).** Deletions are soft voids: a new version whose end date equals its effective date, with the row kept (model section 5). Hard deletes are not used by the member pipeline.

### ADR-05: event time, ingestion time, cutoff, SLA and replay modes

**Status: approved by the user, 2026-10-16.**

**Decision.**

- **Times.** The five times are those in model section 8. Only the deadline is compared with the cutoff.
- **History class.** Each dataset declares `history`: `append_only`, `mutable` or `snapshot_partitioned`.
- **Modes.** There are three: NORMAL; CURRENT_RECHECK, which today's RERUN and REPLAY become, labelled in the digest and the CLI and never presented as historical; and AS_OF_REPLAY.
- **When AS_OF_REPLAY is allowed.** Only when every dataset the edge reads is `append_only` or has a snapshot at the cutoff, and T1_PARTITION_DRIFT shows no drift on it since the cutoff. Otherwise it is DID_NOT_RUN / `no_as_of_evidence`.
- **Historical evidence.** For a mutable dataset, the historical evidence is the stored NORMAL result.

**Consequence.**

- The `execution_type` values RERUN and REPLAY stay in the stored history, for compatibility.
- A new `evaluation_mode` column carries NORMAL, CURRENT_RECHECK or AS_OF_REPLAY.

### ADR-06: evidence levels, unknown states and permitted claims

**Status: approved by the user, 2026-10-16.**

**Decision.** There are five evidence levels. They are attached to every disposition count, every cause line and every result's scope:

| Level | Meaning | Example | May support |
| --- | --- | --- | --- |
| observed | DRE read it directly | A row in D; a file listing | Any claim |
| reconstructed | DRE recomputed it from observed data under a declared rule | An exclusion match; a group membership; the as-of state of an append-only layer | A disposition. A cause only when the rule is the cause |
| reported | The pipeline's own artefact says so | A rejects row; an audit control row; a log line (`docs/evidence-contract.md`) | A disposition, only under policy (reject policy, key lineage). A cause line marked "reported" |
| inferred | By elimination | Every other cause ruled out | A **suspected** cause only. Never a disposition, never "confirmed" |
| unavailable | Cannot be seen from outside | Merge insert or update split; record-level contribution to an aggregate | Nothing: listed as a blind spot in the scope and by `dre validate` |

**Claim ceiling.** A result's claim is limited by the lowest level its PASS depends on. For example, an aggregate edge with contribution unavailable passes at group level only, and the scope says so.

### ADR-07: transactional publication of evidence

**Status: approved by the user, 2026-10-16.**

**Decision.**

- **Write order.** Each evaluation writes its evidence first (key events, disposition counts, partition state, exception keys), then its result row, then one **commit row**. The commit row goes to a new append-only table `dq_commit` (run id, evaluation id, the row count written to each evidence table). **Commit rows are written per batch of evaluations** (amended on approval): one append covers every evaluation whose evidence and result are already stored. This is as safe as a row per evaluation, because nothing in the batch is visible before the append, and it avoids a small Hive file per evaluation.
- **What is visible.** Every view that exposes current state (`v_latest_result`, `v_open_keys`, `v_latest_partition_state`, and the new reconciliation views) shows only rows of committed evaluations.
- **Failures.** A crash anywhere before the commit leaves the evaluation invisible. The run is not COMPLETED, so its window is held and the next run evaluates it again.
- **Idempotency.** Deterministic evaluation ids keep the retry idempotent.
- **Concurrency.** It stays as today: one NORMAL run at a time. A RERUN alongside a NORMAL run writes under its own run id and is never current state (spec section 5).

**Cost.** Every view read joins `dq_commit`, which is bounded by `run_date` partition like the reads it guards.

**Alternative.** Gating on the run's final status was rejected: it would hide every evaluation of a PARTIAL run, including the complete ones.

**Proof.** Failure injection at each write boundary (after evidence, after result, before commit), in `tests/test_recovery.py`.

### ADR-08: operational isolation and security

**Status: approved by the user, 2026-10-16.**

**Decision.**

- **Budgets.** A run-level budget (`run_budget_minutes`, default **40** of the hourly run) on top of the per-check budget. Checks beyond the run budget are DID_NOT_RUN / BUDGET, in a fixed priority order: on-time and completeness first, value checks after.
- **Queue.** A dedicated YARN queue with a capped share. Its name is a platform value, left `null` until the user provides it. **DRE never runs in the cluster's high-priority queue** (user, 2026-10-16): that queue belongs to the production pipelines. Its name is a production value, so it is kept only in the deployed configuration, never in this repository. `dre validate` will refuse a configured queue that is on the deployment's `protected_queues` list.
- **Concurrency.** At most N concurrent Spark jobs per run.
- **Kill switch.** A kill switch read at start and between checks: a file path in config. When it is present, the run ends PARTIAL with every remaining check DID_NOT_RUN / `disabled`. No pipeline is touched either way. The path and who may create the file are deployment values (left `null` until named).
- **Backpressure.** When the previous run used over 80% of its budget, the next run defers the weekly full sweep.
- **Security.** It stays as today: HMAC keys, no `key_value` outside the store, the secret from a file.

**Proof.** A load test in a representative cluster (acceptance criterion 7). It cannot be done in CI. This needs the platform team.

## 5. Phase 1 exit criteria

| # | Criterion | Status |
| --- | --- | --- |
| 1 | Every critical finding resolved by an approved ADR | **Met**: ADR-01 (P1-01), ADR-02 (P1-02) and ADR-05 (P1-03) approved on 2026-10-16 |
| 2 | The contract schema represents each required pipeline topology | **Met on paper**: eight mapping kinds (ADR-02, ADR-03, approved). `fan_out` is explicitly unsupported and visible. The member pipeline needs `identity`/`dedup`/`merge` and `union_partition`; whether it needs `join_contribution` is developer question 19 |
| 3 | The accounting identities and temporal semantics are unambiguous | **Met on paper**: model sections 2, 3 and 8 (ADR-01, ADR-05, approved) |
| 4 | Observability limits and evidence levels are enforceable | **Met on paper**: ADR-06 approved on 2026-10-16 (claim ceilings, blind spots in `dre validate`) |
| 5 | Each proof scenario has an expected result | Done on paper: model section 10 |
| 6 | A traceability matrix separates proposed from implemented and tested behaviour | **Done**: `docs/design/traceability-matrix.md` (2026-10-16). It is kept current by each build step |

## 6. Decisions needed

**From the user (approval).** Approved on 2026-10-16: ADR-01, ADR-02 and ADR-05, and with them:

- the result states stay at three (section 3);
- the tolerance is an absolute count on deficits only (ADR-01);
- today's RERUN and REPLAY are relabelled as current rechecks (ADR-05).

Then approved, the same day: ADR-03 and ADR-04, including that the reject policy defaults to defect; then ADR-06 to ADR-08, with commit rows per batch, a 40-minute run budget, and on-time and completeness checks first.

**From the developer:**

- whether the hand-off table is overwritten per batch partition (question 17);
- whether any gold row is built by joining columns from two sources (a contributor, 6.2 of the model), as opposed to rows stacked by source;
- whether a void ever has an end date other than its effective date.

Answered on 2026-10-16 (`docs/decisions.md`, "Developer answers on the reconciliation model"):
- **Deletions** are soft voids: end date set equal to effective date, and the row is kept.
- **No gold table holds counts or totals.**
- **All sources have equal priority.** A member with group and retail coverage appears in every associated table, so there is no survivorship, and multi-source tables are union partitions by source.

**From the platform team:**

- the YARN queue for DRE (the user will provide it; never the high-priority queue);
- the kill switch's path and who may use it;
- whether any layer is genuinely append-only (needed for AS_OF_REPLAY);
- a representative cluster for the load test.

## 7. After approval: the build order

Each step ends at an architectural checkpoint (`docs/design/checkpoints.md`) that the user reviews before the next step starts. How each built step is implemented (modules, data flow, tables, views, tests) is in `docs/design/implementation.md`.

1. **Contract schema and validation.** `grain`, mapping kinds, the versioned exclusions, `reject_policy`, `deletion`, `history`, and the aliases for today's fields. `dre validate` lists unsupported edges and blind spots. **Built 2026-10-16; checkpoint 1.**
2. **Commit rows and committed-only views (ADR-07).** These go first among the store changes because every later result depends on them. **Built 2026-10-17; checkpoint 2.**
3. **HOP_LEDGER and `dq_reconciliation`.** The identity, the scope columns, orphans and key collisions.
4. **Evaluation modes**, and AS_OF_REPLAY for append-only datasets.
5. **T1_SOURCE_ATTRIBUTION, HOP_CONTRIBUTION and survivorship.**
6. **CHAIN_COMPLETENESS.**
7. **Replay tests R14 to R23**, one per proof scenario, and the traceability matrix updated to "unit-tested".
8. **Run-level budget, kill switch, load test (ADR-08).**
