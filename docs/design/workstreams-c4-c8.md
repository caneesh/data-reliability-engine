# Workstreams for build steps 4 to 8: inventory, frozen interfaces, ownership (2026-10-20)

**Outcome (2026-10-20).** All five workstreams were built and merged in order (C4, C5, C6, C7, C8); the gate runs and the evidence are in `docs/design/checkpoints.md`, "Gate A and Gate B". Where a build departed from the interfaces frozen below, `docs/decisions.md` says so (M1, K8 and the C8 additions in O1 to O7). The inventory in section 1 is the state before the work.

This file records Phase 1 (inventory) and Phase 2 (interface freeze) of the parallel development of
checkpoints 3 to 8. It is a working document: anything below marked **default taken** is a choice
the spec and the ADRs leave open, recorded in `docs/decisions.md` for ratification at Gate A or Gate B.
Nothing here is approved until the user ratifies it.

## 1. Inventory at 486792a (Phase 1)

| Checkpoint | Scope (target-architecture section 7) | Status | What exists | What is missing |
| --- | --- | --- | --- | --- |
| C3 | HOP_LEDGER, `dq_reconciliation`, corrections C3-01 to C3-04, L1 to L15 | TESTED, INTEGRATED (CI #90 on 6387989); revised checkpoint awaits review | `checks/hop/ledger.py`, `hop_ledger.sql.j2`, `checks/hop/coverage.py`, `store/reconciliation.py`, qualifications and claim-aware `healthy` | Independent review of the revised checkpoint 3 |
| C4 | Evaluation modes; AS_OF_REPLAY for append-only datasets | NOT_STARTED | `execution_type` NORMAL, RERUN, REPLAY; a printed "not a snapshot" note; `grain.history` declared and validated | `evaluation_mode` column, CURRENT_RECHECK labels (CLI, digest), AS_OF_REPLAY and `no_as_of_evidence`; a schema migration path for existing stores |
| C5 | T1_SOURCE_ATTRIBUTION, HOP_CONTRIBUTION, survivorship | NOT_STARTED | `union_partition`, `join_contribution`, `survivorship` validated in config (`reference`, `precedence`); the ledger caps union claims (`attribution_unverified`) | The two checks, the `superseded` disposition, attribution `verified` |
| C6 | CHAIN_COMPLETENESS | NOT_STARTED | `dre trace` follows one key across hops | The chain check, its outcomes, `partial_chain`, chain due population |
| C7 | Replay R14 to R23 | NOT_STARTED (R21's content exists as `tests/test_commit_protocol.py`) | Replay R01 to R13; PS1, PS2, PS3, PS6, PS10 proven at check level | Replay tests R14 to R23; traceability "unit-tested" |
| C8 | Run budget, kill switch, queue, backpressure, load test | NOT_STARTED (protected-queue rule validated statically) | Per-check `compute_budget_minutes`, `within_budget`, `yarn_queue`, `protected_queues` | Run budget with priority order, kill switch, queue check at run start, backpressure, local measurements; the load test needs a representative cluster |

**Shared dependencies.** `runner.py` (C4 modes, C8 budget and kill switch), `store/ddl.sql` (C4 column),
`config/models.py` (C5 contributed columns, C8 settings), `checks/base.py` (new reason codes),
`checks/hop/ledger.py` and `hop_ledger.sql.j2` (C5 attribution and `superseded`; C6 reads its per-key dispositions).

**Blocking risks found.**

1. **No migration path.** `dre install --apply` only creates missing objects (`CREATE ... IF NOT EXISTS`), so a store
   installed before step 3 keeps its old columns, and `dre run` then fails in the writer (column mismatch). The
   guard forbids `ALTER TABLE ... ADD COLUMNS` everywhere. C4 adds a column, so it needs an additive migration
   through the install path (section 3).
2. **Concurrency cap N** (ADR-08) is unspecified. The engine evaluates checks one at a time, so a run has at most
   one evaluation's Spark jobs at a time; N is a measurement to report, not a value to invent.
3. **Deployment values** stay `null`: the YARN queue, the kill switch path and who may create it.
4. **The load test** needs a representative cluster (platform team). Local measurements only.
5. **R22** (PS9) needs the source's file record count from release 4 step 2, which is not built.
6. **Resources.** 4 CPUs and 15 GB: concurrent local Spark test runs slow each other. Each workstream runs its own
   targeted tests; the full suite runs once per integration, on the integration branch.

## 2. Ownership

| Area | Owner | Others may |
| --- | --- | --- |
| `runner.py`, `store/ddl.sql`, `store/install.py`, `store/schema.py`, migrations, `config/models.py`, `checks/base.py`, `config/contracts.py`, `checks/hop/ledger.py`, `hop_ledger.sql.j2`, `docs/spec.md`, `docs/decisions.md` | Integration owner (main session) | Propose a minimal change in their branch; the owner reviews it at merge |
| C4: evaluation modes, AS_OF_REPLAY, migrations | Workstream A (main session) | |
| C5: `checks/hop/attribution.py`, `checks/hop/contribution.py`, their SQL, survivorship in the ledger | Workstream B (agent, worktree, local branch `feature/dre-source-attribution`) | |
| C6: `checks/hop/chain.py`, `chain.sql.j2` | Workstream C (after C5 merges) | |
| C7: `tests/replay/test_r14_*` to `test_r23_*`, fixtures | Workstream D1 (agent, worktree, local branch `feature/dre-replay-proofs`) | |
| C8: `operations.py`, run budget and kill switch in the runner | Workstream D2 (agent, worktree, local branch `feature/dre-quality-operations`) | |

Feature branches stay local. Only `claude/claude-md-documentation-a2lxt0` is pushed (the session's designated branch);
each workstream is merged into it, in order, after its tests pass, and the full suite runs after each merge.

## 3. Frozen interfaces (Phase 2)

### 3.1 Evaluation context and modes (C4)

- `dq_check_result.evaluation_mode STRING`, the last data column: `NORMAL`, `CURRENT_RECHECK` or `AS_OF_REPLAY`.
  Rows stored before it read as `NORMAL` for `execution_type = 'NORMAL'` and `CURRENT_RECHECK` otherwise
  (`COALESCE` in the views; nothing stored is rewritten).
- `execution_type` keeps NORMAL, RERUN and REPLAY, and gains `AS_OF_REPLAY` (CLI `--execution-type AS_OF_REPLAY
  --window-start --window-end`). RERUN and REPLAY are stored with `evaluation_mode = 'CURRENT_RECHECK'`.
- `CheckContext.as_of: datetime | None`: the evidence cutoff C of an AS_OF_REPLAY evaluation (the window end);
  None otherwise. A check that can read as of C sets the class attribute `Check.as_of_capable = True` and bounds
  every read by load time at or before C. Any other check, under AS_OF_REPLAY, is DID_NOT_RUN / `no_as_of_evidence`.
- `checks/modes.py`: `as_of_refusal(ctx, check, datasets) -> CheckResult | None`, the gate of ADR-05 (every dataset
  read is `append_only`, and every T1_PARTITION_DRIFT result committed since C on it is PASSED with VALID evidence,
  with at least one; otherwise `no_as_of_evidence` naming the dataset and why).
- Reason code `no_as_of_evidence`, category DATA_UNAVAILABLE (default taken: an AS_OF run never holds a window).

### 3.2 Migrations (C4)

- `store/migrations.py`: an ordered tuple of `Migration(version, table, columns)` (additive only: `ADD COLUMNS`), and
  `pending(spark, db) -> list[Migration]` (a migration is pending while any of its columns is absent).
- `dre install --apply` creates the missing tables, applies the pending migrations, then replaces the views.
  No bookkeeping table (as first frozen): the store's own columns say which migrations it has, and a new table
  would have changed `retention_months`, which must list every dq table (changed at build, decisions M1).
  `dre install --check` reports a pending migration as PENDING. `dre run` refuses to start while one is
  pending, naming `dre install --apply`.
- Guard (spec section 9, rule 11): `ALTER TABLE <dq>.<table> ADD COLUMNS` is allowed only in `store/migrations.py`; any other
  ALTER stays forbidden there. Never DROP, never recreate, never rewrite a stored row.

### 3.3 Contract and mapping (C5)

- `EdgeContract.contributes: dict[Identifier, Identifier]` (join_contribution: the contributor's column -> this
  dataset's column), validated only on `join_contribution`.
- Attribution of a union partition: the `feed_filter` of every dataset on the same physical table whose contract
  is `union_partition` (its *siblings*). `checks/hop/attribution.py` exposes
  `siblings(datasets, ds) -> list[Dataset]` and `attribution_sql(...)` (one rendering of
  `source_attribution.sql.j2` shared by the check and the ledger).
- T1_SOURCE_ATTRIBUTION: one result per physical table per window (selected like the table checks, `once_per_table`);
  population = rows of the table (as of C); violations = rows matching no sibling predicate (unattributed) plus rows
  matching more than one (overlap); FAILED lists the counts per predicate. Zero rows: `empty_population`.
- HOP_LEDGER attribution states: `single_source`, `shared_target_unattributed`, `feed_filter_unverified`, `unknown`,
  and `verified`, which the ledger sets when its own evaluation proves the siblings' predicates disjoint and
  complete over the table at C (the same SQL as T1_SOURCE_ATTRIBUTION). `verified` drops `attribution_unverified`.
- HOP_CONTRIBUTION: one result per contributor hop (`join_contribution`) per window; population = rows here loaded in
  the window (all rows on the full sweep); `unresolved` (no contributor version loaded at or before the row's load
  time) and `divergent` (a contributed column disagrees with that version) are violations; ledger lines in
  `dq_reconciliation` with the same columns as HOP_LEDGER's (`line` = `due`, `resolved`, `unresolved`, `divergent`).
- Survivorship: HOP_LEDGER judges `survivorship` hops. A key a higher-precedence upstream's edge delivers, in the
  same evaluation, is `superseded[<winner>]` on a lower one (an explaining disposition, ranked after `deleted`);
  otherwise the lower edge judges the key normally.

### 3.4 Chain (C6)

- Parametrised check CHAIN_COMPLETENESS, configured on the chain's source dataset:
  `check_params: {CHAIN_COMPLETENESS: {terminal: <dataset id>}}`. The path is the unique `key_map` path from the
  source to the terminal; none, or more than one, is DID_NOT_RUN / `invalid_config`.
- Outcomes `reached`, `accounted`, `lost`, `unobservable` in `dq_reconciliation` (`line` = the outcome, `class` = the
  edge where it ended); exception keys (lost, unobservable) as key events.
- FAILED if any record is lost (detail names the first observed gap); otherwise DID_NOT_RUN / `partial_chain`
  (default taken: category DATA_UNAVAILABLE, never holds a window) if any is unobservable; otherwise PASSED.

### 3.5 Qualifications and health

Unchanged from checkpoint 3: `healthy` = PASSED, VALID evidence, no qualifications. New qualification codes are
added to `QUALIFICATIONS` only by the integration owner.

### 3.6 Commit and integrity

Unchanged (ADR-07). Every new check's evidence goes through `CheckResult.reconciliation`, `key_events` or
`partition_state`, and is counted in its commit row. No new write path.

### 3.7 Budgets and cancellation (C8)

- `Defaults.run_budget_minutes: float = 40` (ADR-08), `Defaults.kill_switch_file: str | None = None` (an absolute
  path; a deployment value), `Defaults.backpressure_share: float = 0.8` (ADR-08's 80%).
- `operations.py`: `priority(planned) -> int` (0: on-time and completeness checks, T1_ON_TIME, T1_ZERO_ROWS,
  T1_FILES_NOT_LOADED, HOP_FILE_COMPLETENESS, HOP_LEDGER, CHAIN_COMPLETENESS; 1: every other check),
  `order(jobs)` (stable sort by priority), `kill_switch_on(config) -> bool`, `queue_refusal(spark, config) -> str | None`,
  `defer_full_sweep(spark, db, config, now) -> bool` (the previous NORMAL run used more than `backpressure_share`
  of the run budget).
- Reason codes `run_budget_exceeded` (BUDGET; holds the window) and `disabled` (PLATFORM; the run ends PARTIAL, the
  window is held).

## 4. Integration order (Phase 4)

C3 (already integrated) → C4 → C5 → C6 → C7 → C8, with the full suite after each merge and CI on each push.
