# Evidence package: Gate A (checkpoints 3 to 5)

For the reviewer of Gate A. Built under the master implementation prompt of 2026-10-20 (parallel development of checkpoints 3 to 8). Nothing here is approved: the revised checkpoint 3 still awaits its independent review, and steps 4 and 5 were built against it on the prompt's authority. Everything is synthetic.

## Status

| Checkpoint | Scope | Status | Defaults to ratify |
| --- | --- | --- | --- |
| C3 | HOP\_LEDGER, `dq_reconciliation`, corrections C3-01 to C3-04, L1 to L15 | INTEGRATED (CI #90 on 6387989); revised checkpoint awaits review | as recorded at checkpoint 3 |
| C4 | Evaluation modes, AS\_OF\_REPLAY, versioned migrations | INTEGRATED | `docs/decisions.md`, step 4, M1 to M9 (M2 widens a guard) |
| C5 | T1\_SOURCE\_ATTRIBUTION, HOP\_CONTRIBUTION, survivorship | INTEGRATED | `docs/decisions.md`, step 5, S1 to S6 |

## (a) Commits

All on `claude/claude-md-documentation-a2lxt0`; each workstream was built on a local feature branch in its own worktree, then merged.

| Commit | What |
| --- | --- |
| `ab36f21` | Inventory, frozen interfaces, ownership (`docs/design/workstreams-c4-c8.md`) |
| `e799311` | Build step 4 (branch `feature/dre-core-reconciliation`) |
| `88e4255` | Merge C4 |
| `814a403` | Build step 5 (branch `feature/dre-source-attribution`, built by a parallel agent) |
| `4da17be` | Merge C5, with the recheck fix (one attribution result per table per window) and the spec and decisions |

## (b) Source changes

| Area | Files |
| --- | --- |
| Modes (C4) | `checks/modes.py` (new: `mode_of`, the AS\_OF gate `as_of_refusal`, `drift_proof`), `checks/base.py` (`CheckContext.as_of`, `as_of_proof`, `Check.as_of_capable`, reason `no_as_of_evidence`), `runner.py` (AS\_OF\_REPLAY execution type, `evaluation_mode` on every row, labels, idempotency by kind of reading), `cli.py`, `notify/digest.py` (labels), `checks/hop/ledger.py` and `hop_ledger.sql.j2` (downstream reads bounded by the cutoff; the scope says what the reading rests on) |
| Migrations (C4) | `store/migrations.py` (new), `store/install.py` (tables, then migrations, then views; PENDING), `store/ddl.sql` (`dq_check_result.evaluation_mode`; `v_latest_result.evaluation_mode`), `guard.py` (rule 11) |
| Attribution (C5) | `checks/hop/attribution.py`, `checks/sql/source_attribution.sql.j2` (new); `checks/hop/ledger.py` (`verified` from the ledger's own proof) |
| Contribution (C5) | `checks/hop/contribution.py`, `checks/sql/hop_contribution.sql.j2` (new); `config/models.py` (`EdgeContract.contributes`), `config/validate.py` |
| Survivorship (C5) | `checks/hop/ledger.py`, `hop_ledger.sql.j2` (`superseded[winner]`) |
| Shared | `config/contracts.py` (kinds and blind spots), `runner.py` (`once_per_table_checks`, `once_per_table_window`, `upstreams_of`, `CheckContext.datasets`), the three patterns |
| Docs | `docs/spec.md` sections 3, 4, 5, 6, 8, 9 (rule 11); `docs/decisions.md`; `docs/design/traceability-matrix.md`; this file |

## (c) Tests, with the commands that ran

| Command (from the repository root, `PYSPARK_PYTHON=.venv/bin/python`) | On | Exit | Result |
| --- | --- | --- | --- |
| `pytest -q -p no:cacheprovider` (baseline, before any change) | `486792a` | 0 | 849 passed (74.5 min, CPUs shared with three agents) |
| `pytest tests/guard tests/test_evaluation_modes.py` | C4 branch | 0 | 112 passed (after one assertion fixed: a grouped line prints `[src_sys_nm #1]`) |
| `pytest tests/store` | C4 branch | 0 | 52 passed |
| `pytest tests/test_execution_types.py tests/test_runner.py tests/notify tests/test_commit_protocol.py tests/checks/test_hop_ledger.py tests/test_cli.py` | C4 branch | 0 | 110 passed |
| The C5 agent's targeted runs (`tests/checks/test_source_attribution.py`, `test_hop_ledger_sources.py`, `test_hop_contribution.py`, `test_hop_ledger.py`, `tests/config tests/guard`, replays R09, R11a, R11b, the runner) | C5 branch | 0 each | 9, 11, 9, 39, 295, ... passed (agent report) |
| **Gate A:** `pytest -q -p no:cacheprovider`, then `tests/e2e/build_synthetic.py`, `dre install --conf conf --apply`, `dre run --conf conf`, `tests/e2e/verify.py` | `4da17be` | 0, 0, 0, 0, 0 | **901 passed** (55.9 min); e2e run COMPLETED, 41 of 41 checks written; verify: 51 results, no problem |

The first, unrecorded attempt at the C4 store tests showed one failure that did not reproduce in isolation (52 passed) nor in Gate A; it ran while three agents shared the CPUs. It is listed as a residual risk below.

## (d) What each decision rests on

- **Modes:** `tests/test_evaluation_modes.py` (14): each type's mode; migration of a checkpoint-3 store without rewriting a row; a new store has nothing to migrate; migrations only add columns; a RERUN stored and labelled CURRENT\_RECHECK (CLI line and `v_latest_result`); AS\_OF\_REPLAY on mutable gold through `dre run` (every result DID\_NOT\_RUN, gold's no\_as\_of\_evidence); AS\_OF\_REPLAY on append-only tables reads as of the cutoff (a key loaded later is `missing` as of C, `delivered` now); seven fail-closed refusals (no drift since C, drift FAILED, no baseline, mutable, no load time, a rejects table, a check that cannot read as of C).
- **Guard rule 11:** `tests/guard/test_read_only.py` (ADD COLUMNS caught outside `store/migrations.py`, outside dq, and every other ALTER there; allowed for dq inside it).
- **Attribution:** `tests/checks/test_source_attribution.py` (model example 9.2: 2 unattributed rows FAILED; disjoint and complete PASSED; overlap FAILED; empty; a sibling without `feed_filter`; an exception; siblings; no key value; one evaluation per table, rechecks included), `tests/checks/test_hop_ledger_sources.py` (verified and contract-proven; overlap or unattributed keeps the qualification and is never healthy; a proof that cannot run says why), `tests/test_execution_types.py::test_a_recheck_keeps_one_source_attribution_per_table_and_window`.
- **Contribution:** `tests/checks/test_hop_contribution.py` (9.2: 1 unresolved of 50 FAILED; PASS; the version current at the row's load time; divergent; empty; configuration; exception; never counted toward completeness; no key value).
- **Survivorship:** `tests/checks/test_hop_ledger_sources.py` (9.2 both ways; the ledger balances with `superseded`; an unreadable winner; no key value).

## (e) Limitations and residual risks

1. **Only HOP\_LEDGER reads as of a cutoff.** Every other check is DID\_NOT\_RUN / no\_as\_of\_evidence in an AS\_OF\_REPLAY; `snapshot_partitioned` has no reader; a hop with a rejects table is refused. No sample dataset is `append_only`, and the platform has not said whether any layer is (target architecture section 6).
2. **The drift proof** reaches only the latest T1\_PARTITION\_DRIFT evaluation after the cutoff; there is no staleness bound (none is specified).
3. **Migrations start at checkpoint 3's layout.** An older store is reported, not migrated. No store is deployed.
4. **Attribution scans** every row of a union target, once in T1\_SOURCE\_ATTRIBUTION and once per union hop in the ledger: cost on a large gold table is unmeasured.
5. **HOP\_CONTRIBUTION is strict:** a null reference is unresolved, and the contract's tolerance is not applied. Whether the member pipeline has a join contribution at all is developer question 19.
6. **A non-reproduced store-test failure** under heavy CPU contention (section c).
7. **Ratification needed:** M2 (a guard widened), S1 (where `superseded` ranks), and every M and S default.

## (f) Gate A asks

1. Independent review of the revised checkpoint 3 (unchanged by this package).
2. Ratify or correct M1 to M9 and S1 to S6.
3. ~~CI on the pushed head.~~ Run #93 on `4da17be`: green.
