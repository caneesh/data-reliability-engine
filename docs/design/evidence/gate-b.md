# Evidence package: Gate B (checkpoints 6 to 8)

For the reviewer of Gate B. Built under the master implementation prompt of 2026-10-20, on top of Gate A (`docs/design/evidence/gate-a.md`), which is not yet ratified either. Nothing here is approved, and nothing here claims production readiness: no row of the traceability matrix is at level O. Everything is synthetic.

## Status

| Checkpoint | Scope | Status | Defaults to ratify |
| --- | --- | --- | --- |
| C6 | CHAIN\_COMPLETENESS, `partial_chain`, exception keys only | INTEGRATED | `docs/decisions.md`, step 6, K1 to K9 |
| C7 | Replay tests R14 to R23, traceability | INTEGRATED for R14, R15, R16, R17, R19, R20, R23 (R21's content is `tests/test_commit_protocol.py`); R18 NOT\_STARTED (nothing to run it on); R22 BLOCKED (release 4 step 2) | |
| C8 | Run budget, priority, kill switch, queue at run start, backpressure, measurement | INTEGRATED; concurrency cap N not enforced; load test BLOCKED (needs a representative cluster) | `docs/decisions.md`, step 8, O1 to O7 |

## (a) Commits

| Commit | What |
| --- | --- |
| `ef42e12` | Build step 6 (branch `feature/dre-chain-completeness`, built by a parallel agent) |
| `4edcb79` | Replays R14, R15, R16, R19, R23 (branch `feature/dre-replay-proofs`, parallel agent) |
| `1f91b64` | Build step 8 (branch `feature/dre-quality-operations`, parallel agent) |
| `5c9b496` | Merge C6, with the spec entry and decisions K1 to K9 |
| `0eebdf8` | Merge C7 |
| `1b800ca` | Merge C8; replays R17 and R20; R14 to R16 and R19 restated for step 5 (below); decisions O1 to O7 |

**Restated, not weakened.** R14, R15, R16 and R19 were written before step 5 was merged, and asserted that the sample's union hop carries `attribution_unverified`. Step 5 makes the ledger prove that attribution (every sample gold row is SRC\_A, its only union sibling), so those passes now carry no qualification (R16's authorised reject still carries `evidence_reported` and stays unhealthy). R17 shows the other side: a union whose filters leave rows unattributed keeps `attribution_unverified` and is never healthy.

## (b) Source changes

| Area | Files |
| --- | --- |
| Chain (C6) | `checks/hop/chain.py`, `checks/sql/chain.sql.j2` (new); `hop_ledger.sql.j2` (optional `carried` keys: HOP\_LEDGER's own output unchanged); `checks/base.py` (`partial_chain`, `CheckContext.config`); `checks/hop/coverage.py` (`group`, `sla_hours`); `runner.py` (`covers` counts partial\_chain; key events labelled by chain); the patterns |
| Operations (C8) | `operations.py`, `scripts/measure_run.py` (new); `config/models.py` (`run_budget_minutes`, `kill_switch_file`, `backpressure_share`); `checks/base.py` (`run_budget_exceeded`, `disabled`); `runner.py` (priority order, run clock, `Halt`, deferred and owed sweeps, queue refusal at start); `conf/defaults.yaml` |
| Replays (C7) | `tests/replay/ledger_views.py`, `test_r14_substituted_key.py`, `test_r15_exclusions.py`, `test_r16_defect_reject.py`, `test_r17_two_sources.py`, `test_r19_cdc_versions.py`, `test_r20_historical_rerun.py`, `test_r23_fan_out_unsupported.py` |
| Docs | `docs/spec.md` (sections 3, 4, 5, 6, 8), `docs/decisions.md`, `docs/design/traceability-matrix.md` (sections 4 to 6; new level R), this file |

## (c) Tests, with the commands that ran

| Command | On | Exit | Result |
| --- | --- | --- | --- |
| C6 agent: `tests/checks/test_chain.py tests/checks/test_hop_ledger.py tests/checks/test_hop_coverage.py tests/config tests/guard tests/test_registries.py tests/checks/test_base.py tests/checks/test_check_params.py`; then `tests/test_runner.py` | C6 branch | 0, 0 | 395, 21 passed |
| C7 agent: the five replay files, then the two it corrected, then R16, R23 and `tests/guard` | C7 branch | 1, 0, 0 | 8 passed 2 failed (its own wrong guesses at HOP\_KEY\_CURRENCY's wording, corrected), then 2, then 94 passed |
| C8 agent: `tests/config/test_operations_config.py`; `tests/test_operations.py`; `tests/test_operations_runner.py`; `tests/config tests/guard tests/checks/test_base.py`; `tests/test_runner.py tests/test_execution_types.py`; the e2e sample by hand | C8 branch | 0 each | 8, 10, 6, 291, 27 passed; e2e COMPLETED, 40 of 40 |
| `tests/checks/test_chain.py tests/checks/test_hop_ledger_sources.py tests/test_registries.py tests/guard tests/config` | C6 merged on Gate A | 0 | 328 passed |
| The seven replay files, `tests/test_operations.py`, `tests/config/test_operations_config.py`, `tests/test_registries.py`, `tests/guard` | all merged | 1 | 159 passed, 2 failed: R19's PASS (stale `attribution_unverified`, restated) and R20 (it filtered the refused AS\_OF result by an upstream group it does not carry; now read by dataset and mode) |
| R19 and R20 again | all merged | 0 | 3 passed |
| **Gate B:** full suite, then the e2e build, install, run and verify | `1b800ca` | 0, 0, 0, 0, 0 | **955 passed** (67.0 min); e2e run COMPLETED, 41 of 41 checks written; verify: 51 results, no problem |

## (d) Failure injection and fail-closed behaviour

- **Kill switch:** at the start every check is DID\_NOT\_RUN / disabled and the run PARTIAL; between checks, the remaining ones (`tests/test_operations_runner.py`). An unreadable switch location counts as on (`tests/test_operations.py`).
- **Run budget:** jobs past it are DID\_NOT\_RUN / run\_budget\_exceeded, committed, and their windows held (the next run starts from the same window); on-time and completeness checks evaluated first.
- **Queue:** a protected or foreign queue cannot start a run (exit 3).
- **Backpressure:** the full sweep deferred under pressure, recorded, and performed by the next run not under pressure; integrity's forced sweep never deferred.
- **Chain:** an unjudged edge is partial\_chain, never PASSED; a loss on the observable segment still FAILS; an unassessable record at an edge counts as lost; a missing layer is table\_missing naming the edge; an ambiguous or missing path is invalid\_config.
- **Commit protocol** (unchanged, R21): `tests/test_commit_protocol.py`, every write boundary.

## (e) Measurements (local only; no thresholds proposed beyond ADR-08's)

`scripts/measure_run.py --conf DIR` on the e2e synthetic sample (local Spark, 4 shared CPUs), from the C8 agent's run: `dq_run` elapsed 2.60 minutes, 6.5% of the 40-minute run budget; 40 evaluations took 1.26 minutes inside checks, 1.34 minutes outside them (planning, landing listing, writes, commits, the first integrity sweep, the digest). Largest per-evaluation medians: HOP\_LEDGER 9.9 s, HOP\_KEY\_CURRENCY 4.5 s, HOP\_VALUE\_AGREEMENT 3.5 s, T1\_PARTITION\_DRIFT 2.8 s; tier 1 checks 0.3 to 1.6 s. On the Gate B head (`1b800ca`, the e2e store of the gate run): elapsed 1.00 minute, 2.5% of the budget; 41 evaluations, 0.49 minutes inside checks and 0.52 outside; medians HOP\_LEDGER 3.1 s, HOP\_VALUE\_AGREEMENT 1.7 s, HOP\_KEY\_CURRENCY 1.6 s, T1\_PARTITION\_DRIFT 1.3 s, T1\_SOURCE\_ATTRIBUTION 0.2 s, every other check under 1 s. The sample configures no chain and no contributor, so CHAIN\_COMPLETENESS and HOP\_CONTRIBUTION are not in it (their cost is unmeasured even locally); the run budget, kill switch and backpressure did not trigger.

These are synthetic-volume figures. They say nothing about a production table: the load test (acceptance criterion 7) needs a representative cluster.

## (f) Limitations and residual risks

1. **No load test.** Budgets, backpressure and the attribution scans are unmeasured at production volume.
2. **Concurrency cap N** (ADR-08) is not enforced: one check at a time, but a check's SQL can run several Spark jobs, and a cancelled check's thread can keep starting jobs; the YARN queue's share is the real cap (O6).
3. **Deployment values** stay null: `yarn_queue`, `protected_queues`, `kill_switch_file`.
4. **R18** (aggregation through `dre run`) is not added: no aggregate is configured for the member pipeline and the sample has none; the chain's partial\_chain for an unjudged edge is proven at check level. **R22** needs the file record count (release 4 step 2).
5. **The chain** reads key\_map edges only (a contributor declared only in `contracts` is not on a path), its later edges are judged only once per source range (K2), and `dre validate` does not check its terminal or path. It cannot read as of a cutoff.
6. **SWEEP\_DEFERRED** is a new `dq_integrity` record kind (O5): ratify it, or ask for a dedicated table through a migration.
7. **CI:** on the 4 shared CPUs here a full local run takes about 56 to 75 minutes; CI runs it on its own runner.

## (g) Gate B asks

1. Ratify or correct K1 to K9 and O1 to O7, after Gate A.
2. The platform team: a representative cluster for the load test, the YARN queue and the kill switch path.
3. ~~CI on the pushed head.~~ Run #95 on `88df3e5` (this code plus documentation): green; #94 on `1b800ca` itself was still running when recorded.
