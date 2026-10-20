# Known issues register

What the engine does not do yet, or does only partly, and why. Each entry has a status: **OPEN** (a known limit, disclosed where it applies), **BLOCKED** (waits on someone outside this repository), **NOT_STARTED** (planned, nothing to build it on yet) or **CLOSED** (fixed; the evidence is named). Updated with the final consolidated review corrections (2026-10-20, `docs/design/evidence/final-corrections.md`). Nothing here is counted as passed or completed unless its status is CLOSED.

## Closed by the final review corrections

| Id | Issue | Status | Evidence |
| --- | --- | --- | --- |
| P1-01 | A digest whose passes were qualified read "all clear" and was suppressed by `all_clear_digest` | CLOSED | `tests/notify/test_digest.py::test_a_digest_whose_passes_are_all_qualified_is_never_all_clear`, `::test_a_digest_with_failures_and_qualified_passes_counts_both`, `::test_only_a_fully_healthy_digest_is_all_clear` |
| P1-02 | A hop idle for longer than `history_lookback_days` restarted its range at the window: the idle span was judged only by the full sweep | CLOSED (with a declared gap beyond retention: KI-08) | `tests/checks/test_hop_coverage.py`, the four P1-02 tests |
| P1-03 | A record pending at a later chain edge was not judged again before the full sweep | CLOSED for NORMAL runs with key events (KI-07) | `tests/checks/test_chain.py`, the three P1-03 tests |
| O6 | ADR-08's concurrency cap N was not enforced, and a cancelled check could keep starting Spark jobs | CLOSED at DRE's unit level (KI-04) | `tests/test_concurrency.py` (all); `tests/config/test_operations_config.py` (`concurrency-zero`, `grace-zero`) |

## Open, blocked and not started

| Id | Issue | Status | Owner / unblocks it | Where it is disclosed |
| --- | --- | --- | --- | --- |
| KI-01 | **Load test** (acceptance criterion 7): resource consumption, scan volume, elapsed time and whether the 40-minute run budget is met, at production volume | BLOCKED | Platform team: a representative cluster | Gate B evidence (local measurements only) |
| KI-02 | **R18** (aggregation through `dre run`, proof scenario 5) | NOT_STARTED | An applicable aggregate fixture or contract; no gold aggregate exists in the member pipeline | Traceability matrix PS5; checked at check level only |
| KI-03 | **R22** (file-level record counts, proof scenario 9) | BLOCKED | Release 4 step 2 (T1\_FILE\_RECORD\_COUNT), not built | Traceability matrix PS9; the cadence half is R11a/R11b |
| KI-04 | More than one Spark job inside one Spark action (a broadcast, a subquery) is not limited by DRE's cap, which counts DRE's units | OPEN | The YARN queue's share (a deployment value) | Spec section 8, "Concurrency"; decisions, final review O6 |
| KI-05 | Deployment values unset: `yarn_queue`, `protected_queues`, `kill_switch_file` | BLOCKED | The user and the platform team | `dre validate`; spec section 3 |
| KI-06 | AS\_OF\_REPLAY reads as of a cutoff only in HOP\_LEDGER; `snapshot_partitioned` has no reader; a hop with a rejects table is refused; no layer is confirmed `append_only` | OPEN | Platform: whether any layer is append-only | DID\_NOT\_RUN / no\_as\_of\_evidence with its reason; decisions M6 |
| KI-07 | Chain records pending at a later edge are carried only in NORMAL runs with an `hmac_secret_file`; otherwise they are judged by the full sweep | OPEN | Configuration (the secret) | The chain's scope (blind spot) |
| KI-08 | Coverage older than `dq_check_result`'s retention cannot be continued: the range restarts at the window and the span before it is judged only by the full sweep | OPEN | None (retention is a policy) | HOP\_LEDGER's scope, `coverage_basis: unresolved coverage gap ...` |
| KI-09 | `coverage_basis` is in HOP\_LEDGER's scope only; the other hop checks share the same range but have no scope JSON | OPEN | None | Spec section 6, Coverage |
| KI-10 | One store-test failure seen once under heavy CPU contention (three agents), not reproduced in isolation or in Gate A or B | OPEN (watch) | None | Gate A evidence (e) |
| KI-11 | The union attribution proof reads every row of the target, once in T1\_SOURCE\_ATTRIBUTION and once per union hop's ledger: cost unmeasured at volume | OPEN | KI-01 | Decisions S3 |
| KI-12 | HOP\_CONTRIBUTION is strict (a null reference is unresolved; tolerance not applied); whether the member pipeline has a join contribution is developer question 19 | OPEN | Developer | Decisions S5 |
| KI-13 | A store older than checkpoint 3's layout is reported by `dre install --check`, not migrated | OPEN | None (no store is deployed) | Decisions M1 |
| KI-14 | `dre validate` does not check a chain's terminal or that its path is unique: found at run time (invalid\_config) | OPEN | None | Decisions K9 |
| KI-15 | Python work inside a cancelled unit cannot be interrupted; while it lingers past `cancel_grace_seconds`, every later unit is refused (DID\_NOT\_RUN / concurrency\_limit, windows held): fail closed, possibly many checks in one run | OPEN | None | Spec section 8, "Concurrency" |
| KI-16 | Ratification pending: the revised checkpoint 3, Gate A (M1 to M9, S1 to S6), Gate B (K1 to K9, O1 to O7), the final corrections (N = 1, grace 60 seconds) | OPEN | The user | `docs/decisions.md` |
