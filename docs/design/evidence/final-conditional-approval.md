# Evidence package: final conditional approval, conditions CA-01 and CA-02

For independent review. This continues from the integrated checkpoints 3 to 8 and the final review corrections, at `81b9736` (its code is the same as `0c4fabd`, CI #98 green). It answers two correctness questions and nothing else:

- **CA-01:** can DRE report an unqualified healthy PASS while historical coverage is unresolved?
- **CA-02:** can a long-pending record silently drop out of chain reconciliation?

Both conditions were first established as defects with tests run on the unchanged engine code, then corrected with the smallest change that uses existing mechanisms: the C3-03 qualification and the P1-02 bounded read with a fallback. There is no new result state, store column, view or framework, and ADR-01 to ADR-08 are unchanged. Nothing here is approved, and nothing claims production readiness. Everything is synthetic.

Commands were run from the repository root with `PYSPARK_PYTHON=$PWD/.venv/bin/python` (local-mode Spark, Python 3.11, 4 CPUs). The quoted output is copied from logs kept in the session's scratch directory (`ca01-before.txt`, `ca02-before.txt`, `ca-before-final.txt`, `ca-after-focused2.txt`, `ca_gate_run1/`, `ca_gate/`).

**Classification:** CA-01 **FIXED AND VERIFIED**; CA-02 **FIXED AND VERIFIED**. See section C for the regression runs these rest on.

## A. CA-01: historical coverage gap

### Trace (before the correction, `81b9736`)

1. **How the range is calculated.** `checks/hop/coverage.py` `judged()`. The range ends at the window end minus `sla_hours`. It starts at the hop's latest trusted committed coverage found inside `history_lookback_days`. If none is trusted there, a second read reaches back to `dq_check_result`'s retention (P1-02). If only INVALID results are retained, it starts at the first evaluated window. If nothing is retained, it starts at this window.
2. **How a gap was represented.** Only as text: the `basis` string "unresolved coverage gap: ...", which `checks/hop/ledger.py` copied into HOP\_LEDGER's scope as `coverage_basis`.
   - The INVALID-only case ("the hop's first evaluated window") was not marked at all.
   - The same text also labelled every first evaluation in a deployed store, where no history is missing: a gap that cannot be acted on.
3. **How it propagated.** It did not.
   - HOP\_KEY\_CURRENCY, HOP\_VALUE\_AGREEMENT and HOP\_FILE\_COMPLETENESS took only `(start, end)` (`judged_range`, `checks/hop/common.py`).
   - CHAIN\_COMPLETENESS took only `(start, end)` and had no `coverage_basis` in its scope.
4. **Could the result be PASSED?** Yes. The state comes from the records judged in the range.
5. **Could it be `healthy = true`?** Yes.
   - `healthy` is `state = 'PASSED' AND evidence_state = 'VALID' AND qualifications IS NULL` (`store/ddl.sql`, `v_latest_result`).
   - A gap added no qualification.
6. **Did the digest report it?**
   - Not as a gap. The digest lists a PASS under "Passed with qualifications" only when it carries a qualification (`notify/digest.py`, `qualifications=` at line 335 and `if item.qualifications` at line 361).
   - A gap PASS was therefore counted among the healthy passes, and could be "all clear".
   - The watchdog (`watchdog.py`) alerts when the hourly run did not complete; it does not read result health, by design (spec section 8), so it reports no result-level condition.
7. **Could a later window hide it?** Yes. The next window continued from the gap result's `coverage_to`, and its basis was "committed coverage". The gap appeared once, in one result's scope text, and nowhere after.

### Reproducing scenario and result

`tests/checks/test_coverage_gap.py` (new), on a declared dedup hop whose clean PASS is healthy:

| Test | Scenario | Expected | Before (`81b9736` code) |
| --- | --- | --- | --- |
| `test_coverage_inside_the_lookback_leaves_a_clean_pass_healthy` | 1. Boundary inside the lookback | PASSED, no qualification, `v_latest_result.healthy` true | passed |
| `test_coverage_before_the_lookback_but_retained_leaves_a_clean_pass_healthy` | 2. Boundary older than the lookback, retained | Range from the old boundary (idle span judged), PASSED, no qualification | passed |
| `test_no_coverage_retained_is_a_qualified_pass_never_healthy` | 3. No boundary within retention | PASSED and qualified `coverage_gap`; claim "declared contract, qualified: ..."; healthy false. HOP\_KEY\_CURRENCY and HOP\_VALUE\_AGREEMENT qualified too | **failed**: `assert 'coverage_gap' in ()` |
| `test_a_hop_with_nothing_retained_in_a_store_older_than_the_retention_is_a_gap` | 3. Nothing retained for the hop, store older than the retention (coverage may have been dropped) | Qualified `coverage_gap` | **failed** |
| `test_a_first_evaluation_in_a_store_that_lost_nothing_is_healthy` | 3, the control. Nothing stored for the hop, store younger than the retention | Clean PASS, healthy, basis "first evaluation (...)" | **failed** on the basis label only (the old code called it an "unresolved coverage gap"); its health was already right |
| `test_invalid_evidence_with_trusted_coverage_before_it_leaves_a_clean_pass_healthy` | 4. INVALID result inside the lookback, trusted coverage before it | Range from the trusted boundary (the INVALID span judged again), clean PASS | passed |
| `test_invalid_evidence_with_no_trusted_coverage_retained_is_a_coverage_gap` | 4. Only an INVALID result retained | Qualified `coverage_gap` | **failed** |
| `test_later_windows_carry_the_gap_until_a_full_sweep_covers_the_hop` | 5. Normal runs after a gap | The next window continues (nothing judged twice) and stays qualified; a full sweep that was DID\_NOT\_RUN resolves nothing; a covering full sweep judges the key due before the gap's range and clears the qualification; the window after it is clean and healthy | **failed** at its first assertion (no qualification to carry) |

Chain: `tests/checks/test_chain.py::test_a_chain_without_retained_coverage_is_a_qualified_pass_until_the_full_sweep`.
- After the correction, the chain's first evaluation in a store that lost nothing is not a gap.
- Once the store is older than the retention, it is qualified, and the full sweep is not.
- Before the correction it **failed**: `KeyError: 'coverage_basis'` (the chain's scope had none), and it had no `coverage_gap`.

The before-run of the final tests is in section B: CA-01 and CA-02 were run together against the original engine code.

**Defect confirmed.** An unresolved gap could support an unqualified healthy PASS, and a later window erased it.

**A first attempt that went too far** (kept here because it bears on approved semantics).
- The first correction qualified every range that did not continue from trusted coverage, first evaluations included.
- Its full-suite run (section C, run 1) failed 7 existing tests, unchanged since before this work:
  - R14, R15 (three tests), R16 and R19 replays;
  - `tests/store/test_reconciliation.py::test_a_run_publishes_each_hop_ledger_with_its_result`.
- Each runs `dre run` once on a fresh store and expects the approved outcome: for example, PS2's contract-proven, healthy PASS (`assert ('coverage_gap', False) == (None, True)`).
- Those tests are right: a first evaluation in a store that has lost nothing misses no history. So the rule was narrowed, and the tests were not changed:
  - a gap is declared only where coverage may have existed (INVALID-only, or nothing retained in a store older than the retention);
  - the store's age is read from its partition list (`_may_have_dropped`).

### Correction

Source:

- `checks/hop/coverage.py`:
  - `GAP = "coverage_gap"`.
  - `Judged` gains `gap` and `in_lookback`, plus a `qualifications` property.
  - `_read()` returns the latest trusted coverage as one `named_struct` maximum: `coverage_to`, whether that row was qualified `coverage_gap` (on a tie of `coverage_to` a gapped row wins), and its `run_date`.
  - `judged()` sets `gap` when the range does not continue from trusted coverage that may have existed: the INVALID-only branch, and the "unresolved coverage gap" branch, now reached only when `_may_have_dropped()` finds a `dq_check_result` partition at or before `coverage_since`, so the store is older than the retention.
  - Otherwise a hop with nothing retained is a "first evaluation (the hop has no result, and the store has lost none to retention)", not a gap.
  - `judged()` also carries the gap when continuing from coverage whose result carried it, with the basis "committed coverage, continuing an unresolved coverage gap (judged by the next full sweep)".
  - A full sweep (`ctx.full_sweep`) never has a gap: it judges every record due before its range end.
  - A context without a history bound keeps the earlier "first evaluation" basis, unqualified. The runner always sets one.
- `checks/hop/ledger.py`:
  - The qualification `coverage_gap` is added to `QUALIFICATIONS`.
  - `ledger_params` passes `coverage_gap`.
  - `_hop` adds the qualification to the scope and the result, and restates the claim.
- `checks/hop/common.py`: `hop_params` passes the range's qualifications. `key_currency.py`, `value_agreement.py` and `file_completeness.py` set them on their result.
- `checks/hop/chain.py`: the chain uses `judged()`, adds the range's qualification to its qualifications, and states `coverage_basis` in its scope.

The qualification reaches health, the digest and the subject through the existing paths: `runner.result_rows` stores it, `v_latest_result.healthy` excludes it, `notify/digest.py` lists it, and P1-01 counts it.

After the correction: section C, all seven tests and the chain test pass.

## B. CA-02: long-pending chain records

### Trace (before the correction, `81b9736`)

1. **How PENDING is stored.** `checks/sql/chain.sql.j2`, the events mode, emits PENDING for a record pending at an edge other than the first. The runner appends it to `dq_key_event` under the evaluation's commit (ADR-07), and `v_committed_key_event` shows it unless the commit is INVALID.
2. **How pending records are selected.** `checks/hop/chain.py` `_judge` takes the latest committed event per `key_hash` for the chain and keeps those still PENDING. The first edge judges these whatever the range (`hop_ledger.sql.j2`, the `recheck` view).
3. **How the lookback limited it.** The read was `run_date >= history_since`. Every carrying evaluation records a still-pending record again, so the latest event stays inside the lookback while the chain keeps being evaluated.
   - A chain not evaluated for longer than `history_lookback_days` would not see its PENDING events at all, for example an intermediate table missing for weeks, or the chain refused or failing each run.
   - Its range continues from its coverage (P1-02) and has moved past the record's chain deadline.
   - So the record was not judged again before the full sweep, and not counted pending.
4. **How a record leaves the carry.**
   - RESOLVED: reached, or accounted for by an authorised disposition.
   - LOST or UNOBSERVABLE, as for any record.
   - PENDING again while it is still pending.
   - The latest event decides, so a terminal outcome is recorded once and the record is not carried after it.
5. **Replay and the full sweep.**
   - A recheck (CURRENT\_RECHECK) or AS\_OF\_REPLAY neither reads nor writes key state.
   - The full sweep reads the layers: its first edge takes every source record due before its range end, so it re-finds a still-pending record and records it PENDING again.
6. **Silent disappearance.** Yes, between the lookback lapsing and the next full sweep that completes. The chain PASSes in that span without the record, and nothing names it.

### Reproducing scenario and result

The scenario is in `tests/checks/test_chain.py`, CA-02 section.
- **Pending setup:** in window EVENT (2026-01-15/16), record 123402 is pending at curated -> gold and recorded PENDING.
- **The long gap:** the chain is next evaluated in FAR, 60 days later, with a 30-day lookback, so the PENDING event is older than the lookback.

| Test | Scenario | Expected | Before |
| --- | --- | --- | --- |
| `test_a_pending_record_younger_than_the_lookback_is_carried` | 1. Younger than the lookback | Carried (`carried_in` 1), reached | passed |
| `test_a_pending_record_older_than_the_lookback_is_carried_and_reaches_gold` | 2 and 3. Older than the lookback, then reaches gold | Carried; PASSED, due 1, reached 1; one RESOLVED; the next window carries nothing (empty\_population, `carried_in` 0) | **failed**: `assert 0 == 1` (`carried_in`) |
| `test_a_long_pending_record_that_never_reaches_gold_is_lost_at_the_first_failing_edge` | 4 and 6. Never reaches gold; repeated runs | FAILED, lost 1; `first_observed_gap` curated -> gold, with "not necessarily where the loss began" (no root cause invented); one LOST; two later windows carry nothing and record no event | **failed**: `('DID_NOT_RUN', None, None) == ('FAILED', 1, 1)` (the record was never judged) |
| `test_a_pending_record_whose_evidence_has_expired_is_still_judged_from_the_layers` | 5. PENDING evidence EXPIRED | Carried; the outcome comes from the layers: FAILED, lost 1 | **failed**: `carried_in` 0 (the lookback) |
| `test_a_pending_record_past_dq_key_event_retention_is_not_carried_says_so_and_the_full_sweep_judges_it` | 5. PENDING event past `dq_key_event`'s retention | Not carried (gone by the approved retention policy); the scope names the horizon date and "retention"; the full sweep judges it: FAILED, first gap curated -> gold | **failed**: `TypeError` (no retention bound existed) |
| `test_a_pending_record_recorded_by_invalid_evidence_is_judged_again_not_trusted` | 5. PENDING recorded by INVALID evidence | Not carried (untrusted), but EVENT's range is judged again from the layers: PASSED, due 2, reached 2 | passed |

Command and output: the final CA-01 and CA-02 tests against the original engine code. The working tree's `src/` was stashed, the tests were run, and the stash was restored; the restored diff was compared with a saved patch and found identical.

```
pytest -q -p no:cacheprovider tests/checks/test_coverage_gap.py tests/checks/test_chain.py \
  -k "gap or younger or older or long_pending or expired or past_dq or invalid_evidence or retained"
FAILED tests/checks/test_coverage_gap.py::test_no_coverage_retained_is_a_qualified_pass_never_healthy
FAILED tests/checks/test_coverage_gap.py::test_a_first_evaluation_in_a_store_that_lost_nothing_is_healthy
FAILED tests/checks/test_coverage_gap.py::test_a_hop_with_nothing_retained_in_a_store_older_than_the_retention_is_a_gap
FAILED tests/checks/test_coverage_gap.py::test_invalid_evidence_with_no_trusted_coverage_retained_is_a_coverage_gap
FAILED tests/checks/test_coverage_gap.py::test_later_windows_carry_the_gap_until_a_full_sweep_covers_the_hop
FAILED tests/checks/test_chain.py::test_a_pending_record_older_than_the_lookback_is_carried_and_reaches_gold
FAILED tests/checks/test_chain.py::test_a_long_pending_record_that_never_reaches_gold_is_lost_at_the_first_failing_edge
FAILED tests/checks/test_chain.py::test_a_pending_record_whose_evidence_has_expired_is_still_judged_from_the_layers
FAILED tests/checks/test_chain.py::test_a_pending_record_past_dq_key_event_retention_is_not_carried_says_so_and_the_full_sweep_judges_it
FAILED tests/checks/test_chain.py::test_a_chain_without_retained_coverage_is_a_qualified_pass_until_the_full_sweep
10 failed, 7 passed, 16 deselected in 124.18s (0:02:04)
exit=1
```

The failure lines:
- **CA-01:** `assert 'coverage_gap' in ()` (three tests); the first-evaluation basis label; and the chain's `KeyError: 'coverage_basis'`.
- **CA-02:**
  - `assert 0 == 1` (`carried_in`);
  - `('DID_NOT_RUN', None, None) == ('FAILED', 1, 1)`;
  - `assert (0 == 1)` (`carried_in`, with expired evidence);
  - `TypeError: ... 'key_events_since'` (no retention bound existed).

The 7 that passed show behaviour that was already correct, and still is:
- CA-01 scenarios 1, 2 and the first case of 4 (clean continuity is healthy);
- CA-02's younger-than-lookback carry and the INVALID re-judge;
- two step 6 chain tests the filter also selected (`test_a_record_lost_at_the_second_edge_fails_and_names_the_first_observed_gap`, `test_the_first_observed_gap_is_the_first_edge_that_lost_a_record`).

**Defect confirmed.** A record pending longer than the lookback was forgotten until the full sweep.

### Correction

Source:

- `checks/hop/chain.py`: the carry read is bounded by `carry_since`. That is `history_since` while the chain's coverage continues from inside the lookback (`Judged.in_lookback`), and `CheckContext.key_events_since` (`dq_key_event`'s retention) otherwise.
  - When the chain has been evaluated regularly, every outstanding record's latest event is inside the lookback, because each carrying evaluation records it again. When it has not, the read reaches back to retention.
  - This is P1-02's bounded read with a fallback, so the read stays cheap while continuity holds.
  - The blind spot names the horizon.
- `checks/base.py`: `CheckContext.key_events_since`.
- `runner.py`: `coverage_since(config, run_start, table)` serves both tables, and `check_context` sets `key_events_since` from `retention_months[dq_key_event]`.

Duplicate terminal outcomes and false PASS:
- Terminal events and the latest-event rule are unchanged (RESOLVED, LOST and UNOBSERVABLE stop the carry). The tests show no second outcome over repeated windows.
- A carried record is judged from the layers in this evaluation, so expired or untrusted evidence only nominates a key, never supports the claim.
- An INVALID evaluation's events are not trusted and its range is judged again.

After the correction: section C, all six CA-02 tests pass.

## C. Regression results

Both gate runs used the same script, on base commit `81b9736` plus the working tree of this change:
1. `pytest -q -p no:cacheprovider` (all tests).
2. The end-to-end check in an empty directory: `tests/e2e/build_synthetic.py`, `dre install --conf conf --apply`, `dre run --conf conf`, `tests/e2e/verify.py`.

Each exit code was written to a log.

**Focused, after the narrowed correction.** The CA-01 tests (8), the chain tests (25: step 6, P1-03, CA-02, and CA-01 for the chain), the coverage tests (14), and the 7 tests that run 1 failed:

```
pytest -q -p no:cacheprovider tests/checks/test_coverage_gap.py tests/checks/test_chain.py tests/checks/test_hop_coverage.py \
  tests/replay/test_r14_substituted_key.py tests/replay/test_r15_exclusions.py tests/replay/test_r16_defect_reject.py \
  tests/replay/test_r19_cdc_versions.py tests/store/test_reconciliation.py
62 passed in 591.30s (0:09:51)
```

**Run 1, the first correction** (every first evaluation a gap):

```
suite exit=1
7 failed, 984 passed in 3711.12s (1:01:51)
FAILED tests/replay/test_r14_substituted_key.py::test_r14_the_same_counts_with_the_right_keys_pass
FAILED tests/replay/test_r15_exclusions.py::test_r15_approved_exclusion_is_excluded_and_an_unapproved_filter_is_missing
FAILED tests/replay/test_r15_exclusions.py::test_r15_only_the_approved_exclusion_passes_and_is_contract_proven
FAILED tests/replay/test_r15_exclusions.py::test_r15_the_approved_exclusion_alone_under_a_merge_contract_is_healthy
FAILED tests/replay/test_r16_defect_reject.py::test_r16_an_authorised_reason_accounts_for_its_record_and_is_reported_evidence
FAILED tests/replay/test_r19_cdc_versions.py::test_r19_every_version_applied_passes
FAILED tests/store/test_reconciliation.py::test_a_run_publishes_each_hop_ledger_with_its_result
e2e: build 0, install 0, run 0, verify 0; 51 results {'PASSED': 31, 'DID_NOT_RUN': 20}; 41 of 41 checks written
```

The overreach showed in the e2e digests too: `DRE dev: 0 new failures, 14 checks did not run, 3 passed with qualifications`.

**Run 2, the final correction:**

```
suite exit=0
992 passed in 3733.90s (1:02:13)
build exit=0
install exit=0
run exit=0
verify exit=0
concurrency: at most 1 unit(s) of Spark work at a time; peak 1
email for membership-gold: DRE dev: 0 new failures, 14 checks did not run
email for provider-data: DRE dev: 0 new failures, 6 checks did not run, 1 passed with qualifications
dre run: COMPLETED, 41 of 41 checks written
e2e: 51 results {'PASSED': 31, 'DID_NOT_RUN': 20}; run ['COMPLETED']
```

- The suite grew from 977 tests (the final review corrections) to 992: 7 CA-01 tests in the new file and 8 in `test_chain.py` (6 CA-02, one CA-01 for the chain, one young-store first-evaluation control).
- No existing test was changed. The P1-03 helpers `chain_at` and `publish` gained optional parameters whose defaults keep the old behaviour.
- The e2e expectations are unchanged.
- The provider digest's one qualified pass is the sample's inferred-contract HOP\_LEDGER, as in the P1-01 golden file.

The areas the conditional approval asked to keep correct are all covered by this run, each by its own test files:
- HOP\_LEDGER dispositions and balancing (`tests/checks/test_hop_ledger*.py`);
- SLA continuity (`test_hop_coverage.py`);
- qualified PASS health (`tests/notify/test_digest.py`, `test_coverage_gap.py`);
- commit-gated visibility (`tests/store`, `tests/test_evidence_integrity.py`);
- source attribution and survivorship (`tests/checks/test_hop_ledger_sources.py`, `tests/checks/test_source_attribution.py`);
- chain completeness (`test_chain.py`);
- evaluation modes and replay (`tests/test_evaluation_modes.py`, `tests/replay`);
- digest and watchdog (`tests/notify`, `tests/replay/test_r12_watchdog.py`);
- concurrency and cancellation (`tests/test_concurrency.py`, `tests/test_evidence_integrity.py`).

**Commit and CI:** the commit carrying this change and its CI run are recorded in `docs/handoff.md`.

## D. Residual limitations

**Correctness defects:** none known open. CA-01 and CA-02 are corrected. Both corrections are defaults to ratify (KI-16, KI-18).

**Operational limitations** (`docs/known-issues.md`):
- KI-18: in a store older than the retention, a hop's genuine first evaluation is qualified `coverage_gap` until its first full sweep (fail closed). A store whose retained partitions all start after the retention's first day reads as having lost nothing.
- KI-07: the chain carry needs an HMAC secret. If the secret is unset for longer than the lookback while the chain keeps covering, records pending before then are carried no further.
- KI-19: HOP\_KEY\_CURRENCY's open-key read has the same lookback bound as the chain carry had. A hop not evaluated for longer can leave a recovered key open in `v_open_keys`: stale, not silent. Found while tracing CA-02, outside its scope, not changed.
- KI-08: coverage past retention is a qualified gap until the full sweep.
- KI-09: `coverage_basis` is only in HOP\_LEDGER's and the chain's scope. The other hop checks show the gap as their qualification.
- KI-04, KI-15, KI-17: concurrency.
- KI-11: the cost of the union attribution proof.

**Deferred capabilities:**
- R18 (aggregation through `dre run`) is **NOT_STARTED** (KI-02).
- R22 (file-level record counts) is **BLOCKED** on release 4 step 2 (KI-03).
- The load test is **BLOCKED** on a representative cluster (KI-01).
- None of these is counted as passed or completed.

**Production deployment prerequisites:**
- The deployment values `yarn_queue`, `protected_queues` and `kill_switch_file` (KI-05).
- An `hmac_secret_file` for key events and the chain carry.
- `history_lookback_days` and `retention_months` as approved.
- Ratification of the pending decisions (KI-16).
- The load test (KI-01).
