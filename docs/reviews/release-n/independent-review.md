# Release N independent verification

**Scope:** Gate A remediation, release N (schema and compatibility preparation), reviewed against the approved specification: `docs/spec.md`, `docs/reviews/gate-a-step0/04-release-n-acceptance-criteria.md`, and `docs/reviews/gate-a-step0/final-approval-package/04-release-n-compatibility-matrix.md` (which governs where the two differ, for example T-N-6 moving to N+1).

**Independence (stated plainly):** this review was carried out by the same assistant session that implemented release N. The review did take a different method from the implementation. It wrote its own adversarial tests, executed the baseline code from a separate worktree, and re-derived each requirement from the approved text. But it does **not** meet the compatibility matrix §3.5 requirement for "a reviewer other than the implementer". That requirement is listed below as unverified.

## 1. Reviewed commit

| Item | Value |
| --- | --- |
| Branch | `claude/claude-md-documentation-a2lxt0` (local = origin) |
| Reviewed HEAD | `b72b45a` (evidence, decisions, handoff, known issues, equivalence script) |
| Reviewed source commit | `079b6cb` (source, tests, `docs/spec.md`) |
| Baseline | `390b0bd` (Step 0 specification closure; CI #107 success). Checked out as a separate worktree in the session scratchpad and run with `PYTHONPATH=<worktree>/src` |
| Working tree | Clean except for this review's own files under `docs/reviews/release-n/` |
| Diff against the baseline | 19 files, +911 / −62. Source: 12 files under `src/` (one new template). Tests: 6 new files, 35 tests. No existing test file changed (`git diff 390b0bd 079b6cb --stat -- tests` lists only new files) |
| CI on the reviewed commit | #108 on `b72b45a`. The e2e and package jobs passed. The test job's result is in §6 |

## 2. Acceptance criteria applied

These come from the compatibility matrix §3 and acceptance criteria §1 and §2:

- **Functional tests:**
  - T-MIG-1, migration;
  - T-MIG-2, views;
  - T-MIG-3, writer;
  - T-MIG-4, readers on a store that fixtures wrote as stand-ins for N+1;
  - T-MIG-6, chain;
  - T-MIG-8, overlapping versions;
  - T-N-5, trace.
- **Non-regression:**
  - N-R1: no existing test restated;
  - N-R2: equivalence;
  - N-R3: replays;
  - N-R4: guards;
  - N-R5: validate and package;
  - N-R6: CI.
- **Release N must not:**
  - compute encoding 2;
  - change F-02, F-05 or F-07 semantics, the qualification list, the configuration schema or any check result.
- **The user's eleven review items** (§3 below).

## 3. Requirement-by-requirement disposition

| # | Requirement | Code | Evidence | Disposition |
| --- | --- | --- | --- | --- |
| 1 | Migration 2 schema: `dq_key_event` + `key_encoding INT`, `predecessor_hash STRING`, at the end of the data columns, nullable | `store/migrations.py:45-47`; `store/ddl.sql` `dq_key_event` | `test_migration_2.py`; `test_ddl.py::test_table_matches_spec` (the spec DDL equals the built store); `install --print` in the package job | **Verified** |
| 2 | Fresh and existing stores | `migrations.py` (pending while any column is absent) | `test_migration_2.py`: a fresh store holds both columns; an existing store migrates without rewriting a row. Equivalence upgrade scenario: `install --check` gives 1 before apply and 0 after | **Verified** |
| 3 | Repeat and partial migration | Same | `test_migration_2.py`: reapplying changes nothing; a store holding one of the two columns is detected and completed | **Verified** |
| 4 | Writer compatibility | `store/writer.py:30-47` | `test_migration_2.py`: a migration-added column is filled with a typed NULL; any other missing or extra column raises. Adversarial: the **baseline** writer, loaded from the worktree, raises on a migrated `dq_key_event`, so rollback below N fails closed (executed, not only documented). See F3 | **Verified**, with observation F3 |
| 5 | `key_encoding = 1` persisted | `checks/keys.py` `KEY_ENCODING = 1`; `store/key_events.py:30-41` | `test_migration_2.py`; `test_open_keys_encodings.py::test_a_legacy_open_key_is_read_as_encoding_1_and_written_with_it`; equivalence: fresh and upgrade release N rows are all `(1, None)` | **Verified** |
| 6 | Encoding 1 and 2 interpretation | `hop_common.sql.j2:41-51` (`coalesce(key_encoding, 1) = 1`, REKEYED excluded); `hop_foreign_keys.sql.j2`; `checks/hop/common.py` (`foreign_params`, `foreign_open`, `rekeyed_back`); `key_currency.py:69-74`; `chain.py` carry | `test_open_keys_encodings.py` (8); `test_chain_encodings.py` (3). Adversarial: NULL and explicit 1 are one identity; the rollback sequence (legacy L flagged, then REKEYED by "N+1", then continued as S in encoding 2) leaves L open as encoding 1 with its spell intact and closes S with REKEYED "key encoding 1 (rollback)" in encoding 2. See F2 | **Verified**, with observation F2 |
| 7 | Final views (`v_open_keys`, `v_key_lineage`) | `store/ddl.sql:300-381` (`v_open_keys` from line 311, `v_key_lineage` from line 362) | `test_views_key_encoding.py` (8); `test_views.py::test_open_keys` (unchanged). Adversarial: **F1 reproduced** (an open key is missing from `v_open_keys`) | **Defect F1** |
| 8 | Deterministic selection | `ddl.sql` `latest` (`observed_at DESC, run_id DESC`); the same order in `hop_common.sql.j2:43`, `hop_foreign_keys.sql.j2:13`, `chain.py:354`; at most one event per identity per evaluation | `test_open_keys_encodings.py` (one event per identity); the evidence §9 test at 1 and 7 partitions. The F1 tie is decided deterministically: the defect is that the spell join drops the row, not that the order is unstable | **Verified** |
| 9 | Existing-store equivalence (N-R2) | — | `docs/design/evidence/release-n/equivalence.py`, re-run by this review: **EQUIVALENT**. Fresh: 50 results and 2 key events compared. Upgrade: 33 run-2 results and 2 key events compared. Adversarial: on 30 ordinary synthetic histories, baseline and release N `v_open_keys` agree exactly on every baseline column. **F1 is a case where they differ** | **Defect F1** (otherwise verified) |
| 10 | N+1 → N contract | Readers (item 6); the overlap guard (`runner.py`, unchanged) | `test_open_keys_encodings.py`, `test_chain_encodings.py`, `test_run_overlap_versions.py` (2), plus the adversarial rollback sequence. All of this is at **unit and fixture level only**: no N+1 engine exists, so the end-to-end N+1 → N demonstration was **not performed**. It remains a release N+1 merge gate | **Verified at unit and fixture level; end-to-end NOT VERIFIED** |
| 11 | CA-01, CA-02, KI-18 and KI-19 protection | `ctx.full_sweep` unchanged (ID-4): the foreign sweep sets only the hop's params; chain carry bound unchanged; the open-key read's `events_since` bound unchanged | `test_coverage_gap.py`, `test_hop_coverage.py`, `test_open_keys_idle.py`, `test_chain.py`, `test_evaluation_modes.py` re-run unchanged (§5). `test_chain_encodings.py::test_an_open_encoding_2_pending_record_makes_the_first_edge_judged_in_full_and_is_closed` (no PENDING record silently dropped) | **Verified** |

**No encoding 2 is computed.** `grep -rn "octet_length\|key_string_v1\|KEY_ENCODING = 2" src` finds nothing. `checks/keys.py` `key_string` and `register_key_hash` are unchanged: the diff only adds the `KEY_ENCODING` constant. `tests/checks/test_hop.py::test_sql_key_hash_matches_python_hmac` passes unchanged, so STRING hashing behaves as before. The A-14 guard is release N+1's.

## 4. Findings

Severities follow the brief: P0 is data corruption; P1 is incorrect reconciliation, evidence loss, an incompatible migration or a false healthy PASS; P2 is a bounded reliability or operational concern; P3 is maintainability. Only P0 and P1 block, unless a mandatory merge gate requires a P2.

### F1 · `v_open_keys` drops an open key when CLEARED and the re-FLAGGED event share `observed_at` · P2, escalated to blocking (equivalence merge gate)

- **Evidence:**
  - `src/hcsc/datalake/dre/store/ddl.sql:329`: a spell is built only from events with `e.observed_at > c.last_cleared_at`, which is strict.
  - `ddl.sql:347`: the latest-event row is `JOIN spell s` (INNER).
  - Consider one identity with a CLEARED event and a later FLAGGED event at the same `observed_at`, written by two runs. Run order (`run_id DESC`) makes FLAGGED the latest event, so the key is open. But the FLAGGED event is not strictly after the CLEARED one, so the identity has no spell row, and the INNER JOIN removes it from the view.
  - The baseline view (`390b0bd`, `ddl.sql:299-321`) lists the key, with `first_flagged_at` NULL.
- **Reproduction:**
  - Test: `docs/reviews/release-n/adversarial/test_release_n_adversarial.py::test_defect_same_instant_cleared_and_reflagged_drops_an_open_key_from_v_open_keys`, run with `BASELINE=<worktree>`.
  - The baseline view returns `[("FLAGGED", None)]`; release N returns `[]`.
- **Impact:**
  - **Only the consumer view is affected.** The engine's own open-key read (`hop_common.sql.j2:41-51`) does not use the spell and still re-judges the key. No check result or key event changes; there is no evidence loss and no false PASS.
  - But a consumer reading `v_open_keys` would not see an open discrepancy.
  - `observed_at` is the evaluation's wall-clock `evaluated_at` (`runner.py:490`), so the trigger needs two runs with identical timestamps: rare (for example, a clock correction), but not impossible.
- **Why escalated:** compatibility matrix §2 says "Reader: N on encoding-1 store: identical behaviour to the previous release", and §3.3 makes equivalence a merge gate. Here the old and new views differ on an encoding-1 store.
- **Minimal correction (for the implementation owner):**
  - Make the spell join a `LEFT JOIN`, so the identity is listed with `first_flagged_at` NULL, exactly as the baseline does.
  - Changing the spell rule itself (for example, breaking the tie by `run_id`) would change behaviour against the baseline, so it is out of release N's scope.
  - Add a regression test beside `tests/store/test_views_key_encoding.py`.

### F2 · A retried cause investigation during a rollback window judges the whole hop · P2

- **Evidence:**
  - `runner.py:798` sets `judge_open_keys=True` for a retried investigation, which makes `params["use_open"]` true.
  - `key_currency.py:69-74` then finds the other encoding's open identities and sets `full_sweep` for the hop.
  - So the retry's failure list (the population the causes are investigated over) can be larger than the original evaluation's.
- **Reproduction:** `test_p2_a_retried_investigation_during_a_rollback_judges_the_whole_hop`. A plain recheck is `empty_population`; the retry is FAILED with population 1. `key_events` is None, so a retry writes no key event.
- **Impact:**
  - Causes only, and only while another encoding's identities are open (after an N+1 → N rollback, until the next NORMAL evaluation closes them).
  - No result, key event or evidence changes.
  - Extra cause rows name keys that were genuinely open.
- **Disposition:** not blocking. Documented as a known limitation. A fix would need a rule the approved text does not state.

### F3 · Writer tolerance also covers migration 1's `evaluation_mode` · P3

- **Evidence:** `writer.py:38-47`. `added_columns("dq_check_result")` includes `evaluation_mode`, so a result frame that omitted it would be written with NULL. `v_latest_result` would then derive the mode from `execution_type`, and an AS_OF_REPLAY written that way would read as CURRENT_RECHECK.
- **Reproduction:** `test_writer_tolerance_also_accepts_a_check_result_without_evaluation_mode`.
- **Impact:** latent only. No current writer omits the column (full suite), and the approved text asks for tolerance of any column `MIGRATIONS` added. Not a defect against the spec.

### F4 · `v_open_keys` aggregates every spell event per identity · P2 (operational)

- **Evidence:** `ddl.sql:324`, `element_at(array_sort(collect_list(named_struct(...))), 1)`. Memory per identity grows with spell length.
- **Impact:** only consumers query the view; the engine does not. Bounded by spell length times open identities. Belongs to the cluster load test (KI-01), which is already a production gate.

### F5 · `test_migration_2.py`'s rollback-below-N test checks the spec text only · P3

- **Evidence:** the test asserts the documented rule. It does not execute the previous writer.
- **Mitigation:** this review executed the baseline writer (§3 item 4).
- **Disposition:** documentation note only; no change needed.

### Not findings: ratification items

TR-1 (trace labels only when another encoding is present) and ID-1 to ID-4 are recorded in `docs/decisions.md` for the user's ratification. They stay inside the approved text. Ratifying them is the user's decision, not this review's.

## 5. Test evidence

| Run | Command | Result |
| --- | --- | --- |
| Adversarial | `BASELINE=<worktree> python -m pytest docs/reviews/release-n/adversarial` (from the repository root, so `tests` imports) | 8 passed. F1 and F2 reproduced; F3 observed; the baseline writer refuses a migrated store |
| Release N tests plus regression protection | `pytest` on the 6 release N files with `test_coverage_gap`, `test_hop_coverage`, `test_open_keys_idle`, `test_chain`, `test_views`, `test_ddl`, `test_evaluation_modes` and `test_hop.py::test_sql_key_hash_matches_python_hmac` | See §5.1 |
| Equivalence | `equivalence.py <baseline-worktree> <scratch>` | **EQUIVALENT** (exit 0). Fresh: 50 results and 2 key events. Upgrade: `install --check` gives 1 before apply; `dre run` gives 3 before apply; `install --check` gives 0 after; run 2 compared 33 results and 2 key events. Release N values are all `(key_encoding 1, predecessor_hash NULL)` |
| Release N tests on the baseline | The 35 new tests against `390b0bd` | 26 fail and 9 pass. The 9 pin preserved behaviour, so the new tests discriminate |
| Full suite (implementation evidence, not re-run by the review) | `pytest` | 1036 passed |

### 5.1 Combined run

**First invocation: 109 passed, 24 errors** (666 s; the 14 paths listed in §5, in that order).

**What the 24 errors were:** every one was `fixture 'store' not found` at setup:
- 4 in `tests/store/test_views.py`;
- 20 in `tests/store/test_ddl.py`.

**Diagnosis:** an argument-order artifact of pytest, not a defect.
- It occurs when a command line returns to `tests/store/` after a top-level `tests/` file, which loses `tests/store/conftest.py`'s module-scoped `store` fixture.
- `pytest --setup-plan` reproduces it in under a second.
- The **baseline** (`390b0bd`, none of release N's tests present) gives the same 20 errors for `tests/store/test_views.py tests/test_evaluation_modes.py tests/store/test_ddl.py`. So it predates release N and comes from the command, not from the code.

**Re-run of the affected files:** `pytest tests/store/test_views.py tests/store/test_ddl.py` on the reviewed code gives **28 passed**.

**Net result:** every selected test passes. (The full suite in default order passed 1036 at implementation.)

## 6. CI for the exact source commit

| Run | Commit | Jobs | Result |
| --- | --- | --- | --- |
| #107 (baseline) | `390b0bd` | test, e2e, package | success |
| **#108** (https://github.com/caneesh/data-reliability-engine/actions/runs/39770480974) | **`b72b45a`** (contains source commit `079b6cb`; no CI ran on `079b6cb` alone, because both were pushed together) | test (guard tests, `dre validate`, all tests, 11:26 to 12:26 UTC), e2e, package (spark-submit validate, `install --print`, dry-run) | **success, all three** |

CI therefore passes on the exact reviewed source. **CI does not catch F1**, because no existing test, the e2e run or the equivalence store produces a CLEARED event and a later FLAGGED event at the same instant.

## 7. Migration and rollback assessment

| Path | Assessment |
| --- | --- |
| Previous → N (apply migration 2) | Additive, nullable columns at the end; no row rewritten; idempotent; a partial migration is completed. `dre run` refuses (exit 3) until it is applied. Verified on test stores and in the equivalence upgrade scenario |
| N → previous, before migration 2 | Supported: no schema change |
| N → previous, after migration 2 | **Fails closed**: the baseline writer raises on `dq_key_event`, so no evidence is written. Unsupported by the approved text (N-O1); the release notes must say so (evidence §10 and decisions) |
| N on an encoding-1 store | Identical results and key events (equivalence), **except F1** in the consumer view |
| N+1 → N (rollback target) | Readers verified on fixture stores standing in for N+1: legacy spells continue, foreign identities are judged in full and closed with REKEYED, and nothing open is lost. **End-to-end not demonstrated** (no N+1 exists): a release N+1 merge gate |
| Concurrent N and N+1 | Prevented by the existing NORMAL-run overlap guard, which ignores engine version (`test_run_overlap_versions.py`) |

## 8. Remaining unverified conditions

1. **A reviewer other than the implementer** (compatibility matrix §3.5): not met by this review (see the independence note at the top).
2. **End-to-end N+1 → N rollback**: not performed; a release N+1 merge gate.
3. **Cluster-scale behaviour** (F4, KI-01): not tested; a production gate.
4. **The user's ratification** of ID-1 to ID-4 and TR-1.
5. **Separate approvals** for merging, for migration 2 on a shared store, and for deployment. None was sought or given here.

## 9. Recommendation

NOT READY — BLOCKING DEFECTS

F1 is the only blocking item. It is a P2 escalated because the existing-store equivalence merge gate requires identical old and new reader behaviour on an encoding-1 store. It has a one-line correction (§4, F1). F2 to F5 do not block. This recommendation is an engineering review result only. It is not organisational approval, merge approval or a production-readiness statement.
