# Release N final acceptance evidence (Gate A remediation)

**Status: release N engineering evidence, after the independent review and its correction.** This is not merged, not deployed, and not an organisational or production approval. Migration 2 ran only on local scratch stores created by the tests, the e2e run and the equivalence script. Release N+1 is not started.

## 1. Reviewed source commit

| Item | Value |
| --- | --- |
| Branch | `claude/claude-md-documentation-a2lxt0` |
| Baseline (before release N) | `390b0bd`, CI #107 success |
| Release N as reviewed | source `079b6cb`, docs `b72b45a`; CI #108 on `b72b45a` success (test, e2e, package) |
| Independent review | `docs/reviews/release-n/independent-review.md`, committed as `eb6102f` (CI #109 success, docs only). Recommendation: **NOT READY — BLOCKING DEFECTS** (F1) |
| Correction | `2d27831` (F1 fix and its regression test) |
| **Final source commit** | **`2d27831`**: CI in §8. This evidence and the handoff update are a later docs-only commit |

## 2. Review findings and dispositions

| Finding | Severity | Disposition | Reason |
| --- | --- | --- | --- |
| **F1** · `v_open_keys` drops an open key whose CLEARED event and later FLAGGED event share `observed_at` (`store/ddl.sql`, spell filter `>` and INNER JOIN to the spell) | P2, escalated to blocking by the existing-store equivalence merge gate | **Fixed** | Confirmed: reproduced against the baseline view (the adversarial test) and against the engine's own view (the regression test fails on `b72b45a`) |
| F2 · a retried cause investigation during an N+1 → N rollback judges the whole hop (`runner.py:798`, `key_currency.py:69`) | P2 | **Accepted, documented** as KI-21 | Causes only: no result, key event or evidence changes. A fix would need a rule the approved text does not state |
| F3 · writer tolerance also covers migration 1's `evaluation_mode` | P3 | **No change** | It is what the approved text requires ("a column `MIGRATIONS` added"), and no current writer omits it. Recorded in the review |
| F4 · `v_open_keys` collects every spell event per identity | P2 (operational) | **Accepted, documented** as KI-22 | Only consumers read the view. Measured with the cluster load test (KI-01) |
| F5 · `test_migration_2.py`'s rollback-below-N test checks the spec text only | P3 | **No change** | The review executed the baseline writer instead (adversarial test). Not a defect |
| The 24 errors in the review's first combined run | — | **Not a defect** | A pytest argument-order artifact (`fixture 'store' not found` when a command line returns to `tests/store/`). It reproduces on the baseline; the affected files pass (28) |
| TR-1, ID-1 to ID-4 | — | **Awaiting the user's ratification** | Not review defects (KI-16) |

## 3. Corrective change

`2d27831` makes one source change and adds one test:

- **`src/hcsc/datalake/dre/store/ddl.sql`, `v_open_keys`:** `JOIN spell s` becomes `LEFT JOIN spell s`, plus a two-line comment.
  - Effect: an open identity with no spell is listed with `first_flagged_at` NULL and `predecessor_hash` NULL, exactly as the pre-release-N view lists it.
  - Nothing else changes: an identity that has a spell matches as before. The predecessor joins key on `s.spell_predecessor`, so they match nothing for such a row.
  - The engine's own open-key read (`hop_common.sql.j2`) does not use the view and was never affected.
- **`tests/store/test_views_key_encoding.py::test_a_key_flagged_again_at_the_instant_it_was_cleared_stays_listed`** (new):
  - **On `b72b45a`:** fails with `KeyError: (1, 'g_tie')`, because the key is missing from the view.
  - **On `2d27831`:** passes.
- **No existing test changed.** No spec text changed: the spec defines the view's columns and the spell, not the join.
- **Changes considered and not made:** breaking the CLEARED/FLAGGED tie by `run_id` would give such a key a `first_flagged_at`. That would change behaviour against the baseline, so it is outside release N's scope.

## 4. Commands and exit codes (final source `2d27831`, local)

| Command | Exit | Output |
| --- | --- | --- |
| `pytest tests/store/test_views_key_encoding.py::…stays_listed` with the fix stashed (`b72b45a`'s view) | 1 | `KeyError: (1, 'g_tie')` |
| `pytest tests/store/test_views_key_encoding.py` | 0 | 9 passed |
| `pytest -q -p no:cacheprovider -rs` (full suite) | 0 | **1037 passed**, 0 failed, 0 skipped, 4459 s |
| `pytest tests/guard` | 0 | 98 passed |
| `tests/e2e/build_synthetic.py`; `dre install --conf conf --apply`; `dre run --conf conf`; `tests/e2e/verify.py` | 0, 0, 0, 0 | `dre run: COMPLETED, 41 of 41 checks written`; verify: 51 results {PASSED 31, DID_NOT_RUN 20} |
| `scripts/package.py` | 0 | the wheel, the py-files zip, the deps zip and `dre_main.py` |
| `spark-submit … dre_main.py validate --conf conf` | 0 | |
| `spark-submit … install --conf conf --print` | 0 | Contains `CREATE TABLE IF NOT EXISTS dq.dq_run`, `LEFT JOIN spell s` and `v_key_lineage` |
| `spark-submit … dry-run --conf conf --feed provider_roster_monthly` | 0 | `dre dry-run: 7 checks evaluated, 0 SQL fragment problem(s), nothing written` |
| `dre validate --conf conf/` | 0 | |
| `equivalence.py <baseline worktree> <scratch>` | 0 | **EQUIVALENT** (§5, G7) |
| `BASELINE=<worktree> python -m pytest docs/reviews/release-n/adversarial` | 1 (expected) | 7 passed, 1 failed. The failure is `test_defect_same_instant_…`, which pins the defective `b72b45a` behaviour (`after == []`) and now finds the key listed (`[Row(key_hash='b1')]`). It is the review's record of the old behaviour; the regression test in `tests/` is the guard |

## 5. Merge gates and required evidence

| # | Gate | Evidence | Status |
| --- | --- | --- | --- |
| G1 | Migration 2 on a fresh store and an existing store | `test_migration_2.py`: `test_a_fresh_store_has_the_final_key_event_columns`, `test_an_existing_store_is_migrated_without_rewriting_a_row`. Equivalence upgrade: `install --check` gives 1 before apply and 0 after; `dre run` gives 3 before apply | **VERIFIED** |
| G2 | Reruns and partial failure of the migration | `test_reapplying_migration_2_changes_nothing`; `test_a_migration_interrupted_after_one_column_is_detected_and_completed` | **VERIFIED** |
| G3 | Writer tolerance | `test_the_writer_fills_a_migration_added_column_with_null`, `…still_refuses_any_other_missing_column`, `…still_refuses_an_extra_column`, `…tolerates_migration_1_too`. Adversarial: the baseline writer refuses a migrated store (fails closed) | **VERIFIED** |
| G4 | `key_encoding = 1` persisted | `test_key_events_are_written_with_key_encoding_1`; `test_a_legacy_open_key_is_read_as_encoding_1_and_written_with_it`. Equivalence: every release N row is `(1, NULL)` | **VERIFIED** |
| G5 | Encoding-aware readers | `test_open_keys_encodings.py` (8), `test_chain_encodings.py` (3), `test_trace_encodings.py` (2) | **VERIFIED** |
| G6 | STRING hash behaviour unchanged | `tests/checks/test_hop.py::test_sql_key_hash_matches_python_hmac` unchanged and passing. `key_string` and `register_key_hash` untouched (the diff only adds `KEY_ENCODING`). `grep -rn "octet_length\|key_string_v1\|KEY_ENCODING = 2" src` finds nothing | **VERIFIED** |
| G7 | Existing-store equivalence (N-R2) | See the note below the table | **FIXED AND VERIFIED** (F1) |
| G8 | View behaviour | `test_views_key_encoding.py` (9, including the F1 regression); `tests/store/test_views.py::test_open_keys` unchanged; adversarial `test_null_and_explicit_encoding_1_are_one_identity` | **FIXED AND VERIFIED** (F1) |
| G9a | N+1 → N reader compatibility, unit and fixture level | Fixture stores standing in for N+1: `test_an_open_encoding_2_identity_is_judged_by_a_full_sweep_and_closed_with_rekeyed`, `test_rekeyed_is_no_event_for_the_open_key_read`, `test_an_open_encoding_2_pending_record_makes_the_first_edge_judged_in_full_and_is_closed`; adversarial `test_rollback_sequence_n1_continuation_then_release_n`; `test_run_overlap_versions.py` (2) | **VERIFIED** (unit and fixture level only) |
| G9b | N+1 → N rollback, end to end (real N+1 build, then release N) | Not performed: no N+1 engine exists. A release N+1 merge gate (decisions, A-15r) | **NOT VERIFIED** |
| G10 | No regression to coverage continuity (CA-01), key currency (KI-18, KI-19), chain carry (CA-02) or evidence integrity | Full suite 1037 passed with no existing test changed, including `test_coverage_gap.py`, `test_hop_coverage.py`, `test_open_keys_idle.py`, `test_chain.py`, `test_commit_protocol.py`, replays R01 to R17, R19, R20 and R23, and guard tests 98 | **VERIFIED** |
| G11 | Full regression, e2e, package | §4 and §6 | **VERIFIED** |
| G12 | CI on the final source commit | §8 | **VERIFIED** |
| G13 | Independent verification by a reviewer other than the implementer (compatibility matrix §3.5) | The review was carried out by the implementing session | **NOT VERIFIED** |
| G14 | The user's ratification of ID-1 to ID-4 and TR-1 | Pending (KI-16) | **NOT VERIFIED** |
| G15 | Separate approvals: merging, migration 2 on a shared store, deployment | None sought or given | **NOT VERIFIED** (outside engineering acceptance) |

**G7 in detail:**

- The script was re-run on `2d27831` against the `390b0bd` worktree.
- **Fresh:** 49 results and 2 key events compared, identical.
- **Upgrade:**
  - `install --check` gives 1, then `dre run` refuses with 3, then `install --apply`, then `install --check` gives 0;
  - run 2: 33 results and 2 key events compared, identical.
- The fresh count was 50 in the earlier runs. Which windows are due depends on the wall-clock time of the run, and both engines ran at the same time on identical stores.
- The F1 case itself is now the baseline's row, shown by the regression test and the adversarial reproduction.

## 6. Full regression counts

| Suite | Count |
| --- | --- |
| Before release N (`390b0bd`) | 1001 |
| Release N as reviewed (`079b6cb`) | 1036 (1001 + 35 new) |
| **Final (`2d27831`)** | **1037 passed** (1036 + the F1 regression test), 0 failed, 0 skipped |
| Existing tests changed by release N or the correction | 0 |

## 7. End to end and package

- **e2e:** COMPLETED, 41 of 41 checks written; 51 results {PASSED 31, DID_NOT_RUN 20}, as in every recorded run since the Phase 1 steps. Emails not sent (no SMTP host in the sample config).
- **Package:** artifacts built. `spark-submit` from the py-files zip: validate, `install --print` (including the corrected view) and dry-run ("7 checks evaluated") all exit 0.
- CI's `package` job repeats this on a runtime with only PySpark and the dependency wheelhouse installed (§8).

## 8. CI

| Run | Commit | Content | Result |
| --- | --- | --- | --- |
| #107 | `390b0bd` | baseline | success |
| #108 | `b72b45a` | release N as reviewed | success (test, e2e, package) |
| #109 | `eb6102f` | independent review (docs) | success |
| **#110** | **`2d27831`** | **final source: the F1 correction** | **success**: test (guard tests, `dre validate`, all tests, 13:26 to 14:24 UTC), e2e, package. https://github.com/caneesh/data-reliability-engine/actions/runs/39786688470 |

## 9. Limitations

- **N+1 → N rollback:** shown at unit and fixture level only, using stores written by test fixtures as stand-ins for N+1. **The full N+1 → N rollback has not been demonstrated.**
- **Independence:** the review was not carried out by a reviewer other than the implementer (G13).
- **KI-21 (F2):** a retried investigation during a rollback can investigate more keys than the original evaluation failed on.
- **KI-22 (F4):** the view's cost at volume is unmeasured; this belongs to the cluster load test (KI-01, a production gate).
- **One synthetic shape:** the equivalence evidence uses one synthetic store shape.
- **Not this release's work:** release N computes encoding 1 only. Every behavioural change for F-02, F-05 and F-07 is release N+1's.

## 10. Merge recommendation

**CONDITIONALLY READY**

- **Engineering evidence:**
  - the one blocking finding (F1) is fixed, with a test that fails on the reviewed commit and passes now;
  - the full suite, e2e, package, equivalence and CI on the final source commit are green;
  - no existing test was changed.
- **Conditions before release N engineering acceptance:**
  - G13: independent verification by a reviewer other than the implementer;
  - G14: the user's ratification of ID-1 to ID-4 and TR-1.
- **Separate approvals (outside engineering acceptance):** merging, migration 2 on any shared store, and deployment (G15).
- **Not covered here:** G9b, the end-to-end N+1 → N demonstration, is a release N+1 gate, not a release N one.

This is an engineering recommendation only. It is not organisational approval, merge approval or production readiness.
