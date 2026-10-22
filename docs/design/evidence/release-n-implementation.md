# Release N implementation evidence (Gate A remediation)

**Status: built and developer-verified. Not independently reviewed, not merged, not deployed.** Migration 2 ran only on local scratch stores created by the tests and the equivalence script. Release N+1 is not started.

## 1. Initial baseline

| Item | Value |
| --- | --- |
| Authorisation | The user, 2026-10-22: release N compatibility implementation, conditional on successful baseline CI. Release N+1, merging, shared-database migration and deployment are not authorised |
| Branch | `claude/claude-md-documentation-a2lxt0`, tracking origin; working tree clean |
| HEAD | `390b0bd` (= origin; the Step 0 specification closure) |
| Baseline CI | **#107 on `390b0bd`: success** (jobs test, e2e, package) |
| Baseline tests | 1001 collected (`pytest --collect-only -q`, run on a worktree of `390b0bd`) |

## 2. Requirements to code

| Requirement (spec, "Gate A remediation, release N") | Code | Tests |
| --- | --- | --- |
| Migration 2: `dq_key_event.key_encoding INT`, `predecessor_hash STRING`, real columns (spec §5 DDL) | `store/migrations.py` (`MIGRATIONS[1]`, `added_columns`); `store/ddl.sql` (`dq_key_event`); `docs/spec.md` §5 DDL | `tests/store/test_migration_2.py`: declared after migration 1; fresh store; existing store migrated without rewriting a row; reapply changes nothing; interrupted migration detected and completed. `tests/store/test_ddl.py` (spec DDL = built store), unchanged |
| Writer: a column a migration added may be absent from a frame (typed NULL); any other missing or extra column raises | `store/writer.py` `append` | `test_migration_2.py`: fills NULL; refuses another missing column; refuses an extra column; migration 1 tolerated too |
| Every key event written with `key_encoding = 1`, `predecessor_hash` NULL | `checks/keys.py` (`KEY_ENCODING = 1`); `store/key_events.py` `append_key_events`; `checks/hop/key_currency.py`; `checks/hop/chain.py` | `test_migration_2.py` (key_encoding 1; a frame naming its encoding keeps it); `tests/checks/test_open_keys_encodings.py::test_a_legacy_open_key_is_read_as_encoding_1_and_written_with_it` |
| Final `v_open_keys`: identity = encoding + hash; spell ends at CLEARED only; REKEYED never open; lineage-aware first flagged date; `category` | `store/ddl.sql` `v_open_keys` | `tests/store/test_views_key_encoding.py` (8 tests); `tests/store/test_views.py::test_open_keys`, unchanged |
| `v_key_lineage`: continuation / ambiguous; `rollback` kind deferred (INC-1); commit-gated | `store/ddl.sql` `v_key_lineage`; `store/schema.py` `VIEWS` | `test_views_key_encoding.py` (lineage kinds; uncommitted invisible) |
| Reading: this engine's encoding only, REKEYED as no event (HOP_KEY_CURRENCY and the chain carry) | `checks/sql/hop_common.sql.j2` `open_keys`; `checks/hop/chain.py` carry read | `test_open_keys_encodings.py`; `tests/checks/test_chain_encodings.py` |
| Another encoding open at the hop: judged as on the full sweep, closed with REKEYED "key encoding 1 (rollback)" in its own encoding; at most one event per identity | `checks/sql/hop_foreign_keys.sql.j2`; `checks/hop/common.py` (`foreign_params`, `foreign_open`, `rekeyed_back`); `key_currency.py` `_hop`, `_judge`; `chain.py` (first edge `sweep`, events) | `test_open_keys_encodings.py` (foreign identity; same hash text in another encoding; unknown encoding; uncommitted; no secret; one event per identity); `test_chain_encodings.py` (3) |
| `failure_ref` encoding by run | No change needed: a retry already writes its own engine's hashes (encoding 1) | Covered by the existing cause tests, unchanged |
| Trace labels | `trace.py` (`other_encodings`, `cause_lines`) | `tests/test_trace_encodings.py` (2); `tests/test_trace.py` and R05, unchanged |
| One NORMAL writer at a time, whatever the engine version (R2-D14) | No change (existing guard) | `tests/test_run_overlap_versions.py` (2) |

**Not built in release N:** encoding 2, the hash input change, untimed rows and the disorder bound, tied rows, unparseable target keys, the UTC start check, the transition. A grep confirms no encoding-2 key builder exists (`grep -rn "octet_length\|key_string_v1\|KEY_ENCODING = 2" src` returns nothing).

## 3. Changed files

- **Source** (11 changed, 1 new): `checks/hop/chain.py`, `checks/hop/common.py`, `checks/hop/key_currency.py`, `checks/keys.py`, `checks/sql/hop_common.sql.j2`, `checks/sql/hop_foreign_keys.sql.j2` (new), `store/ddl.sql`, `store/key_events.py`, `store/migrations.py`, `store/schema.py`, `store/writer.py`, `trace.py`.
- **Tests** (6 new files, 35 tests): `tests/store/test_migration_2.py` (12), `tests/store/test_views_key_encoding.py` (8), `tests/checks/test_open_keys_encodings.py` (8), `tests/checks/test_chain_encodings.py` (3), `tests/test_trace_encodings.py` (2), `tests/test_run_overlap_versions.py` (2). **No existing test changed.**
- **Documents:**
  - `docs/spec.md`: §5 DDL (real columns); release N text marked built; INC-1; TR-1 note.
  - `docs/decisions.md`: "Gate A remediation: release N built".
  - `docs/known-issues.md`: KI-16 ratification list.
  - `docs/handoff.md`.
  - This file, and `docs/design/evidence/release-n/equivalence.py`.

## 4. Migration design and execution evidence

- **Mechanism.** The existing additive migration mechanism (pending while any column is absent; `ALTER TABLE … ADD COLUMNS` in `store/migrations.py` only, guard rule 11; `dre run` refuses to start while a migration is pending).
- **Columns.** Migration 2 adds two nullable columns at the end of `dq_key_event`'s data columns, the same columns in the same order as a fresh store's DDL.
- **Stored rows.** They are never rewritten; NULL means encoding 1.
- **Executed only on scratch stores:**

| Case | Result |
| --- | --- |
| Fresh store | Both columns present, in DDL order; nothing pending (`test_a_fresh_store_has_the_final_key_event_columns`) |
| Existing store (release N−1 layout, two events) | `pending == [2]`; `missing_objects` reports "migration pending" (`dre run` refuses); `install --check` exits 1; `install --apply` exits 0 and prints "applied migration 2: dq_key_event ADD COLUMNS (key_encoding INT, predecessor_hash STRING)"; the two rows unchanged with NULL encoding; `install --check` exits 0; columns equal a fresh store's |
| Reapply | Exit 0; `apply_pending` applies nothing; rows unchanged |
| Interrupted (one column present) | Still pending, still refused; `--apply` adds only the absent column |
| Equivalence script, upgrade scenario (separate processes) | `install --check` before apply exits 1; `dre run` exits 3 (cannot start); after `--apply`, `install --check` exits 0 |
| Rollback below N | Unsupported, documented (spec §6 "Key events"; decisions); the release before N refuses a frame without the new columns (examination prototype m1, `docs/reviews/gate-a-remediation-design/revision-2/verification/test_migration.py`) |

## 5. Compatibility matrix (as built)

| Concern | As built | Evidence |
| --- | --- | --- |
| Encoding-1 records (NULL or 1) | Read as this engine's own; open-key semantics unchanged | `test_open_keys_encodings.py` (legacy), the full suite |
| Encoding-2 records | Never matched to an encoding-1 key, even with the same hash text; when open, the hop is judged in full and they are closed with REKEYED (encoding 2) | `test_open_keys_encodings.py`, `test_chain_encodings.py`, `test_views_key_encoding.py` |
| Unknown encoding | Handled like another engine's (ID-2) | `test_an_unknown_encoding_is_handled_as_foreign` |
| REKEYED | No event for the open-key read; never ends a spell; never open in the view | `test_rekeyed_is_no_event_for_the_open_key_read`, `test_rekeyed_is_not_open_and_does_not_end_a_spell` |
| Lineage age | A provable continuation keeps the predecessor's first flagged date; an ambiguous legacy identity keeps its own and is `legacy_unresolved` | `test_views_key_encoding.py` |
| Commit-gated visibility | Both views and both readers read `v_committed_key_event` only | `test_uncommitted_events_are_invisible_to_both_views`, `test_an_uncommitted_encoding_2_event_is_invisible` |
| Determinism | Latest event by `observed_at`, then `run_id`; at most one event per identity per evaluation; same results at 1 and 7 shuffle partitions | `test_latest_event_selection_is_deterministic_across_shuffle_partitions`, `test_at_most_one_event_per_identity` |
| Mixed versions | One NORMAL run at a time, whatever the version | `test_run_overlap_versions.py` |
| N+1 → N rollback | Readers ready (above); **end-to-end with real N and N+1 builds not demonstrated** (N+1 does not exist): a release N+1 merge gate | — |

## 6. Existing-store equivalence

**Command:**
```
PYSPARK_PYTHON=$PWD/.venv/bin/python .venv/bin/python docs/design/evidence/release-n/equivalence.py <worktree of 390b0bd> <scratch dir>
```
Exit 0, "EQUIVALENT".

- **How it was run.** As separate `dre` processes, on the e2e synthetic store, with an HMAC secret. Gold loses four synthetic keys, so key events exist.
- **What was compared.** Every result (state, reason, population, violations, observed with times masked, qualifications) and every key event (dataset, check, hop, key, event, detail).

| Scenario | Compared | Result |
| --- | --- | --- |
| Fresh store: baseline engine against release N | 50 results, 2 key events | identical; release N's events carry `key_encoding` 1, `predecessor_hash` NULL |
| Upgrade: baseline run 1, then baseline run 2 (control) against migration 2 + release N run 2 (three `--feed` runs) | 33 run-2 results, 2 run-2 key events (open keys judged again across the migration) | identical |

A first version of the script had no failing keys, so it compared no key events (vacuous on that point). It was corrected to remove four gold rows, and it now fails if a scenario has no key events to compare.

## 7. Focused tests

| Command | Result |
| --- | --- |
| `pytest tests/store/test_migration_2.py tests/store/test_views_key_encoding.py tests/checks/test_open_keys_encodings.py tests/checks/test_chain_encodings.py tests/test_trace_encodings.py tests/test_run_overlap_versions.py` | **35 passed** |
| The same 35 against the baseline engine (`PYTHONPATH=<worktree of 390b0bd>/src`) | **26 failed, 9 passed**: the 9 pin preserved behaviour (other missing or extra column refused; reapply idempotent; uncommitted invisible; no secret reads nothing; single-encoding trace unchanged; overlap guard ×2; the rollback-below-N statement) |
| `pytest tests/store tests/test_evaluation_modes.py tests/guard` (after the store layer) | 184 passed |
| `pytest tests/guard` (final) | 98 passed |
| Existing STRING hash test `tests/checks/test_hop.py::test_sql_key_hash_matches_python_hmac` | passed unchanged (in the full suite) |

## 8. Full regression, end to end, package

| Command | Result |
| --- | --- |
| `pytest -q -p no:cacheprovider -rs` (full suite, final source) | **1036 passed, 0 failed, 0 skipped**, 4507 s (1001 existing + 35 new) |
| `tests/e2e/build_synthetic.py`; `dre install --conf conf --apply`; `dre run --conf conf`; `tests/e2e/verify.py` | 0, 0, 0, 0; run COMPLETED, 41 of 41 checks written; verify: 51 results {PASSED 31, DID_NOT_RUN 20}, as in the last recorded runs |
| `scripts/package.py` | exit 0; the wheel and the py-files zip include `checks/sql/hop_foreign_keys.sql.j2` and the new `store/ddl.sql` |
| `spark-submit … dre_main.py validate --conf conf` | exit 0 |
| `spark-submit … install --conf conf --print` | exit 0; the DDL includes `dq.dq_run`, `key_encoding INT, predecessor_hash STRING`, and `v_key_lineage` |
| `spark-submit … dry-run --conf conf --feed provider_roster_monthly` | exit 0; "dre dry-run: 7 checks evaluated, 0 SQL fragment problem(s), nothing written" |
| `dre validate --conf conf/` | exit 0 |

## 9. Performance and correctness boundaries

| Boundary | As built |
| --- | --- |
| Unbounded scans | The other-encoding query reads `v_committed_key_event` with the same `run_date` bound as the open-key read (`events_since`; the chain's `carry_since`): no wider than before. It runs only where open keys are read (HOP_KEY_CURRENCY and the chain with a secret, NORMAL runs). Cost: one extra bounded aggregate per such hop evaluation. The full-sweep judgement runs only while another encoding's identities are open (after a rollback) |
| Duplicate authoritative events | At most one event per (encoding, hash) per evaluation (tested) |
| Nondeterministic latest event | `observed_at`, then `run_id`; no ties within an evaluation (tested at 1 and 7 partitions) |
| New false healthy PASS | None: no check result changes on an encoding-1 store (equivalence; full suite). The forced judgement only widens the population |
| Hidden coverage gaps | The check context's `full_sweep` is unchanged, so a rollback never resolves a CA-01 gap (ID-4) |
| Long-pending records (CA-02) | The chain carry keeps its bound; another encoding's PENDING records force the first edge's full judgement, never a silent drop |
| Untrusted evidence advancing coverage | Unchanged: only committed, non-INVALID events are read; coverage logic untouched |
| KI-18, KI-19 | Unchanged (the open-key read's bound and continuity logic untouched; the encoding filter narrows it) |

## 10. Known limitations

- **TR-1** (to ratify): trace labels appear only when the hop holds key events of another encoding (R05 and `test_trace.py` assert the single-encoding output exactly).
- **ID-1 to ID-4** (to ratify): implementation decisions within the approved text (`docs/decisions.md`).
- The equivalence evidence uses one synthetic store shape. The cluster load test (KI-01) remains a production gate.
- N+1 → N rollback is not demonstrated end to end (no N+1 exists): a release N+1 merge gate.

## 11. Remaining merge gates (release N)

1. CI green on the pushed release N commits (§12).
2. Independent review: re-run the 35 tests and the equivalence script; confirm no encoding-2 key is computed (grep in §2); review the views' SQL.
3. The user's ratification of ID-1 to ID-4 and TR-1.
4. Separate approvals for merging, migration 2 on a shared store, and deployment.

## 12. CI

#108 on `b72b45a` (source `079b6cb`): success (test, e2e, package). The independent review and its correction (F1, `2d27831`) are recorded in `release-n-final-acceptance.md`.
