# 09 · Revised implementation plan (revision 2)

**Nothing here is implemented.** This plan starts only after explicit approval of the revised design (10).

**How to read it:**
- Every change names its **file**, **function** and **reason**.
- **Verified** marks behaviour a prototype or the current code already shows.
- **Proposed** marks behaviour the implementation must still prove, with the test that will prove it.
- Line numbers refer to HEAD `c5e6237`.

**Order.**
1. Spec amendments (step 0).
2. Release N, which ships alone and must be green and deployed before N+1.
3. Release N+1 (Gate A).

Each step ends with the full suite and the e2e run (the standing rule). Tests are written first where practical. No existing test is weakened. A test is restated only where its rule changed, and the restatement is named in the step summary.

## Step 0: spec and decisions (documentation only)

| File | Change | Reason |
| --- | --- | --- |
| `docs/spec.md` §5 | Key string = LPB (with `!` form); `key_encoding`, `predecessor_hash`; event `REKEYED`; supported key types | R2-D1, D4, D7, D12 |
| `docs/spec.md` §6, the reconciliation contract | `disorder_bound_hours` and its approval; qualification `version_by_disorder_bound`; tie agreement; reject ambiguity; new labels | R2-D2, D3, D8, D9 |
| `docs/design/reconciliation-model.md` | Correct D-03 (ties are not ordered by `winner_rule`) | R2-D15 |
| `docs/decisions.md` | Record the answers to U-1 … U-6 | CLAUDE.md |

## Release N (preparatory, rollback target)

| # | File · function | Change | Reason | Verified / proposed | Test |
| --- | --- | --- | --- | --- | --- |
| N-1 | `store/migrations.py` · `MIGRATIONS` | Add `Migration(2, "dq_key_event", (("key_encoding","INT"), ("predecessor_hash","STRING")), …)` | R2-D12 (ship the schema in N, so rollback needs no schema change) | Mechanism verified (migration 1; `pending`) | T-MIG-1: a checkpoint store migrates without rewriting a row (pattern of `test_evaluation_modes.py:60`) |
| N-2 | `store/ddl.sql` · `dq_key_event`, `v_open_keys` | Add the two columns; the view: REKEYED latest is not open; one-level lineage for `first_flagged_at`; project `key_encoding`, `predecessor_hash` | R2-09 (age continuity) | Proposed | T-MIG-2: age is preserved across REKEYED (1:1); not inherited (1:n) |
| N-3 | `store/writer.py` · `append` | Tolerate a missing column only if `MIGRATIONS` added it to that table (fill a typed NULL); everything else still raises | R2-09, Q7 | Verified as a prototype (`test_m1`, `test_m2`) | T-MIG-3: the m2 cases against the real writer; the guard tests stay green (one write path) |
| N-4 | `store/key_events.py` · `append_key_events` | Select `key_encoding` (constant 1 in N) and `predecessor_hash` (NULL unless given) | R2-D12 | Proposed | T-MIG-3 |
| N-5 | `checks/sql/hop_common.sql.j2` · `open_keys` (:36-50) | Filter `coalesce(key_encoding, 1) = 1`; take the latest event excluding `REKEYED`; a second CTE lists foreign (encoding 2) open hashes | R2-D13 | Verified as a state machine (`test_m5`) | T-MIG-4: rollback keeps every open discrepancy visible (real store, synthetic events) |
| N-6 | `checks/hop/common.py` (the function that builds the open-key parameters, :100-112) and `runner.py` · the context builder (:390-399) | When foreign open hashes exist for a hop, set `force_full_sweep`; write `REKEYED 'key encoding 1 (rollback)'` for them in the same evaluation | R2-D13 | Proposed (the forced-sweep path exists: `force_full_sweep`) | T-MIG-4 |
| N-7 | `checks/hop/chain.py` · the PENDING carry (:338-345) | The same encoding filter; foreign PENDING forces the chain's recheck | R2-D13 | Proposed | T-MIG-6 |
| N-8 | `checks/hop/key_currency.py`, `hop_key_currency.sql.j2` | Accept `REKEYED` as a non-closing event (no other change) | R2-D12 | Proposed | T-MIG-2 |
| N-9 | `trace.py` · the key-event lookup (:115-123) | Label events by encoding; encoding 2 shown as "written by a later engine" | R2-D13 | Proposed | T-MIG-5 |
| N-10 | `tests/` | T-MIG-8: a STARTED NORMAL run of another `engine_version` blocks `dre run` | R2-D14 (pin the existing guard) | Existing behaviour (read) | T-MIG-8 |

**Release N "done when":**
- the full suite and e2e are green;
- T-MIG-1 to T-MIG-5 and T-MIG-8 pass;
- N in production writes `key_encoding = 1` on every new event.

## Release N+1 (Gate A)

| # | File · function | Change | Reason | Verified / proposed | Test |
| --- | --- | --- | --- | --- | --- |
| P-1 | `checks/keys.py` · `key_string` (:119-122) | LPB over `(expr, raw)` pairs; `KEY_ENCODING = 2`; new `canonical_text(expr, dtype)` (02 §3); `key_string_v1` keeps today's body | F-02, R2-D1, D7 | Verified (l1–l5, g01b) | T-F02-1 … 5 (port of a19, a20, l05, c05 + the l1/l2/l3 properties against the engine) |
| P-2 | `checks/keys.py` · new `unsupported_key_parts(schema, columns)` and its callers before rendering (`ledger.py`, `key_currency.py`, `value_agreement.py`, `chain.py`, `contribution.py`) | DID_NOT_RUN `invalid_config` for refused types | R2-D4 | Proposed | T-F02-6: a DOUBLE / ARRAY / BINARY key part is DID_NOT_RUN with the fix named; a supported type is unaffected |
| P-3 | `checks/hop/common.py` · `register_key_hash`, `key_hash`; new `hashed(expr)` | UDF over bytes; every template calls `hashed(k)` (via `hash_fn`: `common.py:109`, `chain.py:455`, `probes.py:218`) | R2-03, R2-D5 | Verified (l4, l8) | T-F02-7: two keys differing only in invalid bytes get different hashes; a valid key's hash equals `HMAC(secret, k)` |
| P-4 | every `key_string(` call site: `common.py:96, 100`; `ledger.py:291, 294, 348, 366`; `chain.py:196-197`; `contribution.py:86, 89`; `probes.py:204`; `merge.py:137-138`; `trace.py:123` | Pass the raw column for normalised parts (for the `!` form); no other change | F-02 (a partial switch would match nothing) | Proposed | T-F02-8 (a guard test): every call site passes through the encoding-2 renderer; only the remap and trace call `key_string_v1` |
| P-5 | `runner.py` · before any evaluation | Assert session time zone UTC, else DID_NOT_RUN `query_failed` | R2-D6 | Proposed | T-F02-9 |
| P-6 | new `checks/sql/hop_time.sql.j2` (macros) imported by `hop_ledger.sql.j2` and `hop_common.sql.j2` | Relevance (03 §3), due load (§3.1), labels, falsification guard (§3.2) | F-05, R2-D3 | Verified as a model (b01–b11, t1–t3, g06, g07) | T-F05-1 … 8 (port of l01, m04, b01–b06, b09–b11 against the engine) |
| P-7 | `checks/sql/hop_ledger.sql.j2` · `up_ranked`, `up_keys` (:59-100), `dn_*` (:101-131), `judged` (:200-217) | Per-row expected state; agreement count; signatures; total reject order (`rejected`, :184-189); the reason order of 04 §4 | F-07, R2-D8, D9 | Verified as a model (c01–c08, g02–g05) | T-F07-1 … 6 (port of l03, l04; c03–c08; soft + absent) |
| P-8 | `checks/hop/ledger.py` · the parameter builder (:280-300), `qualifications_of` (:156-165), `QUALIFICATIONS` (:120-131), the scope summary | Raw columns for the `!` form; D and the guard; the qualification `version_by_disorder_bound`; scope counts (set aside, refuted, unparseable target rows) | R2-D2, D10, D11 | Proposed | T-F05-9 (qualified, never healthy), T-DN-1 … 4 |
| P-9 | `checks/sql/hop_common.sql.j2` · `up_keys` / `dn_keys` (:80-95); `key_currency.py`, `value_agreement.py` | The same macros; UNASSESSABLE with the same labels; raw-variant guard | F-02, F-05 parity | Proposed | T-R1-1 … 3 (release 1 checks agree with the ledger on the same fixtures) |
| P-10 | `models.py` (dataset record-time model), `validate.py` | `disorder_bound_hours: float \| None` with an approval reference; validation errors name the file, line and field | R2-D3 | Proposed | T-CFG-1 … 3 (unapproved, non-positive, absent = strict) |
| P-11 | new `checks/hop/rekey.py` + `checks/sql/hop_rekey.sql.j2`; called where the open keys are read (`common.py`) and in `chain.py` | The transition remap (06 §3.4), written in the same evaluation and commit | R2-09 | Verified as a state machine (m3, m4, m6, m7) | T-MIG-5, T-MIG-6, T-MIG-7 (the remap on a real store: 1:1, 1:n, 1:0, interrupted, rerun) |
| P-12 | `store/key_events.py` · `append_key_events` | `key_encoding = 2`; `predecessor_hash` from the remap | R2-D12 | Proposed | T-MIG-7 |
| P-13 | `hop_common.sql.j2` · `open_keys` | N+1 reads encoding 2; REKEYED closes the legacy identity | R2-D12 | Verified as a state machine (m3) | T-MIG-7 |

**The examination's finding tests** (`docs/reviews/reconciliation-examination/adversarial/`: a19, a20, l05, c05, l01, m04, l03, l04) assert the **defects** as they exist at `c5e6237`. They are review evidence and stay unchanged. Against N+1 they are expected to fail, because the defects are gone. Their scenarios are ported into `tests/` (T-F02, T-F05, T-F07) asserting the fixed outcome.

**Release N+1 "done when":**
- the full suite, e2e and R01–R12 replay are green;
- every T-F02, T-F05, T-F07, T-DN, T-MIG and T-R1 test passes;
- the 4 approved-semantics tests (07 §3) pass unchanged;
- the end-to-end rollback demonstration (10, gate G-6) is recorded.
