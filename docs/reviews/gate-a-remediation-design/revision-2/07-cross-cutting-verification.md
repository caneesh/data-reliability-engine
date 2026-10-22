# 07 · Cross-cutting verification (revision 2)

All runs used local-mode Spark 3.5.1 and Python 3.11 (`.venv`, `PYSPARK_PYTHON=$PWD/.venv/bin/python`), on HEAD `c5e6237` with an unmodified working tree (§4).
- **What the prototypes are:** models of the proposed rules and failure injection on the real writer. They are **not** engine code.
- **What a pass means:** the proposed rules behave as specified on the model. It does **not** mean the engine has been changed or verified.

## 1. Commands and results

| Run | Command | Result |
| --- | --- | --- |
| All prototypes: revision 2, the original Gate A design, and the examination | `pytest -q -p no:cacheprovider docs/reviews/gate-a-remediation-design/revision-2/verification docs/reviews/gate-a-remediation-design/verification docs/reviews/reconciliation-examination/adversarial` | **116 passed in 288.32 s** |
| The same set, before `test_l8` and `test_disorder_guard.py` were added | (same command, run twice) | 112 passed in 286.51 s; 112 passed in 291.63 s |
| New prototypes alone | `pytest … revision-2/verification/test_disorder_guard.py …/test_canonical_key_lp.py::test_l8_a_binary_input_udf_hashes_the_raw_bytes` | 4 passed in 17.67 s |
| Existing approved-semantics tests, on current code | `pytest -q tests/checks/test_hop.py::test_value_agreement_compares_the_tied_latest_rows_as_sets tests/replay/test_r04_to_r07_hops.py::test_r06_tied_open_and_terminated_rows_is_value_agreement_failed tests/checks/test_hop_ledger.py::test_no_record_time_and_no_key_are_unassessable tests/checks/test_hop_ledger.py::test_an_identity_hop_requires_one_record_on_each_side` | **4 passed in 81.97 s** |

**The full suite was not run.** No production code changed (§4), so it would only repeat CI #104 on `c5e6237` (green).

## 2. Revision-2 prototypes (57 tests)

| File | Tests | What they establish |
| --- | --- | --- |
| `test_canonical_key.py` | k1–k8 (8) | **Against the original JSON design:** R2-01 (the mirror's escape case), injectivity over valid strings, v1 split collisions (k3), and the type hazards: time zone (k4), numeric rendering (k5), array and binary (k6), NFC/NFD (k7). Also that `get_spark` forces UTC (k8) |
| `test_canonical_key_lp.py` | l1–l8 (8) | **For LPB:**<br>• exact mirror and injectivity (l1);<br>• split, NULL and empty pairs (l2);<br>• invalid UTF-8 kept distinct where JSON collides (l3);<br>• a hash over `k` collides while one over `hex(k)` does not (l4);<br>• the ORC round trip (l5);<br>• cost (l6);<br>• a JSON contrast (l7);<br>• a bytes-input hash, which is collision-free and equal to `HMAC(secret, k)` for valid keys (l8) |
| `test_rules_model.py` | b01–b11, c01–c08, d01–d04, g01, g01b, g02–g07 (31) | Temporal rule (03), ties and rejects (04), downstream (05), combined failures, the coverage invariant, and determinism across shuffle partitions |
| `test_disorder_guard.py` | t1–t3 (3) | D kept on an in-order feed (the old row set aside); D refuted by a backfill, so the evaluation is strict; disorder within D does not refute it |
| `test_migration.py` | m1–m7 (7) | The **real** writer fails after an `ADD COLUMNS` (m1). Then the prototypes: tolerant append (m2); remap with lineage, fan-out and no successor (m3); interruption and idempotent rerun (m4); rollback (m5); re-upgrade (m6); auditability (m7) |

## 3. Invariants checked

| Invariant | Evidence |
| --- | --- |
| Distinct key tuples give distinct keys (STRING domain, including invalid bytes) | l1, l2, l3, l5 |
| Distinct keys give distinct hashes (bytes reach the HMAC) | l4, l8 |
| No untimed row of a candidate key is lost: each is relevant (its key unassessable) or set aside and counted | g06 (3 seeds) |
| No PASS-class disposition on a key with relevant untimed rows | g06 |
| Exactly one line per candidate key | g06 |
| An unparseable key part never merges with a NULL part, upstream or downstream | g01, g01b, d02 |
| Results are independent of shuffle partitioning | g07 |
| An interrupted transition leaves no visible state | m4 |
| Every open discrepancy survives upgrade, rollback and re-upgrade | m3, m5, m6 |
| Approved value-tie semantics, R06, the existing no-record-time rule and identity one-to-one are unchanged on current code | the 4 existing tests (§1) |

## 4. Repository state after this work

- `git status --short`: `?? docs/reviews/`. That is the examination, the original design and this revision, all untracked.
- `git diff --stat`: empty. **No tracked file changed:** no production code, test, configuration, spec or architecture decision.
- No commit, push or pull request.
- The only changes in this revision are new files under `revision-2/`. The original design (`../01`–`../08`, `../verification/`) and the examination are unchanged.

## 5. Limits of this verification

- **The prototypes are models.** `proto_ledger.py` carries one key part, one exclusion pair, no identity, soft-delete, superseded or load-time paths, and a simplified reject table. Every rule is re-proved in the engine by the tests of 09.
- **g06's independent count** encodes a malformed effective date as NULL, not with the `!` form. The generated population holds no genuinely NULL effective date, so the two coincide there. Stated, not hidden.
- **The migration prototypes** use a temp view with a `committed` flag in place of ADR-07's commit rows. The commit mechanism itself is existing, read behaviour (`ddl.sql:60-75`).
- **Cost figures** (l6) come from one laptop-class local session and vary run to run (02 §2.1). They are not a cluster measurement.
- **Not prototyped:**
  - the timestamp renderer and UTC guard;
  - type refusal;
  - soft-delete ties;
  - the chain remap;
  - the full-sweep target count;
  - the `v_open_keys` lineage SQL;
  - end-to-end rollback with real builds (gate C-5).
