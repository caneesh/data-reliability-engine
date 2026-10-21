# 01 · Review findings on the Gate A design (revision 2)

**Scope.** This is an adversarial review of `docs/reviews/gate-a-remediation-design/` 01–08 (the original design), covering F-02 (key identity), F-05 (record times) and F-07 (ties).
- The original documents and the examination (`docs/reviews/reconciliation-examination/`) are preserved unchanged. This revision is written beside them.
- No production code, test, configuration or architecture decision was changed.

**Baseline.** Branch `claude/claude-md-documentation-a2lxt0`, HEAD `c5e6237`, working tree clean apart from the untracked `docs/reviews/`.

**Evidence.** Each finding names the prototype that shows it.
- Prototypes live under `revision-2/verification/`. They are verification code, not engine code, and run against local Spark 3.5.1.
- **Verified** means a prototype executed it.
- **Proposed** means design only.
- **Read** means established by reading the code at the cited lines.

## Verdict on the original design

The original design was **directionally sound but not implementable as written**.
- **Sound:**
  - It identifies the three defects correctly.
  - It fails closed.
  - It preserves approved semantics (value ties compare as sets, R06, spec §6 "Ties").
  - It keeps the evidence store append-only.
- **Not sound, on three load-bearing points:**
  1. **The chosen encoding** (`to_json(array(...))`) is not injective over Spark STRING values, and its Python mirror is not exact (R2-01, R2-02, R2-03).
  2. **The strict untimed rule** turns one historic bad row into a permanent inconclusive FAIL for that key, with no remedy short of fixing production data (R2-06).
  3. **The transition** (a forced full sweep with closing events) silently resets the age of every open discrepancy, because `v_open_keys.first_flagged_at` is per hash. Its rollback claim also fails against the real writer (R2-09, verified m1).

## Findings

| ID | Severity | Area | Finding | Evidence |
| --- | --- | --- | --- | --- |
| **R2-01** | High | F-02 | The original Python mirror (`json.dumps`) writes C0 controls as lower-case `\u000e`; Spark (Jackson) writes `\u000E`. The mirror differs from Spark on every tuple holding such a character, so "trace computes the same hash in Python" was false. | Verified: `test_canonical_key.py::test_k1` (5,000 seeded tuples) |
| **R2-02** | **Critical** | F-02 | `to_json` writes invalid UTF-8 bytes as U+FFFD. Two STRING values that differ only in invalid bytes (`X'FF'`, `X'FE'`) **collide** under the proposed encoding, although `CAST` alone keeps them apart. Encoding 2 is therefore not injective over the STRING domain Hive tables can hold, and a collision is a silent merge, the very F-02 defect. | Verified: `test_k6`, `test_canonical_key_lp.py::test_l3` (`json_equal = True`) |
| **R2-03** | High | F-02 | `key_hash` is computed by a Python UDF (`common.py:191-201`). A Python UDF receives a decoded `str`, so invalid bytes arrive as U+FFFD: an HMAC over the key string collides even when the key strings differ. The hash input must be ASCII (`hex(k)`). | Verified: `test_l4` (`on_k = True`, `on_hex = False`) |
| **R2-04** | High | F-02 | Original G-2 marked a key "unparseable" but still encoded the unparsed part as NULL. So (A, NULL) and (A, `'2026-13-01'`) share **one** key, and the genuine record (A, NULL) becomes collateral: unassessable for another record's defect. | Verified: `test_rules_model.py::test_g01b` (fixed by the `!` marker; it failed before the marker was added, as noted in 07) |
| **R2-05** | Medium | F-02 | Type hazards the original spec did not cover:<br>• `CAST(timestamp AS STRING)` depends on the session time zone. In a DST zone two instants render identically (k4). `get_spark` does force UTC on an existing session (k8), so the engine is safe today, but only by convention.<br>• ARRAY casts collide (k6).<br>• BINARY casts collide through `to_json` (k6).<br>• DECIMAL scale changes the rendering (`1.00` against `1`, k5).<br>• Floats are inexact (`0.30000000000000004`, `NaN`, k5).<br>• NFC and NFD forms are distinct keys (k7). | Verified: `test_k4`–`test_k8` |
| **R2-06** | High | F-05 | Original G-3 (every row of the key must be timed) has no expiry. A key with one malformed row two years ago, valid updates since and a correct gold stays `unassessable` (an inconclusive FAIL) **forever**. Fail-closed, but operationally unbounded, and Q2/Q3 were left as separate open questions although they are one decision. | Verified: `test_b02`; the remedy in `test_b03`–`test_b05` |
| **R2-07** | Medium | F-07 | Original G-5's MIN = MAX test covered tombstones and each exclusion **separately**. It did not say what happens when one tied row is deleted and the other is excluded (both mean "absent"). It also did not cover a version-lineage reject among tied rows that differ (the reject names a version, not a row). | Verified: `test_c03`–`test_c08` |
| **R2-08** | Low | Downstream | Original 02 §1.2 counted unparsed target rows in a scope figure when orphans are unchecked. Reading `ledger.py:105-118, 156-161`: in that case the result already carries the `orphans_unchecked` qualification and is never healthy, so no **false PASS** arises. The open question is reporting only (Q5). | Read; verified at model level (`test_d01`–`test_d03`) |
| **R2-09** | **Critical** | Migration | 1. The forced-sweep transition closes every legacy open hash and opens a new one. `v_open_keys.first_flagged_at` (`ddl.sql:299-322`) is per `key_hash`, so every open discrepancy's age resets to the upgrade day, and age-based escalation and digests silently lose history.<br>2. The rollback claim fails against the real writer: `store/writer.append` (`writer.py:19-39`) requires a frame to carry exactly the table's columns, so a release-N−1 engine cannot write to a migrated `dq_key_event` (verified m1). Release N must therefore carry more than writer tolerance. | Verified: `test_migration.py::test_m1`; the remedy in `test_m2`–`test_m7` |
| **R2-10** | Low | Migration | Two engine versions writing key events to one store during a rollout would interleave encodings. On reading the code this is **already prevented**: only NORMAL runs write key events (`checks/base.py:156-158`), and `dre run` refuses a NORMAL run while another NORMAL run is STARTED within the watchdog grace (`runs.in_progress`, `runner.py:1075-1082`), whatever its engine version. The residual risk is the existing one: a live run older than the grace counts as dead. No new fence is needed; one test should pin the guard across versions (T-MIG-8). | Read |

## What the original design got right (confirmed, kept)

- **Approved value-tie semantics are preserved.** Tied rows compare as sets (`test_value_agreement_compares_the_tied_latest_rows_as_sets`). R06 FAILs with cause `TIE_RESOLVED_BY_RULE`. The existing tests pass on the current code (07). Revision 2's tie rule (04) leaves value ties to these semantics.
- **`winner_rule` stays non-authoritative (Q1 = No).** This is consistent with spec line 649 ("Every upstream row at a key's latest record time is kept") and R06. Making it authoritative is a spec change, outside Gate A.
- **Record-level invalidity is a key outcome, not DID_NOT_RUN** (CLAUDE.md constraint 4: an exception or empty population is DID_NOT_RUN; a bad row is not).
- **Exclusions already carry their effective dates in the row condition** (`exclusion_terms`, `common.py:136-160`). So "each tied row is independently covered by an approved exclusion" (Q4) is a per-row fact the ledger already computes as `x_i`. Read.
- **Interrupted evaluations are invisible.** ADR-07 commit rows (`ddl.sql:60-75`): every current-state view reads committed rows only, so a transition written as one committed evaluation is atomic in effect. Read; modelled in m4.

## Documentation discrepancy

- **D-03 (carried from the examination).** The model document says ties are ordered by `winner_rule`; the ledger ignores `winner_rule` (only `causes/merge.py:149-163` reads it). Revision 2 keeps the code's behaviour, which matches the spec, and proposes correcting the model document's sentence in N+1. It does not change the code to match the document.
