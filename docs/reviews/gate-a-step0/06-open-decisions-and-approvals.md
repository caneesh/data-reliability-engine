# 06 · Open decisions and approvals

The provisional decisions U-1 to U-6 are **not** recorded here as approved. Each row says what is still needed before it becomes final.

## 1. The user's provisional decisions: state after review

| ID | Provisional decision | Review finding | Still needed |
| --- | --- | --- | --- |
| U-1 | A PASS relying on rows set aside under D is never fully healthy | Implementable with no new mechanism: a qualification flows to `v_latest_result.healthy` and to the digest (contract §3). Must cover every hop check and the chain (C-6) | Approve A-7 |
| U-2 | Binary key components are unsupported | Implemented as a refusal (DID_NOT_RUN / invalid_config). Extended, fail-closed, to INTERVAL, TIMESTAMP_NTZ and VOID | Approve A-1; O-2 |
| U-3 | Hash the canonical raw bytes with the existing algorithm and key management | HMAC-SHA256 and the secret contract are unchanged. Only the input definition changes, so every hash value changes once (R-8) | Approve A-2; O-3 acknowledged |
| U-4 | No age inheritance without provable identity continuity; preserve the original discrepancy and its history | **Revision 2 contradicted this** (C-1). Revised: continuity is proved by syntax only (L-3, 6 prototype tests pass); otherwise the legacy discrepancy stays open with its age (L-5) | Approve A-9 and A-10; decide Q-5 |
| U-5 | D defaults to NULL; a non-NULL D needs dataset evidence and owner approval | Implemented as the `disorder_bound` block with `approved_by`, `approved_on` and `evidence` (A-5); validation refuses an incomplete block | Approve A-4 and A-5; O-1 per dataset |
| U-6 | Spec amendments approved before dependent implementation | Respected: nothing is applied; 04 and 05 make approval a precondition | Your approval of 02 |

## 2. Approvals required

| ID | Approval | From | Blocks |
| --- | --- | --- | --- |
| S-1 | Amendments A-3, A-9, A-10 (reading), A-11 | User (spec owner) | Release N |
| S-2 | Amendments A-1, A-2, A-4 to A-8, A-10 (transition), A-12, A-13, A-14 | User (spec owner) | Release N+1 |
| S-3 | A-13 changes a frozen design document (the reconciliation model), so it needs a decision-log entry (A-15) | User | Release N+1 |
| O-1 | Any non-NULL D for a dataset: the value, evidence that versions arrive within D, the approver | Production dataset owner | Nothing (NULL is safe and strict) |
| O-2 | Confirmation that no production key column (after normalisation) is FLOAT, DOUBLE, BINARY, ARRAY, MAP, STRUCT, INTERVAL, TIMESTAMP_NTZ or VOID; or a list of affected datasets, accepted as DID_NOT_RUN until a normaliser is declared | Production dataset owners (or the platform team with schema access) | Release N+1 deployment |
| O-3 | Acknowledgement that every `key_hash` changes once at N+1, so hashes kept outside the store (sampled keys in old emails) stop matching | Owners who receive digests | Release N+1 deployment |
| D-1 | Authorisation to deploy N, and later N+1, and to run migration 2 on a shared store | User / platform team | Deployment (not requested here) |

## 3. Unresolved questions

| ID | Question | Options | Recommendation |
| --- | --- | --- | --- |
| Q-1 | Spec self-contradiction SC-1 (line 607 against line 649) | Reword 649 to the set rule (A-12; no behaviour change) / keep both | Reword. The set rule is the tested, approved behaviour |
| Q-2 | CHAR columns are padded on read in Spark 3.5 (verified: CHAR(3) `'a'` reads as `'a  '`, `verification/check_char_and_decimal.py`). A CHAR(n) key part against a STRING part in another layer differs (a false FAIL) | Disclose only / pad or trim via a declared normaliser / refuse CHAR | Disclose; owners declare `trim` where needed (no new code) |
| Q-3 | DECIMAL renders as Spark's cast: `-0.50`, `12345678901234567890.12`, `0.000`, but **`1.00E-8`** for a small value (verified, `check_char_and_decimal.py`). Injective within a type; a layer storing the same number as plain text would not match (a false FAIL) | Keep the cast (unchanged from encoding 1) / render plain text with a format function (new behaviour, not yet verified) | Keep the cast: no change from today, no false PASS. Disclose. Revisit only if an owner reports DECIMAL keys across differing types |
| Q-4 | Does a D refutation persist to later evaluations? | Per evaluation (proposed) / sticky until the owner re-approves (needs stored state per dataset) | Per evaluation, with a digest line every time. Revisit if owners prefer sticky |
| Q-5 | Under L-5, an open legacy identity "awaiting successors" is counted beside its open successors, so the open-key count rises by one per such identity | Accept and label (proposed) / exclude legacy identities from counts (hides history) | Accept and label |
| Q-6 | NF-1: `cause_key_ties.sql.j2:27` counts value variants by joining with `|` and writing NULL as `<null>` (F-02's class, over values); TIE_RESOLVED_BY_RULE evidence can undercount variants | Fix in Gate A / separate item | A separate item after Gate A (it is not a key and cannot create a PASS: it affects a cause's evidence only). Recorded here so it is not lost |
| Q-7 | TIMESTAMP_NTZ key parts | Refuse (proposed) / render without a zone | Refuse until a canonical form is approved, as with U-2 |

## 4. Recommendation

**READY FOR SPEC APPROVAL.**
- The amendments are complete, cite their sections, give replacement text and list their affected tests.
- The contradictions found (C-1 to C-8, C-16, C-17, SC-1) each have a proposed resolution.
- Every item still open is either a wording confirmation (Q-1), a disclosure with a no-change default (Q-3), or a choice with a safe default (Q-2, Q-4, Q-5, Q-7).
- No owner input blocks the spec approval itself. O-1 to O-3 gate deployment, not the specification.

**This is not an approval.**
- Nothing is implemented, applied, committed, pushed or deployed.
- Release N starts only after S-1 and your explicit instruction.
