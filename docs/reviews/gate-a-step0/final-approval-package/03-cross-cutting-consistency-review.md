# 03 · Cross-cutting consistency review

## 1. Invariants against the amendments

| Invariant | Supported by | Conflict found in the original text | Resolution |
| --- | --- | --- | --- |
| **Identity integrity**: two distinct logical keys never become one identity | A-1r (length-prefixed; `!` form; per-column injective CHAR and DECIMAL text), A-2 (bytes to the HMAC), A-14 | 02's A-1: none for identity. Its DECIMAL and CHAR rules only caused false FAILs (not merges) | A-1r improves cross-layer matching without merging within a side (verified: `test_q2_char.py`, `test_q3_decimal.py`) |
| **Evidence completeness**: no relevant record or malformed key disappears silently | A-4r (every untimed row relevant or set aside and counted), A-8 (target rows counted), A-1r (`!` keeps the record's own identity) | none | — |
| **Temporal correctness**: an invalid timestamp never yields a false PASS | A-4r | **02's A-4: a refutation lasted one evaluation**, so the next window could set rows aside again on a refuted declaration | **A-4r:** a refuted bound stays refuted until a new version; unverifiable means refuted |
| **Determinism**: same inputs, contract and window give the same result | A-6 (total reject order), A-10r | **02's A-10 implied two events (REKEYED, STILL_FLAGGED) for one ambiguous legacy identity in one evaluation**, tied on `observed_at` and `run_id`, the ordering keys of `v_open_keys` (NR-1) | **A-10r:** at most one event per identity per evaluation; ambiguous legacy identities are not REKEYED (`test_q5_reporting.py`) |
| **Historical continuity**: migration never resets or loses discrepancy age | A-9r, A-10r | 02's A-9/A-10: consistent with U-4, but they did not separate categories, so legacy identities would inflate business counts | A-9r categories and `v_key_lineage`; the digest counts by category (Q-5) |
| **Qualification integrity**: a result resting on conditional evidence is never healthy | A-7 (every hop check and the chain), A-4r (set-aside only under a bound in force) | none | — |
| **Rollback safety**: N and N+1 compatibility explicit and testable | A-3, A-9r, A-10r reading half in N; 04 matrix | **02 placed A-11 (UTC) in release N**, which is not schema and compatibility work | A-11 moved to N+1 |
| **Pipeline independence**: no ingestion pipeline changes | all; every change is DRE SQL, DRE configuration or the `dq` store | none | Confirmed: owners are asked for facts and approvals only (O-1 to O-3) |

## 2. Contradictions: complete list and state

| ID | Contradiction | State |
| --- | --- | --- |
| SC-1 | Spec line 607 against line 649 (value ties) | Resolved by A-12 (wording; behaviour verified unchanged) |
| C-1 | Revision 2's age inheritance against U-4 | Resolved (A-10r: continuity by syntax only; step 0 prototype, 6 tests) |
| C-2 | Malformed target keys against the orphan definition | Resolved (A-8: own line) |
| C-3 | Exclusion "at the version's load time" (spec) against per row (code) | Resolved toward the code (A-6) |
| C-5 | Binary-only hash UDF against the existing STRING test | Resolved (A-2: dual input; the test unchanged) |
| C-6 | Qualification scope (ledger only) against the bound's scope (all hop checks) | Resolved (A-7) |
| C-7 | UTC check as a per-evaluation DID_NOT_RUN | Resolved (A-11: start refusal; N+1) |
| C-8 | Refused types against currently judged datasets | Open as an owner input (O-2) |
| C-16 | Cause `failure_ref` hashes have no encoding column | Resolved by rule (A-10r §7; trace looks up by encoding) |
| C-17 / NF-1 | Value-variant collision in a cause template | Deferred with a known-issue entry (A-16) |
| **NR-1 (new)** | Two events per identity per evaluation | Resolved (A-10r) |
| **NR-2 (new)** | A refutation lasting one evaluation | Resolved (A-4r) |
| **NR-3 (new)** | DECIMAL renders in scientific notation and keeps scale; `format_string` cannot fix it | Resolved by A-1r's string-function form (verified) |
| **NR-4 (new)** | The defect register (`docs/known-issues.md`) cannot be updated under this task's restrictions | Reported: A-16 holds the entry text until approval |

**No contradiction remains unresolved in the specification text.** C-8 and the three owner inputs are deployment facts, not specification conflicts.

## 3. Unresolved risks (accepted, disclosed or gated)

| Risk | Direction | Handling |
| --- | --- | --- |
| TIMESTAMP canonical rendering not executed | Possible false FAIL across layers if wrong; no merge (one pattern per type) | Condition T-F02-9 |
| Refutation persistence not prototyped | A design error could re-enable a refuted bound | Conditions T-Q4-1 to T-Q4-5; fail-closed default (unverifiable means refuted) |
| Soft-delete tie and full-sweep target count not prototyped | — | Conditions T-F07-6, T-DN-4 |
| Rollback proven at state-machine level only | Loss of an open discrepancy if the engine deviates | Gate GP-4 (end-to-end with real builds) |
| A wrong bound never refuted by the timed rows | Possible false PASS | Owner approval with evidence (O-1); every such PASS qualified, never healthy |
| Production key types unknown | Datasets may become DID_NOT_RUN | O-2 before deployment |
| Hashes held outside the store change | Old sampled hashes stop matching | O-3 before deployment |
| Performance at scale | Wider keys (about 10 bytes); a one-off transition scan per hop | Indicative local evidence only; cluster load test is a separate production gate |
| NF-1 | A misleading cause line (never a check result) | KI-20 until fixed |

## 4. Verification evidence (this package)

| Set | Command (from the repository root, `PYSPARK_PYTHON=$PWD/.venv/bin/python`) | Result |
| --- | --- | --- |
| All design prototypes:<br>• this package (31);<br>• step 0 continuity (6);<br>• revision 2 (57);<br>• the original design (7);<br>• the examination (52) | `.venv/bin/python -m pytest -q -p no:cacheprovider -rs docs/reviews/gate-a-step0/final-approval-package/verification docs/reviews/gate-a-step0/verification docs/reviews/gate-a-remediation-design/revision-2/verification docs/reviews/gate-a-remediation-design/verification docs/reviews/reconciliation-examination/adversarial` | **153 passed**, 0 failed, 0 skipped, in 296.90 s |
| Q-5 prototype after the A-9r category rule | `… pytest … final-approval-package/verification/test_q5_reporting.py` | **4 passed** |
| Existing tests the amendments name as affected or unchanged | `.venv/bin/python -m pytest -q -p no:cacheprovider -rs tests/guard tests/checks/test_hop.py tests/checks/test_hop_ledger.py tests/checks/test_open_keys_idle.py tests/checks/test_chain.py tests/replay/test_r04_to_r07_hops.py tests/replay/test_r19_cdc_versions.py tests/causes/test_causes.py tests/store/test_views.py tests/store/test_install.py tests/test_evaluation_modes.py tests/test_execution_types.py tests/test_trace.py tests/notify/test_digest.py` | **297 passed**, 0 failed, 0 skipped, in 1288.68 s (on unmodified code) |
| Spark behaviour checks used by Q-2 / Q-3 | `../verification/check_char_and_decimal.py` (step 0) | CHAR(3) `'a'` reads as `'a  '`; DECIMAL `1.00E-8` |
| CI | GitHub Actions run #105 on `1d88659` | success (jobs test, e2e, package) |

**What these runs show and do not show:**
- The existing tests show the behaviour the amendments preserve (or must restate) **as it is today**.
- The prototypes show the proposed rules on models and the Spark behaviour they rely on.
- **Neither shows that the engine implements the amendments, nor production correctness.**

**Unverified:**
- TIMESTAMP rendering;
- refutation persistence;
- the soft-delete tie;
- the full-sweep target count;
- the `v_open_keys` lineage SQL in the real view;
- end-to-end rollback;
- performance at volume.

## 5. Does any finding invalidate the design?

No. The new findings (NR-1 to NR-4) each changed a text, not the architecture. They are corrected in A-4r, A-10r and A-1r, or recorded (A-16). The prototypes confirm the revised rules on their models. They do not establish production correctness.
