# 10 · Approval readiness (revision 2)

**Outcome: READY FOR IMPLEMENTATION APPROVAL.**
- This means the revised design is complete enough to be approved for implementation. It is **not** a claim that anything is implemented, verified in the engine, or ready for production.
- Nothing has been implemented, committed or pushed.

## 1. Why this outcome, not the other two

**Not DESIGN REVISION REQUIRED.** Every defect found in the original design has a revised rule with executed evidence:
- the encoding (R2-01 to R2-05);
- the temporal rule (R2-06);
- the tie rule (R2-07);
- the transition and rollback (R2-09).

The remaining open items (08 §2, U-1 to U-6) are choices between verified options, each with a recommendation. They are approval inputs, not design gaps.

**Not BLOCKED.** No item depends on information that cannot be obtained:
- The one production unknown, which datasets declare D and with what value (U-5), defaults to `null`, the strict, fail-closed behaviour. It does not block implementation (CLAUDE.md: unknown production values stay unknown).

**What the evidence does and does not cover.** Several parts are **proposed but not prototyped**, and each is assigned an implementation test (09):
- the timestamp renderer and the UTC guard (T-F02-9);
- type refusal (T-F02-6);
- the soft-delete tie case (T-F07-6);
- the chain remap (T-MIG-6);
- the full-sweep count of unparsed target rows (T-DN).

The rollback is prototyped **at state-machine level only**. Its end-to-end demonstration is gate G-6, before N+1 merges.

## 2. Gates

### A. Before implementation starts (approval)

| Gate | Item | Owner |
| --- | --- | --- |
| A-1 | Review of this revised package (01–10) | User |
| A-2 | Approve decisions R2-D1 to R2-D15 (08 §1) | User / architecture |
| A-3 | Answer U-1 to U-6 (08 §2) | User |
| A-4 | Approve the spec amendments of step 0 (09): the spec is the source of truth, so they precede code | User |
| A-5 | Explicit instruction to begin implementation | User |

### B. Before release N merges

| Gate | Item |
| --- | --- |
| B-1 | Full suite and e2e green locally; CI green on the pushed commit |
| B-2 | T-MIG-1 to T-MIG-5 and T-MIG-8 pass; guard tests green (the writer remains the one write path; append-only) |
| B-3 | No behaviour change outside key events (the existing suite unchanged and green) |

### C. Before release N+1 merges (Gate A)

| Gate | Item |
| --- | --- |
| C-1 | Release N deployed and writing `key_encoding = 1` |
| C-2 | All T-F02, T-F05, T-F07, T-DN, T-R1, T-CFG and T-MIG tests pass. The examination's eight scenarios (a19, a20, l05, c05, l01, m04, l03, l04) are ported and give the fixed outcome |
| C-3 | The 4 approved-semantics tests (07 §3) pass **unchanged**; the R01–R12 replay passes |
| C-4 | Full suite, e2e and CI green |
| **C-5 (G-6)** | **End-to-end rollback demonstration** with the real N and N+1 builds on a scratch store with synthetic data: N → N+1 → N → N+1. Every discrepancy open before each switch is open after it, with its age; no event is lost; the evidence is recorded |
| C-6 | Indicative performance on synthetic volume: key width (+~10 bytes per key, 02 §2.1) and the one-off remap cost per hop (06 §3.4) recorded |

## 3. Residual risks (accepted or disclosed)

| Risk | Direction | Handling |
| --- | --- | --- |
| DECIMAL scale differing across layers | false FAIL | Unchanged from today; disclosed; normaliser required |
| NFC / NFD forms differing across layers | false FAIL | Disclosed; out of Gate A |
| D declared wrongly but not refuted by the timed rows | possible false PASS | Mitigated: approval, falsification guard, and the PASS qualified (never healthy, if U-1 = yes) |
| A live run older than the watchdog grace | interleaving | Existing, version-independent (R2-D14) |
| Rollback below N after migration 2 | fails closed | Unsupported by design (06 §5) |
| Prototype ≠ engine | — | The prototypes are models. Every rule is re-proved in the engine by the tests of 09 |
