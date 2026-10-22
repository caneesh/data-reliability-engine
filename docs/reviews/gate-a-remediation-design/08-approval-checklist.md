# 08 · Approval checklist

**This design is not approved.** Each box is for the reviewer.

## A. Before implementation starts

- [ ] The root causes (01) are independently confirmed against the code at the implementation base commit, and the examination tests a19, a20, l05, c05, l01, m04, l03 and l04 still reproduce.
- [ ] **G-1** (encoding 2) is accepted, including wider keys (05).
- [ ] **G-2** (unparseable key) and **G-3** (strict version determinability) are accepted, knowing they can raise false FAILs on data with garbage dates or record times (this is the intended fail-closed direction).
- [ ] **G-5** and **G-6** (tie unanimity; total reject order) are accepted. The approved value-tie rule and R06 are confirmed unchanged.
- [ ] **G-7** (migration 2, the transition, release N first) is accepted, or Q6 is answered forward-only.
- [ ] Q1 to Q7 are answered, or their safe defaults explicitly accepted.
- [ ] Explicit written authorisation to implement, naming the release(s).

## B. Before merging (release N)

- [ ] Writer tests (I8a) pass. The strict check still rejects other missing and extra columns.
- [ ] The full suite and e2e are green locally and in CI on the release commit.

## C. Before merging (release N+1, Gate A)

- [ ] Every new test in 04 §2 exists, was seen to **fail on the unchanged code** first (logged), and passes after.
- [ ] The eight FINDING tests are revised in place to assert the corrected outcome (none deleted). Their corrected copies are in `tests/`.
- [ ] Every existing assertion passes unchanged, or is restated with a written reason tied to F-02, F-05 or F-07 (04 §3). Literal key strings are replaced by `canonical_key`, and the list of changed tests is in the decisions.
- [ ] The determinism test (X1) passes with shuffle partitions 1 and 8. The invariant-B generated test (X3) passes.
- [ ] Migration 2 is applied idempotently. `dre install --check` reports OK. No stored row changed (row counts and checksums of `dq_key_event` before and after, for the legacy rows).
- [ ] The transition tests (I9, I10) pass. A second run forces no sweep.
- [ ] Positive controls are unchanged: legitimate exclusions, dedup, CDC updates (out-of-order, duplicates), deletes, survivorship, union attribution, contribution, and replays R01 to R23 as built.
- [ ] The full suite, the e2e (with expectations unchanged or each change explained), the package build and CI are green on the final source commit.
- [ ] Spec section 6 and the decisions are updated; known issues are updated (F-02, F-05 and F-07 closed with evidence; any new limitation recorded).
- [ ] The benchmarks B1 to B6 are run on a representative cluster, or explicitly deferred as a production prerequisite (KI-01). **No production-readiness claim either way.**
- [ ] An independent review of the evidence package (separate from the implementer).

## D. Explicitly out of scope for Gate A

- The other examination findings (F-01, F-03, F-04, F-06, F-08 to F-18, S-01 to S-23).
- Q1 (authoritative `winner_rule`) and Q2 (load-order fallback), unless answered yes.
- Any change to ingestion pipelines, source data or target tables.
