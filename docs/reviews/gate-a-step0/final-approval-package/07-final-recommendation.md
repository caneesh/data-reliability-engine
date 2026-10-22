# 07 · Final recommendation

## Outcome: READY FOR SPEC APPROVAL

The recommendation is approval of the amendments **as dispositioned in 01**: the revised texts A-1r, A-4r, A-5r, A-9r, A-10r and A-15r replace the corresponding texts of `../02-specification-amendments.md`. A-16 is new.

| Kind of approval | State |
| --- | --- |
| **Architecture (specification) approval** | **Requested now** (checklist 06) |
| **Implementation authorisation** | **Not requested.** Release N may start only after the release N approvals and your explicit instruction (05 PN-1 to PN-3) |
| **Production deployment approval** | **Not requested.** It needs D-1, O-2 and O-3, and for N+1 the end-to-end rollback demonstration; the cluster load test remains a separate production gate (05) |

## 1. Baseline (verified)

- Branch `claude/claude-md-documentation-a2lxt0`; HEAD `1d88659` (equal to the remote).
- **CI #105 on `1d88659`: success.** CI #104 on `c5e6237`: success.
- Untracked: `docs/reviews/gate-a-step0/` only.
- No tracked file changed: no code, tests, configuration, specification, decision log or known-issues register.

## 2. Why READY rather than REVISION REQUIRED or BLOCKED

- **Every amendment has a disposition.** Six revised texts are included in full, so nothing is left to "revise later".
- **The new findings were each corrected in the text**:
  - NR-1: two events per identity per evaluation;
  - NR-2: a refutation lasting one evaluation;
  - NR-3: DECIMAL scientific notation and scale;
  - NR-4: the register restriction, recorded in A-16.
- **The invariants of 03 §1 hold** under the revised texts.
- **No owner input blocks the specification.** D stays NULL by default; O-1 to O-3 gate deployment.
- **The remaining unverified points are named conditions** attached to approval (06), each with its test, verified before the dependent release merges.

## 3. What would change this recommendation

- Rejecting Q-3 (numeric DECIMAL identity) or Q-2 (CHAR without pad): A-1r would revert to the cast, with its disclosed false FAILs. Still approvable, but the text must say which.
- Rejecting Q-4 (persistent refutation) without an alternative: A-4r would lose temporal correctness (NR-2). **SPEC REVISION REQUIRED.**
- Any requirement to change an ingestion pipeline: **BLOCKED** (CLAUDE.md constraints 1 and 2). None was found.

## 4. Not claimed

- That the engine implements any of this. No code was written.
- That the prototypes prove production correctness. They are models and Spark behaviour checks.
- Production readiness.
