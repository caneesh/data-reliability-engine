# Gate A remediation, Step 0: specification closure report (2026-10-22)

**Authorisation.** On 2026-10-22 the user gave the architecture review disposition: "APPROVED, subject to the documented implementation verification gates". It covers documentation and specification alignment only. It does **not** authorise implementing release N or N+1, executing migrations, merging, or deploying.

**Result.** The approved amendments are integrated into the governing documents. **Implementation has not begun.** No production code, test, migration script, configuration, deployment manifest or CI workflow was changed.

## 1. Git baseline (verified before any change)

| Item | Value |
| --- | --- |
| Branch | `claude/claude-md-documentation-a2lxt0`, tracking `origin/claude/claude-md-documentation-a2lxt0` |
| HEAD | `21e54f2` (equal to the remote); working tree clean |
| Ancestry | `1d88659`, `c5e6237`, `9fa68fa`, `bf7c014`: all ancestors of HEAD |
| CI | #106 on `21e54f2`: success; #105 on `1d88659`: success |
| Approval package | `docs/reviews/gate-a-step0/final-approval-package/` (7 documents and `verification/`), present. It is consistent with the approval: the approved list (A-1r, A-2 to A-3, A-4r, A-5r, A-6 to A-8, A-9r, A-10r, A-11 to A-14, A-15r, A-16) equals the package's dispositions |

## 2. Modified files

| File | Change |
| --- | --- |
| `docs/spec.md` | Approved amendments in §1 (status paragraph), §3, §5, §6, §7, §8 and §9. Each is marked with its release and "not built"; the amended as-built text is marked "as built until release N(+1)" |
| `docs/design/reconciliation-model.md` | A-13: two wording corrections (they describe the engine as built) and one release N+1 sentence in §8 |
| `docs/decisions.md` | New entry "Gate A remediation: Step 0 specification approval (2026-10-22)": the A-15r text verbatim, plus conditions, release boundary, TIMESTAMP_NTZ, KI-20 deferral, O-1 to O-3, and approval ≠ authorisation |
| `docs/known-issues.md` | KI-20 (A-16) added to the open register; KI-18, KI-19 and every other entry unchanged |
| `docs/handoff.md` | Date; history rows for `c5e6237` (#104), `1d88659` (#105), `21e54f2` (#106) and this commit; a status bullet; the current milestone (release N awaits authorisation) |
| `docs/reviews/gate-a-step0/spec-closure-report.md` | This report |

Not modified:
- `docs/design/target-architecture.md` (ADR-01 to ADR-08 unchanged);
- every file under `src/`, `tests/`, `conf/` and `.github/`;
- every earlier review document.

## 3. Amendment traceability

| Amendment | Source (package 01 unless stated) | Governing document section | Status | Verification gate |
| --- | --- | --- | --- | --- |
| A-1r | §A-1r | spec §6 "Hop details", first bullet (sub-bullet) | Integrated; release N+1, not built | TIMESTAMP rendering test (before N+1 merges) |
| A-2 | §A-2 (text: step 0 `02`, A-2) | spec §5, after the views | Integrated; N+1 | Existing STRING-argument hash test unchanged |
| A-3 | §A-3 (text: `02`, A-3) | spec §5 DDL `dq_key_event` (comment lines) | Integrated; release N | Migration tests (package 04) |
| A-4r | §A-4r (605 and 613 text: `02`, A-4) | spec §6 HOP_KEY_CURRENCY; HOP_LEDGER dispositions sub-bullet; new "Untimed rows" bullet | Integrated; N+1 | Refutation-persistence tests |
| A-5r | §A-5r | spec §3 `record_time` example (comment) and "Validation" | Integrated; N+1 | Configuration tests |
| A-6 | §A-6 (text: `02`, A-6) | spec §6 HOP_LEDGER dispositions sub-bullet | Integrated; N+1 | Soft-delete tie test |
| A-7 | §A-7 (text: `02`, A-7) | spec §5 item 7; §6 "Qualifications" sub-bullet | Integrated; N+1 | Never-healthy tests |
| A-8 | §A-8 (text: `02`, A-8) | spec §6 "Orphans" sub-bullet | Integrated; N+1 | Full-sweep target-count test |
| A-9r | §A-9r | spec §5 views | Integrated; release N | View tests; equivalence |
| A-10r | §A-10r | spec §6 "Key events" sub-bullets (reading: N; transition and reporting: N+1); §7; §8 "Trace" | Integrated; N (reading, one event per identity, cause encoding, trace) / N+1 (transition, reporting) | Reader and rollback tests (N); transition tests and end-to-end rollback (N+1) |
| A-11 | §A-11 (text: `02`, A-11) | spec §8 `dre run` exit 3 | Integrated; **N+1** | UTC start test |
| A-12 | §A-12 (text: `02`, A-12) | spec §6 "Ties" | Integrated; **applies now** (wording; behaviour unchanged) | Existing tests unchanged |
| A-13 | §A-13 (text: `02`, A-13) | model §4 (two corrections, apply now), §8 (N+1) | Integrated | none |
| A-14 | §A-14 (text: `02`, A-14) | spec §9, guard rule 12 | Integrated; N+1 | New guard test |
| A-15r | §A-15r | `docs/decisions.md` new entry | Recorded (date and authority filled) | none |
| A-16 | §A-16 | `docs/known-issues.md` KI-20 | Recorded | none (deferred) |

**Rollback statements** (the R-4 and GP-4 rules of the package's 04 and 05) are recorded in spec §1, the "Key events" sub-bullet and the decision entry:
- rollback below N is unsupported;
- N+1 → N is demonstrated end to end before N+1 merges.

## 4. Consistency verification

| Check | Result |
| --- | --- |
| 1. No approved text omitted | **Pass.** 38 distinctive phrases, at least one per amendment, all found in the governing documents (scripted check) |
| 2. No superseded provision authoritative | **Pass.** The superseded originals (02's A-1 CHAR and DECIMAL rules, 02's per-evaluation refutation, line 649's "always fail", the model's `winner_rule` ordering and status) are absent. The three as-built rules that remain (the `|` key string, "all rows untimed", "no record time on either side") are each marked "as built until release N+1" |
| 3. No contradiction across spec, ADRs, decisions, handoff, known issues | **Pass,** with the notes in §6. The ADRs are unchanged; the decision entry records the approved behaviour changes as the frozen model requires |
| 4. Each mandatory test assigned to the right release | **Pass.** N: migration, writer, views, readers, rollback reading, overlap guard, trace, equivalence. N+1: hash test unchanged, soft-delete tie, full-sweep count, UTC start, TIMESTAMP rendering, refutation persistence, end-to-end rollback |
| 5. No deferred capability described as completed | **Pass.** Every integrated text carries "not built", or applies now as wording that describes current behaviour (A-12, A-13's corrections). KI-20 is OPEN and deferred |
| 6. Release N leaves accepted behaviour unchanged | **Pass.** Release N items are schema, view and reader compatibility only. The DDL additions are comments, so `tests/store/test_ddl.py` still matches the built store (it strips comments): run, passed |
| 7. Architecture approval and deployment approval kept distinct | **Pass.** Spec §1, the decision entry and the handoff each state it; O-1 to O-3 are recorded as deployment prerequisites, none given |
| Markdown sanity | 127 added lines, no unbalanced backticks; no production names |
| Tests that read the documents | `tests/store/test_ddl.py` and the guard tests: **121 passed**. `dre validate --conf conf/`: exit 0 |

## 5. Remaining gates

**Release N:**
1. Explicit implementation authorisation.
2. The tests of the package's 04 §3.
3. Equivalence against existing stores.
4. No existing test restated.
5. Full suite, replays, guard tests, e2e, packaging and CI green.
6. Independent verification.
7. Deployment approval, separately: migration 2 only with platform approval.

**Release N+1:**
1. Release N merged and deployed.
2. Explicit implementation authorisation.
3. All N+1 tests, including the conditions in §3.
4. Only the three named KEY-constant tests restated.
5. The end-to-end N+1 → N rollback demonstration.
6. Deployment prerequisites O-2 and O-3 (and O-1 for any non-NULL bound).
7. The cluster load test remains a separate production gate (KI-01).

## 6. Unresolved issues and notes (flagged, not decided)

| ID | Issue | Effect | Proposed handling |
| --- | --- | --- | --- |
| INC-1 | A-9r defines a `v_key_lineage` kind `rollback` ("an encoding-1 continuation after an encoding-2 engine"). Under the approved texts nothing produces it: release N writes `predecessor_hash` NULL, and A-10r's rollback continuation names no predecessor | No correctness effect (the view only lists events that name a predecessor). The kind is simply never observed | Integrated as approved. Release N's implementation should either have the rollback continuation name the encoding-2 identity as predecessor, or drop the kind. **Either is a small spec decision for the user**; not decided here |
| INC-2 | "Gate A" also names the earlier build gate (steps 4 and 5, M1 to M9; KI-16) | Possible confusion | The new texts say "Gate A remediation" and state the distinction |
| INC-3 | The request described NF-1 as a "diagnostic hash collision". The approved A-16 describes a text-join collision in the value-combination count; no hash is involved | Wording only | KI-20 uses the approved description and says "not in the key hash" |
| N-1 | A-15r cites spec line numbers (605, 613, 641) | They refer to `21e54f2`; this commit shifts lines | Stated in the decision entry |
| N-2 | Release N must turn the DDL comment lines (A-3) into real columns in `docs/spec.md` in the same commit as `store/ddl.sql` | Otherwise `tests/store/test_ddl.py` fails | Noted here and in the handoff milestone (test first) |

## 7. Confirmation

- **Release N implementation: not started.**
- **Release N+1 implementation: not started.**
- No migration was executed anywhere. No pull request was opened or merged. No deployment.
