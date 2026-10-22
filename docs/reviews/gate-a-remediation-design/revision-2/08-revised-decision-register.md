# 08 · Revised decision register (revision 2)

- **None of these decisions is approved.** The original register (`../07-architecture-decisions.md`, G-1 to G-7, Q1 to Q7) is preserved unchanged. This register records what revision 2 keeps, refines or supersedes there.
- **No existing architecture decision is changed** (ADR-01 to ADR-08; spec sections 5 and 6). Where a decision below needs a spec amendment, it says so: the spec is the source of truth (CLAUDE.md), so the amendment is a prerequisite of implementation, not a side effect of it.

## 1. Decisions

| ID | Decision | Relation to the original | Alternatives considered | Evidence | Needs a spec amendment? | Status |
| --- | --- | --- | --- | --- | --- | --- |
| **R2-D1** | Key encoding 2 = **length-prefixed bytes** (LPB): `~;` NULL, `<len>:<bytes>;` value, `!<len>:<raw>;` unparseable part (02 §2) | **Supersedes G-1** (JSON) | JSON (not injective: R2-02); struct keys (cannot retype a stored column); hash only (loses `key_value` for remap and trace) | l1–l5, g01, g01b | Yes: spec section 5 (key string definition) | Proposed |
| **R2-D2** | A PASS resting on set-aside untimed rows carries the new qualification **`version_by_disorder_bound`** (never healthy) | New | No qualification (D treated as proven); the result made unassessable (D useless) | b03 | Yes: the qualification list | **Choose** (recommendation: qualify) |
| **R2-D3** | Untimed rows are relevant unless loaded more than an **approved disorder bound D** before the version's load. D defaults to `null` (strict). A falsification guard reverts the evaluation to strict when the timed rows refute D (03) | **Supersedes G-3; merges Q2 and Q3** | All rows forever (original G-3: a permanent inconclusive FAIL); load-order fallback (Q2, unbounded); "since the last judged version" (Q3, needs per-key state the ledger does not keep) | b01–b11, t1–t3 | Yes: dataset contract field and its approval | Proposed |
| **R2-D4** | Key part types FLOAT, DOUBLE, BINARY, ARRAY, MAP, STRUCT are **refused**: DID_NOT_RUN `invalid_config`, naming the dataset, column, type and the fix (02 §3) | New | Accept with a rendering (inexact or collision-prone: k5, k6); accept BINARY under LPB (safe, but needs a binary mirror) | k5, k6 | Yes: supported key types | Proposed |
| **R2-D5** | `key_hash = HMAC(secret, bytes of k)` via `dre_key_hash(CAST(k AS BINARY))` | **Refines G-1's hash** | `hex(k)` (verified too, but changes the hash definition) | l4, l8 | No: section 5's definition holds as worded | Proposed |
| **R2-D6** | Evaluations require session time zone UTC; otherwise DID_NOT_RUN `query_failed` | New (hardens a convention) | Rely on `get_spark` (k8: true today, unenforced) | k4, k8 | No (spec section 4 already states UTC) | Proposed |
| **R2-D7** | An unparseable part keeps its raw bytes (`!` form); its key is `unassessable 'unparseable key'` | **Refines G-2** (which encoded it as NULL: R2-04) | NULL (merges with a genuine NULL) | g01b, d02 | Yes: section 5 / the reconciliation model's "unparseable key" | Proposed |
| **R2-D8** | Tied version rows must agree on **expected presence** (present / absent / soft-deleted); otherwise `unassessable 'tied rows disagree'`. Tombstone + exclusion agree on absent (hard). Each tied row is independently covered by its own approved exclusion (**Q4 answered: independent**) | **Refines G-5** | MAX (today: false PASS); unanimity per rule (G-5: did not combine deletion and exclusion) | c01–c06 | Yes: the reconciliation model's tie rows | Proposed |
| **R2-D9** | A reject among tied rows that differ is `unassessable 'reject ambiguous among tied rows'`; the reject order is total and defect-first | **Keeps G-6, adds ambiguity** | `max_by` on time only | c07, c08 | Yes: the model's reject attribution | Proposed |
| **R2-D10** | Labels carry the reason only; counts go in the scope | **Resolves G-4** | Counts in labels (unbounded label cardinality) | — | No | Proposed |
| **R2-D11** | Unparsed target rows never match; counted as orphans (`unparseable key`) when orphans are checked, otherwise as a scope count plus a triage line; all of them on the full sweep (05) | **Refines G-2's downstream half; answers Q5** | A new qualification (redundant with `orphans_unchecked`) | d01–d04, g03 | Minor (reporting) | Proposed |
| **R2-D12** | Migration 2 = `dq_key_event ADD COLUMNS (key_encoding INT, predecessor_hash STRING)`, **shipped in release N**. A new event `REKEYED` that does not end a spell. `v_open_keys` gains lineage (one level) (06 §3) | **Supersedes G-7** (forced sweep, age reset: R2-09) | Forced sweep (age reset); a bridge in every evaluation (cost; ambiguous) | m3, m4, m7 | Yes: section 5 (event vocabulary, columns) | Proposed |
| **R2-D13** | Release N is rollback-aware: tolerant writer (migration-added columns only), encoding-aware readers that ignore REKEYED, detection of foreign open identities, forced sweep and REKEYED back. **Q6: two releases. Q7: yes** (the tolerance covers migration 1) | **Enlarges G-7's release N** | Forward-only N+1 (no rollback); writer tolerance alone (insufficient: R2-09) | m1, m2, m5, m6 | Yes (as R2-D12) | Proposed |
| **R2-D14** | No new concurrency fence: the existing overlapping-NORMAL-run guard already prevents interleaved encodings | Withdraws the fence first drafted for R2-10 | A version fence (redundant) | Read: `runner.py:1075-1082`, `checks/base.py:156-158` | No | Proposed |
| **R2-D15** | `winner_rule` stays non-authoritative in the ledger and HOP_VALUE_AGREEMENT. The reconciliation model document's sentence saying ties are ordered by `winner_rule` (D-03) is corrected to match the code and the spec | **Keeps Q1 = No** | Authoritative `winner_rule` (changes spec line 649 and R06) | Existing tests (07) | No (the spec is already right; the model document is corrected) | Proposed |

## 2. Questions still open for the user

| ID | Question | Recommendation |
| --- | --- | --- |
| U-1 | R2-D2: qualify a PASS that rests on set-aside rows? | Yes (D is unproven by DRE) |
| U-2 | R2-D4: refuse BINARY key parts, or support them under LPB with a binary test mirror? | Refuse (no such key expected; cheaper) |
| U-3 | R2-D5: hash bytes (`CAST AS BINARY`) or hex? | Bytes (spec wording unchanged) |
| U-4 | A 1:n remap successor: inherit no age (proposed), or the predecessor's age (overstates for the successor that was not open)? | No inheritance; predecessor recorded |
| U-5 | Which datasets will declare D, who approves it, and what value? (Production values; not inventable here) | Leave `null` until the data owners answer |
| U-6 | Approve the spec amendments listed in §1 before implementation begins? | Required by CLAUDE.md (spec is the source of truth) |
