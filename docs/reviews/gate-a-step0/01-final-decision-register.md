# 01 · Final decision register (Gate A, step 0)

**Status of this register.** These are proposed final positions for review. **Nothing here is approved, and nothing here changes the authoritative specification (`docs/spec.md`), the decision log (`docs/decisions.md`) or the approved ADRs (`docs/design/target-architecture.md`).**

**The user's provisional decisions U-1 to U-6** are applied as instructed. They stay *provisional*: each is listed under "approved in principle", meaning the direction was given, and every one still needs either exact specification review (02) or a production owner's approval (06). None is converted into a final approval here.

**Baseline.**
- Branch `claude/claude-md-documentation-a2lxt0`, HEAD `1d88659` (the review documents, committed and pushed at your request).
- Working tree clean except this new folder.
- CI run #105 for `1d88659` was queued at the time of writing. The last completed run is #104 (green) on `c5e6237`.

**Sources read for this register:**
- `docs/spec.md` §§4, 5 and 6 (lines 422, 461-466, 555, 569-574, 605-653);
- `docs/design/target-architecture.md` (invariants; ADR-02, ADR-05, ADR-06, ADR-08);
- `docs/design/reconciliation-model.md` §§3, 4 and 8;
- the revision-2 package and its prototypes;
- the code at `keys.py:109-122`, `common.py:96-201`, `ledger.py:105-165`, `hop_ledger.sql.j2:196-264`, `writer.py:19-39`, `key_events.py:22-39`, `runs.py:86-98`, `runner.py:773-800, 1072-1082`, `causes/failures.py:31-60`, `cause_key_ties.sql.j2:15-35`, `ddl.sql:270-322`;
- the tests that pin the behaviour in question (listed per decision).

## 1. What the existing contract actually says (verified against the spec and the code)

Three Gate A defects are **written into the current specification**, not just the code. Fixing them is therefore an **approved-behaviour change** that needs explicit spec approval, not a bug fix the spec already requires.

| Defect | Current spec text | Code follows it | Consequence |
| --- | --- | --- | --- |
| F-02 | §6 "Hop details": "Keys … as one string (parts joined with `|`, null written as `<null>`)" (line 641) | `keys.py:119-122` | The encoding change is a spec amendment (02, A-1) |
| F-05 | §6 HOP_KEY_CURRENCY: "UNASSESSABLE (**all** of the key's upstream rows, or its rows here, have no usable record time)" (line 605); HOP_LEDGER: "no record time on either side" (line 613) | `hop_common.sql.j2:83-90`, `hop_ledger.sql.j2:70-125` | The relevance rule is a spec amendment (02, A-4) |
| F-07 | §6 HOP_LEDGER `excluded` "an approved exclusion effective at the version's load time"; `deleted` "the version matches … `deletion.tombstone`". **Silent on tied rows.** | `MAX` over the tied rows (`hop_ledger.sql.j2:77-84`) | The tie-agreement rule fills a gap (02, A-6) |

**A self-contradiction in the existing spec, found during this review (SC-1).**
- §6 HOP_VALUE_AGREEMENT (line 607): tied rows are compared "as the set of distinct owned-column values … Equal sets agree whatever the number of tied rows".
- §6 "Ties" (line 649): "requires each of them to match this dataset's latest row, so tied upstream rows that disagree with each other **always fail**".
- The executable behaviour follows line 607: `tests/checks/test_hop.py::test_value_agreement_compares_the_tied_latest_rows_as_sets` passes with two differing tied upstream rows against the same two rows in gold. R06 (`test_r06_…`) FAILs because gold holds **one** row against two.
- **Position:** the approved, tested behaviour is the set rule. Line 649 is wrong when the target holds the same set.
- CLAUDE.md requires asking when the spec contradicts itself, so the rewording (02, A-12) is listed for confirmation (06, Q-1). Gate A does not depend on its outcome, because value-tie semantics are untouched.

## 2. Decision-by-decision assessment (R2-D1 … R2-D15)

**Legend.**
- **Approved in principle:** the direction was given by the user's provisional decisions or by an existing approved ADR.
- **Spec review:** needs exact specification text approved (02).
- **Owner approval:** needs a production dataset owner (06).
- **Unresolved:** open (06).

| ID | Decision (final proposed form) | Assessment against the spec, ADRs and code | Change from revision 2 | Category |
| --- | --- | --- | --- | --- |
| **R2-D1** | Key encoding 2: length-prefixed bytes (LPB) as defined exactly in 02 A-1 / 03 §1 | Consistent with ADR-02 ("collisions detected at run time"). Replaces spec line 641. Verified injective on the STRING domain, including invalid UTF-8 (rev-2 l1–l5) | Exact byte grammar fixed (length in ASCII decimal without leading zeros; the empty string is `0:;`); the encoding version is held out of band | Approved in principle (U-3 context) · **spec review** |
| **R2-D2** | Qualification `version_by_disorder_bound` on any result whose judgement set aside at least one untimed row under D: **every hop check** (HOP_KEY_CURRENCY, HOP_VALUE_AGREEMENT, HOP_LEDGER) and CHAIN_COMPLETENESS (which carries every edge's qualifications) | Consistent with §5 item 7 (a qualified PASS is never healthy) and the generic plumbing: `v_latest_result.healthy` requires `qualifications IS NULL` (`ddl.sql:283`), and the digest lists any code (`digest.py:335-362`). Read | **Widened** from the ledger only to every hop check (contradiction C-6) | **Approved in principle (U-1)** · spec review |
| **R2-D3** | Temporal eligibility with bound D (exact definition: 03 §2). D NULL by default. Integer hours ≥ 1. Falsification guard; refutation makes the evaluation strict | Consistent with ADR-05 and model §8: D compares load time with load time (one kind), never with record time, and never chooses a version (model §4: "never by load time"). Changes approved text at spec lines 605 and 613 | D restricted to integer hours. Refutation persistence is an open question (Q-4) | **Approved in principle (U-5)** · spec review · **owner approval** for any non-NULL D |
| **R2-D4** | Refused key part types: FLOAT, DOUBLE, BINARY, ARRAY, MAP, STRUCT, and also INTERVAL, TIMESTAMP_NTZ and VOID (not analysed, so fail closed). Refusal gives DID_NOT_RUN / invalid_config | CONFIGURATION reasons never hold a window (§4), so a refused hop reports DID_NOT_RUN every run until fixed. Changes the outcome for any dataset whose key has such a type | Three types added to the refused list (fail closed) | **Approved in principle (U-2 for BINARY)** · spec review · **owner confirmation** that no production key uses a refused type |
| **R2-D5** | `key_hash` = the existing HMAC-SHA256 (lower-case hex) over the **canonical key bytes**, passed to the UDF as `CAST(k AS BINARY)`. The UDF accepts STRING (UTF-8-encoded) or BINARY, with an identical result for valid UTF-8 | §5 line 574 ("HMAC-SHA256 of the normalised key"), the secret contract and ADR-08 "Security" are unchanged. The existing test `test_sql_key_hash_matches_python_hmac` (STRING argument) must pass unchanged, which is the dual-input requirement | Dual input made a requirement (C-5); `hex(k)` dropped | **Approved in principle (U-3)** · spec review (wording only) |
| **R2-D6** | Session time zone UTC: checked **once at run start** after `get_spark`. Anything else makes `dre run` refuse to start (exit 3), like an unreadable secret file | `get_spark` sets UTC today (rev-2 k8). §4 already requires UTC comparison | **Revised** from a per-evaluation DID_NOT_RUN to a start refusal: one failure point, no new reason code (C-7) | Spec review |
| **R2-D7** | An unparseable part keeps its raw bytes in the `!` form, and its key is `unassessable 'unparseable key'`. "No key" stays defined on the **raw** columns (`has_a_key`, `common.py:171-174`) | Implements model §3 row 1 ("unparseable key"), which the code lacks today. Consistent with §6 "a row whose every key column is null has no key" (raw) | Interplay with "no key" stated (C-11) | Approved in principle (ADR-02, model §3) · spec review |
| **R2-D8** | Tied version rows must agree on expected presence (present / absent / soft-deleted); otherwise `unassessable 'tied rows disagree'`. Exclusion effectiveness is judged **per row at that row's own load time** (the executable behaviour, `common.py:136-160`) | Fills a spec gap. **The spec's "effective at the version's load time" differs from the code for tied rows with different load times (C-3)**; the code's per-row reading is kept and the wording amended | C-3 surfaced and resolved toward the executable behaviour | Spec review |
| **R2-D9** | A reject among tied rows that differ: `unassessable 'reject ambiguous among tied rows'`. Total order for "the latest such reject": `rejected_ts` desc, then defect before authorised, then reason (byte order, greatest first), then rule id (byte order, greatest first) | Extends §6 "the latest such reject gives the reason and the rule", which leaves ties undefined. Fails closed toward defect | Rule-id tiebreak direction made explicit | Spec review |
| **R2-D10** | Labels carry the reason only; counts go in the scope | Consistent with the existing labels (`test_hop.py:403`, `test_hop_ledger.py:177` keep their text) | none | Approved in principle · spec review (label list) |
| **R2-D11** | Unparseable target rows never match. They are counted on their own breach line, **`unparseable target key`**, reported with the orphans when orphans are checked, and in the scope otherwise; every such row on the full sweep | §6 defines orphans as keys "that no upstream row has". An unparseable target row whose raw bytes equal an unparseable upstream row's is not an orphan by that definition, yet must not match (C-2). Hence a separately named line, not the orphan label | **Revised**: its own line instead of `orphan 'unparseable key'` (C-2) | Spec review |
| **R2-D12** | Migration 2 (`dq_key_event` + `key_encoding INT`, `predecessor_hash STRING`), shipped in release N. Event `REKEYED`, which never ends a spell. `v_open_keys` lineage | Consistent with the guard rule 11 (additive, only in `migrations.py`) and §5 append-only. The 1:1 age-inheritance rule of revision 2 **contradicts U-4** (C-1) | **Revised:** age is inherited only on **provable** identity continuity (03 §5, prototype `verification/test_identity_continuity.py`). Otherwise the legacy discrepancy **stays open with its original age** until none of its successors is open | **Approved in principle (U-4)** · spec review |
| **R2-D13** | Release N is rollback-aware: the writer tolerates migration-added columns; readers are encoding-aware, ignore REKEYED, detect foreign identities, force a sweep and write REKEYED back | Consistent with ADR-07 (commit rows make the transition atomic in effect). Verified at state-machine level only | Adds the cause `failure_ref` encoding rule (C-16) | Spec review · **end-to-end demonstration gate** (05) |
| **R2-D14** | No new concurrency fence: only NORMAL runs write key events (`checks/base.py:156-158`), and overlapping NORMAL runs are refused (`runner.py:1075-1082`) | Verified by reading; one test pins it across engine versions | none | Approved in principle (existing behaviour) |
| **R2-D15** | `winner_rule` stays non-authoritative in the ledger and HOP_VALUE_AGREEMENT. R06 is unchanged. The reconciliation model's sentence "ties ordered by a hop's `winner_rule`" (model §4, line 90) is corrected | Matches the code (`causes/merge.py:149-163` is the only reader), spec line 649's intent and R06 | none | **Approved in principle (user: preserve)** · spec review (model wording) |

## 3. Contradictions found between the decisions, and how each is resolved

| ID | Between | The contradiction | Resolution (proposed) |
| --- | --- | --- | --- |
| C-1 | R2-D12 (rev-2 06 §3.4) vs U-4 | Revision 2 inherited age for any legacy key with exactly one successor *in today's data*. Today's data cannot prove continuity: a colliding record may have existed when the key was flagged and left since. Revision 2 also closed a 1:n legacy identity with REKEYED, which removed the original discrepancy from `v_open_keys` | Continuity is provable **only by syntax**: the legacy `key_value` decodes to exactly one tuple given each part's type (03 §5; 6 prototype tests pass). Otherwise the legacy identity stays open (encoding 1, original age) until none of its successors is open |
| C-2 | R2-D11 vs spec §6 "Orphans" | "Orphan" means no upstream row has the key, but an unparseable target key can coincide with an unparseable upstream key | A separate breach line, `unparseable target key` |
| C-3 | R2-D8 vs spec §6 `excluded` | The spec says the version's load time; the code uses each row's own load time | Keep the code's behaviour (per row); amend the wording (02, A-6) |
| C-4 | Spec line 607 vs line 649 (SC-1) | Set rule against "always fail" | Keep the set rule (tested); reword line 649 (02, A-12); confirm (06, Q-1) |
| C-5 | R2-D5 vs `test_sql_key_hash_matches_python_hmac` | A BINARY-only UDF would break a STRING call | The UDF accepts both, with an identical result for valid UTF-8 |
| C-6 | R2-D2 vs R2-D3 | D applies to every hop check, but the qualification was specified for the ledger only | Qualify every hop check that set a row aside, and the chain |
| C-7 | R2-D6 vs §4 window holding | A per-evaluation PLATFORM DID_NOT_RUN on every check would hold every window, with a misleading reason | Refuse the run at start (exit 3) |
| C-8 | R2-D4 vs approved runtime behaviour | Datasets keyed on a refused type change from judged to DID_NOT_RUN | Needs owner confirmation of key types (06, O-2) |
| C-16 | R2-D13 vs causes | `dq_cause_result.failure_ref` holds key hashes with no encoding column. A retried investigation (`runner.py:773-800`) re-derives the failing keys with the **retrying** engine's encoding | `failure_ref` hashes are interpreted by their run's `engine_version`. Trace looks up both encodings. No migration (02, A-10) |
| C-17 | Outside Gate A | `cause_key_ties.sql.j2:27` counts value variants with `concat_ws('|', coalesce(…, '<null>'))`, the same collision class as F-02 but over **values**: it can undercount variants in the TIE_RESOLVED_BY_RULE evidence | Not a key; recorded as new finding NF-1, out of Gate A scope (06, Q-6) |

## 4. Summary by category

| Category | Items |
| --- | --- |
| **Approved in principle** (provisional; given by U-1 to U-6, the user's instruction to preserve winner_rule, R06 and the tie behaviour, or by existing approved ADRs) | U-1 (R2-D2), U-2 (BINARY in R2-D4), U-3 (R2-D5), U-4 (R2-D12 continuity), U-5 (R2-D3 default), U-6 (this step), R2-D7 (model §3), R2-D14 (existing guard), R2-D15 |
| **Requires exact specification review** (02) | A-1 to A-15: every R2-D except R2-D14 |
| **Requires production dataset-owner approval** (06) | O-1: any non-NULL D per dataset. O-2: confirmation that no key column has a refused type. O-3: awareness that every `key_hash` value changes once at N+1 (any hash kept outside the store, such as sampled keys in old emails, stops matching) |
| **Unresolved** (06) | Q-1 (SC-1 wording), Q-2 (CHAR padding across layers), Q-3 (DECIMAL renders in scientific notation below 10^-6: cross-layer disclosure), Q-4 (whether a D refutation persists), Q-5 (an open legacy identity counted beside its successors in the open count), Q-6 (NF-1), Q-7 (TIMESTAMP_NTZ support) |
