# 01 · Amendment approval matrix (A-1 to A-16)

**Baseline (verified 2026-10-21).**
- Branch `claude/claude-md-documentation-a2lxt0`; HEAD `1d88659` = `origin/claude/claude-md-documentation-a2lxt0`.
- **CI #105 on `1d88659`: success** (jobs test, e2e and package). The previous run, #104 on `c5e6237`, was also a success.
- Working tree: `docs/reviews/gate-a-step0/` is untracked (the Step 0 package and this one). Nothing else differs.

**Scope.**
- This matrix judges each amendment of `../02-specification-amendments.md` (unchanged; the source of the original text).
- Where an amendment needs different text, the **revised text is given here in full** and marked `r`. Approval is requested for the revised text, not the original.
- Nothing is applied to `docs/spec.md`, `docs/decisions.md` or `docs/known-issues.md`.

**Dispositions:**
- **APPROVE:** the text as proposed in 02.
- **APPROVE WITH CONDITIONS:** the text as proposed, plus a named test or fact that must hold before the release that depends on it merges.
- **REVISE:** 02's text is superseded by the revised text below.

## Summary

| ID | Topic | Release | Disposition | Why |
| --- | --- | --- | --- | --- |
| A-1 | Canonical key bytes and part types | N+1 | **REVISE → A-1r** | CHAR pad rule (Q-2) and DECIMAL numeric text (Q-3) added; TIMESTAMP rendering conditional |
| A-2 | Key hash input | N+1 | **APPROVE WITH CONDITIONS** | Same algorithm; condition: the existing STRING-argument hash test passes unchanged |
| A-3 | `dq_key_event` columns, `REKEYED` | N | **APPROVE** | Additive; verified compatible with the existing migration tests |
| A-4 | Untimed rows, version, due load | N+1 | **REVISE → A-4r** | A refutation now persists until re-review (Q-4) |
| A-5 | `disorder_bound` configuration | N+1 | **REVISE → A-5r** | The bound needs an `id` and `version`, so a refutation and a re-approval are keyed |
| A-6 | Tied rows in the ledger | N+1 | **APPROVE WITH CONDITIONS** | Condition: T-F07-6 (soft + absent), not prototyped |
| A-7 | `version_by_disorder_bound` qualification | N+1 | **APPROVE** | Rides the existing generic health definition (read: `ddl.sql:283`, `digest.py:335-362`) |
| A-8 | Unparseable target keys | N+1 | **APPROVE WITH CONDITIONS** | Condition: T-DN-4 (the full-sweep count), not prototyped |
| A-9 | `v_open_keys` | N | **REVISE → A-9r** | Reporting categories (Q-5); lineage view |
| A-10 | Key events across the encoding change | N (reading) / N+1 (transition) | **REVISE → A-10r** | At most one event per identity per evaluation (new finding NR-1); ambiguous legacy identities are not REKEYED |
| A-11 | UTC precondition | **N+1** (moved from N) | **APPROVE WITH CONDITIONS** | Release N is schema and compatibility only; condition: T-N-6 |
| A-12 | Ties wording (Q-1) | N+1 (wording) | **APPROVE** | All four Q-1 conditions verified (02 §Q-1) |
| A-13 | Reconciliation model wording | N+1 | **APPROVE** | Needs a decision-log entry (frozen document), which A-15r carries |
| A-14 | Guard: legacy key construction | N+1 | **APPROVE** | Enforces "no partial switch" |
| A-15 | Decision-log entry | N and N+1 | **REVISE → A-15r** | Must list the identity-semantics changes of Q-2 and Q-3 and the deferral of NF-1 |
| A-16 | **New:** known-issue entry for NF-1 | none (documentation) | **APPROVE** (new) | Q-6: record the defect in `docs/known-issues.md` once approved |

## Per-amendment review

Each entry gives: the existing section, the proposed text (by reference or revised in full), consistency with the code and approved semantics, the affected code, the affected tests, migration and compatibility, any remaining uncertainty, and the disposition.

### A-1r · Canonical key string (REVISE)

**Existing:** §6 "Hop details" (spec line 641): "as one string (parts joined with `|`, null written as `<null>`)".

**Revised text (replaces 02's A-1 in full):**
> Keys on both sides are built from the mapped columns with each side's `key_normalise`, in this dataset's key order, as one canonical key (key encoding 2).
>
> **Parts.** Each part is rendered as one of three byte sequences:
> - `~;` for a NULL part;
> - `<n>:<bytes>;` for a value, where `<bytes>` is the part's canonical text and `<n>` its length in bytes in ASCII decimal without leading zeros (the empty string is `0:;`);
> - `!<n>:<raw bytes>;` for an unparseable part, whose raw value is not NULL but whose normaliser returned NULL. `<raw bytes>` is the raw value's canonical text.
>
> The key is the concatenation of its parts.
>
> **Canonical text, by the part's type after `key_normalise`:**
> - STRING and VARCHAR: the bytes as read.
> - CHAR(n): the bytes as read without trailing U+0020 pad characters. A CHAR column holds no trailing-space information: values written with and without trailing spaces read back identically.
> - TINYINT to BIGINT: ASCII decimal, with `-` for negatives.
> - DECIMAL(p, s): the number in plain notation, without trailing fractional zeros or a bare decimal point, and `0` for zero (so `1.00`, `1` and `1.0000000000` are all `1`, and `0.00000001` is never written in scientific notation).
> - DATE: `yyyy-MM-dd`.
> - TIMESTAMP: `yyyy-MM-dd HH:mm:ss.SSSSSS` in UTC.
> - BOOLEAN: `true` or `false`.
>
> No Unicode normalisation, trimming (other than CHAR's pad) or case change is applied beyond the declared normalisers.
>
> **Refused types.** A key part of any other type (FLOAT, DOUBLE, BINARY, ARRAY, MAP, STRUCT, INTERVAL, TIMESTAMP_NTZ, VOID) makes every check that builds the hop's keys DID_NOT_RUN / invalid_config. The detail names the dataset, column and type, and says to declare a normaliser that yields a supported type.
>
> **Keys.**
> - A key with an unparseable part is `unassessable` ("unparseable key") on the side where it occurs.
> - A row whose every key column is NULL as read has no key.
> - Distinct part sequences always give distinct keys.
> - The encoding of each stored key is recorded in `dq_key_event.key_encoding` (section 5).
> - Encoding 1 (parts joined with `|`, null written as `<null>`) is read only to carry open keys across the change (section 6, "Key events").

**Consistency with the code and approved semantics:**
- Replaces the approved line 641. This is an **approved-behaviour change** (A-15r records it).
- Within one column, the CHAR and DECIMAL rules are injective. CHAR reads `'a'` and `'a '` identically, so pad removal merges nothing that was distinct (verified on Hive ORC and a Parquet datasource table). A DECIMAL column has a fixed scale, so distinct values keep distinct texts (2,001 values verified, including values below 10^-6).
- Across layers, the rules remove false FAILs (CHAR(3) against CHAR(5) or STRING; DECIMAL scales).
- ADR-02 (collisions detected at run time) is unchanged.

**Affected code:**
- `checks/keys.py` (`key_string`, new `canonical_text`);
- every `key_string(` call site (`common.py:96, 100`; `ledger.py:291, 294, 348, 366`; `chain.py:196-197`; `contribution.py:86, 89`; `probes.py:204`; `merge.py:137-138`; `trace.py:123`).

**Affected tests:**
- *Restated* (the expected key string changes): `test_hop.py` (lines 36, 370, 562), `test_r04_to_r07_hops.py` (line 22), `test_causes.py` (line 41).
- *New:* T-F02-1 to T-F02-9; T-Q2-1, T-Q3-1 (05).

**Migration and compatibility:** encoding 2 ships only in N+1. The transition and rollback rules are in A-10r and 04.

**Remaining uncertainty:**
- The TIMESTAMP rendering pattern is not executed yet (T-F02-9).
- DATE beyond year 9999 is not verified.

**Evidence:** rev-2 l1–l5, g01b; `verification/test_q2_char.py` (6 tests), `verification/test_q3_decimal.py` (17 tests).

**Disposition: REVISE → approve A-1r**, on condition that T-F02-9 passes before N+1 merges.

### A-2 · Key hash (APPROVE WITH CONDITIONS)

**Existing:** §5 line 574. **Proposed:** 02 A-2, unchanged.

**Consistency:** HMAC-SHA256, lower-case hex, the secret from a file, closure registration: all unchanged (U-3, ADR-08). For valid UTF-8 the value equals today's formula over the new input (rev-2 l8).

**Affected code:** `common.py` `key_hash` and `register_key_hash` (accept STRING or BINARY); the templates' hash call (`hash_fn`).

**Affected tests:** `test_sql_key_hash_matches_python_hmac` **must pass unchanged** (it calls with a STRING).

**Compatibility:** every hash value changes once at N+1 (O-3).

**Uncertainty:** none on the algorithm. Hashes held outside the store stop matching (disclosed).

**Disposition: APPROVE WITH CONDITIONS** (the test above unchanged; T-F02-7).

### A-3 · Key-event columns (APPROVE)

**Existing:** §5 DDL (lines 461-466). **Proposed:** 02 A-3, unchanged.

**Consistency:**
- Additive, at the end of the data columns (`migrations.py` rule).
- `checkpoint3_store` strips only `evaluation_mode`, so `tests/test_evaluation_modes.py` keeps `pending == [1]` and its version-contiguity check (line 107) holds with versions 1 and 2. Verified by running the file (07).

**Affected code:** `store/migrations.py` (`MIGRATIONS`), `store/ddl.sql`, `store/key_events.py`.

**Affected tests:** unchanged: `test_evaluation_modes.py`, `test_install.py`. New: T-MIG-1, T-MIG-3.

**Migration:** idempotent: pending while a column is absent (existing mechanism).

**Disposition: APPROVE.**

### A-4r · Untimed rows (REVISE)

**Existing:** §6 HOP_KEY_CURRENCY (line 605), HOP_LEDGER (line 613). The replacements for lines 605 and 613 are as in 02 A-4.

**Revised "Untimed rows" paragraph (replaces 02's in full):**
> **Untimed rows.** A row whose record time is NULL or does not parse is untimed.
>
> **Relevance.** An untimed row of a key, among the rows loaded before the cutoff, is **relevant** unless all of these hold:
> - the dataset declares a disorder bound `id@version` (section 3) whose state is **in force** (below);
> - the key has a timed row;
> - the row and the key's version (the latest load time among the rows at the latest record time) both have a load time;
> - the row was loaded more than the bound's hours before the version's load time.
>
> A relevant untimed row makes the key unassessable ("unparseable record time" when its raw value is not NULL, "no record time" otherwise; "… here" for this dataset). A row that is not relevant is **set aside** and counted in the scope. Every result that set aside at least one row is qualified `version_by_disorder_bound` (section 5).
>
> The comparison is between two load times, never with a record time, and the bound never chooses a version. A key's deadline is the version's load time, or the latest load time of a relevant untimed row if later, plus `sla_hours`.
>
> **Refutation and the bound's state.** A bound is refuted when, in any evaluation reading the dataset, a key has a timed row older than its version but loaded more than the bound's hours after the version's load time.
> - **Effect in the refuting evaluation:** it applies no bound. Its scope records `disorder_bound: {dataset, id, version, state: refuted, keys: n, key_hashes: up to 10}` (key hashes only, kept in the store, never emailed).
> - **Persistence:** a bound `id@version` that any committed, non-INVALID result has recorded as refuted is **refuted for good**. Every later evaluation reading the dataset applies no bound and records `state: refuted (since <run_id>)`, so the state is carried in the latest results, as `coverage_gap` is.
> - **When the state cannot be read:** an evaluation that cannot read the results back to the bound's `approved_on` (a store whose retention dropped them) treats the bound as refuted, with `state: unverifiable`.
> - **Re-approval:** a refuted bound is in force again only under a new `version`, approved after the refutation and validated by `dre validate` (section 3). Earlier results stay as stored.
> - **Reporting:** the digest names every dataset whose bound is refuted or unverifiable, in every digest, until a new version is in force.

**Consistency:**
- ADR-05 and model §8: one kind of time (load against load); the bound never chooses the version (model §4).
- Spec lines 605 and 613 change (an **approved-behaviour change**; A-15r).
- The persistence uses committed results only (ADR-07), with no new table.

**Affected code:**
- the new `checks/sql/hop_time.sql.j2` (relevance, refutation);
- `hop_ledger.sql.j2`, `hop_common.sql.j2`;
- `ledger.py` and the release 1 checks (scope, qualification);
- a reader for the bound's state (a bounded read of committed results, like `coverage.py`'s);
- the digest line.

**Affected tests:** unchanged: `test_hop.py:392`, `test_hop_ledger.py:170, 180`. New: T-F05-1 to T-F05-10, T-Q4-1 to T-Q4-5.

**Compatibility:** N+1 only. A store without bounds behaves strictly (every untimed row relevant).

**Uncertainty:** the persistence read is **not prototyped** (T-Q4-1 to T-Q4-5 are conditions). Rev-2 b01–b11 and t1–t3 cover relevance and refutation within one evaluation.

**Disposition: REVISE → approve A-4r**, on condition that T-Q4-1 to T-Q4-5 pass before N+1 merges.

### A-5r · Disorder bound configuration (REVISE)

**Existing:** §3 dataset `record_time` (around line 187); §3 "Validation" (line 362).

**Revised text:**
```yaml
record_time: { column: ..., format: "...", timezone: ...,
               disorder_bound: null }   # optional; { id: <name>, version: <int >= 1>, hours: <int >= 1>,
                                        #   approved_by: <name>, approved_on: <date>, evidence: <reference> }
```
> **Validation.** A `disorder_bound` needs every field; `hours` and `version` are integers ≥ 1. A missing or invalid field is an error naming the file, line and field. `null` (the default) means no bound. A bound is identified by `id@version`. A new version is required to put a refuted bound back in force (section 6, "Untimed rows").

**Consistency:** U-5 (NULL by default; evidence and approval required); mirrors exclusions' `id@version` (spec §3, line 247).

**Affected code:** `models.py` (the record-time model), `validate.py`. **Affected tests:** new T-CFG-1 to T-CFG-5; `dre validate --conf conf/` unchanged (the sample stays `null`).

**Disposition: REVISE → approve A-5r.**

### A-6 · Tied rows (APPROVE WITH CONDITIONS)

**Existing:** §6 HOP_LEDGER dispositions (line 613). **Proposed:** 02 A-6, unchanged.

**Consistency:**
- Fills a spec gap.
- The per-row exclusion load time is the executable behaviour (`common.py:136-160`, read). The spec's "the version's load time" is reworded to match.
- Value ties and R06 are untouched.

**Affected code:** `hop_ledger.sql.j2` (`up_keys`, `judged`, `rejected`). **Affected tests:** unchanged: `test_hop_ledger.py`, `test_r19_cdc_versions.py`, R06. New: T-F07-1 to T-F07-7.

**Uncertainty:** soft-deleted + absent is not modelled (rev-2 had no soft mode).

**Disposition: APPROVE WITH CONDITIONS** (T-F07-6).

### A-7 · Qualification (APPROVE)

**Existing:** §5 item 7 (line 555); §6 (line 620). **Proposed:** 02 A-7, unchanged.

**Consistency:** health is defined once (`healthy = PASSED AND VALID AND qualifications IS NULL`, `ddl.sql:283`). The digest is generic over codes (`digest.py:335-362`). Read.

**Affected tests:** unchanged: `test_hop_ledger.py:486-532`, `tests/notify/test_digest.py`. New: T-H-1 to T-H-4.

**Disposition: APPROVE.**

### A-8 · Unparseable target keys (APPROVE WITH CONDITIONS)

**Existing:** §6 "Orphans" (line 616). **Proposed:** 02 A-8, unchanged.

**Consistency:** a separate breach line avoids redefining "orphan" (C-2). It can only push a result toward FAIL.

**Affected code:** `hop_ledger.sql.j2` (`dn_*`), `hop_common.sql.j2`, `ledger.py` (lines, triage). **Affected tests:** new T-DN-1 to T-DN-5.

**Uncertainty:** the full-sweep count is not prototyped.

**Disposition: APPROVE WITH CONDITIONS** (T-DN-4).

### A-9r · `v_open_keys` and reporting categories (REVISE)

**Existing:** §5 views (line 570).

**Revised text:**
> - `dq.v_open_keys`: identities whose latest committed `dq_key_event` at a hop is FLAGGED or STILL_FLAGGED, with `key_encoding`, `predecessor_hash`, `category` and the first flagged date of the identity's open spell.
>   - A spell ends only at CLEARED; REKEYED never ends one.
>   - `category` is `legacy_unresolved` when the latest event's `state_detail` is "awaiting successors" (an encoding-1 identity kept open after the encoding change without a provable successor; section 6, "Key events"), and `active` otherwise.
>   - When an active identity's spell began with an event naming a `predecessor_hash` whose latest event is REKEYED (a provable continuation), the first flagged date is the predecessor's. Otherwise it is the identity's own.
>   - An identity whose latest event is REKEYED is not open.
> - `dq.v_key_lineage`: one row per event that names a `predecessor_hash`: the predecessor, the successor, the hop, `kind` (`continuation` when the predecessor was REKEYED to it, `ambiguous` otherwise, and `rollback` for an encoding-1 continuation after an encoding-2 engine) and when. Lineage rows are records of the transition, never discrepancies.

**Consistency:** `tests/store/test_views.py::test_open_keys` keeps its assertions (same rows and dates; `key_value` absent). The new view is additive (views are replaced by `dre install --apply`).

**Affected code:** `store/ddl.sql` (two views), `store/schema.py` (`VIEWS`). **Affected tests:** unchanged: `test_views.py`, `test_install.py` (both expected unchanged: the views list grows; to be re-run). New: T-MIG-2, T-Q5-1 to T-Q5-4.

**Release:** N (both views; in N every identity is `active`).

**Evidence:** `verification/test_q5_reporting.py` (4 tests: the categories, age kept, lineage never active, one event per identity).

**Disposition: REVISE → approve A-9r.**

### A-10r · Key events across the encoding change (REVISE)

**Existing:** §6 "Key events" (lines 651-653); §7 "What a cause explains" (line 779); §8 "Trace" (line 821).

**Revised text (replaces 02's A-10 in full):**
> **Key encodings.** Each key event records its `key_encoding`. **A hop writes at most one event per identity per evaluation.** Two events of one identity in one evaluation would share `observed_at` and `run_id`, the order that decides the latest event.
>
> **Reading.** A hop reads the open identities of its engine's encoding, taking REKEYED as no event.
> - An engine of encoding 1 that finds encoding-2 identities open at a hop judges the hop as on the full sweep. It writes REKEYED ("key encoding 1 (rollback)") for each, and continues the encoding-1 spells, which REKEYED never ended.
>
> **Transition.** On the first NORMAL evaluation of a hop by an engine of encoding 2 while encoding-1 identities are open there, the evaluation reads every upstream row before the cutoff once. For each open encoding-1 identity it finds the successors: keys whose encoding-1 string equals the stored `key_value`. Then:
> - **Provable continuation:** the stored string decodes to exactly one tuple given the parts' types, and that tuple's key exists upstream. The engine writes REKEYED ("key encoding 2", encoding 1), and the successor's own event (STILL_FLAGGED, or CLEARED when current) names the predecessor.
> - **No successor:** CLEARED "no longer upstream" (encoding 1).
> - **Otherwise** (several successors, or a string that decodes to more than one tuple): the encoding-1 identity is **not** REKEYED. It stays open as STILL_FLAGGED "awaiting successors" (encoding 1, its original first flagged date), re-emitted while any successor is open, and is CLEARED "successors current" when none is. Each successor still wrong is FLAGGED (encoding 2) naming the predecessor, with its own age.
>
> All transition events are written in the evaluation's own commit. CHAIN_COMPLETENESS's PENDING records follow the same rules.
>
> **Reporting.** The digest counts open identities by `category` (section 5): active business discrepancies; legacy identities awaiting successors (with their first flagged date). Lineage and ambiguous transitions are never counted as discrepancies.
>
> **§7 addition:** A `failure_ref` that is a `key_hash` is in the key encoding of its run's engine (`dq_run.engine_version`). A retried investigation writes its own engine's encoding.
>
> **§8 Trace addition:** found by its `key_hash` in each key encoding the engine can compute, each line labelled by its encoding. An engine of encoding 1 reports events of a later encoding at the hop as present but not searchable by key.

**Consistency:**
- U-4 (no age without provable continuity; the original discrepancy kept open).
- Determinism (one event per identity: new finding NR-1).
- ADR-07 (one commit).
- Append-only.

**Affected code:**
- `hop_common.sql.j2` `open_keys` (:36-50);
- `chain.py` (:338-345);
- the new `checks/hop/rekey.py` + `hop_rekey.sql.j2`;
- `store/key_events.py`;
- `notify/digest.py` (:206, the counts by category);
- `trace.py` (:115-123).

**Affected tests:** unchanged: `test_open_keys_idle.py`, `test_chain.py`, `test_trace.py`, `test_digest.py`. New: T-MIG-4 to T-MIG-9, T-Q5-1 to T-Q5-4.

**Release:** reading, rollback handling, the single-event rule and trace labelling in N; the transition and digest categories in N+1.

**Uncertainty:**
- The transition is modelled (rev-2 m3–m7; step 0 continuity; Q-5) but not run against the engine.
- The end-to-end rollback is a gate (05).

**Disposition: REVISE → approve A-10r.**

### A-11 · UTC precondition (APPROVE WITH CONDITIONS; moved to N+1)

**Existing:** §8 `dre run` exit 3 list (line 790). **Proposed:** 02 A-11, unchanged.

**Consistency:** `get_spark` already sets UTC (rev-2 k8). The precondition guards encoding 2's TIMESTAMP rendering, so it belongs with N+1. Release N is schema and compatibility only.

**Affected code:** `runner.py` (run start). **Affected tests:** new T-N-6. Existing CLI tests are expected unchanged (they use `get_spark`).

**Disposition: APPROVE WITH CONDITIONS** (T-N-6; release N+1).

### A-12 · Ties wording (APPROVE)

**Existing:** §6 "Ties" (line 649). **Proposed:** 02 A-12, unchanged.

**Q-1 conditions verified:**
1. The value-set comparison is unchanged: the text describes the executable behaviour (`test_value_agreement_compares_the_tied_latest_rows_as_sets` passes).
2. R06 stays valid (`test_r06_…` passes; gold holds one row against a set of two).
3. `winner_rule` does not become authoritative (the text says so).
4. No approved tie behaviour changes.

**Affected tests:** none change.

**Disposition: APPROVE.**

### A-13 · Model wording (APPROVE)

**Existing:** `reconciliation-model.md` §4 (lines 90, 97) and §8. **Proposed:** 02 A-13, unchanged.

**Consistency:** matches the code (`winner_rule` read only by `causes/merge.py:149-163`).

**Disposition: APPROVE**, with the decision-log entry (A-15r), since the model is frozen.

### A-14 · Guard (APPROVE)

**Proposed:** 02 A-14, unchanged.

**Consistency:** prevents comparing encoding 1 with encoding 2 (a partial switch matches nothing).

**Affected tests:** new `tests/guard/test_key_encoding.py`.

**Disposition: APPROVE.**

### A-15r · Decision-log entry (REVISE)

**Revised text** (for `docs/decisions.md`, applied only after approval):

> **Gate A: key identity, untimed rows, ties (approved YYYY-MM-DD).**
>
> Approved by the user: A-1r, A-2, A-3, A-4r, A-5r, A-6 to A-8, A-9r, A-10r, A-11 to A-14 and A-16 of `docs/reviews/gate-a-step0/final-approval-package/01-amendment-approval-matrix.md`.
>
> **Previously approved behaviour changed:**
> - the key string (spec §6, line 641), including:
>   - CHAR keys compared without pad;
>   - DECIMAL keys compared by numeric value;
> - HOP_KEY_CURRENCY's UNASSESSABLE (line 605);
> - the ledger's "no record time" (line 613);
> - the ledger's exclusion effectiveness for tied rows (per row).
>
> **Unchanged:** `winner_rule` semantics, R06, the value-set tie rule, ADR-01 to ADR-08 (ADR-02's collision rule implemented).
>
> **Defaults taken:**
> - every disorder bound `null`;
> - a refuted bound stays refuted until a new version;
> - TIMESTAMP_NTZ and BINARY key parts refused.
>
> **Deferred:** NF-1 (KI-20), outside Gate A.
>
> **Release split:**
> - N: A-3, A-9r, A-10r (reading);
> - N+1: the rest.

**Disposition: REVISE → approve A-15r.**

### A-16 · Known issue KI-20 for NF-1 (APPROVE, new)

**Register:** `docs/known-issues.md` is the project's defect register. This task may not modify it, so the entry is proposed here.

**Proposed entry:**
> **KI-20 · TIE_RESOLVED_BY_RULE can under-count tied value combinations (open; outside Gate A).**
> - **The defect:** `checks/sql/cause_key_ties.sql.j2:27` counts distinct owned-value combinations among tied rows by joining them with `|` and writing NULL as `<null>`. Combinations such as (`x|y`, `z`) and (`x`, `y|z`), or NULL and the text `<null>`, count as one. `causes/merge.py:202` confirms the cause only when the count exceeds 1, so such a tie is reported RULED_OUT.
> - **Impact:** the check's state is unaffected (causes never change a result); a cause line can be wrong.
> - **Until fixed:** a RULED_OUT TIE_RESOLVED_BY_RULE is not conclusive when owned values can contain `|` or the text `<null>`.
> - **Fix:** count with the canonical part encoding (shown collision-free in `verification/test_q6_nf1.py`).

**Disposition: APPROVE** (documentation only; no release scope).
