# 02 · Final architecture decisions (Q-1 to Q-7; R2-D1 to R2-D15)

**Status of everything here.** These are recommendations for architecture approval. **Not approved**, not authorised for implementation, and not approved for deployment (07 separates the three).

**Evidence** lives in `verification/` (this package):
- `test_q2_char.py`, 6 tests;
- `test_q3_decimal.py`, 17 tests;
- `test_q5_reporting.py`, 4 tests;
- `test_q6_nf1.py`, 4 tests.

It also draws on the earlier prototypes: step 0 `../verification/` (identity continuity, 6 tests; CHAR/DECIMAL check) and revision 2. Results are in 03 §4.

## Q-1 to Q-7

### Q-1 · Value-tie wording (spec line 649)

| Aspect | Content |
| --- | --- |
| Decision | **Approve A-12:** reword line 649 to describe the set rule already stated at line 607 and implemented |
| Conditions (verified) | 1. The value-set comparison is unchanged (`test_value_agreement_compares_the_tied_latest_rows_as_sets` passes).<br>2. R06 is valid (`test_r06_…` passes).<br>3. `winner_rule` is stated as non-authoritative.<br>4. No tie behaviour changes |
| Alternatives | Keep line 649 ("always fail"), contradicting line 607 and the tests; change the code to match 649 (a behaviour change, rejected: it would break an approved test) |
| Justification | The spec contradicts itself; the executable behaviour and line 607 agree |
| False-PASS risk | None (no behaviour change) |
| False-FAIL risk | None |
| Compatibility | None |
| Tests | The existing two, unchanged |

### Q-2 · CHAR padding

| Aspect | Content |
| --- | --- |
| Verified behaviour | Spark 3.5.1 pads CHAR(n) on read, for Hive SerDe ORC tables and Parquet datasource tables alike (`readSideCharPadding` true): `'a'` and `'a '` written to CHAR(3) both read as `'a  '`. A leading space is kept. VARCHAR and STRING keep trailing spaces as written. CHAR(3) `'a'` and CHAR(5) `'a'` cast to `'a  '` and `'a    '` (`test_q2_char.py`) |
| Decision | **CHAR parts:** remove trailing U+0020 only (`rtrim(' ', …)`), so the canonical text is the value without its pad. **STRING and VARCHAR:** never trimmed. Tabs, NBSP and other whitespace are always kept (verified) |
| Why no business-significant space is trimmed | A CHAR column cannot hold trailing-space information: the type erases it on write and read (verified). Removing the pad loses nothing the type kept, and merges no two values the column could hold apart. A trailing space significant in a STRING or VARCHAR column is never touched |
| Alternatives | (a) Keep the pad: CHAR(3) against CHAR(5) or STRING never match, a false FAIL. (b) Trim all whitespace: destroys data in STRING/VARCHAR. (c) Refuse CHAR: excessive. (d) Require a declared `trim` normaliser: works, but every owner must know the rule, and `trim` also strips leading spaces |
| False-PASS risk | None within a side (no two distinct CHAR values merge). Across sides, a STRING `'a'` matches a CHAR `'a'`, which is correct: CHAR `'a'` cannot be distinguished from `'a '` |
| False-FAIL risk | Removed for CHAR against CHAR of another length or against STRING. Remains when a STRING carries a significant trailing space and the other side is CHAR (the CHAR side has lost it: a genuine difference) |
| Compatibility | A change from encoding 1 (which kept the pad). It takes effect only with encoding 2 (N+1). The transition computes encoding 1 exactly as before |
| Tests | T-Q2-1: CHAR(3) against CHAR(5) against STRING keys match in the engine. T-Q2-2: STRING `'a '` against STRING `'a'` stay distinct. T-Q2-3: a leading space is kept. T-Q2-4: a raw-variant guard over CHAR does not fire on pad differences |

### Q-3 · DECIMAL canonicalisation

| Aspect | Content |
| --- | --- |
| Verified behaviour | `CAST(decimal AS STRING)` keeps the scale (`1.00` against `1`) and writes values below 10^-6 in scientific notation (`1.00E-8`). Numerically equal values in layers with different DECIMAL types therefore get **different identities** today (encoding 1) and would under 02's A-1. `format_string('%f')` cannot format a Spark Decimal (verified: it receives Spark's `Decimal`, not a `BigDecimal`) |
| Decision | **DECIMAL parts:** the canonical text is the number in plain notation, without trailing fractional zeros or a bare point, and `0` for zero (including negative zero). Built from the cast with string functions only: expand the scientific form (Spark decimals have scale ≥ 0, so only negative exponents occur), then strip. Verified exact on 12 boundary cases (including DECIMAL(38,0) of 38 nines and DECIMAL(38,38) of 10^-38), injective over 2,001 values of one column (including values below 10^-6), equal to an exact Python mirror on 2,000 random values, and independent of ANSI mode (`test_q3_decimal.py`) |
| Is this a contract change? | **Yes.** DECIMAL key identity becomes the **numeric value**, not the value and its scale. It is an approved-behaviour change (A-15r lists it) and needs explicit approval. It takes effect only with encoding 2 |
| Alternatives | (a) Keep the cast: numerically equal keys in different DECIMAL types never match (a false FAIL), and the scientific form appears in `key_value`. (b) Require a declared normaliser per column: works, but leaves the trap in place. (c) Cast every DECIMAL to one wide type: fails for DECIMAL(38,38) and changes values on overflow |
| False-PASS risk | None within a side: one column has one scale, so its numeric texts are injective (verified). Across sides, only numerically equal values match, which is the intended identity |
| False-FAIL risk | Removed for DECIMAL against DECIMAL of a different type. Remains for DECIMAL against a STRING written with a different format (needs a normaliser; unchanged) |
| Compatibility | N+1 only. The transition computes encoding 1 with the old cast, so legacy identities are found exactly |
| Tests | T-Q3-1: engine keys over DECIMAL(10,2), DECIMAL(10,0) and DECIMAL(20,10) of one number match. T-Q3-2: the boundary cases. T-Q3-3: per-column injectivity. T-Q3-4: the raw-variant guard over DECIMAL |

### Q-4 · Disorder-bound refutation

| Aspect | Content |
| --- | --- |
| Decision | **A refuted bound stays invalid until explicitly re-approved** (A-4r, A-5r) |
| How refutation is persisted | In the scope of the committed result that observed it: `disorder_bound: {dataset, id, version, state: refuted, keys: n, key_hashes: ≤10}`. Committed, append-only and auditable (ADR-07). Every later evaluation reading the dataset carries `state: refuted (since <run_id>)`, so the state is always in the latest results (the `coverage_gap` pattern; CA-01) |
| Subsequent evaluations | Read the bound's state: a bounded read of committed, non-INVALID results back to the bound's `approved_on`. When the bound is refuted, no row is set aside: the dataset is judged under **strict** temporal assessment (every untimed row relevant) |
| When the state cannot be read | Retention dropped results after `approved_on`: the bound is treated as refuted, `state: unverifiable` (fail closed) |
| Re-approval | Only by a **new `version`** of the bound (new evidence, new `approved_on` after the refutation, validated by `dre validate`). The refuted `id@version` stays refuted for good |
| Historical auditability | Earlier results, including those that set rows aside, stay as stored, with their qualification. The refuting result names the keys (hashes, kept in the store only) and the counts |
| Reporting | Every digest names datasets whose bound is refuted or unverifiable until a new version is in force |
| Alternatives | (a) Per evaluation only (02 A-4): the bound flips back the next window, so a PASS can rest on a refuted declaration. (b) A new store table of bound events: clean, but needs a `dq_commit` count column (a migration) and a new writer path. (c) Store-free (config only): cannot remember a refutation |
| False-PASS risk | Reduced: a refuted bound can never set a row aside again. Residual: a wrong bound never refuted by the timed rows (its PASSes are qualified, never healthy) |
| False-FAIL risk | A dataset whose bound is refuted is judged strictly, so old untimed rows can make keys unassessable until re-approval. That is intended |
| Compatibility | N+1 only. No schema change |
| Tests | T-Q4-1: refuted in one window, still refuted in later windows. T-Q4-2: a new version is in force. T-Q4-3: unverifiable state when retention dropped the evidence. T-Q4-4: digest reporting. T-Q4-5: refutation by a hop of another dataset that reads the same upstream also binds this hop. **Not prototyped**: these are conditions on A-4r |

### Q-5 · Legacy discrepancy counts

| Aspect | Content |
| --- | --- |
| Decision | **Four reporting categories, never merged** (A-9r, A-10r):<br>1. **Active business discrepancies:** `v_open_keys.category = 'active'`.<br>2. **Unresolved legacy identities:** `category = 'legacy_unresolved'` ("awaiting successors"), with their original first flagged date.<br>3. **Migration lineage records:** `v_key_lineage` rows of `kind = 'continuation'` (or `rollback`).<br>4. **Ambiguous identity transitions:** `v_key_lineage` rows of `kind = 'ambiguous'`.<br>The digest counts 1 as discrepancies, and lists 2 apart with its age. 3 and 4 are never counted as discrepancies |
| No age reset | A provable continuation inherits the predecessor's first flagged date. A legacy identity without one keeps its own date and stays open. Successors of an ambiguous identity carry their own age and name their predecessor. Nothing is closed by the transition except by CLEARED on evidence ("no longer upstream", "successors current") |
| Determinism finding (NR-1) | Writing REKEYED and STILL_FLAGGED for the same legacy identity in one evaluation (as 02's A-10 implied for ambiguous identities) gives two events with equal `observed_at` and `run_id`, the ordering keys of `v_open_keys`, so the "latest" event is arbitrary. **Rule: at most one event per identity per evaluation.** Ambiguous legacy identities are therefore not REKEYED (A-10r) |
| Alternatives | One combined count (overstates business defects by the number of legacy identities); closing ambiguous legacy identities (loses age; violates U-4) |
| False-PASS / false-FAIL risk | None on check results (reporting only). A misreported defect count is avoided |
| Compatibility | The views ship in N (every identity `active` there), so rollback needs no view change |
| Tests | T-Q5-1 to T-Q5-4 (the model: `test_q5_reporting.py`, 4 pass); T-MIG-2 |

### Q-6 · NF-1 (cause-evidence collision)

| Aspect | Content |
| --- | --- |
| Can distinct combinations collide? | **Yes.** Verified: (`x|y`, `z`) and (`x`, `y|z`) count as one; NULL and the text `<null>` count as one (`test_q6_nf1.py`) |
| Can evidence be under-counted? | Yes: `distinct_values` in the cause evidence is under-counted |
| Can conclusions mislead? | **Yes.** `causes/merge.py:202` confirms TIE_RESOLVED_BY_RULE only when `variants > 1`. A collision makes a real tie RULED_OUT, which is a positive "not this cause" statement, and the engine moves on to other causes |
| Can a check result be wrong? | **No.** Causes never change a check's state (spec §7). No false PASS or false FAIL of a check |
| Deferrable independently of Gate A? | **Yes.** It touches one cause template and no key identity, store schema or check semantics. The fix is small (count with the canonical part encoding: `test_q6_nf1.py` shows it is collision-free) |
| Qualification until fixed | KI-20 (A-16): a RULED_OUT TIE_RESOLVED_BY_RULE is not conclusive where owned values can contain `|` or the text `<null>` |
| Register | `docs/known-issues.md` is the register, but this task may not modify it. The entry is proposed as A-16, to be applied with the approval |
| Release scope | **Not in N or N+1** unless separately approved. No silent scope expansion |
| Tests (when fixed) | T-NF1-1: separator collision; T-NF1-2: NULL against `<null>`; T-NF1-3: ordinary values unchanged; T-NF1-4: R06's cause still CONFIRMED |

### Q-7 · TIMESTAMP_NTZ

| Aspect | Content |
| --- | --- |
| Decision | **Unsupported** (refused, DID_NOT_RUN / invalid_config), like BINARY (U-2) |
| Conditions for future support | All of:<br>1. A canonical text defined independently of the session time zone (the NTZ value's wall-clock fields in a fixed pattern).<br>2. A stated rule for comparing NTZ with TIMESTAMP across layers (refused, or a declared zone).<br>3. Executed proof that the rendering is identical under every session time zone and with ANSI on and off.<br>4. A Python mirror equal on generated values.<br>5. A spec amendment approved |
| False-PASS / false-FAIL risk | None from refusal (the hop is DID_NOT_RUN and visible). Operational: a dataset keyed on NTZ gets no hop judgement until a normaliser converts it |
| Compatibility | O-2 must confirm no production key uses it |
| Tests | T-F02-6 (refusal) |

## R2-D1 to R2-D15: final status

| ID | Final status | Through |
| --- | --- | --- |
| R2-D1 | Recommended **as revised** (CHAR, DECIMAL) | A-1r |
| R2-D2 | Recommended | A-7 |
| R2-D3 | Recommended **as revised** (persistent refutation; keyed bound) | A-4r, A-5r |
| R2-D4 | Recommended (refused types incl. TIMESTAMP_NTZ) | A-1r; O-2 |
| R2-D5 | Recommended (dual-input UDF) | A-2 |
| R2-D6 | Recommended, **moved to N+1** | A-11 |
| R2-D7 | Recommended | A-1r |
| R2-D8 | Recommended (per-row exclusion time) | A-6 |
| R2-D9 | Recommended | A-6 |
| R2-D10 | Recommended | A-4r, A-8 |
| R2-D11 | Recommended (`unparseable target key` line) | A-8 |
| R2-D12 | Recommended **as revised** (provable continuity; no REKEYED for ambiguous identities; one event per identity) | A-3, A-9r, A-10r |
| R2-D13 | Recommended (rollback-aware N; end-to-end gate) | A-10r; 04 |
| R2-D14 | Recommended (existing guard) | none (T-MIG-8) |
| R2-D15 | Recommended | A-12, A-13 |

**None** of these is final until the approvals in 06 are given.
