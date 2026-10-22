# 03 · Temporal eligibility (F-05, revision 2)

This replaces the original G-3 ("every row of the key must be timed") and merges Q2 and Q3 into one decision (R2-06).
- **Status.** Proposed.
- **Evidence.** The rules are modelled in `verification/proto_ledger.py` and exercised in `test_rules_model.py`.

## 1. The defect, restated

`MAX(record_ts) OVER (PARTITION BY k)` ignores NULLs (`hop_ledger.sql.j2:70, 75, 120, 125`; `hop_common.sql.j2:83-84, 89-90`). A key whose **newest** row has a NULL or unparseable record time is judged on an **older** version, and may not be due at all.
- **Effect:** a false PASS (examination l01, m04).
- **Cause:** a row that cannot be placed in time is treated as if it were older than everything.

## 2. Terms

| Term | Definition |
| --- | --- |
| **record_ts** | The row's record time, parsed with its configured format (`utc_expr`, as today). NULL when the raw value is NULL **or** fails to parse. |
| **timed / untimed** | record_ts is non-NULL / NULL. "Unparseable" = untimed with a non-NULL raw value. |
| **version** | The timed rows of the key with `record_ts = MAX(record_ts)` (unchanged). |
| **v_load** | `MAX(load_ts)` over the version's rows. |
| **D** | An optional, declared and approved **disorder bound** for the dataset, in hours: versions of a key reach the table at most D out of record-time order. Not declared means NULL, the strict default. |

## 3. The rule

An untimed row of a key (before the cutoff) is **relevant** unless it is provably older than the version:

```
relevant(u) :=  top_ts IS NULL              -- no timed row: nothing to be older than
             OR u.load_ts IS NULL           -- unknown load: cannot be placed
             OR v_load IS NULL
             OR D IS NULL                   -- strict default: no declaration, no proof
             OR u.load_ts >= v_load - D     -- loaded within D of the version: could be newer
```

- **Any relevant untimed row makes the key unassessable**, labelled:
  - `unparseable record time` when any relevant untimed row has a non-NULL raw value;
  - otherwise `no record time`.

  The downstream side has the same pair of labels, ending `… here`. Counts go in the scope, not the label (G-4 resolved: bounded labels).
- **A non-relevant untimed row is set aside.** It cannot be newer than the version, so it does not affect the judgement. It is **counted** in the scope (`untimed rows set aside`), never dropped silently.
- **Load is compared with load** (`u.load_ts` against `v_load`), never with a record time (CLAUDE.md, times rule). D says nothing about record times. It bounds how far out of order *loads* can be.

### 3.1 The due load

`due_load = greatest(v_load, MAX(load_ts of relevant untimed rows))`.
- An untimed row arriving later makes the key due again in its own window. That is new evidence, judged once there; earlier windows stand.
- Set-aside rows never move the due load, so a two-year-old bad row cannot make a key due today.
- For a fully timed key, `due_load = v_load`: **unchanged** from today, so nothing already judged is re-judged.

### 3.2 Why D, and why strict by default

The user's example: one malformed row two years ago, valid updates since, gold correct.
- **Without D,** DRE cannot prove the old row is older than the current version. Out-of-order CDC (examination a16) is exactly the case where a row loaded long ago carries a newer record time. The key stays `unassessable`: an inconclusive FAIL, the safe direction (`test_b02`).
- **With an approved D,** the old row (loaded more than D before the version) is set aside and counted, and the key is judged: delivered (`test_b03`).

**D is a declaration DRE cannot prove.** Two safeguards follow:
1. **Qualification.** A PASS that rests on at least one set-aside row carries the new qualification `version_by_disorder_bound` ("untimed rows were set aside on the declared disorder bound"), so it is never counted as healthy (`ledger.py:120-131` semantics). A PASS with no set-aside rows is unaffected. (Decision R2-D2.)
2. **Falsification guard.** The timed rows of the same evaluation can **refute** D.
   - Per key, a timed row older than the version but loaded more than D after it (`MAX(CASE WHEN record_ts < top_ts THEN load_ts END) > v_load + D`) is a version pair out of order by more than D.
   - It is computed in the same window as `top_ts`, so it adds no extra shuffle.
   - When any key refutes D, the evaluation applies the **strict** rule to every key: no row is set aside. The scope and the digest say `disorder bound refuted by n keys`.
   - The guard is a sufficient detector, not a proof. It cannot see disorder involving untimed rows.
   - *Proposed; prototyped in `test_disorder_guard.py` (07).*

A feed with historical backfills (examination b10-type corrections: an old record time loaded now) refutes D by construction. That is correct: such a feed does not deliver versions in order, and must stay strict.

### 3.3 Where D is declared

- **Field:** `disorder_bound_hours` on the dataset's record-time configuration, next to `record_time` and its format.
  - Type: number > 0, or `null`.
  - Default: `null`.
- **Approval:** an approval reference with the same structure as an exclusion's (`id`, `version`, `approved_by`, `approved_on`), so that `dre validate` reports an unapproved bound as an error. The value is never inferred.
- **Unknown production values stay unknown** (CLAUDE.md). The sample configuration leaves it `null`.
- *Proposed; exact field placement to be fixed against `models.py` in step 1 of 09.*

## 4. Edge cases

| Case | Outcome | Evidence |
| --- | --- | --- |
| Newest row untimed (load in range) | due; `unassessable 'no record time'` | `test_b01` (the F-05 defect, fixed) |
| Old untimed row, no D | `unassessable 'no record time'` | `test_b02` |
| Old untimed row beyond D | set aside (counted 1); key judged: delivered | `test_b03` |
| Untimed row within D of the version | relevant: unassessable | `test_b04` |
| Untimed row with no load time | always relevant: unassessable | `test_b05` |
| Empty string, garbage, invalid calendar date (`2026-02-30`), wrong format | `unparseable record time` | `test_b06` |
| Valid record time, no load time | unchanged: `untimed` line (full sweep: unassessable `no load time`) | `test_b07` |
| Record time invalid, load time valid | due by its load; unassessable | `test_b08` |
| Untimed row loaded after the cutoff | not in this window; judged in its own | `test_b09` |
| Backfill / historical correction (older valid version loaded now) | not the version; due load is the version's; not re-judged | `test_b10` |
| Point-in-time replay | sees only rows loaded before its cutoff | `test_b11` |
| Untimed row plus a conflicting tie exclusion | the time label first | `test_g02` |
| Excluded row with no record time | does not account for the key: unassessable | `test_g04` |
| Several invalid keys in one window | each key reported once, with its own label | `test_g05` |
| DST ambiguity in a record time | unchanged (a value is produced); disclosed limitation (spec section 4) | out of F-05 |

## 5. Invariants (verified on generated populations)

- **Exactly one line per candidate key**, and every untimed row of a candidate key is counted: relevant (its key is unassessable) or set aside. No untimed row is lost.
  - Verified on three seeded populations of 300 rows with NULL, garbage and out-of-window times, NULL loads, tombstones, exclusions and malformed key parts: `test_g06`.
  - *Limitation of that check, disclosed:* its independent count query encodes a malformed effective date as NULL rather than with the `!` marker. The generated population holds no genuinely NULL effective date, so the two encodings coincide there.
- **No PASS-class disposition on a key with relevant untimed rows** (`test_g06`).
- **Deterministic** across `spark.sql.shuffle.partitions` 1 and 8 (`test_g07`).

## 6. Interplay with coverage, SLA and the release 1 checks

- **Coverage** (CA-01, `coverage.py`).
  - Unchanged: coverage is about which windows were judged, not which rows.
  - Set-aside rows are counted per evaluation and are not carried.
- **SLA.** The due range is still on the due load with `sla_hours` folded in. Only the due-load definition in §3.1 changes, and only for keys with relevant untimed rows.
- **HOP_KEY_CURRENCY and HOP_VALUE_AGREEMENT** (`hop_common.sql.j2`): the same relevance rule, giving the state `UNASSESSABLE` with the same labels. Today `hop_ledger.sql.j2` does not import `hop_common.sql.j2` (read): the rule is written once as a macro in a new `hop_time.sql.j2`, imported by both templates, so the two cannot drift.
- **Chain** (`chain.sql.j2`): records are carried by key; the record-time rule applies where the chain reads a version (unchanged otherwise).
