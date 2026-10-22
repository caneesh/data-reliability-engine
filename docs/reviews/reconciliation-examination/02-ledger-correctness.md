# 02 · Ledger correctness: dispositions, invariants, accounting

Source: `checks/sql/hop_ledger.sql.j2` (read line by line) and `checks/hop/ledger.py` (`DISPOSITIONS` :63-76, `counted` :384-411, `judge` :415-462, the empty-population return :602). Tests named `aNN`, `lNN`, `mNN` are in `adversarial/` and were executed (07).

## 1. The accounting invariant

**What is counted.**
- The `due` population is **one row per canonical key `k`** among the upstream rows loaded before the cutoff (`up_cut` :47-49). For each key the version under judgement is the rows at the key's latest `record_ts`, ties kept (`up_ranked`/`up_top` :69-76). The key is due when that version's latest load time is in the deadline range (`due_keys` :86-100).
- Each due key gets exactly one disposition: `coalesce(element_at(matched, 1), residual)` (:268). Keyless upstream rows in range are added as `unassessable 'no key'` (:296-299).

**Does the invariant hold?** Structurally, yes. Every CTE joined into `judged` is grouped by `k` (`dn_keys`, `up_sets`/`dn_sets`, `w*_keys`, `rejected`, `unattributed` DISTINCT), so rows do not multiply. And the residual is total: present with record times on both sides gives stale, divergent or delivered, and every NULL case is caught earlier as unassessable. Verified: a14 (duplicate identical events count once), a03 and a26 (key-level, not count-level).

**The balance check is weak.** `counted()` (:408-410) compares `due` with the sum of the buckets. Both come from the same `assessed` rows, so the check cannot detect a key that drops out *before* `due_keys`. That is exactly how F-05 loses a record. It would only catch a NULL disposition or row multiplication. Classification: design weakness (S-22).

**Logical record vs physical row.** A record is a canonical key, so the invariant is only as good as the key string:
- **F-02:** `key_string` joins the parts with `'|'` and writes NULL as `'<null>'` (`keys.py:119-122`). Two different records can share one `k`; executed in a19, a20, l05 and, through the chain, c05.
- The collision guard (`raw_variants` > 1, :71 and :202) uses the same string over the raw columns, so it cannot see this collision.

**Pending and untimed.**
- `pending` = due keys after the range end (:301-303).
- `untimed` = candidate keys whose version has no load time (:304-309). Candidate keys are those with a row loaded since the range start, so a key whose rows all lack a load time is never a candidate on a normal run and appears only on the full sweep (:88-90).
- **F-05:** a key whose **newest** row has no record time is not untimed. `MAX(record_ts)` ignores the NULL, so an older version is judged and the key is not due (l01).

**Orphans** (downstream rows no upstream row has; :311-321): counted only when `orphans_unchecked()` is None, i.e. every row of the target belongs to this hop and its load time is configured. Otherwise the PASS carries `orphans_unchecked`. Gaps:
- **F-06:** an orphan in a window where nothing upstream is due is never reported. The check returns `empty_population` first (:602), which covers the range (l02).
- **F-06:** a downstream row whose load time does not parse is never in any orphan window (l07).

## 2. Precedence: can a rule hide a more serious defect?

The code's order (`DISPOSITIONS`; `matched` :248-260; residual :261-264):
unassessable › deleted › superseded › deleted_not_applied › excluded › excluded_but_present › rejected › (residual: missing › stale › divergent › delivered).

| Interaction | Behaviour | Evidence | Verdict |
| --- | --- | --- | --- |
| An excluded record that is also present and wrong | `excluded_but_present` (breach, FAILED) | a09 | Correct: the breach is reported |
| An excluded record whose exclusion matches only **one of two tied rows** | Whole key `excluded` (`MAX` over the tied rows, :82), PASSED | l03 | **F-07** (false PASS) |
| A tombstone on one of two tied rows | Whole key `deleted`, PASSED | l04 | **F-07** |
| A stale record classified as delivered | No: `dn_record_ts < up_record_ts` gives stale | a07, a16 | Correct |
| A deleted record classified as missing | Hard deletion: absent is `deleted`. Soft deletion: absent is missing (fails closed) | existing `test_hop_ledger` | Correct |
| A soft-deleted target at an **older** version | `deleted` (`soft_applied` ignores the version, :246) | static | S-23 (Low) |
| An unassessable record treated as acceptable | Never: `unassessable` is a failure class (`judge` :425) | existing | Correct |
| An authorised reject while the record is **present and wrong** downstream | `rejected` (:259 has no `NOT present`), PASSED, qualified `evidence_reported` | a13 | **F-04** (hidden divergence; never healthy) |
| An incorrect target row satisfying `delivered` | Yes, if its key matches, its record time ≥ upstream's, and it differs only in columns outside `owned_columns` | a05, a06, m01, m06 | **F-01, F-09, F-13** |
| Survivorship: a lower-precedence source wins | Without owned columns: A's edge `delivered`, B's edge `superseded`, both PASS | m01 | **F-09** |
| A breach reloaded downstream (an excluded or tombstoned key re-inserted) | Not due (due is upstream-driven) and not an orphan: unseen until the full sweep | l06 | **F-08** |

## 3. PASS / FAIL determination (`judge`, :415-462)

**FAILED when any of these is present:**
- unassessable, deleted_not_applied or excluded_but_present;
- a rejected record for a defect reason;
- an orphan (only when orphans are checked);
- a deficit (missing, stale or divergent) above `tolerance`;
- an exclusion or authorised-reject reason above its `max_share`.

**Otherwise PASSED.**

| Condition | Can it PASS? | Evidence |
| --- | --- | --- |
| Missing records | Only within `tolerance.missing`. The PASS is then **unqualified and healthy** (**F-03**), and validation sets no upper bound (`models.py`, `ge=0`). | a18 |
| Incorrect business attributes | Yes, outside `owned_columns` (**F-01**) | a05, a06 |
| Unauthorised exclusion | No: missing, FAILED | a10 |
| Over-broad approved exclusion | Up to its `max_share`; denominator = the whole due population (correct), so a full sweep or a long range dilutes it (Low) | a11 |
| Unexpected downstream orphans | Not when orphans are unchecked (qualified, never healthy), or in an empty window (**F-06**, silent) | a03, l02 |
| Invalid deletion handling | Not applied: FAILED. Tied tombstone: **F-07** | a12, l04 |
| Incomplete evidence | Unattributable rejects become unassessable (FAILED); an unresolved coverage gap is qualified (CA-01) | existing |
| Unbalanced reconciliation | DID_NOT_RUN / ledger_unbalanced, but the check is weak (§1) | existing |
| Failed Spark query, missing table | DID_NOT_RUN | a23, a24 |
| Empty source population | DID_NOT_RUN / empty_population | a24 |
| Misconfigured tolerance | A large `tolerance.missing` PASSes any loss, unqualified (**F-03**) | a18 |
| A reject choice on a timestamp tie | `max_by(..., rejected_ts)` (:186) is nondeterministic: PASS or FAIL can flip between runs | static (S-08) |

## 4. Disposition-by-disposition correctness matrix

| Disposition | Predicate (code) | Class | Correct when | Known incorrect cases | Tests |
| --- | --- | --- | --- | --- | --- |
| unassessable | :202-214 (key collision on raw variants, not one-to-one (identity), no record time on **every** row, no load time, reject not attributable, reject without a rule) | unknown → FAIL | Every row of the key lacks a time; raw keys collide | A NULL record time on only some rows is not caught (**F-05**); a canonical-string collision is not caught (**F-02**) | a22, l01 |
| deleted | Hard: tombstone and absent. Soft: tombstone and present and `soft_when` | authorised | A single row per version | Tied rows (**F-07**); soft ignores version (S-23) | a12, l04 |
| superseded | Winner's latest version present, target record time ≥ winner's, values equal (only when owned columns exist) | authorised | With owned columns | Without owned columns (**F-09**) | m01 |
| deleted_not_applied | Tombstone and present (hard), or present and not voided (soft) | breach | Yes | — | a12 |
| excluded | `MAX(x_i)` over tied rows, and absent | authorised | A single row per version, within `max_share` | Tied rows (**F-07**) | a09, l03 |
| excluded_but_present | Exclusion and present | breach | Yes | A reload not re-judged (**F-08**) | a09, l06 |
| rejected | Reject of this version (or batch) before the cutoff; the latest reject by time | authorised or defect | Record absent | Present and wrong (**F-04**); tie (S-08) | a13 |
| delivered | Present, target record time ≥ upstream's, owned values equal | ok | Owned columns cover the business attributes; the target is current-state | **F-01, F-13** | a01, a05, a06, m06 |
| divergent | Present, at or after the version, owned values differ (NULL sets count as divergent) | deficit | Yes, for owned columns | Ties with `winner_rule` give a false FAIL (S-09) | a04, a08, a15 |
| stale | Present, target record time < upstream's | deficit | Yes | Precision mismatch gives a false FAIL (S-14) | a07, a16 |
| missing | Absent, no rule matched | deficit | Yes | Whitespace or case without a normaliser gives a false FAIL (a21) | a02, a10 |
| orphan | Downstream key loaded in the window with no upstream row (when checked) | breach | Yes | Empty window, unparseable load time (**F-06**) | a03, a26, l02, l07 |
