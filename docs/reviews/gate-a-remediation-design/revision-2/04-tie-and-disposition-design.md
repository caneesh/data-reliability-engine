# 04 · Ties and disposition order (F-07, revision 2)

This refines the original G-5 and G-6 (R2-07). Value-tie semantics are **unchanged**.
- **Status.** Proposed.
- **Evidence.** Model `verification/proto_ledger.py`; tests `test_rules_model.py` c01–c08 and g02–g04.

## 1. The defect, restated

`up_keys` takes `MAX(CASE WHEN tomb ...)` and `MAX(x_i)` over the rows tied at the version (`hop_ledger.sql.j2:77-84`). One tied row's tombstone or exclusion therefore explains the **whole** key, although the other tied row says the record is live.
- **Effect:** a false PASS, `deleted` or `excluded` (examination l03, l04).
- **Scope.** The identity mapping is not affected: ties there are already `unassessable 'not one-to-one'` (`hop_ledger.sql.j2:204-206`). F-07 concerns the other mapping kinds.

## 2. The rule: tied rows must agree on expected presence

Each row of the version (the rows tied at `top_ts`) has one **expected state**, computed per row:

| Expected state | When | What the target should show |
| --- | --- | --- |
| `present` | No tombstone, and no approved exclusion matches the row | The record (value comparison as today) |
| `absent` | A tombstone under `applied_as: hard`, **or** an approved exclusion matches the row (`x_i`) | No record |
| `soft_deleted` | A tombstone under `applied_as: soft` | The record, marked by `soft_when` |

- **`x_i` is already per row.** `exclusion_terms` (`common.py:136-160`, read) folds each exclusion's effective dates (on the upstream load time) into its row condition. So "an approved exclusion matches the row" means the exclusion is in force for that row: each row is **independently** covered (Q4).
- **The decision.** When the version's rows hold **more than one** expected state, the key is `unassessable 'tied rows disagree'`. The rows' states and the rules involved go into the scope. Otherwise the agreed state decides:

| Version rows | Target | Disposition | Evidence |
| --- | --- | --- | --- |
| all `present`, identical | present, same | delivered (one record) | `test_c01` |
| all `present`, values differ | the same set of values | delivered (approved set rule) | `test_c02` |
| all `present`, values differ | a subset (one value kept) | divergent: unchanged; R06 / `TIE_RESOLVED_BY_RULE` explains it | `test_c02` |
| `present` + `absent` (tombstone or exclusion) | any | `unassessable 'tied rows disagree'` | `test_c03` (the F-07 defect, fixed) |
| all `absent`: tombstone + exclusion (hard) | absent | `deleted`, with `excluded_by` also recorded (each rule's volume guard counts it) | `test_c04` |
| all `absent`: tombstone + exclusion (hard) | present | `deleted_not_applied` (existing precedence: before `excluded_but_present`) | `test_c04` |
| all `absent`: two different approved exclusions | absent | `excluded` (both labels; each rule's guard counts it) | `test_c05` |
| all tombstones | absent | `deleted` | `test_c06` |
| `soft_deleted` + `absent` | any | `unassessable 'tied rows disagree'` (the two expect different target states) | *Proposed; not modelled* (the model has no soft mode). Implementation test T-F07-6 (09) |

**Why not `winner_rule`.** The pipeline may resolve the tie by a declared rule, and making that rule authoritative would change approved semantics:
- spec line 649: "Every upstream row at a key's latest record time is kept";
- R06.

The rule stays non-authoritative (Q1 = No, unchanged). `TIE_RESOLVED_BY_RULE` (spec line 761) remains the cause check that explains the resulting FAIL.

## 3. Rejects among tied rows

- A reject record names `(key, version)` (C3-01 lineage), not a row.
- If the version's rows are not all identical (more than one distinct signature over value, tombstone and exclusions), the reject cannot be attributed to the row it rejected: `unassessable 'reject ambiguous among tied rows'` (`test_c08`).
- If they are identical, the reject applies as today.

**A total reject order** (fixes the `max_by` on time only: the original G-6, kept and verified): the latest reject per key is chosen by
1. `rejected_ts` descending;
2. then **defect before authorised**;
3. then `reason` (greatest first, byte order);
4. then rule id.

The order is deterministic and fails closed: when one reject says defect and another says authorised at the same instant, the key is `rejected` with the **defect** reason (`test_c07`, three repetitions).

## 4. The full disposition order (ledger)

`unknown_reason`, first match wins. New or changed rows are in **bold**; the rest are as read at `hop_ledger.sql.j2:202-217`.

| # | Reason / disposition | Source |
| --- | --- | --- |
| 1 | `key collision` (raw variants > 1, either side) | existing; correct once raw keys are LPB (02 §6) |
| 2 | **`unparseable key`** (a `!` part, either side) | 02 §2 |
| 3 | `not one-to-one` (identity mapping only) | existing |
| 4 | **`unparseable record time`** / **`no record time`** (relevant untimed rows) | 03 §3 (replaces the existing `d.up_record_ts IS NULL` test) |
| 5 | `no record time here` (target side; existing `dn_untimed` widened to "any relevant untimed row") | 03 §3 |
| 6 | `no load time` | existing |
| 7 | **`tied rows disagree`** | §2 |
| 8 | **`reject ambiguous among tied rows`** | §3 |
| 9 | `reject not attributable to a version`, `reject names no rule` | existing |

Then the matched dispositions in their existing precedence (`hop_ledger.sql.j2:244-260`):
- `deleted`, `superseded`, `deleted_not_applied`, `excluded`, `excluded_but_present`, `rejected`;
- then the residual: `missing`, `stale`, `divergent`, `delivered`.

What changes is only *what feeds them*: the tombstone and exclusion flags are now the agreed version state, not `MAX` over the tied rows.

- **The model's order** (`proto_ledger.py`, final `CASE`) follows this table, minus the rows the model does not carry: identity, load time, superseded and soft. `test_g02`–`test_g05` exercise the combinations.
- **The release 1 checks** (`hop_common.sql.j2`) apply rules 1, 2, 4, 5 and 7 as `UNASSESSABLE` with the same labels. They have no reject or exclusion dispositions beyond the existing `excluded` count.

## 5. What stays as it is

- **HOP_VALUE_AGREEMENT** compares the tied rows' values as a set (`test_value_agreement_compares_the_tied_latest_rows_as_sets`, passing on current code: 07).
- **R06**: VALUE_AGREEMENT FAILED, cause `TIE_RESOLVED_BY_RULE`.
- **`winner_rule`** is read only by `causes/merge.py:149-163`.
- The identity mapping's `not one-to-one`.
