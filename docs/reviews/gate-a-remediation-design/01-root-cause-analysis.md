# 01 · Root-cause analysis: F-02, F-05, F-07

**Baseline.**
- Branch `claude/claude-md-documentation-a2lxt0`, HEAD `c5e6237`; `c5e6237` is in the history.
- Working tree: only `docs/reviews/` is untracked (the examination and this design).
- Sources: the examination documents (`docs/reviews/reconciliation-examination/`, all ten read) and the 52 adversarial tests in its `adversarial/`.

Every conclusion below was re-checked against the code, and the reproductions were re-run as part of the examination run of 2026-10-21 (52 passed). Line numbers are at `c5e6237`, under `src/hcsc/datalake/dre/`.

**Where the examination was not exact**, corrected here:
- **F-02.** Hashing does not mask the collision; it inherits it. `key_hash` = HMAC of the collided string (`checks/hop/common.py:191-201`), so the open-key state of two colliding records is merged too.
- **F-07.** Not every tie is a defect. The approved tie rule for **values** (sets compared, `tests/checks/test_hop.py::test_value_agreement_compares_the_tied_latest_rows_as_sets`) is deliberate and stays. Only the **rule predicates** (exclusion, tombstone) aggregated over tied rows, and the **reject** choice, are defective.
- **F-05 has a downstream half** that the examination did not test. `dn_untimed` is computed over `dn_top` (`hop_ledger.sql.j2:126-131`), which has already dropped NULL rows whenever any row has a time. So a downstream row with a NULL record time is ignored in the same way.

## F-02: Composite business-key collisions

**Mechanism.** `checks/keys.py:119-122`:

```python
parts = [f"coalesce(CAST({e} AS STRING), '<null>')" for e in exprs]
return f"concat_ws('|', {', '.join(parts)})"
```

The encoding is not injective:
- `'|'` inside a part is indistinguishable from the separator;
- the NULL marker `'<null>'` is a legal value;
- a normaliser that maps an invalid value to NULL (`parse_date`, `keys.py:70-72`) makes "unparseable" equal "genuinely NULL".

**Every place it is used** (one encoder, so every use is affected identically):

| Use | Where | Effect of a collision |
| --- | --- | --- |
| Ledger identity, both sides; raw-variant guard | `ledger.py:291, 294` (`up_key`, `up_raw`, `dn_key`, `dn_raw`); `hop_ledger.sql.j2:29, 71, 101, 121` | Two records become one `k`. The guard compares raw strings built the same way, so it cannot see it (a19, a20) |
| Release 1 hop checks | `hop/common.py:96, 100` | The same, with no raw-variant guard at all |
| Survivorship winners, rejects | `ledger.py:348, 366` | A reject or winner can attach to the wrong record |
| Chain forward mapping | `hop/chain.py:196-197` | The collision is carried end to end (c05) |
| Contribution references | `hop/contribution.py:86, 89` | A reference resolves to the wrong contributor |
| Cause probes | `causes/probes.py:204`, `causes/merge.py:137-138` | A cause attached to the wrong record |
| Trace | `trace.py:115-123` | A lookup by hash finds the merged history |
| **Persisted:** `dq_key_event.key_value` = the string; `key_hash` = HMAC(string) | `hop_key_currency.sql.j2:53`, `chain.sql.j2:100-110`, `store/key_events.py` | The open-key state is merged; **these strings are read back as live identity** (`hop_common.sql.j2:39-50`; `hop/chain.py:338-345`) |

**Not affected:**
- `T1_KEY_DUPLICATES` and the rule templates group by the separate key columns (`key_exprs`, not `key_string`). They neither collide nor detect this collision.
- Evaluation ids, `dq_reconciliation` and the results hold no key strings.

**Where the record disappears.** In `up_keys` (`:77-84`): two upstream records share one `k`, and only one version is judged (a19, l05, c05). Counts, dispositions and replays all inherit the merge.

## F-05: The newest version is ignored when its record time is invalid or missing

**The selection path** (`hop_ledger.sql.j2`), step by step:
1. **Population:** `up_rows` (:28-45), every row.
2. **Parsing:** `utc_expr` (`checks/times.py`). With ANSI off (the Spark 3.5 default), `to_timestamp` returns NULL for an empty, malformed, unsupported-format or invalid-calendar value, and for a NULL.
3. **Cutoff:** `up_cut` (:47-49).
4. **Candidacy:** `up_cand` (:59-67), a key with any row loaded since the range start. The untimed new row qualifies its key here.
5. **Version ordering**, **where the record disappears:** `up_ranked` takes `MAX(record_ts) OVER (PARTITION BY k)`, which ignores NULLs (:70). `up_top` then keeps rows `WHERE record_ts = top_ts OR top_ts IS NULL` (:75), so an untimed row survives only when every row of the key is untimed.
6. **Dedup and version:** `up_keys` (:77-84). `up_load_ts` = MAX(load) over `up_top`, which is the old version's load time.
7. **Due assignment:** `due_keys` (:86-100), on `up_load_ts`, the old load. So the key is not due, not pending (:301-303) and not untimed (:304-309; its `up_load_ts` is not NULL).
8. **SLA and ledger:** never reached for this key.
9. **PASS/FAIL:** `judge` (`ledger.py:415-462`) sees a smaller population. The balance check (`ledger.py:408-410`) cannot notice, because `due` and the buckets come from the same rows.

`no record time` (:207) fires only when `up_record_ts IS NULL`, i.e. every row is untimed.

**The same defect elsewhere:**
- **downstream:** `dn_ranked`/`dn_top` (:119-126), `dn_untimed` over `dn_top` (:129);
- **release 1:** `hop_common.sql.j2:82-96` (`up_top`, `dn_top`).

**Reproduced:** l01 (ledger), m04 (HOP_KEY_CURRENCY, full sweep).

## F-07: Conflicting tied records classified as excluded or deleted

**How equal timestamps are ordered.** They are not ordered. Every row at the top record time is kept (`up_top`, :74-76).

**Rule aggregation** (`up_keys`, :79-84):

```sql
MAX(CASE WHEN tomb THEN 1 ELSE 0 END) AS tomb
, MAX(CASE WHEN x_i THEN 1 ELSE 0 END) AS x_i
```

Rules are evaluated per row before version selection, then **OR-ed** across the tied rows. If one tied row matches, the whole key is excluded or deleted (:250-256). No conflict is detected.

**Rejects** (:184-189): `max_by(named_struct(reason, rule), rejected_ts)` is not deterministic when two rejects of the version share `rejected_ts`. Spark returns either, so PASS or FAIL can flip between runs (S-08).

**Values** (:133-142): compared as sets. That is fail-closed and approved (spec section 6 "Ties"; the existing test above). It is not defective.

**Authoritative tie-breaking already exists in configuration but not in the ledger.**
- `Dataset.winner_rule` (`config/models.py:452-459, 704`; validated `config/validate.py:303-321`) is used only by the cause engine (`causes/merge.py:149-163`).
- Spec section 6 and replay R06 (`tests/replay/test_r04_to_r07_hops.py:138-153`) define a rule-resolved tie with disagreeing values as a **FAIL**, explained by the cause `TIE_RESOLVED_BY_RULE`.

**Reproduced:** l03 (exclusion), l04 (tombstone).

## How the three interact (they fail in sequence)

1. Identity is computed first. A collision (F-02) can make two records' rows look like **tied versions of one key**. It can then feed F-07 (one collided row matching an exclusion excludes both) or F-05 (one collided row with a NULL record time is dropped).
2. Version ordering (F-05) runs before rule aggregation (F-07). A tie that includes an untimed row is today decided by the timed rows only.
3. All three end in the same blind spot: the balance check cannot see a record lost before `due_keys`.
