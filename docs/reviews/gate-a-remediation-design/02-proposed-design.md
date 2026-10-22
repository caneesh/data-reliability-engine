# 02 · Proposed technical design

**Scope.** F-02, F-05 and F-07 only. The design keeps the architecture: a read-only observer, the existing commit protocol, the three result states, and the existing disposition vocabulary (`unassessable` with a label, HOP_KEY_CURRENCY's `UNASSESSABLE`). No pipeline, source or target changes.

**The guiding rule.** A record DRE cannot identify, order or classify without ambiguity is `unassessable`, which is a failing class. It is never dropped and never resolved arbitrarily.

## 0. Correctness invariants (shared by the three fixes)

| Invariant | Statement | How the design enforces it |
| --- | --- | --- |
| **A. Identity** | Distinct logical keys (distinct normalised part tuples) never share a reconciliation identity | An injective encoding (§1); unparseable parts are unassessable, never a value |
| **B. Population coverage** | Every upstream row loaded before the cutoff whose key is a candidate is accounted for in exactly one line: due (with a disposition), pending, or untimed | No row excluded from version selection without a disposition (§2); the due load over all rows of an untimed key |
| **C. Determinism** | Same input, configuration and evidence give the same result | No `max_by` on ties; total orders for rejects; set comparisons sorted (already so) (§3.3) |
| **D. Ambiguity** | An unresolved conflict never yields PASS | Tied rows that disagree on a rule are unassessable (§3); untimed rows make the version undeterminable (§2) |
| **E. Historical integrity** | Committed evidence is never rewritten or silently reinterpreted | A forward-only migration adds `dq_key_event.key_encoding`; legacy events are closed by new events, never edited (§4) |

**Evaluation order** in `unknown_reason`, from identity to version to rules (`hop_ledger.sql.j2` `judged`), each step a precondition of the next:

1. `key collision` (raw variants under the new encoding)
2. `unparseable key` (new)
3. `not one-to-one` (identity mapping, unchanged)
4. `no record time` / `unparseable record time` on **any** row of either side (changed)
5. `tied rows disagree: <rule>` (new)
6. `no load time` (unchanged)
7. reject attribution labels (unchanged)

## 1. F-02: Injective record identity

### 1.1 The alternatives

| Option | Collision resistance | NULL handling | Type preservation | Spark compatibility | Persisted-evidence impact | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| Native struct / array keys (`named_struct(...)` as `k`) | Exact | Exact (NULL fields) | Kept, which would **break** today's intended cross-layer string comparison (INT here, STRING there) | Joins, grouping and windows work on structs. But `k` is a STRING throughout: temp views, `key_value STRING`, the HMAC UDF, `collect_set`, `IN (SELECT k ...)` | Every template plus a column retype in `dq_key_event` (not allowed: migrations only ADD) | Rejected: too broad |
| Length-prefixed parts (`len:value`, NULL as `~`) | Exact (a prefix code) | Exact | Strings, as today | Expressible in SQL (`concat`, `length`) | New format; readable only with care | Acceptable, second choice |
| **Canonical JSON array** of the normalised parts as strings: `to_json(array(CAST(p1 AS STRING), …))` | Exact: JSON escaping is injective for string arrays; quotes, backslashes and controls are escaped | Exact: JSON `null` ≠ `"<null>"` | Strings, as today (deliberately: the cross-layer comparison is by normalised string) | Built-in `to_json` (Spark ≥ 2.4); no UDF; deterministic | New format, readable, with a standard parser | **Recommended** |
| A hash of a canonical form | Only as good as the canonical form under it; hashing an ambiguous string preserves the ambiguity | Inherits | Inherits | — | — | Not a fix by itself. The existing HMAC (`key_hash`) stays **on top of** the canonical form |

**Verified in Spark 3.5.1** (`verification/test_design_prototypes.py`, 7 passed):
- v1 collides on every examination case, and v2 on none (V1);
- a Python mirror `json.dumps(values, ensure_ascii=False, separators=(",", ":"))` equals Spark's output for quotes, backslashes, tabs, newlines, Unicode, `|`, `<null>`, empty strings and NULLs (V2);
- INT against STRING parts still match (V3).

### 1.2 The specification

```python
# checks/keys.py
KEY_ENCODING = 2

def key_string(exprs: list[str]) -> str:
    """One canonical, injective string for a (normalised) key: a JSON array of the parts as strings;
    null is JSON null. (Encoding 2; encoding 1 joined parts with '|' and wrote null as '<null>'.)"""
    return f"to_json(array({', '.join(f'CAST({e} AS STRING)' for e in exprs)}))"

def key_string_v1(exprs: list[str]) -> str:      # kept for trace lookups of legacy events only
    ...  # the current body

def canonical_key(values: list[str | None]) -> str:  # the Python mirror: tests, trace
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))

def key_parsed(dataset, columns) -> str:
    """TRUE when no part is NULL after normalisation unless it was NULL before: a normaliser that
    turns a value into NULL (parse_date on a non-date) leaves the key unparseable."""
    terms = [f"({c} IS NULL OR {column_expr(dataset, c)} IS NOT NULL)"
             for c in columns if c in dataset.key_normalise]
    return " AND ".join(terms) or "TRUE"
```

**Unparseable key parts** (the model's "unparseable key", section 3 row 1):
- **Ledger.**
  - Each `up_rows` and `dn_rows` row gets `parsed = key_parsed(...)`.
  - An upstream key with any unparsed row is unassessable `unparseable key`.
  - Downstream rows that are unparsed are removed from `dn_cand`, so they can never satisfy a due key.
  - When orphans are checked, they are counted as `orphan 'unparseable key'`. Otherwise they are counted in the scope (`unparseable_downstream_rows`).
- **Release 1 checks** (`hop_common.sql.j2`): the same flags, with state `UNASSESSABLE` and label `unparseable key`.

**Raw-variant guard.**
- The ledger's existing guard (`raw_variants`, :71, :121) becomes correct automatically, since `up_raw`/`dn_raw` are built with the new encoding.
- The same guard is added to `hop_common.sql.j2` (`up_keys`/`dn_keys`: `size(collect_set(raw_k))`), giving `UNASSESSABLE 'key collision'`, so lossy normalisers (`strip_leading_zeros`, `upper`, `trim`) can no longer merge two raw keys silently in the release 1 checks.

**Hashes.**
- `key_hash = HMAC(secret, encoding-2 string)`. Every key's hash changes once (migration, §4).
- The HMAC design, secret handling and `dre_key_hash` are unchanged.

## 2. F-05: No version disappears because its record time is unusable

### 2.1 The rule

The version under judgement is determinable only when **every** row of the key in `up_cut` (upstream; `dn_cand` downstream) has a usable record time.
- One unusable row makes the key `unassessable`, with one of two labels:
  - `unparseable record time (n of m rows)` when the raw value was non-NULL;
  - `no record time (n of m rows)` when it was NULL.
- The downstream side has the same rule with `… here`.

This is **record-level** invalidity: the key fails, and the evaluation proceeds. DID_NOT_RUN stays reserved for an evaluation that cannot proceed at all (missing table, failed SQL, empty population). An unusable record time on a single key must not suppress the judgement of every other key.

### 2.2 The due load

For a key with an unusable row, the due load is `MAX(load_ts)` over **all** its rows in `up_cut`, not over the top version. So the window in which an untimed row arrives judges the key, and an untimed row that arrives later makes the key due again. That is new evidence: it is counted in that window, and the earlier window's judgement stands. For a fully timed key, the due load is unchanged (the top version's load), so nothing is judged twice.

### 2.3 The SQL change (`hop_ledger.sql.j2`; mirrored in `hop_common.sql.j2`)

```sql
up_ranked AS (
  SELECT u.*, MAX(record_ts) OVER w AS top_ts,
         size(collect_set(raw_k) OVER w) AS raw_variants,
         COUNT(*) OVER w - COUNT(record_ts) OVER w AS untimed_rows,                 -- new
         SUM(CASE WHEN record_ts IS NULL AND raw_record IS NOT NULL THEN 1 ELSE 0 END) OVER w AS unparsed_rows,  -- new
         COUNT(*) OVER w AS key_rows,                                               -- new
         MAX(load_ts) OVER w AS latest_load,                                        -- new
         MIN(CASE WHEN parsed THEN 1 ELSE 0 END) OVER w AS key_parsed               -- F-02, new
  FROM up_cand u WINDOW w AS (PARTITION BY k)
),
up_top AS (SELECT * FROM up_ranked WHERE record_ts = top_ts OR top_ts IS NULL),     -- unchanged
up_keys AS (
  SELECT k, MAX(record_ts) AS up_record_ts,
         CASE WHEN MAX(untimed_rows) > 0 THEN MAX(latest_load) ELSE MAX(load_ts) END AS up_load_ts,  -- changed
         MAX(untimed_rows) AS untimed_rows, MAX(unparsed_rows) AS unparsed_rows, MAX(key_rows) AS key_rows,
         MIN(key_parsed) AS key_parsed, ...
  FROM up_top GROUP BY k
)
-- judged.unknown_reason (in the order of §0):
--   WHEN d.key_parsed = 0 THEN 'unparseable key'
--   WHEN d.unparsed_rows > 0 THEN concat('unparseable record time (', d.unparsed_rows, ' of ', d.key_rows, ' rows)')
--   WHEN d.untimed_rows  > 0 THEN concat('no record time (', d.untimed_rows, ' of ', d.key_rows, ' rows)')
--   (and the same for n.* downstream, '... here')
```

- `raw_record` is the raw record-time column, selected unparsed next to `record_ts`, so that "unparseable" and "absent" can be told apart.
- Every new aggregate uses the **same window** (`PARTITION BY k`) as the existing `MAX`/`collect_set`, so Spark computes them in one Window operator: no extra shuffle (V4 proves the logic).

**Labels are bounded.** The `(n of m rows)` counts go in the label. Spark groups the summary by label, so the label set grows with distinct `(n, m)` pairs. To keep `dq_reconciliation` small, the counts can instead go into the scope, with the label kept as just `unparseable record time` / `no record time`. That is decision G-4 (07).

### 2.4 The edge cases

| Case | Outcome | Notes |
| --- | --- | --- |
| NULL record time | `no record time (n of m rows)` | — |
| Empty string, malformed, unsupported format, invalid calendar date | `unparseable record time (n of m rows)` | `to_timestamp` gives NULL (ANSI off); the raw value is non-NULL |
| Timezone ambiguity (DST fall-back) | Unchanged (a value is produced); disclosed limitation (spec section 4) | Out of F-05 |
| Mixed valid and invalid versions | Unassessable whatever the valid rows say | The examination's l01 / m04 |
| Missing record time, valid load time | Unassessable; due in the window of its latest load | §2.2 |
| Valid record time, missing load time | Unchanged: `untimed` line on normal runs, `no load time` on the full sweep | F-12 is separate (release 1 checks) |
| Backfilled records (valid older record time, loaded now) | Unchanged: not the top version, so the key is not re-judged | Current-state semantics |
| Rows crossing window boundaries | An untimed row loaded after the cutoff is not in `up_cut`; it is judged in its own window | — |

### 2.5 Fallback timestamps

**Not allowed by default.**
- A load time cannot stand in for a record time. Out-of-order CDC (a16) is exactly why the version is the record time, and CLAUDE.md forbids comparing record time with load time.
- The only defensible fallback is an **opt-in contract declaration** that the dataset's load order is its version order (an append-only stream applied in commit order). Then an untimed row loaded **no later than** the judged version's load time can be ignored as older. That compares load with load, never record with load.
- DRE cannot verify such a declaration, so a PASS resting on it must carry a qualification (`version_by_load_order`) and is never healthy.
- **Deferred.** It is not part of Gate A; it is question Q2 in 07.

## 3. F-07: Tied rows must agree on a rule; the reject choice is total

### 3.1 Rules over tied rows

```sql
up_keys AS (
  SELECT k, ...,
         MIN(CASE WHEN tomb THEN 1 ELSE 0 END) AS tomb_all, MAX(CASE WHEN tomb THEN 1 ELSE 0 END) AS tomb_any,
         -- per exclusion i:
         MIN(CASE WHEN x_i THEN 1 ELSE 0 END) AS x_i_all, MAX(CASE WHEN x_i THEN 1 ELSE 0 END) AS x_i_any,
         -- every tied row excluded by some exclusion (row-level OR, then key-level AND):
         MIN(CASE WHEN x_1 OR x_2 OR ... THEN 1 ELSE 0 END) AS excluded_all
  FROM up_top GROUP BY k
)
-- judged:
--   tomb       := tomb_all = 1
--   excluded_by := the first i with x_i_all = 1;
--                  else, when excluded_all = 1, a combined label of the rules that match ('a@1+b@2');
--                  else NULL
--   unknown_reason (step 5 of section 0):
--     WHEN tomb_any = 1 AND tomb_all = 0 THEN 'tied rows disagree: deletion'
--     WHEN <any x_i_any = 1> AND excluded_all = 0 THEN 'tied rows disagree: exclusion <label>'
```

**Volume guards** count a combined label against each rule in it, which is conservative (V5 proves the logic).

### 3.2 What stays as it is

- **Values over tied rows:** compared as sets. This is approved (spec section 6, `test_value_agreement_compares_the_tied_latest_rows_as_sets`), and it fails closed when the sets differ.
- **Identity mapping:** tied rows are `not one-to-one`.
- **Survivorship between upstreams:** precedence, unchanged.

### 3.3 Rejects: a total, fail-closed order

```sql
rejected AS (
  SELECT d.k, max_by(named_struct('reason', r.reason, 'rule', r.rule),
                     named_struct('t', r.rejected_ts,
                                  'defect', CASE WHEN <reason, rule authorised> THEN 0 ELSE 1 END,
                                  'reason', r.reason, 'rule', coalesce(r.rule, ''))) AS latest
  ...
)
```

The latest reject wins. On equal time, a **defect** reason beats an authorised one (fail closed), then the reason and the rule break the tie lexically. The order is total, so the result is deterministic (V6).

The authorisation test is the same one already rendered for `variant`, by moving that CASE into a macro.

### 3.4 Authoritative tie-breaking (`winner_rule`)

**Not adopted in Gate A.**
- `winner_rule` exists and is validated. But the approved semantics (spec section 6; replay R06) make a rule-resolved tie with disagreeing values a FAIL, explained by `TIE_RESOLVED_BY_RULE`.
- Using it to resolve ties in the ledger would turn approved FAILs into PASSes. That is a contract decision (Q1 in 07), not a correction.

**If it is adopted later:** an opt-in `winner_rule.resolves_ties: true`.
- Ties resolve only when every `order_by` term is non-NULL on the tied rows and the first differing term is distinct.
- Otherwise the result is `unassessable 'ambiguous version'`.
- HOP_VALUE_AGREEMENT changes in step, so the two checks never contradict each other (L14).

### 3.5 The scenarios

| Scenario | Outcome | Resolvable? |
| --- | --- | --- |
| Same timestamp, identical records | One record; rules agree trivially | Yes |
| Same timestamp, different business values | Value sets compared (unchanged): delivered when the target holds the same set, else divergent | Yes (approved rule) |
| Same timestamp, exclusion vs live | `unassessable 'tied rows disagree: exclusion …'` | **No:** inconclusive |
| Same timestamp, delete vs update | `unassessable 'tied rows disagree: deletion'` | **No** |
| Same timestamp, two different deletes (both tombstones) | `deleted` / `deleted_not_applied` (unanimous) | Yes |
| Same timestamp, two conflicting rejects | Defect-first total order; deterministic | Yes (fail closed) |
| Same timestamp, different source priorities (two upstreams) | Survivorship precedence (unchanged) | Yes, by contract |
| Same timestamp, valid sequence numbers | Gate A: as "different values" or rule disagreement above. With Q1 adopted: resolved by `winner_rule` | Only with Q1 |
| Same timestamp, missing sequence numbers | Gate A: as above. With Q1: `ambiguous version` | No |

## 4. Persisted identity: key encoding version and transition

**Problem.** `dq_key_event.key_value` and `key_hash` (encoding 1) are read back as live identity:
- open keys: `hop_common.sql.j2:39-50`, joined on `key_hash` (:111);
- chain carry: `chain.py:338-345`.

After the switch to encoding 2:
- legacy open keys would no longer match;
- on the next full sweep, HOP_KEY_CURRENCY's `GONE` branch (`hop_key_currency.sql.j2:26-32`) would CLEAR them as "no longer upstream". That is false: the record may be both upstream and still wrong;
- legacy PENDING chain records would no longer be carried.

**Design.** Forward-only and self-healing. No stored row is changed.

1. **Migration 2** (`store/migrations.py`, `store/ddl.sql`): `dq_key_event ADD COLUMNS (key_encoding INT)`.
   - Existing rows read NULL, which means encoding 1.
   - `append_key_events` writes `KEY_ENCODING`.
2. **Readers** of open keys (`hop_common.sql.j2`) and PENDING keys (`chain.py`) read only `coalesce(key_encoding, 1) = 2` events.
3. **Transition.** Per hop or chain, the runner detects legacy state: the latest event per legacy hash is still open (FLAGGED or STILL_FLAGGED) or PENDING. It is one query per run, over the same partitions the reads already use.
   - For each hop or chain with legacy state, the evaluation runs as a **forced full sweep**, through the path that compromised baselines already take (`runner.py`: `force_full_sweep=`).
   - The sweep judges every upstream key under encoding 2, which re-flags whatever is still wrong.
   - It writes a **closing event** for every legacy open or pending hash: `CLEARED` (HOP_KEY_CURRENCY) or `RESOLVED` (chain), with `state_detail 'key encoding changed'` and `key_encoding = 1`.
   - On the next run nothing legacy is open, so no further sweep is forced. No bookkeeping table is needed.
4. **`v_open_keys`** partitions by `key_hash`, so legacy hashes close through their closing events, and encoding-2 hashes open afresh. No view change is needed. Optionally, `key_encoding` can be added to its projection.
5. **Trace** (`trace.py:115-123`) computes both hashes (`key_string` and `key_string_v1`) and shows events of each encoding, labelled.

**Rejected alternative: a bridge.** Map legacy `key_value` to the new identity by recomputing encoding 1 for the current rows. It keeps open-since continuity without a sweep, but it needs encoding-1 computation inside every evaluation, and an encoding-1 key is ambiguous by definition (F-02), so the bridge would have to fan out conservatively. Its complexity is not worth one sweep per hop.

## 5. Interactions

| Interaction | Before | After |
| --- | --- | --- |
| A key collision hides an untimed row | Collided rows merge, so one record's NULL row is ignored | Collisions are unassessable first (§0 step 1); separate keys after encoding 2 |
| An untimed row inside a tie | Ignored; the tie is decided by the timed rows | `no record time` precedes the tie checks (§0 step 4 before 5) |
| A conflicting tie satisfies a deletion | Yes (MAX) | `tied rows disagree: deletion` |
| A replay after the encoding change differs | n/a | Results differ only where a defect was hidden. New results carry new evaluation ids (they include `engine_version`), and old results stay as stored (E) |
| An old evidence record attached to a new key | Possible through a collided legacy `key_value` | Readers ignore legacy encodings except for closing; the closing events name the legacy hash only |
| Two executions classify differently | `max_by` reject tie | A total order (C); determinism tests (04) |
