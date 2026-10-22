# 02 · Canonical key specification (encoding 2, revision 2)

This replaces the original design's G-1 (`to_json(array(...))`). The reasons are R2-01, R2-02 and R2-03 (01).
- **Status.** Proposed; nothing is implemented.
- **Evidence.** Each rule cites the prototype that verifies it. Rules marked *proposed, not prototyped* have no executed evidence yet, and become implementation tests (09).

## 1. Requirements

| # | Requirement | Why |
| --- | --- | --- |
| K-R1 | **Injective** over every value the supported part types can hold, including invalid UTF-8 bytes in a STRING. | F-02 is a silent merge; any residual collision is the same defect. |
| K-R2 | Built in **Spark SQL only** (it runs inside the ledger, release 1 checks, chain, contribution, probes and trace queries). | CLAUDE.md: check logic is SQL in templates. |
| K-R3 | **Byte-wise** equality semantics in joins, `GROUP BY`, `IN` and `collect_set`, also after an ORC round trip (`key_value` is read back). | Open-key and chain carry read stored keys. |
| K-R4 | **Independent of session settings** (time zone, ANSI mode, locale). | R2-05 (k4). |
| K-R5 | A **hash input that cannot lose bytes** on the way to the Python HMAC. | R2-03. |
| K-R6 | **Cross-layer** comparison stays by normalised value, as today (`INT 1` upstream matches `STRING '1'` downstream). | Existing approved behaviour (original V3). |
| K-R7 | **Fails closed** on part types that cannot meet K-R1, before any SQL runs. | Constraint 4: a configuration problem is DID_NOT_RUN with a reason, not a wrong answer. |

## 2. The encoding: length-prefixed bytes (LPB)

```
key   := part*                                 (parts in the downstream key order; upstream parts via key_map)
part  := "~;"                                  a NULL part (raw NULL)
       | <octet_length(s)> ":" s ";"           a value s = canonical_text(normalised part)
       | "!" <octet_length(r)> ":" r ";"       an UNPARSEABLE part: raw r non-NULL, normalised NULL
```

- **`octet_length`** counts bytes, not characters, so a decoder (and a proof of injectivity) never depends on how a byte sequence decodes. A part is self-delimiting: read digits up to `:`, then exactly that many bytes, then `;`.
- **Reserved first characters.** `~` (NULL) and `!` (unparseable) cannot start a value part, whose first character is a digit. So the three forms cannot be confused, and a NULL can never equal the text `'<null>'` or `'~;'`.
- **The unparseable form** (fixes R2-04). It keeps the raw bytes of the part whose normaliser returned NULL. So (A, `'2026-13-01'`) and (A, NULL) are **different** keys: the genuine record is not collateral of the malformed one. The key carrying `!` is `unassessable 'unparseable key'` (03, ledger order step 2).

**SQL** (verified as `lpb_sql` in `test_canonical_key_lp.py`; the `!` form as `EFF_PART` in `proto_ledger.py`):

```sql
-- per part; n = the normalised expression, r = the raw column (only when a normaliser is configured)
CASE WHEN r IS NOT NULL AND n IS NULL THEN concat('!', octet_length(r), ':', r, ';')
     WHEN n IS NULL                    THEN '~;'
     ELSE concat(octet_length(n), ':', n, ';') END
-- the key: concat(part_1, part_2, ...)
```

For a part with no normaliser, `r` and `n` are the same column, and the first branch can never fire. The renderer emits only the last two branches.

### 2.1 Verified properties

| Property | Result | Prototype |
| --- | --- | --- |
| Python mirror exact, and injective over 10,000 seeded 3-part tuples (separators, quotes, escapes, every C0 control, DEL, U+2028/2029, combining marks, an astral emoji, `'<null>'`, empty strings, NULLs) | Exact on every tuple; distinct tuples give distinct keys | `test_l1` |
| Split pairs `(a|b, c)` / `(a, b|c)`, `(NULL, c)` / `('<null>', c)`, `('', c)` / `(NULL, c)`, `(a;, c)` / `(a, ;c)` | 4,000 pairs, no collision (v1 collides on all of the first two kinds: `test_k3`) | `test_l2` |
| Invalid UTF-8: `X'FF'` against `X'FE'` | LPB distinct, `hex = 313AFF3B` (raw byte kept); JSON collides | `test_l3` |
| Stored and read back (ORC, Hive table) then compared | Byte-equal match | `test_l5` |
| An unparseable part never merges with a NULL part | Two keys, judged independently | `test_g01`, `test_g01b` |
| Cost: `COUNT(DISTINCT key)` over 1,000,000 four-part keys, local, 2 cores | Run 1: v1 1.81 s, JSON 2.22 s, LPB 1.78 s. Run 2: v1 1.61 s, JSON 1.66 s, LPB 1.74 s. Average width v1 21.9 bytes, JSON and LPB 31.9 bytes. **Time differences are within run-to-run noise; the cost of LPB is about 10 bytes more per key** (stored in `key_value`, shuffled in joins) | `test_l6` (indicative only; not a cluster measurement) |

## 3. Canonical text per part type (`canonical_text`)

The renderer takes the **normalised** part (after `key_normalise`, as today) and its Spark data type, resolved from the table schema at evaluation time.

| Spark type of the normalised part | Canonical text | Status |
| --- | --- | --- |
| STRING, CHAR, VARCHAR | the value as stored (bytes unchanged) | Verified (l1–l5) |
| TINYINT, SMALLINT, INT, BIGINT | `CAST(n AS STRING)` (no leading zeros; `-` sign) | Verified that the types render alike (`test_k5`: INT 42 = BIGINT 42) |
| DECIMAL(p, s) | `CAST(n AS STRING)`, which renders with scale `s` | Verified behaviour (`test_k5`: `1.00` against `1`). **Unchanged from today:** different scales on the two sides are a false FAIL, never a false PASS; a normaliser must be declared. Disclosed limitation |
| DATE | `CAST(n AS STRING)` (`yyyy-MM-dd`; independent of the session time zone) | Read (Spark semantics); covered by the model's date part |
| TIMESTAMP | `date_format(n, 'yyyy-MM-dd HH:mm:ss.SSSSSS')`, with the evaluation's session time zone **required to be UTC** (guard below) | *Proposed, not prototyped* as written. k4 verifies the hazard, and k8 that `get_spark` sets UTC today |
| BOOLEAN | `CAST(n AS STRING)` (`true` / `false`) | Read |
| FLOAT, DOUBLE | **refused** | k5: inexact (`0.30000000000000004`), `NaN`, signed zero. Equal business keys can render differently, and two values can be equal under one rendering and different under another |
| BINARY | **refused** | Under LPB the raw bytes would be safe (l3). But no Python mirror of a binary part is specified, and keys of this type are not expected. Refusal is the cheaper fail-closed choice (decision R2-D4) |
| ARRAY, MAP, STRUCT | **refused** | k6: `CAST(array('a, b'))` = `CAST(array('a','b'))` |

- **Refusal is a configuration outcome.** Every check that builds a key from that dataset returns DID_NOT_RUN `invalid_config`, naming the dataset, column and type, and saying how to fix it: declare a normaliser that yields a supported type.
  - *Where:* the type is known only from the table schema, so the check is made at evaluation time, before the SQL is rendered. *Proposed, not prototyped.*
  - `dre validate` cannot see schemas (static validation), so it cannot catch this earlier.
- **Session time-zone guard.** Before any key-building evaluation the runner asserts `spark.sql.session.timeZone == 'UTC'`. A different value makes the evaluation DID_NOT_RUN `query_failed` (PLATFORM), with the observed zone in the reason detail. This turns k8's "true by convention" into an enforced precondition. *Proposed.*
- **Unicode.** Values are compared as stored, with no NFC/NFD normalisation (k7). Two layers storing different forms is a false FAIL, never a false PASS. Disclosed; a normaliser would be a generic capability, out of Gate A.

## 4. Version and hash

- **`KEY_ENCODING = 2`** in `checks/keys.py`. Every `dq_key_event` row written by N+1 carries `key_encoding = 2` (migration 2, 06).
- **`key_hash`.** The HMAC must see the key's bytes, not a decoded string (R2-03). Two verified options:
  - **(a) Recommended: `dre_key_hash(CAST(k AS BINARY))`**, with the UDF hashing the raw bytes. For any valid UTF-8 key this equals `HMAC(secret, k)`, so spec section 5's definition ("HMAC-SHA256 of the canonical key string") holds word for word. Verified: `test_l8` (added in this revision; see 07).
  - **(b) `dre_key_hash(hex(k))`.** ASCII input, verified collision-free (`test_l4`). It changes the hash definition to "HMAC of the hex of the key", which is a spec-wording change.
- **One call site.** The hash call is emitted in one place: `checks/hop/common.py` gains a `hashed(expr)` helper returning `"{HASH_FUNCTION}(CAST({expr} AS BINARY))"`, used by the templates through the `hash_fn` parameter. That parameter is today passed at `common.py:109`, `chain.py:455` and `probes.py:218`, and the templates call `{{ hash_fn }}(k)`. The secret handling (`register_key_hash`) is unchanged.

## 5. Cross-language behaviour

- **Spark is the authority.** No Python code produces a key that is compared with a Spark-produced key in production.
  - `trace.py:115-123` already computes the hash in SQL (`SELECT dre_key_hash(key_string(parts))`). That stays: trace builds a one-row view and runs the same SQL renderer.
- **The Python mirror** (`lpb_py` in the prototypes) is **test-only**. Tests use it to state expected keys; it is exact for STRING parts (l1).
- **No JSON escaping** is involved, so R2-01's class of mismatch (escape case) cannot arise.

## 6. Compatibility

- **Encoding 1 stays computable.** `key_string_v1` keeps today's body (`keys.py:119-122`) unchanged. It is used only:
  - by the N+1 transition remap (06 §3), to find the encoding-2 successors of a legacy open key;
  - by trace, to look up legacy events, labelled `encoding 1`.
- **No other reader may use it.**
- **Every key-building call site switches to encoding 2 in the same release.** The sites, read:
  - `common.py:96, 100`;
  - `ledger.py:291, 294, 348, 366`;
  - `chain.py:196-197`;
  - `contribution.py:86, 89`;
  - `probes.py:204`;
  - `merge.py:137-138`;
  - `trace.py:123`.

  A partial switch would compare encoding-1 keys with encoding-2 keys and match nothing. A guard test enumerates `key_string(` call sites (09).
- **The raw-variant guard** (`up_raw`, `dn_raw`, `ledger.py:291, 294`) uses the same LPB encoding over the raw columns. A lossy normaliser that merges two raw keys stays `unassessable 'key collision'` (verified at model level: `raw_variants` in `proto_ledger.py`; `test_g01`).
