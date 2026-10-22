# 03 · Compatibility contract (Gate A)

This is the normative contract the implementation must satisfy. Every clause has an ID that the acceptance criteria (04, 05) test.
- **Proposed** until the amendments of 02 are approved.
- Where a clause rests on evidence, the evidence is cited. Where it does not yet, it says so.

## 1. Canonical key bytes (encoding 2)

| ID | Clause |
| --- | --- |
| K-1 | **Parts and order.** A key is a sequence of parts in the **downstream** dataset's key order. Upstream parts come through `key_map`, as today (`common.py:96, 100`). Each part is the column after its `key_normalise`, if one is configured. |
| K-2 | **Framing** (bytes, shown as ASCII):<br>• **NULL:** `0x7E 0x3B` (`~;`).<br>• **Value:** `<n> 0x3A <bytes> 0x3B`, where `<n>` is the byte length of `<bytes>` in ASCII decimal digits with no sign and no leading zeros (`0` for the empty string).<br>• **Unparseable:** `0x21 <n> 0x3A <raw bytes> 0x3B`.<br>The key is the concatenation of its parts, with nothing between parts and no prefix or suffix. |
| K-3 | **Unambiguous by construction.** A value part starts with a digit, a NULL part with `~`, an unparseable part with `!`, and every part's length is explicit. So decoding is unique and distinct part sequences give distinct keys. Verified on 10,000 generated tuples, 4,000 split, NULL and empty pairs, and invalid UTF-8 (rev-2 l1, l2, l3). |
| K-4 | **Malformed values.** A part is unparseable when its raw value is not NULL and its normaliser returned NULL. Its raw bytes are kept, so (A, NULL) and (A, `2026-13-01`) are different keys (rev-2 g01b). A key with an unparseable part is `unassessable 'unparseable key'` on that side. |
| K-5 | **"No key"** is decided on the raw columns: a row whose every key column is NULL as read is not a key (`has_a_key`, `common.py:171-174`, unchanged). It is not encoded. |
| K-6 | **Canonical text by type** (the type after normalisation, resolved from the table schema at evaluation time):<br>• STRING, VARCHAR, CHAR: bytes as read;<br>• TINYINT to BIGINT: ASCII decimal, `-` sign;<br>• DECIMAL(p, s): Spark's `CAST(n AS STRING)` (Java `BigDecimal.toString`): exactly s fractional digits, **scientific notation below 10^-6** (`1.00E-8`); unchanged from encoding 1;<br>• DATE: `yyyy-MM-dd`;<br>• TIMESTAMP: `yyyy-MM-dd HH:mm:ss.SSSSSS` in UTC;<br>• BOOLEAN: `true` / `false`.<br>*Verified:* STRING (l1–l5), integral (k5), DECIMAL `1.00` / `1` (k5), and DECIMAL `-0.50`, `1.00E-8`, `12345678901234567890.12`, `0.000` (`verification/check_char_and_decimal.py`). Within one DECIMAL type the rendering is injective (one string per unscaled value and scale).<br>*Not yet verified:* the TIMESTAMP pattern (T-F02-9); DATE beyond year 9999. |
| K-7 | **Refused types.** FLOAT, DOUBLE, BINARY, ARRAY, MAP, STRUCT, INTERVAL, TIMESTAMP_NTZ, VOID give DID_NOT_RUN / invalid_config, naming the dataset, column and type, with the fix (declare a normaliser that yields a supported type). Refusal happens before any SQL is rendered for the hop. |
| K-8 | **Normalisation.** Only declared normalisers apply. No Unicode normalisation (NFC and NFD stay distinct: rev-2 k7), no implicit trim, no case folding. Two layers storing different forms is a false FAIL, never a false PASS. |
| K-9 | **Session independence.** Rendering must not depend on session settings. `dre run` refuses to start (exit 3) unless the session time zone is UTC after `get_spark` (A-11). |
| K-10 | **Hash.** `key_hash = lower-hex(HMAC-SHA256(secret, key bytes))`. SQL passes `CAST(key AS BINARY)`. The UDF accepts BINARY (raw bytes) or STRING (UTF-8 bytes), with an identical result for valid UTF-8 (rev-2 l8; `test_sql_key_hash_matches_python_hmac` unchanged). Algorithm, secret source and closure registration are unchanged (U-3, ADR-08). |
| K-11 | **Encoding version.** It is out of band: `dq_key_event.key_encoding` (2 for this encoding; NULL means 1). The key bytes carry no version marker. |
| K-12 | **Encoding 1.** It stays computable as `key_string_v1`, unchanged, and is called only by the transition and trace (guard A-14). |
| K-13 | **Cross-layer matching** is by canonical text (an INT `1` matches a STRING `1`), as today. Differing DECIMAL types (scale, or scientific against plain text in another layer) or CHAR padding across layers (CHAR(3) `'a'` reads as `'a  '`, verified) can make equal business keys differ: a false FAIL, never a false PASS; disclosed (Q-2, Q-3). |

## 2. Temporal eligibility and bound D

| ID | Clause |
| --- | --- |
| T-1 | **Rows considered:** the key's rows loaded before the cutoff (or with no load time), on each side, after `feed_filter`. Unchanged. |
| T-2 | **record_ts** is the record time parsed with its configured format and zone to UTC (`utc_expr`, unchanged). A row is **untimed** when record_ts is NULL. It is **unparseable** when the raw value is also non-NULL. |
| T-3 | **Version** = the timed rows at `MAX(record_ts)`. **v_load** = `MAX(load_ts)` over the version's rows. Unchanged. |
| T-4 | **D** is the dataset's `record_time.disorder_bound.hours`, an integer ≥ 1, or NULL. Its meaning, as declared by the owner: no version of a key is loaded more than D hours before an older version of that key. D says nothing about record times and never chooses a version. |
| T-5 | **Relevance.** An untimed row u is relevant **unless all** of these hold: D is not NULL, the evaluation did not refute D (T-8), the key has a timed row, u.load_ts and v_load are not NULL, and `u.load_ts < v_load − D hours`. The boundary `u.load_ts = v_load − D` is relevant. |
| T-6 | **Outcome.** Any relevant untimed row makes the key unassessable: "unparseable record time" if any relevant untimed row is unparseable, else "no record time"; on the target side, the same labels ending "… here". Non-relevant rows are set aside and counted in the scope (`untimed rows set aside`). |
| T-7 | **Due load** = `greatest(v_load, MAX(load_ts of relevant untimed rows))`. Set-aside rows never change it. A fully timed key's due load is v_load, as today. |
| T-8 | **Refutation.** Over the evaluation's candidate keys, if any key has a timed row with `record_ts < top_ts` and `load_ts > v_load + D hours`, D is refuted for the evaluation: no row is set aside (T-5). The scope records `disorder_bound_refuted: n keys`, and the digest prints it. Prototype: rev-2 t1–t3. Whether a refutation persists to later evaluations is Q-4. |
| T-9 | **Qualification.** Any result whose judgement set aside at least one row carries `version_by_disorder_bound` (§3). |
| T-10 | **Determinism.** Results are independent of partitioning and read order (rev-2 g07). |

## 3. "Never healthy": the exact meaning

The engine has **one** definition of health: `v_latest_result.healthy = (state = 'PASSED' AND evidence_state = 'VALID' AND qualifications IS NULL)` (`ddl.sql:283`; spec §5 item 7). A result carrying `version_by_disorder_bound` is never healthy in every surface:

| Surface | Required behaviour | Mechanism (read) |
| --- | --- | --- |
| Stored result | `dq_check_result.qualifications` contains `version_by_disorder_bound`, whatever the state. The scope's `claim` reads `declared contract, qualified: version_by_disorder_bound` (or `inferred contract`). The scope gives the count of rows set aside | `qualifications_of` (`ledger.py:156-165`) and the release 1 checks' qualification path (as for `coverage_gap`) |
| `v_latest_result` | `healthy = false` | `ddl.sql:283`, unchanged |
| Digest and every sink | Listed under "Passed with qualifications (not counted as healthy)" with its qualification and scope line. Counted in the subject (`, n passed with qualifications`). **Never** in the healthy passing count. Its presence prevents "all clear" | `digest.py:106-123, 335-362`, generic over codes. Unchanged |
| Watchdog | Unaffected: it judges whether runs happened, not result health | spec §8 |
| CLI output (`dre run`) | Unaffected: it reports counts of checks written, not health | spec §8 |
| Dashboards and APIs | DRE ships none (no web server, CLAUDE.md). **Contract for any external consumer:** health is read only from `v_latest_result.healthy`. A consumer that derives health from `state = 'PASSED'` is non-conforming. This clause is to be added to the consumer documentation (`docs/architecture.md`) when A-7 is applied | documentation |
| Chain | CHAIN_COMPLETENESS carries every edge's qualifications, so a chain resting on a set-aside row is never healthy | spec §6 chain "Evidence" |

## 4. Ties and dispositions (deterministic)

| ID | Clause |
| --- | --- |
| D-1 | **Expected state per version row:**<br>• `present`: no tombstone, no matching approved exclusion;<br>• `absent`: a tombstone under `applied_as: hard`, or an approved exclusion matching the row, effective at that row's own load time;<br>• `soft_deleted`: a tombstone under `applied_as: soft`. |
| D-2 | More than one expected state among the version's rows gives `unassessable 'tied rows disagree'`. The states and rules involved go in the scope. |
| D-3 | All `absent`: with any tombstone, `deleted` or `deleted_not_applied`, with `excluded_by` also recorded; otherwise `excluded` or `excluded_but_present` under each matching rule. Each rule's volume guard counts the record. |
| D-4 | Value ties: the set rule, unchanged (spec line 607). R06 is unchanged. `winner_rule` orders nothing in a check. |
| D-5 | A reject naming the version, when the version's rows are not identical (value, tombstone or any exclusion flag differs), gives `unassessable 'reject ambiguous among tied rows'`. |
| D-6 | **Reject order:** `rejected_ts` desc, then defect before authorised, then reason (byte order, greatest first), then rule id (byte order, greatest first, NULL last). |
| D-7 | **The `unknown_reason` order**, first match wins:<br>1. key collision;<br>2. unparseable key;<br>3. not one-to-one;<br>4. unparseable record time / no record time;<br>5. … here;<br>6. no load time;<br>7. tied rows disagree;<br>8. reject ambiguous among tied rows;<br>9. reject not attributable to a version;<br>10. reject names no rule.<br>Then the matched dispositions in the existing precedence (spec line 613), then the residual. |
| D-8 | The identity (every due record in exactly one bucket) and `ledger_unbalanced` are unchanged. |

## 5. Downstream malformed keys

| ID | Clause |
| --- | --- |
| M-1 | A target row with an unparseable part gets the `!` form (K-4). It is removed from the target's candidate rows before matching, so it never satisfies a due key. |
| M-2 | It is counted on the breach line `unparseable target key`: rows loaded in the window, or every such row on the full sweep. The line is reported with the orphans when orphans are counted, otherwise in the scope and triage. Duplicates are counted per row. |
| M-3 | The upstream record whose target row is malformed is therefore `missing` (or `stale` / `divergent` against another, parseable target row): FAIL direction. |
| M-4 | The release 1 checks exclude such rows from the target's keys and count them in `observed`. CHAIN_COMPLETENESS treats the record as not reached at that edge (pending, then lost, by the existing rules). HOP_CONTRIBUTION counts the reference as unresolved. |
| M-5 | No path lets a malformed target key produce or strengthen a PASS. |

## 6. Key-event lineage and age (invariants)

| ID | Invariant |
| --- | --- |
| L-1 | Every key event names its encoding (`key_encoding`, or NULL meaning 1). |
| L-2 | `REKEYED` never ends an open spell; only `CLEARED` does. |
| L-3 | **Provable continuity** (U-4): a legacy `key_value` provably denotes one tuple only when it decodes to exactly one tuple given the parts' types. Typed parts render in a fixed pattern holding neither `|` nor `<null>`; string parts can hold both. Prototype: `verification/test_identity_continuity.py` (6 tests). Data present upstream today never proves continuity. |
| L-4 | **Age inheritance only on provable continuity.** The successor's first STILL_FLAGGED names `predecessor_hash`, and `v_open_keys.first_flagged_at` takes the predecessor's. |
| L-5 | **No silent loss of a discrepancy.** Without provable continuity, the legacy identity stays open (encoding 1, original first flagged date, `state_detail` "awaiting successors") while any successor is open. It is CLEARED "successors current" when none is, or "no longer upstream" when no successor exists. Successors open with their own age and name their predecessor. |
| L-6 | Every discrepancy open before an encoding switch (upgrade or rollback) is open after it, under some identity, with its age when continuity is provable (rev-2 m3, m5, m6; to be re-verified with the L-3/L-5 rule: T-MIG-5, T-MIG-9). |
| L-7 | Transition events are written in the evaluation's own commit. An interrupted transition is invisible; a rerun is idempotent (ADR-07; rev-2 m4). |
| L-8 | Stored rows are never rewritten (append-only; guard rules 2, 4 and 11). |

## 7. Rollback and mixed-version compatibility

| ID | Requirement |
| --- | --- |
| R-1 | Release N ships migration 2 and writes `key_encoding = 1`. Release N+1 adds no migration. So **N+1 → N needs no schema change**. |
| R-2 | **Release N reads encoding 1 only.** It takes REKEYED as no event. On finding encoding-2 open identities at a hop, it judges the hop as on the full sweep and writes REKEYED (encoding 2) for them (rev-2 m5). |
| R-3 | **The release N writer** tolerates a column missing from a frame only if `MIGRATIONS` added it, writing a typed NULL. A missing column of any other kind, or any extra column, raises (rev-2 m1, m2). This covers migration 1 too. |
| R-4 | **Below release N:** once migration 2 is applied, a pre-N engine cannot write key events. It **fails closed** (`evidence_incomplete`, the window is evaluated again). Rollback below N is unsupported. |
| R-5 | **One writer at a time.** Only NORMAL runs write key events, and `dre run` refuses a NORMAL run while another is STARTED within the watchdog grace, whatever its engine version (`runner.py:1075-1082`). Residual risk, existing and version-independent: a live run older than the grace. |
| R-6 | **Rechecks across versions.** RERUN, REPLAY and AS_OF_REPLAY write no key events. Results of a new engine get new `evaluation_id`s (`events.py:75-78`), and old results stay as stored. |
| R-7 | **Causes.** `failure_ref` key hashes are in the encoding of their run's engine (`dq_run.engine_version`). A retried investigation (`runner.py:773-800`) writes its own encoding. Trace looks up both (A-10). Each release declares its `KEY_ENCODING`, and the mapping from engine version to encoding is recorded in the release notes. |
| R-8 | **Hashes held outside the store.** Every `key_hash` changes once at N+1. Sampled key hashes in emails sent before N+1 do not match afterwards. Owners are told (O-3). |
| R-9 | **End-to-end demonstration** with real N and N+1 builds on a scratch store with synthetic data: N → N+1 → N → N+1. This is a merge gate for N+1 (05). |

## 8. Ingestion pipelines

**No ingestion pipeline needs modification.** Every change is inside DRE:
- the key bytes are computed by DRE's own SQL over tables it reads;
- the disorder bound is DRE configuration;
- refused types are reported by DRE;
- the store changes are additive and confined to `dq` (guard rules 2 and 11);
- the transition reads upstream tables only.

Owners are asked to approve or confirm facts (06: O-1 to O-3). They are never asked to change a pipeline, a schedule or a table (CLAUDE.md constraints 1 and 2).
